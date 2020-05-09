use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom, Write};
use std::marker::PhantomData;
use stream_cipher::SyncStreamCipher;
use zstd::stream::raw::{Decoder as ZDecoder, Encoder as ZEncoder, InBuffer, Operation, OutBuffer};

#[derive(Serialize, Deserialize, Debug)]
struct Test {
    inner: u32,
}

pub struct CborZstdArray<F, T> {
    data: Vec<u8>,
    sealed: bool,
    mk_cipher: F,
    _t: PhantomData<T>,
}

impl<F: Fn() -> C + Clone, T: DeserializeOwned, C: SyncStreamCipher> CborZstdArray<F, T> {
    pub fn new(mk_cipher: F, data: Vec<u8>, sealed: bool) -> Self {
        Self {
            data,
            mk_cipher,
            sealed,
            _t: PhantomData,
        }
    }

    pub fn as_ref<'a>(&'a self) -> CborZstdArrayRef<'a, F, T> {
        CborZstdArrayRef {
            data: &self.data,
            mk_cipher: self.mk_cipher.clone(),
            sealed: self.sealed,
            _t: PhantomData,
        }
    }
}

pub struct CborZstdArrayRef<'a, F, T> {
    data: &'a [u8],
    sealed: bool,
    mk_cipher: F,
    _t: PhantomData<T>,
}

impl<'a, F: Fn() -> C, T: DeserializeOwned, C: SyncStreamCipher> CborZstdArrayRef<'a, F, T> {
    pub fn new(mk_cipher: F, data: &'a [u8], sealed: bool) -> Self {
        Self {
            data,
            mk_cipher,
            sealed,
            _t: PhantomData,
        }
    }

    pub fn data(&self) -> &[u8] {
        self.data
    }

    pub fn sealed(&self) -> bool {
        self.sealed
    }

    pub fn items(&self) -> std::io::Result<Vec<T>> {
        let mut cipher = (self.mk_cipher)();
        // todo: avoid cloning the whole thing, but have a buffer for stream apply and decompression source
        let mut data = self.data.to_vec();
        cipher.apply_keystream(&mut data);
        let mut src = InBuffer::around(&data);
        let mut tmp = [0u8; 4096];
        let mut decompressor = ZDecoder::new()?;
        let mut uncompressed = Vec::<u8>::new();
        // decompress until input is consumed
        loop {
            let mut out: OutBuffer = OutBuffer::around(&mut tmp);
            let _ = decompressor.run(&mut src, &mut out)?;
            let n = out.pos;
            uncompressed.extend_from_slice(&tmp[..n]);
            if src.pos == src.src.len() {
                break;
            }
        }
        loop {
            let mut out: OutBuffer = OutBuffer::around(&mut tmp);
            let remaining = decompressor.flush(&mut out)?;
            let n = out.pos;
            uncompressed.extend_from_slice(&tmp[..n]);
            if remaining == 0 {
                break;
            }
        }
        if !self.sealed {
            uncompressed.push(CBOR_BREAK);
        }
        Ok(serde_cbor::from_slice(&uncompressed).unwrap())
    }
}

/// A builder that can be used to add items that are immediately incrementally
/// encoded, compressed and encrypted using a stream cipher.
///
/// The encoded data can be persisted at any time, even before sealing.
///
/// This is a relatively heavy data structure, since it contains a zstd compression context
/// as well as a stream cipher.
pub struct CborZstdArrayBuilder<C, T> {
    cbor_buffer: Vec<u8>,
    cipher_buffer: [u8; 4096],
    data: Vec<u8>,
    encoder: ZEncoder,
    cipher: C,
    _t: PhantomData<T>,
}

pub enum WriteMode {
    None,
    Flush,
    Finish,
}

impl<C: SyncStreamCipher, T: Serialize> CborZstdArrayBuilder<C, T> {
    pub fn new(cipher: C, level: i32) -> std::io::Result<Self> {
        Ok(Self {
            cbor_buffer: Vec::new(),
            cipher_buffer: [0; 4096],
            data: Vec::new(),
            encoder: ZEncoder::new(level)?,
            cipher,
            _t: PhantomData,
        })
    }

    pub fn data<'a, F: Fn() -> C>(&'a self, f: F) -> CborZstdArrayRef<'a, F, T>
        where T: DeserializeOwned
    {
        CborZstdArrayRef::new(f, self.data.as_ref(), false)
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn push(mut self, value: &T) -> std::io::Result<Self> {
        let writer = self.get_cbor_cursor()?;
        // write CBOR
        serde_cbor::to_writer(writer, &value).expect("CBOR encoding should not fail!");
        self.write_cbor_buffer()?;
        self.flush()?;
        Ok(self)
    }

    pub fn push_items<I: IntoIterator<Item=T>>(mut self, elems: I) -> std::io::Result<Self> {
        for item in elems.into_iter() {
            let writer = self.get_cbor_cursor()?;
            serde_cbor::to_writer(writer,&item).expect("CBOR encoding should not fail!");
            self.write_cbor_buffer()?;
        }
        self.flush()?;
        Ok(self)
    }

    pub fn seal(mut self) -> std::io::Result<Vec<u8>> {
        let mut writer = self.get_cbor_cursor()?;
        writer.write_all(&[CBOR_BREAK])?;
        self.compress_and_encrypt(WriteMode::Finish)?;
        Ok(self.data)
    }

    /// Get a writer to the cbor_buffer    
    fn get_cbor_cursor(&mut self) -> std::io::Result<Cursor<&mut Vec<u8>>> {
        let first = self.is_empty();
        self.cbor_buffer.clear();
        let mut writer = Cursor::new(&mut self.cbor_buffer);
        // add cbor array start before first element
        if first {
            writer.write_all(&[CBOR_ARRAY_START])?;
        }
        Ok(writer)
    }

    fn write_cbor_buffer(&mut self) -> std::io::Result<()> {
        let mut src = InBuffer::around(&self.cbor_buffer);
        // encode until input is consumed
        loop {
            let mut out: OutBuffer = OutBuffer::around(&mut self.cipher_buffer);
            // run encoder and move it forward
            let _ = self.encoder.run(&mut src, &mut out)?;
            let n = out.pos;
            // apply the cipher and move it forward
            self.cipher.apply_keystream(&mut self.cipher_buffer[..n]);
            // append to data
            self.data.extend_from_slice(&self.cipher_buffer[..n]);
            // break once output is consumed
            if src.pos == src.src.len() {
                break;
            }
        }
        Ok(())
    }

    fn finish(&mut self, finished_frame: bool) -> std::io::Result<()> {
        loop {
            let mut out: OutBuffer = OutBuffer::around(&mut self.cipher_buffer);
            // flush the encoder
            let remaining = self.encoder.finish(&mut out, finished_frame)?;
            let n = out.pos;
            // apply the cipher and move it forward
            self.cipher.apply_keystream(&mut self.cipher_buffer[..n]);
            // append to data
            self.data.extend_from_slice(&self.cipher_buffer[..n]);
            // break once everything is flushed
            if remaining == 0 {
                break;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        loop {
            let mut out: OutBuffer = OutBuffer::around(&mut self.cipher_buffer);
            // flush the encoder
            let remaining = self.encoder.flush(&mut out)?;
            let n = out.pos;
            // apply the cipher and move it forward
            self.cipher.apply_keystream(&mut self.cipher_buffer[..n]);
            // append to data
            self.data.extend_from_slice(&self.cipher_buffer[..n]);
            // break once everything is flushed
            if remaining == 0 {
                break;
            }
        }
        Ok(())
    }

    /// Compress and encrypt the content of the cbor buffer
    fn compress_and_encrypt(&mut self, mode: WriteMode) -> std::io::Result<()> {
        self.write_cbor_buffer()?;
        // flush or finish
        match mode {
            WriteMode::Flush => self.flush(),
            WriteMode::Finish => self.finish(true),
            WriteMode::None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use salsa20::Salsa20;
    use serde::de::DeserializeOwned;
    use stream_cipher::NewStreamCipher;

    /// create a test cipher
    fn test_cipher() -> Salsa20 {
        let key = [0u8; 32];
        let nonce = [0u8; 8];
        Salsa20::new(&key.into(), &nonce.into())
    }

    /// decode a completed cbor/zstd/cipher block
    fn test_decode<T: DeserializeOwned>(mut data: Vec<u8>) -> std::io::Result<Vec<T>> {
        let mut cipher = test_cipher();
        // undo the cipher
        cipher.apply_keystream(&mut data);
        // undo the compression
        let uncompressed = zstd::decode_all(Cursor::new(data))?;
        // deser
        Ok(serde_cbor::from_slice(&uncompressed).unwrap())
    }

    #[test]
    fn incremental_decode_test() {
        let mut target = Vec::<u8>::new();
        for x in 0..100 {
            serde_cbor::to_writer(&mut target, &x.to_string()).unwrap();
        }
        let r = Reader::new(&target);
        loop {
            let mut deserializer = serde_cbor::Deserializer::from_reader(r.clone());
            let res: std::result::Result<String, _> = serde::de::Deserialize::deserialize(&mut deserializer);
            if res.is_err() {
                break;
            }
            println!("{}", res.unwrap());
        }
    }

    use std::sync::Mutex;
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    struct Reader<'a>(Arc<Mutex<Cursor<&'a [u8]>>>);

    impl<'a> Reader<'a> {
        fn new(data: &'a [u8]) -> Self {
            Self(Arc::new(Mutex::new(Cursor::new(data))))
        }
    }

    impl<'a> std::io::Read for Reader<'a> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().read(buf)
        }
    }

    #[test]
    fn roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let cipher = test_cipher();
        let mut buffer: CborZstdArrayBuilder<Salsa20, u64> = CborZstdArrayBuilder::new(cipher, 10)?;
        let mut expected = Vec::<u64>::new();
        for i in 0..1000 {
            println!("push {}", i);
            buffer = buffer.push(&i)?;
            expected.push(i);
            let actual = buffer.data(|| test_cipher()).items()?;
            // let mut persisted = buffer.data.clone();
            // let actual = decode::<Salsa20, u64>(test_cipher(), &mut persisted, false)?;
            assert_eq!(actual, expected);
        }
        println!("seal");
        let data = buffer.seal()?;
        println!("sealed");
        println!("{}", hex::encode(&data));
        println!("{}", data.len());
        let decoded: Vec<u64> = test_decode(data)?;
        println!("decded {:?}", decoded);
        assert_eq!(expected, decoded);
        Ok(())
    }
}

const CBOR_ARRAY_START: u8 = (4 << 5) | 31;
const CBOR_BREAK: u8 = 255;

fn decode<C: SyncStreamCipher, T: DeserializeOwned>(
    mut cipher: C,
    data: &mut [u8],
    sealed: bool,
) -> std::io::Result<Vec<T>> {
    cipher.apply_keystream(data);
    let mut src = InBuffer::around(&data);
    let mut tmp = [0u8; 4096];
    let mut decompressor = ZDecoder::new()?;
    let mut uncompressed = Vec::<u8>::new();
    // decompress until input is consumed
    loop {
        let mut out: OutBuffer = OutBuffer::around(&mut tmp);
        let _ = decompressor.run(&mut src, &mut out)?;
        let n = out.pos;
        uncompressed.extend_from_slice(&tmp[..n]);
        if src.pos == src.src.len() {
            break;
        }
    }
    loop {
        let mut out: OutBuffer = OutBuffer::around(&mut tmp);
        let remaining = decompressor.flush(&mut out)?;
        let n = out.pos;
        uncompressed.extend_from_slice(&tmp[..n]);
        if remaining == 0 {
            break;
        }
    }
    if !sealed {
        uncompressed.push(CBOR_BREAK);
    }
    Ok(serde_cbor::from_slice(&uncompressed).unwrap())
}

fn transform<O: Operation, W: Write>(encoder: &mut O, data: &[u8], mut w: W) -> std::io::Result<W> {
    let mut src = InBuffer::around(data);
    let mut tmp = [0u8; 1024];
    // encode until input is consumed
    loop {
        let mut out: OutBuffer = OutBuffer::around(&mut tmp);
        let size_hint = encoder.run(&mut src, &mut out)?;
        println!("{:?} {:?} {}", src, out, size_hint);
        let n = out.pos;
        w.write_all(&mut tmp[0..n])?;
        if src.pos == src.src.len() {
            break;
        }
    }
    Ok(w)
}

fn flush<W: Write>(encoder: &mut ZEncoder, mut w: W) -> std::io::Result<W> {
    let mut tmp = [0u8; 1024];
    // finish it
    loop {
        let mut out: OutBuffer = OutBuffer::around(&mut tmp);
        let remaining = encoder.flush(&mut out)?;
        println!("{:?} {}", out, remaining);
        let n = out.pos;
        w.write_all(&mut tmp[0..n])?;
        if remaining == 0 {
            break;
        }
    }
    Ok(w)
}

fn finish<W: Write>(encoder: &mut ZEncoder, finished_frame: bool, mut w: W) -> std::io::Result<W> {
    let mut tmp = [0u8; 1024];
    // finish it
    loop {
        let mut out: OutBuffer = OutBuffer::around(&mut tmp);
        let remaining = encoder.finish(&mut out, finished_frame)?;
        println!("{:?} {}", out, remaining);
        let n = out.pos;
        w.write_all(&mut tmp[0..n])?;
        if remaining == 0 {
            break;
        }
    }
    Ok(w)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tgt: Vec<u8> = Vec::new();
    tgt.push(CBOR_ARRAY_START);
    for x in 0..10 {
        let value = Test { inner: x };
        let mut writer = Cursor::new(&mut tgt);
        writer.seek(SeekFrom::End(0)).unwrap();
        serde_cbor::to_writer(writer, &value).unwrap();
    }
    tgt.push(CBOR_BREAK);
    
    let res: Vec<Test> = serde_cbor::from_slice(&tgt)?;
    println!("CBOR {:?}", res);

    let mut encoder = ZEncoder::new(10)?;
    let tgt: Vec<u8> = Vec::new();
    let tgt = transform(&mut encoder, b"ABCDEFGHABCDEFGHABCDEFGHABCDEFGH", tgt)?;
    let tgt = flush(&mut encoder, tgt)?;
    let tgt = transform(&mut encoder, b"ABCDEFGHABCDEFGHABCDEFGHABCDEFGH", tgt)?;
    let tgt = flush(&mut encoder, tgt)?;
    // let tgt = finish(&mut encoder, true, tgt)?;
    println!("CBOR-ZSTD {:?}", tgt);

    let dec = zstd::decode_all(Cursor::new(tgt.clone()));
    println!("{:?}", dec);

    let mut decoder = ZDecoder::new()?;
    let decompressed: Vec<u8> = Vec::new();
    let decompressed = transform(&mut decoder, &tgt, decompressed)?;
    println!("{:?}", decompressed);

    Ok(())
}
