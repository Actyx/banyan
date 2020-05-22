use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom, Write};
use std::marker::PhantomData;
use zstd::stream::raw::{Decoder as ZDecoder, Encoder as ZEncoder, InBuffer, Operation, OutBuffer};

#[derive(Serialize, Deserialize, Debug)]
struct Test {
    inner: u32,
}

pub struct CborZstdArray<T> {
    data: Vec<u8>,
    _t: PhantomData<T>,
}

impl<T: DeserializeOwned> CborZstdArray<T> {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            _t: PhantomData,
        }
    }

    pub fn as_ref<'a>(&'a self) -> CborZstdArrayRef<'a, T> {
        CborZstdArrayRef {
            data: &self.data,
            _t: PhantomData,
        }
    }
}

pub struct CborZstdArrayRef<'a, T> {
    data: &'a [u8],
    _t: PhantomData<T>,
}

impl<'a, T> CborZstdArrayRef<'a, T> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            _t: PhantomData,
        }
    }
}

impl<'a, T: DeserializeOwned> CborZstdArrayRef<'a, T> {
    pub fn data(&self) -> &[u8] {
        self.data
    }

    pub fn items(&self) -> std::io::Result<Vec<T>> {
        // let mut cipher = (self.mk_cipher)();
        // todo: avoid cloning the whole thing, but have a buffer for stream apply and decompression source
        let data = self.data.to_vec();
        // cipher.apply_keystream(&mut data);
        let mut src = InBuffer::around(&data);
        // todo: thread local buffers that grow dynamically
        let mut tmp = [0u8; 4096 * 10];
        let mut decompressor = ZDecoder::new()?;
        let mut uncompressed = Vec::<u8>::new();
        uncompressed.push(CBOR_ARRAY_START);
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
        uncompressed.push(CBOR_BREAK);
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
pub struct CborZstdArrayBuilder<T> {
    cbor_buffer: Vec<u8>,
    cipher_buffer: [u8; 4096],
    data: Vec<u8>,
    encoder: ZEncoder,
    len: u64,
    _t: PhantomData<T>,
}

impl<T> std::fmt::Debug for CborZstdArrayBuilder<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CborZstdArrayBuilder")
    }
}

impl<T> Clone for CborZstdArrayBuilder<T> {
    fn clone(&self) -> Self {
        panic!()
    }
}

pub enum WriteMode {
    None,
    Flush,
    Finish,
}

impl<T: Serialize + DeserializeOwned> CborZstdArrayBuilder<T> {
    pub fn init(data: &[u8], level: i32) -> std::io::Result<Self> {
        let items = CborZstdArrayRef::<T>::new(data).items()?;
        let mut res = CborZstdArrayBuilder::new(level)?;
        for item in items.iter() {
            res = res.push(item)?;
        }
        Ok(res)
    }
}

impl<T> CborZstdArrayBuilder<T> {
    pub fn new(level: i32) -> std::io::Result<Self> {
        Ok(Self {
            cbor_buffer: Vec::new(),
            cipher_buffer: [0; 4096],
            data: Vec::new(),
            encoder: ZEncoder::new(level)?,
            len: 0,
            _t: PhantomData,
        })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn buffer(&self) -> &[u8] {
        &self.data
    }
}

impl<T: Serialize> CborZstdArrayBuilder<T> {
    pub fn data<'a>(&'a self) -> CborZstdArrayRef<'a, T>
    where
        T: DeserializeOwned,
    {
        CborZstdArrayRef::new(self.data.as_ref())
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn push(mut self, value: &T) -> std::io::Result<Self> {
        let writer = self.get_cbor_cursor()?;
        // write CBOR
        serde_cbor::to_writer(writer, &value).expect("CBOR encoding should not fail!");
        self.write_cbor_buffer()?;
        self.flush()?;
        self.len += 1;
        Ok(self)
    }

    pub fn push_items<I: IntoIterator<Item = T>>(mut self, elems: I) -> std::io::Result<Self> {
        for item in elems.into_iter() {
            let writer = self.get_cbor_cursor()?;
            serde_cbor::to_writer(writer, &item).expect("CBOR encoding should not fail!");
            self.write_cbor_buffer()?;
            self.len += 1;
        }
        self.flush()?;
        Ok(self)
    }

    pub fn seal(self) -> std::io::Result<Vec<u8>> {
        Ok(self.data)
    }

    /// Get a writer to the cbor_buffer    
    fn get_cbor_cursor(&mut self) -> std::io::Result<Cursor<&mut Vec<u8>>> {
        self.cbor_buffer.clear();
        Ok(Cursor::new(&mut self.cbor_buffer))
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
            // self.cipher.apply_keystream(&mut self.cipher_buffer[..n]);
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
            // self.cipher.apply_keystream(&mut self.cipher_buffer[..n]);
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
            // self.cipher.apply_keystream(&mut self.cipher_buffer[..n]);
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
    use stream_cipher::NewStreamCipher;

    /// create a test cipher
    fn test_cipher() -> Salsa20 {
        let key = [0u8; 32];
        let nonce = [0u8; 8];
        Salsa20::new(&key.into(), &nonce.into())
    }

    #[test]
    fn incremental_decode_test() {
        let mut target = Vec::<u8>::new();
        for x in 0..100 {
            serde_cbor::to_writer(&mut target, &x.to_string()).unwrap();
        }
        let mut r = Cursor::new(&target);
        loop {
            let mut deserializer = serde_cbor::Deserializer::from_reader(r.by_ref());
            let res: std::result::Result<String, _> =
                serde::de::Deserialize::deserialize(&mut deserializer);
            if res.is_err() {
                break;
            }
            println!("{}", res.unwrap());
        }
    }

    use std::sync::Arc;
    use std::sync::Mutex;

    #[test]
    fn roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        // let cipher = test_cipher();
        let mut buffer: CborZstdArrayBuilder<u64> = CborZstdArrayBuilder::new(10)?;
        let mut expected = Vec::<u64>::new();
        for i in 0..3 {
            println!("push {}", i);
            buffer = buffer.push(&i)?;
            println!(
                "buffer {} {}",
                hex::encode(buffer.buffer()),
                buffer.buffer().len()
            );
            expected.push(i);
            let actual = buffer.data().items()?;
            // let mut persisted = buffer.data.clone();
            // let actual = decode::<Salsa20, u64>(test_cipher(), &mut persisted, false)?;
            assert_eq!(actual, expected);
        }
        Ok(())
    }
}

const CBOR_ARRAY_START: u8 = (4 << 5) | 31;
const CBOR_BREAK: u8 = 255;

fn decode<T: DeserializeOwned>(data: &mut [u8], sealed: bool) -> std::io::Result<Vec<T>> {
    // cipher.apply_keystream(data);
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
