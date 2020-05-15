use futures::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom, Write};
use std::sync::Arc;
use zstd::stream::raw::{Decoder as ZDecoder, Encoder as ZEncoder, InBuffer, Operation, OutBuffer};

mod czaa;
mod tree;
mod zstd_array;

use tree::*;

const CBOR_ARRAY_START: u8 = (4 << 5) | 31;
const CBOR_BREAK: u8 = 255;

fn decode<T: DeserializeOwned>(data: &mut [u8]) -> std::io::Result<Vec<T>> {
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

#[derive(Debug, Serialize, Deserialize)]
struct Test {
    inner: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    println!("building a tree");
    let store = TestStore::new();
    let forest = Arc::new(Forest::new(Arc::new(store)));
    let mut tree = Tree::<u64>::new(forest);
    for i in 0..1000 {
        println!("{}", i);
        let res = tree.push(&i).await?;
    }

    for i in 0..1000 {
        println!("{:?}", tree.get(i).await?);
    }

    tree.dump().await?;

    let mut stream = tree.stream();
    while let Some(Ok(v)) = stream.next().await {
        println!("{:?}", v);
    }

    Ok(())
}
