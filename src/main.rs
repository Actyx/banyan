use futures::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::prelude::*;
use std::io::{Cursor, SeekFrom, Write};
use std::{collections::BTreeSet, sync::Arc};
use zstd::stream::raw::{Decoder as ZDecoder, Encoder as ZEncoder, InBuffer, Operation, OutBuffer};

mod czaa;
mod forest;
mod ipfs;
mod tree;
mod zstd_array;

use forest::{Semigroup, SimpleCompactSeq, CompactSeq};
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

struct TT();

impl Semigroup for u32 {
    fn combine(&mut self, b: &Self) {
        *self = std::cmp::Ord::max(*self, *b);
    }
}

impl TreeTypes for TT {
    type Key = Value;
    type Seq = ValueSeq;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Ord, Eq)]
struct Tag(String);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tags(BTreeSet<Tag>);

impl Tags {
    fn empty() -> Self {
        Self(BTreeSet::new())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Value {
    min_lamport: u64,
    min_time: u64,
    max_time: u64,
    tags: Tags,
}

impl Value {
    fn single(lamport: u64, time: u64, tags: Tags) -> Self {
        Self {
            min_lamport: lamport,
            min_time: time,
            max_time: time,
            tags,
        }
    }
}

impl Semigroup for Value {
    fn combine(&mut self, b: &Value) {
        self.min_lamport = self.min_lamport.min(b.min_lamport);
        self.min_time = self.min_time.min(b.min_time);
        self.max_time = self.max_time.max(b.max_time);
        self.tags.0.extend(b.tags.0.iter().cloned());
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ValueSeq {
    min_lamport: Vec<u64>,
    min_time: Vec<u64>,
    max_time: Vec<u64>,
    tags: Vec<Tags>,
}

impl CompactSeq for ValueSeq {
    type Item = Value;
    
    fn single(value: &Value) -> Self {
        Self {
            min_lamport: vec![value.min_lamport],
            min_time: vec![value.min_time],
            max_time: vec![value.max_time],
            tags: vec![value.tags.clone()],
        }
    }

    fn push(&mut self, value: &Value) {
        self.min_lamport.push(value.min_lamport);
        self.min_time.push(value.min_time);
        self.max_time.push(value.max_time);
        self.tags.push(value.tags.clone());
    }

    fn extend(&mut self, value: &Value) {
        let min_lamport = self.min_lamport.last_mut().unwrap();
        let min_time = self.min_time.last_mut().unwrap();
        let max_time = self.max_time.last_mut().unwrap();
        let tags = self.tags.last_mut().unwrap();
        *min_lamport = value.min_lamport.min(*min_lamport);
        *min_time = value.min_time.min(*min_time);
        *max_time = value.max_time.min(*max_time);
        tags.0.extend(value.tags.0.iter().cloned());
    }

    fn get(&self, index: usize) -> Option<Value> {
        if let (
            Some(min_lamport),
            Some(min_time),
            Some(max_time),
            Some(tags)) =
        (
            self.min_lamport.get(index),
            self.min_time.get(index),
            self.max_time.get(index),
            self.tags.get(index),
        ) {
            Some(Value {
                min_lamport: *min_lamport,
                min_time: *min_time,
                max_time: *max_time,
                tags: tags.clone(),
            })
        } else {
            None
        }
    }

    fn items(&self) -> Vec<Value> {
        let mut items = Vec::new();
        for i in 0..self.tags.len() {
            items.push(self.get(i).unwrap());
        }
        items
    }

    fn summarize(&self) -> Value {
        let mut result = self.get(0).unwrap();        
        for i in 1..self.tags.len() {
            result.combine(&self.get(i).unwrap());
        }
        result
    }
}

#[tokio::main]
async fn main() -> Result<()> {
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
    // let store = TestStore::new();
    let store = IpfsStore::new();
    let forest = Arc::new(Forest::new(Arc::new(store)));
    let mut tree = Tree::<TT, u64>::new(forest.clone());
    tree.push(&Value::single(0, 0, Tags::empty()), &0u64).await?;
    println!("{:?}", tree.get(0).await?);

    let mut tree = Tree::<TT, u64>::new(forest);
    for i in 0..1000 {
        println!("{}", i);
        tree.push(&Value::single(i,i, Tags::empty()), &i).await?;
    }

    tree.dump().await?;

    for i in 0..1000 {
        println!("{:?}", tree.get(i).await?);
    }

    let mut stream = tree.stream();
    while let Some(Ok(v)) = stream.next().await {
        println!("{:?}", v);
    }

    println!("{:?}", tree);

    Ok(())
}
