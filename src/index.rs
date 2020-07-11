//! The index data structures for the tree
use super::ipfs::Cid;
use super::zstd_array::{ZstdArray, ZstdArrayBuilder, ZstdArrayRef};
use anyhow::{anyhow, Result};
use bitvec::prelude::*;
use derive_more::From;
use salsa20::{
    stream_cipher::{NewStreamCipher, SyncStreamCipher},
    XSalsa20,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{convert::From, sync::Arc};

/// trait for items that can be combined in an associative way
///
/// Currently all examples I can think of are also commutative, so it would be an [abelian semigroup](https://mathworld.wolfram.com/AbelianSemigroup.html).
/// But I am not sure we need to require that as of now.
pub trait Semigroup {
    fn combine(&mut self, b: &Self);
}
/// a compact representation of a sequence of 1 or more items
///
/// in general, this will have a different internal representation than just a bunch of values that is more compact and
/// makes it easier to query an entire sequence for matching indices.
pub trait CompactSeq: Serialize + DeserializeOwned {
    /// item type
    type Item: Semigroup;
    /// Creates a sequence with a single element
    fn empty() -> Self;
    /// Creates a sequence with a single element
    fn single(item: &Self::Item) -> Self;
    /// pushes an additional element to the end
    fn push(&mut self, value: &Self::Item);
    /// extends the last element with the item
    fn extend(&mut self, value: &Self::Item);
    /// number of elements
    fn count(&self) -> u64;
    /// number of elements, as an usize, for convenience
    fn len(&self) -> usize {
        self.count() as usize
    }
    /// get nth element. Guaranteed to succeed with Some for index < count.
    fn get(&self, index: usize) -> Option<Self::Item>;
    /// combines all elements with the semigroup op
    fn summarize(&self) -> Self::Item;

    fn new(mut items: impl Iterator<Item = Self::Item>) -> Result<Self> {
        let mut result = Self::single(
            &items
                .next()
                .ok_or(anyhow!("iterator must have at least one item"))?,
        );
        for item in items {
            result.push(&item);
        }
        Ok(result)
    }

    /// utility function to get all items for a compactseq.
    fn to_vec(&self) -> Vec<Self::Item> {
        (0..self.len()).map(move |i| self.get(i).unwrap()).collect()
    }

    /// utility function to select some items for a compactseq.
    fn select(&self, bits: &BitVec) -> Vec<(usize, Self::Item)> {
        (0..self.len())
            .filter_map(move |i| {
                if bits[i] {
                    Some((i, self.get(i).unwrap()))
                } else {
                    None
                }
            })
            .collect()
    }
}

/// A trivial implementation of a CompactSeq as just a Seq.
///
/// This is useful mostly as a reference impl and for testing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleCompactSeq<T>(Vec<T>);

impl<T: Serialize + DeserializeOwned + Semigroup + Clone> CompactSeq for SimpleCompactSeq<T> {
    type Item = T;
    fn empty() -> Self {
        Self(Vec::new())
    }
    fn single(item: &T) -> Self {
        Self(vec![item.clone()])
    }
    fn push(&mut self, item: &T) {
        self.0.push(item.clone())
    }
    fn extend(&mut self, item: &T) {
        self.0.last_mut().unwrap().combine(item);
    }
    fn get(&self, index: usize) -> Option<T> {
        self.0.get(index).cloned()
    }
    fn count(&self) -> u64 {
        self.0.len() as u64
    }
    fn summarize(&self) -> T {
        let mut res = self.0[0].clone();
        for i in 1..self.0.len() {
            res.combine(&self.0[i]);
        }
        res
    }
}

/// index for a leaf of n events
#[derive(Debug, Clone)]
pub struct LeafIndex<L, T> {
    // block is sealed
    pub sealed: bool,
    // link to the block
    pub cid: Option<L>,
    /// A sequence of keys with the same number of values as the data block the cid points to.
    pub keys: T,
    // serialized size of the data
    pub value_bytes: u64,
}

impl<L, T: CompactSeq> LeafIndex<L, T> {
    pub fn keys(&self) -> impl Iterator<Item = T::Item> {
        self.keys.to_vec().into_iter()
    }
    pub fn select_keys(&self, bits: &BitVec) -> impl Iterator<Item = (usize, T::Item)> {
        self.keys.select(bits).into_iter()
    }
}

/// index for a branch node
#[derive(Debug, Clone)]
pub struct BranchIndex<L, T> {
    // number of events
    pub count: u64,
    // level of the tree node
    pub level: u32,
    // block is sealed
    pub sealed: bool,
    // link to the branch node
    pub cid: Option<L>,
    // extra data
    pub summaries: T,
    // serialized size of the children
    pub value_bytes: u64,
    // serialized size of the data
    pub key_bytes: u64,
}

impl<L, T: CompactSeq> BranchIndex<L, T> {
    pub fn summaries<'a>(&'a self) -> impl Iterator<Item = T::Item> + 'a {
        self.summaries.to_vec().into_iter()
    }
}

/// index
#[derive(Debug, Clone, From)]
pub enum Index<L, T> {
    Leaf(LeafIndex<L, T>),
    Branch(BranchIndex<L, T>),
}

impl<L, T: CompactSeq> Index<L, T> {
    pub fn data(&self) -> &T {
        match self {
            Index::Leaf(x) => &x.keys,
            Index::Branch(x) => &x.summaries,
        }
    }

    pub fn cid(&self) -> &Option<L> {
        match self {
            Index::Leaf(x) => &x.cid,
            Index::Branch(x) => &x.cid,
        }
    }
    pub fn count(&self) -> u64 {
        match self {
            Index::Leaf(x) => x.keys.count(),
            Index::Branch(x) => x.count,
        }
    }
    pub fn sealed(&self) -> bool {
        match self {
            Index::Leaf(x) => x.sealed,
            Index::Branch(x) => x.sealed,
        }
    }
    pub fn level(&self) -> u32 {
        match self {
            Index::Leaf(_) => 0,
            Index::Branch(x) => x.level,
        }
    }
    pub fn value_bytes(&self) -> u64 {
        match self {
            Index::Leaf(x) => x.value_bytes,
            Index::Branch(x) => x.value_bytes,
        }
    }
    pub fn key_bytes(&self) -> u64 {
        match self {
            Index::Leaf(_) => 0,
            Index::Branch(x) => x.key_bytes,
        }
    }
}

#[derive(Debug, Clone)]
/// fully in memory representation of a branch node
pub struct Branch<L, T> {
    // index data for the children
    pub children: Vec<Index<L, T>>,
}

impl<L: Clone, T: Clone> Branch<L, T> {
    pub fn new(children: Vec<Index<L, T>>) -> Self {
        assert!(!children.is_empty());
        Self { children }
    }
    pub fn last_child(&mut self) -> &Index<L, T> {
        self.children
            .last()
            .expect("branch can never have 0 children")
    }
    pub fn last_child_mut(&mut self) -> &mut Index<L, T> {
        self.children
            .last_mut()
            .expect("branch can never have 0 children")
    }
}

/// fully in memory representation of a leaf node
#[derive(Debug)]
pub struct Leaf(ZstdArray);

impl Leaf {
    /// Create a leaf from data in readonly mode. Conversion to writeable will only happen on demand.
    ///
    /// Note that this does not provide any validation that the passed data is in fact zstd compressed cbor.    
    /// If you pass random data, you will only notice that something is wrong once you try to use it.
    pub fn new(data: Arc<[u8]>) -> Self {
        Self(ZstdArray::new(data))
    }

    pub fn from_builder(builder: ZstdArrayBuilder) -> Result<Self> {
        Ok(Self(builder.build()?))
    }

    pub fn builder(self, level: i32) -> Result<ZstdArrayBuilder> {
        ZstdArrayBuilder::init((&self.0).compressed(), level)
    }

    /// Create a leaf containing a single item, with the given compression level
    pub fn single<V: Serialize>(value: &V, level: i32) -> Result<Self> {
        Ok(Leaf::from_builder(
            ZstdArrayBuilder::new(level)?.push(value)?,
        )?)
    }

    /// Push an item. The compression level will only be used if this leaf is in readonly mode, otherwise
    /// the compression level of the builder will be used.
    pub fn fill<V: Serialize>(
        self,
        from: impl FnMut() -> Option<V>,
        compressed_size: u64,
        level: i32,
    ) -> Result<Self> {
        Leaf::from_builder(self.builder(level)?.fill(from, compressed_size)?)
    }

    pub fn as_ref(&self) -> &ZstdArrayRef {
        &self.0
    }
}

pub(crate) enum NodeInfo<'a, L, T> {
    Branch(&'a BranchIndex<L, T>, Branch<L, T>),
    Leaf(&'a LeafIndex<L, T>, Leaf),
    PurgedBranch(&'a BranchIndex<L, T>),
    PurgedLeaf(&'a LeafIndex<L, T>),
}

impl Leaf {
    pub fn child_at<T: DeserializeOwned>(&self, offset: u64) -> Result<T> {
        self.as_ref()
            .get(offset)?
            .ok_or_else(|| anyhow!("index out of bounds {}", offset).into())
    }
}

#[derive(Debug, Clone, Serialize)]
struct IndexWC<'a, T> {
    // number of events
    count: Option<u64>,
    // level of the tree node
    level: Option<u32>,
    key_bytes: Option<u64>,
    value_bytes: u64,
    // block is sealed
    sealed: bool,
    // block is purged
    purged: bool,
    // extra data
    data: &'a T,
}

impl<'a, L, T> From<&'a Index<L, T>> for IndexWC<'a, T> {
    fn from(value: &'a Index<L, T>) -> Self {
        match value {
            Index::Branch(i) => Self {
                sealed: i.sealed,
                purged: i.cid.is_none(),
                data: &i.summaries,
                value_bytes: i.value_bytes,
                count: Some(i.count),
                level: Some(i.level),
                key_bytes: Some(i.key_bytes),
            },
            Index::Leaf(i) => Self {
                sealed: i.sealed,
                purged: i.cid.is_none(),
                data: &i.keys,
                value_bytes: i.value_bytes,
                count: None,
                level: None,
                key_bytes: None,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct IndexRC<T> {
    // block is sealed
    sealed: bool,
    // block is purged
    purged: bool,
    // extra data
    data: T,
    // number of events, for branches
    count: Option<u64>,
    // level of the tree node, for branches
    level: Option<u32>,
    // value bytes
    value_bytes: u64,
    // key bytes
    key_bytes: Option<u64>,
}

impl<T> IndexRC<T> {
    fn to_index<L>(self, cids: &mut VecDeque<L>) -> Index<L, T> {
        let cid = if !self.purged { cids.pop_front() } else { None };
        if let (Some(level), Some(count), Some(key_bytes)) =
            (self.level, self.count, self.key_bytes)
        {
            BranchIndex {
                summaries: self.data,
                sealed: self.sealed,
                value_bytes: self.value_bytes,
                key_bytes,
                count,
                level,
                cid,
            }
            .into()
        } else {
            LeafIndex {
                keys: self.data,
                sealed: self.sealed,
                value_bytes: self.value_bytes,
                cid,
            }
            .into()
        }
    }
}

use std::{
    collections::VecDeque,
    io::{Cursor, Write},
};

const CBOR_ARRAY_START: u8 = (4 << 5) | 31;
const CBOR_BREAK: u8 = 255;

pub fn serialize_compressed<L: Serialize, T: Serialize + CompactSeq>(
    key: &salsa20::Key,
    nonce: &salsa20::XNonce,
    items: &[Index<L, T>],
    level: i32,
    into: &mut Vec<u8>,
) -> Result<()> {
    let mut cids: Vec<&L> = Vec::new();
    let mut compressed: Vec<u8> = Vec::new();
    compressed.extend_from_slice(&nonce);
    let mut writer = zstd::stream::write::Encoder::new(compressed.by_ref(), level)?;
    writer.write_all(&[CBOR_ARRAY_START])?;
    for item in items.iter() {
        if let Some(cid) = item.cid() {
            cids.push(cid);
        }
        serde_cbor::to_writer(writer.by_ref(), &IndexWC::from(item))?;
    }
    writer.write_all(&[CBOR_BREAK])?;
    writer.finish()?;
    salsa20::XSalsa20::new(key, nonce).apply_keystream(&mut compressed[24..]);
    Ok(serde_cbor::to_writer(
        into,
        &(cids, serde_cbor::Value::Bytes(compressed)),
    )?)
}

pub fn deserialize_compressed<L: DeserializeOwned, T: DeserializeOwned>(
    key: &salsa20::Key,
    ipld: &[u8],
) -> Result<Vec<Index<L, T>>> {
    let (mut cids, compressed): (VecDeque<L>, serde_cbor::Value) = serde_cbor::from_slice(ipld)?;
    if let serde_cbor::Value::Bytes(mut compressed) = compressed {
        if compressed.len() < 24 {
            return Err(anyhow!("nonce missing"));
        }
        let (nonce, compressed) = compressed.split_at_mut(24);
        XSalsa20::new(key, (&*nonce).into()).apply_keystream(compressed);
        let reader = zstd::stream::read::Decoder::new(Cursor::new(compressed))?;

        let data: Vec<IndexRC<T>> = serde_cbor::from_reader(reader)?;
        let result = data
            .into_iter()
            .map(|data| data.to_index(&mut cids))
            .collect::<Vec<_>>();
        Ok(result)
    } else {
        Err(anyhow!(
            "expected a byte array containing zstd compressed cbor"
        ))
    }
}
