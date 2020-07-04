//! The index data structures for the tree
use super::ipfs::Cid;
use super::zstd_array::ZstdArrayBuilder;
use anyhow::{anyhow, Result};
use bitvec::prelude::*;
use derive_more::From;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};
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
    /// get nth element. Guaranteed to succeed with Some for index < count.
    fn get(&self, index: u64) -> Option<Self::Item>;
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
        (0..self.count())
            .map(move |i| self.get(i).unwrap())
            .collect()
    }

    /// utility function to select some items for a compactseq.
    fn select(&self, bits: &BitVec) -> Vec<(u64, Self::Item)> {
        (0..self.count())
            .filter_map(move |i| {
                if bits[i as usize] {
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
    fn get(&self, index: u64) -> Option<T> {
        self.0.get(index as usize).cloned()
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
pub struct LeafIndex<T> {
    // block is sealed
    pub sealed: bool,
    // link to the block
    pub cid: Option<Cid>,
    /// A sequence of keys with the same number of values as the data block the cid points to.
    pub keys: T,
    // serialized size of the data
    pub value_bytes: u64,
}

impl<T: CompactSeq> LeafIndex<T> {
    pub fn keys(&self) -> impl Iterator<Item = T::Item> {
        self.keys.to_vec().into_iter()
    }
    pub fn select_keys(&self, bits: &BitVec) -> impl Iterator<Item = (u64, T::Item)> {
        self.keys.select(bits).into_iter()
    }
}

/// index for a branch node
#[derive(Debug, Clone)]
pub struct BranchIndex<T> {
    // number of events
    pub count: u64,
    // level of the tree node
    pub level: u32,
    // block is sealed
    pub sealed: bool,
    // link to the branch node
    pub cid: Option<Cid>,
    // extra data
    pub summaries: T,
    // serialized size of the children
    pub value_bytes: u64,
    // serialized size of the data
    pub key_bytes: u64,
}

impl<T: CompactSeq> BranchIndex<T> {
    pub fn summaries<'a>(&'a self) -> impl Iterator<Item = T::Item> + 'a {
        self.summaries.to_vec().into_iter()
    }
}

#[derive(Debug, Clone, Serialize)]
struct IndexW<'a, T> {
    // number of events
    count: Option<u64>,
    // level of the tree node
    level: Option<u32>,
    // block is sealed
    sealed: bool,
    // link to the branch node
    cid: Option<&'a Cid>,
    // extra data
    data: &'a T,
    value_bytes: u64,
    key_bytes: Option<u64>,
}

impl<'a, T> From<&'a Index<T>> for IndexW<'a, T> {
    fn from(value: &'a Index<T>) -> Self {
        match value {
            Index::Branch(i) => Self {
                sealed: i.sealed,
                value_bytes: i.value_bytes,
                cid: i.cid.as_ref(),
                data: &i.summaries,
                count: Some(i.count),
                level: Some(i.level),
                key_bytes: Some(i.key_bytes),
            },
            Index::Leaf(i) => Self {
                sealed: i.sealed,
                value_bytes: i.value_bytes,
                cid: i.cid.as_ref(),
                data: &i.keys,
                count: None,
                level: None,
                key_bytes: None,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct IndexR<T> {
    // number of events
    count: Option<u64>,
    // level of the tree node
    level: Option<u32>,
    // value bytes
    key_bytes: Option<u64>,
    // block is sealed
    sealed: bool,
    // link to the branch node
    cid: Option<Cid>,
    // extra data
    data: T,
    // value bytes
    value_bytes: u64,
}

impl<T> From<IndexR<T>> for Index<T> {
    fn from(v: IndexR<T>) -> Self {
        if let (Some(level), Some(count), Some(key_bytes)) = (v.level, v.count, v.key_bytes) {
            BranchIndex {
                cid: v.cid,
                summaries: v.data,
                sealed: v.sealed,
                value_bytes: v.value_bytes,
                key_bytes,
                count,
                level,
            }
            .into()
        } else {
            LeafIndex {
                cid: v.cid,
                keys: v.data,
                sealed: v.sealed,
                value_bytes: v.value_bytes,
            }
            .into()
        }
    }
}

/// index
#[derive(Debug, Clone, From)]
pub enum Index<T> {
    Leaf(LeafIndex<T>),
    Branch(BranchIndex<T>),
}

use crate::zstd_array::{ZstdArray, ZstdArrayRef};
use std::result;

impl<T: Serialize> Serialize for Index<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> result::Result<S::Ok, S::Error> {
        IndexW::<T>::from(self).serialize(serializer)
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for Index<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> result::Result<Index<T>, D::Error> {
        IndexR::<T>::deserialize(deserializer).map(Into::into)
    }
}

impl<T: CompactSeq> Index<T> {
    pub fn data(&self) -> &T {
        match self {
            Index::Leaf(x) => &x.keys,
            Index::Branch(x) => &x.summaries,
        }
    }

    pub fn cid(&self) -> &Option<Cid> {
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
pub struct Branch<T> {
    // index data for the children
    pub children: Vec<Index<T>>,
}

impl<T: Clone> Branch<T> {
    pub fn new(children: Vec<Index<T>>) -> Self {
        assert!(!children.is_empty());
        Self { children }
    }
    pub fn last_child(&mut self) -> &Index<T> {
        self.children
            .last()
            .expect("branch can never have 0 children")
    }
    pub fn last_child_mut(&mut self) -> &mut Index<T> {
        self.children
            .last_mut()
            .expect("branch can never have 0 children")
    }
}

/// fully in memory representation of a leaf node
#[derive(Debug)]
pub enum Leaf {
    Writable(ZstdArrayBuilder),
    Readonly(ZstdArray),
}

impl Leaf {
    /// Create a leaf from data in readonly mode. Conversion to writeable will only happen on demand.
    ///
    /// Note that this does not provide any validation that the passed data is in fact zstd compressed cbor.    
    /// If you pass random data, you will only notice that something is wrong once you try to use it.
    pub fn new(data: Arc<[u8]>) -> Self {
        Self::Readonly(ZstdArray::new(data))
    }

    pub fn builder(self, level: i32) -> Result<ZstdArrayBuilder> {
        match self {
            Leaf::Writable(x) => Ok(x),
            Leaf::Readonly(x) => ZstdArrayBuilder::init(x.as_ref().compressed(), level),
        }
    }

    /// Create a leaf containing a single item, with the given compression level
    pub fn single<V: Serialize>(value: &V, level: i32) -> Result<Self> {
        Ok(Leaf::Writable(ZstdArrayBuilder::new(level)?.push(value)?))
    }

    /// Push an item. The compression level will only be used if this leaf is in readonly mode, otherwise
    /// the compression level of the builder will be used.
    pub fn push<V: Serialize>(self, value: &V, level: i32) -> Result<Self> {
        Ok(Leaf::Writable(self.builder(level)?.push(value)?))
    }

    /// Push an item. The compression level will only be used if this leaf is in readonly mode, otherwise
    /// the compression level of the builder will be used.
    pub fn fill<V: Serialize>(
        self,
        from: impl FnMut() -> Option<V>,
        compressed_size: u64,
        level: i32,
    ) -> Result<Self> {
        Ok(Leaf::Writable(
            self.builder(level)?.fill(from, compressed_size)?,
        ))
    }

    pub fn as_ref(&self) -> ZstdArrayRef {
        match self {
            Leaf::Writable(x) => x.as_ref(),
            Leaf::Readonly(x) => x.as_ref(),
        }
    }
}

pub(crate) enum NodeInfo<'a, T> {
    Branch(&'a BranchIndex<T>, Branch<T>),
    Leaf(&'a LeafIndex<T>, Leaf),
    PurgedBranch(&'a BranchIndex<T>),
    PurgedLeaf(&'a LeafIndex<T>),
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

impl<'a, T> From<&'a Index<T>> for IndexWC<'a, T> {
    fn from(value: &'a Index<T>) -> Self {
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
    fn to_index(self, cids: &mut VecDeque<Cid>) -> Index<T> {
        let cid = if !self.purged {
            cids.pop_front()
        } else {
            None
        };
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

pub fn serialize_compressed<T: Serialize + CompactSeq>(
    items: &[Index<T>],
    level: i32,
) -> Result<Vec<u8>> {
    let mut cids: Vec<&Cid> = Vec::new();
    let mut compressed: Vec<u8> = Vec::new();
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
    let ipld = serde_cbor::to_vec(&(cids, serde_cbor::Value::Bytes(compressed)))?;
    Ok(ipld)
}

pub fn deserialize_compressed<T: DeserializeOwned>(ipld: &[u8]) -> Result<Vec<Index<T>>> {
    let (mut cids, compressed): (VecDeque<Cid>, serde_cbor::Value) = serde_cbor::from_slice(ipld)?;
    if let serde_cbor::Value::Bytes(compressed) = compressed {
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
