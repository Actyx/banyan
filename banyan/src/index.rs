//! The index data structures for the tree
//!
//! In order to have good storage and query efficiency, indexes contain sequences of keys instead of on individual keys.
//! To support a key type in banyan trees, you need to define how a sequence of keys is stored in memory and on persistent storage,
//! by implementing [CompactSeq] for the key type, and define how keys are combined to compute summaries,
//! by implementing [Semigroup] for the key type.
//!
//! If you just want to quickly get things working, you can use [SimpleCompactSeq], which is just a vec.
//!
//! Indexes are structured in such a way that key data is stored closer to the root than value data.
//!
//! # Indexes
//!
//! Indexes contain an optional link to children/data, and some additonal information. An index
//! that no longer has a link to its children/data is called *purged*.
//!
//! There are two kinds of indexes.
//!
//! ## Leaf indexes
//!
//! Leaf indexes contain *the actual keys* for a leaf. This is not redundant data that can be recomputed from the values.
//! Leaf indexes have a level of `0`.
//!
//! ### Invariants
//!
//! A leaf index must contain exactly the same number of keys as there are values.
//!
//! ## Branch indexes
//!
//! Branch indices contain *summaries* for their children. This is redundant data that can be recomputed from the children.
//! Branch indexes have a level of `max(level of children) + 1`.
//!
//! ### Invariants
//!
//! For a sealed branch, the level of all its children is exactly `level-1`.
//! For an unsealed branch, the level of all children must be smaller than the level of the branch.
//!
//! [CompactSeq]: trait.CompactSeq.html
//! [Semigroup]: trait.Semigroup.html
//! [SimpleCompactSeq]: struct.SimpleCompactSeq.html
use super::zstd_array::ZstdArray;
use anyhow::{anyhow, Result};
use derive_more::From;
use libipld::{cbor::DagCborCodec, codec::Codec, Ipld};
use salsa20::{
    cipher::{NewStreamCipher, SyncStreamCipher},
    XSalsa20,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::BTreeMap, convert::From, sync::Arc};

/// An object that can compute a summary of type T of itself
pub trait Summarizable<T> {
    fn summarize(&self) -> T;
}

/// a compact representation of a sequence of 1 or more items
///
/// in general, this will have a different internal representation than just a bunch of values that is more compact and
/// makes it easier to query an entire sequence for matching indices.
pub trait CompactSeq: Serialize + DeserializeOwned {
    /// item type
    type Item;
    /// number of elements
    fn len(&self) -> usize;
    /// get nth element. Guaranteed to succeed with Some for index < count.
    fn get(&self, index: usize) -> Option<Self::Item>;
    /// first key
    fn first(&self) -> Self::Item {
        self.get(0).unwrap()
    }
    /// last key
    fn last(&self) -> Self::Item {
        self.get(self.len() - 1).unwrap()
    }
    /// utility function to get all items for a compactseq.
    fn to_vec(&self) -> Vec<Self::Item> {
        (0..self.len()).map(move |i| self.get(i).unwrap()).collect()
    }
    /// utility function to select some items for a compactseq.
    fn select(&self, bits: &[bool]) -> Vec<(u64, Self::Item)> {
        (0..self.len())
            .filter_map(move |i| {
                if bits[i] {
                    Some((i as u64, self.get(i).unwrap()))
                } else {
                    None
                }
            })
            .collect()
    }
    /// number of elements as an u64, for convenience
    fn count(&self) -> u64 {
        self.len() as u64
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// index for a leaf node, containing keys and some statistics data for its children
#[derive(Debug)]
pub struct LeafIndex<T: TreeTypes> {
    // block is sealed
    pub sealed: bool,
    // link to the block containing the values
    pub link: Option<T::Link>,
    /// A sequence of keys with the same number of values as the data block the link points to.
    pub keys: T::KeySeq,
    // serialized size of the data
    pub value_bytes: u64,
}

impl<T: TreeTypes> Clone for LeafIndex<T> {
    fn clone(&self) -> Self {
        Self {
            sealed: self.sealed,
            value_bytes: self.value_bytes,
            link: self.link,
            keys: self.keys.clone(),
        }
    }
}

impl<T: TreeTypes> LeafIndex<T> {
    pub fn keys(&self) -> impl Iterator<Item = T::Key> {
        self.keys.to_vec().into_iter()
    }
    pub fn select_keys(&self, bits: &[bool]) -> impl Iterator<Item = (u64, T::Key)> {
        self.keys.select(bits).into_iter()
    }
}

/// index for a branch node, containing summary data for its children
#[derive(Debug)]
pub struct BranchIndex<T: TreeTypes> {
    // number of events
    pub count: u64,
    // level of the tree node
    pub level: u32,
    // block is sealed
    pub sealed: bool,
    // link to the branch node
    pub link: Option<T::Link>,
    // extra data
    pub summaries: T::SummarySeq,
    // accumulated serialized size of all values in this tree
    pub value_bytes: u64,
    // accumulated serialized size of all keys and summaries in this tree
    pub key_bytes: u64,
}

impl<T: TreeTypes> Clone for BranchIndex<T> {
    fn clone(&self) -> Self {
        Self {
            count: self.count,
            level: self.level,
            sealed: self.sealed,
            value_bytes: self.value_bytes,
            key_bytes: self.key_bytes,
            link: self.link,
            summaries: self.summaries.clone(),
        }
    }
}

impl<T: TreeTypes> BranchIndex<T> {
    pub fn summaries(&self) -> impl Iterator<Item = T::Summary> + '_ {
        self.summaries.to_vec().into_iter()
    }
}

/// enum for a leaf or branch index
#[derive(Debug, From)]
pub enum Index<T: TreeTypes> {
    Leaf(LeafIndex<T>),
    Branch(BranchIndex<T>),
}

/// enum for a leaf or branch index
#[derive(Debug, From)]
pub enum IndexRef<'a, T: TreeTypes> {
    Leaf(&'a LeafIndex<T>),
    Branch(&'a BranchIndex<T>),
}

impl<T: TreeTypes> Clone for Index<T> {
    fn clone(&self) -> Self {
        match self {
            Index::Leaf(x) => Index::Leaf(x.clone()),
            Index::Branch(x) => Index::Branch(x.clone()),
        }
    }
}

impl<T: TreeTypes> Index<T> {
    pub fn as_index_ref(&self) -> IndexRef<T> {
        match self {
            Index::Leaf(x) => IndexRef::Leaf(x),
            Index::Branch(x) => IndexRef::Branch(x),
        }
    }

    pub fn summarize(&self) -> T::Summary {
        match self {
            Index::Leaf(x) => x.keys.summarize(),
            Index::Branch(x) => x.summaries.summarize(),
        }
    }

    pub fn link(&self) -> &Option<T::Link> {
        match self {
            Index::Leaf(x) => &x.link,
            Index::Branch(x) => &x.link,
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

#[derive(Debug)]
/// fully in memory representation of a branch node
///
/// This is a wrapper around a non-empty sequence of child indices.
pub struct Branch<T: TreeTypes> {
    // index data for the children
    pub children: Arc<[Index<T>]>,
}

impl<T: TreeTypes> Clone for Branch<T> {
    fn clone(&self) -> Self {
        Self {
            children: self.children.clone(),
        }
    }
}

impl<T: TreeTypes> Branch<T> {
    pub fn new(children: Vec<Index<T>>) -> Self {
        assert!(!children.is_empty());
        Self {
            children: children.into(),
        }
    }
    pub fn last_child(&self) -> &Index<T> {
        self.children
            .last()
            .expect("branch can never have 0 children")
    }

    pub fn first_child(&self) -> &Index<T> {
        self.children
            .first()
            .expect("branch can never have 0 children")
    }

    pub fn count(&self) -> u64 {
        self.children.len() as u64
    }
}

/// fully in memory representation of a leaf node
///
/// This is a wrapper around a cbor encoded and zstd compressed sequence of values
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

    pub fn child_at<T: DeserializeOwned>(&self, offset: u64) -> Result<T> {
        self.as_ref()
            .get(offset)?
            .ok_or_else(|| anyhow!("index out of bounds {}", offset))
    }
}

impl AsRef<ZstdArray> for Leaf {
    fn as_ref(&self) -> &ZstdArray {
        &self.0
    }
}

/// enum that combines index and corrsponding data
pub(crate) enum NodeInfo<'a, T: TreeTypes> {
    // Branch with index and data
    Branch(&'a BranchIndex<T>, Branch<T>),
    /// Leaf with index and data
    Leaf(&'a LeafIndex<T>, Leaf),
    /// Purged branch, with just the index
    PurgedBranch(&'a BranchIndex<T>),
    /// Purged leaf, with just the index
    PurgedLeaf(&'a LeafIndex<T>),
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum IndexWC<'a, KS, SS> {
    Branch {
        // block is sealed
        sealed: bool,
        // value bytes statistics
        value_bytes: u64,
        // number of events
        count: u64,
        // level of the tree node
        level: u32,
        // key bytes statistics
        key_bytes: u64,
        // summaries
        summaries: &'a SS,
    },
    Leaf {
        // block is sealed
        sealed: bool,
        // value bytes statistics
        value_bytes: u64,
        // keys
        keys: &'a KS,
    },
}

impl<'a, T: TreeTypes> From<&'a Index<T>> for IndexWC<'a, T::KeySeq, T::SummarySeq> {
    fn from(value: &'a Index<T>) -> Self {
        match value {
            Index::Branch(i) => Self::Branch {
                sealed: i.sealed,
                summaries: &i.summaries,
                value_bytes: i.value_bytes,
                count: i.count,
                level: i.level,
                key_bytes: i.key_bytes,
            },
            Index::Leaf(i) => Self::Leaf {
                sealed: i.sealed,
                keys: &i.keys,
                value_bytes: i.value_bytes,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum IndexRC<KS, SS> {
    Branch {
        sealed: bool,
        value_bytes: u64,
        summaries: SS,
        count: u64,
        level: u32,
        key_bytes: u64,
    },
    Leaf {
        sealed: bool,
        value_bytes: u64,
        keys: KS,
    },
}

impl<
        K: Eq + Debug + Send,
        KS: CompactSeq<Item = K> + Clone + Debug + FromIterator<K> + Send + Sync,
        S: Eq + Debug + Send,
        SS: CompactSeq<Item = S> + Clone + Debug + FromIterator<S> + Send + Sync,
    > IndexRC<KS, SS>
{
    fn into_index<T: TreeTypes<KeySeq = KS, Key = K, SummarySeq = SS, Summary = S>>(
        self,
        links: Option<&Ipld>,
    ) -> Index<T> {
        let link = if let Some(link) = links {
            let bytes = DagCborCodec.encode(link).unwrap();
            let link: T::Link = DagCborCodec.decode(&bytes).unwrap();
            Some(link)
        } else {
            None
        };
        match self {
            Self::Branch {
                sealed,
                value_bytes,
                summaries,
                count,
                level,
                key_bytes,
            } => BranchIndex {
                summaries,
                sealed,
                key_bytes,
                value_bytes,
                count,
                level,
                link,
            }
            .into(),
            Self::Leaf {
                sealed,
                value_bytes,
                keys,
            } => LeafIndex {
                keys,
                sealed,
                value_bytes,
                link,
            }
            .into(),
        }
    }
}

use crate::forest::TreeTypes;
use std::{
    fmt::Debug,
    io::{Cursor, Write},
    iter::FromIterator,
};

const CBOR_ARRAY_START: u8 = (4 << 5) | 31;
const CBOR_BREAK: u8 = 255;

pub(crate) fn serialize_compressed<T: TreeTypes>(
    key: &salsa20::Key,
    nonce: &salsa20::XNonce,
    items: &[Index<T>],
    level: i32,
) -> Result<Vec<u8>> {
    let mut links: Vec<(u64, Ipld)> = Vec::new();
    let mut compressed: Vec<u8> = Vec::new();
    compressed.extend_from_slice(&nonce);
    let mut writer = zstd::stream::write::Encoder::new(compressed.by_ref(), level)?;
    writer.write_all(&[CBOR_ARRAY_START])?;
    for (index, item) in items.iter().enumerate() {
        if let Some(link) = item.link() {
            // transcode to an IPLD AST
            let bytes = DagCborCodec.encode(link)?;
            let ipld = DagCborCodec.decode(&bytes)?;
            links.push((index as _, ipld));
        }
        serde_cbor::to_writer(writer.by_ref(), &IndexWC::from(item))?;
    }
    writer.write_all(&[CBOR_BREAK])?;
    writer.finish()?;
    salsa20::XSalsa20::new(key, nonce).apply_keystream(&mut compressed[24..]);
    Ok(serialize_branch(links, compressed)?)
}

pub(crate) fn deserialize_compressed<T: TreeTypes>(
    key: &salsa20::Key,
    ipld: &[u8],
) -> Result<Vec<Index<T>>> {
    let (links, mut compressed) = deserialize_branch(ipld)?;
    let links: BTreeMap<u64, Ipld> = links.into_iter().collect();
    if compressed.len() < 24 {
        return Err(anyhow!("nonce missing"));
    }
    let (nonce, compressed) = compressed.split_at_mut(24);
    XSalsa20::new(key, (&*nonce).into()).apply_keystream(compressed);
    let reader = zstd::stream::read::Decoder::new(Cursor::new(compressed))?;

    let data: Vec<IndexRC<T::KeySeq, T::SummarySeq>> = serde_cbor::from_reader(reader)?;
    let result = data
        .into_iter()
        .enumerate()
        .map(|(index, data)| data.into_index(links.get(&(index as _))))
        .collect::<Vec<_>>();
    Ok(result)
}

/// Utility method to zip a number of indices with an offset that is increased by each index value
pub(crate) fn zip_with_offset<'a, I: IntoIterator<Item = Index<T>> + 'a, T: TreeTypes + 'a>(
    value: I,
    offset: u64,
) -> impl Iterator<Item = (Index<T>, u64)> + 'a {
    value.into_iter().scan(offset, |offset, x| {
        let o0 = *offset;
        *offset += x.count();
        Some((x, o0))
    })
}

/// Utility method to zip a number of indices with an offset that is increased by each index value
pub(crate) fn zip_with_offset_ref<
    'a,
    I: IntoIterator<Item = &'a Index<T>> + 'a,
    T: TreeTypes + 'a,
>(
    value: I,
    offset: u64,
) -> impl Iterator<Item = (&'a Index<T>, u64)> + 'a {
    value.into_iter().scan(offset, |offset, x| {
        let o0 = *offset;
        *offset += x.count();
        Some((x, o0))
    })
}

#[derive(libipld::DagCbor)]
struct IpldNode(BTreeMap<u64, Ipld>, Box<[u8]>);

type LinksAndData = (Vec<(u64, libipld::Ipld)>, Vec<u8>);

fn serialize_branch(links: Vec<(u64, libipld::Ipld)>, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    let node = IpldNode(links.into_iter().collect(), data.into());
    let bytes = DagCborCodec.encode(&node)?;
    Ok(bytes)
}

fn deserialize_branch(data: &[u8]) -> anyhow::Result<LinksAndData> {
    let IpldNode(links, data) = DagCborCodec.decode(data)?;
    Ok((links.into_iter().collect(), data.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_format() {
        let bytes = serialize_branch(
            vec![(42, Ipld::Integer(255)), (43, Ipld::Integer(-367))],
            b"abcd".to_vec(),
        )
        .unwrap();
        assert_eq!(
            bytes,
            vec![
                0x82, // list 0x80 of length 2
                0xa2, // map 0xa0 of length 2
                0x18, // u8
                42,   // value
                0x18, // u8
                255,  // value
                0x18, // u8
                43,   // value
                0x39, // i16
                1,    // -1 - (256 + 110) = -367
                110,  // second byte of i16
                0x44, // bytes 0x40 of length 4
                97,   // a
                98,   // b
                99,   // c
                100,  // d
            ]
        );
    }
}
