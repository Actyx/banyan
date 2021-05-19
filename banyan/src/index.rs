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
use crate::{
    forest::TreeTypes,
    store::{ReadOnlyStore, ZstdDagCborSeq},
    CipherOffset, Forest, Secrets,
};
use anyhow::{anyhow, Result};
use derive_more::From;
use libipld::{
    cbor::{DagCbor, DagCborCodec},
    codec::{Decode, Encode},
    DagCbor,
};
use std::{
    convert::TryFrom,
    fmt::{self, Debug, Display},
    io,
    iter::FromIterator,
    ops::Range,
    sync::Arc,
};

/// An object that can compute a summary of type T of itself
pub trait Summarizable<T> {
    fn summarize(&self) -> T;
}

/// a compact representation of a sequence of 1 or more items
///
/// in general, this will have a different internal representation than just a bunch of values that is more compact and
/// makes it easier to query an entire sequence for matching indices.
pub trait CompactSeq: DagCbor {
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
    fn select(&self, bits: &[bool]) -> Vec<Self::Item> {
        (0..self.len())
            .filter_map(move |i| if bits[i] { self.get(i) } else { None })
            .collect()
    }
    /// number of elements as an u64, for convenience
    fn count(&self) -> u64 {
        self.len() as u64
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Provide a size estimation, which is used to calculate the overall size of
    /// the node for caching purposes. This function should be overridden, if the
    /// used item type contains any heap allocated objects, otherwise the default
    /// implementation is a rough estimate.
    fn estimated_size(&self) -> usize {
        self.len() * std::mem::size_of::<Self::Item>()
    }
}

/// index for a leaf node, containing keys and some statistics data for its children
#[derive(Debug, DagCbor)]
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
    pub fn select_keys(&self, bits: &[bool]) -> impl Iterator<Item = T::Key> {
        self.keys.select(bits).into_iter()
    }
}

/// index for a branch node, containing summary data for its children
#[derive(Debug, DagCbor)]
pub struct BranchIndex<T: TreeTypes> {
    // number of items
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
#[derive(Debug, DagCbor)]
#[ipld(repr = "kinded")]
pub enum Index<T: TreeTypes> {
    #[ipld(repr = "value")]
    Leaf(Arc<LeafIndex<T>>),
    #[ipld(repr = "value")]
    Branch(Arc<BranchIndex<T>>),
}

impl<T: TreeTypes> From<LeafIndex<T>> for Index<T> {
    fn from(value: LeafIndex<T>) -> Self {
        Index::Leaf(Arc::new(value))
    }
}

impl<T: TreeTypes> From<BranchIndex<T>> for Index<T> {
    fn from(value: BranchIndex<T>) -> Self {
        Index::Branch(Arc::new(value))
    }
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

#[derive(Debug, Clone)]
/// fully in memory representation of a branch node
///
/// This is a wrapper around a non-empty sequence of child indices.
pub struct Branch<T: TreeTypes> {
    // index data for the children
    pub children: Arc<[Index<T>]>,
    // byte range of this branch
    pub byte_range: Range<u64>,
}

impl<T: TreeTypes> Branch<T> {
    pub fn new(children: Vec<Index<T>>, byte_range: Range<u64>) -> Self {
        assert!(!children.is_empty());
        Self {
            children: children.into(),
            byte_range,
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
pub struct Leaf {
    pub items: ZstdDagCborSeq,
    pub byte_range: Range<u64>,
}

impl Leaf {
    pub fn new(items: ZstdDagCborSeq, byte_range: Range<u64>) -> Self {
        Self { items, byte_range }
    }

    pub fn child_at<T: DagCbor>(&self, offset: u64) -> Result<T> {
        self.as_ref()
            .get(offset)?
            .ok_or_else(|| anyhow!("index out of bounds {}", offset))
    }
}

impl AsRef<ZstdDagCborSeq> for Leaf {
    fn as_ref(&self) -> &ZstdDagCborSeq {
        &self.items
    }
}

#[derive(Debug)]
pub struct BranchLoader<T: TreeTypes, V, R> {
    forest: Forest<T, V, R>,
    secrets: Secrets,
    index: Arc<BranchIndex<T>>,
    branch: Option<Branch<T>>,
}

impl<
        T: TreeTypes,
        V: Debug + Send + Sync + Clone + DagCbor + 'static,
        R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
    > BranchLoader<T, V, R>
{
    pub fn new(forest: &Forest<T, V, R>, secrets: &Secrets, index: Arc<BranchIndex<T>>) -> Self {
        Self {
            forest: forest.clone(),
            secrets: secrets.clone(),
            index,
            branch: None,
        }
    }

    pub fn load(&mut self) -> anyhow::Result<&Branch<T>> {
        Ok({
            if self.branch.is_none() {
                if let Some(branch) = self.forest.load_branch_cached(&self.secrets, &self.index)? {
                    self.branch = Some(branch)
                } else {
                    anyhow::bail!("unable to load branch")
                }
            }
            self.branch.as_ref().unwrap()
        })
    }
}

#[derive(Debug)]
pub struct LeafLoader<T: TreeTypes, V, R> {
    forest: Forest<T, V, R>,
    secrets: Secrets,
    index: Arc<LeafIndex<T>>,
    leaf: Option<Leaf>,
}

impl<
        T: TreeTypes,
        V: Debug + Send + Sync + Clone + DagCbor + 'static,
        R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
    > LeafLoader<T, V, R>
{
    pub fn new(forest: &Forest<T, V, R>, secrets: &Secrets, index: Arc<LeafIndex<T>>) -> Self {
        Self {
            forest: forest.clone(),
            secrets: secrets.clone(),
            index,
            leaf: None,
        }
    }

    pub fn load(&mut self) -> anyhow::Result<&Leaf> {
        Ok({
            if self.leaf.is_none() {
                if let Some(leaf) = self.forest.load_leaf(&self.secrets, &self.index)? {
                    self.leaf = Some(leaf)
                } else {
                    anyhow::bail!("unable to load leaf")
                }
            }
            self.leaf.as_ref().unwrap()
        })
    }
}

#[derive(Debug)]
pub enum NodeInfo<T: TreeTypes, V, R> {
    Branch(Arc<BranchIndex<T>>, BranchLoader<T, V, R>),
    Leaf(Arc<LeafIndex<T>>, LeafLoader<T, V, R>),
    PurgedBranch(Arc<BranchIndex<T>>),
    PurgedLeaf(Arc<LeafIndex<T>>),
}

impl<T: TreeTypes, V, R> Display for NodeInfo<T, V, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leaf(index, _) => {
                write!(
                    f,
                    "Leaf(count={}, value_bytes={}, sealed={}, link={})",
                    index.keys.count(),
                    index.value_bytes,
                    index.sealed,
                    index
                        .link
                        .map(|x| format!("{}", x))
                        .unwrap_or_else(|| "".to_string())
                )
            }

            Self::Branch(index, _) => {
                write!(
                    f,
                    "Branch(count={}, key_bytes={}, value_bytes={}, sealed={}, link={}, children={})",
                    index.count,
                    index.key_bytes,
                    index.value_bytes,
                    index.sealed,
                    index
                        .link
                        .map(|x| format!("{}", x))
                        .unwrap_or_else(|| "".to_string()),
                    index.summaries.len()
                )
            }
            Self::PurgedBranch(index) => {
                write!(
                    f,
                    "PurgedBranch(count={}, key_bytes={}, value_bytes={}, sealed={})",
                    index.count, index.key_bytes, index.value_bytes, index.sealed,
                )
            }
            Self::PurgedLeaf(index) => {
                write!(
                    f,
                    "PurgedLeaf(count={}, key_bytes={}, sealed={})",
                    index.keys.count(),
                    index.value_bytes,
                    index.sealed,
                )
            }
        }
    }
}

pub(crate) fn serialize_compressed<T: TreeTypes>(
    key: &chacha20::Key,
    state: &mut CipherOffset,
    items: &[Index<T>],
    level: i32,
) -> Result<Vec<u8>> {
    let zs = ZstdDagCborSeq::from_iter(items, level)?;
    zs.into_encrypted(key, state)
}

pub(crate) fn deserialize_compressed<T: TreeTypes>(
    key: &chacha20::Key,
    ipld: &[u8],
) -> Result<(Vec<Index<T>>, Range<u64>)> {
    let (seq, byte_range) = ZstdDagCborSeq::decrypt(ipld, key)?;
    let seq = seq.items::<Index<T>>()?;
    Ok((seq, byte_range))
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

/// Every CompactSeq can be summarized to unit
impl<T: CompactSeq> Summarizable<()> for T {
    fn summarize(&self) {}
}

/// A sequence of unit values, in case you want to create a tree without a summary
#[derive(Debug, Clone)]
pub struct UnitSeq(usize);

impl Encode<DagCborCodec> for UnitSeq {
    fn encode<W: io::Write>(&self, c: DagCborCodec, w: &mut W) -> anyhow::Result<()> {
        (self.0 as u64).encode(c, w)
    }
}

impl Decode<DagCborCodec> for UnitSeq {
    fn decode<R: io::Read + io::Seek>(c: DagCborCodec, r: &mut R) -> anyhow::Result<Self> {
        let t = u64::decode(c, r)?;
        Ok(Self(usize::try_from(t)?))
    }
}

impl CompactSeq for UnitSeq {
    type Item = ();
    fn get(&self, index: usize) -> Option<()> {
        if index < self.0 {
            Some(())
        } else {
            None
        }
    }
    fn len(&self) -> usize {
        self.0 as usize
    }
}

impl FromIterator<()> for UnitSeq {
    fn from_iter<T: IntoIterator<Item = ()>>(iter: T) -> Self {
        Self(iter.into_iter().count())
    }
}

/// A trivial implementation of a CompactSeq as just a Seq.
///
/// This is useful mostly as a reference impl and for testing.
#[derive(Debug, Clone, DagCbor)]
pub struct VecSeq<T: DagCbor>(Vec<T>);

impl<T: DagCbor + Clone> CompactSeq for VecSeq<T> {
    type Item = T;
    fn get(&self, index: usize) -> Option<T> {
        self.0.get(index).cloned()
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn estimated_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.0.capacity() * std::mem::size_of::<T>()
    }
}

impl<T: DagCbor> FromIterator<T> for VecSeq<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}
