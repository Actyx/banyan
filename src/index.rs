//! The index data structures for the tree
use super::ipfs::Cid;
use super::zstd_array::ZstdArrayBuilder;
use anyhow::{anyhow, Result};
use derive_more::From;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};
use std::convert::From;

/// trait for items that can be combined in an associative way
///
/// Currently all examples I can think of are also commutative, so it would be an abelian semigroup.
/// But I am not sure we need to require that as of now.
/// https://mathworld.wolfram.com/AbelianSemigroup.html
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
}

/// utility function to get all items for a compactseq.
///
/// This can not be a trait method because it returns an unnameable type, and therefore requires impl Trait,
/// which is not available within traits.
pub fn compactseq_items<'a, T: CompactSeq>(value: &'a T) -> impl Iterator<Item = T::Item> + 'a {
    (0..value.count()).map(move |i| value.get(i).unwrap())
}

/// utility function to select some items for a compactseq.
///
/// This can not be a trait method because it returns an unnameable type, and therefore requires impl Trait,
/// which is not available within traits.
pub fn compactseq_select_items<'a, T: CompactSeq>(
    value: &'a T,
    it: impl Iterator<Item = bool> + 'a,
) -> impl Iterator<Item = (u64, T::Item)> + 'a {
    (0..value.count()).zip(it).filter_map(move |(i, take)| {
        if take {
            Some((i, value.get(i).unwrap()))
        } else {
            None
        }
    })
}

/// A trivial implementation of a CompactSeq as just a Seq.
///
/// This is useful mostly as a reference impl and for testing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleCompactSeq<T>(Vec<T>);

impl<T: Serialize + DeserializeOwned + Semigroup + Clone> CompactSeq for SimpleCompactSeq<T> {
    type Item = T;
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
    pub cid: Cid,
    //
    pub data: T,
}

impl<T: CompactSeq> LeafIndex<T> {
    pub fn items<'a>(&'a self) -> impl Iterator<Item = T::Item> + 'a {
        compactseq_items(&self.data)
    }
    pub fn select<'a>(
        &'a self,
        it: impl Iterator<Item = bool> + 'a,
    ) -> impl Iterator<Item = (u64, T::Item)> + 'a {
        compactseq_select_items(&self.data, it)
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
    pub cid: Cid,
    // extra data
    pub data: T,
}

impl<T: CompactSeq> BranchIndex<T> {
    pub fn items<'a>(&'a self) -> impl Iterator<Item = T::Item> + 'a {
        compactseq_items(&self.data)
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
    cid: &'a Cid,
    // extra data
    data: &'a T,
}

impl<'a, T> From<&'a Index<T>> for IndexW<'a, T> {
    fn from(value: &'a Index<T>) -> Self {
        match value {
            Index::Branch(i) => Self {
                count: Some(i.count),
                sealed: i.sealed,
                level: Some(i.level),
                cid: &i.cid,
                data: &i.data,
            },
            Index::Leaf(i) => Self {
                count: None,
                sealed: i.sealed,
                level: None,
                cid: &i.cid,
                data: &i.data,
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
    // block is sealed
    sealed: bool,
    // link to the branch node
    cid: Cid,
    // extra data
    data: T,
}

impl<T> From<IndexR<T>> for Index<T> {
    fn from(v: IndexR<T>) -> Self {
        if let (Some(level), Some(count)) = (v.level, v.count) {
            BranchIndex {
                cid: v.cid,
                data: v.data,
                sealed: v.sealed,
                count,
                level,
            }
            .into()
        } else {
            LeafIndex {
                cid: v.cid,
                data: v.data,
                sealed: v.sealed,
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
            Index::Leaf(x) => &x.data,
            Index::Branch(x) => &x.data,
        }
    }

    pub fn cid(&self) -> &Cid {
        match self {
            Index::Leaf(x) => &x.cid,
            Index::Branch(x) => &x.cid,
        }
    }
    pub fn count(&self) -> u64 {
        match self {
            Index::Leaf(x) => x.data.count(),
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
pub struct Leaf {
    pub items: ZstdArrayBuilder,
}

impl Leaf {
    pub fn new(items: ZstdArrayBuilder) -> Self {
        Self { items }
    }
}

pub enum NodeInfo<'a, T> {
    Branch(&'a BranchIndex<T>, Branch<T>),
    Leaf(&'a LeafIndex<T>, Leaf),
}

impl Leaf {
    pub fn child_at<T: DeserializeOwned>(&self, offset: u64) -> Result<T> {
        self.items
            .as_ref()
            .select((0..=offset).map(|x| x == offset))?
            .into_iter()
            .last()
            .ok_or_else(|| anyhow!("index out of bounds {}", offset).into())
    }
}
