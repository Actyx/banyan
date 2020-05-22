use super::index::*;
use crate::ipfs::Cid;
use crate::store::ArcStore;
use crate::zstd_array::*;
use anyhow::{anyhow, Result};
use future::LocalBoxFuture;
use futures::{prelude::*, stream::LocalBoxStream};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::{
    fmt,
    marker::PhantomData,
    sync::{Arc, RwLock},
};
use tracing::{debug, info, trace};

/// Trees can be parametrized with the key type and the sequence type
///
/// There might be more types in the future, so all of them are grouped in this trait.
pub trait TreeTypes {
    /// key type. This also doubles as the type for a combination (union) of keys
    type Key: Semigroup + Debug;
    /// compact sequence type to be used for indices
    type Seq: CompactSeq<Item = Self::Key> + Serialize + DeserializeOwned + Clone + Debug;
}

/// A number of trees that are grouped together, sharing common caches and settings
///
/// They not necessarily share the same values. Trees with different value types can be grouped together.
pub struct Forest<T: TreeTypes> {
    store: ArcStore,
    config: Config,
    branch_cache: RwLock<lru::LruCache<Cid, Branch<T::Seq>>>,
    leaf_cache: RwLock<lru::LruCache<Cid, Leaf>>,
    _tt: PhantomData<T>,
}

/// A tree
pub struct Tree<T: TreeTypes, V> {
    root: Option<Index<T::Seq>>,
    forest: Arc<Forest<T>>,
    _t: PhantomData<V>,
}

/// A query
///
/// Queries work on value sequences instead of individual values for efficiency
pub trait Query<T: TreeTypes> {
    /// the iterator type
    type IndexIterator: Iterator<Item = bool>;
    /// checks whether a single item that could be a combination of multiple values can possibly match the query
    fn intersects(&self, x: &T::Key) -> bool;
    /// checks wether an individual item does match the query
    fn contains(&self, x: &T::Key) -> bool;
    /// an iterator returning x.count() elements, where each value is a bool indicating if the query does match
    fn containing(&self, offset: u64, x: &LeafIndex<T::Seq>) -> Self::IndexIterator;
    /// an iterator returning x.count() elements, where each value is a bool indicating if the query can match
    fn intersecting(&self, offset: u64, x: &BranchIndex<T::Seq>) -> Self::IndexIterator;
}

/// Configuration for a forest. Includes settings for when a node is considered full
pub struct Config {
    max_leaf_size: u64,
    max_leaf_count: u64,
    max_branch_size: u64,
    max_branch_count: u64,
    zstd_level: i32,
}

impl Config {
    pub fn debug() -> Self {
        Self {
            max_leaf_size: 10000,
            max_leaf_count: 10,
            max_branch_size: 1000,
            max_branch_count: 4,
            zstd_level: 10,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_leaf_size: 1 << 12,
            max_leaf_count: 1 << 12,
            max_branch_size: 1 << 16,
            max_branch_count: 32,
            zstd_level: 10,
        }
    }
}

impl<T: TreeTypes, V> fmt::Debug for Tree<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => write!(f, "Tree {}", root.cid()),
            None => write!(f, "empty tree"),
        }
    }
}

impl<V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static, T: TreeTypes>
    Tree<T, V>
{
    pub fn empty(forest: Arc<Forest<T>>) -> Self {
        Self {
            root: None,
            forest,
            _t: PhantomData,
        }
    }

    /// dumps the tree structure
    pub async fn dump(&self) -> Result<()> {
        match &self.root {
            Some(index) => self.forest.dump(index, "").await,
            None => Ok(()),
        }
    }

    /// append a single element
    pub async fn push(&mut self, key: &T::Key, value: &V) -> Result<()> {
        self.root = Some(match &self.root {
            Some(index) => {
                if !index.sealed() {
                    self.forest.push(index, key, &value).await?
                } else {
                    let index = self.forest.single_branch(index.clone()).await?.into();
                    self.forest.push(&index, key, &value).await?
                }
            }
            None => self.forest.single_leaf(key, &value).await?.into(),
        });
        Ok(())
    }

    /// element at index
    pub async fn get(&self, offset: u64) -> Result<Option<(T::Key, V)>> {
        Ok(match &self.root {
            Some(index) => Some(self.forest.get(index, offset).await?),
            None => None,
        })
    }

    /// stream the entire content of the tree
    pub fn stream<'a>(&'a self) -> impl Stream<Item = Result<(T::Key, V)>> + 'a {
        match &self.root {
            Some(index) => self.forest.stream(index.clone()).left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    /// stream the tree, filtered by a query
    pub fn stream_filtered<'a, Q: Query<T>>(
        &'a self,
        query: &'a Q,
    ) -> impl Stream<Item = Result<(u64, T::Key, V)>> + 'a {
        match &self.root {
            Some(index) => self
                .forest
                .stream_filtered(0, query, index.clone())
                .left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    /// true for an empty tree
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// number of elements in the tree
    pub fn count(&self) -> u64 {
        self.root.as_ref().map(|x| x.count()).unwrap_or(0)
    }
}

/// Utility method to zip a number of indices with an offset that is increased by each index value
fn zip_with_offset<'a, I: Iterator<Item = Index<T>> + 'a, T: CompactSeq + 'a>(
    value: I,
    offset: u64,
) -> impl Iterator<Item = (Index<T>, u64)> + 'a {
    value.scan(offset, |offset, x| {
        *offset += x.count();
        Some((x, *offset))
    })
}

/// basic random access append only async tree
impl<T> Forest<T>
where
    T: TreeTypes,
{
    /// predicate to determine if a leaf is sealed, based on the config
    fn leaf_sealed(&self, bytes: u64, count: u64) -> bool {
        bytes >= self.config.max_leaf_size || count >= self.config.max_leaf_count
    }

    /// load a leaf given a leaf index
    async fn load_leaf(&self, index: &LeafIndex<T::Seq>) -> Result<Leaf> {
        let bytes = self.store.get(&index.cid).await?;
        let items = ZstdArrayBuilder::init(bytes.as_ref(), self.config.zstd_level)?;
        Ok(Leaf::new(items))
    }

    /// load a leaf from a cache
    async fn load_leaf_cached(&self, index: &LeafIndex<T::Seq>) -> Result<Leaf> {
        match self.leaf_cache.write().unwrap().pop(&index.cid) {
            Some(leaf) => Ok(leaf),
            None => self.load_leaf(index).await,
        }
    }

    /// Creates a tree containing a single item, and returns the index of that tree
    async fn single_leaf<V: Serialize + Debug>(
        &self,
        key: &T::Key,
        value: &V,
    ) -> Result<LeafIndex<T::Seq>> {
        let items = ZstdArrayBuilder::new(self.config.zstd_level)?.push(value)?;
        let cid = self.store.put(items.as_ref().raw()).await?;
        let index = LeafIndex {
            cid,
            sealed: self.leaf_sealed(items.as_ref().raw().len() as u64, 1),
            data: T::Seq::single(key),
        };
        Ok(index)
    }

    /// predicate to determine if a leaf is sealed, based on the config
    fn branch_sealed(&self, bytes: u64, items: &[Index<T::Seq>], level: u32) -> bool {
        // a branch with less than 2 children is never considered sealed.
        // if we ever get this a branch that is too large despite having just 1 child,
        // we should just panic.
        if let Some(last_child) = items.last() {
            items.len() > 1
                && (last_child.sealed() && last_child.level() >= level - 1)
                && ((bytes >= self.config.max_branch_size)
                    || items.len() as u64 >= self.config.max_branch_count)
        } else {
            false
        }
    }

    /// load a branch given a branch index
    async fn load_branch(&self, index: &BranchIndex<T::Seq>) -> Result<Branch<T::Seq>> {
        let bytes = self.store.get(&index.cid).await?;
        let children: Vec<_> = serde_cbor::from_slice(&bytes)?;
        // let children = CborZstdArrayRef::new(bytes.as_ref()).items()?;
        Ok(Branch::<T::Seq>::new(children))
    }

    /// load a branch given a branch index, from the cache
    async fn load_branch_cached(&self, index: &BranchIndex<T::Seq>) -> Result<Branch<T::Seq>> {
        match self.branch_cache.write().unwrap().pop(&index.cid) {
            Some(branch) => Ok(branch),
            None => self.load_branch(index).await,
        }
    }

    /// creates a branch containing a single item
    async fn single_branch(&self, value: Index<T::Seq>) -> Result<BranchIndex<T::Seq>> {
        let cbor = serde_cbor::to_vec(&vec![&value])?;
        let cid = self.store.put(&cbor).await?;
        let index = BranchIndex {
            level: value.level() + 1,
            count: value.count(),
            sealed: false,
            cid,
            data: T::Seq::single(&value.data().summarize()),
        };
        Ok(index)
    }

    /// load a node, returning a structure containing the index and value for convenient matching
    async fn load_node<'a>(&self, index: &'a Index<T::Seq>) -> Result<NodeInfo<'a, T::Seq>> {
        Ok(match index {
            Index::Branch(index) => NodeInfo::Branch(index, self.load_branch_cached(index).await?),
            Index::Leaf(index) => NodeInfo::Leaf(index, self.load_leaf_cached(index).await?),
        })
    }

    /// store a leaf in the cache, and return it wrapped into a generic index
    fn cache_leaf(&self, index: LeafIndex<T::Seq>, node: Leaf) -> Index<T::Seq> {
        self.leaf_cache
            .write()
            .unwrap()
            .put(index.cid.clone(), node);
        index.into()
    }

    /// store a branch in the cache, and return it wrapped into a generic index
    fn cache_branch(&self, index: BranchIndex<T::Seq>, node: Branch<T::Seq>) -> Index<T::Seq> {
        self.branch_cache
            .write()
            .unwrap()
            .put(index.cid.clone(), node);
        index.into()
    }

    /// Push a single item to the end of the tree
    ///
    /// note: you could make this more efficient by passing in a mut &Index, but then
    /// you would have to be really careful to do all modification after anything that
    /// can fail, otherwise you might end up with an inconsistent state.
    ///
    /// The functional way of threading the index through the call and returning it is
    /// easier to get correct.
    async fn push<V: Serialize + Debug>(
        &self,
        index: &Index<T::Seq>,
        key: &T::Key,
        value: &V,
    ) -> Result<Index<T::Seq>> {
        // calling push0 for a sealed node makes no sense and should not happen!
        assert!(!index.sealed());
        match self.load_node(index).await? {
            NodeInfo::Leaf(index, mut leaf) => {
                leaf.items = leaf.items.push(&value)?;
                let mut index = index.clone();
                // update the index data
                index.data.push(key);
                index.sealed = self.leaf_sealed(leaf.items.raw().len() as u64, index.data.count());
                index.cid = self.store.put(leaf.items.raw()).await?;
                Ok(self.cache_leaf(index, leaf))
            }
            NodeInfo::Branch(index, mut branch) => {
                let child_index = branch.last_child();
                let mut index = index.clone();
                if !child_index.sealed() {
                    // there is room in the child. Just push it down and update us
                    index.data.extend(&key);
                    *branch.last_child_mut() = self.pushr(child_index, key, value).await?;
                } else if child_index.level() < index.level - 1 {
                    // there is room for another tree node. Create a new one and push down to it
                    index.data.extend(&key);
                    let child_index = self.single_branch(child_index.clone()).await?;
                    *branch.last_child_mut() = self.pushr(&child_index.into(), key, value).await?;
                } else {
                    // all our children are full, we need to append
                    let child_index = self.single_leaf(key, &value).await?;
                    // add new index element with child summary
                    index.data.push(&child_index.data.summarize());
                    // add actual new child
                    branch.children.push(child_index.into());
                }
                let cbor = serde_cbor::to_vec(&branch.children)?;
                let cid = self.store.put(&cbor).await?;
                index.count += 1;
                index.sealed = self.branch_sealed(cbor.len() as u64, &branch.children, index.level);
                index.cid = cid;
                Ok(self.cache_branch(index, branch))
            }
        }
    }

    /// helper to avoid compiler error when doing recursive call from async fn
    ///
    /// If you call push0().boxed_local() directly in push0, you get this error:
    /// ```
    /// recursion in an `async fn` requires boxing a recursive `async fn` must be rewritten to return a boxed `dyn Future`
    /// ```
    ///
    /// in any case, it is nice to have everything explicitly spelled out.
    fn pushr<'a, V: Serialize + Debug>(
        &'a self,
        node: &'a Index<T::Seq>,
        key: &'a T::Key,
        value: &'a V,
    ) -> LocalBoxFuture<'a, Result<Index<T::Seq>>> {
        self.push(node, key, value).boxed_local()
    }

    async fn get<V: DeserializeOwned>(
        &self,
        index: &Index<T::Seq>,
        mut offset: u64,
    ) -> Result<(T::Key, V)> {
        assert!(offset < index.count());
        match self.load_node(index).await? {
            NodeInfo::Branch(_, node) => {
                for child in node.children.iter() {
                    if offset < child.count() {
                        return self.getr(child, offset).await;
                    } else {
                        offset -= child.count();
                    }
                }
                Err(anyhow!("index out of bounds").into())
            }
            NodeInfo::Leaf(index, node) => {
                let v = node.child_at::<V>(offset)?;
                let k = index.data.get(offset).unwrap();
                Ok((k, v))
            }
        }
    }

    fn getr<'a, V: DeserializeOwned + 'a>(
        &'a self,
        node: &'a Index<T::Seq>,
        offset: u64,
    ) -> LocalBoxFuture<'a, Result<(T::Key, V)>> {
        self.get(node, offset).boxed_local()
    }

    fn stream<'a, V: DeserializeOwned>(
        &'a self,
        index: Index<T::Seq>,
    ) -> LocalBoxStream<'a, Result<(T::Key, V)>> {
        let s = async move {
            Ok(match self.load_node(&index).await? {
                NodeInfo::Leaf(index, node) => {
                    let keys = index.items();
                    let elems: Vec<V> = node.items.items()?;
                    let pairs = keys.zip(elems).collect::<Vec<_>>();
                    stream::iter(pairs).map(|x| Ok(x)).left_stream()
                }
                NodeInfo::Branch(_, node) => stream::iter(node.children)
                    .map(move |child| self.stream(child))
                    .flatten()
                    .right_stream(),
            })
        }
        .try_flatten_stream();
        Box::pin(s)
    }

    fn stream_filtered<'a, V: DeserializeOwned, Q: Query<T>>(
        &'a self,
        offset: u64,
        query: &'a Q,
        index: Index<T::Seq>,
    ) -> LocalBoxStream<'a, Result<(u64, T::Key, V)>> {
        let s = async move {
            Ok(match self.load_node(&index).await? {
                NodeInfo::Leaf(index, node) => {
                    let matching = query.containing(offset, index).collect::<Vec<_>>();
                    let keys = index.select(matching.iter().cloned());
                    let elems: Vec<V> = node.items.as_ref().select(matching.iter().cloned())?;
                    let pairs = keys
                        .zip(elems)
                        .map(|((o, k), v)| (o + offset, k, v))
                        .collect::<Vec<_>>();
                    stream::iter(pairs).map(|x| Ok(x)).left_stream()
                }
                NodeInfo::Branch(index, node) => {
                    let matching = query.intersecting(offset, index);
                    let offsets = zip_with_offset(node.children.into_iter(), offset);
                    let children = matching
                        .zip(offsets)
                        .filter_map(|(m, c)| if m { Some(c) } else { None });
                    stream::iter(children)
                        .map(move |(child, offset)| self.stream_filtered(offset, query, child))
                        .flatten()
                        .right_stream()
                }
            })
        }
        .try_flatten_stream();
        Box::pin(s)
    }

    fn dumpr<'a>(
        &'a self,
        index: &'a Index<T::Seq>,
        prefix: &'a str,
    ) -> LocalBoxFuture<'a, Result<()>> {
        self.dump(index, prefix).boxed_local()
    }

    async fn dump(&self, index: &Index<T::Seq>, prefix: &str) -> Result<()> {
        match self.load_node(index).await? {
            NodeInfo::Leaf(index, _) => {
                println!("{}Leaf({}, {})", prefix, index.data.count(), index.sealed);
            }
            NodeInfo::Branch(index, branch) => {
                println!(
                    "{}Branch({}, {} {:?})",
                    prefix, index.count, index.sealed, index.data
                );
                let prefix = prefix.to_string() + "  ";
                for x in branch.children.iter() {
                    self.dumpr(x, &prefix).await?;
                }
            }
        };
        Ok(())
    }

    /// creates a new forest
    pub fn new(store: ArcStore) -> Self {
        let branch_cache = RwLock::new(lru::LruCache::<Cid, Branch<T::Seq>>::new(1000));
        let leaf_cache = RwLock::new(lru::LruCache::<Cid, Leaf>::new(1000));
        Self {
            store,
            config: Config::debug(),
            branch_cache,
            leaf_cache,
            _tt: PhantomData,
        }
    }
}
