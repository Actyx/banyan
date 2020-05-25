use super::index::*;
use crate::ipfs::Cid;
use crate::{store::ArcStore, zstd_array::ZstdArrayBuilder};
use anyhow::{anyhow, Result};
use future::LocalBoxFuture;
use futures::{prelude::*, stream::LocalBoxStream};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::{
    fmt,
    marker::PhantomData,
    sync::{Arc, RwLock}, iter::FromIterator,
};
use tracing::*;

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

/// A tree. This is mostly an user friendly handle.
///
/// Most of the logic except for handling the empty case is implemented in the forest
pub struct Tree<T: TreeTypes, V> {
    root: Option<Index<T::Seq>>,
    forest: Arc<Forest<T>>,
    _t: PhantomData<V>,
}

/// A query
///
/// Queries work on value sequences instead of individual values for efficiency. Methods that work on individual
/// values are just provided for consistency checks.
pub trait Query<T: TreeTypes> {
    /// the iterator type
    type IndexIterator: Iterator<Item = bool>;
    /// an iterator returning x.count() elements, where each value is a bool indicating if the query does match
    fn containing(&self, offset: u64, x: &LeafIndex<T::Seq>) -> Self::IndexIterator;
    /// an iterator returning x.count() elements, where each value is a bool indicating if the query can match
    fn intersecting(&self, offset: u64, x: &BranchIndex<T::Seq>) -> Self::IndexIterator;
}

/// Configuration for a forest. Includes settings for when a node is considered full
pub struct Config {
    max_leaf_size: u64,
    max_leaf_count: u64,
    max_branch_count: u64,
    zstd_level: i32,
}

impl Config {
    pub fn debug() -> Self {
        Self {
            max_leaf_size: 10000,
            max_leaf_count: 10,
            max_branch_count: 4,
            zstd_level: 10,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_leaf_size: 1 << 14,
            max_leaf_count: 1 << 14,
            max_branch_count: 32,
            zstd_level: 10,
        }
    }
}

impl<T: TreeTypes, V> fmt::Debug for Tree<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => write!(
                f,
                "Tree {} {} {}",
                root.cid(),
                root.key_bytes(),
                root.value_bytes()
            ),
            None => write!(f, "empty tree"),
        }
    }
}

impl<V, T: TreeTypes> Clone for Tree<T, V> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            forest: self.forest.clone(),
            _t: PhantomData,
        }
    }
}

impl<V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static, T: TreeTypes>
    Tree<T, V>
{
    pub async fn new(cid: Cid, forest: Arc<Forest<T>>) -> Result<Self> {
        Ok(Self {
            root: Some(forest.load_branch_from_cid(cid).await?),
            forest,
            _t: PhantomData,
        })
    }
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

    fn with_root(&self, root: Option<Index<T::Seq>>) -> Self {
        Tree {
            root,
            forest: self.forest.clone(),
            _t: PhantomData,
        }
    }

    /// returns the part of the tree that is considered filled, basically everything up to the first gap
    ///
    /// a gap is a non-full leaf or a "level hole"
    pub async fn filled(&self) -> Result<Self> {
        Ok(match &self.root {
            Some(index) => self.with_root(self.forest.filled(index).await?),
            None => self.with_root(None),
        })
    }

    pub async fn collect_from<B: FromIterator<(T::Key, V)>>(&self, offset: u64) -> Result<B> {
        let query = OffsetQuery::new(offset);
        let items: Vec<Result<(T::Key, V)>> = self.stream_filtered(&query)
            .map_ok(|(_, k, v)| (k, v))
            .collect::<Vec<_>>().await;
        items.into_iter().collect::<Result<_>>()
    }

    pub async fn balance(&self) -> Result<Self> {
        let mut filled = self.filled().await?;
        let remainder: Vec<_> = self.collect_from(filled.count()).await?;
        filled.extend(remainder).await?;
        Ok(filled)
    }

    /// append a single element
    pub async fn push(&mut self, key: &T::Key, value: &V) -> Result<()> {
        self.root = Some(match &self.root {
            Some(index) => {
                if !index.sealed() {
                    self.forest.push(index, key, value).await?
                } else {
                    let index = self.forest.single_branch(index.clone()).await?.into();
                    self.forest.push(&index, key, value).await?
                }
            }
            None => self.forest.single_leaf(key, value).await?.into(),
        });
        Ok(())
    }

    /// extend the node with the given iterator of key/value pairs
    pub async fn extend(&mut self, from: impl IntoIterator<Item = (T::Key, V)>) -> Result<()> {
        let mut from = from.into_iter().peekable();
        let root = if let Some(root) = &self.root {
            root.clone()
        } else {
            self.forest.fill_node(0, from.by_ref()).await?.into()
        };
        self.root = Some(self.forest.extend_above(root, from.by_ref()).await?);
        Ok(())
    }

    /// extend the node with the given iterator of key/value pairs
    pub async fn extend_unbalanced(
        &mut self,
        from: impl IntoIterator<Item = (T::Key, V)>,
    ) -> Result<()> {
        let mut from = from.into_iter();
        let mut tree = Tree::empty(self.forest.clone());
        tree.extend(from.by_ref()).await?;
        self.root = match (self.root.clone(), tree.root) {
            (Some(a), Some(b)) => {
                let level = a.level().max(b.level()) + 1;
                let mut from = from.peekable();
                Some(
                    self.forest
                        .create_branch(vec![a, b], level, from.by_ref())
                        .await?
                        .into(),
                )
            },
            (None, tree) => tree,
            (tree, None) => tree,
        };
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

pub struct OffsetQuery {
    from: u64,
}

impl OffsetQuery {
    pub fn new(from: u64) -> Self {
        Self { from }
    }
}

impl<T: TreeTypes> Query<T> for OffsetQuery {
    type IndexIterator = Box<dyn std::iter::Iterator<Item=bool>>;
    fn containing(&self, offset: u64, x: &LeafIndex<T::Seq>) -> Self::IndexIterator {
        info!("OffsetQuery::containing {}", offset);
        let from = self.from;
        Box::new((0..x.data.count()).map(move |o| o + offset >= from))
    }
    fn intersecting(&self, offset: u64, x: &BranchIndex<T::Seq>) -> Self::IndexIterator {
        info!("OffsetQuery::intersecting {}", offset);
        // TODO
        Box::new(std::iter::repeat(true).take(x.data.count() as usize))
    }
}

/// Utility method to zip a number of indices with an offset that is increased by each index value
fn zip_with_offset<'a, I: Iterator<Item = Index<T>> + 'a, T: CompactSeq + 'a>(
    value: I,
    offset: u64,
) -> impl Iterator<Item = (Index<T>, u64)> + 'a {
    value.scan(offset, |offset, x| {
        let o0 = *offset;
        *offset += x.count();
        Some((x, o0))
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
        Ok(Leaf::new(self.store.get(&index.cid).await?))
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
        let leaf = Leaf::single(value, self.config.zstd_level)?;
        let cid = self.store.put(leaf.as_ref().raw(), cid::Codec::Raw).await?;
        let index = LeafIndex {
            cid,
            value_bytes: leaf.as_ref().raw().len() as u64,
            sealed: self.leaf_sealed(leaf.as_ref().raw().len() as u64, 1),
            data: T::Seq::single(key),
        };
        Ok(self.cache_leaf(index, leaf))
    }

    /// Creates a leaf from a sequence that either contains all items from the sequence, or is full
    async fn create_leaf<V: Serialize + Debug>(
        &self,
        mut data: T::Seq,
        builder: ZstdArrayBuilder,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<LeafIndex<T::Seq>> {
        info!("create_leaf {}", builder.raw().len());
        assert!(from.peek().is_some());
        let builder = builder.fill(
            || {
                if data.count() < self.config.max_leaf_count {
                    from.next().map(|(k, v)| {
                        data.push(&k);
                        v
                    })
                } else {
                    None
                }
            },
            self.config.max_leaf_size,
        )?;
        let leaf = Leaf::Writable(builder);
        let cid = self.store.put(leaf.as_ref().raw(), cid::Codec::Raw).await?;
        let index = LeafIndex {
            cid,
            value_bytes: leaf.as_ref().raw().len() as u64,
            sealed: self.leaf_sealed(leaf.as_ref().raw().len() as u64, data.count()),
            data,
        };
        info!(
            "leaf created {} {} {}",
            index.data.count(),
            index.value_bytes,
            index.sealed
        );
        Ok(self.cache_leaf(index, leaf))
    }

    async fn extend_above<V: Serialize + Debug>(
        &self,
        node: Index<T::Seq>,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<Index<T::Seq>> {
        let mut node = self.extend(&node, from).await?;
        while from.peek().is_some() {
            let level = node.level() + 1;
            node = self.create_branch(vec![node], level, from).await?.into();
        }
        Ok(node)
    }

    async fn create_branch<V: Serialize + Debug>(
        &self,
        mut children: Vec<Index<T::Seq>>,
        level: u32,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<BranchIndex<T::Seq>> {
        assert!(children.iter().all(|child| child.level() <= level - 1));
        let mut keys = children
            .iter()
            .map(|child| child.data().summarize())
            .collect::<Vec<_>>();
        while from.peek().is_some() && ((children.len() as u64) <= self.config.max_branch_count) {
            let child = self.fill_noder(level - 1, from).await?;
            let summary = child.data().summarize();
            keys.push(summary);
            children.push(child);
        }
        let cbor = serialize_compressed(&children, self.config.zstd_level)?;
        let cid = self.store.put(&cbor, cid::Codec::DagCBOR).await?;
        let count = children.iter().map(|x| x.count()).sum();
        let value_bytes = children.iter().map(|x| x.value_bytes()).sum();
        let key_bytes = children.iter().map(|x| x.key_bytes()).sum::<u64>() + cbor.len() as u64;
        let sealed = self.branch_sealed(&children, level);
        let data: T::Seq = compactseq_create(keys.into_iter())?;
        let index = BranchIndex {
            level,
            count,
            sealed,
            cid,
            data,
            key_bytes,
            value_bytes,
        };
        Ok(index)
    }

    async fn fill_node<V: Serialize + Debug>(
        &self,
        level: u32,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<Index<T::Seq>> {
        assert!(from.peek().is_some());
        if level == 0 {
            Ok(self
                .create_leaf(
                    T::Seq::empty(),
                    ZstdArrayBuilder::new(self.config.zstd_level)?,
                    from,
                )
                .await?
                .into())
        } else {
            Ok(self.create_branch(Vec::new(), level, from).await?.into())
        }
    }

    fn fill_noder<'a, V: Serialize + Debug + 'a>(
        &'a self,
        level: u32,
        from: &'a mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> LocalBoxFuture<'a, Result<Index<T::Seq>>> {
        self.fill_node(level, from).boxed_local()
    }

    /// predicate to determine if a leaf is sealed, based on the config
    fn branch_sealed(&self, items: &[Index<T::Seq>], level: u32) -> bool {
        // a branch with less than 2 children is never considered sealed.
        // if we ever get this a branch that is too large despite having just 1 child,
        // we should just panic.
        // must have at least 2 children
        if items.len() < 2 {
            return false;
        }
        // all children must be sealed
        if items.iter().any(|x| !x.sealed()) {
            return false;
        }
        // all children must be one level below us
        if items.iter().any(|x| x.level() != level - 1) {
            return false;
        }
        // we must be full
        items.len() as u64 >= self.config.max_branch_count
    }

    /// load a branch given a branch index
    async fn load_branch(&self, index: &BranchIndex<T::Seq>) -> Result<Branch<T::Seq>> {
        let bytes = self.store.get(&index.cid).await?;
        let children: Vec<_> = deserialize_compressed(&bytes)?;
        // let children = CborZstdArrayRef::new(bytes.as_ref()).items()?;
        Ok(Branch::<T::Seq>::new(children))
    }

    async fn load_branch_from_cid(&self, cid: Cid) -> Result<Index<T::Seq>> {
        assert_eq!(cid.codec(), cid::Codec::DagCBOR);
        let bytes = self.store.get(&cid).await?;
        let children: Vec<Index<T::Seq>> = deserialize_compressed(&bytes)?;
        let count = children.iter().map(|x| x.count()).sum();
        let level = children.iter().map(|x| x.level()).max().unwrap() + 1;
        let value_bytes = children.iter().map(|x| x.value_bytes()).sum();
        let key_bytes = children.iter().map(|x| x.key_bytes()).sum::<u64>() + (bytes.len() as u64);
        let data = compactseq_create(children.iter().map(|x| x.data().summarize()))?;
        let result = BranchIndex {
            cid,
            level,
            count,
            data,
            sealed: self.branch_sealed(&children, level),
            value_bytes,
            key_bytes,
        }
        .into();
        Ok(result)
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
        let t = [value];
        let cbor = serialize_compressed(&t, self.config.zstd_level)?;
        let value = &t[0];
        let cid = self.store.put(&cbor, cid::Codec::DagCBOR).await?;
        let index = BranchIndex {
            level: value.level() + 1,
            count: value.count(),
            value_bytes: value.value_bytes(),
            key_bytes: value.key_bytes() + cbor.len() as u64,
            sealed: false,
            cid,
            data: T::Seq::single(&value.data().summarize()),
        };
        Ok(index)
    }

    /// load a node, returning a structure containing the index and value for convenient matching
    ///
    /// this is identical to load_node, except that it will also try to find leaf nodes in the cache.
    /// readonly leaf nodes are too cheap to bother caching them, but writable leaf nodes are quite expensive and therefore
    /// worthwhile to cache.
    ///
    /// Branch nodes are expensive to decode, so they will always be cached even for reading.
    async fn load_node_write<'a>(&self, index: &'a Index<T::Seq>) -> Result<NodeInfo<'a, T::Seq>> {
        Ok(match index {
            Index::Branch(index) => NodeInfo::Branch(index, self.load_branch_cached(index).await?),
            Index::Leaf(index) => NodeInfo::Leaf(index, self.load_leaf_cached(index).await?),
        })
    }

    /// load a node, returning a structure containing the index and value for convenient matching
    async fn load_node<'a>(&self, index: &'a Index<T::Seq>) -> Result<NodeInfo<'a, T::Seq>> {
        Ok(match index {
            Index::Branch(index) => NodeInfo::Branch(index, self.load_branch_cached(index).await?),
            Index::Leaf(index) => NodeInfo::Leaf(index, self.load_leaf(index).await?),
        })
    }

    /// store a leaf in the cache, and return it wrapped into a generic index
    fn cache_leaf(&self, index: LeafIndex<T::Seq>, node: Leaf) -> LeafIndex<T::Seq> {
        self.leaf_cache
            .write()
            .unwrap()
            .put(index.cid.clone(), node);
        index
    }

    /// store a branch in the cache, and return it wrapped into a generic index
    fn cache_branch(
        &self,
        index: BranchIndex<T::Seq>,
        node: Branch<T::Seq>,
    ) -> BranchIndex<T::Seq> {
        self.branch_cache
            .write()
            .unwrap()
            .put(index.cid.clone(), node);
        index
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
        match self.load_node_write(index).await? {
            NodeInfo::Leaf(index, mut leaf) => {
                leaf = leaf.push(&value, self.config.zstd_level)?;
                let mut index = index.clone();
                // update the index data
                index.data.push(key);
                index.sealed =
                    self.leaf_sealed(leaf.as_ref().raw().len() as u64, index.data.count());
                index.cid = self.store.put(leaf.as_ref().raw(), cid::Codec::Raw).await?;
                Ok(self.cache_leaf(index, leaf).into())
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
                let ipld = serialize_compressed(&branch.children, self.config.zstd_level)?;
                let cid = self.store.put(&ipld, cid::Codec::DagCBOR).await?;
                index.count += 1;
                index.sealed = self.branch_sealed(&branch.children, index.level);
                index.cid = cid;
                Ok(self.cache_branch(index, branch).into())
            }
        }
    }

    /// helper to avoid compiler error when doing recursive call from async fn
    ///
    /// If you call push0().boxed_local() directly in push0, you get this error:
    /// recursion in an `async fn` requires boxing a recursive `async fn` must be rewritten to return a boxed `dyn Future`
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

    async fn extend<V: Serialize + Debug>(
        &self,
        index: &Index<T::Seq>,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<Index<T::Seq>> {
        info!(
            "extend {} {} {} {}",
            index.level(),
            index.key_bytes(),
            index.value_bytes(),
            index.sealed()
        );
        if index.sealed() || from.peek().is_none() {
            return Ok(index.clone());
        }
        match self.load_node_write(index).await? {
            NodeInfo::Leaf(index, leaf) => {
                info!("extending existing leaf");
                let data = index.data.clone();
                let builder = leaf.builder(self.config.zstd_level)?;
                Ok(self.create_leaf(data, builder, from).await?.into())
            }
            NodeInfo::Branch(index, branch) => {
                info!("extending branch");
                let mut children = branch.children;
                if let Some(last_child) = children.last_mut() {
                    *last_child = self.extendr(&last_child, from).await?;
                }
                Ok(self
                    .create_branch(children, index.level, from)
                    .await?
                    .into())
            }
        }
    }

    fn extendr<'a, V: Serialize + Debug + 'a>(
        &'a self,
        node: &'a Index<T::Seq>,
        from: &'a mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> LocalBoxFuture<'a, Result<Index<T::Seq>>> {
        self.extend(node, from).boxed_local()
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
                Err(anyhow!("index out of bounds: {}", offset).into())
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

    fn stream<'a, V: DeserializeOwned + Debug>(
        &'a self,
        index: Index<T::Seq>,
    ) -> LocalBoxStream<'a, Result<(T::Key, V)>> {
        let s = async move {
            Ok(match self.load_node(&index).await? {
                NodeInfo::Leaf(index, node) => {
                    info!("streaming leaf {}", index.data.count());
                    let keys = index.items();
                    info!("raw compressed data {}", node.as_ref().raw().len());
                    let elems = node.as_ref().items()?;
                    let pairs = keys.zip(elems).collect::<Vec<_>>();
                    stream::iter(pairs).map(|x| Ok(x)).left_stream()
                }
                NodeInfo::Branch(_, node) => {
                    info!("streaming branch {} {}", index.level(), node.children.len());
                    stream::iter(node.children)
                        .map(move |child| self.stream(child))
                        .flatten()
                        .right_stream()
                }
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
                    let elems: Vec<V> = node.as_ref().select(matching.iter().cloned())?;
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
                println!(
                    "{}Leaf({}, {}, {})",
                    prefix,
                    index.data.count(),
                    index.sealed,
                    index.value_bytes
                );
            }
            NodeInfo::Branch(index, branch) => {
                println!(
                    "{}Branch({}, {}, {}, {})",
                    prefix, index.count, index.sealed, index.key_bytes, index.value_bytes,
                );
                let prefix = prefix.to_string() + "  ";
                for x in branch.children.iter() {
                    self.dumpr(x, &prefix).await?;
                }
            }
        };
        Ok(())
    }

    async fn filled(&self, index: &Index<T::Seq>) -> Result<Option<Index<T::Seq>>> {
        if index.sealed() {
            Ok(Some(index.clone()))
        } else if let NodeInfo::Branch(_, branch) = self.load_node(index).await? {
            if let Some(child) = branch.children.first() {
                self.filledr(child).await
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
    
    fn filledr<'a>(&'a self, index: &'a Index<T::Seq>) -> LocalBoxFuture<'a, Result<Option<Index<T::Seq>>>> {
        self.filled(index).boxed_local()
    }

    /// creates a new forest
    pub fn new(store: ArcStore, config: Config) -> Self {
        let branch_cache = RwLock::new(lru::LruCache::<Cid, Branch<T::Seq>>::new(1000));
        let leaf_cache = RwLock::new(lru::LruCache::<Cid, Leaf>::new(1000));
        Self {
            store,
            config,
            branch_cache,
            leaf_cache,
            _tt: PhantomData,
        }
    }
}
