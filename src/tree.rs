use super::index::*;
use crate::ipfs::Cid;
use crate::{store::ArcStore, zstd_array::ZstdArrayBuilder};
use anyhow::{anyhow, Result};
use bitvec::prelude::*;
use future::LocalBoxFuture;
use futures::{prelude::*, stream::LocalBoxStream};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::{
    fmt,
    iter::FromIterator,
    marker::PhantomData,
    sync::{Arc, RwLock},
};
use tracing::*;

/// Trees can be parametrized with the key type and the sequence type
///
/// There might be more types in the future, so all of them are grouped in this trait.
pub trait TreeTypes {
    /// key type. This also doubles as the type for a combination (union) of keys
    type Key: Semigroup + Debug + Eq;
    /// compact sequence type to be used for indices
    type Seq: CompactSeq<Item = Self::Key> + Serialize + DeserializeOwned + Clone + Debug;
}

/// A number of trees that are grouped together, sharing common key type, caches, and config
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
/// Queries work on compact value sequences instead of individual values for efficiency.
pub trait Query<T: TreeTypes> {
    /// an iterator returning `x.data.count()` elements, where each value is a bool indicating if the query *does* match
    fn containing(&self, offset: u64, x: &LeafIndex<T::Seq>) -> BitVec;
    /// an iterator returning `x.data.count()` elements, where each value is a bool indicating if the query *can* match
    fn intersecting(&self, offset: u64, x: &BranchIndex<T::Seq>) -> BitVec;
}

/// Configuration for a forest. Includes settings for when a node is considered full
pub struct Config {
    /// maximum number of values in a leaf
    pub max_leaf_count: u64,
    /// maximum number of children in a branch
    pub max_branch_count: u64,
    /// zstd level to use for compression
    pub zstd_level: i32,
    /// rough maximum compressed bytes of a leaf. If a node has more bytes than this, it is considered full.
    ///
    /// note that this might overshoot due to the fact that the zstd encoder has internal state, and it is not possible
    /// to flush after each value without losing compression efficiency. The overshoot is bounded though.
    pub max_leaf_size: u64,
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
                "Tree(root={},key_bytes={},value_bytes={})",
                root.cid(),
                root.key_bytes(),
                root.value_bytes()
            ),
            None => write!(f, "empty tree"),
        }
    }
}

impl<T: TreeTypes, V> fmt::Display for Tree<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => write!(f, "{}", root.cid(),),
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

    pub async fn check_invariants(&self) -> Result<Vec<String>> {
        let mut msgs = Vec::new();
        if let Some(root) = &self.root {
            if root.level() == 0 {
                msgs.push("tree should not have a leaf as direct child.".into());
            }
            self.forest.check_invariants(&root, &mut msgs).await?;
        }
        Ok(msgs)
    }

    pub async fn is_packed(&self) -> Result<bool> {
        if let Some(root) = &self.root {
            self.forest.is_packed(&root).await
        } else {
            Ok(true)
        }
    }

    pub async fn assert_invariants(&self) -> Result<()> {
        let msgs = self.check_invariants().await?;
        if !msgs.is_empty() {
            for msg in msgs {
                error!("Invariant failed: {}", msg);
            }
            panic!("assert_invariants failed");
        }
        Ok(())
    }

    pub async fn collect_from<B: FromIterator<(T::Key, V)>>(&self, offset: u64) -> Result<B> {
        let query = OffsetQuery::new(offset);
        let items: Vec<Result<(T::Key, V)>> = self
            .stream_filtered(&query)
            .map_ok(|(_, k, v)| (k, v))
            .collect::<Vec<_>>()
            .await;
        items.into_iter().collect::<Result<_>>()
    }

    /// packs the tree to the left
    pub async fn pack(&self) -> Result<Self> {
        let mut filled = self.filled().await?;
        let remainder: Vec<_> = self.collect_from(filled.count()).await?;
        filled.extend(remainder).await?;
        Ok(filled)
    }

    /// append a single element
    pub async fn push(&mut self, key: T::Key, value: V) -> Result<()> {
        self.extend(vec![(key, value)]).await
    }

    /// extend the node with the given iterator of key/value pairs
    pub async fn extend(&mut self, from: impl IntoIterator<Item = (T::Key, V)>) -> Result<()> {
        let mut from = from.into_iter().peekable();
        if from.peek().is_none() {
            // nothing to do
            return Ok(());
        }
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
            }
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

pub(crate) struct OffsetQuery {
    from: u64,
}

impl OffsetQuery {
    pub fn new(from: u64) -> Self {
        Self { from }
    }
}

impl<T: TreeTypes> Query<T> for OffsetQuery {
    fn containing(&self, offset: u64, x: &LeafIndex<T::Seq>) -> BitVec {
        info!("OffsetQuery::containing {}", offset);
        let from = self.from;
        (0..x.keys.count()).map(|o| o + offset >= from).collect()
    }
    fn intersecting(&self, offset: u64, x: &BranchIndex<T::Seq>) -> BitVec {
        info!("OffsetQuery::intersecting {}", offset);
        // TODO
        BitVec::repeat(true, x.summaries.count() as usize)
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

    /// Creates a leaf from a sequence that either contains all items from the sequence, or is full
    async fn create_leaf<V: Serialize + Debug>(
        &self,
        mut keys: T::Seq,
        builder: ZstdArrayBuilder,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<LeafIndex<T::Seq>> {
        assert!(from.peek().is_some());
        let builder = builder.fill(
            || {
                if keys.count() < self.config.max_leaf_count {
                    from.next().map(|(k, v)| {
                        keys.push(&k);
                        v
                    })
                } else {
                    None
                }
            },
            self.config.max_leaf_size,
        )?;
        let leaf = Leaf::Writable(builder);
        let cid = self
            .store
            .put(leaf.as_ref().compressed(), cid::Codec::Raw)
            .await?;
        let index = LeafIndex {
            cid,
            value_bytes: leaf.as_ref().compressed().len() as u64,
            sealed: self.leaf_sealed(leaf.as_ref().compressed().len() as u64, keys.count()),
            keys,
        };
        info!(
            "leaf created count={} bytes={} sealed={}",
            index.keys.count(),
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
        while from.peek().is_some() || node.level() == 0 {
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
        let mut summaries = children
            .iter()
            .map(|child| child.data().summarize())
            .collect::<Vec<_>>();
        while from.peek().is_some() && ((children.len() as u64) <= self.config.max_branch_count) {
            let child = self.fill_noder(level - 1, from).await?;
            let summary = child.data().summarize();
            summaries.push(summary);
            children.push(child);
        }
        let cbor = serialize_compressed(&children, self.config.zstd_level)?;
        let cid = self.store.put(&cbor, cid::Codec::DagCBOR).await?;
        let count = children.iter().map(|x| x.count()).sum();
        let value_bytes = children.iter().map(|x| x.value_bytes()).sum();
        let key_bytes = children.iter().map(|x| x.key_bytes()).sum::<u64>() + cbor.len() as u64;
        let sealed = self.branch_sealed(&children, level);
        let summaries: T::Seq = T::Seq::new(summaries.into_iter())?;
        let index = BranchIndex {
            level,
            count,
            sealed,
            cid,
            summaries,
            key_bytes,
            value_bytes,
        };
        info!(
            "branch created count={} value_bytes={} key_bytes={} sealed={}",
            index.summaries.count(),
            index.value_bytes,
            index.key_bytes,
            index.sealed
        );
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
        let summaries = T::Seq::new(children.iter().map(|x| x.data().summarize()))?;
        let result = BranchIndex {
            cid,
            level,
            count,
            summaries,
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
                let data = index.keys.clone();
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
                let k = index.keys.get(offset).unwrap();
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
                    info!("streaming leaf {}", index.keys.count());
                    let keys = index.keys();
                    info!("raw compressed data {}", node.as_ref().compressed().len());
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
                    let matching = query.containing(offset, index);
                    let keys = index.select_keys(&matching);
                    let elems: Vec<V> = node.as_ref().select(&matching)?;
                    let pairs = keys
                        .zip(elems)
                        .map(|((o, k), v)| (o + offset, k, v))
                        .collect::<Vec<_>>();
                    stream::iter(pairs).map(|x| Ok(x)).left_stream()
                }
                NodeInfo::Branch(index, node) => {
                    let matching = query.intersecting(offset, index);
                    let offsets = zip_with_offset(node.children.into_iter(), offset);
                    let children = matching.into_iter().zip(offsets).filter_map(|(m, c)| {
                        if m {
                            Some(c)
                        } else {
                            None
                        }
                    });
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
                    "{}Leaf(count={}, key_bytes={}, sealed={})",
                    prefix,
                    index.keys.count(),
                    index.value_bytes,
                    index.sealed,
                );
            }
            NodeInfo::Branch(index, branch) => {
                println!(
                    "{}Branch(count={}, key_bytes={}, value_bytes={}, sealed={})",
                    prefix, index.count, index.key_bytes, index.value_bytes, index.sealed,
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

    async fn check_invariants(&self, index: &Index<T::Seq>, msgs: &mut Vec<String>) -> Result<()> {
        macro_rules! check {
            ($expression:expr) => {
                if !$expression {
                    let text = stringify!($expression);
                    msgs.push(format!("{}", text));
                }
            };
        }
        match self.load_node(index).await? {
            NodeInfo::Leaf(index, leaf) => {
                let value_count = leaf.as_ref().count()?;
                check!(value_count == index.keys.count());
                let leaf_sealed = self.leaf_sealed(index.value_bytes, index.keys.count());
                check!(index.sealed == leaf_sealed);
            }
            NodeInfo::Branch(index, branch) => {
                let child_count = branch.children.len() as u64;
                check!(child_count == index.summaries.count());
                for child in &branch.children {
                    check!(child.level() < index.level);
                }
                for (child, summary) in branch.children.iter().zip(index.summaries()) {
                    let child_summary = child.data().summarize();
                    check!(child_summary == summary);
                }
                let branch_sealed = self.branch_sealed(&branch.children, index.level);
                check!(index.sealed == branch_sealed);
            }
        };
        Ok(())
    }

    async fn is_packed(&self, index: &Index<T::Seq>) -> Result<bool> {
        if let NodeInfo::Branch(index, branch) = self.load_node(index).await? {
            Ok(if index.sealed {
                // sealed nodes, for themselves, are packed
                true
            } else {
                if let Some((last, rest)) = branch.children.split_last() {
                    // for the first n-1 children, they must all be sealed and at exactly 1 level below
                    let first_ok = rest
                        .iter()
                        .all(|child| child.sealed() && child.level() == index.level - 1);
                    // for the last child, it can be at any level below, and does not have to be sealed, but it must itself be balanced
                    let last_ok = self.is_packedr(last).await?;
                    first_ok && last_ok
                } else {
                    // this should not happen, but a branch with no children can be considered packed
                    true
                }
            })
        } else {
            Ok(true)
        }
    }

    fn is_packedr<'a>(&'a self, index: &'a Index<T::Seq>) -> LocalBoxFuture<'a, Result<bool>> {
        self.is_packed(index).boxed_local()
    }

    fn filledr<'a>(
        &'a self,
        index: &'a Index<T::Seq>,
    ) -> LocalBoxFuture<'a, Result<Option<Index<T::Seq>>>> {
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
