use super::index::*;
use crate::{
    query::{OffsetRangeQuery, Query},
    store::ArcStore,
    zstd_array::ZstdArrayBuilder,
};
use anyhow::{anyhow, Result};
use bitvec::prelude::*;
use future::LocalBoxFuture;
use futures::{prelude::*, stream::LocalBoxStream};
use rand::RngCore;
use salsa20::{
    stream_cipher::{NewStreamCipher, SyncStreamCipher},
    XSalsa20,
};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::{
    fmt,
    hash::Hash,
    iter::FromIterator,
    marker::PhantomData,
    sync::{Arc, RwLock},
};
use tracing::*;

type FutureResult<'a, T> = LocalBoxFuture<'a, Result<T>>;

/// Trees can be parametrized with the key type and the sequence type. Also, to avoid a dependency
/// on a link type with all its baggage, we parameterize the link type.
///
/// There might be more types in the future, so this essentially acts as a module for the entire
/// code base.
pub trait TreeTypes: Debug {
    /// key type. This also doubles as the type for a combination (union) of keys
    type Key: Semigroup + Debug + Eq;
    /// compact sequence type to be used for indices
    type Seq: CompactSeq<Item = Self::Key> + Serialize + DeserializeOwned + Clone + Debug;
    /// link type to use over block boundaries
    type Link: ToString + Hash + Eq + Serialize + DeserializeOwned + Clone + Debug;
}

/// A number of trees that are grouped together, sharing common key type, caches, and config
///
/// They not necessarily share the same values. Trees with different value types can be grouped together.
pub struct Forest<T: TreeTypes> {
    store: ArcStore<T::Link>,
    config: Config,
    branch_cache: RwLock<lru::LruCache<T::Link, Branch<T>>>,
    leaf_cache: RwLock<lru::LruCache<T::Link, Leaf>>,
    _tt: PhantomData<T>,
}

/// A tree. This is mostly an user friendly handle.
///
/// Most of the logic except for handling the empty case is implemented in the forest
pub struct Tree<T: TreeTypes, V> {
    root: Option<Index<T>>,
    forest: Arc<Forest<T>>,
    _t: PhantomData<V>,
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
    pub target_leaf_size: u64,
    /// salsa20 key to decrypt index nodes
    pub index_key: salsa20::Key,
    /// salsa20 key to decrypt value nodes
    pub value_key: salsa20::Key,
}

impl Config {
    /// config that will produce complex tree structures with few values
    ///
    /// keys are hardcoded to 0
    pub fn debug() -> Self {
        Self {
            target_leaf_size: 10000,
            max_leaf_count: 10,
            max_branch_count: 4,
            zstd_level: 10,
            index_key: salsa20::Key::from([0u8; 32]),
            value_key: salsa20::Key::from([0u8; 32]),
        }
    }
    /// reasonable default config from index key and value key
    pub fn from_keys(index_key: salsa20::Key, value_key: salsa20::Key) -> Self {
        Self {
            target_leaf_size: 1 << 14,
            max_leaf_count: 1 << 14,
            max_branch_count: 32,
            zstd_level: 10,
            index_key,
            value_key,
        }
    }

    pub fn random_key() -> salsa20::Key {
        // todo: not very random...
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        key.into()
    }
}

impl<T: TreeTypes, V> fmt::Debug for Tree<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => write!(
                f,
                "Tree(root={:?},key_bytes={},value_bytes={})",
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
            Some(root) => write!(f, "{:?}", root.cid(),),
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

impl<
        V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
        T: TreeTypes + 'static,
    > Tree<T, V>
{
    pub async fn new(cid: T::Link, forest: Arc<Forest<T>>) -> Result<Self> {
        Ok(Self {
            root: Some(forest.load_branch_from_cid(cid).await?),
            forest,
            _t: PhantomData,
        })
    }
    pub async fn from_roots(forest: Arc<Forest<T>>, mut roots: Vec<Index<T>>) -> Result<Self> {
        assert!(roots.iter().all(|x| x.sealed()));
        while roots.len() > 1 {
            println!(
                "{:?}",
                roots
                    .iter()
                    .map(|r| (r.level(), r.sealed()))
                    .collect::<Vec<_>>()
            );
            forest.simplify_roots(&mut roots, 0).await?;
        }
        println!(
            "Result: {:?}",
            roots
                .iter()
                .map(|r| (r.level(), r.sealed()))
                .collect::<Vec<_>>()
        );
        Ok(Self {
            root: roots.pop(),
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

    /// sealed roots of the tree
    pub async fn roots(&self) -> Result<Vec<Index<T>>> {
        match &self.root {
            Some(index) => self.forest.roots(index).await,
            None => Ok(Vec::new()),
        }
    }

    fn with_root(&self, root: Option<Index<T>>) -> Self {
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
        let query = OffsetRangeQuery::from(offset..);
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

    pub async fn pack2(&self) -> Result<Self> {
        let roots = self.roots().await?;
        let mut filled = Tree::<T, V>::from_roots(self.forest.clone(), roots).await?;
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
        self.root = Some(self.forest.extend_above(&root, u32::max_value(), from.by_ref()).await?);
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
                        .create_branch_unbalanced(vec![a, b], level, from.by_ref())
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
            Some(index) => self.forest.get(index, offset).await?,
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
    pub fn stream_filtered<'a, Q: Query<T> + Debug>(
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

    pub fn stream_filtered_static(
        self,
        query: impl Query<T> + Clone + 'static,
    ) -> impl Stream<Item = Result<(u64, T::Key, V)>> + 'static {
        match &self.root {
            Some(index) => self
                .forest
                .stream_filtered_static(0, query, index.clone())
                .left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    pub fn stream_filtered_static_chunked(
        self,
        query: impl Query<T> + Clone + 'static,
    ) -> impl Stream<Item = Result<FilteredChunk<T, V>>> + 'static {
        match &self.root {
            Some(index) => self
                .forest
                .stream_filtered_static_chunked(0, query, index.clone())
                .left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    /// forget all data except the one matching the query
    ///
    /// this is done as best effort and will not be precise. E.g. if a chunk of data contains
    /// just a tiny bit that needs to be retained, it will not be forgotten.
    ///
    /// from this follows that this is not a suitable method if you want to ensure that the
    /// non-matching data is completely gone.
    ///
    /// note that offsets will not be affected by this. Also, unsealed nodes will not be forgotten
    /// even if they do not match the query.
    pub async fn forget_except<'a, Q: Query<T>>(&'a mut self, query: &'a Q) -> Result<()> {
        if let Some(index) = &self.root {
            self.root = Some(self.forest.forget_except(0, query, index).await?);
        }
        Ok(())
    }

    /// repair a tree by purging parts of the tree that can not be resolved.
    ///
    /// produces a report of links that could not be resolved.const
    ///
    /// TODO: figure out implications when forgetting non-sealed nodes
    pub async fn repair<'a>(&mut self) -> Result<Vec<String>> {
        let mut report = Vec::new();
        if let Some(index) = &self.root {
            self.root = Some(self.forest.repair(index, &mut report).await?);
        }
        Ok(report)
    }

    /// true for an empty tree
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// number of elements in the tree
    pub fn count(&self) -> u64 {
        self.root.as_ref().map(|x| x.count()).unwrap_or_default()
    }

    /// root of a non-empty tree
    pub fn root(self) -> Option<T::Link> {
        self.root.and_then(|index| index.cid().clone())
    }
}

/// Utility method to zip a number of indices with an offset that is increased by each index value
fn zip_with_offset<'a, I: Iterator<Item = Index<T>> + 'a, T: TreeTypes + 'a>(
    value: I,
    offset: u64,
) -> impl Iterator<Item = (Index<T>, u64)> + 'a {
    value.scan(offset, |offset, x| {
        let o0 = *offset;
        *offset += x.count();
        Some((x, o0))
    })
}

/// Utility method to zip a number of indices with an offset that is increased by each index value
fn zip_with_offset_ref<'a, I: Iterator<Item = &'a Index<T>> + 'a, T: TreeTypes + 'a>(
    value: I,
    offset: u64,
) -> impl Iterator<Item = (&'a Index<T>, u64)> + 'a {
    value.scan(offset, |offset, x| {
        let o0 = *offset;
        *offset += x.count();
        Some((x, o0))
    })
}

/// basic random access append only async tree
impl<T> Forest<T>
where
    T: TreeTypes + 'static,
{
    fn value_key(&self) -> salsa20::Key {
        self.config().value_key
    }

    fn index_key(&self) -> salsa20::Key {
        self.config().index_key
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn store(&self) -> &ArcStore<T::Link> {
        &self.store
    }

    /// predicate to determine if a leaf is sealed, based on the config
    fn leaf_sealed(&self, bytes: u64, count: u64) -> bool {
        bytes >= self.config().target_leaf_size || count >= self.config().max_leaf_count
    }

    /// load a leaf given a leaf index
    async fn load_leaf(&self, index: &LeafIndex<T>) -> Result<Option<Leaf>> {
        Ok(if let Some(cid) = &index.cid {
            let data = &self.store().get(cid).await?;
            if data.len() < 24 {
                return Err(anyhow!("leaf data without nonce"));
            }
            let (nonce, data) = data.split_at(24);
            let mut data = data.to_vec();
            XSalsa20::new(&self.value_key().into(), nonce.into()).apply_keystream(&mut data);
            // cipher.apply_keystream(data)
            Some(Leaf::new(data.into()))
        } else {
            None
        })
    }

    /// Creates a leaf from a sequence that either contains all items from the sequence, or is full
    ///
    /// The result is the index of the leaf. The iterator will contain the elements that did not fit.
    async fn create_leaf<V: Serialize + Debug>(
        &self,
        mut keys: T::Seq,
        builder: ZstdArrayBuilder,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<LeafIndex<T>> {
        assert!(from.peek().is_some());
        let builder = builder.fill(
            || {
                if keys.count() < self.config().max_leaf_count {
                    from.next().map(|(k, v)| {
                        keys.push(&k);
                        v
                    })
                } else {
                    None
                }
            },
            self.config().target_leaf_size,
        )?;
        let leaf = Leaf::from_builder(builder)?;
        // encrypt leaf
        let mut tmp: Vec<u8> = Vec::with_capacity(leaf.as_ref().compressed().len() + 24);
        let nonce = self.random_nonce();
        tmp.extend(nonce.as_slice());
        tmp.extend(leaf.as_ref().compressed());
        XSalsa20::new(&self.value_key().into(), &nonce.into()).apply_keystream(&mut tmp[24..]);
        // store leaf
        let cid = self.store.put(&tmp, true).await?;
        let index: LeafIndex<T> = LeafIndex {
            cid: Some(cid),
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

    async fn create_branch<V: Serialize + Debug>(
        &self,
        children: Vec<Index<T>>,
        level: u32,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<BranchIndex<T>> {
        assert!(from.peek().is_none() || children.iter().all(|child| child.level() == level - 1));
        self.create_branch_unbalanced(children, level, from).await
    }

    /// given some children and some additional elements, creates a node with the given
    /// children and new children from `from` until it is full
    async fn create_branch_unbalanced<V: Serialize + Debug>(
        &self,
        mut children: Vec<Index<T>>,
        level: u32,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<BranchIndex<T>> {
        assert!(children.iter().all(|child| child.level() <= level - 1));
        assert!(children.is_empty() || children.iter().any(|child| child.level() == level - 1));
        let mut summaries = children
            .iter()
            .map(|child| child.data().summarize())
            .collect::<Vec<_>>();
        println!("{:?}", children.iter().map(|x| (x.level(), x.sealed())).collect::<Vec<_>>());
        if !is_sorted(children.iter().map(|x| x.level()).rev()) {
            panic!("children passed to create_branch unsorted");
        }
        while from.peek().is_some() && ((children.len() as u64) <= self.config().max_branch_count) {
            let child = self.fill_noder(level - 1, from).await?;
            let summary = child.data().summarize();
            summaries.push(summary);
            children.push(child);
        }
        if !is_sorted(children.iter().map(|x| x.level()).rev()) {
            println!("{:?}", show_levels(&children));
            panic!("children after adding are unsorted");
        }
        let index = self.new_branch(&children).await?;
        info!(
            "branch created count={} value_bytes={} key_bytes={} sealed={}",
            index.summaries.count(),
            index.value_bytes,
            index.key_bytes,
            index.sealed
        );
        Ok(index)
    }

    /// Given an iterator of values and a level, consume from the iterator until either
    /// the iterator is consumed or the node is "filled". At the end of this call, the
    /// iterator will contain the remaining elements that did not "fit".
    async fn fill_node<V: Serialize + Debug>(
        &self,
        level: u32,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<Index<T>> {
        assert!(from.peek().is_some());
        if level == 0 {
            Ok(self
                .create_leaf(
                    T::Seq::empty(),
                    ZstdArrayBuilder::new(self.config().zstd_level)?,
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
    ) -> FutureResult<'a, Index<T>> {
        self.fill_node(level, from).boxed_local()
    }

    /// creates a new branch from the given children and returns the branch index as a result.
    ///
    /// The level will be the max level of the children + 1. We do not want to support branches that are artificially high.
    async fn new_branch(&self, children: &[Index<T>]) -> Result<BranchIndex<T>> {
        assert!(!children.is_empty());
        assert!(is_sorted(children.iter().map(|x| x.level()).rev()));
        let (cid, encoded_children_len) = self.persist_branch(&children).await?;
        let level = children.iter().map(|x| x.level()).max().unwrap() + 1;
        let count = children.iter().map(|x| x.count()).sum();
        let summaries = children
            .iter()
            .map(|child| child.data().summarize())
            .collect::<Vec<_>>();
        let value_bytes = children.iter().map(|x| x.value_bytes()).sum();
        let key_bytes = children.iter().map(|x| x.key_bytes()).sum::<u64>() + encoded_children_len;
        let sealed = self.branch_sealed(&children, level);
        let summaries: T::Seq = T::Seq::new(summaries.into_iter())?;
        Ok(BranchIndex {
            level,
            count,
            sealed,
            cid: Some(cid),
            summaries,
            key_bytes,
            value_bytes,
        })
    }

    /// Find a valid branch in an array of children.
    /// This can be either a sealed node at the start, or an unsealed node at the end
    fn find_valid_branch(&self, children: &[Index<T>]) -> (usize, bool) {
        assert!(!children.is_empty());
        let max_branch_count = self.config.max_branch_count as usize;
        let level = children[0].level();
        let pos = children
            .iter()
            .position(|x| x.level() < level)
            .unwrap_or(children.len());
        if pos >= max_branch_count {
            // sequence starts with roots that we can turn into a sealed node
            (max_branch_count, true)
        } else if pos == children.len() || pos == children.len() - 1 {
            // sequence is something we can turn into an unsealed node
            (max_branch_count.min(children.len()), true)
        } else {
            // remove first pos and recurse
            (pos, false)
        }
    }

    /// Performs a single step of simplification on a sequence of sealed roots of descending level
    async fn simplify_roots(&self, roots: &mut Vec<Index<T>>, from: usize) -> Result<()> {
        assert!(roots.len() > 1);
        assert!(is_sorted(roots.iter().map(|x| x.level()).rev()));
        let (count, create) = self.find_valid_branch(&roots[from..]);
        if create {
            let range = from..from + count;
            let node = self.new_branch(&roots[range.clone()]).await?;
            roots.splice(range, Some(node.into()));
        } else {
            self.simplify_rootsr(roots, from + count).await?;
        }
        Ok(())
    }

    fn simplify_rootsr<'a>(
        &'a self,
        roots: &'a mut Vec<Index<T>>,
        from: usize,
    ) -> FutureResult<'a, ()> {
        self.simplify_roots(roots, from).boxed_local()
    }

    /// predicate to determine if a leaf is sealed, based on the config
    fn branch_sealed(&self, items: &[Index<T>], level: u32) -> bool {
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
        items.len() as u64 >= self.config().max_branch_count
    }

    /// load a branch given a branch index
    async fn load_branch(&self, index: &BranchIndex<T>) -> Result<Option<Branch<T>>> {
        Ok(if let Some(cid) = &index.cid {
            let bytes = self.store.get(&cid).await?;
            let children: Vec<_> = deserialize_compressed(&self.index_key(), &bytes)?;
            // let children = CborZstdArrayRef::new(bytes.as_ref()).items()?;
            Some(Branch::<T>::new(children))
        } else {
            None
        })
    }

    async fn load_branch_from_cid(&self, cid: T::Link) -> Result<Index<T>> {
        let bytes = self.store.get(&cid).await?;
        let children: Vec<Index<T>> = deserialize_compressed(&self.index_key(), &bytes)?;
        let level = children.iter().map(|x| x.level()).max().unwrap() + 1;
        let count = children.iter().map(|x| x.count()).sum();
        let value_bytes = children.iter().map(|x| x.value_bytes()).sum();
        let key_bytes = children.iter().map(|x| x.key_bytes()).sum::<u64>() + (bytes.len() as u64);
        let summaries = T::Seq::new(children.iter().map(|x| x.data().summarize()))?;
        let result = BranchIndex {
            cid: Some(cid),
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
    async fn load_branch_cached(&self, index: &BranchIndex<T>) -> Result<Option<Branch<T>>> {
        if let Some(cid) = &index.cid {
            match self.branch_cache().write().unwrap().pop(cid) {
                Some(branch) => Ok(Some(branch)),
                None => self.load_branch(index).await,
            }
        } else {
            Ok(None)
        }
    }

    fn random_nonce(&self) -> salsa20::XNonce {
        // todo: not very random...
        let mut nonce = [0u8; 24];
        rand::thread_rng().fill_bytes(&mut nonce);
        nonce.into()
    }

    /// load a node, returning a structure containing the index and value for convenient matching
    async fn load_node<'a>(&self, index: &'a Index<T>) -> Result<NodeInfo<'a, T>> {
        Ok(match index {
            Index::Branch(index) => {
                if let Some(branch) = self.load_branch_cached(index).await? {
                    NodeInfo::Branch(index, branch)
                } else {
                    NodeInfo::PurgedBranch(index)
                }
            }
            Index::Leaf(index) => {
                if let Some(leaf) = self.load_leaf(index).await? {
                    NodeInfo::Leaf(index, leaf)
                } else {
                    NodeInfo::PurgedLeaf(index)
                }
            }
        })
    }

    /// store a leaf in the cache, and return it wrapped into a generic index
    fn cache_leaf(&self, index: LeafIndex<T>, node: Leaf) -> LeafIndex<T> {
        if let Some(cid) = &index.cid {
            self.leaf_cache().write().unwrap().put(cid.clone(), node);
        }
        index
    }
    
    /// extends an existing node with some values
    ///
    /// The result will have the max level `level`. `from` will contain all elements that did not fit.
    async fn extend_above<V: Serialize + Debug>(
        &self,
        node: &Index<T>,
        level: u32,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<Index<T>> {
        assert!(level >= node.level());
        let mut node = self.extendr(node, from).await?;
        while (from.peek().is_some() || node.level() == 0) && node.level() < level {
            let level = node.level() + 1;
            node = self.create_branch(vec![node], level, from).await?.into();
        }
        Ok(node)
    }

    /// extends an existing node with some values
    ///
    /// The result will have the same level as the input. `from` will contain all elements that did not fit.
    async fn extend<V: Serialize + Debug>(
        &self,
        index: &Index<T>,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<Index<T>> {
        info!(
            "extend {} {} {} {}",
            index.level(),
            index.key_bytes(),
            index.value_bytes(),
            index.sealed(),
        );
        if index.sealed() || from.peek().is_none() {
            return Ok(index.clone());
        }
        match self.load_node(index).await? {
            NodeInfo::Leaf(index, leaf) => {
                info!("extending existing leaf");
                let data = index.keys.clone();
                let builder = leaf.builder(self.config().zstd_level)?;
                Ok(self.create_leaf(data, builder, from).await?.into())
            }
            NodeInfo::Branch(index, branch) => {
                info!("extending existing branch");
                let mut children = branch.children;
                if let Some(last_child) = children.last_mut() {                    
                    *last_child = self.extend_above(last_child, index.level - 1, from).await?;
                }
                let result = self
                    .create_branch(children, index.level, from)
                    .await?
                    .into();
                Ok(result)
            }
            NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => {
                // purged nodes can not be extended
                Ok(index.clone())
            }
        }
    }

    fn extendr<'a, V: Serialize + Debug + 'a>(
        &'a self,
        node: &'a Index<T>,
        from: &'a mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> FutureResult<'a, Index<T>> {
        self.extend(node, from).boxed_local()
    }

    async fn get<V: DeserializeOwned>(
        &self,
        index: &Index<T>,
        mut offset: u64,
    ) -> Result<Option<(T::Key, V)>> {
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
                Err(anyhow!("index out of bounds: {}", offset))
            }
            NodeInfo::Leaf(index, node) => {
                let v = node.child_at::<V>(offset)?;
                let k = index.keys.get(offset as usize).unwrap();
                Ok(Some((k, v)))
            }
            NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => Ok(None),
        }
    }

    fn getr<'a, V: DeserializeOwned + 'a>(
        &'a self,
        node: &'a Index<T>,
        offset: u64,
    ) -> FutureResult<'a, Option<(T::Key, V)>> {
        self.get(node, offset).boxed_local()
    }

    fn stream<'a, V: DeserializeOwned + Debug>(
        &'a self,
        index: Index<T>,
    ) -> LocalBoxStream<'a, Result<(T::Key, V)>> {
        let s = async move {
            Ok(match self.load_node(&index).await? {
                NodeInfo::Leaf(index, node) => {
                    info!("streaming leaf {}", index.keys.count());
                    let keys = index.keys();
                    info!("raw compressed data {}", node.as_ref().compressed().len());
                    let elems = node.as_ref().items()?;
                    let pairs = keys.zip(elems).collect::<Vec<_>>();
                    stream::iter(pairs).map(Ok).left_stream().left_stream()
                }
                NodeInfo::Branch(_, node) => {
                    info!("streaming branch {} {}", index.level(), node.children.len());
                    stream::iter(node.children)
                        .map(move |child| self.stream(child))
                        .flatten()
                        .right_stream()
                        .left_stream()
                }
                NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => {
                    stream::empty().right_stream()
                }
            })
        }
        .try_flatten_stream();
        Box::pin(s)
    }

    fn stream_filtered<'a, V: DeserializeOwned, Q: Query<T> + Debug>(
        &'a self,
        offset: u64,
        query: &'a Q,
        index: Index<T>,
    ) -> LocalBoxStream<'a, Result<(u64, T::Key, V)>> {
        let s = async move {
            Ok(match self.load_node(&index).await? {
                NodeInfo::Leaf(index, node) => {
                    // todo: don't get the node here, since we might not need it
                    let mut matching = BitVec::repeat(true, index.keys.len());
                    query.containing(offset, index, &mut matching);
                    let keys = index.select_keys(&matching);
                    let elems: Vec<V> = node.as_ref().select(&matching)?;
                    let pairs = keys
                        .zip(elems)
                        .map(|((o, k), v)| (o + offset, k, v))
                        .collect::<Vec<_>>();
                    stream::iter(pairs).map(Ok).left_stream().left_stream()
                }
                NodeInfo::Branch(index, node) => {
                    // todo: don't get the node here, since we might not need it
                    let mut matching = BitVec::repeat(true, index.summaries.len());
                    query.intersecting(offset, index, &mut matching);
                    let offsets = zip_with_offset(node.children.into_iter(), offset);
                    let children = matching.into_iter().zip(offsets).filter_map(|(m, c)| {
                        // use bool::then_some in case it gets stabilized
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
                        .left_stream()
                }
                NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => {
                    stream::empty().right_stream()
                }
            })
        }
        .try_flatten_stream();
        Box::pin(s)
    }

    fn stream_filtered_static<V: DeserializeOwned, Q: Query<T> + Clone + 'static>(
        self: Arc<Self>,
        offset: u64,
        query: Q,
        index: Index<T>,
    ) -> LocalBoxStream<'static, Result<(u64, T::Key, V)>> {
        let s = async move {
            Ok(match self.load_node(&index).await? {
                NodeInfo::Leaf(index, node) => {
                    // todo: don't get the node here, since we might not need it
                    let mut matching = BitVec::repeat(true, index.keys.len());
                    query.containing(offset, index, &mut matching);
                    let keys = index.select_keys(&matching);
                    let elems: Vec<V> = node.as_ref().select(&matching)?;
                    let pairs = keys
                        .zip(elems)
                        .map(|((o, k), v)| (o + offset, k, v))
                        .collect::<Vec<_>>();
                    stream::iter(pairs).map(Ok).left_stream().left_stream()
                }
                NodeInfo::Branch(index, node) => {
                    // todo: don't get the node here, since we might not need it
                    let mut matching = BitVec::repeat(true, index.summaries.len());
                    query.intersecting(offset, index, &mut matching);
                    let offsets = zip_with_offset(node.children.into_iter(), offset);
                    let children = matching.into_iter().zip(offsets).filter_map(|(m, c)| {
                        // use bool::then_some in case it gets stabilized
                        if m {
                            Some(c)
                        } else {
                            None
                        }
                    });
                    stream::iter(children)
                        .map(move |(child, offset)| {
                            self.clone()
                                .stream_filtered_static(offset, query.clone(), child)
                        })
                        .flatten()
                        .right_stream()
                        .left_stream()
                }
                NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => {
                    stream::empty().right_stream()
                }
            })
        }
        .try_flatten_stream();
        Box::pin(s)
    }

    fn stream_filtered_static_chunked<V: DeserializeOwned, Q: Query<T> + Clone + 'static>(
        self: Arc<Self>,
        offset: u64,
        query: Q,
        index: Index<T>,
    ) -> LocalBoxStream<'static, Result<FilteredChunk<T, V>>> {
        let s = async move {
            Ok(match self.load_node(&index).await? {
                NodeInfo::Leaf(index, node) => {
                    // todo: don't get the node here, since we might not need it
                    let mut matching = BitVec::repeat(true, index.keys.len());
                    query.containing(offset, index, &mut matching);
                    let keys = index.select_keys(&matching);
                    let elems: Vec<V> = node.as_ref().select(&matching)?;
                    let pairs = keys
                        .zip(elems)
                        .map(|((o, k), v)| (o + offset, k, v))
                        .collect::<Vec<_>>();
                    let chunk = FilteredChunk {
                        min: offset,
                        max: offset + index.keys.count(),
                        data: pairs,
                    };
                    stream::once(future::ready(chunk))
                        .map(Ok)
                        .left_stream()
                        .left_stream()
                }
                NodeInfo::Branch(index, node) => {
                    // todo: don't get the node here, since we might not need it
                    let mut matching = BitVec::repeat(true, index.summaries.len());
                    query.intersecting(offset, index, &mut matching);
                    let offsets = zip_with_offset(node.children.into_iter(), offset);
                    let iter = matching.into_iter().zip(offsets).map(
                        move |(is_matching, (child, offset))| {
                            if is_matching {
                                self.clone()
                                    .stream_filtered_static_chunked(offset, query.clone(), child)
                                    .right_stream()
                            } else {
                                let placeholder = FilteredChunk {
                                    min: offset,
                                    max: offset + child.count(),
                                    data: Vec::new(),
                                };
                                stream::once(future::ok(placeholder)).left_stream()
                            }
                        },
                    );
                    let stream = stream::iter(iter).flatten().right_stream().left_stream();
                    stream
                }
                NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => {
                    stream::empty().right_stream()
                }
            })
        }
        .try_flatten_stream();
        Box::pin(s)
    }

    async fn persist_branch(&self, children: &[Index<T>]) -> Result<(T::Link, u64)> {
        let mut cbor = Vec::new();
        serialize_compressed(
            &self.index_key(),
            &self.random_nonce(),
            &children,
            self.config().zstd_level,
            &mut cbor,
        )?;
        Ok((self.store.put(&cbor, false).await?, cbor.len() as u64))
    }

    async fn forget_except<Q: Query<T>>(
        &self,
        offset: u64,
        query: &Q,
        index: &Index<T>,
    ) -> Result<Index<T>> {
        match index {
            Index::Branch(index) => {
                let mut index = index.clone();
                // only do the check unless we are already purged
                let mut matching = BitVec::repeat(true, index.summaries.len());
                query.intersecting(offset, &index, &mut matching);
                if index.cid.is_some() && index.sealed && !matching.any() {
                    index.cid = None;
                }
                // this will only be executed unless we are already purged
                if let Some(node) = self.load_branch(&index).await? {
                    let mut children = node.children.clone();
                    let mut changed = false;
                    let offsets = zip_with_offset_ref(node.children.iter(), offset);
                    for (i, (child, offset)) in offsets.enumerate() {
                        let child1 = self.forget_exceptr(offset, query, child).await?;
                        if child1.cid().is_none() != child.cid().is_none() {
                            children[i] = child1;
                            changed = true;
                        }
                    }
                    // rewrite the node and update the cid if children have changed
                    if changed {
                        let (cid, _) = self.persist_branch(&children).await?;
                        // todo: update size data
                        index.cid = Some(cid);
                    }
                }
                Ok(index.into())
            }
            Index::Leaf(index) => {
                let mut index = index.clone();
                let mut matching = BitVec::repeat(true, index.keys.len());
                query.containing(offset, &index, &mut matching);
                // only do the check unless we are already purged
                if index.cid.is_some() && index.sealed && !matching.any() {
                    index.cid = None
                }
                Ok(index.into())
            }
        }
    }

    // all these stubs could be replaced with https://docs.rs/async-recursion/
    fn forget_exceptr<'a, Q: Query<T>>(
        &'a self,
        offset: u64,
        query: &'a Q,
        index: &'a Index<T>,
    ) -> FutureResult<'a, Index<T>> {
        self.forget_except(offset, query, index).boxed_local()
    }

    async fn repair(&self, index: &Index<T>, report: &mut Vec<String>) -> Result<Index<T>> {
        match index {
            Index::Branch(index) => {
                let mut index = index.clone();
                // important not to hit the cache here!
                let branch = self.load_branch(&index).await;
                match branch {
                    Ok(Some(node)) => {
                        let mut children = node.children.clone();
                        let mut changed = false;
                        for (i, child) in node.children.iter().enumerate() {
                            let child1 = self.repairr(child, report).await?;
                            if child1.cid().is_none() != child.cid().is_none() {
                                children[i] = child1;
                                changed = true;
                            }
                        }
                        // rewrite the node and update the cid if children have changed
                        if changed {
                            let (cid, _) = self.persist_branch(&children).await?;
                            index.cid = Some(cid);
                        }
                    }
                    Ok(None) => {
                        // already purged, nothing to do
                    }
                    Err(cause) => {
                        let cid_txt = index.cid.map(|x| x.to_string()).unwrap_or("".into());
                        report.push(format!("forgetting branch {} due to {}", cid_txt, cause));
                        if !index.sealed {
                            report.push(format!("warning: forgetting unsealed branch!"));
                        }
                        index.cid = None;
                    }
                }
                Ok(index.into())
            }
            Index::Leaf(index) => {
                let mut index = index.clone();
                // important not to hit the cache here!
                if let Err(cause) = self.load_leaf(&index).await {
                    let cid_txt = index.cid.map(|x| x.to_string()).unwrap_or("".into());
                    report.push(format!("forgetting leaf {} due to {}", cid_txt, cause));
                    if !index.sealed {
                        report.push(format!("warning: forgetting unsealed leaf!"));
                    }
                    index.cid = None;
                }
                Ok(index.into())
            }
        }
    }

    // all these stubs could be replaced with https://docs.rs/async-recursion/
    fn repairr<'a>(
        &'a self,
        index: &'a Index<T>,
        report: &'a mut Vec<String>,
    ) -> FutureResult<'a, Index<T>> {
        self.repair(index, report).boxed_local()
    }

    async fn dump(&self, index: &Index<T>, prefix: &str) -> Result<()> {
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
            NodeInfo::PurgedBranch(index) => {
                println!(
                    "{}PurgedBranch(count={}, key_bytes={}, value_bytes={}, sealed={})",
                    prefix, index.count, index.key_bytes, index.value_bytes, index.sealed,
                );
            }
            NodeInfo::PurgedLeaf(index) => {
                println!(
                    "{}PurgedLeaf(count={}, key_bytes={}, sealed={})",
                    prefix,
                    index.keys.count(),
                    index.value_bytes,
                    index.sealed,
                );
            }
        };
        Ok(())
    }

    fn dumpr<'a>(&'a self, index: &'a Index<T>, prefix: &'a str) -> FutureResult<'a, ()> {
        self.dump(index, prefix).boxed_local()
    }

    async fn filled(&self, index: &Index<T>) -> Result<Option<Index<T>>> {
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

    async fn roots(&self, index: &Index<T>) -> Result<Vec<Index<T>>> {
        let mut res = Vec::new();
        let mut level: i32 = i32::max_value();
        self.roots0(index, &mut level, &mut res).await?;
        Ok(res)
    }

    async fn roots0(
        &self,
        index: &Index<T>,
        level: &mut i32,
        res: &mut Vec<Index<T>>,
    ) -> Result<()> {
        if index.sealed() && index.level() as i32 <= *level {
            *level = index.level() as i32;
            res.push(index.clone());
        } else {
            *level = (*level).min(index.level() as i32 - 1);
            if let Index::Branch(b) = index {
                if let Some(branch) = self.load_branch(b).await? {
                    for child in branch.children.iter() {
                        self.roots0r(child, level, res).await?;
                    }
                }
            }
        }
        Ok(())
    }

    fn roots0r<'a>(
        &'a self,
        index: &'a Index<T>,
        level: &'a mut i32,
        res: &'a mut Vec<Index<T>>,
    ) -> FutureResult<'a, ()> {
        self.roots0(index, level, res).boxed_local()
    }

    async fn check_invariants(&self, index: &Index<T>, msgs: &mut Vec<String>) -> Result<()> {
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
            NodeInfo::PurgedBranch(_) => {
                // not possible to check invariants since the data to compare to is gone
            }
            NodeInfo::PurgedLeaf(_) => {
                // not possible to check invariants since the data to compare to is gone
            }
        };
        Ok(())
    }

    async fn is_packed(&self, index: &Index<T>) -> Result<bool> {
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
                    if !first_ok {
                        println!("first_ok false {:?}", show_levels(&branch.children));
                    }
                    if !last_ok {
                        println!("last_ok false {:?}", show_levels(&branch.children));
                    }
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

    fn is_packedr<'a>(&'a self, index: &'a Index<T>) -> FutureResult<'a, bool> {
        self.is_packed(index).boxed_local()
    }

    fn filledr<'a>(&'a self, index: &'a Index<T>) -> FutureResult<'a, Option<Index<T>>> {
        self.filled(index).boxed_local()
    }

    fn leaf_cache(&self) -> &RwLock<lru::LruCache<T::Link, Leaf>> {
        &self.leaf_cache
    }

    fn branch_cache(&self) -> &RwLock<lru::LruCache<T::Link, Branch<T>>> {
        &self.branch_cache
    }

    /// creates a new forest
    pub fn new(store: ArcStore<T::Link>, config: Config) -> Self {
        let branch_cache = RwLock::new(lru::LruCache::<T::Link, Branch<T>>::new(1000));
        let leaf_cache = RwLock::new(lru::LruCache::<T::Link, Leaf>::new(1000));
        Self {
            store,
            config,
            branch_cache,
            leaf_cache,
            _tt: PhantomData,
        }
    }
}

fn is_sorted<T: Ord>(iter: impl Iterator<Item = T>) -> bool {
    iter.collect::<Vec<_>>().windows(2).all(|x| x[0] <= x[1])
}

fn show_levels<T: TreeTypes>(children: &[Index<T>]) -> String {
    format!("{:?}", children.iter().map(|x| (x.level(), x.sealed())).collect::<Vec<_>>())
}

pub struct FilteredChunk<T: TreeTypes, V> {
    // inclusive
    pub min: u64,
    // exclusive
    pub max: u64,
    // data
    pub data: Vec<(u64, T::Key, V)>,
}
