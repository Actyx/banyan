//! creation and traversal of banyan trees
use super::index::*;
use crate::stream::SourceStream;
use crate::{
    query::{AllQuery, OffsetRangeQuery, Query},
    store::ArcStore,
    zstd_array::ZstdArrayBuilder,
};
use anyhow::{anyhow, Result};
use bitvec::prelude::*;
use future::BoxFuture;
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
    io,
    iter::FromIterator,
    marker::PhantomData,
    sync::{Arc, RwLock},
};
use tracing::*;

type FutureResult<'a, T> = BoxFuture<'a, Result<T>>;

/// Trees can be parametrized with the key type and the sequence type. Also, to avoid a dependency
/// on a link type with all its baggage, we parameterize the link type.
///
/// There might be more types in the future, so this essentially acts as a module for the entire
/// code base.
pub trait TreeTypes: Debug + Send + Sync {
    /// key type. This also doubles as the type for a combination (union) of keys
    type Key: Debug + Eq + Send;
    /// compact sequence type to be used for indices
    type Seq: CompactSeq<Item = Self::Key>
        + Serialize
        + DeserializeOwned
        + Clone
        + Debug
        + FromIterator<Self::Key>
        + Send
        + Sync;
    /// link type to use over block boundaries
    type Link: ToString + Hash + Eq + Clone + Debug + Send + Sync;

    fn serialize_branch(
        links: &[&Self::Link],
        data: Vec<u8>,
        w: impl io::Write,
    ) -> anyhow::Result<()>;
    fn deserialize_branch(reader: impl io::Read) -> anyhow::Result<(Vec<Self::Link>, Vec<u8>)>;
}

/// A number of trees that are grouped together, sharing common key type, caches, and config
///
/// They not necessarily share the same values. Trees with different value types can be grouped together.
pub struct Forest<T: TreeTypes> {
    store: ArcStore<T::Link>,
    config: Config,
    branch_cache: Arc<RwLock<lru::LruCache<T::Link, Branch<T>>>>,
    leaf_cache: Arc<RwLock<lru::LruCache<T::Link, Leaf>>>,
    _tt: PhantomData<T>,
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
    /// config that will produce efficient trees
    ///
    /// keys are hardcoded to 0
    pub fn debug_fast() -> Self {
        Self::from_keys(salsa20::Key::from([0u8; 32]), salsa20::Key::from([0u8; 32]))
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
        Ok(if let Some(cid) = &index.link {
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

    /// create a leaf from scratch from an interator
    async fn leaf_from_iter<V: Serialize + Debug>(
        &self,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<LeafIndex<T>> {
        assert!(from.peek().is_some());
        self.extend_leaf(None, ZstdArrayBuilder::new(self.config().zstd_level)?, from)
            .await
    }

    /// Creates a leaf from a sequence that either contains all items from the sequence, or is full
    ///
    /// The result is the index of the leaf. The iterator will contain the elements that did not fit.
    async fn extend_leaf<V: Serialize + Debug>(
        &self,
        keys: Option<T::Seq>,
        builder: ZstdArrayBuilder,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<LeafIndex<T>> {
        assert!(from.peek().is_some());
        let mut keys = keys.map(|x| x.to_vec()).unwrap_or_default();
        let builder = builder.fill(
            || {
                if (keys.len() as u64) < self.config().max_leaf_count {
                    from.next().map(|(k, v)| {
                        keys.push(k);
                        v
                    })
                } else {
                    None
                }
            },
            self.config().target_leaf_size,
        )?;
        let leaf = Leaf::from_builder(builder)?;
        let keys = keys.into_iter().collect::<T::Seq>();
        // encrypt leaf
        let mut tmp: Vec<u8> = Vec::with_capacity(leaf.as_ref().compressed().len() + 24);
        let nonce = self.random_nonce();
        tmp.extend(nonce.as_slice());
        tmp.extend(leaf.as_ref().compressed());
        XSalsa20::new(&self.value_key().into(), &nonce.into()).apply_keystream(&mut tmp[24..]);
        // store leaf
        let cid = self.store.put(&tmp, true).await?;
        let index: LeafIndex<T> = LeafIndex {
            link: Some(cid),
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

    /// given some children and some additional elements, creates a node with the given
    /// children and new children from `from` until it is full
    pub(crate) async fn extend_branch<V: Serialize + Debug + Send>(
        &self,
        mut children: Vec<Index<T>>,
        level: u32,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
        mode: CreateMode,
    ) -> Result<BranchIndex<T>> {
        assert!(
            children.iter().all(|child| child.level() <= level - 1),
            "All children must be below the level of the branch to be created."
        );
        if mode == CreateMode::Packed {
            assert!(
                from.peek().is_none()
                    || children
                        .iter()
                        .all(|child| child.level() == level - 1 && child.sealed()),
                "All children must be sealed, and directly below the branch to be created."
            );
        } else {
            assert!(
                children.is_empty() || children.iter().any(|child| child.level() == level - 1),
                "If there are children, at least one must be directly below the branch to be created."
            );
        }
        let mut summaries = children
            .iter()
            .map(|child| child.data().summarize())
            .collect::<Vec<_>>();
        while from.peek().is_some() && ((children.len() as u64) < self.config().max_branch_count) {
            let child = self.fill_noder(level - 1, from).await?;
            let summary = child.data().summarize();
            summaries.push(summary);
            children.push(child);
        }
        let index = self.new_branch(&children, mode).await?;
        info!(
            "branch created count={} value_bytes={} key_bytes={} sealed={}",
            index.summaries.count(),
            index.value_bytes,
            index.key_bytes,
            index.sealed
        );
        if from.peek().is_some() {
            assert!(index.level == level);
            if mode == CreateMode::Packed {
                assert!(index.sealed);
            }
        } else {
            assert!(index.level <= level);
        }
        Ok(index)
    }

    /// Given an iterator of values and a level, consume from the iterator until either
    /// the iterator is consumed or the node is "filled". At the end of this call, the
    /// iterator will contain the remaining elements that did not "fit".
    async fn fill_node<V: Serialize + Debug + Send>(
        &self,
        level: u32,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
    ) -> Result<Index<T>> {
        assert!(from.peek().is_some());
        Ok(if level == 0 {
            self.leaf_from_iter(from).await?.into()
        } else {
            self.extend_branch(Vec::new(), level, from, CreateMode::Packed)
                .await?
                .into()
        })
    }

    /// recursion helper for fill_node
    fn fill_noder<'a, V: Serialize + Debug + Send + 'a>(
        &'a self,
        level: u32,
        from: &'a mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
    ) -> FutureResult<'a, Index<T>> {
        self.fill_node(level, from).boxed()
    }

    /// creates a new branch from the given children and returns the branch index as a result.
    ///
    /// The level will be the max level of the children + 1. We do not want to support branches that are artificially high.
    async fn new_branch(&self, children: &[Index<T>], mode: CreateMode) -> Result<BranchIndex<T>> {
        assert!(!children.is_empty());
        if mode == CreateMode::Packed {
            assert!(is_sorted(children.iter().map(|x| x.level()).rev()));
        }
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
        let summaries: T::Seq = summaries.into_iter().collect();
        Ok(BranchIndex {
            level,
            count,
            sealed,
            link: Some(cid),
            summaries,
            key_bytes,
            value_bytes,
        })
    }

    /// extends an existing node with some values
    ///
    /// The result will have the max level `level`. `from` will contain all elements that did not fit.
    pub(crate) async fn extend_above<V: Serialize + Debug + Send>(
        &self,
        node: Option<&Index<T>>,
        level: u32,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
    ) -> Result<Index<T>> {
        assert!(from.peek().is_some());
        assert!(node.map(|node| level >= node.level()).unwrap_or(true));
        let mut node = if let Some(node) = node {
            self.extendr(node, from).await?
        } else {
            self.leaf_from_iter(from).await?.into()
        };
        while (from.peek().is_some() || node.level() == 0) && node.level() < level {
            let level = node.level() + 1;
            node = self
                .extend_branch(vec![node], level, from, CreateMode::Packed)
                .await?
                .into();
        }
        if from.peek().is_some() {
            assert!(node.level() == level);
            assert!(node.sealed());
        } else {
            assert!(node.level() <= level);
        }
        Ok(node)
    }

    /// extends an existing node with some values
    ///
    /// The result will have the same level as the input. `from` will contain all elements that did not fit.        
    async fn extend<V: Serialize + Debug + Send>(
        &self,
        index: &Index<T>,
        from: &mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
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
        Ok(match self.load_node(index).await? {
            NodeInfo::Leaf(index, leaf) => {
                info!("extending existing leaf");
                let keys = index.keys.clone();
                let builder = leaf.builder(self.config().zstd_level)?;
                self.extend_leaf(Some(keys), builder, from).await?.into()
            }
            NodeInfo::Branch(index, branch) => {
                info!("extending existing branch");
                let mut children = branch.children;
                if let Some(last_child) = children.last_mut() {
                    *last_child = self
                        .extend_above(Some(last_child), index.level - 1, from)
                        .await?;
                }
                self.extend_branch(children, index.level, from, CreateMode::Packed)
                    .await?
                    .into()
            }
            NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => {
                // purged nodes can not be extended
                index.clone()
            }
        })
    }

    /// recursion helper for extend
    fn extendr<'a, V: Serialize + Debug + Send + 'a>(
        &'a self,
        node: &'a Index<T>,
        from: &'a mut std::iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
    ) -> FutureResult<'a, Index<T>> {
        self.extend(node, from).boxed()
    }

    /// Find a valid branch in an array of children.
    /// This can be either a sealed node at the start, or an unsealed node at the end
    fn find_valid_branch(&self, children: &[Index<T>]) -> BranchResult {
        assert!(!children.is_empty());
        let max_branch_count = self.config.max_branch_count as usize;
        let level = children[0].level();
        let pos = children
            .iter()
            .position(|x| x.level() < level)
            .unwrap_or(children.len());
        if pos >= max_branch_count {
            // sequence starts with roots that we can turn into a sealed node
            BranchResult::Sealed(max_branch_count)
        } else if pos == children.len() || pos == children.len() - 1 {
            // sequence is something we can turn into an unsealed node
            BranchResult::Unsealed(max_branch_count.min(children.len()))
        } else {
            // remove first pos and recurse
            BranchResult::Skip(pos)
        }
    }

    /// Performs a single step of simplification on a sequence of sealed roots of descending level
    pub(crate) async fn simplify_roots(
        &self,
        roots: &mut Vec<Index<T>>,
        from: usize,
    ) -> Result<()> {
        assert!(roots.len() > 1);
        assert!(is_sorted(roots.iter().map(|x| x.level()).rev()));
        match self.find_valid_branch(&roots[from..]) {
            BranchResult::Sealed(count) | BranchResult::Unsealed(count) => {
                let range = from..from + count;
                let node = self
                    .new_branch(&roots[range.clone()], CreateMode::Packed)
                    .await?;
                roots.splice(range, Some(node.into()));
            }
            BranchResult::Skip(count) => {
                self.simplify_rootsr(roots, from + count).await?;
            }
        }
        Ok(())
    }

    /// recursion helper for simplify_roots
    fn simplify_rootsr<'a>(
        &'a self,
        roots: &'a mut Vec<Index<T>>,
        from: usize,
    ) -> FutureResult<'a, ()> {
        self.simplify_roots(roots, from).boxed()
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
        Ok(if let Some(cid) = &index.link {
            let bytes = self.store.get(&cid).await?;
            let children: Vec<_> = deserialize_compressed(&self.index_key(), &bytes)?;
            // let children = CborZstdArrayRef::new(bytes.as_ref()).items()?;
            Some(Branch::<T>::new(children))
        } else {
            None
        })
    }

    pub(crate) async fn load_branch_from_cid(&self, cid: T::Link) -> Result<Index<T>> {
        let bytes = self.store.get(&cid).await?;
        let children: Vec<Index<T>> = deserialize_compressed(&self.index_key(), &bytes)?;
        let level = children.iter().map(|x| x.level()).max().unwrap() + 1;
        let count = children.iter().map(|x| x.count()).sum();
        let value_bytes = children.iter().map(|x| x.value_bytes()).sum();
        let key_bytes = children.iter().map(|x| x.key_bytes()).sum::<u64>() + (bytes.len() as u64);
        let summaries = children.iter().map(|x| x.data().summarize()).collect();
        let result = BranchIndex {
            link: Some(cid),
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
        if let Some(cid) = &index.link {
            let res = self.branch_cache().write().unwrap().get(cid).cloned();
            match res {
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
        if let Some(cid) = &index.link {
            self.leaf_cache().write().unwrap().put(cid.clone(), node);
        }
        index
    }

    pub(crate) async fn get<V: DeserializeOwned>(
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

    /// recursion helper for get
    fn getr<'a, V: DeserializeOwned + 'a>(
        &'a self,
        node: &'a Index<T>,
        offset: u64,
    ) -> FutureResult<'a, Option<(T::Key, V)>> {
        self.get(node, offset).boxed()
    }

    pub(crate) fn stream<'a, V: DeserializeOwned + Debug>(
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

    pub(crate) fn stream_filtered<'a, V: DeserializeOwned, Q: Query<T> + Debug>(
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

    pub(crate) fn stream_filtered_static<V: DeserializeOwned, Q: Query<T> + Clone + 'static>(
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

    pub(crate) fn stream_filtered_static_chunked<
        V: DeserializeOwned,
        Q: Query<T> + Clone + 'static,
    >(
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
                        range: offset..offset + index.keys.count(),
                        data: pairs,
                    };
                    stream::once(future::ok(chunk)).left_stream().left_stream()
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
                                    range: offset..offset + child.count(),
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

    pub(crate) fn stream_filtered_static_chunked_reverse<
        V: DeserializeOwned,
        Q: Query<T> + Clone + 'static,
    >(
        self: Arc<Self>,
        offset: u64,
        query: Q,
        index: Index<T>,
    ) -> LocalBoxStream<'static, Result<FilteredChunk<T, V>>> {
        let s =
            async move {
                Ok(match self.load_node(&index).await? {
                    NodeInfo::Leaf(index, node) => {
                        // todo: don't get the node here, since we might not need it
                        let mut matching = BitVec::repeat(true, index.keys.len());
                        query.containing(offset, index, &mut matching);
                        let keys = index.select_keys(&matching);
                        let elems: Vec<V> = node.as_ref().select(&matching)?;
                        let mut pairs = keys
                            .zip(elems)
                            .map(|((o, k), v)| (o + offset, k, v))
                            .collect::<Vec<_>>();
                        pairs.reverse();
                        let chunk = FilteredChunk {
                            range: offset..offset + index.keys.count(),
                            data: pairs,
                        };
                        stream::once(future::ok(chunk)).left_stream().left_stream()
                    }
                    NodeInfo::Branch(index, node) => {
                        // todo: don't get the node here, since we might not need it
                        let mut matching = BitVec::repeat(true, index.summaries.len());
                        query.intersecting(offset, index, &mut matching);
                        let offsets = zip_with_offset(node.children.into_iter(), offset);
                        let children: Vec<_> = matching.into_iter().zip(offsets).collect();
                        let iter = children.into_iter().rev().map(
                            move |(is_matching, (child, offset))| {
                                if is_matching {
                                    self.clone()
                                        .stream_filtered_static_chunked_reverse(
                                            offset,
                                            query.clone(),
                                            child,
                                        )
                                        .right_stream()
                                } else {
                                    let placeholder = FilteredChunk {
                                        range: offset..offset + child.count(),
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

    pub(crate) async fn retain<Q: Query<T> + Send + Sync>(
        &self,
        offset: u64,
        query: &Q,
        index: &Index<T>,
        level: &mut i32,
    ) -> Result<Index<T>> {
        if !index.sealed() {
            *level = (*level).min((index.level() as i32) - 1);
        }
        match index {
            Index::Branch(index) => {
                let mut index = index.clone();
                // only do the check unless we are already purged
                let mut matching = BitVec::repeat(true, index.summaries.len());
                query.intersecting(offset, &index, &mut matching);
                if index.link.is_some()
                    && index.sealed
                    && !matching.any()
                    && index.level as i32 <= *level
                {
                    index.link = None;
                }
                // this will only be executed unless we are already purged
                if let Some(node) = self.load_branch(&index).await? {
                    let mut children = node.children.clone();
                    let mut changed = false;
                    let offsets = zip_with_offset_ref(node.children.iter(), offset);
                    for (i, (child, offset)) in offsets.enumerate() {
                        // TODO: ensure we only purge children that are in the packed part!
                        let child1 = self.retainr(offset, query, child, level).await?;
                        if child1.link() != child.link() {
                            children[i] = child1;
                            changed = true;
                        }
                    }
                    // rewrite the node and update the cid if children have changed
                    if changed {
                        let (cid, _) = self.persist_branch(&children).await?;
                        // todo: update size data
                        index.link = Some(cid);
                    }
                }
                Ok(index.into())
            }
            Index::Leaf(index) => {
                // only do the check unless we are already purged
                let mut index = index.clone();
                if index.sealed && index.link.is_some() && *level >= 0 {
                    let mut matching = BitVec::repeat(true, index.keys.len());
                    query.containing(offset, &index, &mut matching);
                    if !matching.any() {
                        index.link = None
                    }
                }
                Ok(index.into())
            }
        }
    }

    // all these stubs could be replaced with https://docs.rs/async-recursion/
    /// recursion helper for retain
    fn retainr<'a, Q: Query<T> + Send + Sync>(
        &'a self,
        offset: u64,
        query: &'a Q,
        index: &'a Index<T>,
        level: &'a mut i32,
    ) -> FutureResult<'a, Index<T>> {
        self.retain(offset, query, index, level).boxed()
    }

    pub(crate) async fn repair(
        &self,
        index: &Index<T>,
        report: &mut Vec<String>,
        level: &mut i32,
    ) -> Result<Index<T>> {
        if !index.sealed() {
            *level = (*level).min((index.level() as i32) - 1);
        }
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
                            let child1 = self.repairr(child, report, level).await?;
                            if child1.link() != child.link() {
                                children[i] = child1;
                                changed = true;
                            }
                        }
                        // rewrite the node and update the cid if children have changed
                        if changed {
                            let (cid, _) = self.persist_branch(&children).await?;
                            index.link = Some(cid);
                        }
                    }
                    Ok(None) => {
                        // already purged, nothing to do
                    }
                    Err(cause) => {
                        let cid_txt = index.link.map(|x| x.to_string()).unwrap_or("".into());
                        report.push(format!("forgetting branch {} due to {}", cid_txt, cause));
                        if !index.sealed {
                            report.push(format!("warning: forgetting unsealed branch!"));
                        } else if index.level as i32 > *level {
                            report.push(format!("warning: forgetting branch in unpacked part!"));
                        }
                        index.link = None;
                    }
                }
                Ok(index.into())
            }
            Index::Leaf(index) => {
                let mut index = index.clone();
                // important not to hit the cache here!
                if let Err(cause) = self.load_leaf(&index).await {
                    let cid_txt = index.link.map(|x| x.to_string()).unwrap_or("".into());
                    report.push(format!("forgetting leaf {} due to {}", cid_txt, cause));
                    if !index.sealed {
                        report.push(format!("warning: forgetting unsealed leaf!"));
                    } else if *level < 0 {
                        report.push(format!("warning: forgetting leaf in unpacked part!"));
                    }
                    index.link = None;
                }
                Ok(index.into())
            }
        }
    }

    /// recursion helper for repair
    fn repairr<'a>(
        &'a self,
        index: &'a Index<T>,
        report: &'a mut Vec<String>,
        level: &'a mut i32,
    ) -> FutureResult<'a, Index<T>> {
        self.repair(index, report, level).boxed()
    }

    pub(crate) async fn dump(&self, index: &Index<T>, prefix: &str) -> Result<()> {
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

    /// recursion helper for dump
    fn dumpr<'a>(&'a self, index: &'a Index<T>, prefix: &'a str) -> FutureResult<'a, ()> {
        self.dump(index, prefix).boxed()
    }

    pub(crate) async fn roots(&self, index: &Index<T>) -> Result<Vec<Index<T>>> {
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

    /// recursion helper for roots0
    fn roots0r<'a>(
        &'a self,
        index: &'a Index<T>,
        level: &'a mut i32,
        res: &'a mut Vec<Index<T>>,
    ) -> FutureResult<'a, ()> {
        self.roots0(index, level, res).boxed()
    }

    pub(crate) async fn check_invariants(
        &self,
        index: &Index<T>,
        level: &mut i32,
        msgs: &mut Vec<String>,
    ) -> Result<()> {
        macro_rules! check {
            ($expression:expr) => {
                if !$expression {
                    let text = stringify!($expression);
                    msgs.push(format!("{}", text));
                }
            };
        }
        if !index.sealed() {
            *level = (*level).min((index.level() as i32) - 1);
        }
        match self.load_node(index).await? {
            NodeInfo::Leaf(index, leaf) => {
                let value_count = leaf.as_ref().count()?;
                check!(value_count == index.keys.count());
                let leaf_sealed = self.leaf_sealed(index.value_bytes, index.keys.count());
                check!(index.sealed == leaf_sealed);
            }
            NodeInfo::Branch(index, branch) => {
                check!(branch.count() == index.summaries.count());
                for child in &branch.children {
                    if index.sealed {
                        check!(child.level() == index.level - 1);
                    } else {
                        check!(child.level() < index.level);
                    }
                }
                for (child, summary) in branch.children.iter().zip(index.summaries()) {
                    let child_summary = child.data().summarize();
                    check!(child_summary == summary);
                }
                let branch_sealed = self.branch_sealed(&branch.children, index.level);
                check!(index.sealed == branch_sealed);
                for child in &branch.children {
                    self.check_invariantsr(child, level, msgs).await?;
                }
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

    /// Recursion helper for check_invariants
    fn check_invariantsr<'a>(
        &'a self,
        index: &'a Index<T>,
        level: &'a mut i32,
        msgs: &'a mut Vec<String>,
    ) -> FutureResult<'a, ()> {
        self.check_invariants(index, level, msgs).boxed()
    }

    /// Checks if a node is packed to the left
    pub(crate) async fn is_packed(&self, index: &Index<T>) -> Result<bool> {
        Ok(
            if let NodeInfo::Branch(index, branch) = self.load_node(index).await? {
                if index.sealed {
                    // sealed nodes, for themselves, are packed
                    true
                } else {
                    if let Some((last, rest)) = branch.children.split_last() {
                        // for the first n-1 children, they must all be sealed and at exactly 1 level below
                        let first_ok = rest
                            .iter()
                            .all(|child| child.sealed() && child.level() == index.level - 1);
                        // for the last child, it can be at any level below, and does not have to be sealed,
                        let last_ok = last.level() <= index.level - 1;
                        // but it must itself be packed
                        let rec_ok = self.is_packedr(last).await?;
                        first_ok && last_ok && rec_ok
                    } else {
                        // this should not happen, but a branch with no children can be considered packed
                        true
                    }
                }
            } else {
                true
            },
        )
    }

    /// Recursion helper for is_packed
    fn is_packedr<'a>(&'a self, index: &'a Index<T>) -> FutureResult<'a, bool> {
        self.is_packed(index).boxed()
    }

    fn leaf_cache(&self) -> &Arc<RwLock<lru::LruCache<T::Link, Leaf>>> {
        &self.leaf_cache
    }

    fn branch_cache(&self) -> &Arc<RwLock<lru::LruCache<T::Link, Branch<T>>>> {
        &self.branch_cache
    }

    /// creates a new forest
    pub fn new(store: ArcStore<T::Link>, config: Config) -> Self {
        let branch_cache = Arc::new(RwLock::new(lru::LruCache::<T::Link, Branch<T>>::new(1000)));
        let leaf_cache = Arc::new(RwLock::new(lru::LruCache::<T::Link, Leaf>::new(1000)));
        Self {
            store,
            config,
            branch_cache,
            leaf_cache,
            _tt: PhantomData,
        }
    }

    /// Creates a query object that can be used to translate a stream of roots
    /// to a stream of filtered values.
    ///
    /// This is done in a two step process to have two separate methods, one for
    /// which the type parameter can be inferred (Q), and one for which the type
    /// parameter must be provided (V)
    pub fn query<Q>(self: Arc<Self>, query: Q) -> SourceStream<T, Q> {
        SourceStream(self, query)
    }
}

fn is_sorted<T: Ord>(iter: impl Iterator<Item = T>) -> bool {
    iter.collect::<Vec<_>>().windows(2).all(|x| x[0] <= x[1])
}

fn show_levels<T: TreeTypes>(children: &[Index<T>]) -> String {
    format!(
        "{:?}",
        children
            .iter()
            .map(|x| (x.level(), x.sealed()))
            .collect::<Vec<_>>()
    )
}

/// Helper enum for finding a valid branch in a sequence of nodes
enum BranchResult {
    /// We found a sealed branch at the start.
    Sealed(usize),
    /// We found an unsealed branch that goes all the way to the end.
    Unsealed(usize),
    /// We found neither and have to skip a number of elements and recurse.
    Skip(usize),
}

/// Create mode for methods that create branch nodes.
/// This only controls how strict the assertions are.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub(crate) enum CreateMode {
    /// node is created packed, so strict invariant checking should be used
    Packed,
    /// node is created unpacked, so loose invariant checking should be used
    Unpacked,
}

/// A filtered chunk.
/// Contains both data and information about the offsets the data resulted from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredChunk<T: TreeTypes, V> {
    /// index range for this chunk
    pub range: std::ops::Range<u64>,
    // data
    pub data: Vec<(u64, T::Key, V)>,
}
