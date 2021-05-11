//! creation and traversal of banyan trees
use super::index::*;
use crate::{
    forest::{
        Config, FilteredChunk, Forest, ForestIter, IndexIter, Secrets, Transaction, TreeTypes,
    },
    store::BlockWriter,
    util::BoxedIter,
};
use crate::{query::Query, store::ReadOnlyStore, util::IterExt};
use anyhow::Result;
use futures::prelude::*;
use libipld::cbor::DagCbor;
use std::{collections::BTreeMap, fmt, fmt::Debug, iter, sync::Arc, usize};
use tracing::*;

/// A thing that hands out unique offsets. Parts of StreamBuilderState
#[derive(Debug, Clone)]
pub struct Offset {
    value: u64,
}

impl Offset {
    pub fn new(value: u64) -> Self {
        Self { value }
    }

    pub fn reserve(&mut self, n: usize) -> u64 {
        let result = self.value;
        self.value = self
            .value
            .checked_add(n as u64)
            .expect("ran out of offsets");
        result
    }
}

#[derive(Debug, Clone)]
pub struct StreamBuilderState {
    /// the secrets used for building the trees of the stream
    secrets: Secrets,
    /// config that determines the branch size etc.
    pub(crate) config: Config,
    /// current offset
    pub(crate) offset: Offset,
}

impl StreamBuilderState {
    pub fn new(offset: u64, secrets: Secrets, config: Config) -> Self {
        Self {
            offset: Offset::new(offset),
            secrets,
            config,
        }
    }

    pub fn zstd_level(&self) -> i32 {
        self.config().zstd_level
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn secrets(&self) -> &Secrets {
        &self.secrets
    }

    pub fn value_key(&self) -> &chacha20::Key {
        self.secrets.value_key()
    }

    pub fn index_key(&self) -> &chacha20::Key {
        self.secrets.index_key()
    }
}

/// A tree. This is mostly an user friendly handle.
///
/// Most of the logic except for handling the empty case is implemented in the forest
pub struct Tree<T: TreeTypes> {
    root: Option<Arc<Index<T>>>,
    state: StreamBuilderState,
}

#[derive(Debug, Clone)]
pub struct Snapshot<T: TreeTypes> {
    root: Option<Arc<Index<T>>>,
    secrets: Secrets,
    offset: u64,
}

impl<T: TreeTypes> Snapshot<T> {
    pub(crate) fn new(root: Option<Arc<Index<T>>>, secrets: Secrets, offset: u64) -> Self {
        Self {
            root,
            secrets,
            offset,
        }
    }

    pub fn current(self) -> Option<(Arc<Index<T>>, Secrets)> {
        let secrets = self.secrets.clone();
        self.root.map(|x| (x, secrets))
    }
}

impl<T: TreeTypes> fmt::Debug for Tree<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => f
                .debug_struct("Tree")
                .field("count", &self.count())
                .field("key_bytes", &root.key_bytes())
                .field("value_bytes", &root.value_bytes())
                .field("link", &root.link())
                .finish(),
            None => f
                .debug_struct("Tree")
                .field("count", &self.count())
                .finish(),
        }
    }
}

impl<T: TreeTypes> fmt::Display for Tree<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => write!(f, "{:?}", root.link(),),
            None => write!(f, "empty tree"),
        }
    }
}

impl<T: TreeTypes> Clone for Tree<T> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            state: self.state.clone(),
        }
    }
}

pub type GraphEdges = Vec<(usize, usize)>;
pub type GraphNodes<S> = BTreeMap<usize, S>;

impl<
        T: TreeTypes,
        V: DagCbor + Clone + Send + Sync + Debug + 'static,
        R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
    > Forest<T, V, R>
{
    pub fn load_tree(&self, secrets: Secrets, config: Config, link: T::Link) -> Result<Tree<T>> {
        let (index, offset) = self.load_branch_from_link(
            &secrets,
            |items, level| config.branch_sealed(items, level),
            link,
        )?;
        let state = StreamBuilderState::new(offset, secrets, config);
        Ok(Tree::new_from_index(Some(index), state))
    }

    pub fn load_snapshot(&self, secrets: Secrets, link: T::Link) -> Result<Snapshot<T>> {
        // we pass in a predicate that makes the nodes sealed, since we don't care
        let (index, offset) = self.load_branch_from_link(&secrets, |_, _| true, link)?;
        // store the offset with the snapshot. Snapshots are immutable, so this won't change.
        Ok(Snapshot::new(Some(Arc::new(index)), secrets, offset))
    }

    /// dumps the tree structure
    pub fn dump(&self, tree: &Tree<T>) -> Result<()> {
        match &tree.root {
            Some(index) => self.dump0(tree.state.secrets(), index, ""),
            None => Ok(()),
        }
    }

    /// dumps the tree structure
    pub fn dump_graph<S>(
        &self,
        tree: &Tree<T>,
        f: impl Fn((usize, &NodeInfo<T>)) -> S + Clone,
    ) -> Result<(GraphEdges, GraphNodes<S>)> {
        match &tree.root {
            Some(index) => self.dump_graph0(tree.state.secrets(), None, 0, index, f),
            None => anyhow::bail!("Tree must not be empty"),
        }
    }

    pub(crate) fn traverse0<
        Q: Query<T> + Clone + Send + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    >(
        &self,
        secrets: Secrets,
        query: Q,
        index: Arc<Index<T>>,
        mk_extra: &'static F,
    ) -> BoxedIter<'static, Result<FilteredChunk<T, V, E>>> {
        ForestIter::new(self.clone(), secrets, query, index, mk_extra).boxed()
    }

    pub(crate) fn traverse_rev0<
        Q: Query<T> + Clone + Send + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    >(
        &self,
        secrets: Secrets,
        query: Q,
        index: Arc<Index<T>>,
        mk_extra: &'static F,
    ) -> BoxedIter<'static, Result<FilteredChunk<T, V, E>>> {
        ForestIter::new_rev(self.clone(), secrets, query, index, mk_extra).boxed()
    }

    fn index_iter0<Q: Query<T> + Clone + Send + 'static>(
        &self,
        secrets: Secrets,
        query: Q,
        index: Arc<Index<T>>,
    ) -> BoxedIter<'static, Result<Arc<Index<T>>>> {
        IndexIter::new(self.clone(), secrets, query, index).boxed()
    }

    fn index_iter_rev0<Q: Query<T> + Clone + Send + 'static>(
        &self,
        secrets: Secrets,
        query: Q,
        index: Arc<Index<T>>,
    ) -> BoxedIter<'static, Result<Arc<Index<T>>>> {
        IndexIter::new_rev(self.clone(), secrets, query, index).boxed()
    }

    pub(crate) fn dump_graph0<S>(
        &self,
        stream: &Secrets,
        parent_id: Option<usize>,
        next_id: usize,
        index: &Index<T>,
        f: impl Fn((usize, &NodeInfo<T>)) -> S + Clone,
    ) -> Result<(GraphEdges, GraphNodes<S>)> {
        let mut edges = vec![];
        let mut nodes: BTreeMap<usize, S> = Default::default();

        let node = self.load_node(stream, index)?;
        if let Some(p) = parent_id {
            edges.push((p, next_id));
        }
        nodes.insert(next_id, f((next_id, &node)));
        if let NodeInfo::Branch(_, branch) = node {
            let mut cur = next_id;
            for x in branch.children.iter() {
                let (mut e, mut n) =
                    self.dump_graph0(stream, Some(next_id), cur + 1, &x, f.clone())?;
                cur += n.len();
                edges.append(&mut e);
                nodes.append(&mut n);
            }
        }

        Ok((edges, nodes))
    }

    /// sealed roots of the tree
    pub fn roots(&self, tree: &Tree<T>) -> Result<Vec<Index<T>>> {
        match &tree.root {
            Some(index) => self.roots_impl(tree.state.secrets(), index),
            None => Ok(Vec::new()),
        }
    }

    /// leftmost branches of the tree as separate trees
    pub fn left_roots(&self, tree: &Tree<T>) -> Result<Vec<Tree<T>>> {
        Ok(if let Some(index) = tree.as_index_ref() {
            self.left_roots0(tree.state.secrets(), index)?
                .into_iter()
                .map(|x| Tree::new_from_index(Some(x), tree.state.clone()))
                .collect()
        } else {
            Vec::new()
        })
    }

    /// leftmost branches of the tree as separate trees
    fn left_roots0(&self, stream: &Secrets, index: &Index<T>) -> Result<Vec<Index<T>>> {
        let mut result = if let Index::Branch(branch) = index {
            if let Some(branch) = self.load_branch_cached(stream, branch)? {
                self.left_roots0(stream, branch.first_child())?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        result.push(index.clone());
        Ok(result)
    }

    pub fn check_invariants(&self, tree: &Tree<T>) -> Result<Vec<String>> {
        let mut msgs = Vec::new();
        if let Some(root) = &tree.root {
            let mut level = i32::max_value();
            self.check_invariants0(&tree.state, root, &mut level, &mut msgs)?;
        }
        Ok(msgs)
    }

    pub fn is_packed(&self, tree: &Tree<T>) -> Result<bool> {
        if let Some(root) = &tree.root {
            self.is_packed0(tree.state.secrets(), &root)
        } else {
            Ok(true)
        }
    }

    pub fn assert_invariants(&self, tree: &Tree<T>) -> Result<()> {
        let msgs = self.check_invariants(tree)?;
        if !msgs.is_empty() {
            let invariants = msgs.join(",");
            for msg in msgs {
                error!("Invariant failed: {}", msg);
            }
            panic!("assert_invariants failed {}", invariants);
        }
        Ok(())
    }

    pub fn stream_filtered(
        &self,
        tree: &Tree<T>,
        query: impl Query<T> + Clone + 'static,
    ) -> impl Stream<Item = Result<(u64, T::Key, V)>> + 'static {
        match &tree.root {
            Some(index) => self
                .stream_filtered0(tree.state.secrets().clone(), query, index.clone())
                .left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    /// Returns an iterator yielding all indexes that have values matching the
    /// provided query.
    pub fn iter_index(
        &self,
        tree: &Tree<T>,
        query: impl Query<T> + Clone + 'static,
    ) -> impl Iterator<Item = Result<Arc<Index<T>>>> + 'static {
        match &tree.root {
            Some(index) => self
                .index_iter0(tree.state.secrets().clone(), query, index.clone())
                .boxed()
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    /// Returns an iterator yielding all indexes that have values matching the
    /// provided query in reverse order.
    pub fn iter_index_reverse(
        &self,
        tree: &Tree<T>,
        query: impl Query<T> + Clone + 'static,
    ) -> impl Iterator<Item = Result<Arc<Index<T>>>> + 'static {
        match &tree.root {
            Some(index) => self
                .index_iter_rev0(tree.state.secrets().clone(), query, index.clone())
                .boxed()
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn iter_filtered(
        &self,
        tree: &Tree<T>,
        query: impl Query<T> + Clone + 'static,
    ) -> impl Iterator<Item = Result<(u64, T::Key, V)>> + 'static {
        match &tree.root {
            Some(index) => self
                .iter_filtered0(tree.state.secrets().clone(), query, index.clone())
                .boxed()
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn iter_filtered_reverse(
        &self,
        tree: &Tree<T>,
        query: impl Query<T> + Clone + 'static,
    ) -> impl Iterator<Item = Result<(u64, T::Key, V)>> + 'static {
        match &tree.root {
            Some(index) => self
                .iter_filtered_reverse0(tree.state.secrets().clone(), query, index.clone())
                .boxed()
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn iter_from(
        &self,
        tree: &Tree<T>,
    ) -> impl Iterator<Item = Result<(u64, T::Key, V)>> + 'static {
        match &tree.root {
            Some(index) => self
                .iter_filtered0(
                    tree.state.secrets().clone(),
                    crate::query::AllQuery,
                    index.clone(),
                )
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn iter_filtered_chunked<Q, E, F>(
        &self,
        tree: &Tree<T>,
        query: Q,
        mk_extra: &'static F,
    ) -> impl Iterator<Item = Result<FilteredChunk<T, V, E>>> + 'static
    where
        Q: Query<T> + Send + Clone + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    {
        match &tree.root {
            Some(index) => self
                .traverse0(tree.state.secrets().clone(), query, index.clone(), mk_extra)
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn iter_filtered_chunked_reverse<Q, E, F>(
        &self,
        tree: &Tree<T>,
        query: Q,
        mk_extra: &'static F,
    ) -> impl Iterator<Item = Result<FilteredChunk<T, V, E>>> + 'static
    where
        Q: Query<T> + Send + Clone + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    {
        match &tree.root {
            Some(index) => self
                .traverse_rev0(tree.state.secrets().clone(), query, index.clone(), mk_extra)
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn stream_filtered_chunked<Q, E, F>(
        &self,
        tree: &Tree<T>,
        query: Q,
        mk_extra: &'static F,
    ) -> impl Stream<Item = Result<FilteredChunk<T, V, E>>> + 'static
    where
        Q: Query<T> + Send + Clone + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    {
        match &tree.root {
            Some(index) => self
                .stream_filtered_chunked0(
                    tree.state.secrets().clone(),
                    query,
                    index.clone(),
                    mk_extra,
                )
                .left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    pub fn stream_filtered_chunked_reverse<Q, E, F>(
        &self,
        tree: &Tree<T>,
        query: Q,
        mk_extra: &'static F,
    ) -> impl Stream<Item = Result<FilteredChunk<T, V, E>>> + 'static
    where
        Q: Query<T> + Send + Clone + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    {
        match &tree.root {
            Some(index) => self
                .stream_filtered_chunked_reverse0(
                    tree.state.secrets(),
                    query,
                    index.clone(),
                    mk_extra,
                )
                .left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    /// element at index
    ///
    /// returns Ok(None) when offset is larger than count, or when hitting a purged
    /// part of the tree. Returns an error when part of the tree should be there, but could
    /// not be read.
    pub fn get(&self, tree: &Tree<T>, offset: u64) -> Result<Option<(T::Key, V)>> {
        Ok(match &tree.root {
            Some(index) => self.get0(tree.state.secrets(), index, offset)?,
            None => None,
        })
    }

    /// Collects all elements from a stream. Might produce an OOM for large streams.
    #[allow(clippy::type_complexity)]
    pub fn collect(&self, tree: &Tree<T>) -> Result<Vec<Option<(T::Key, V)>>> {
        self.collect_from(tree, 0)
    }

    /// Collects all elements from the given offset. Might produce an OOM for large streams.
    #[allow(clippy::type_complexity)]
    pub fn collect_from(&self, tree: &Tree<T>, offset: u64) -> Result<Vec<Option<(T::Key, V)>>> {
        let mut res = Vec::new();
        if let Some(index) = &tree.root {
            self.collect0(tree.state.secrets(), index, offset, &mut res)?;
        }
        Ok(res)
    }
}

impl<
        T: TreeTypes,
        V: DagCbor + Clone + Send + Sync + Debug + 'static,
        R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
        W: BlockWriter<T::Link> + 'static,
    > Transaction<T, V, R, W>
{
    pub fn tree_from_roots(
        &self,
        mut roots: Vec<Index<T>>,
        stream: &mut StreamBuilderState,
    ) -> Result<Tree<T>> {
        assert!(roots.iter().all(|x| x.sealed()));
        assert!(is_sorted(roots.iter().map(|x| x.level()).rev()));
        while roots.len() > 1 {
            self.simplify_roots(&mut roots, 0, stream)?;
        }
        Ok(Tree::new_from_index(roots.pop(), stream.clone()))
    }

    /// Packs the tree to the left.
    ///
    /// For an already packed tree, this is a noop.
    /// Otherwise, all packed subtrees will be reused without touching them.
    /// Likewise, sealed subtrees or leafs will be reused if possible.
    ///
    /// ![packing illustration](https://ipfs.io/ipfs/QmaEDTjHSdCKyGQ3cFMCf73kE67NvffLA5agquLW5qSEVn/packing.jpg)
    pub fn pack(&self, tree: &Tree<T>) -> Result<Tree<T>> {
        let roots = self.roots(tree)?;
        let mut state = tree.state.clone();
        let filled = self.tree_from_roots(roots, &mut state)?;
        let remainder: Vec<_> = self
            .collect_from(tree, filled.count())?
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| anyhow::anyhow!("found purged data"))?;
        let extended = self.extend(&filled, remainder)?;
        Ok(extended)
    }

    /// append a single element. This is just a shortcut for extend.
    pub fn push(&mut self, tree: &mut Tree<T>, key: T::Key, value: V) -> Result<Tree<T>> {
        self.extend(tree, Some((key, value)))
    }

    /// extend the node with the given iterator of key/value pairs
    ///
    /// ![extend illustration](https://ipfs.io/ipfs/QmaEDTjHSdCKyGQ3cFMCf73kE67NvffLA5agquLW5qSEVn/extend.jpg)
    pub fn extend<I>(&self, tree: &Tree<T>, from: I) -> Result<Tree<T>>
    where
        I: IntoIterator<Item = (T::Key, V)>,
        I::IntoIter: Send,
    {
        let mut from = from.into_iter().peekable();
        if from.peek().is_none() {
            // nothing to do
            return Ok(tree.clone());
        }
        let mut state = tree.state.clone();
        let index = self.extend_above(
            tree.as_index_ref(),
            u32::max_value(),
            from.by_ref(),
            &mut state,
        )?;
        Ok(Tree::new_from_index(Some(index), state))
    }

    /// extend the node with the given iterator of key/value pairs
    ///
    /// This variant will not pack the tree, but just create a new tree from the new values and join it
    /// with the previous tree via an unpacked branch node. Essentially this will produce a degenerate tree
    /// that resembles a linked list.
    ///
    /// To pack a tree, use the pack method.
    ///
    /// ![extend_unpacked illustration](https://ipfs.io/ipfs/QmaEDTjHSdCKyGQ3cFMCf73kE67NvffLA5agquLW5qSEVn/extend_unpacked.jpg)
    pub fn extend_unpacked<I>(&self, tree: &Tree<T>, from: I) -> Result<Tree<T>>
    where
        I: IntoIterator<Item = (T::Key, V)>,
        I::IntoIter: Send,
    {
        let mut state = tree.state.clone();
        let index = self.extend_unpacked0(tree.as_index_ref(), from, &mut state)?;
        Ok(Tree::new_from_index(index, state))
    }

    /// Retain just data matching the query
    ///
    /// this is done as best effort and will not be precise. E.g. if a chunk of data contains
    /// just a tiny bit that needs to be retained, the entire chunk will be retained.
    ///
    /// from this follows that this is not a suitable method if you want to ensure that the
    /// non-matching data is completely gone.
    ///
    /// note that offsets will not be affected by this. Also, unsealed nodes will not be forgotten
    /// even if they do not match the query.
    pub fn retain<'a, Q: Query<T> + Send + Sync>(
        &'a self,
        tree: &Tree<T>,
        query: &'a Q,
    ) -> Result<Tree<T>> {
        Ok(if let Some(index) = &tree.root {
            let mut level: i32 = i32::max_value();
            let mut state = tree.state.clone();
            let res = self.retain0(0, query, index, &mut level, &mut state)?;
            Tree::new_from_index(Some(res), state)
        } else {
            tree.clone()
        })
    }

    /// repair a tree by purging parts of the tree that can not be resolved.
    ///
    /// produces a report of links that could not be resolved.
    ///
    /// Note that this is an emergency measure to recover data if the tree is not completely
    /// available. It might result in a degenerate tree that can no longer be safely added to,
    /// especially if there are repaired blocks in the non-packed part.
    pub fn repair(&self, tree: &Tree<T>) -> Result<(Tree<T>, Vec<String>)> {
        let mut report = Vec::new();
        Ok(if let Some(index) = &tree.root {
            let mut level: i32 = i32::max_value();
            let mut state = tree.state.clone();
            let repaired = self.repair0(index, &mut report, &mut level, &mut state)?;
            (Tree::new_from_index(Some(repaired), state), report)
        } else {
            (tree.clone(), report)
        })
    }
}

impl<T: TreeTypes> Tree<T> {
    pub fn state(&self) -> StreamBuilderState {
        self.state.clone()
    }

    pub fn new(config: Config, secrets: Secrets) -> Self {
        let state = StreamBuilderState::new(0, secrets, config);
        Self::new_from_index(None, state)
    }

    pub(crate) fn new_from_index(root: Option<Index<T>>, state: StreamBuilderState) -> Self {
        Self {
            root: root.map(Arc::new),
            state,
        }
    }

    pub fn snapshot(&self) -> Snapshot<T> {
        Snapshot::new(
            self.root.clone(),
            self.state.secrets().clone(),
            self.state.offset.value,
        )
    }

    pub fn link(&self) -> Option<T::Link> {
        self.root.as_ref().and_then(|r| *r.link())
    }

    pub fn as_index_ref(&self) -> Option<&Index<T>> {
        self.root.as_ref().map(|arc| arc.as_ref())
    }

    pub fn level(&self) -> i32 {
        self.root.as_ref().map(|x| x.level() as i32).unwrap_or(-1)
    }

    pub fn debug() -> Self {
        let state = StreamBuilderState::new(0, Secrets::default(), Config::debug());
        Self::new_from_index(None, state)
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
    pub fn root(&self) -> Option<&T::Link> {
        self.root.as_ref().and_then(|index| index.link().as_ref())
    }
}

fn is_sorted<T: Ord>(iter: impl Iterator<Item = T>) -> bool {
    iter.collect::<Vec<_>>().windows(2).all(|x| x[0] <= x[1])
}
