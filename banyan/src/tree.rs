//! creation and traversal of banyan trees
use super::index::*;
use crate::{
    forest::{Config, FilteredChunk, Forest, IndexIter, Secrets, Transaction, TreeIter, TreeTypes},
    store::BlockWriter,
};
use crate::{query::Query, store::ReadOnlyStore, util::IterExt, StreamBuilder, StreamBuilderState};
use anyhow::Result;
use core::fmt;
use futures::prelude::*;
use libipld::cbor::DagCbor;
use std::{collections::BTreeMap, fmt::Debug, iter, usize};
use tracing::*;

#[derive(Clone)]
pub struct Tree<T: TreeTypes>(Option<(Index<T>, Secrets, u64)>);

impl<T: TreeTypes> Tree<T> {
    pub(crate) fn new(root: Index<T>, secrets: Secrets, offset: u64) -> Self {
        Self(Some((root, secrets, offset)))
    }

    pub(crate) fn into_inner(self) -> Option<(Index<T>, Secrets, u64)> {
        self.0
    }

    pub fn as_index_ref(&self) -> Option<&Index<T>> {
        self.0.as_ref().map(|(r, _, _)| r)
    }

    pub fn link(&self) -> Option<T::Link> {
        self.0.as_ref().and_then(|(r, _, _)| *r.link())
    }

    pub fn level(&self) -> i32 {
        self.0
            .as_ref()
            .map(|(r, _, _)| r.level() as i32)
            .unwrap_or(-1)
    }

    /// true for an empty tree
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    /// number of elements in the tree
    pub fn count(&self) -> u64 {
        self.0
            .as_ref()
            .map(|(r, _, _)| r.count())
            .unwrap_or_default()
    }

    /// root of a non-empty tree
    pub fn root(&self) -> Option<&T::Link> {
        self.0.as_ref().and_then(|(r, _, _)| r.link().as_ref())
    }

    /// root of a non-empty tree
    pub fn index(&self) -> Option<&Index<T>> {
        self.0.as_ref().map(|(r, _, _)| r)
    }

    pub fn secrets(&self) -> Option<&Secrets> {
        self.0.as_ref().map(|(_, secrets, _)| secrets)
    }
}

impl<T: TreeTypes> Default for Tree<T> {
    fn default() -> Self {
        Self(None)
    }
}

impl<T: TreeTypes> fmt::Debug for Tree<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some((root, ..)) => f
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
        match &self.0 {
            Some((root, ..)) => write!(f, "{:?}", root.link(),),
            None => write!(f, "empty tree"),
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
    pub fn load_stream_builder(
        &self,
        secrets: Secrets,
        config: Config,
        link: T::Link,
    ) -> Result<StreamBuilder<T>> {
        let (index, byte_range) = self.load_branch_from_link(
            &secrets,
            |items, level| config.branch_sealed(items, level),
            link,
        )?;
        let state = StreamBuilderState::new(byte_range.end, secrets, config);
        Ok(StreamBuilder::new_from_index(Some(index), state))
    }

    pub fn load_tree(&self, secrets: Secrets, link: T::Link) -> Result<Tree<T>> {
        // we pass in a predicate that makes the nodes sealed, since we don't care
        let (index, byte_range) = self.load_branch_from_link(&secrets, |_, _| true, link)?;
        // store the offset with the snapshot. Snapshots are immutable, so this won't change.
        Ok(Tree::new(index, secrets, byte_range.end))
    }

    /// dumps the tree structure
    pub fn dump(&self, tree: &Tree<T>) -> Result<()> {
        match &tree.0 {
            Some((index, secrets, _)) => self.dump0(secrets, index, ""),
            None => Ok(()),
        }
    }

    /// dumps the tree structure
    pub fn dump_graph<S>(
        &self,
        tree: &Tree<T>,
        f: impl Fn((usize, &NodeInfo<T>)) -> S + Clone,
    ) -> Result<(GraphEdges, GraphNodes<S>)> {
        match &tree.0 {
            Some((index, secrets, _)) => self.dump_graph0(secrets, None, 0, index, f),
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
        index: Index<T>,
        mk_extra: &'static F,
    ) -> impl Iterator<Item = Result<FilteredChunk<(u64, T::Key, V), E>>> {
        TreeIter::new(self.clone(), secrets, query, index, mk_extra)
    }

    pub(crate) fn traverse_rev0<
        Q: Query<T> + Clone + Send + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    >(
        &self,
        secrets: Secrets,
        query: Q,
        index: Index<T>,
        mk_extra: &'static F,
    ) -> impl Iterator<Item = Result<FilteredChunk<(u64, T::Key, V), E>>> {
        TreeIter::new_rev(self.clone(), secrets, query, index, mk_extra)
    }

    fn index_iter0<Q: Query<T> + Clone + Send + 'static>(
        &self,
        secrets: Secrets,
        query: Q,
        index: Index<T>,
    ) -> impl Iterator<Item = Result<Index<T>>> {
        IndexIter::new(self.clone(), secrets, query, index)
    }

    fn index_iter_rev0<Q: Query<T> + Clone + Send + 'static>(
        &self,
        secrets: Secrets,
        query: Q,
        index: Index<T>,
    ) -> impl Iterator<Item = Result<Index<T>>> {
        IndexIter::new_rev(self.clone(), secrets, query, index)
    }

    pub(crate) fn dump_graph0<S>(
        &self,
        secrets: &Secrets,
        parent_id: Option<usize>,
        next_id: usize,
        index: &Index<T>,
        f: impl Fn((usize, &NodeInfo<T>)) -> S + Clone,
    ) -> Result<(GraphEdges, GraphNodes<S>)> {
        let mut edges = vec![];
        let mut nodes: BTreeMap<usize, S> = Default::default();

        let node = self.load_node(secrets, index)?;
        if let Some(p) = parent_id {
            edges.push((p, next_id));
        }
        nodes.insert(next_id, f((next_id, &node)));
        if let NodeInfo::Branch(_, branch) = node {
            let mut cur = next_id;
            for x in branch.children.iter() {
                let (mut e, mut n) =
                    self.dump_graph0(secrets, Some(next_id), cur + 1, &x, f.clone())?;
                cur += n.len();
                edges.append(&mut e);
                nodes.append(&mut n);
            }
        }

        Ok((edges, nodes))
    }

    /// sealed roots of the tree
    pub fn roots(&self, tree: &StreamBuilder<T>) -> Result<Vec<Index<T>>> {
        match tree.index() {
            Some(index) => self.roots_impl(tree.state().secrets(), index),
            None => Ok(Vec::new()),
        }
    }

    /// leftmost branches of the tree as separate trees
    pub fn left_roots(&self, tree: &Tree<T>) -> Result<Vec<Tree<T>>> {
        Ok(if let Some((index, secrets, _)) = &tree.0 {
            self.left_roots0(secrets, index)?
                .into_iter()
                .map(|x| Tree::new(x, secrets.clone(), u64::max_value()))
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

    pub fn check_invariants(&self, tree: &StreamBuilder<T>) -> Result<Vec<String>> {
        let mut msgs = Vec::new();
        if let Some(root) = tree.index() {
            let mut level = i32::max_value();
            self.check_invariants0(
                tree.state().secrets(),
                tree.state().config(),
                root,
                &mut level,
                &mut msgs,
            )?;
        }
        Ok(msgs)
    }

    pub fn is_packed(&self, tree: &Tree<T>) -> Result<bool> {
        if let Some((root, secrets, _)) = &tree.0 {
            self.is_packed0(secrets, &root)
        } else {
            Ok(true)
        }
    }

    pub fn assert_invariants(&self, tree: &StreamBuilder<T>) -> Result<()> {
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
        match &tree.0 {
            Some((index, secrets, _)) => self
                .stream_filtered0(secrets.clone(), query, index.clone())
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
    ) -> impl Iterator<Item = Result<Index<T>>> + 'static {
        match &tree.0 {
            Some((index, secrets, _)) => self
                .index_iter0(secrets.clone(), query, index.clone())
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
    ) -> impl Iterator<Item = Result<Index<T>>> + 'static {
        match &tree.0 {
            Some((index, secrets, _)) => self
                .index_iter_rev0(secrets.clone(), query, index.clone())
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn iter_filtered(
        &self,
        tree: &Tree<T>,
        query: impl Query<T> + Clone + 'static,
    ) -> impl Iterator<Item = Result<(u64, T::Key, V)>> + 'static {
        match &tree.0 {
            Some((index, secrets, _)) => self
                .iter_filtered0(secrets.clone(), query, index.clone())
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn iter_filtered_reverse(
        &self,
        tree: &Tree<T>,
        query: impl Query<T> + Clone + 'static,
    ) -> impl Iterator<Item = Result<(u64, T::Key, V)>> + 'static {
        match &tree.0 {
            Some((index, secrets, _)) => self
                .iter_filtered_reverse0(secrets.clone(), query, index.clone())
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn iter_from(
        &self,
        tree: &Tree<T>,
    ) -> impl Iterator<Item = Result<(u64, T::Key, V)>> + 'static {
        match &tree.0 {
            Some((index, secrets, _)) => self
                .iter_filtered0(secrets.clone(), crate::query::AllQuery, index.clone())
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn iter_filtered_chunked<Q, E, F>(
        &self,
        tree: &Tree<T>,
        query: Q,
        mk_extra: &'static F,
    ) -> impl Iterator<Item = Result<FilteredChunk<(u64, T::Key, V), E>>> + 'static
    where
        Q: Query<T> + Send + Clone + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    {
        match &tree.0 {
            Some((index, secrets, _)) => self
                .traverse0(secrets.clone(), query, index.clone(), mk_extra)
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn iter_filtered_chunked_reverse<Q, E, F>(
        &self,
        tree: &Tree<T>,
        query: Q,
        mk_extra: &'static F,
    ) -> impl Iterator<Item = Result<FilteredChunk<(u64, T::Key, V), E>>> + 'static
    where
        Q: Query<T> + Send + Clone + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    {
        match &tree.0 {
            Some((index, secrets, _)) => self
                .traverse_rev0(secrets.clone(), query, index.clone(), mk_extra)
                .left_iter(),
            None => iter::empty().right_iter(),
        }
    }

    pub fn stream_filtered_chunked<Q, E, F>(
        &self,
        tree: &Tree<T>,
        query: Q,
        mk_extra: &'static F,
    ) -> impl Stream<Item = Result<FilteredChunk<(u64, T::Key, V), E>>> + 'static
    where
        Q: Query<T> + Send + Clone + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    {
        match &tree.0 {
            Some((index, secrets, _)) => self
                .stream_filtered_chunked0(secrets.clone(), query, index.clone(), mk_extra)
                .left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    pub fn stream_filtered_chunked_reverse<Q, E, F>(
        &self,
        tree: &Tree<T>,
        query: Q,
        mk_extra: &'static F,
    ) -> impl Stream<Item = Result<FilteredChunk<(u64, T::Key, V), E>>> + 'static
    where
        Q: Query<T> + Send + Clone + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    {
        match &tree.0 {
            Some((index, secrets, _)) => self
                .stream_filtered_chunked_reverse0(secrets, query, index.clone(), mk_extra)
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
        Ok(match &tree.0 {
            Some((index, secrets, _)) => self.get0(secrets, index, offset)?,
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
        if let Some((index, secrets, _)) = &tree.0 {
            self.collect0(secrets, index, offset, &mut res)?;
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
    pub(crate) fn tree_from_roots(
        &self,
        mut roots: Vec<Index<T>>,
        stream: &mut StreamBuilder<T>,
    ) -> Result<()> {
        assert!(roots.iter().all(|x| x.sealed()));
        assert!(is_sorted(roots.iter().map(|x| x.level()).rev()));
        while roots.len() > 1 {
            self.simplify_roots(&mut roots, 0, stream.state_mut())?;
        }
        stream.set_index(roots.pop());
        Ok(())
    }

    /// Packs the tree to the left.
    ///
    /// For an already packed tree, this is a noop.
    /// Otherwise, all packed subtrees will be reused without touching them.
    /// Likewise, sealed subtrees or leafs will be reused if possible.
    ///
    /// ![packing illustration](https://ipfs.io/ipfs/QmaEDTjHSdCKyGQ3cFMCf73kE67NvffLA5agquLW5qSEVn/packing.jpg)
    pub fn pack(&self, tree: &mut StreamBuilder<T>) -> Result<()> {
        let initial = tree.snapshot();
        let roots = self.roots(tree)?;
        self.tree_from_roots(roots, tree)?;
        let remainder: Vec<_> = self
            .collect_from(&initial, tree.count())?
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| anyhow::anyhow!("found purged data"))?;
        self.extend(tree, remainder)?;
        Ok(())
    }

    /// append a single element. This is just a shortcut for extend.
    pub fn push(&mut self, tree: &mut StreamBuilder<T>, key: T::Key, value: V) -> Result<()> {
        self.extend(tree, Some((key, value)))
    }

    /// extend the node with the given iterator of key/value pairs
    ///
    /// ![extend illustration](https://ipfs.io/ipfs/QmaEDTjHSdCKyGQ3cFMCf73kE67NvffLA5agquLW5qSEVn/extend.jpg)
    pub fn extend<I>(&self, tree: &mut StreamBuilder<T>, from: I) -> Result<()>
    where
        I: IntoIterator<Item = (T::Key, V)>,
        I::IntoIter: Send,
    {
        let mut from = from.into_iter().peekable();
        if from.peek().is_none() {
            // nothing to do
            return Ok(());
        }
        let index = tree.as_index_ref().cloned();
        let index = self.extend_above(
            index.as_ref(),
            u32::max_value(),
            from.by_ref(),
            tree.state_mut(),
        )?;
        tree.set_index(Some(index));
        Ok(())
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
    pub fn extend_unpacked<I>(&self, tree: &mut StreamBuilder<T>, from: I) -> Result<()>
    where
        I: IntoIterator<Item = (T::Key, V)>,
        I::IntoIter: Send,
    {
        let index = tree.as_index_ref().cloned();
        let index = self.extend_unpacked0(index.as_ref(), from, tree.state_mut())?;
        tree.set_index(index);
        Ok(())
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
        tree: &mut StreamBuilder<T>,
        query: &'a Q,
    ) -> Result<()> {
        let index = tree.index().cloned();
        if let Some(index) = index {
            let mut level: i32 = i32::max_value();
            let index = self.retain0(0, query, &index, &mut level, tree.state_mut())?;
            tree.set_index(Some(index));
        }
        Ok(())
    }

    /// repair a tree by purging parts of the tree that can not be resolved.
    ///
    /// produces a report of links that could not be resolved.
    ///
    /// Note that this is an emergency measure to recover data if the tree is not completely
    /// available. It might result in a degenerate tree that can no longer be safely added to,
    /// especially if there are repaired blocks in the non-packed part.
    pub fn repair(&self, tree: &mut StreamBuilder<T>) -> Result<Vec<String>> {
        let mut report = Vec::new();
        let index = tree.index().cloned();
        if let Some(index) = index {
            let mut level: i32 = i32::max_value();
            let repaired = self.repair0(&index, &mut report, &mut level, tree.state_mut())?;
            tree.set_index(Some(repaired));
        }
        Ok(report)
    }
}

fn is_sorted<T: Ord>(iter: impl Iterator<Item = T>) -> bool {
    iter.collect::<Vec<_>>().windows(2).all(|x| x[0] <= x[1])
}
