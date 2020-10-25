//! creation and traversal of banyan trees
use super::index::*;
use crate::{
    forest::{FilteredChunk, Forest, Transaction, TreeTypes},
    store::BlockWriter,
};
use crate::{
    query::{AllQuery, OffsetRangeQuery, Query},
    store::ReadOnlyStore,
};
use anyhow::Result;
use futures::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::{fmt, iter::FromIterator, marker::PhantomData};
use tracing::*;

/// A tree. This is mostly an user friendly handle.
///
/// Most of the logic except for handling the empty case is implemented in the forest
pub struct Tree<T: TreeTypes, V> {
    root: Option<Index<T>>,
    _t: PhantomData<V>,
}

impl<T: TreeTypes, V> fmt::Debug for Tree<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => write!(
                f,
                "Tree(root={:?},key_bytes={},value_bytes={})",
                root.link(),
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
            Some(root) => write!(f, "{:?}", root.link(),),
            None => write!(f, "empty tree"),
        }
    }
}

impl<V, T: TreeTypes> Clone for Tree<T, V> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            _t: PhantomData,
        }
    }
}

impl<
        T: TreeTypes + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
        R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
    > Forest<T, V, R>
{
    pub async fn load_tree(&self, link: T::Link) -> Result<Tree<T, V>> {
        Ok(Tree {
            root: Some(self.load_branch_from_link(link).await?),
            _t: PhantomData,
        })
    }

    /// dumps the tree structure
    pub async fn dump(&self, tree: &Tree<T, V>) -> Result<()> {
        match &tree.root {
            Some(index) => self.dump0(index, "").await,
            None => Ok(()),
        }
    }

    /// sealed roots of the tree
    pub async fn roots(&self, tree: &Tree<T, V>) -> Result<Vec<Index<T>>> {
        match &tree.root {
            Some(index) => self.roots_impl(index).await,
            None => Ok(Vec::new()),
        }
    }

    pub async fn check_invariants(&self, tree: &Tree<T, V>) -> Result<Vec<String>> {
        let mut msgs = Vec::new();
        if let Some(root) = &tree.root {
            if root.level() == 0 {
                msgs.push("tree should not have a leaf as direct child.".into());
            }
            let mut level = i32::max_value();
            self.check_invariants0(&root, &mut level, &mut msgs).await?;
        }
        Ok(msgs)
    }

    pub async fn is_packed(&self, tree: &Tree<T, V>) -> Result<bool> {
        if let Some(root) = &tree.root {
            self.is_packed0(&root).await
        } else {
            Ok(true)
        }
    }

    pub async fn assert_invariants(&self, tree: &Tree<T, V>) -> Result<()> {
        let msgs = self.check_invariants(tree).await?;
        if !msgs.is_empty() {
            for msg in msgs {
                error!("Invariant failed: {}", msg);
            }
            panic!("assert_invariants failed");
        }
        Ok(())
    }

    /// Collects all elements from a stream. Might produce an OOM for large streams.
    pub async fn collect<B: FromIterator<(T::Key, V)>>(&self, tree: &Tree<T, V>) -> Result<B> {
        let query = AllQuery;
        let items: Vec<Result<(T::Key, V)>> = self
            .stream_filtered(tree, query)
            .map_ok(|(_, k, v)| (k, v))
            .collect::<Vec<_>>()
            .await;
        items.into_iter().collect::<Result<_>>()
    }

    /// Collects all elements from the given offset. Might produce an OOM for large streams.
    pub async fn collect_from<B: FromIterator<(T::Key, V)>>(
        &self,
        tree: &Tree<T, V>,
        offset: u64,
    ) -> Result<B> {
        let query = OffsetRangeQuery::from(offset..);
        let items: Vec<Result<(T::Key, V)>> = self
            .stream_filtered(tree, query)
            .map_ok(|(_, k, v)| (k, v))
            .collect::<Vec<_>>()
            .await;
        items.into_iter().collect::<Result<_>>()
    }

    pub fn stream_filtered(
        &self,
        tree: &Tree<T, V>,
        query: impl Query<T> + Clone + 'static,
    ) -> impl Stream<Item = Result<(u64, T::Key, V)>> + 'static {
        match &tree.root {
            Some(index) => self
                .clone()
                .stream_filtered0(0, query, index.clone())
                .left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    pub fn stream_filtered_chunked<Q, E, F>(
        &self,
        tree: &Tree<T, V>,
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
                .clone()
                .stream_filtered_chunked0(0, query, index.clone(), mk_extra)
                .left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    pub fn stream_filtered_chunked_reverse<Q, E, F>(
        &self,
        tree: &Tree<T, V>,
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
                .clone()
                .stream_filtered_chunked_reverse0(0, query, index.clone(), mk_extra)
                .left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    /// element at index
    pub async fn get(&self, tree: &Tree<T, V>, offset: u64) -> Result<Option<(T::Key, V)>> {
        Ok(match &tree.root {
            Some(index) => self.get0(index, offset).await?,
            None => None,
        })
    }
}

impl<
        T: TreeTypes + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
        R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
        W: BlockWriter<T::Link> + 'static,
    > Transaction<T, V, R, W>
{
    pub async fn tree_from_roots(&self, mut roots: Vec<Index<T>>) -> Result<Tree<T, V>> {
        assert!(roots.iter().all(|x| x.sealed()));
        assert!(is_sorted(roots.iter().map(|x| x.level()).rev()));
        while roots.len() > 1 {
            self.simplify_roots(&mut roots, 0).await?;
        }
        Ok(Tree::new(roots.pop()))
    }

    /// Packs the tree to the left.
    ///
    /// For an already packed tree, this is a noop.
    /// Otherwise, all packed subtrees will be reused without touching them.
    /// Likewise, sealed subtrees or leafs will be reused if possible.
    ///
    /// ![packing illustration](https://ipfs.io/ipfs/QmaEDTjHSdCKyGQ3cFMCf73kE67NvffLA5agquLW5qSEVn/packing.jpg)
    pub async fn pack(&self, tree: &Tree<T, V>) -> Result<Tree<T, V>> {
        let roots = self.roots(tree).await?;
        let filled = self.tree_from_roots(roots).await?;
        let remainder: Vec<_> = self.collect_from(tree, filled.count()).await?;
        let extended = self.extend(&filled, remainder).await?;
        Ok(extended)
    }

    /// append a single element. This is just a shortcut for extend.
    pub async fn push(
        &mut self,
        tree: &mut Tree<T, V>,
        key: T::Key,
        value: V,
    ) -> Result<Tree<T, V>> {
        self.extend(tree, Some((key, value))).await
    }

    /// extend the node with the given iterator of key/value pairs
    ///    
    /// ![extend illustration](https://ipfs.io/ipfs/QmaEDTjHSdCKyGQ3cFMCf73kE67NvffLA5agquLW5qSEVn/extend.jpg)
    pub async fn extend<I>(&self, tree: &Tree<T, V>, from: I) -> Result<Tree<T, V>>
    where
        I: IntoIterator<Item = (T::Key, V)>,
        I::IntoIter: Send,
    {
        let mut from = from.into_iter().peekable();
        if from.peek().is_none() {
            // nothing to do
            return Ok(tree.clone());
        }
        Ok(Tree::new(Some(
            self.extend_above(tree.root.as_ref(), u32::max_value(), from.by_ref())
                .await?,
        )))
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
    pub async fn extend_unpacked<I>(&self, tree: &Tree<T, V>, from: I) -> Result<Tree<T, V>>
    where
        I: IntoIterator<Item = (T::Key, V)>,
        I::IntoIter: Send,
    {
        Ok(Tree::new(
            self.extend_unpacked0(tree.root.as_ref(), from).await?,
        ))
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
    pub async fn retain<'a, Q: Query<T> + Send + Sync>(
        &'a self,
        tree: &Tree<T, V>,
        query: &'a Q,
    ) -> Result<Tree<T, V>> {
        Ok(if let Some(index) = &tree.root {
            let mut level: i32 = i32::max_value();
            let res = self.retain0(0, query, index, &mut level).await?;
            Tree::new(Some(res))
        } else {
            Tree::empty()
        })
    }

    /// repair a tree by purging parts of the tree that can not be resolved.
    ///
    /// produces a report of links that could not be resolved.
    ///
    /// Note that this is an emergency measure to recover data if the tree is not completely
    /// available. It might result in a degenerate tree that can no longer be safely added to,
    /// especially if there are repaired blocks in the non-packed part.
    pub async fn repair<'a>(&self, tree: &Tree<T, V>) -> Result<(Tree<T, V>, Vec<String>)> {
        let mut report = Vec::new();
        Ok(if let Some(index) = &tree.root {
            let mut level: i32 = i32::max_value();
            (
                Tree::new(Some(self.repair0(index, &mut report, &mut level).await?)),
                report,
            )
        } else {
            (Tree::empty(), report)
        })
    }
}

impl<
        V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
        T: TreeTypes + 'static,
    > Tree<T, V>
{
    pub(crate) fn new(root: Option<Index<T>>) -> Self {
        Self {
            root,
            _t: PhantomData,
        }
    }

    pub fn link(&self) -> Option<T::Link> {
        self.root.as_ref().and_then(|r| *r.link())
    }

    pub fn level(&self) -> i32 {
        self.root.as_ref().map(|x| x.level() as i32).unwrap_or(-1)
    }

    pub fn empty() -> Self {
        Self::new(None)
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
