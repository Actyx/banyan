//! creation and traversal of banyan trees
use super::index::*;
use crate::forest::{CreateMode, FilteredChunk, Forest, TreeTypes};
use crate::query::{AllQuery, OffsetRangeQuery, Query};
use anyhow::Result;
use futures::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::{fmt, iter::FromIterator, marker::PhantomData, sync::Arc};
use tracing::*;

/// A tree. This is mostly an user friendly handle.
///
/// Most of the logic except for handling the empty case is implemented in the forest
pub struct Tree<T: TreeTypes, V> {
    root: Option<Index<T>>,
    forest: Arc<Forest<T>>,
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
    pub async fn from_link(cid: T::Link, forest: Arc<Forest<T>>) -> Result<Self> {
        Ok(Self {
            root: Some(forest.load_branch_from_cid(cid).await?),
            forest,
            _t: PhantomData,
        })
    }
    pub fn link(&self) -> Option<T::Link> {
        self.root.as_ref().and_then(|r| r.link().clone())
    }
    pub fn level(&self) -> i32 {
        self.root.as_ref().map(|x| x.level() as i32).unwrap_or(-1)
    }
    pub async fn from_roots(forest: Arc<Forest<T>>, mut roots: Vec<Index<T>>) -> Result<Self> {
        assert!(roots.iter().all(|x| x.sealed()));
        assert!(is_sorted(roots.iter().map(|x| x.level()).rev()));
        while roots.len() > 1 {
            forest.simplify_roots(&mut roots, 0).await?;
        }
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

    pub async fn check_invariants(&self) -> Result<Vec<String>> {
        let mut msgs = Vec::new();
        if let Some(root) = &self.root {
            if root.level() == 0 {
                msgs.push("tree should not have a leaf as direct child.".into());
            }
            let mut level = i32::max_value();
            self.forest
                .check_invariants(&root, &mut level, &mut msgs)
                .await?;
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

    /// Collects all elements from a stream. Might produce an OOM for large streams.
    pub async fn collect<B: FromIterator<(T::Key, V)>>(&self) -> Result<B> {
        let query = AllQuery;
        let items: Vec<Result<(T::Key, V)>> = self
            .stream_filtered(&query)
            .map_ok(|(_, k, v)| (k, v))
            .collect::<Vec<_>>()
            .await;
        items.into_iter().collect::<Result<_>>()
    }

    /// Collects all elements from the given offset. Might produce an OOM for large streams.
    pub async fn collect_from<B: FromIterator<(T::Key, V)>>(&self, offset: u64) -> Result<B> {
        let query = OffsetRangeQuery::from(offset..);
        let items: Vec<Result<(T::Key, V)>> = self
            .stream_filtered(&query)
            .map_ok(|(_, k, v)| (k, v))
            .collect::<Vec<_>>()
            .await;
        items.into_iter().collect::<Result<_>>()
    }

    /// Packs the tree to the left.
    ///
    /// For an already packed tree, this is a noop.
    /// Otherwise, all packed subtrees will be reused without touching them.
    /// Likewise, sealed subtrees or leafs will be reused if possible.
    ///
    /// ![packing illustration](https://ipfs.io/ipfs/QmaEDTjHSdCKyGQ3cFMCf73kE67NvffLA5agquLW5qSEVn/packing.jpg)
    pub async fn pack(&mut self) -> Result<()> {
        let roots = self.roots().await?;
        let mut filled = Tree::<T, V>::from_roots(self.forest.clone(), roots).await?;
        let remainder: Vec<_> = self.collect_from(filled.count()).await?;
        filled.extend(remainder).await?;
        *self = filled;
        Ok(())
    }

    /// append a single element. This is just a shortcut for extend.
    pub async fn push(&mut self, key: T::Key, value: V) -> Result<()> {
        self.extend(Some((key, value))).await
    }

    /// extend the node with the given iterator of key/value pairs
    ///    
    /// ![extend illustration](https://ipfs.io/ipfs/QmaEDTjHSdCKyGQ3cFMCf73kE67NvffLA5agquLW5qSEVn/extend.jpg)
    pub async fn extend<I>(&mut self, from: I) -> Result<()>
    where
        I: IntoIterator<Item = (T::Key, V)>,
        I::IntoIter: Send,
    {
        let mut from = from.into_iter().peekable();
        if from.peek().is_none() {
            // nothing to do
            return Ok(());
        }
        self.root = Some(
            self.forest
                .extend_above(self.root.as_ref(), u32::max_value(), from.by_ref())
                .await?,
        );
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
    pub async fn extend_unpacked<I>(&mut self, from: I) -> Result<()>
    where
        I: IntoIterator<Item = (T::Key, V)>,
        I::IntoIter: Send,
    {
        let mut from = from.into_iter();
        let mut tree = Tree::empty(self.forest.clone());
        tree.extend(from.by_ref()).await?;
        self.root = match (self.root.clone(), tree.root) {
            (Some(a), Some(b)) => {
                let level = a.level().max(b.level()) + 1;
                let mut from = from.peekable();
                Some(
                    self.forest
                        .extend_branch(vec![a, b], level, from.by_ref(), CreateMode::Unpacked)
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

    pub fn stream_filtered_static_chunked_reverse(
        self,
        query: impl Query<T> + Clone + 'static,
    ) -> impl Stream<Item = Result<FilteredChunk<T, V>>> + 'static {
        match &self.root {
            Some(index) => self
                .forest
                .stream_filtered_static_chunked_reverse(0, query, index.clone())
                .left_stream(),
            None => stream::empty().right_stream(),
        }
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
    pub async fn retain<'a, Q: Query<T> + Send + Sync>(&'a mut self, query: &'a Q) -> Result<()> {
        if let Some(index) = &self.root {
            let mut level: i32 = i32::max_value();
            let res = self.forest.retain(0, query, index, &mut level).await?;
            self.root = Some(res);
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
    pub async fn repair<'a>(&mut self) -> Result<Vec<String>> {
        let mut report = Vec::new();
        if let Some(index) = &self.root {
            let mut level: i32 = i32::max_value();
            self.root = Some(self.forest.repair(index, &mut report, &mut level).await?);
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
        self.root.and_then(|index| index.link().clone())
    }
}

fn is_sorted<T: Ord>(iter: impl Iterator<Item = T>) -> bool {
    iter.collect::<Vec<_>>().windows(2).all(|x| x[0] <= x[1])
}
