use crate::{
    forest::{
        BranchResult, Config, CreateMode, CryptoConfig, Forest, FutureResult, Transaction,
        TreeTypes,
    },
    index::zip_with_offset_ref,
};
use crate::{
    index::serialize_compressed,
    index::Branch,
    index::BranchIndex,
    index::CompactSeq,
    index::Index,
    index::Leaf,
    index::LeafIndex,
    index::NodeInfo,
    query::Query,
    store::ArcBlockWriter,
    store::ArcReadOnlyStore,
    util::{is_sorted, BoolSliceExt},
    zstd_array::ZstdArrayBuilder,
};
use anyhow::{ensure, Result};
use core::fmt::Debug;
use futures::prelude::*;
use rand::RngCore;
use salsa20::{stream_cipher::NewStreamCipher, stream_cipher::SyncStreamCipher, XSalsa20};
use serde::{de::DeserializeOwned, Serialize};
use std::{iter, sync::Arc, sync::RwLock};
use tracing::info;

/// basic random access append only async tree
impl<T, V> Transaction<T, V>
where
    T: TreeTypes + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    pub fn read(&self) -> &Forest<T, V> {
        &self.read
    }

    /// create a leaf from scratch from an interator
    async fn leaf_from_iter(
        &self,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
    ) -> Result<LeafIndex<T>> {
        assert!(from.peek().is_some());
        self.extend_leaf(None, ZstdArrayBuilder::new(self.config().zstd_level)?, from)
            .await
    }

    /// Creates a leaf from a sequence that either contains all items from the sequence, or is full
    ///
    /// The result is the index of the leaf. The iterator will contain the elements that did not fit.
    async fn extend_leaf(
        &self,
        keys: Option<T::Seq>,
        builder: ZstdArrayBuilder,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
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
        XSalsa20::new(&self.value_key(), &nonce).apply_keystream(&mut tmp[24..]);
        // todo: avoid the extra allocation by reserving space for the length prefix in tmp.
        let tmp = serde_cbor::to_vec(&serde_cbor::Value::Bytes(tmp))?;
        // store leaf
        let link = self.writer.put(&tmp).await?;
        let index: LeafIndex<T> = LeafIndex {
            link: Some(link),
            value_bytes: leaf.as_ref().compressed().len() as u64,
            sealed: self
                .config()
                .leaf_sealed(leaf.as_ref().compressed().len() as u64, keys.count()),
            keys,
        };
        info!(
            "leaf created count={} bytes={} sealed={}",
            index.keys.count(),
            index.value_bytes,
            index.sealed
        );
        Ok(index)
    }

    /// given some children and some additional elements, creates a node with the given
    /// children and new children from `from` until it is full
    pub(crate) async fn extend_branch(
        &self,
        mut children: Vec<Index<T>>,
        level: u32,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
        mode: CreateMode,
    ) -> Result<BranchIndex<T>> {
        assert!(
            children.iter().all(|child| child.level() < level),
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
    async fn fill_node(
        &self,
        level: u32,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
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
    fn fill_noder<'a>(
        &'a self,
        level: u32,
        from: &'a mut iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
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
        let level = children.iter().map(|x| x.level()).max().unwrap() + 1;
        let count = children.iter().map(|x| x.count()).sum();
        let summaries = children
            .iter()
            .map(|child| child.data().summarize())
            .collect::<Vec<_>>();
        let value_bytes = children.iter().map(|x| x.value_bytes()).sum();
        let sealed = self.config.branch_sealed(&children, level);
        let summaries: T::Seq = summaries.into_iter().collect();
        let (link, encoded_children_len) = self.persist_branch(&children, level).await?;
        let key_bytes = children.iter().map(|x| x.key_bytes()).sum::<u64>() + encoded_children_len;
        Ok(BranchIndex {
            level,
            count,
            sealed,
            link: Some(link),
            summaries,
            key_bytes,
            value_bytes,
        })
    }

    /// extends an existing node with some values
    ///
    /// The result will have the max level `level`. `from` will contain all elements that did not fit.
    pub(crate) async fn extend_above(
        &self,
        node: Option<&Index<T>>,
        level: u32,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
    ) -> Result<Index<T>> {
        ensure!(
            from.peek().is_some(),
            "must have more than 1 element when extending"
        );
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

    pub(crate) async fn extend_unpacked<I>(
        &self,
        index: Option<&Index<T>>,
        from: I,
    ) -> Result<Option<Index<T>>>
    where
        I: IntoIterator<Item = (T::Key, V)>,
        I::IntoIter: Send,
    {
        let mut from = from.into_iter().peekable();
        if from.peek().is_none() {
            return Ok(index.cloned());
        }
        // create a completely new tree
        let b = self.extend_above(None, u32::max_value(), &mut from).await?;
        Ok(Some(match index.cloned() {
            Some(a) => {
                let level = a.level().max(b.level()) + 1;
                self.extend_branch(vec![a, b], level, from.by_ref(), CreateMode::Unpacked)
                    .await?
                    .into()
            }
            None => b,
        }))
    }

    /// extends an existing node with some values
    ///
    /// The result will have the same level as the input. `from` will contain all elements that did not fit.        
    async fn extend(
        &self,
        index: &Index<T>,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
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
                let mut children = branch.children.to_vec();
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
    fn extendr<'a>(
        &'a self,
        node: &'a Index<T>,
        from: &'a mut iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
    ) -> FutureResult<'a, Index<T>> {
        self.extend(node, from).boxed()
    }

    /// Performs a single step of simplification on a sequence of sealed roots of descending level
    pub(crate) async fn simplify_roots(
        &self,
        roots: &mut Vec<Index<T>>,
        from: usize,
    ) -> Result<()> {
        assert!(roots.len() > 1);
        assert!(is_sorted(roots.iter().map(|x| x.level()).rev()));
        match find_valid_branch(&self.config, &roots[from..]) {
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

    fn random_nonce(&self) -> salsa20::XNonce {
        let mut nonce = [0u8; 24];
        rand::thread_rng().fill_bytes(&mut nonce);
        nonce.into()
    }

    async fn persist_branch(&self, children: &[Index<T>], level: u32) -> Result<(T::Link, u64)> {
        let mut cbor = Vec::new();
        serialize_compressed(
            &self.index_key(),
            &self.random_nonce(),
            &children,
            self.config().zstd_level,
            &mut cbor,
        )?;
        Ok((self.writer.put(&cbor).await?, cbor.len() as u64))
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
                let mut matching = vec![true; index.summaries.len()];
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
                    let mut children = node.children.to_vec();
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
                    // rewrite the node and update the link if children have changed
                    if changed {
                        let (link, _) = self.persist_branch(&children, index.level).await?;
                        // todo: update size data
                        index.link = Some(link);
                    }
                }
                Ok(index.into())
            }
            Index::Leaf(index) => {
                // only do the check unless we are already purged
                let mut index = index.clone();
                if index.sealed && index.link.is_some() && *level >= 0 {
                    let mut matching = vec![true; index.keys.len()];
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
                // important not to hit the cache here!
                let branch = self.load_branch(index).await;
                let mut index = index.clone();
                match branch {
                    Ok(Some(node)) => {
                        let mut children = node.children.to_vec();
                        let mut changed = false;
                        for (i, child) in node.children.iter().enumerate() {
                            let child1 = self.repairr(child, report, level).await?;
                            if child1.link() != child.link() {
                                children[i] = child1;
                                changed = true;
                            }
                        }
                        // rewrite the node and update the link if children have changed
                        if changed {
                            let (link, _) = self.persist_branch(&children, index.level).await?;
                            index.link = Some(link);
                        }
                    }
                    Ok(None) => {
                        // already purged, nothing to do
                    }
                    Err(cause) => {
                        let link_txt = index.link.map(|x| x.to_string()).unwrap_or_default();
                        report.push(format!("forgetting branch {} due to {}", link_txt, cause));
                        if !index.sealed {
                            report.push("warning: forgetting unsealed branch!".into());
                        } else if index.level as i32 > *level {
                            report.push("warning: forgetting branch in unpacked part!".into());
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
                    let link_txt = index.link.map(|x| x.to_string()).unwrap_or_default();
                    report.push(format!("forgetting leaf {} due to {}", link_txt, cause));
                    if !index.sealed {
                        report.push("warning: forgetting unsealed leaf!".into());
                    } else if *level < 0 {
                        report.push("warning: forgetting leaf in unpacked part!".into());
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

    /// creates a new forest
    pub fn new(
        store: ArcReadOnlyStore<T::Link>,
        writer: ArcBlockWriter<T::Link>,
        config: Config,
        crypto_config: CryptoConfig,
    ) -> Self {
        let branch_cache = Arc::new(RwLock::new(lru::LruCache::<T::Link, Branch<T>>::new(1000)));
        Self {
            read: Forest::new(store, branch_cache, crypto_config, config),
            writer,
        }
    }
}

/// Find a valid branch in an array of children.
/// This can be either a sealed node at the start, or an unsealed node at the end
fn find_valid_branch<T: TreeTypes>(config: &Config, children: &[Index<T>]) -> BranchResult {
    assert!(!children.is_empty());
    let max_branch_count = config.max_branch_count as usize;
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
