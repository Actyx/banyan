#[cfg(feature = "metrics")]
use super::prom;
use crate::{
    forest::{BranchResult, Config, CreateMode, Forest, Transaction, TreeTypes},
    index::{zip_with_offset_ref, NodeInfo},
    store::{BlockWriter, ReadOnlyStore},
    util::nonce,
    StreamBuilderState,
};
use crate::{
    index::serialize_compressed,
    index::BranchIndex,
    index::CompactSeq,
    index::Index,
    index::LeafIndex,
    query::Query,
    store::ZstdDagCborSeq,
    util::{is_sorted, BoolSliceExt},
};
use anyhow::{ensure, Result};
use libipld::cbor::DagCbor;
use std::iter;

/// basic random access append only tree
impl<T, R, W> Transaction<T, R, W>
where
    T: TreeTypes,
    R: ReadOnlyStore<T::Link>,
    W: BlockWriter<T::Link>,
{
    pub fn read(&self) -> &Forest<T, R> {
        &self.read
    }

    /// create a leaf from scratch from an interator
    fn leaf_from_iter<V: DagCbor>(
        &self,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
        stream: &mut StreamBuilderState,
    ) -> Result<LeafIndex<T>> {
        assert!(from.peek().is_some());
        self.extend_leaf(&[], None, from, stream)
    }

    fn put_block(&self, data: Vec<u8>) -> anyhow::Result<T::Link> {
        #[cfg(feature = "metrics")]
        let _timer = prom::BLOCK_PUT_HIST.start_timer();
        #[cfg(feature = "metrics")]
        prom::BLOCK_PUT_SIZE_HIST.observe(data.len() as f64);
        self.writer.put(data)
    }

    /// Creates a leaf from a sequence that either contains all items from the sequence, or is full
    ///
    /// The result is the index of the leaf. The iterator will contain the elements that did not fit.
    fn extend_leaf<V: DagCbor>(
        &self,
        compressed: &[u8],
        keys: Option<T::KeySeq>,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)>>,
        stream: &mut StreamBuilderState,
    ) -> Result<LeafIndex<T>> {
        #[cfg(feature = "metrics")]
        let _timer = prom::LEAF_STORE_HIST.start_timer();
        assert!(from.peek().is_some());
        let mut keys = keys.map(|keys| keys.to_vec()).unwrap_or_default();
        let (data, sealed) = ZstdDagCborSeq::fill(
            compressed,
            from,
            &mut keys,
            stream.config().zstd_level,
            stream.config().target_leaf_size,
            stream.config().max_uncompressed_leaf_size,
            stream.config().max_leaf_count,
        )?;
        let value_bytes = data.compressed().len() as u64;
        let encrypted = data.into_encrypted(
            &stream.value_key().clone(),
            nonce::<T>(),
            &mut stream.offset,
        )?;
        let keys = keys.into_iter().collect::<T::KeySeq>();
        // store leaf
        let link = self.put_block(encrypted)?;
        let index: LeafIndex<T> = LeafIndex {
            link: Some(link),
            value_bytes,
            sealed,
            keys,
        };
        tracing::trace!(
            "leaf created count={} bytes={} sealed={}",
            index.keys.count(),
            index.value_bytes,
            index.sealed
        );
        Ok(index)
    }

    /// given some children and some additional elements, creates a node with the given
    /// children and new children from `from` until it is full
    pub(crate) fn extend_branch<V: DagCbor>(
        &self,
        mut children: Vec<Index<T>>,
        level: u32,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
        stream: &mut StreamBuilderState,
        mode: CreateMode,
    ) -> Result<BranchIndex<T>> {
        assert!(level > 0);
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
        let max_branch_count = if level == 1 {
            stream.config().max_key_branches
        } else {
            stream.config().max_summary_branches
        };
        let mut summaries = children
            .iter()
            .map(|child| child.summarize())
            .collect::<Vec<_>>();
        while from.peek().is_some() && (children.len() < max_branch_count) {
            let child = self.fill_node(level - 1, from, stream)?;
            let summary = child.summarize();
            summaries.push(summary);
            children.push(child);
        }
        let index = self.new_branch(&children, stream, mode)?;
        tracing::trace!(
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
    fn fill_node<V: DagCbor>(
        &self,
        level: u32,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
        stream: &mut StreamBuilderState,
    ) -> Result<Index<T>> {
        assert!(from.peek().is_some());
        Ok(if level == 0 {
            self.leaf_from_iter(from, stream)?.into()
        } else {
            self.extend_branch(Vec::new(), level, from, stream, CreateMode::Packed)?
                .into()
        })
    }

    /// creates a new branch from the given children and returns the branch index as a result.
    ///
    /// The level will be the max level of the children + 1. We do not want to support branches that are artificially high.
    fn new_branch(
        &self,
        children: &[Index<T>],
        stream: &mut StreamBuilderState,
        mode: CreateMode,
    ) -> Result<BranchIndex<T>> {
        assert!(!children.is_empty());
        if mode == CreateMode::Packed {
            assert!(is_sorted(children.iter().map(|x| x.level()).rev()));
        }
        let level = children.iter().map(|x| x.level()).max().unwrap() + 1;
        let count = children.iter().map(|x| x.count()).sum();
        let summaries = children
            .iter()
            .map(|child| child.summarize())
            .collect::<T::SummarySeq>();
        let value_bytes = children.iter().map(|x| x.value_bytes()).sum();
        let sealed = stream.config().branch_sealed(children, level);
        let (link, encoded_children_len) = self.persist_branch(children, stream)?;
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
    pub(crate) fn extend_above<V: DagCbor>(
        &self,
        node: Option<&Index<T>>,
        level: u32,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
        stream: &mut StreamBuilderState,
    ) -> Result<Index<T>> {
        ensure!(
            from.peek().is_some(),
            "must have more than 1 element when extending"
        );
        assert!(node.map(|node| level >= node.level()).unwrap_or(true));
        let mut node = if let Some(node) = node {
            self.extend0(node, from, stream)?
        } else {
            self.leaf_from_iter(from, stream)?.into()
        };
        while (from.peek().is_some() || node.level() == 0) && node.level() < level {
            let level = node.level() + 1;
            node = self
                .extend_branch(vec![node], level, from, stream, CreateMode::Packed)?
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

    pub(crate) fn extend_unpacked0<I, V>(
        &self,
        index: Option<&Index<T>>,
        from: I,
        stream: &mut StreamBuilderState,
    ) -> Result<Option<Index<T>>>
    where
        I: IntoIterator<Item = (T::Key, V)>,
        I::IntoIter: Send,
        V: DagCbor,
    {
        let mut from = from.into_iter().peekable();
        if from.peek().is_none() {
            return Ok(index.cloned());
        }
        // create a completely new tree
        let b = self.extend_above(None, u32::max_value(), &mut from, stream)?;
        Ok(Some(match index.cloned() {
            Some(a) => {
                let level = a.level().max(b.level()) + 1;
                self.extend_branch(
                    vec![a, b],
                    level,
                    from.by_ref(),
                    stream,
                    CreateMode::Unpacked,
                )?
                .into()
            }
            None => b,
        }))
    }

    /// extends an existing node with some values
    ///
    /// The result will have the same level as the input. `from` will contain all elements that did not fit.
    fn extend0<V: DagCbor>(
        &self,
        index: &Index<T>,
        from: &mut iter::Peekable<impl Iterator<Item = (T::Key, V)> + Send>,
        stream: &mut StreamBuilderState,
    ) -> Result<Index<T>> {
        tracing::trace!(
            "extend {} {} {} {}",
            index.level(),
            index.key_bytes(),
            index.value_bytes(),
            index.sealed(),
        );
        if index.sealed() || from.peek().is_none() {
            return Ok(index.clone());
        }
        let secrets = stream.secrets().clone();
        Ok(match self.node_info(&secrets, index) {
            NodeInfo::Leaf(index, leaf) => {
                tracing::trace!("extending existing leaf");
                let leaf = leaf.load()?;
                let keys = index.keys.clone();
                self.extend_leaf(leaf.as_ref().compressed(), Some(keys), from, stream)?
                    .into()
            }
            NodeInfo::Branch(index, branch) => {
                tracing::trace!("extending existing branch");
                let branch = branch.load_cached()?;
                let mut children = branch.children.to_vec();
                if let Some(last_child) = children.last_mut() {
                    *last_child =
                        self.extend_above(Some(last_child), index.level - 1, from, stream)?;
                }
                self.extend_branch(children, index.level, from, stream, CreateMode::Packed)?
                    .into()
            }
            NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => {
                // purged nodes can not be extended
                index.clone()
            }
        })
    }

    /// Performs a single step of simplification on a sequence of sealed roots of descending level
    pub(crate) fn simplify_roots(
        &self,
        roots: &mut Vec<Index<T>>,
        from: usize,
        stream: &mut StreamBuilderState,
    ) -> Result<()> {
        assert!(roots.len() > 1);
        assert!(is_sorted(roots.iter().map(|x| x.level()).rev()));
        match find_valid_branch(stream.config(), &roots[from..]) {
            BranchResult::Sealed(count) | BranchResult::Unsealed(count) => {
                let range = from..from + count;
                let node = self.new_branch(&roots[range.clone()], stream, CreateMode::Packed)?;
                roots.splice(range, Some(node.into()));
            }
            BranchResult::Skip(count) => {
                self.simplify_roots(roots, from + count, stream)?;
            }
        }
        Ok(())
    }

    fn persist_branch(
        &self,
        items: &[Index<T>],
        stream: &mut StreamBuilderState,
    ) -> Result<(T::Link, u64)> {
        #[cfg(feature = "metrics")]
        let _timer = prom::BRANCH_STORE_HIST.start_timer();
        let level = stream.config().zstd_level;
        let key = *stream.index_key();
        let cbor = serialize_compressed(&key, nonce::<T>(), &mut stream.offset, items, level)?;
        let len = cbor.len() as u64;
        Ok((self.put_block(cbor)?, len))
    }

    pub(crate) fn retain0<Q: Query<T> + Send + Sync>(
        &self,
        offset: u64,
        query: &Q,
        index: &Index<T>,
        level: &mut i32,
        stream: &mut StreamBuilderState,
    ) -> Result<Index<T>> {
        if index.sealed() && index.level() as i32 <= *level {
            // this node is sealed and below the level, so we can proceed.
            // but we must still set the level so we can not go "up" again.
            *level = index.level() as i32;
        } else {
            // this node might be either non-sealed, or we went "up",
            // so we must exclude the current level
            *level = (*level).min(index.level() as i32 - 1);
        }
        match index {
            Index::Branch(index) => {
                let mut index = index.as_ref().clone();
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
                if let Some(node) = self.load_branch(stream.secrets(), &index)? {
                    let mut children = node.children.to_vec();
                    let mut changed = false;
                    let offsets = zip_with_offset_ref(node.children.iter(), offset);
                    for (i, (child, offset)) in offsets.enumerate() {
                        // TODO: ensure we only purge children that are in the packed part!
                        let child1 = self.retain0(offset, query, child, level, stream)?;
                        if child1.link() != child.link() {
                            children[i] = child1;
                            changed = true;
                        }
                    }
                    // rewrite the node and update the link if children have changed
                    if changed {
                        let (link, _) = self.persist_branch(&children, stream)?;
                        // todo: update size data
                        index.link = Some(link);
                    }
                }
                Ok(index.into())
            }
            Index::Leaf(index) => {
                // only do the check unless we are already purged
                let mut index = index.as_ref().clone();
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

    pub(crate) fn repair0(
        &self,
        index: &Index<T>,
        report: &mut Vec<String>,
        level: &mut i32,
        stream: &mut StreamBuilderState,
    ) -> Result<Index<T>> {
        if !index.sealed() {
            *level = (*level).min((index.level() as i32) - 1);
        }
        match index {
            Index::Branch(index) => {
                // important not to hit the cache here!
                let branch = self.load_branch(stream.secrets(), index);
                let mut index = index.as_ref().clone();
                match branch {
                    Ok(Some(node)) => {
                        let mut children = node.children.to_vec();
                        let mut changed = false;
                        for (i, child) in node.children.iter().enumerate() {
                            let child1 = self.repair0(child, report, level, stream)?;
                            if child1.link() != child.link() {
                                children[i] = child1;
                                changed = true;
                            }
                        }
                        // rewrite the node and update the link if children have changed
                        if changed {
                            let (link, _) = self.persist_branch(&children, stream)?;
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
                let mut index = index.as_ref().clone();
                // important not to hit the cache here!
                if let Err(cause) = self.load_leaf(stream.secrets(), &index) {
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
}

/// Find a valid branch in an array of children.
/// This can be either a sealed node at the start, or an unsealed node at the end
fn find_valid_branch<T: TreeTypes>(config: &Config, children: &[Index<T>]) -> BranchResult {
    assert!(children.len() > 1);
    assert!(is_sorted(children.iter().map(|x| x.level()).rev()));
    assert!(!children.is_empty());
    // this is the level of the first child, not the level of the branch to be created
    let first_level = children[0].level();
    let max_count = if first_level == 0 {
        // we are at level 1, so use max_key_branches
        config.max_key_branches
    } else {
        // we are at level >1, so use max_summary_branches
        config.max_summary_branches
    };
    let pos = children
        .iter()
        .position(|x| x.level() < first_level)
        .unwrap_or(children.len());
    if pos >= max_count {
        // sequence starts with roots that we can turn into a sealed node
        BranchResult::Sealed(max_count)
    } else if pos == children.len() || pos == children.len() - 1 {
        // sequence is something we can turn into an unsealed node
        BranchResult::Unsealed(max_count.min(children.len()))
    } else {
        // remove first pos and recurse
        BranchResult::Skip(pos)
    }
}
