use super::{BranchCache, Config, CryptoConfig, FilteredChunk, Forest, TreeTypes};
use crate::{
    index::{
        deserialize_compressed, Branch, BranchIndex, CompactSeq, Index, IndexRef, Leaf, LeafIndex,
        NodeInfo,
    },
    query::Query,
    store::ReadOnlyStore,
    util::{BoxedIter, IterExt},
    zstd_dag_cbor_seq::ZstdDagCborSeq,
};
use anyhow::{anyhow, Result};
use core::fmt::Debug;
use futures::{prelude::*, stream::BoxStream};
use libipld::cbor::DagCbor;
use smallvec::{smallvec, SmallVec};

use std::{iter, sync::Arc, time::Instant};
enum Mode {
    Forward,
    Backward,
}
pub(crate) struct ForestIter<T: TreeTypes, V, R, Q: Query<T>, F> {
    forest: Forest<T, V, R>,
    offset: u64,
    query: Q,
    mk_extra: F,
    index_stack: SmallVec<[Arc<Index<T>>; 64]>,
    pos_stack: SmallVec<[(usize, SmallVec<[bool; 64]>); 32]>,
    mode: Mode,
}

impl<T: TreeTypes, V, R, Q, E, F> ForestIter<T, V, R, Q, F>
where
    T: TreeTypes + 'static,
    V: DagCbor + Clone + Send + Sync + Debug + 'static,
    R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
    Q: Query<T> + Clone + Send + 'static,
    E: Send + 'static,
    F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
{
    pub(crate) fn new(
        forest: Forest<T, V, R>,
        offset: u64,
        query: Q,
        index: Arc<Index<T>>,
        mk_extra: F,
    ) -> Self {
        let index_stack: SmallVec<[_; 64]> = smallvec![index];
        let pos_stack: SmallVec<[_; 32]> = smallvec![(0usize, smallvec![true])];

        ForestIter {
            forest,
            offset,
            query,
            mk_extra,
            index_stack,
            pos_stack,
            mode: Mode::Forward,
        }
    }
    pub(crate) fn new_rev(
        forest: Forest<T, V, R>,
        query: Q,
        index: Arc<Index<T>>,
        mk_extra: F,
    ) -> Self {
        let offset = index.count();
        let index_stack: SmallVec<[_; 64]> = smallvec![index];
        let pos_stack: SmallVec<[_; 32]> = smallvec![(usize::MAX, smallvec![true])];

        ForestIter {
            forest,
            // Reverse traversal doesn't support arbitrary offsets to start with (yet)
            offset,
            query,
            mk_extra,
            index_stack,
            pos_stack,
            mode: Mode::Backward,
        }
    }
}

// This is not exposed, as `ForestIter` has to be constructed with the proper
// initial values for reverse iteration
impl<T: TreeTypes, V, R, Q, E, F> DoubleEndedIterator for ForestIter<T, V, R, Q, F>
where
    T: TreeTypes + 'static,
    V: DagCbor + Clone + Send + Sync + Debug + 'static,
    R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
    Q: Query<T> + Clone + Send + 'static,
    E: Send + 'static,
    F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        debug_assert!(matches!(self.mode, Mode::Backward));
        self.next()
    }
}

impl<T: TreeTypes, V, R, Q, E, F> Iterator for ForestIter<T, V, R, Q, F>
where
    T: TreeTypes + 'static,
    V: DagCbor + Clone + Send + Sync + Debug + 'static,
    R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
    Q: Query<T> + Clone + Send + 'static,
    E: Send + 'static,
    F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
{
    type Item = Result<FilteredChunk<T, V, E>>;

    fn next(&mut self) -> Option<Self::Item> {
        let res: FilteredChunk<T, V, E> = 'outer: loop {
            let head = match self.index_stack.last() {
                Some(i) => i,
                // Nothing to do ..
                _ => return None,
            };

            let (pos, matching) = self.pos_stack.last_mut().expect("not empty");

            // Branch is exhausted: Ascend.
            let exhausted = match self.mode {
                Mode::Forward => *pos == matching.len(),
                Mode::Backward => *pos == 0,
            };

            if exhausted {
                // Ascend to parent's node
                self.index_stack.pop().expect("not empty");
                self.pos_stack.pop();

                // increase last stack ptr, if there is still something left to
                // traverse
                if !self.index_stack.is_empty() {
                    let last = self.pos_stack.last_mut().expect("not empty");
                    match self.mode {
                        Mode::Forward => {
                            last.0 += 1;
                        }
                        Mode::Backward => {
                            last.0 -= 1;
                        }
                    }
                }
                continue 'outer;
            }

            match self.forest.load_node(head) {
                Ok(NodeInfo::Branch(index, branch)) => {
                    // we hit this branch node for the first time. Apply the
                    // query on its children and store it
                    let new_branch = match self.mode {
                        Mode::Forward => *pos == 0,
                        Mode::Backward => *pos == usize::MAX,
                    };
                    if new_branch {
                        let mut q_matching = smallvec![true; index.summaries.len()];
                        let start_offset = match self.mode {
                            Mode::Forward => self.offset,
                            Mode::Backward => self.offset - index.count,
                        };
                        self.query
                            .intersecting(start_offset, index, &mut q_matching);
                        debug_assert_eq!(branch.children.len(), q_matching.len());
                        let _ = std::mem::replace(matching, q_matching);

                        if matches!(self.mode, Mode::Backward) {
                            *pos = branch.children.len();
                        }
                    }

                    let next_idx = match self.mode {
                        Mode::Forward => *pos,
                        Mode::Backward => *pos - 1,
                    };
                    if matching[next_idx] {
                        // Descend into next child
                        self.index_stack
                            // TODO: clone :-( ?
                            .push(Arc::new(branch.children[next_idx].clone()));
                        let new_vec: SmallVec<[_; 64]> = smallvec![matching[next_idx]];
                        let start_pos = match self.mode {
                            Mode::Forward => 0,
                            Mode::Backward => usize::MAX,
                        };
                        self.pos_stack.push((start_pos, new_vec));
                        continue 'outer;
                    } else {
                        let index = &branch.children[next_idx];

                        let range = match self.mode {
                            Mode::Forward => {
                                self.offset += index.count();
                                *pos += 1;
                                self.offset - index.count()..self.offset
                            }
                            Mode::Backward => {
                                self.offset -= index.count();
                                *pos -= 1;
                                self.offset + index.count()..self.offset
                            }
                        };
                        let placeholder: FilteredChunk<T, V, E> = FilteredChunk {
                            range,
                            data: Vec::new(),
                            extra: (self.mk_extra)(index.as_index_ref()),
                        };

                        break placeholder;
                    }
                }

                Ok(NodeInfo::Leaf(index, leaf)) => {
                    let chunk = {
                        if matches!(self.mode, Mode::Backward) {
                            self.offset -= index.keys.count();
                        }
                        let mut matching: SmallVec<[_; 32]> = smallvec![true; index.keys.len()];
                        self.query.containing(self.offset, index, &mut matching);
                        let keys = index.select_keys(&matching);
                        let elems: Vec<V> = match leaf.as_ref().select(&matching) {
                            Ok(i) => i,
                            Err(e) => return Some(Err(e)),
                        };
                        let mut pairs = keys
                            .zip(elems)
                            .map(|((o, k), v)| (o + self.offset, k, v))
                            .collect::<Vec<_>>();
                        if matches!(self.mode, Mode::Backward) {
                            pairs.reverse();
                        }
                        FilteredChunk {
                            range: self.offset..self.offset + index.keys.count(),
                            data: pairs,
                            extra: (self.mk_extra)(IndexRef::Leaf(index)),
                        }
                    };
                    if matches!(self.mode, Mode::Forward) {
                        self.offset += index.keys.count();
                    }

                    // Ascend to parent's node
                    self.index_stack.pop().expect("not empty");
                    self.pos_stack.pop();
                    let last = self.pos_stack.last_mut().expect("not empty");
                    match self.mode {
                        Mode::Forward => {
                            last.0 += 1;
                        }
                        Mode::Backward => {
                            last.0 -= 1;
                        }
                    }

                    break chunk;
                }

                // even for purged leafs and branches or ignored chunks,
                // produce a placeholder.
                //
                // the caller can find out if we skipped purged parts of the
                // tree by using an appropriate mk_extra fn, or check
                // `data.len()`.
                Ok(_) => {
                    // Ascend to parent's node
                    let index = self.index_stack.pop().expect("not empty");
                    self.pos_stack.pop();
                    let last = self.pos_stack.last_mut().expect("Index stack not empty");

                    let range = match self.mode {
                        Mode::Forward => {
                            self.offset += index.count();
                            last.0 += 1;
                            self.offset - index.count()..self.offset
                        }
                        Mode::Backward => {
                            self.offset -= index.count();
                            last.0 -= 1;
                            self.offset + index.count()..self.offset
                        }
                    };

                    let placeholder: FilteredChunk<T, V, E> = FilteredChunk {
                        range,
                        data: Vec::new(),
                        extra: (self.mk_extra)(index.as_index_ref()),
                    };
                    break placeholder;
                }
                Err(e) => return Some(Err(e)),
            };
        };
        Some(Ok(res))
    }
}

/// basic random access append only tree
impl<T, V, R> Forest<T, V, R>
where
    T: TreeTypes + 'static,
    V: DagCbor + Clone + Send + Sync + Debug + 'static,
    R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
{
    pub(crate) fn crypto_config(&self) -> &CryptoConfig {
        &self.0.crypto_config
    }

    pub(crate) fn config(&self) -> &Config {
        &self.0.config
    }

    pub(crate) fn value_key(&self) -> salsa20::Key {
        self.crypto_config().value_key
    }

    pub(crate) fn index_key(&self) -> salsa20::Key {
        self.crypto_config().index_key
    }

    pub fn store(&self) -> &R {
        &self.0.store
    }

    fn branch_cache(&self) -> &BranchCache<T> {
        &self.0.branch_cache
    }

    /// load a leaf given a leaf index
    pub(crate) fn load_leaf(&self, index: &LeafIndex<T>) -> Result<Option<Leaf>> {
        Ok(if let Some(link) = &index.link {
            let data = &self.store().get(link)?;
            let items = ZstdDagCborSeq::decrypt(data, &self.value_key())?;
            Some(Leaf::new(items))
        } else {
            None
        })
    }

    pub(crate) fn load_branch_from_link(&self, link: T::Link) -> Result<Index<T>> {
        let store = self.store.clone();
        let index_key = self.index_key();
        let bytes = store.get(&link)?;
        let children: Vec<Index<T>> = deserialize_compressed(&index_key, &bytes)?;
        let level = children.iter().map(|x| x.level()).max().unwrap() + 1;
        let count = children.iter().map(|x| x.count()).sum();
        let value_bytes = children.iter().map(|x| x.value_bytes()).sum();
        let key_bytes = children.iter().map(|x| x.key_bytes()).sum::<u64>() + (bytes.len() as u64);
        let summaries = children.iter().map(|x| x.summarize()).collect();
        let result = BranchIndex {
            link: Some(link),
            level,
            count,
            summaries,
            sealed: self.config.branch_sealed(&children, level),
            value_bytes,
            key_bytes,
        }
        .into();
        Ok(result)
    }

    /// load a branch given a branch index, from the cache
    pub(crate) fn load_branch_cached(&self, index: &BranchIndex<T>) -> Result<Option<Branch<T>>> {
        if let Some(link) = &index.link {
            let res = self.branch_cache().get(link);
            match res {
                Some(branch) => Ok(Some(branch)),
                None => {
                    let branch = self.load_branch(index)?;
                    if let Some(branch) = &branch {
                        self.branch_cache().put(*link, branch.clone());
                    }
                    Ok(branch)
                }
            }
        } else {
            Ok(None)
        }
    }

    /// load a node, returning a structure containing the index and value for convenient matching
    #[allow(clippy::needless_lifetimes)]
    pub(crate) fn load_node<'a>(&self, index: &'a Index<T>) -> Result<NodeInfo<'a, T>> {
        let t0 = Instant::now();
        let result = Ok(match index {
            Index::Branch(index) => {
                if let Some(branch) = self.load_branch_cached(index)? {
                    NodeInfo::Branch(index, branch)
                } else {
                    NodeInfo::PurgedBranch(index)
                }
            }
            Index::Leaf(index) => {
                if let Some(leaf) = self.load_leaf(index)? {
                    NodeInfo::Leaf(index, leaf)
                } else {
                    NodeInfo::PurgedLeaf(index)
                }
            }
        });
        tracing::debug!("load_node {}", t0.elapsed().as_secs_f64());
        result
    }

    /// load a branch given a branch index
    pub(crate) fn load_branch(&self, index: &BranchIndex<T>) -> Result<Option<Branch<T>>> {
        let t0 = Instant::now();
        let result = Ok(if let Some(link) = &index.link {
            let bytes = self.store.get(&link)?;
            let children = deserialize_compressed(&self.index_key(), &bytes)?;
            Some(Branch::<T>::new(children))
        } else {
            None
        });
        tracing::debug!("load_branch {}", t0.elapsed().as_secs_f64());
        result
    }

    pub(crate) fn get0(&self, index: &Index<T>, mut offset: u64) -> Result<Option<(T::Key, V)>> {
        if offset >= index.count() {
            return Ok(None);
        }
        match self.load_node(index)? {
            NodeInfo::Branch(_, node) => {
                for child in node.children.iter() {
                    if offset < child.count() {
                        return self.get0(child, offset);
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

    pub(crate) fn collect0(
        &self,
        index: &Index<T>,
        mut offset: u64,
        into: &mut Vec<Option<(T::Key, V)>>,
    ) -> Result<()> {
        assert!(offset < index.count());
        match self.load_node(index)? {
            NodeInfo::Branch(_, node) => {
                for child in node.children.iter() {
                    if offset < child.count() {
                        self.collect0(child, offset, into)?;
                    }
                    offset = offset.saturating_sub(child.count());
                }
            }
            NodeInfo::Leaf(index, node) => {
                let vs = node.as_ref().items::<V>()?;
                let ks = index.keys.to_vec();
                for (k, v) in ks.into_iter().zip(vs.into_iter()).skip(offset as usize) {
                    into.push(Some((k, v)));
                }
            }
            NodeInfo::PurgedLeaf(index) => {
                for _ in offset..index.keys.count() {
                    into.push(None);
                }
            }
            NodeInfo::PurgedBranch(index) => {
                for _ in offset..index.count {
                    into.push(None);
                }
            }
        }
        Ok(())
    }

    /// Convenience method to stream filtered.
    ///
    /// Implemented in terms of stream_filtered_chunked
    pub(crate) fn stream_filtered0<Q: Query<T> + Clone + 'static>(
        &self,
        offset: u64,
        query: Q,
        index: Arc<Index<T>>,
    ) -> BoxStream<'static, Result<(u64, T::Key, V)>> {
        self.stream_filtered_chunked0(offset, query, index, &|_| {})
            .map_ok(|chunk| stream::iter(chunk.data).map(Ok))
            .try_flatten()
            .boxed()
    }

    pub(crate) fn stream_filtered_chunked0<
        Q: Query<T> + Clone + Send + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    >(
        &self,
        offset: u64,
        query: Q,
        index: Arc<Index<T>>,
        mk_extra: &'static F,
    ) -> BoxStream<'static, Result<FilteredChunk<T, V, E>>> {
        let iter = self.traverse0(offset, query, index, mk_extra);
        stream::unfold(iter, |mut iter| async move {
            iter.next().map(|res| (res, iter))
        })
        .boxed()
    }

    /// Convenience method to iterate filtered.
    pub(crate) fn iter_filtered0<Q: Query<T> + Clone + Send + 'static>(
        &self,
        offset: u64,
        query: Q,
        index: Arc<Index<T>>,
    ) -> BoxedIter<'static, Result<(u64, T::Key, V)>> {
        self.traverse0(offset, query, index, &|_| {})
            .map(|res| match res {
                Ok(chunk) => chunk.data.into_iter().map(Ok).left_iter(),
                Err(cause) => iter::once(Err(cause)).right_iter(),
            })
            .flatten()
            .boxed()
    }
    pub(crate) fn iter_filtered_reverse0<Q: Query<T> + Clone + Send + 'static>(
        &self,
        query: Q,
        index: Arc<Index<T>>,
    ) -> BoxedIter<'static, Result<(u64, T::Key, V)>> {
        self.traverse_rev0(query, index, &|_| {})
            .map(|res| match res {
                Ok(chunk) => chunk.data.into_iter().map(Ok).left_iter(),
                Err(cause) => iter::once(Err(cause)).right_iter(),
            })
            .flatten()
            .boxed()
    }

    pub(crate) fn stream_filtered_chunked_reverse0<
        Q: Query<T> + Clone + Send + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    >(
        &self,
        query: Q,
        index: Arc<Index<T>>,
        mk_extra: &'static F,
    ) -> BoxStream<'static, Result<FilteredChunk<T, V, E>>> {
        let iter = self.traverse_rev0(query, index, mk_extra);
        stream::unfold(iter, |mut iter| async move {
            iter.next().map(|res| (res, iter))
        })
        .boxed()
    }

    pub(crate) fn dump0(&self, index: &Index<T>, prefix: &str) -> Result<()> {
        match self.load_node(index)? {
            NodeInfo::Branch(index, branch) => {
                println!(
                    "Branch(count={}, key_bytes={}, value_bytes={}, sealed={}, link={}, children={})",
                    index.count,
                    index.key_bytes,
                    index.value_bytes,
                    index.sealed,
                    index
                        .link
                        .map(|x| format!("{}", x))
                        .unwrap_or_else(|| "".to_string()),
                    branch.children.len()
            );
                let prefix = prefix.to_string() + "  ";
                for x in branch.children.iter() {
                    self.dump0(x, &prefix)?;
                }
            }
            x => println!("{}{}", prefix, x),
        };
        Ok(())
    }

    pub(crate) fn roots_impl(&self, index: &Index<T>) -> Result<Vec<Index<T>>> {
        let mut res = Vec::new();
        let mut level: i32 = i32::max_value();
        self.roots0(index, &mut level, &mut res)?;
        Ok(res)
    }

    fn roots0(&self, index: &Index<T>, level: &mut i32, res: &mut Vec<Index<T>>) -> Result<()> {
        if index.sealed() && index.level() as i32 <= *level {
            *level = index.level() as i32;
            res.push(index.clone());
        } else {
            *level = (*level).min(index.level() as i32 - 1);
            if let Index::Branch(b) = index {
                if let Some(branch) = self.load_branch(b)? {
                    for child in branch.children.iter() {
                        self.roots0(child, level, res)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn check_invariants0(
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
        match self.load_node(index)? {
            NodeInfo::Leaf(index, leaf) => {
                let value_count = leaf.as_ref().count()?;
                let key_count = index.keys.count();
                check!(value_count == key_count);
            }
            NodeInfo::Branch(index, branch) => {
                check!(branch.count() == index.summaries.count());
                for child in &branch.children.to_vec() {
                    if index.sealed {
                        check!(child.level() == index.level - 1);
                    } else {
                        check!(child.level() < index.level);
                    }
                }
                for (child, summary) in branch.children.iter().zip(index.summaries()) {
                    let child_summary = child.summarize();
                    check!(child_summary == summary);
                }
                let branch_sealed = self.config.branch_sealed(&branch.children, index.level);
                check!(index.sealed == branch_sealed);
                for child in &branch.children.to_vec() {
                    self.check_invariants0(child, level, msgs)?;
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

    /// Checks if a node is packed to the left
    pub(crate) fn is_packed0(&self, index: &Index<T>) -> Result<bool> {
        Ok(
            if let NodeInfo::Branch(index, branch) = self.load_node(index)? {
                if index.sealed {
                    // sealed nodes, for themselves, are packed
                    true
                } else if let Some((last, rest)) = branch.children.split_last() {
                    // for the first n-1 children, they must all be sealed and at exactly 1 level below
                    let first_ok = rest
                        .iter()
                        .all(|child| child.sealed() && child.level() == index.level - 1);
                    // for the last child, it can be at any level below, and does not have to be sealed,
                    let last_ok = last.level() < index.level;
                    // but it must itself be packed
                    let rec_ok = self.is_packed0(last)?;
                    first_ok && last_ok && rec_ok
                } else {
                    // this should not happen, but a branch with no children can be considered packed
                    true
                }
            } else {
                true
            },
        )
    }
}
