#[cfg(feature = "metrics")]
use super::prom;
use super::{BranchCache, Config, FilteredChunk, Forest, Secrets, TreeTypes};
use crate::{
    index::{
        deserialize_compressed, Branch, BranchIndex, BranchLoader, CompactSeq, Index, Leaf,
        LeafIndex, LeafLoader, NodeInfo,
    },
    query::Query,
    store::ZstdDagCborSeq,
    store::{BanyanValue, ReadOnlyStore},
    util::{nonce, BoolSliceExt, IterExt},
};
use anyhow::{anyhow, Result};
use futures::{prelude::*, stream::BoxStream};
use libipld::cbor::DagCbor;
use smallvec::{smallvec, SmallVec};
use std::{iter, marker::PhantomData, ops::Range, sync::Arc, time::Instant};

pub(crate) trait TreeVisitor<T: TreeTypes, R> {
    type Item;
    /// We are going to skip a branch or leaf. Compute placeholder item.
    fn skip(&self, range: Range<u64>, index: &NodeInfo<T, R>) -> Self::Item;
    /// We made it all the way to a leaf. Compute a value from it.
    ///
    /// Here we have the choice of loading the leaf or not. Since the loading
    /// can fail, this method returns a Result.
    fn leaf(
        &self,
        range: Range<u64>,
        index: Arc<LeafIndex<T>>,
        leaf: LeafLoader<T, R>,
        mask: &[bool],
    ) -> Result<Self::Item>;
}

/// A tree visitor that produces chunks, consisting of value triples and some
/// arbitary extra data.
pub(crate) struct ChunkVisitor<F, X>
where
    F: 'static,
{
    mk_extra: &'static F,
    _p: PhantomData<X>,
}

impl<F, X> ChunkVisitor<F, X> {
    pub fn new(mk_extra: &'static F) -> Self {
        Self {
            mk_extra,
            _p: PhantomData,
        }
    }
}

impl<T, R, F, V, E> TreeVisitor<T, R> for ChunkVisitor<F, (V, E)>
where
    T: TreeTypes,
    R: ReadOnlyStore<T::Link>,
    V: BanyanValue,
    F: Fn(&NodeInfo<T, R>) -> E + Send + Sync + 'static,
{
    type Item = FilteredChunk<(u64, T::Key, V), E>;

    fn skip(&self, range: Range<u64>, index: &NodeInfo<T, R>) -> Self::Item {
        // produce an empy chunk with just the range and the extra set
        FilteredChunk {
            range,
            data: vec![],
            extra: (self.mk_extra)(index),
        }
    }

    fn leaf(
        &self,
        range: Range<u64>,
        index: Arc<LeafIndex<T>>,
        leaf: LeafLoader<T, R>,
        matching: &[bool],
    ) -> Result<Self::Item> {
        // materialize the actual (offset, key, value) triples for the matching bits
        let data = if matching.any() {
            tracing::trace!("loading leaf {:?}", range);
            let leaf = leaf.load()?;
            let offsets = matching
                .iter()
                .enumerate()
                .filter(|(_, m)| **m)
                .map(|(i, _)| range.start + i as u64);
            let keys = index.select_keys(matching);
            let elems: Vec<V> = leaf.as_ref().select(matching)?;
            offsets
                .zip(keys)
                .zip(elems)
                .map(|((o, k), v)| (o, k, v))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let extra = (self.mk_extra)(&NodeInfo::Leaf(index, leaf));
        Ok(FilteredChunk { range, data, extra })
    }
}

#[derive(PartialEq)]
enum Mode {
    Forward,
    Backward,
}
pub(crate) struct TreeIter<T: TreeTypes, R, Q, V> {
    forest: Forest<T, R>,
    secrets: Secrets,
    offset: u64,
    stack: SmallVec<[TraverseState<T>; 5]>,
    mode: Mode,
    query: Q,
    visitor: V,
}

struct TraverseState<T: TreeTypes> {
    index: Index<T>,
    // If `index` points to a branch node, `position` points to the currently
    // traversed child
    position: isize,
    // For each child, indicates whether it should be visited or not. This is
    // initially empty, and initialized whenver we hit a branch.
    // Branches can not have zero children, so when this is empty we know that we have
    // to initialize it.
    filter: SmallVec<[bool; 64]>,
}

impl<T: TreeTypes> TraverseState<T> {
    fn new(index: Index<T>) -> Self {
        Self {
            index,
            position: 0,
            filter: smallvec![],
        }
    }
    fn is_exhausted(&self, mode: &Mode) -> bool {
        match mode {
            Mode::Forward => !self.filter.is_empty() && self.position >= self.filter.len() as isize,
            Mode::Backward => self.position < 0,
        }
    }
    fn next_pos(&mut self, mode: &Mode) {
        match mode {
            Mode::Forward => self.position += 1,
            Mode::Backward => self.position -= 1,
        }
    }
}

impl<T: TreeTypes, R, Q, V> TreeIter<T, R, Q, V>
where
    T: TreeTypes,
    R: ReadOnlyStore<T::Link>,
    Q: Query<T>,
    V: TreeVisitor<T, R>,
{
    pub(crate) fn new(
        forest: Forest<T, R>,
        secrets: Secrets,
        query: Q,
        visitor: V,
        index: Index<T>,
    ) -> Self {
        let mode = Mode::Forward;
        let stack = smallvec![TraverseState::new(index)];

        Self {
            forest,
            secrets,
            offset: 0,
            stack,
            mode,
            query,
            visitor,
        }
    }
    pub(crate) fn new_rev(
        forest: Forest<T, R>,
        secrets: Secrets,
        query: Q,
        visitor: V,
        index: Index<T>,
    ) -> Self {
        let offset = index.count();
        let mode = Mode::Backward;
        let stack = smallvec![TraverseState::new(index)];

        Self {
            forest,
            secrets,
            offset,
            stack,
            mode,
            query,
            visitor,
        }
    }

    /// common code for early returns. Pop a state from the stack and completely skip the index.
    ///
    /// this can only be called before the index is partially processed.
    fn skip(&mut self, range: Range<u64>) -> V::Item {
        let TraverseState { index, .. } = self.stack.pop().expect("not empty");
        // Ascend to parent's node. This might be none in case the
        // tree's root node is a `PurgedBranch`.
        if let Some(last) = self.stack.last_mut() {
            last.next_pos(&self.mode);
        };
        match self.mode {
            Mode::Forward => self.offset += index.count(),
            Mode::Backward => self.offset -= index.count(),
        };
        let info = self.forest.node_info(&self.secrets, &index);
        self.visitor.skip(range, &info)
    }

    #[allow(clippy::type_complexity)]
    fn next_fallible(&mut self) -> Result<Option<V::Item>> {
        Ok(Some(loop {
            let head = match self.stack.last_mut() {
                Some(i) => i,
                // Nothing to do ..
                _ => return Ok(None),
            };

            //  Branch is exhausted: Ascend.
            if head.is_exhausted(&self.mode) {
                // Ascend to parent's node
                self.stack.pop();

                // increase last stack ptr, if there is still something left to
                // traverse
                if let Some(last) = self.stack.last_mut() {
                    last.next_pos(&self.mode);
                }
                continue;
            }

            // calculate the range in advance
            let range = match self.mode {
                Mode::Forward => self.offset..self.offset.saturating_add(head.index.count()),
                Mode::Backward => self.offset.saturating_sub(head.index.count())..self.offset,
            };

            let info = self.forest.node_info(&self.secrets, &head.index);
            match info {
                NodeInfo::Branch(index, branch) => {
                    if head.filter.is_empty() {
                        // we hit this branch node for the first time. Apply the
                        // query on its children and store it
                        head.filter = smallvec![true; index.summaries.len()];
                        head.position = match self.mode {
                            Mode::Forward => 0,
                            Mode::Backward => index.summaries.len() as isize - 1,
                        };
                        self.query
                            .intersecting(range.start, &index, &mut head.filter);

                        // important early return - if not a single bit matches, there is no
                        // need to go into the individual children.
                        if !head.filter.any() {
                            break self.skip(range);
                        }
                    }

                    let branch = branch.load_cached()?;

                    let next_idx = head.position as usize;
                    if head.filter[next_idx] {
                        // Descend into next child
                        self.stack
                            .push(TraverseState::new(branch.children[next_idx].clone()));
                        continue;
                    } else {
                        let index = &branch.children[next_idx];
                        head.next_pos(&self.mode);
                        // calculate the range in advance
                        let range = match self.mode {
                            Mode::Forward => self.offset..self.offset.saturating_add(index.count()),
                            Mode::Backward => {
                                self.offset.saturating_sub(index.count())..self.offset
                            }
                        };
                        let info = self.forest.node_info(&self.secrets, index);
                        // move offset
                        match self.mode {
                            Mode::Forward => self.offset += index.count(),
                            Mode::Backward => self.offset -= index.count(),
                        };
                        break self.visitor.skip(range, &info);
                    }
                }

                NodeInfo::Leaf(index, leaf) => {
                    let mut matching: SmallVec<[_; 32]> = smallvec![true; index.keys.len()];
                    self.query.containing(range.start, &index, &mut matching);
                    let result = self.visitor.leaf(range, index.clone(), leaf, &matching)?;
                    match self.mode {
                        Mode::Backward => self.offset -= index.keys.count(),
                        Mode::Forward => self.offset += index.keys.count(),
                    }

                    // Ascend to parent's node, if it exists
                    self.stack.pop();
                    if let Some(last) = self.stack.last_mut() {
                        last.next_pos(&self.mode);
                    }
                    break result;
                }

                // even for purged leafs and branches or ignored chunks,
                // produce a placeholder.
                //
                // the caller can find out if we skipped purged parts of the
                // tree by using an appropriate mk_extra fn, or check
                // `data.len()`.
                _ => break self.skip(range),
            };
        }))
    }
}

impl<T, R, Q, V> Iterator for TreeIter<T, R, Q, V>
where
    T: TreeTypes,
    R: ReadOnlyStore<T::Link>,
    Q: Query<T>,
    V: TreeVisitor<T, R>,
{
    #[allow(clippy::type_complexity)]
    type Item = Result<V::Item>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        match self.next_fallible() {
            Ok(value) => value.map(Ok),
            Err(cause) => {
                // ensure we are done after the error
                self.stack.clear();
                Some(Err(cause))
            }
        }
    }
}

/// basic random access append only tree
impl<T, R> Forest<T, R>
where
    T: TreeTypes,
    R: ReadOnlyStore<T::Link>,
{
    pub fn store(&self) -> &R {
        &self.0.store
    }

    fn branch_cache(&self) -> &BranchCache<T> {
        &self.0.branch_cache
    }

    /// load a leaf given a leaf index
    pub(crate) fn load_leaf(&self, stream: &Secrets, index: &LeafIndex<T>) -> Result<Option<Leaf>> {
        Ok(if let Some(link) = &index.link {
            Some(self.load_leaf_from_link(stream, link)?)
        } else {
            None
        })
    }

    /// load a leaf given a leaf index
    pub(crate) fn load_leaf_from_link(&self, stream: &Secrets, link: &T::Link) -> Result<Leaf> {
        #[cfg(feature = "metrics")]
        let _timer = prom::LEAF_LOAD_HIST.start_timer();
        let data = &self.get_block(link)?;
        let (items, range) = ZstdDagCborSeq::decrypt(data, stream.value_key(), nonce::<T>())?;
        Ok(Leaf::new(items, range))
    }

    pub(crate) fn create_index_from_link(
        &self,
        secrets: &Secrets,
        sealed: impl Fn(&[Index<T>], u32) -> bool,
        link: T::Link,
    ) -> Result<(Index<T>, Range<u64>)> {
        let index_key = secrets.index_key();
        let bytes = self.get_block(&link)?;
        let (children, byte_range) = deserialize_compressed::<T>(index_key, nonce::<T>(), &bytes)?;
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
            sealed: sealed(&children, level),
            value_bytes,
            key_bytes,
        }
        .into();
        Ok((result, byte_range))
    }

    /// load a branch given a branch index, from the cache
    pub(crate) fn load_branch_cached_from_link(
        &self,
        secrets: &Secrets,
        link: &T::Link,
    ) -> Result<Branch<T>> {
        let res = self.branch_cache().get(link);
        match res {
            Some(branch) => Ok(branch),
            None => {
                let branch = self.load_branch_from_link(secrets, link)?;
                self.branch_cache().put(*link, branch.clone());
                Ok(branch)
            }
        }
    }

    fn get_block(&self, link: &T::Link) -> anyhow::Result<Box<[u8]>> {
        #[cfg(feature = "metrics")]
        let _timer = prom::BLOCK_GET_HIST.start_timer();
        let res = self.store.get(link);
        #[cfg(feature = "metrics")]
        if let Ok(x) = &res {
            prom::BLOCK_GET_SIZE_HIST.observe(x.len() as f64);
        }
        res
    }

    /// load a branch given a branch index
    pub(crate) fn load_branch_from_link(
        &self,
        secrets: &Secrets,
        link: &T::Link,
    ) -> Result<Branch<T>> {
        #[cfg(feature = "metrics")]
        let _timer = prom::BRANCH_LOAD_HIST.start_timer();
        Ok({
            let bytes = self.get_block(link)?;
            let (children, byte_range) =
                deserialize_compressed(secrets.index_key(), nonce::<T>(), &bytes)?;
            Branch::<T>::new(children, byte_range)
        })
    }

    pub(crate) fn node_info(&self, secrets: &Secrets, index: &Index<T>) -> NodeInfo<T, R> {
        match index {
            Index::Branch(index) => match index.link {
                Some(link) => {
                    NodeInfo::Branch(index.clone(), BranchLoader::new(self, secrets, link))
                }
                None => NodeInfo::PurgedBranch(index.clone()),
            },
            Index::Leaf(index) => match index.link {
                Some(link) => NodeInfo::Leaf(index.clone(), LeafLoader::new(self, secrets, link)),
                None => NodeInfo::PurgedLeaf(index.clone()),
            },
        }
    }

    /// load a branch given a branch index
    pub(crate) fn load_branch(
        &self,
        secrets: &Secrets,
        index: &BranchIndex<T>,
    ) -> Result<Option<Branch<T>>> {
        let t0 = Instant::now();
        let result = Ok(if let Some(link) = &index.link {
            let bytes = self.get_block(link)?;
            let (children, byte_range) =
                deserialize_compressed(secrets.index_key(), nonce::<T>(), &bytes)?;
            Some(Branch::<T>::new(children, byte_range))
        } else {
            None
        });
        tracing::trace!("load_branch {}", t0.elapsed().as_secs_f64());
        result
    }

    pub(crate) fn get0<V: DagCbor>(
        &self,
        stream: &Secrets,
        index: &Index<T>,
        mut offset: u64,
    ) -> Result<Option<(T::Key, V)>> {
        if offset >= index.count() {
            return Ok(None);
        }
        match self.node_info(stream, index) {
            NodeInfo::Branch(_, info) => {
                let node = info.load_cached()?;
                for child in node.children.iter() {
                    if offset < child.count() {
                        return self.get0(stream, child, offset);
                    } else {
                        offset -= child.count();
                    }
                }
                Err(anyhow!("index out of bounds: {}", offset))
            }
            NodeInfo::Leaf(index, leaf) => {
                let k = index.keys.get(offset as usize).unwrap();
                let leaf = leaf.load()?;
                let v = leaf.child_at::<V>(offset)?;
                Ok(Some((k, v)))
            }
            NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => Ok(None),
        }
    }

    pub(crate) fn collect0<V: DagCbor>(
        &self,
        stream: &Secrets,
        index: &Index<T>,
        mut offset: u64,
        into: &mut Vec<Option<(T::Key, V)>>,
    ) -> Result<()> {
        if offset >= index.count() {
            return Ok(());
        }
        match self.node_info(stream, index) {
            NodeInfo::Branch(_, node) => {
                for child in node.load_cached()?.children.iter() {
                    if offset < child.count() {
                        self.collect0(stream, child, offset, into)?;
                    }
                    offset = offset.saturating_sub(child.count());
                }
            }
            NodeInfo::Leaf(index, node) => {
                let vs = node.load()?.as_ref().items::<V>()?;
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
    pub(crate) fn stream_filtered0<Q: Query<T>, V: BanyanValue>(
        &self,
        secrets: Secrets,
        query: Q,
        index: Index<T>,
    ) -> BoxStream<'static, Result<(u64, T::Key, V)>> {
        self.stream_filtered_chunked0(secrets, query, index, &|_| {})
            .map_ok(|chunk| stream::iter(chunk.data).map(Ok))
            .try_flatten()
            .boxed()
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn stream_filtered_chunked0<
        Q: Query<T>,
        V: BanyanValue,
        E: Send + 'static,
        F: Fn(&NodeInfo<T, R>) -> E + Send + Sync + 'static,
    >(
        &self,
        secrets: Secrets,
        query: Q,
        index: Index<T>,
        mk_extra: &'static F,
    ) -> BoxStream<'static, Result<FilteredChunk<(u64, T::Key, V), E>>> {
        let iter = self.traverse0(secrets, query, index, mk_extra);
        stream::unfold(iter, |mut iter| async move {
            iter.next().map(|res| (res, iter))
        })
        .boxed()
    }

    /// Convenience method to iterate filtered.
    pub(crate) fn iter_filtered0<Q: Query<T>, V: BanyanValue>(
        &self,
        secrets: Secrets,
        query: Q,
        index: Index<T>,
    ) -> impl Iterator<Item = Result<(u64, T::Key, V)>> {
        self.traverse0(secrets, query, index, &|_| {})
            .map(|res| match res {
                Ok(chunk) => chunk.data.into_iter().map(Ok).left_iter(),
                Err(cause) => iter::once(Err(cause)).right_iter(),
            })
            .flatten()
    }
    pub(crate) fn iter_filtered_reverse0<Q: Query<T>, V: BanyanValue>(
        &self,
        secrets: Secrets,
        query: Q,
        index: Index<T>,
    ) -> impl Iterator<Item = Result<(u64, T::Key, V)>> {
        self.traverse_rev0(secrets, query, index, &|_| {})
            .map(|res| match res {
                Ok(chunk) => chunk.data.into_iter().map(Ok).left_iter(),
                Err(cause) => iter::once(Err(cause)).right_iter(),
            })
            .flatten()
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn stream_filtered_chunked_reverse0<
        Q: Query<T>,
        V: BanyanValue,
        E: Send + 'static,
        F: Fn(&NodeInfo<T, R>) -> E + Send + Sync + 'static,
    >(
        &self,
        secrets: &Secrets,
        query: Q,
        index: Index<T>,
        mk_extra: &'static F,
    ) -> BoxStream<'static, Result<FilteredChunk<(u64, T::Key, V), E>>> {
        let iter = self.traverse_rev0(secrets.clone(), query, index, mk_extra);
        stream::unfold(iter, |mut iter| async move {
            iter.next().map(|res| (res, iter))
        })
        .boxed()
    }

    pub(crate) fn dump0(&self, secrets: &Secrets, index: &Index<T>, prefix: &str) -> Result<()> {
        match self.node_info(secrets, index) {
            NodeInfo::Branch(index, branch) => {
                let branch = branch.load_cached()?;
                println!(
                    "{}Branch(count={}, key_bytes={}, value_bytes={}, sealed={}, link={}, children={})",
                    prefix,
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
                    self.dump0(secrets, x, &prefix)?;
                }
            }
            x => println!("{}{}", prefix, x),
        };
        Ok(())
    }

    pub(crate) fn roots_impl(&self, stream: &Secrets, index: &Index<T>) -> Result<Vec<Index<T>>> {
        let mut res = Vec::new();
        let mut level: i32 = i32::max_value();
        self.roots0(stream, index, &mut level, &mut res)?;
        Ok(res)
    }

    fn roots0(
        &self,
        secrets: &Secrets,
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
                if let Some(link) = b.link {
                    let branch = self.load_branch_from_link(secrets, &link)?;
                    for child in branch.children.iter() {
                        self.roots0(secrets, child, level, res)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn check_invariants0(
        &self,
        secrets: &Secrets,
        config: &Config,
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
        match self.node_info(secrets, index) {
            NodeInfo::Leaf(index, leaf) => {
                let leaf = leaf.load()?;
                let value_count = leaf.as_ref().count()?;
                let key_count = index.keys.count();
                check!(value_count == key_count);
            }
            NodeInfo::Branch(index, branch) => {
                let branch = branch.load_cached()?;
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
                let branch_sealed = config.branch_sealed(&branch.children, index.level);
                check!(index.sealed == branch_sealed);
                for child in &branch.children.to_vec() {
                    self.check_invariants0(secrets, config, child, level, msgs)?;
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
    pub(crate) fn is_packed0(&self, secrets: &Secrets, index: &Index<T>) -> Result<bool> {
        Ok(
            if let NodeInfo::Branch(index, branch) = self.node_info(secrets, index) {
                if index.sealed {
                    // sealed nodes, for themselves, are packed
                    true
                } else if let Some((last, rest)) = branch.load_cached()?.children.split_last() {
                    // for the first n-1 children, they must all be sealed and at exactly 1 level below
                    let first_ok = rest
                        .iter()
                        .all(|child| child.sealed() && child.level() == index.level - 1);
                    // for the last child, it can be at any level below, and does not have to be sealed,
                    let last_ok = last.level() < index.level;
                    // but it must itself be packed
                    let rec_ok = self.is_packed0(secrets, last)?;
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
