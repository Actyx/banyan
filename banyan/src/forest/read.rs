use super::{BranchCache, Config, CryptoConfig, FilteredChunk, Forest, TreeTypes};
use crate::{
    index::deserialize_compressed,
    index::zip_with_offset,
    index::Branch,
    index::BranchIndex,
    index::CompactSeq,
    index::Index,
    index::IndexRef,
    index::Leaf,
    index::LeafIndex,
    index::NodeInfo,
    query::Query,
    store::ReadOnlyStore,
    util::IpldNode,
    util::{BoxedIter, IterExt},
};
use anyhow::{anyhow, Result};
use core::fmt::Debug;
use futures::{prelude::*, stream::BoxStream};
use libipld::{cbor::DagCborCodec, codec::Codec};
use salsa20::{cipher::NewStreamCipher, cipher::SyncStreamCipher, XSalsa20};
use serde::{de::DeserializeOwned, Serialize};
use std::{iter, sync::Arc, time::Instant};

/// basic random access append only tree
impl<T, V, R> Forest<T, V, R>
where
    T: TreeTypes + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
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

    // pub fn with_value_type<
    //     W: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
    // >(
    //     &self,
    // ) -> Forest<T, W> {
    //     Forest {
    //         store: self.store.clone(),
    //         branch_cache: self.branch_cache.clone(),
    //         crypto_config: self.crypto_config,
    //         config: self.config,
    //         _tt: PhantomData,
    //     }
    // }

    /// load a leaf given a leaf index
    pub(crate) fn load_leaf(&self, index: &LeafIndex<T>) -> Result<Option<Leaf>> {
        Ok(if let Some(link) = &index.link {
            let data = &self.store().get(link)?;
            let data: Vec<u8> = DagCborCodec.decode::<IpldNode>(&data)?.into_data()?;
            if data.len() < 24 {
                anyhow::bail!("leaf data without nonce");
            }
            let (nonce, data) = data.split_at(24);
            let mut data = data.to_vec();
            XSalsa20::new(&self.value_key(), nonce.into()).apply_keystream(&mut data);
            // cipher.apply_keystream(data)
            Some(Leaf::new(data.into()))
        } else {
            None
        })
    }

    pub(crate) fn load_branch_from_link(&self, link: T::Link) -> Result<Index<T>> {
        let store = self.store.clone();
        let index_key = self.index_key();
        let config = self.config;
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
            sealed: config.branch_sealed(&children, level),
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
        assert!(offset < index.count());
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
        let this: Forest<T, V, R> = self.clone();
        async move {
            Ok(match this.load_node(&index)? {
                NodeInfo::Leaf(index, node) => {
                    // todo: don't get the node here, since we might not need it
                    let mut matching = vec![true; index.keys.len()];
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
                        extra: mk_extra(IndexRef::Leaf(index)),
                    };
                    stream::once(future::ok(chunk)).left_stream().left_stream()
                }
                NodeInfo::Branch(index, node) => {
                    // todo: don't get the node here, since we might not need it
                    let mut matching = vec![true; index.summaries.len()];
                    query.intersecting(offset, index, &mut matching);
                    let offsets = zip_with_offset(node.children.to_vec(), offset);
                    let iter = matching.into_iter().zip(offsets).map(
                        move |(is_matching, (child, offset))| {
                            if is_matching {
                                this.clone()
                                    .stream_filtered_chunked0(
                                        offset,
                                        query.clone(),
                                        Arc::new(child),
                                        mk_extra,
                                    )
                                    .right_stream()
                            } else {
                                let placeholder = FilteredChunk {
                                    range: offset..offset + child.count(),
                                    data: Vec::new(),
                                    extra: mk_extra(child.as_index_ref()),
                                };
                                stream::once(future::ok(placeholder)).left_stream()
                            }
                        },
                    );
                    stream::iter(iter).flatten().right_stream().left_stream()
                }
                NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => {
                    stream::empty().right_stream()
                }
            })
        }
        .try_flatten_stream()
        .boxed()
    }

    pub(crate) fn iter_filtered_chunked0<
        'a,
        Q: Query<T> + Clone + Send + 'a,
        E: Send + 'a,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'a,
    >(
        &'a self,
        offset: u64,
        query: Q,
        index: Arc<Index<T>>,
        mk_extra: &'a F,
    ) -> BoxedIter<'a, Result<FilteredChunk<T, V, E>>> {
        let inner = || {
            Ok(match self.load_node(&index)? {
                NodeInfo::Leaf(index, node) => {
                    // todo: don't get the node here, since we might not need it
                    let mut matching = vec![true; index.keys.len()];
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
                        extra: mk_extra(IndexRef::Leaf(index)),
                    };
                    iter::once(Ok(chunk)).left_iter().left_iter()
                }
                NodeInfo::Branch(index, node) => {
                    // todo: don't get the node here, since we might not need it
                    let mut matching = vec![true; index.summaries.len()];
                    query.intersecting(offset, index, &mut matching);
                    let offsets = zip_with_offset(node.children.to_vec(), offset);
                    let iter = matching.into_iter().zip(offsets).map(
                        move |(is_matching, (child, offset))| {
                            if is_matching {
                                self.iter_filtered_chunked0(
                                    offset,
                                    query.clone(),
                                    Arc::new(child),
                                    mk_extra,
                                )
                                .left_iter()
                            } else {
                                let placeholder = FilteredChunk {
                                    range: offset..offset + child.count(),
                                    data: Vec::new(),
                                    extra: mk_extra(child.as_index_ref()),
                                };
                                iter::once(Ok(placeholder)).right_iter()
                            }
                        },
                    );
                    iter.flatten().right_iter().left_iter()
                }
                NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => iter::empty().right_iter(),
            })
        };
        match inner() {
            Ok(iter) => iter.boxed(),
            Err(cause) => iter::once(Err(cause)).boxed(),
        }
    }

    pub(crate) fn stream_filtered_chunked_reverse0<
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
        let this = self.clone();
        let s =
            async move {
                Ok(match this.load_node(&index)? {
                    NodeInfo::Leaf(index, node) => {
                        // todo: don't get the node here, since we might not need it
                        let mut matching = vec![true; index.keys.len()];
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
                            extra: mk_extra(IndexRef::Leaf(index)),
                        };
                        stream::once(future::ok(chunk)).left_stream().left_stream()
                    }
                    NodeInfo::Branch(index, node) => {
                        // todo: don't get the node here, since we might not need it
                        let mut matching = vec![true; index.summaries.len()];
                        query.intersecting(offset, index, &mut matching);
                        let offsets = zip_with_offset(node.children.to_vec(), offset);
                        let children: Vec<_> = matching.into_iter().zip(offsets).collect();
                        let iter = children.into_iter().rev().map(
                            move |(is_matching, (child, offset))| {
                                if is_matching {
                                    this.clone()
                                        .stream_filtered_chunked_reverse0(
                                            offset,
                                            query.clone(),
                                            Arc::new(child),
                                            mk_extra,
                                        )
                                        .right_stream()
                                } else {
                                    let placeholder = FilteredChunk {
                                        range: offset..offset + child.count(),
                                        data: Vec::new(),
                                        extra: mk_extra(child.as_index_ref()),
                                    };
                                    stream::once(future::ok(placeholder)).left_stream()
                                }
                            },
                        );
                        stream::iter(iter).flatten().right_stream().left_stream()
                    }
                    NodeInfo::PurgedBranch(_) | NodeInfo::PurgedLeaf(_) => {
                        stream::empty().right_stream()
                    }
                })
            }
            .try_flatten_stream();
        Box::pin(s)
    }

    pub(crate) fn dump0(&self, index: &Index<T>, prefix: &str) -> Result<()> {
        match self.load_node(index)? {
            NodeInfo::Leaf(index, _) => {
                println!(
                    "{}Leaf(count={}, value_bytes={}, sealed={})",
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
                    self.dump0(x, &prefix)?;
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
                check!(value_count == index.keys.count());
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
