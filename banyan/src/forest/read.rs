use super::{Config, CryptoConfig, ReadForest, TreeTypes, BranchCache, FilteredChunk, FutureResult};
use crate::{
    index::deserialize_compressed, index::Branch, index::BranchIndex, index::CompactSeq,
    index::Index, index::IndexRef, index::Leaf, index::LeafIndex, index::NodeInfo, query::Query,
    store::ArcReadOnlyStore,
index::zip_with_offset};
use anyhow::{anyhow, Result};
use bitvec::prelude::BitVec;
use core::fmt::Debug;
use futures::{prelude::*, stream::BoxStream};
use salsa20::{stream_cipher::NewStreamCipher, stream_cipher::SyncStreamCipher, XSalsa20};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tracing::info;

/// basic random access append only async tree
impl<T, V> ReadForest<T, V>
where
    T: TreeTypes + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    pub(crate) fn crypto_config(&self) -> &CryptoConfig {
        &self.crypto_config
    }

    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn value_key(&self) -> salsa20::Key {
        self.crypto_config().value_key
    }

    pub(crate) fn index_key(&self) -> salsa20::Key {
        self.crypto_config().index_key
    }

    fn store(&self) -> &ArcReadOnlyStore<T::Link> {
        &self.store
    }

    fn branch_cache(&self) -> &BranchCache<T> {
        &self.branch_cache
    }

    /// load a leaf given a leaf index
    pub(crate) async fn load_leaf(&self, index: &LeafIndex<T>) -> Result<Option<Leaf>> {
        Ok(if let Some(link) = &index.link {
            let data = &self.store().get(link).await?;
            if data.len() < 24 {
                return Err(anyhow!("leaf data without nonce"));
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

    pub(crate) fn load_branch_from_link(
        &self,
        link: T::Link,
    ) -> impl Future<Output = Result<Index<T>>> {
        let store = self.store.clone();
        let index_key = self.index_key();
        let config = self.config;
        async move {
            let bytes = store.get(&link).await?;
            let children: Vec<Index<T>> = deserialize_compressed(&index_key, &bytes)?;
            let level = children.iter().map(|x| x.level()).max().unwrap() + 1;
            let count = children.iter().map(|x| x.count()).sum();
            let value_bytes = children.iter().map(|x| x.value_bytes()).sum();
            let key_bytes =
                children.iter().map(|x| x.key_bytes()).sum::<u64>() + (bytes.len() as u64);
            let summaries = children.iter().map(|x| x.data().summarize()).collect();
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
    }

    /// load a branch given a branch index, from the cache
    async fn load_branch_cached(&self, index: &BranchIndex<T>) -> Result<Option<Branch<T>>> {
        if let Some(link) = &index.link {
            let res = self.branch_cache().write().unwrap().get(link).cloned();
            match res {
                Some(branch) => Ok(Some(branch)),
                None => self.load_branch(index).await,
            }
        } else {
            Ok(None)
        }
    }

    /// load a node, returning a structure containing the index and value for convenient matching
    #[allow(clippy::needless_lifetimes)]
    pub(crate) async fn load_node<'a>(&self, index: &'a Index<T>) -> Result<NodeInfo<'a, T>> {
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

    /// load a branch given a branch index
    pub(crate) async fn load_branch(&self, index: &BranchIndex<T>) -> Result<Option<Branch<T>>> {
        Ok(if let Some(link) = &index.link {
            let bytes = self.store.get(&link).await?;
            let children: Vec<_> = deserialize_compressed(&self.index_key(), &bytes)?;
            // let children = CborZstdArrayRef::new(bytes.as_ref()).items()?;
            Some(Branch::<T>::new(children))
        } else {
            None
        })
    }
    pub(crate) async fn get(
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
    fn getr<'a>(
        &'a self,
        node: &'a Index<T>,
        offset: u64,
    ) -> FutureResult<'a, Option<(T::Key, V)>> {
        self.get(node, offset).boxed()
    }

    pub(crate) fn stream<'a>(&'a self, index: Index<T>) -> BoxStream<'a, Result<(T::Key, V)>> {
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
                    stream::iter(node.children.to_vec())
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

    pub(crate) fn stream_filtered<'a, Q: Query<T> + Debug>(
        &'a self,
        offset: u64,
        query: &'a Q,
        index: Index<T>,
    ) -> BoxStream<'a, Result<(u64, T::Key, V)>> {
        async move {
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
                    let offsets = zip_with_offset(node.children.to_vec(), offset);
                    // todo: figure out how to avoid collecting into a vec to get send
                    let matching = matching.into_iter().collect::<Vec<_>>();
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
        .try_flatten_stream()
        .boxed()
    }

    pub(crate) fn stream_filtered_static<Q: Query<T> + Clone + 'static>(
        self: Arc<Self>,
        offset: u64,
        query: Q,
        index: Index<T>,
    ) -> BoxStream<'static, Result<(u64, T::Key, V)>> {
        async move {
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
                    let offsets = zip_with_offset(node.children.to_vec(), offset);
                    // todo: figure out how to avoid collecting into a vec to get send
                    let matching = matching.into_iter().collect::<Vec<_>>();
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
        .try_flatten_stream()
        .boxed()
    }

    pub(crate) fn stream_filtered_static_chunked<
        Q: Query<T> + Clone + Send + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    >(
        self: Arc<Self>,
        offset: u64,
        query: Q,
        index: Index<T>,
        mk_extra: &'static F,
    ) -> BoxStream<'static, Result<FilteredChunk<T, V, E>>> {
        async move {
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
                        extra: mk_extra(IndexRef::Leaf(index)),
                    };
                    stream::once(future::ok(chunk)).left_stream().left_stream()
                }
                NodeInfo::Branch(index, node) => {
                    // todo: don't get the node here, since we might not need it
                    let mut matching = BitVec::repeat(true, index.summaries.len());
                    query.intersecting(offset, index, &mut matching);
                    let offsets = zip_with_offset(node.children.to_vec(), offset);
                    // todo: figure out how to avoid collecting into a vec to get send
                    let matching = matching.into_iter().collect::<Vec<_>>();
                    let iter = matching.into_iter().zip(offsets).map(
                        move |(is_matching, (child, offset))| {
                            if is_matching {
                                self.clone()
                                    .stream_filtered_static_chunked(
                                        offset,
                                        query.clone(),
                                        child,
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

    pub(crate) fn stream_filtered_static_chunked_reverse<
        Q: Query<T> + Clone + Send + 'static,
        E: Send + 'static,
        F: Fn(IndexRef<T>) -> E + Send + Sync + 'static,
    >(
        self: Arc<Self>,
        offset: u64,
        query: Q,
        index: Index<T>,
        mk_extra: &'static F,
    ) -> BoxStream<'static, Result<FilteredChunk<T, V, E>>> {
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
                            extra: mk_extra(IndexRef::Leaf(index)),
                        };
                        stream::once(future::ok(chunk)).left_stream().left_stream()
                    }
                    NodeInfo::Branch(index, node) => {
                        // todo: don't get the node here, since we might not need it
                        let mut matching = BitVec::repeat(true, index.summaries.len());
                        query.intersecting(offset, index, &mut matching);
                        let offsets = zip_with_offset(node.children.to_vec(), offset);
                        let children: Vec<_> = matching.into_iter().zip(offsets).collect();
                        let iter = children.into_iter().rev().map(
                            move |(is_matching, (child, offset))| {
                                if is_matching {
                                    self.clone()
                                        .stream_filtered_static_chunked_reverse(
                                            offset,
                                            query.clone(),
                                            child,
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
}
