//! interface to a content-addressed store
use anyhow::anyhow;
use anyhow::Result;
use core::hash::Hash;
use fnv::FnvHashMap;
use parking_lot::Mutex;
use std::{
    num::NonZeroUsize,
    sync::{Arc, RwLock},
};
use weight_cache::{Weighable, WeightCache};
mod thread_local_zstd;
mod zstd_dag_cbor_seq;

pub(crate) use thread_local_zstd::decompress_and_transform;
pub use zstd_dag_cbor_seq::ZstdDagCborSeq;

use crate::{
    index::{Branch, CompactSeq, Index},
    TreeTypes,
};

pub trait BlockWriter<L>: Send + Sync {
    /// adds a block to a temporary staging area
    ///
    /// We might have to do this async at some point, but let's keep it sync for now.
    fn put(&self, data: Vec<u8>) -> Result<L>;
}

/// A block writer, we use dyn to avoid having just another type parameter
pub type ArcBlockWriter<L> = Arc<dyn BlockWriter<L> + 'static>;

impl<L> BlockWriter<L> for ArcBlockWriter<L> {
    fn put(&self, data: Vec<u8>) -> Result<L> {
        self.as_ref().put(data)
    }
}

pub trait ReadOnlyStore<L> {
    fn get(&self, link: &L) -> Result<Box<[u8]>>;
}

pub type ArcReadOnlyStore<L> = Arc<dyn ReadOnlyStore<L> + Send + Sync + 'static>;

impl<L> ReadOnlyStore<L> for ArcReadOnlyStore<L> {
    fn get(&self, link: &L) -> Result<Box<[u8]>> {
        self.as_ref().get(link)
    }
}

pub struct MemReader<R, L> {
    inner: R,
    store: MemStore<L>,
}

impl<R, L: Copy + Eq + Hash + Send + Sync + 'static> MemReader<R, L>
where
    R: ReadOnlyStore<L>,
{
    /// Creates a MemStore and wraps an existing reader so it reads from the memstore
    /// and the reader.
    pub fn new(
        reader: R,
        max_size: usize,
        digest: impl Fn(&[u8]) -> L + Send + Sync + 'static,
    ) -> (Self, MemStore<L>) {
        let store = MemStore::new(max_size, digest);
        (
            MemReader {
                inner: reader,
                store: store.clone(),
            },
            store,
        )
    }
}

impl<R: ReadOnlyStore<L> + Send + Sync + 'static, L: Copy + Eq + Hash + Send + Sync + 'static>
    ReadOnlyStore<L> for MemReader<R, L>
{
    fn get(&self, link: &L) -> anyhow::Result<Box<[u8]>> {
        match self.store.get0(link) {
            Some(block) => Ok(block),
            None => self.inner.get(link),
        }
    }
}

#[derive(Clone)]
pub struct MemStore<L>(Arc<Inner<L>>);

struct Inner<L> {
    blocks: RwLock<Blocks<L>>,
    digest: Arc<dyn Fn(&[u8]) -> L + Send + Sync>,
    max_size: usize,
}

#[derive(Debug)]
struct Blocks<L> {
    map: FnvHashMap<L, Box<[u8]>>,
    current_size: usize,
}

impl<L: Eq + Hash + Copy> MemStore<L> {
    pub fn new(max_size: usize, digest: impl Fn(&[u8]) -> L + Send + Sync + 'static) -> Self {
        Self(Arc::new(Inner {
            digest: Arc::new(digest),
            blocks: RwLock::new(Blocks {
                map: FnvHashMap::default(),
                current_size: 0,
            }),
            max_size,
        }))
    }

    pub fn into_inner(self) -> anyhow::Result<FnvHashMap<L, Box<[u8]>>> {
        let inner = Arc::try_unwrap(self.0).map_err(|_| anyhow!("busy"))?;
        let blocks = inner.blocks.into_inner().map_err(|_| anyhow!("poisoned"))?;
        Ok(blocks.map)
    }

    fn get0(&self, link: &L) -> Option<Box<[u8]>> {
        let blocks = self.0.as_ref().blocks.read().unwrap();
        blocks.map.get(link).cloned()
    }

    fn put0(&self, data: Vec<u8>) -> anyhow::Result<L> {
        let digest = (self.0.digest)(&data);
        let len = data.len();
        let mut blocks = self.0.blocks.write().unwrap();
        if blocks.current_size + data.len() > self.0.max_size {
            anyhow::bail!("full");
        }
        let new = blocks.map.insert(digest, data.into()).is_none();
        if new {
            blocks.current_size += len;
        }
        std::mem::drop(blocks);
        Ok(digest)
    }
}

impl<L: Eq + Hash + Copy> ReadOnlyStore<L> for MemStore<L> {
    fn get(&self, link: &L) -> anyhow::Result<Box<[u8]>> {
        if let Some(value) = self.get0(link) {
            Ok(value)
        } else {
            Err(anyhow!("not there"))
        }
    }
}

impl<L: Eq + Hash + Send + Sync + Copy + 'static> BlockWriter<L> for MemStore<L> {
    fn put(&self, data: Vec<u8>) -> anyhow::Result<L> {
        self.put0(data)
    }
}

impl<T: TreeTypes> Weighable for Branch<T> {
    fn measure(value: &Self) -> usize {
        let mut bytes = std::mem::size_of::<Branch<T>>();
        for child in value.children.iter() {
            bytes += std::mem::size_of::<Index<T>>();
            match child {
                Index::Leaf(leaf) => {
                    bytes += leaf.keys.estimated_size();
                }
                Index::Branch(branch) => {
                    bytes += branch.summaries.estimated_size();
                }
            }
        }
        bytes
    }
}

type CacheOrBypass<T> = Option<Arc<Mutex<WeightCache<<T as TreeTypes>::Link, Branch<T>>>>>;

#[derive(Debug, Clone)]
pub struct BranchCache<T: TreeTypes>(CacheOrBypass<T>);

impl<T: TreeTypes> Default for BranchCache<T> {
    fn default() -> Self {
        Self::new(64 << 20)
    }
}

impl<T: TreeTypes> BranchCache<T> {
    /// Passing a capacity of 0 disables the cache.
    pub fn new(capacity: usize) -> Self {
        let cache = if capacity == 0 {
            None
        } else {
            Some(Arc::new(Mutex::new(WeightCache::new(
                NonZeroUsize::new(capacity).expect("Cache capacity must be "),
            ))))
        };
        Self(cache)
    }

    pub fn get<'a>(&'a self, link: &'a T::Link) -> Option<Branch<T>> {
        self.0.as_ref().and_then(|x| x.lock().get(link).cloned())
    }

    pub fn put(&self, link: T::Link, branch: Branch<T>) {
        if let Some(Err(e)) = self.0.as_ref().map(|x| x.lock().put(link, branch)) {
            tracing::warn!("Adding {} to cache failed: {}", link, e);
        }
    }

    pub fn reset(&self, capacity: NonZeroUsize) {
        if let Some(cache) = self.0.as_ref() {
            let mut cache = cache.lock();
            *cache = WeightCache::new(capacity);
        }
    }
}
