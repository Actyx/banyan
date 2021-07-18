use parking_lot::Mutex;
use std::{convert::TryInto, num::NonZeroUsize, sync::Arc, usize};
use weight_cache::{Weighable, WeightCache};

use super::{BlockWriter, ReadOnlyStore};

/// Newtype wrapper for a boxed slice so we can implement Weighable
#[derive(Debug)]
struct MemBlock(Box<[u8]>);

impl Weighable for MemBlock {
    fn measure(value: &Self) -> usize {
        value.0.len()
    }
}

#[derive(Debug, Clone)]
struct Cache {
    cache: Arc<Mutex<WeightCache<(u64, u64), MemBlock>>>,
    /// maximum size of blocks to cache
    /// we want this to remain very small, so we only cache tiny blocks
    max_size: NonZeroUsize,
}

impl Cache {
    fn new(max_size: usize, capacity: usize) -> Option<Self> {
        let capacity = capacity.try_into().ok()?;
        let max_size = max_size.try_into().ok()?;
        Some(Self {
            cache: Arc::new(Mutex::new(WeightCache::new(capacity))),
            max_size,
        })
    }
}

/// A MemCache wraps an existing store with a cache with limited block size and limited total size
///
/// Note that this makes no attempts to remain consistent with the underlying store.
/// If the underlying store is a content-addressed store, (links are hashes of data),
/// you might get values despite the underlying store no longer having the data.
///
/// If it is not a content-addressed store, all bets are off and you might even get stale values.
#[derive(Debug, Clone)]
pub struct MemCache<I> {
    inner: I,
    /// the actual cache, None if a capacity of 0 was configured
    cache: Option<Cache>,
}

impl<I: ReadOnlyStore> MemCache<I> {
    /// create a new MemCache
    /// `max_size` the maximum size for a block to be considered for caching, 0 to disable
    /// `capacity` the total capacity of the cache, 0 to disable
    pub fn new(inner: I, max_size: usize, capacity: usize) -> Self {
        Self {
            inner,
            cache: Cache::new(max_size, capacity),
        }
    }

    /// offer some data just to the cache, without writing it to the underlying store
    pub fn offer(&self, link: (u64, u64), data: &[u8]) {
        if let Some(cache) = self.cache.as_ref() {
            if data.len() <= cache.max_size.into() {
                let copy: Box<[u8]> = data.into();
                let _ = cache.cache.lock().put(link, MemBlock(copy));
            }
        }
    }

    pub fn write<W>(&self, f: impl Fn(&I) -> anyhow::Result<W>) -> anyhow::Result<MemWriter<W>> {
        Ok(MemWriter::new(f(&self.inner)?, self.cache.clone()))
    }

    /// get the value, just from ourselves, as a
    fn get0(&self, key: (u64, u64)) -> Option<Box<[u8]>> {
        self.cache
            .as_ref()
            .and_then(|cache| cache.cache.lock().get(&key).map(|x| x.0.clone()))
    }
}

impl<I: ReadOnlyStore + Send + Sync + 'static> ReadOnlyStore for MemCache<I> {
    fn get(&self, stream_id: u128, link: (u64, u64)) -> anyhow::Result<Box<[u8]>> {
        match self.get0(link) {
            Some(data) => Ok(data),
            None => self.inner.get(stream_id, link),
        }
    }
}

pub struct MemWriter<I> {
    inner: I,
    cache: Option<Cache>,
}

impl<I> MemWriter<I> {
    fn new(inner: I, cache: Option<Cache>) -> Self {
        Self { inner, cache }
    }

    pub fn into_inner(self) -> I {
        self.inner
    }
}

impl<I: BlockWriter + Send + Sync + 'static> BlockWriter for MemWriter<I> {
    fn put(&self, stream_id: u128, offset: u64, data: Vec<u8>) -> anyhow::Result<()> {
        if let Some(cache) = self.cache.as_ref() {
            if data.len() <= cache.max_size.into() {
                let copy: Box<[u8]> = data.as_slice().into();
                let link = self.inner.put(stream_id, offset, data)?;
                let _ = cache
                    .cache
                    .lock()
                    .put((offset, copy.len() as u64), MemBlock(copy));
                return Ok(link);
            }
        }
        self.inner.put(stream_id, offset, data)
    }
}
