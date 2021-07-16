use parking_lot::Mutex;
use std::{convert::TryInto, hash::Hash, sync::Arc};
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

/// A MemCache wraps an existing store with a cache with limited block size and limited total size
///
/// Note that this makes no attempts to remain consistent with the underlying store. So if the underlying
/// store is a content-addressed store, you might get values despite the underlying store no longer having
/// the data. If it is not a content-addressed store, all bets are off and you might even get old values.
#[derive(Debug, Clone)]
pub struct MemCache<L, I> {
    inner: I,
    /// the actual cache, disabled if a capacity of 0 was configured
    cache: Option<Arc<Mutex<WeightCache<L, MemBlock>>>>,
    /// maximum size of blocks to cache
    /// we want this to remain very small, so we only cache tiny blocks
    max_size: usize,
}

impl<L: Eq + Hash + Copy, I: ReadOnlyStore<L>> MemCache<L, I> {
    /// create a new MemCache
    /// `max_size` the maximum size for a block to be considered for caching
    /// `capacity` the total capacity of the cache, 0 to disable
    pub fn new(inner: I, max_size: usize, capacity: usize) -> Self {
        let capacity = capacity.try_into().ok();
        Self {
            inner,
            max_size,
            cache: capacity.map(|capacity| Arc::new(Mutex::new(WeightCache::new(capacity)))),
        }
    }

    /// get the value, just from ourselves, as a
    fn get0(&self, key: &L) -> Option<Box<[u8]>> {
        self.cache
            .as_ref()
            .and_then(|cache| cache.lock().get(key).map(|x| x.0.clone()))
    }
}

impl<L: Eq + Hash + Send + Sync + Copy + 'static, I: ReadOnlyStore<L> + Send + Sync + 'static>
    ReadOnlyStore<L> for MemCache<L, I>
{
    fn get(&self, link: &L) -> anyhow::Result<Box<[u8]>> {
        match self.get0(link) {
            Some(data) => Ok(data),
            None => self.inner.get(link),
        }
    }
}

pub struct MemWriter<L, I> {
    inner: I,
    /// the actual cache, disabled if a capacity of 0 was configured
    cache: Option<Arc<Mutex<WeightCache<L, MemBlock>>>>,
    /// maximum size of blocks to cache
    /// we want this to remain very small, so we only cache tiny blocks
    max_size: usize,
}

impl<L, I> MemWriter<L, I> {
    pub fn new(inner: I, w: MemCache<L, I>) -> Self {
        Self {
            inner,
            cache: w.cache,
            max_size: w.max_size,
        }
    }
}

impl<L: Eq + Hash + Send + Sync + Copy + 'static, I: BlockWriter<L> + Send + Sync + 'static>
    BlockWriter<L> for MemWriter<L, I>
{
    fn put(&self, data: Vec<u8>) -> anyhow::Result<L> {
        if let Some(cache) = self.cache.as_ref() {
            if data.len() <= self.max_size {
                let copy: Box<[u8]> = data.as_slice().into();
                let link = self.inner.put(data)?;
                let _ = cache.lock().put(link, MemBlock(copy));
                return Ok(link);
            }
        }
        self.inner.put(data)
    }
}
