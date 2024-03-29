use crate::{
    index::{Branch, CompactSeq, Index},
    TreeTypes,
};
use parking_lot::Mutex;
use std::{num::NonZeroUsize, sync::Arc};
use weight_cache::{Weighable, WeightCache};

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
        let cache = NonZeroUsize::new(capacity)
            .map(WeightCache::new)
            .map(Mutex::new)
            .map(Arc::new);

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
