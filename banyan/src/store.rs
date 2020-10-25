//! interface to a content-addressed store
use anyhow::Result;
use futures::future::BoxFuture;
use std::sync::Arc;

pub trait BlockWriter<L>: Send + Sync {
    /// adds a block to a temporary staging area
    ///
    /// We might have to do this async at some point, but let's keep it sync for now.
    fn put(&self, data: &[u8]) -> BoxFuture<Result<L>>;
}

/// A block writer, we use dyn to avoid having just another type parameter
pub type ArcBlockWriter<L> = Arc<dyn BlockWriter<L> + Send + Sync + 'static>;

impl<L> BlockWriter<L> for ArcBlockWriter<L> {
    fn put(&self, data: &[u8]) -> BoxFuture<Result<L>> {
        self.as_ref().put(data)
    }
}

pub trait ReadOnlyStore<L> {
    fn get(&self, link: &L) -> BoxFuture<Result<Arc<[u8]>>>;
}

pub type ArcReadOnlyStore<L> = Arc<dyn ReadOnlyStore<L> + Send + Sync + 'static>;

impl<L> ReadOnlyStore<L> for ArcReadOnlyStore<L> {
    fn get(&self, link: &L) -> BoxFuture<Result<Arc<[u8]>>> {
        self.as_ref().get(link)
    }
}
