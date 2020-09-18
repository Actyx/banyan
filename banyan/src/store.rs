//! interface to a content-addressed store
use anyhow::Result;
use futures::future::BoxFuture;
use std::sync::Arc;

pub trait BlockWriter<L> {
    /// adds a block to a temporary staging area
    ///
    /// We might have to do this async at some point, but let's keep it sync for now.
    fn put(&self, data: &[u8], raw: bool, level: u32) -> anyhow::Result<L>;
}

pub type ArcBlockWriter<L> = Arc<dyn BlockWriter<L> + Send + Sync + 'static>;

pub trait ReadOnlyStore<L> {
    fn get(&self, cid: &L) -> BoxFuture<Result<Arc<[u8]>>>;
}

pub type ArcReadOnlyStore<L> = Arc<dyn ReadOnlyStore<L> + Send + Sync + 'static>;

pub trait Store<L>: ReadOnlyStore<L> {
    fn put(&self, data: &[u8], raw: bool) -> BoxFuture<Result<L>>;
}

pub type ArcStore<L> = Arc<dyn Store<L> + Send + Sync + 'static>;
