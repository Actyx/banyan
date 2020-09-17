//! interface to a content-addressed store
use anyhow::Result;
use futures::future::BoxFuture;
use std::sync::Arc;

pub trait ReadOnlyStore<L> {
    fn get(&self, cid: &L) -> BoxFuture<Result<Arc<[u8]>>>;
}

pub trait Store<L>: ReadOnlyStore<L> {
    fn put(&self, data: &[u8], raw: bool) -> BoxFuture<Result<L>>;
}

pub type ArcStore<L> = Arc<dyn Store<L> + Send + Sync + 'static>;
