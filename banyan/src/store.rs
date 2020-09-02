//! interface to a content-addressed store
use anyhow::Result;
use futures::future::BoxFuture;
use std::sync::Arc;

pub trait Store<L> {
    fn put(&self, data: &[u8], raw: bool) -> BoxFuture<Result<L>>;
    fn get(&self, cid: &L) -> BoxFuture<Result<Arc<[u8]>>>;
}

pub type ArcStore<L> = Arc<dyn Store<L> + Send + Sync + 'static>;
