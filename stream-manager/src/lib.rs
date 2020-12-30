use std::collections::BTreeMap;

use futures::{prelude::*, future::BoxFuture};
use ipfs_sqlite_block_store::async_block_store::{AsyncBlockStore, RuntimeAdapter};

#[derive(Debug, Clone)]
pub struct TokioRuntime;

impl RuntimeAdapter for TokioRuntime {
    fn unblock<F, T>(self, f: F) -> BoxFuture<'static, Result<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(f).err_into().boxed()
    }

    fn sleep(&self, duration: std::time::Duration) -> BoxFuture<()> {
        tokio::time::delay_for(duration).boxed()
    }
}

struct SourceId([u8; 32]);

struct StreamState {
    validated: 
}

struct StreamManagerImpl {
    streams: BTreeMap<SourceId, StreamState>,
    store: AsyncBlockStore<TokioRuntime>,
}