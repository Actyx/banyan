//! helper methods to work with ipfs/ipld
use anyhow::{anyhow, Result};
use banyan::store::{BlockWriter, ReadOnlyStore};
use futures::{future::BoxFuture, prelude::*};
use ipfs_sqlite_block_store::{BlockStore, Config, OwnedBlock, async_block_store::{AsyncBlockStore, AsyncTempPin, RuntimeAdapter}};
use libipld::Cid;

use crate::tags::Sha256Digest;

#[derive(Clone)]
pub struct SqliteStore(AsyncBlockStore<TokioRuntime>);

impl SqliteStore {
    pub fn new(inner: AsyncBlockStore<TokioRuntime>) -> Self {
        SqliteStore(inner)
    }

    pub fn memory() -> anyhow::Result<Self> {
        let store = BlockStore::memory(Config::default())?;
        let store = AsyncBlockStore::new(TokioRuntime, store);
        Ok(SqliteStore(store))
    }
}

impl ReadOnlyStore<Sha256Digest> for SqliteStore {
    fn get(&self, link: &Sha256Digest) -> BoxFuture<Result<Box<[u8]>>> {
        let cid = Cid::from(*link);
        self.0
            .get_block(cid)
            .err_into()
            .and_then(|block| {
                future::ready(if let Some(block) = block {
                    Ok(block.into())
                } else {
                    Err(anyhow!("block not found!"))
                })
            })
            .boxed()
    }
}

pub struct SqliteStoreWrite(pub AsyncBlockStore<TokioRuntime>, pub AsyncTempPin);

impl BlockWriter<Sha256Digest> for SqliteStoreWrite {
    fn put(&self, data: Vec<u8>) -> BoxFuture<Result<Sha256Digest>> {
        let digest = Sha256Digest::new(&data);
        let cid = digest.into();
        let block = OwnedBlock::new(cid, data);
        self.0
            .put_block(block, Some(&self.1))
            .err_into()
            .map_ok(move |_| digest)
            .boxed()
    }
}

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
