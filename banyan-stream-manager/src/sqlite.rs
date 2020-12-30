//! helper methods to work with ipfs/ipld
use std::sync::{Arc, Mutex, RwLock};

use anyhow::{anyhow, Result};
use banyan::store::{BlockWriter, ReadOnlyStore};
use futures::{future::BoxFuture, prelude::*};
use ipfs_sqlite_block_store::{BlockStore, Config, OwnedBlock, TempPin, async_block_store::{AsyncBlockStore, AsyncTempPin, RuntimeAdapter}};
use libipld::Cid;

use crate::tags::Sha256Digest;

#[derive(Clone)]
pub struct SqliteStore(Arc<Mutex<BlockStore>>);

impl SqliteStore {
    pub fn new(inner: BlockStore) -> Self {
        SqliteStore(Arc::new(Mutex::new(inner)))
    }

    pub fn memory() -> anyhow::Result<Self> {
        let store = BlockStore::memory(Config::default())?;
        Ok(SqliteStore(Arc::new(Mutex::new(store))))
    }
}

impl ReadOnlyStore<Sha256Digest> for SqliteStore {
    fn get(&self, link: &Sha256Digest) -> Result<Box<[u8]>> {
        let cid = Cid::from(*link);
        let block = self.0
            .lock()
            .unwrap()
            .get_block(&cid)?;
    if let Some(block) = block {
        Ok(block.into())
    } else {
        Err(anyhow!("block not found!"))
    }
    }
}

pub struct SqliteStoreWrite(pub Arc<Mutex<BlockStore>>, pub TempPin);

impl BlockWriter<Sha256Digest> for SqliteStoreWrite {
    fn put(&self, data: Vec<u8>) -> Result<Sha256Digest> {
        let digest = Sha256Digest::new(&data);
        let cid = digest.into();
        let block = OwnedBlock::new(cid, data);
        self.0
            .lock()
            .unwrap()
            .put_block(&block, Some(&self.1))?;
        Ok(digest)
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
