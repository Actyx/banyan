//! helper methods to work with ipfs/ipld
use anyhow::{anyhow, Result};
use banyan::store::{BlockWriter, ReadOnlyStore};
use ipfs_sqlite_block_store::{BlockStore, Config, OwnedBlock};
use libipld::Cid;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use crate::tags::Sha256Digest;

pub struct SqliteStore(Arc<Mutex<BlockStore>>);

impl SqliteStore {
    pub fn memory() -> anyhow::Result<Self> {
        let store = BlockStore::memory(Config::default())?;
        Ok(SqliteStore(Arc::new(Mutex::new(store))))
    }
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let store = BlockStore::open(path, Config::default())?;
        Ok(SqliteStore(Arc::new(Mutex::new(store))))
    }
}

impl ReadOnlyStore<Sha256Digest> for SqliteStore {
    fn get(&self, link: &Sha256Digest) -> Result<Box<[u8]>> {
        let cid = Cid::from(*link);
        let block = self.0.lock().unwrap().get_block(&cid)?;
        if let Some(block) = block {
            Ok(block.into())
        } else {
            Err(anyhow!("block not found!"))
        }
    }
}

impl BlockWriter<Sha256Digest> for SqliteStore {
    fn put(&self, data: Vec<u8>) -> Result<Sha256Digest> {
        let digest = Sha256Digest::new(&data);
        let cid = digest.into();
        let block = OwnedBlock::new(cid, data);
        self.0.lock().unwrap().put_block(&block, None)?;
        Ok(digest)
    }
}
