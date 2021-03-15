//! helper methods to work with ipfs/ipld
use anyhow::{anyhow, Result};
use banyan::store::{BlockWriter, ReadOnlyStore};
use ipfs_sqlite_block_store::{BlockStore, OwnedBlock};
use libipld::Cid;
use std::sync::{Arc, Mutex};

use crate::tags::Sha256Digest;

#[derive(Clone)]
pub struct SqliteStore(Arc<Mutex<BlockStore>>);

impl SqliteStore {
    pub fn new(store: BlockStore) -> anyhow::Result<Self> {
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
