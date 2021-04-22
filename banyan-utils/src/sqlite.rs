//! helper methods to work with ipfs/ipld
use anyhow::{anyhow, Result};
use banyan::store::{BlockWriter, ReadOnlyStore};
use ipfs_sqlite_block_store::BlockStore;
use libipld::{codec::References, store::StoreParams, Block, Cid, Ipld};
use parking_lot::Mutex;
use std::sync::Arc;

use crate::tags::Sha256Digest;

#[derive(Clone)]
pub struct SqliteStore<S: StoreParams>(Arc<Mutex<BlockStore<S>>>);

impl<S: StoreParams> SqliteStore<S> {
    pub fn new(store: BlockStore<S>) -> anyhow::Result<Self> {
        Ok(SqliteStore(Arc::new(Mutex::new(store))))
    }
}

impl<S: StoreParams> ReadOnlyStore<Sha256Digest> for SqliteStore<S>
where
    Ipld: References<S::Codecs>,
{
    fn get(&self, link: &Sha256Digest) -> Result<Box<[u8]>> {
        let cid = Cid::from(*link);
        let block = self.0.lock().get_block(&cid)?;
        if let Some(block) = block {
            Ok(block.into())
        } else {
            Err(anyhow!("block not found!"))
        }
    }
}

impl<S: StoreParams> BlockWriter<Sha256Digest> for SqliteStore<S>
where
    Ipld: References<S::Codecs>,
{
    fn put(&self, data: Vec<u8>) -> Result<Sha256Digest> {
        let digest = Sha256Digest::new(&data);
        let cid = digest.into();
        let block = Block::new_unchecked(cid, data);
        self.0.lock().put_block(&block, None)?;
        Ok(digest)
    }
}
