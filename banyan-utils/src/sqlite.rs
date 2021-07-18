//! helper methods to work with ipfs/ipld
use anyhow::Result;
use banyan::{GlobalLink, StreamId, store::{BlockWriter, ReadOnlyStore}};
use ipfs_sqlite_block_store::BlockStore;
use libipld::{codec::References, store::StoreParams, Ipld};
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Clone)]
pub struct SqliteStore<S: StoreParams>(Arc<Mutex<BlockStore<S>>>);

impl<S: StoreParams> SqliteStore<S> {
    pub fn new(store: BlockStore<S>) -> anyhow::Result<Self> {
        Ok(SqliteStore(Arc::new(Mutex::new(store))))
    }
}

impl<S: StoreParams> ReadOnlyStore for SqliteStore<S>
where
    Ipld: References<S::Codecs>,
{
    fn get(&self, _link: GlobalLink) -> Result<Box<[u8]>> {
        todo!()
        // let cid = Cid::from(*link);
        // let block = self.0.lock().get_block(&cid)?;
        // if let Some(block) = block {
        //     Ok(block.into())
        // } else {
        //     Err(anyhow!("block not found!"))
        // }
    }
}

impl<S: StoreParams> BlockWriter for SqliteStore<S>
where
    Ipld: References<S::Codecs>,
{
    fn put(&self, _stream_id: StreamId, _offset: u64, _data: Vec<u8>) -> Result<()> {
        todo!()
        // let digest = Sha256Digest::new(&data);
        // let cid = digest.into();
        // let block = Block::new_unchecked(cid, data);
        // self.0.lock().put_block(&block, None)?;
        // Ok(digest)
    }
}
