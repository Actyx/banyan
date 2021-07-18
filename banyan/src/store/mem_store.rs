use crate::{GlobalLink, LocalLink, StreamId};

use super::{BlockWriter, ReadOnlyStore};
use anyhow::anyhow;
use fnv::FnvHashMap;
use parking_lot::Mutex;
use std::sync::Arc;

/// A MemStore is a pure in memory store. Mostly useful for testing.
#[derive(Clone)]
pub struct MemStore(Arc<Inner>);

struct Inner {
    blocks: Mutex<Blocks>,
    max_size: usize,
}

#[derive(Debug)]
struct Blocks {
    map: FnvHashMap<GlobalLink, Box<[u8]>>,
    current_size: usize,
}

impl MemStore {
    pub fn new(max_size: usize) -> Self {
        Self(Arc::new(Inner {
            blocks: Mutex::new(Blocks {
                map: FnvHashMap::default(),
                current_size: 0,
            }),
            max_size,
        }))
    }

    pub fn into_inner(self) -> anyhow::Result<FnvHashMap<GlobalLink, Box<[u8]>>> {
        let inner = Arc::try_unwrap(self.0).map_err(|_| anyhow!("busy"))?;
        let blocks = inner.blocks.into_inner();
        Ok(blocks.map)
    }

    fn get0(&self, link: GlobalLink) -> Option<Box<[u8]>> {
        let blocks = self.0.as_ref().blocks.lock();
        blocks.map.get(&link).cloned()
    }

    fn put0(&self, stream_id: StreamId, offset: u64, data: Vec<u8>) -> anyhow::Result<()> {
        let len = data.len();
        let link = GlobalLink::new(stream_id, LocalLink::new(offset, len)?);
        let mut blocks = self.0.blocks.lock();
        if blocks.current_size + len > self.0.max_size {
            anyhow::bail!("full");
        }
        let new = blocks.map.insert(link, data.into()).is_none();
        if new {
            blocks.current_size += len;
        }
        std::mem::drop(blocks);
        Ok(())
    }
}

impl ReadOnlyStore for MemStore {
    fn get(&self, link: GlobalLink) -> anyhow::Result<Box<[u8]>> {
        if let Some(value) = self.get0(link) {
            Ok(value)
        } else {
            Err(anyhow!("not there"))
        }
    }
}

impl BlockWriter for MemStore {
    fn put(&self, stream_id: StreamId, offset: u64, data: Vec<u8>) -> anyhow::Result<()> {
        self.put0(stream_id, offset, data)
    }
}
