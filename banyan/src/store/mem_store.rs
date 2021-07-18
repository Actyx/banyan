use crate::{LocalLink, StreamId};

use super::{BlockWriter, ReadOnlyStore};
use anyhow::anyhow;
use fnv::FnvHashMap;
use parking_lot::Mutex;
use std::sync::Arc;

/// A MemStore is a pure in memory store. Mostly useful for testing.
#[derive(Clone)]
pub struct MemStore(Arc<Inner>);

struct Inner {
    blocks: Mutex<Blocks<LocalLink>>,
    max_size: usize,
}

#[derive(Debug)]
struct Blocks<L> {
    map: FnvHashMap<L, Box<[u8]>>,
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

    pub fn into_inner(self) -> anyhow::Result<FnvHashMap<LocalLink, Box<[u8]>>> {
        let inner = Arc::try_unwrap(self.0).map_err(|_| anyhow!("busy"))?;
        let blocks = inner.blocks.into_inner();
        Ok(blocks.map)
    }

    fn get0(&self, _stream_id: StreamId, link: LocalLink) -> Option<Box<[u8]>> {
        let blocks = self.0.as_ref().blocks.lock();
        blocks.map.get(&link).cloned()
    }

    fn put0(&self, _stream_id: StreamId, offset: u64, data: Vec<u8>) -> anyhow::Result<()> {
        let digest = LocalLink::new(offset, data.len())?;
        let len = data.len();
        let mut blocks = self.0.blocks.lock();
        if blocks.current_size + data.len() > self.0.max_size {
            anyhow::bail!("full");
        }
        let new = blocks.map.insert(digest, data.into()).is_none();
        if new {
            blocks.current_size += len;
        }
        std::mem::drop(blocks);
        Ok(())
    }
}

impl ReadOnlyStore for MemStore {
    fn get(&self, stream_id: StreamId, link: LocalLink) -> anyhow::Result<Box<[u8]>> {
        if let Some(value) = self.get0(stream_id, link) {
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
