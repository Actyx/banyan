use super::{BlockWriter, ReadOnlyStore};
use anyhow::anyhow;
use fnv::FnvHashMap;
use parking_lot::Mutex;
use std::{hash::Hash, sync::Arc};

/// A MemStore is a pure in memory store. Mostly useful for testing.
#[derive(Clone)]
pub struct MemStore<L>(Arc<Inner<L>>);

struct Inner<L> {
    blocks: Mutex<Blocks<L>>,
    digest: Arc<dyn Fn(&[u8]) -> L + Send + Sync>,
    max_size: usize,
}

#[derive(Debug)]
struct Blocks<L> {
    map: FnvHashMap<L, Box<[u8]>>,
    current_size: usize,
}

impl<L: Eq + Hash + Copy> MemStore<L> {
    pub fn new(max_size: usize, digest: impl Fn(&[u8]) -> L + Send + Sync + 'static) -> Self {
        Self(Arc::new(Inner {
            digest: Arc::new(digest),
            blocks: Mutex::new(Blocks {
                map: FnvHashMap::default(),
                current_size: 0,
            }),
            max_size,
        }))
    }

    pub fn into_inner(self) -> anyhow::Result<FnvHashMap<L, Box<[u8]>>> {
        let inner = Arc::try_unwrap(self.0).map_err(|_| anyhow!("busy"))?;
        let blocks = inner.blocks.into_inner();
        Ok(blocks.map)
    }

    fn get0(&self, link: &L) -> Option<Box<[u8]>> {
        let blocks = self.0.as_ref().blocks.lock();
        blocks.map.get(link).cloned()
    }

    fn put0(&self, data: Vec<u8>) -> anyhow::Result<L> {
        let digest = (self.0.digest)(&data);
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
        Ok(digest)
    }
}

impl<L: Eq + Hash + Copy + Send + Sync + 'static> ReadOnlyStore<L> for MemStore<L> {
    fn get(&self, link: &L) -> anyhow::Result<Box<[u8]>> {
        if let Some(value) = self.get0(link) {
            Ok(value)
        } else {
            Err(anyhow!("not there"))
        }
    }
}

impl<L: Eq + Hash + Send + Sync + Copy + 'static> BlockWriter<L> for MemStore<L> {
    fn put(&self, data: Vec<u8>) -> anyhow::Result<L> {
        self.put0(data)
    }
}
