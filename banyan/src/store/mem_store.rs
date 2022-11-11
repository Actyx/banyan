use super::{BlockWriter, ReadOnlyStore};
use crate::error::Error;
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

    pub fn into_inner(self) -> Result<FnvHashMap<L, Box<[u8]>>, Error> {
        let inner = Arc::try_unwrap(self.0).map_err(|_| Error::Busy)?;
        let blocks = inner.blocks.into_inner();
        Ok(blocks.map)
    }

    fn get0(&self, link: &L) -> Option<Box<[u8]>> {
        let blocks = self.0.as_ref().blocks.lock();
        blocks.map.get(link).cloned()
    }

    fn put0(&self, data: Vec<u8>) -> Result<L, Error> {
        let digest = (self.0.digest)(&data);
        let len = data.len();
        let mut blocks = self.0.blocks.lock();
        if blocks.current_size + data.len() > self.0.max_size {
            return Err(Error::Full);
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
    fn get(&self, link: &L) -> Result<Box<[u8]>, Error> {
        if let Some(value) = self.get0(link) {
            Ok(value)
        } else {
            Err(Error::NotThere)
        }
    }
}

impl<L: Eq + Hash + Send + Sync + Copy + 'static> BlockWriter<L> for MemStore<L> {
    fn put(&mut self, data: Vec<u8>) -> Result<L, Error> {
        self.put0(data)
    }
}
