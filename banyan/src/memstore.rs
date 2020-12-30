use crate::store::{BlockWriter, ReadOnlyStore};
use anyhow::anyhow;
use core::hash::Hash;
use fnv::FnvHashMap;
use std::sync::{Arc, RwLock};

pub struct MemReader<R, L> {
    inner: R,
    store: MemStore<L>,
}

impl<R, L: Copy + Eq + Hash + Send + Sync + 'static> MemReader<R, L>
where
    R: ReadOnlyStore<L>,
{
    /// Creates a MemStore and wraps an existing reader so it reads from the memstore
    /// and the reader.
    pub fn new(
        reader: R,
        max_size: usize,
        digest: impl Fn(&[u8]) -> L + Send + Sync + 'static,
    ) -> (Self, MemStore<L>) {
        let store = MemStore::new(max_size, digest);
        (
            MemReader {
                inner: reader,
                store: store.clone(),
            },
            store,
        )
    }
}

impl<R: ReadOnlyStore<L> + Send + Sync + 'static, L: Copy + Eq + Hash + Send + Sync + 'static>
    ReadOnlyStore<L> for MemReader<R, L>
{
    fn get(&self, link: &L) -> anyhow::Result<Box<[u8]>> {
        match self.store.get0(link) {
            Some(block) => Ok(block),
            None => self.inner.get(link),
        }
    }
}

#[derive(Clone)]
pub struct MemStore<L>(Arc<Inner<L>>);

struct Inner<L> {
    blocks: RwLock<Blocks<L>>,
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
            blocks: RwLock::new(Blocks {
                map: FnvHashMap::default(),
                current_size: 0,
            }),
            max_size,
        }))
    }

    pub fn into_inner(self) -> anyhow::Result<FnvHashMap<L, Box<[u8]>>> {
        let inner = Arc::try_unwrap(self.0).map_err(|_| anyhow!("busy"))?;
        let blocks = inner.blocks.into_inner().map_err(|_| anyhow!("poisoned"))?;
        Ok(blocks.map)
    }

    fn get0(&self, link: &L) -> Option<Box<[u8]>> {
        let blocks = self.0.as_ref().blocks.read().unwrap();
        blocks.map.get(link).cloned()
    }

    fn put0(&self, data: Vec<u8>) -> anyhow::Result<L> {
        let digest = (self.0.digest)(&data);
        let len = data.len();
        let mut blocks = self.0.blocks.write().unwrap();
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

impl<L: Eq + Hash + Copy> ReadOnlyStore<L> for MemStore<L> {
    fn get(&self, link: &L) -> anyhow::Result<Box<[u8]>> {
        if let Some(value) = self.get0(link) {
            Ok(value.clone())
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
