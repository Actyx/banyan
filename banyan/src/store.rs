//! interface to a content-addressed store
use anyhow::Result;
use futures::{future::BoxFuture, prelude::*};
use std::sync::{Arc, Mutex};

pub trait BlockWriter<L>: Send + Sync {
    /// adds a block to a temporary staging area
    ///
    /// We might have to do this async at some point, but let's keep it sync for now.
    fn put(&self, data: &[u8]) -> BoxFuture<Result<L>>;
}

/// A block writer, we use dyn to avoid having just another type parameter
pub type ArcBlockWriter<L> = Arc<dyn BlockWriter<L> + Send + Sync + 'static>;

impl<L> BlockWriter<L> for ArcBlockWriter<L> {
    fn put(&self, data: &[u8]) -> BoxFuture<Result<L>> {
        self.as_ref().put(data)
    }
}

pub trait ReadOnlyStore<L> {
    fn get(&self, link: &L) -> BoxFuture<Result<Arc<[u8]>>>;
}

pub type ArcReadOnlyStore<L> = Arc<dyn ReadOnlyStore<L> + Send + Sync + 'static>;

impl<L> ReadOnlyStore<L> for ArcReadOnlyStore<L> {
    fn get(&self, link: &L) -> BoxFuture<Result<Arc<[u8]>>> {
        self.as_ref().get(link)
    }
}

pub struct LoggingBlockWriter<L, I> {
    inner: I,
    log: Arc<Mutex<Vec<L>>>,
}

impl<L: Copy, I> LoggingBlockWriter<L, I> {
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            log: Default::default(),
        }
    }

    pub fn written(&self) -> Vec<L> {
        self.log.lock().unwrap().clone()
    }
}

impl<L: Send + Sync + Copy, I: BlockWriter<L>> BlockWriter<L> for LoggingBlockWriter<L, I> {
    fn put(&self, data: &[u8]) -> BoxFuture<Result<L>> {
        self.inner
            .put(data)
            .map_ok(move |link| {
                self.log.lock().unwrap().push(link);
                link
            })
            .boxed()
    }
}

pub struct MemBlockWriter<L> {
    log: Arc<Mutex<Vec<(L, u32, Arc<[u8]>)>>>,
}

impl<L: Clone> MemBlockWriter<L> {
    pub fn new() -> Self {
        Self {
            log: Default::default(),
        }
    }

    pub fn written(&self) -> Vec<(L, u32, Arc<[u8]>)> {
        self.log.lock().unwrap().clone()
    }
}

impl<L: Send + Sync + Copy> BlockWriter<L> for MemBlockWriter<L> {
    fn put(&self, data: &[u8]) -> BoxFuture<Result<L>> {
        let block = todo!();
        todo!()
    }
}
