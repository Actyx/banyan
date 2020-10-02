//! interface to a content-addressed store
use anyhow::Result;
use futures::{future::BoxFuture, prelude::*};
use std::sync::{Arc, Mutex};

pub trait BlockWriter<L>: Send + Sync {
    /// adds a block to a temporary staging area
    ///
    /// We might have to do this async at some point, but let's keep it sync for now.
    fn put(&self, data: &[u8], level: u32) -> BoxFuture<Result<L>>;
}

/// A block writer, we use dyn to avoid having just another type parameter
pub type ArcBlockWriter<L> = Arc<dyn BlockWriter<L> + Send + Sync + 'static>;

pub trait ReadOnlyStore<L> {
    fn get(&self, link: &L) -> BoxFuture<Result<Arc<[u8]>>>;
}

pub type ArcReadOnlyStore<L> = Arc<dyn ReadOnlyStore<L> + Send + Sync + 'static>;

pub struct LoggingBlockWriter<L, I> {
    inner: I,
    log: Arc<Mutex<Vec<(L, u32)>>>,
}

impl<L: Copy, I> LoggingBlockWriter<L, I> {
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            log: Default::default(),
        }
    }

    pub fn written(&self) -> Vec<(L, u32)> {
        self.log.lock().unwrap().clone()
    }
}

impl<L: Send + Sync + Copy, I: BlockWriter<L>> BlockWriter<L> for LoggingBlockWriter<L, I> {
    fn put(&self, data: &[u8], level: u32) -> BoxFuture<Result<L>> {
        self.inner
            .put(data, level)
            .map_ok(move |link| {
                self.log.lock().unwrap().push((link, level));
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
    fn put(&self, data: &[u8], level: u32) -> BoxFuture<Result<L>> {
        let block = todo!();
        todo!()
    }
}
