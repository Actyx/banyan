//! the interface to a content-addressed store, and a memory implementation for testing
use crate::ipfs::Cid;
use anyhow::{anyhow, Result};
use futures::{future::BoxFuture, prelude::*};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

pub trait Store<L> {
    fn put(&self, data: &[u8], codec: cid::Codec) -> BoxFuture<Result<L>>;
    fn get(&self, cid: &L) -> BoxFuture<Result<Arc<[u8]>>>;
}

pub type ArcStore<L> = Arc<dyn Store<L> + Send + Sync + 'static>;

pub struct MemStore(Arc<RwLock<HashMap<Cid, Arc<[u8]>>>>);

impl MemStore {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
}

impl Store<Cid> for MemStore {
    fn put(&self, data: &[u8], codec: cid::Codec) -> BoxFuture<Result<Cid>> {
        let cid = Cid::new(data, codec);
        self.0
            .as_ref()
            .write()
            .unwrap()
            .insert(cid.clone(), data.into());
        future::ok(cid).boxed()
    }
    fn get(&self, cid: &Cid) -> BoxFuture<Result<Arc<[u8]>>> {
        let x = self.0.as_ref().read().unwrap();
        if let Some(value) = x.get(cid) {
            future::ok(value.clone()).boxed()
        } else {
            future::err(anyhow!("not there")).boxed()
        }
    }
}
