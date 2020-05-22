//! ipfs block stores
use crate::ipfs::Cid;
use anyhow::{anyhow, Result};
use futures::{future::BoxFuture, prelude::*};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

pub trait Store {
    fn put(&self, data: &[u8]) -> BoxFuture<Result<Cid>>;
    fn get(&self, cid: &Cid) -> BoxFuture<Result<Arc<[u8]>>>;
}

pub type ArcStore = Arc<dyn Store + Send + Sync + 'static>;

pub struct TestStore(Arc<RwLock<HashMap<Cid, Arc<[u8]>>>>);

impl TestStore {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
}

impl Store for TestStore {
    fn put(&self, data: &[u8]) -> BoxFuture<Result<Cid>> {
        let cid = Cid::dag_cbor(data);
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

pub struct IpfsStore {}

impl IpfsStore {
    pub fn new() -> Self {
        Self {}
    }
}

impl Store for IpfsStore {
    fn put(&self, data: &[u8]) -> BoxFuture<Result<Cid>> {
        let data = data.to_vec();
        async move { crate::ipfs::block_put(&data, false).await }.boxed()
    }

    fn get(&self, cid: &Cid) -> BoxFuture<Result<Arc<[u8]>>> {
        let cid = cid.clone();
        async move { crate::ipfs::block_get(&cid).await }.boxed()
    }
}
