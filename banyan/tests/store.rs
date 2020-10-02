//! helper methods for the tesqts
use anyhow::{anyhow, Result};
use banyan::store::{BlockWriter, ReadOnlyStore};
use futures::{future::BoxFuture, prelude::*};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    convert::TryInto,
    fmt,
    sync::{Arc, RwLock},
};

pub struct MemStore(Arc<RwLock<HashMap<Sha256Digest, Arc<[u8]>>>>);

impl MemStore {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
}

impl ReadOnlyStore<Sha256Digest> for MemStore {
    fn get(&self, link: &Sha256Digest) -> BoxFuture<Result<Arc<[u8]>>> {
        let x = self.0.as_ref().read().unwrap();
        if let Some(value) = x.get(link) {
            future::ok(value.clone()).boxed()
        } else {
            future::err(anyhow!("not there")).boxed()
        }
    }
}

impl BlockWriter<Sha256Digest> for MemStore {
    fn put(&self, data: &[u8], _level: u32) -> BoxFuture<Result<Sha256Digest>> {
        let link = Sha256Digest::digest(data);
        self.0.as_ref().write().unwrap().insert(link, data.into());
        future::ok(link).boxed()
    }
}

/// For tests, we use a Sha2-256 digest as a link
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    pub fn digest(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        Sha256Digest(result.try_into().unwrap())
    }
    pub fn read(data: &[u8]) -> anyhow::Result<Self> {
        Ok(Self(data[0..32].try_into()?))
    }
}

impl AsRef<[u8]> for Sha256Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.as_ref()))
    }
}
