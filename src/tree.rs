use cid::Cid;
use futures::{future::BoxFuture, prelude::*};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::marker::PhantomData;
use crate::czaa::CborZstdArray;
use std::collections::HashMap;
use stream_cipher::SyncStreamCipher;
use salsa20::Salsa20;

/// index for a block of n events
#[derive(Debug, Clone)]
pub struct BlockIndex {
    // base offset in bytes
    offset: u64,
    // number of events
    count: u64,
    // link to the block
    link: Cid,
}

/// index for a branch node
#[derive(Debug, Clone)]
pub struct BranchIndex {
    // base offset in bytes
    offset: u64,
    // number of events
    count: u64,
    // link to the branch node
    link: Cid,
}

trait Store {
    fn put(&self, data: &[u8]) -> BoxFuture<Cid>;
    fn get(&self, cid: Cid) -> BoxFuture<&[u8]>;
}

pub struct Tree<T> {
    store: Box<dyn Store>,
    level_x: HashMap<Cid, CborZstdArray<Salsa20, BranchIndex>>,
    level_0: HashMap<Cid, CborZstdArray<Salsa20, BlockIndex>>,
    leaf: HashMap<Cid, CborZstdArray<Salsa20, T>>,
    _t: PhantomData<T>,
}

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

/// basic random access append only async tree
impl<T: Serialize + DeserializeOwned> Tree<T> {
    /// append an element
    pub async fn push(value: T) -> Result<()> {
        Ok(())
    }
    /// element at index
    pub async fn at(index: u64) -> Result<Option<T>> {
        Ok(None)
    }
    /// creates a new, empty tree
    pub fn new(store: Box<dyn Store>, secret: [u8; 32]) -> Self {
        Self {
            store,
            level_x: HashMap::new(),
            level_0: HashMap::new(),
            leaf: HashMap::new(),
            _t: PhantomData
        }
    }
    /// loads the tree from the store, given a secret and a root cid
    pub async fn load(store: Box<dyn Store>, secret: [u8; 32], root: Cid) -> Result<Self> {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "unable to load").into())
    }
    /// saves the tree in the store
    pub async fn save() -> Result<Cid> {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "unable to save").into())
    }
}
