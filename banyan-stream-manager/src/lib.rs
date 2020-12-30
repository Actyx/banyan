use anyhow::anyhow;
use banyan::forest::Forest;
use clap::{App, Arg, SubCommand};
use futures::prelude::*;
use ipfs_sqlite_block_store::async_block_store::AsyncBlockStore;
use sqlite::{SqliteStore, TokioRuntime};
use tags::{Sha256Digest, TT};
use std::{collections::BTreeMap, str::FromStr, sync::{Arc, Mutex}, time::Duration};
use tag_index::{Tag, TagSet};
use tracing::Level;
use tracing_subscriber;

mod ipfs;
mod sqlite;
mod tag_index;
mod tags;

struct SourceId([u8; 16]);

struct StreamState {
    latest: Sha256Digest,
    validated: Sha256Digest,
    writable: bool,
}

struct StreamManagerImpl {
    streams: BTreeMap<SourceId, StreamState>,
    forest: Forest<TT, serde_json::Value, SqliteStore>,
    store: AsyncBlockStore<TokioRuntime>,
}

impl StreamManagerImpl {
    pub fn new(store: AsyncBlockStore<TokioRuntime>) -> Self {
        let store = SqliteStore::new(store);
        // let forest = Forest::new(store, cache, )
        todo!()
    }
}

pub struct StreamManager(Arc<Mutex<StreamManagerImpl>>);

