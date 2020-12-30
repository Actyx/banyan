use anyhow::anyhow;
use banyan::forest::{self, BranchCache, CryptoConfig};
use clap::{App, Arg, SubCommand};
use futures::{future::BoxFuture, prelude::*};
use ipfs_sqlite_block_store::async_block_store::AsyncBlockStore;
use sqlite::{SqliteStore, TokioRuntime};
use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tag_index::{Tag, TagSet};
use tags::{Key, Sha256Digest, TT};
use tracing::Level;
use tracing_subscriber;

mod ipfs;
mod sqlite;
mod tag_index;
mod tags;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
struct StreamId([u8; 16]);

type Event = serde_cbor::Value;
type Forest = banyan::forest::Forest<TT, Event, SqliteStore>;
type Link = Sha256Digest;
type Tree = banyan::tree::Tree<TT, Event>;
type AsyncResult<T> = BoxFuture<'static, anyhow::Result<T>>;

enum StreamState {
    Own { latest: Tree, forest: Forest },
}

impl StreamState {
    fn own(forest: Forest) -> Self {
        Self::Own {
            forest,
            latest: Tree::empty(),
        }
    }
}

struct StreamManagerImpl {
    streams: BTreeMap<StreamId, StreamState>,
    store: AsyncBlockStore<TokioRuntime>,
    cache: BranchCache<TT>,
    config: Config,
}

impl StreamManagerImpl {
    fn set_own_root(&mut self, stream: StreamId, tree: Tree) {

    }

    fn get_state(&mut self, stream: StreamId) -> &mut StreamState {
        let config = self.config;
        let cache = self.cache.clone();
        let store = self.store.clone();
        let state = self.streams.entry(stream).or_insert_with(|| {
            let forest = Forest::new(
                SqliteStore::new(store),
                cache,
                config.crypto_config,
                config.forest_config,
            );
            StreamState::own(forest)
        });
        state
    }
}

#[derive(Clone)]
struct StreamManager(Arc<Mutex<StreamManagerImpl>>);

#[derive(Debug, Clone, Copy)]
struct Config {
    branch_cache: usize,
    crypto_config: forest::CryptoConfig,
    forest_config: forest::Config,
}

impl StreamManager {
    pub fn new(store: AsyncBlockStore<TokioRuntime>, config: Config) -> Self {
        let cache = BranchCache::<TT>::new(config.branch_cache);
        let streams = BTreeMap::new();
        Self(Arc::new(Mutex::new(StreamManagerImpl {
            streams,
            store,
            cache,
            config,
        })))
    }

    fn append(&self, stream: StreamId, events: Vec<(Key, Event)>) -> AsyncResult<()> {
        let sm2 = self.clone();
        let mut lock = self.lock();        
        let state = lock.get_state(stream);
        if let StreamState::Own { forest, latest } = state {
            let forest = forest.clone();
            let latest = latest.clone();
            let t: AsyncResult<()> = async move {
                forest
                    .transaction(|x| (x.clone(), x))
                    .extend(&latest, events)
                    .map_ok(|tree| {
                        sm2.lock().set_own_root(stream, tree);
                        ()
                    })
                    .await
            }
            .boxed();
            todo!()
        } else {
            future::err(anyhow!("")).boxed()
        }
    }

    fn lock(&self) -> impl std::ops::DerefMut<Target = StreamManagerImpl> + '_ {
        self.0.lock().unwrap()
    }

    fn update_root(&mut self, stream: StreamId, root: Sha256Digest) -> anyhow::Result<()> {
        Ok(())
    }

    fn can_write(&self, stream: StreamId) -> bool {
        true
    }
}
