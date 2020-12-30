use anyhow::anyhow;
use banyan::forest::{self, BranchCache, CryptoConfig};
use clap::{App, Arg, SubCommand};
use forest::FutureResult;
use futures::{future::BoxFuture, prelude::*};
use ipfs_sqlite_block_store::async_block_store::{AsyncBlockStore, AsyncTempPin};
use libipld::Cid;
use sqlite::{SqliteStore, SqliteStoreWrite, TokioRuntime};
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
type Result<T> = anyhow::Result<T>;
type AsyncResult<T> = BoxFuture<'static, Result<T>>;
type BlockStore = AsyncBlockStore<TokioRuntime>;

struct OwnStreamState {
    latest: Tree,
    forest: Forest,
}

impl OwnStreamState {
    fn new(forest: Forest) -> Self {
        Self {
            forest,
            latest: Tree::empty(),
        }
    }
}

struct ReplicatedStreamState {
    pin: Option<AsyncTempPin>,
    latest: Option<Link>,
    validated: Option<Link>,
    wanted: Option<Wanted>,
    forest: Forest,
}

impl ReplicatedStreamState {
    fn new(forest: Forest) -> Self {
        Self {
            forest,
            pin: None,
            latest: None,
            wanted: None,
            validated: None,
        }
    }

    async fn pin(&mut self, store: &BlockStore) -> Result<&AsyncTempPin> {
        if self.pin.is_none() {
            self.pin = Some(store.temp_pin().await?);
        }
        Ok(self.pin.as_ref().unwrap())
    }

    async fn set_root(&mut self, root: Link, store: &BlockStore) -> Result<AsyncTempPin> {
        let pin = self.pin(store).await?.clone();
        let same_root = self.latest.as_ref().map(|r| r == &root).unwrap_or_default();
        let cid = root.into();
        if !same_root {
            store.assign_temp_pin(pin, Some(cid)).await?;
            self.latest = Some(root);
        }
        Ok(self.pin.as_ref().unwrap().clone())
    }
}

struct StreamMaps {
    own_streams: BTreeMap<StreamId, Arc<tokio::sync::Mutex<OwnStreamState>>>,
    replicated_streams: BTreeMap<StreamId, Arc<tokio::sync::Mutex<ReplicatedStreamState>>>,
}

struct StreamManagerInner {
    mutex: Mutex<StreamMaps>,
    validate_sender: futures::channel::mpsc::UnboundedSender<(StreamId, Link)>,
    store: BlockStore,
    cache: BranchCache<TT>,
    config: Config,
}

#[derive(Clone)]
struct StreamManager(Arc<StreamManagerInner>);

#[derive(Debug, Clone, Copy)]
struct Config {
    branch_cache: usize,
    crypto_config: forest::CryptoConfig,
    forest_config: forest::Config,
}

impl StreamManager {

    fn get_own_stream(
        &self,
        stream: StreamId,
    ) -> anyhow::Result<Arc<tokio::sync::Mutex<OwnStreamState>>> {
        let config = self.0.config;
        let cache = self.0.cache.clone();
        let store = self.0.store.clone();
        let mut maps = self.lock();
        let state = maps.own_streams.entry(stream).or_insert_with(|| {
            let forest = Forest::new(
                SqliteStore::new(store),
                cache,
                config.crypto_config,
                config.forest_config,
            );
            Arc::new(tokio::sync::Mutex::new(OwnStreamState::new(forest)))
        });
        Ok(state.clone())
    }

    fn get_replicated_stream(
        &self,
        stream: StreamId,
    ) -> anyhow::Result<Arc<tokio::sync::Mutex<ReplicatedStreamState>>> {
        let config = self.0.config;
        let cache = self.0.cache.clone();
        let store = self.0.store.clone();
        let mut maps = self.lock();
        let state = maps.replicated_streams.entry(stream).or_insert_with(|| {
            let forest = Forest::new(
                SqliteStore::new(store),
                cache,
                config.crypto_config,
                config.forest_config,
            );
            Arc::new(tokio::sync::Mutex::new(ReplicatedStreamState::new(forest)))
        });
        Ok(state.clone())
    }

    pub fn new(store: BlockStore, config: Config) -> (Self, AsyncResult<()>) {
        let cache = BranchCache::<TT>::new(config.branch_cache);
        let (validate_sender, validate_receiver) = futures::channel::mpsc::unbounded();
        let res = Self(Arc::new(StreamManagerInner {
            mutex: Mutex::new(StreamMaps {
                own_streams: Default::default(),
                replicated_streams: Default::default(),
            }),
            validate_sender,
            store: store.clone(),
            cache,
            config,
        }));
        let fut = res.clone().validate(store, validate_receiver).boxed();
        (res, fut)
    }

    pub fn append(&self, stream_id: StreamId, events: Vec<(Key, Event)>) -> AsyncResult<()> {
        let this = self.clone();
        async move {
            let stream = this.get_own_stream(stream_id)?;
            let mut stream = stream.lock().await;
            let pin = this.0.store.temp_pin().await?;
            let w = SqliteStoreWrite(this.0.store.clone(), pin);
            let txn = stream.forest.transaction(|x| (x, w));
            let tree = txn.extend(&stream.latest, events).await?;
            this.0.store.alias(stream_id.0.to_vec(), tree.link().map(Into::into)).await?;
            stream.latest = tree;
            drop(txn);
            Ok(())
        }
        .boxed()
    }

    pub fn update_root(&mut self, stream: StreamId, root: Link) -> Result<()> {
        Ok(self.0.validate_sender.unbounded_send((stream, root))?)
    }

    async fn validate(
        self,
        store: BlockStore,
        mut streams: impl Stream<Item = (StreamId, Link)> + Unpin,
    ) -> Result<()> {
        while let Some((stream_id, root)) = streams.next().await {
            let cid = Cid::from(root);
            let stream = self.get_replicated_stream(stream_id)?;
            let mut state = stream.lock().await;
            let pin = state.set_root(root, &store).await?;
            let missing: Vec<Cid> = store.get_missing_blocks(cid).await?;
            if missing.is_empty() {
                state.validated = Some(root);
                state.latest = None;
                state.wanted = None;
                store.alias(stream_id.0.to_vec(), Some(root.into())).await?;
                store.assign_temp_pin(pin.clone(), None).await?;
            } else {
                state.wanted = Some(self.wanted(missing));
            }
        }
        Ok(())
    }

    fn wanted(&self, _wanted: Vec<Cid>) -> Wanted {
        Wanted
    }

    fn lock(&self) -> impl std::ops::DerefMut<Target = StreamMaps> + '_ {
        self.0.mutex.lock().unwrap()
    }
}

struct Wanted;
