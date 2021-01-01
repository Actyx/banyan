use anyhow::anyhow;
use banyan::forest::{self, BranchCache, CryptoConfig};
use clap::{App, Arg, SubCommand};
use forest::FutureResult;
use futures::{future::BoxFuture, prelude::*};
use ipfs_sqlite_block_store::{BlockStore, TempPin};
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
    pin: TempPin,
    latest: Option<Link>,
    validated: Option<Link>,
    wanted: Option<Wanted>,
    forest: Forest,
}

impl ReplicatedStreamState {
    fn new(forest: Forest, pin: TempPin) -> Self {
        Self {
            forest,
            pin,
            latest: None,
            wanted: None,
            validated: None,
        }
    }

    fn set_root(&mut self, root: Link, store: &SqliteStore) -> Result<()> {
        let same_root = self.latest.as_ref().map(|r| r == &root).unwrap_or_default();
        if !same_root {
            let cid = root.into();
            store.lock().assign_temp_pin(&self.pin, Some(cid))?;
            self.latest = Some(root);
        }
        Ok(())
    }
}

struct StreamMaps {
    own_streams: BTreeMap<StreamId, Arc<tokio::sync::Mutex<OwnStreamState>>>,
    replicated_streams: BTreeMap<StreamId, Arc<tokio::sync::Mutex<ReplicatedStreamState>>>,
}

struct StreamManagerInner {
    mutex: Mutex<StreamMaps>,
    validate_sender: futures::channel::mpsc::UnboundedSender<(StreamId, Link)>,
    forest: Forest,
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
        let mut maps = self.lock();
        let state = maps.own_streams.entry(stream).or_insert_with(|| {
            let forest = self.0.forest.clone();
            Arc::new(tokio::sync::Mutex::new(OwnStreamState::new(forest)))
        });
        Ok(state.clone())
    }

    fn get_replicated_stream(
        &self,
        stream: StreamId,
    ) -> anyhow::Result<Arc<tokio::sync::Mutex<ReplicatedStreamState>>> {
        let mut maps = self.lock();
        let state = maps.replicated_streams.entry(stream).or_insert_with(|| {
            let pin = self.0.forest.store().lock().temp_pin();
            let forest = self.0.forest.clone();
            Arc::new(tokio::sync::Mutex::new(ReplicatedStreamState::new(
                forest, pin,
            )))
        });
        Ok(state.clone())
    }

    fn store(&self) -> &SqliteStore {
        self.0.forest.store()
    }

    pub fn new(store: SqliteStore, config: Config) -> (Self, AsyncResult<()>) {
        let branch_cache = BranchCache::<TT>::new(config.branch_cache);
        let (validate_sender, validate_receiver) = futures::channel::mpsc::unbounded();
        let res = Self(Arc::new(StreamManagerInner {
            mutex: Mutex::new(StreamMaps {
                own_streams: Default::default(),
                replicated_streams: Default::default(),
            }),
            validate_sender,
            forest: Forest::new(
                store.clone(),
                branch_cache,
                config.crypto_config,
                config.forest_config,
            ),
        }));
        let fut = res.clone().validate(store, validate_receiver).boxed();
        (res, fut)
    }

    pub fn append(&self, stream_id: StreamId, events: Vec<(Key, Event)>) -> AsyncResult<()> {
        let this = self.clone();
        async move {
            let stream = this.get_own_stream(stream_id)?;
            let mut stream = stream.lock().await;
            let pin = this.store().lock().temp_pin();
            let w = SqliteStoreWrite(this.store().clone(), pin);
            let txn = stream.forest.transaction(|x| (x, w));
            let tree = txn.extend(&stream.latest, events)?;
            this.store()
                .lock()
                .alias(stream_id.0.to_vec(), tree.link().map(Into::into).as_ref())?;
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
        store: SqliteStore,
        mut streams: impl Stream<Item = (StreamId, Link)> + Unpin,
    ) -> Result<()> {
        while let Some((stream_id, root)) = streams.next().await {
            let cid = Cid::from(root);
            let stream = self.get_replicated_stream(stream_id)?;
            let mut state = stream.lock().await;
            state.set_root(root, &store)?;
            let missing: Vec<Cid> = store.lock().get_missing_blocks(&cid)?;
            if missing.is_empty() {
                state.validated = Some(root);
                state.latest = None;
                state.wanted = None;
                let mut store = store.lock();
                store.alias(stream_id.0.to_vec(), Some(&cid))?;
                store.assign_temp_pin(&state.pin, None)?;
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
