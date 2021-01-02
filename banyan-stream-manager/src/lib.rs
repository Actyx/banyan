use actyxos_sdk::tagged::{NodeId, StreamId};
use anyhow::anyhow;
use banyan::forest::{self, BranchCache, CryptoConfig, Transaction};
use clap::{App, Arg, SubCommand};
use forest::FutureResult;
use futures::{future::BoxFuture, prelude::*};
use ipfs_sqlite_block_store::{BlockStore, TempPin};
use libipld::Cid;
use sqlite::{SqliteStore, SqliteStoreWrite};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::{TryFrom, TryInto},
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

struct StreamAlias([u8; 24]);

impl AsRef<[u8]> for StreamAlias {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<StreamId> for StreamAlias {
    fn from(value: StreamId) -> Self {
        let mut result = [0; 24];
        if let Some(id) = value.node_id() {
            result[0..16].copy_from_slice(id.as_ref());
        }
        result[16..24].copy_from_slice(&value.stream_nr().to_le_bytes());
        StreamAlias(result)
    }
}

impl TryFrom<StreamAlias> for StreamId {
    type Error = anyhow::Error;

    fn try_from(value: StreamAlias) -> anyhow::Result<Self> {
        let stream_nr = u64::from_le_bytes(value.0[16..24].try_into()?);
        let node_id = NodeId::from_bytes(&value.0[0..16])?;
        Ok(node_id
            .stream(stream_nr)
            .ok_or(anyhow!("invalid stream nr 0"))?)
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

struct PublishUpdate {
    stream: StreamId,
    blocks: BTreeSet<Link>,
    root: Option<Link>,
}

struct StreamManagerInner {
    mutex: Mutex<StreamMaps>,
    validate_sender: futures::channel::mpsc::UnboundedSender<(StreamId, Link)>,
    publish_sender: futures::channel::mpsc::UnboundedSender<PublishUpdate>,
    forest: Forest,
    node_id: NodeId,
}

#[derive(Clone)]
struct StreamManager(Arc<StreamManagerInner>);

#[derive(Debug, Clone, Copy)]
struct Config {
    node_id: NodeId,
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
        let (publish_sender, publish_receiver) = futures::channel::mpsc::unbounded();
        let res = Self(Arc::new(StreamManagerInner {
            mutex: Mutex::new(StreamMaps {
                own_streams: Default::default(),
                replicated_streams: Default::default(),
            }),
            validate_sender,
            publish_sender,
            node_id: config.node_id,
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
            let writer = this.store().write();
            let txn = Transaction::new(stream.forest.clone(), writer);
            let tree = txn.extend(&stream.latest, events)?;
            // root of the new tree
            let root = tree.link();
            // update the permanent alias
            this.store()
                .lock()
                .alias(StreamAlias::from(stream_id), root.map(Into::into).as_ref())?;
            stream.latest = tree;
            // new blocks
            let blocks = txn.into_writer().into_written();
            this.publish_update(PublishUpdate {
                stream: stream_id,
                blocks,
                root,
            })?;
            Ok(())
        }
        .boxed()
    }

    pub fn update_root(&mut self, stream: StreamId, root: Link) -> Result<()> {
        Ok(self.0.validate_sender.unbounded_send((stream, root))?)
    }

    fn publish_update(&self, update: PublishUpdate) -> Result<()> {
        Ok(self.0.publish_sender.unbounded_send(update)?)
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
            let mut store = store.lock();
            let missing: Vec<Cid> = store.get_missing_blocks(&cid)?;
            if missing.is_empty() {
                state.validated = Some(root);
                state.latest = None;
                state.wanted = None;
                store.alias(StreamAlias::from(stream_id), Some(&cid))?;
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
