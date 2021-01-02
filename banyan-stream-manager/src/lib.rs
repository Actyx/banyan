use actyxos_sdk::tagged::{NodeId, StreamId};
use anyhow::anyhow;
use banyan::forest::{self, BranchCache, CryptoConfig};
use clap::{App, Arg, SubCommand};
use fnv::FnvHashMap;
use forest::FutureResult;
use futures::{channel::oneshot, future::BoxFuture, prelude::*};
use ipfs_sqlite_block_store::{BlockStore, TempPin};
use libipld::Cid;
use sqlite::{SqliteStore, SqliteStoreWrite};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::{TryFrom, TryInto},
    num::NonZeroU64,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tag_index::{Tag, TagSet};
use tags::{Key, Sha256Digest, TT};
use tracing::Level;
use tracing_subscriber;
use vec_collections::{VecMap, VecMap1};

mod sqlite;
mod tag_index;
mod tags;
#[cfg(test)]
mod tests;

type Event = serde_cbor::Value;
type Forest = banyan::forest::Forest<TT, Event, SqliteStore>;
type Transaction = banyan::forest::Transaction<TT, Event, SqliteStore, SqliteStoreWrite>;
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

struct StreamAlias([u8; 40]);

impl AsRef<[u8]> for StreamAlias {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<StreamId> for StreamAlias {
    fn from(value: StreamId) -> Self {
        let mut result = [0; 40];
        if let Some(id) = value.node_id() {
            result[0..32].copy_from_slice(id.as_ref());
        }
        result[32..40].copy_from_slice(&value.stream_nr().to_be_bytes());
        StreamAlias(result)
    }
}

impl TryFrom<StreamAlias> for StreamId {
    type Error = anyhow::Error;

    fn try_from(value: StreamAlias) -> anyhow::Result<Self> {
        let node_id = NodeId::from_bytes(&value.0[0..32])?;
        let stream_nr = u64::from_be_bytes(value.0[32..40].try_into()?);
        Ok(node_id
            .stream(stream_nr)
            .ok_or(anyhow!("invalid stream nr 0"))?)
    }
}

struct ReplicatedStreamState {    
    pin: TempPin,
    latest: Option<Link>,
    validated: Option<Link>,
    forest: Forest,
}

impl ReplicatedStreamState {
    fn new(forest: Forest, pin: TempPin) -> Self {
        Self {
            forest,
            pin,
            latest: None,
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
    own_streams: BTreeMap<u64, Arc<tokio::sync::Mutex<OwnStreamState>>>,
    replicated_streams: BTreeMap<StreamId, Arc<tokio::sync::Mutex<ReplicatedStreamState>>>,
}

#[derive(Debug)]
struct PublishUpdate {
    stream_nr: u64,
    blocks: BTreeSet<Link>,
    root: Option<Link>,
}

pub struct Wanted {
    stream_id: StreamId,
    links: BTreeSet<Cid>,
}

impl Wanted {
    pub fn new(stream_id: StreamId, links: BTreeSet<Cid>) -> Self {
        Self { stream_id, links }
    }
}

pub struct WantManager {
    wanted: FnvHashMap<StreamId, Wanted>,
}

impl WantManager {
    pub fn new() -> Self {
        Self {
            wanted: FnvHashMap::default(),
        }
    }
    pub fn add(&mut self, wanted: Wanted) {
        if !wanted.links.is_empty() {
            self.wanted.insert(wanted.stream_id, wanted);
        } else {
            self.wanted.remove(&wanted.stream_id);
        }
    }
    pub fn intersecting_stream_ids(
        &self,
        received: BTreeSet<Cid>,
    ) -> impl Iterator<Item = StreamId> + '_ {
        self.wanted
            .values()
            .filter(move |wanted| !wanted.links.is_disjoint(&received))
            .map(|wanted| wanted.stream_id)
    }
}

impl std::fmt::Debug for Wanted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wanted")
            .field("stream_id", &self.stream_id)
            .field(
                "links",
                &self
                    .links
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .finish()
    }
}

struct StreamManagerInner {
    mutex: Mutex<StreamMaps>,
    validate_sender: futures::channel::mpsc::UnboundedSender<IngestUpdate>,
    publish_sender: futures::channel::mpsc::UnboundedSender<PublishUpdate>,
    wanted_sender: futures::channel::mpsc::UnboundedSender<Wanted>,
    forest: Forest,
    node_id: NodeId,
}

enum IngestUpdate {
    RootUpdate(StreamId, Link),
    SyncUpdate(StreamId),
}

impl IngestUpdate {
    fn stream_id(&self) -> StreamId {
        match self {
            IngestUpdate::RootUpdate(stream_id, _) => *stream_id,
            IngestUpdate::SyncUpdate(stream_id) => *stream_id,
        }
    }
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

impl Config {
    #[cfg(test)]
    fn test() -> Self {
        Self {
            node_id: NodeId::from_bytes(&[0u8; 32]).unwrap(),
            branch_cache: 1000,
            crypto_config: Default::default(),
            forest_config: forest::Config::debug(),
        }
    }
}

impl StreamManager {
    fn get_own_stream(&self, stream_nr: u64) -> Result<Arc<tokio::sync::Mutex<OwnStreamState>>> {
        let mut maps = self.lock();
        let state = maps.own_streams.entry(stream_nr).or_insert_with(|| {
            let forest = self.0.forest.clone();
            Arc::new(tokio::sync::Mutex::new(OwnStreamState::new(forest)))
        });
        Ok(state.clone())
    }

    fn get_replicated_stream(
        &self,
        stream: StreamId,
    ) -> Result<Arc<tokio::sync::Mutex<ReplicatedStreamState>>> {
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

    pub fn new(
        store: SqliteStore,
        config: Config,
    ) -> (
        Self,
        AsyncResult<()>,
        impl Stream<Item = PublishUpdate>,
        impl Stream<Item = Wanted>,
    ) {
        let branch_cache = BranchCache::<TT>::new(config.branch_cache);
        let (validate_sender, validate_receiver) = futures::channel::mpsc::unbounded();
        let (publish_sender, publish_receiver) = futures::channel::mpsc::unbounded();
        let (wanted_sender, wanted_receiver) = futures::channel::mpsc::unbounded();
        let res = Self(Arc::new(StreamManagerInner {
            mutex: Mutex::new(StreamMaps {
                own_streams: Default::default(),
                replicated_streams: Default::default(),
            }),
            validate_sender,
            publish_sender,
            wanted_sender,
            node_id: config.node_id,
            forest: Forest::new(
                store.clone(),
                branch_cache,
                config.crypto_config,
                config.forest_config,
            ),
        }));
        let fut = res.clone().sync_others(store, validate_receiver).boxed();
        (res, fut, publish_receiver, wanted_receiver)
    }

    pub fn get_stream_id(&self, stream_nr: u64) -> Result<StreamId> {
        self.0
            .node_id
            .stream(stream_nr)
            .ok_or(anyhow!("invalid stream nr 0"))
    }

    fn transform_stream(
        &self,
        stream_nr: u64,
        f: impl FnOnce(&Transaction, &Tree) -> Result<Tree> + Send + 'static,
    ) -> AsyncResult<()> {
        let this = self.clone();
        async move {
            let stream = this.get_own_stream(stream_nr)?;
            let mut stream = stream.lock().await;
            let writer = this.store().write();
            let txn = Transaction::new(stream.forest.clone(), writer);
            let tree = f(&txn, &stream.latest)?;
            // root of the new tree
            let root = tree.link();
            // update the permanent alias
            let stream_id = this.get_stream_id(stream_nr)?;
            this.store()
                .lock()
                .alias(StreamAlias::from(stream_id), root.map(Into::into).as_ref())?;
            stream.latest = tree;
            // new blocks
            let blocks = txn.into_writer().into_written();
            this.publish_update(PublishUpdate {
                stream_nr,
                blocks,
                root,
            })?;
            Ok(())
        }
        .boxed()
    }

    /// pack the given stream, publishing the new data
    fn pack(&self, stream_nr: u64) -> AsyncResult<()> {
        let this = self.clone();
        async move {
            let stream = this.get_own_stream(stream_nr)?;
            let mut stream = stream.lock().await;
            let writer = this.store().write();
            let txn = Transaction::new(stream.forest.clone(), writer);
            let tree = txn.pack(&stream.latest).await?;
            // root of the new tree
            let root = tree.link();
            let stream_id = this.get_stream_id(stream_nr)?;
            // update the permanent alias
            this.store()
                .lock()
                .alias(StreamAlias::from(stream_id), root.map(Into::into).as_ref())?;
            stream.latest = tree;
            // new blocks
            let blocks = txn.into_writer().into_written();
            this.publish_update(PublishUpdate {
                stream_nr,
                blocks,
                root,
            })?;
            Ok(())
        }
        .boxed()
    }

    /// append events to a stream, publishing the new data
    pub fn append(&self, stream_nr: u64, events: Vec<(Key, Event)>) -> AsyncResult<()> {
        self.transform_stream(stream_nr, |txn, tree| txn.extend(tree, events))
    }

    pub fn update_root(&self, stream: StreamId, root: Link) -> Result<()> {
        Ok(self.0.validate_sender.unbounded_send(IngestUpdate::RootUpdate(stream, root))?)
    }

    fn publish_update(&self, update: PublishUpdate) -> Result<()> {
        Ok(self.0.publish_sender.unbounded_send(update)?)
    }

    async fn compact(self) -> Result<()> {
        loop {
            let stream_ids = self.lock().own_streams.keys().cloned().collect::<Vec<_>>();
            for stream_id in stream_ids {
                self.pack(stream_id).await?;
            }
            tokio::time::delay_for(Duration::from_secs(60)).await;
        }
    }

    async fn sync_others(
        self,
        store: SqliteStore,
        mut streams: impl Stream<Item = IngestUpdate> + Unpin,
    ) -> Result<()> {
        while let Some(update) = streams.next().await {
            let stream = self.get_replicated_stream(update.stream_id())?;
            let mut state = stream.lock().await;
            match update {
                IngestUpdate::RootUpdate(stream_id, root) => {
                    let cid = Cid::from(root);
                    state.set_root(root, &store)?;
                    let mut store = store.lock();
                    let missing: BTreeSet<Cid> = store.get_missing_blocks(&cid)?;
                    if missing.is_empty() {
                        state.validated = Some(root);
                        state.latest = None;
                        store.alias(StreamAlias::from(stream_id), Some(&cid))?;
                        store.assign_temp_pin(&state.pin, None)?;
                        self.wanted(stream_id, BTreeSet::new(), root)?;
                    } else {
                        self.wanted(stream_id, missing, root)?;
                    }
                }
                IngestUpdate::SyncUpdate(stream_id) => {
                    if let Some(root) = state.latest {
                        let cid = Cid::from(root);
                        let mut store = store.lock();
                        let missing: BTreeSet<Cid> = store.get_missing_blocks(&cid)?;
                        if missing.is_empty() {
                            state.validated = Some(root);
                            state.latest = None;
                            store.alias(StreamAlias::from(stream_id), Some(&cid))?;
                            store.assign_temp_pin(&state.pin, None)?;
                            self.wanted(stream_id, BTreeSet::new(), root)?;
                        } else {
                            self.wanted(stream_id, missing, root)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn wanted(&self, stream_id: StreamId, wanted: BTreeSet<Cid>, root: Link) -> Result<()> {
        Ok(self
            .0
            .wanted_sender
            .unbounded_send(Wanted::new(stream_id, wanted))?)
    }

    fn lock(&self) -> impl std::ops::DerefMut<Target = StreamMaps> + '_ {
        self.0.mutex.lock().unwrap()
    }
}
