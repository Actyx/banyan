use crate::forest::{compactseq_items, compactseq_select_items, CompactSeq, Semigroup};
use crate::ipfs::Cid;
use crate::zstd_array::*;
use crate::store::Store;
use derive_more::{Display, From};
use futures::{
    prelude::*,
    future::BoxFuture,
    stream::LocalBoxStream,
};
use serde::{
    de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer,
};
use std::fmt::Debug;
use std::{
    collections::HashMap,
    fmt,
    marker::PhantomData,
    sync::{Arc, RwLock},
};
use tracing::{debug, info, trace};

type ArcStore = Arc<dyn Store + Send + Sync + 'static>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
type ArcResult<T> = std::result::Result<T, ArcError>;

#[derive(Debug, Clone, Display)]
struct ArcError(Arc<dyn std::error::Error + Send + Sync>);

impl From<Error> for ArcError {
    fn from(x: Error) -> Self {
        Self(x.into())
    }
}

impl std::error::Error for ArcError {}

struct Config {
    max_leaf_size: u64,
    max_leaf_count: u64,
    max_branch_size: u64,
    max_branch_count: u64,
    zstd_level: i32,
}

impl Config {
    pub fn debug() -> Self {
        Self {
            max_leaf_size: 10000,
            max_leaf_count: 10,
            max_branch_size: 1000,
            max_branch_count: 4,
            zstd_level: 10,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_leaf_size: 1 << 12,
            max_leaf_count: 1 << 12,
            max_branch_size: 1 << 16,
            max_branch_count: 32,
            zstd_level: 10,
        }
    }
}

/// index for a leaf of n events
#[derive(Debug, Clone)]
pub struct LeafIndex<T> {
    // block is sealed
    pub sealed: bool,
    // link to the block
    pub cid: Cid,
    //
    pub data: T,
}

impl<T: CompactSeq> LeafIndex<T> {
    pub fn items<'a>(&'a self) -> impl Iterator<Item = T::Item> + 'a {
        compactseq_items(&self.data)
    }
    pub fn select<'a>(
        &'a self,
        it: impl Iterator<Item = bool> + 'a,
    ) -> impl Iterator<Item = (u64, T::Item)> + 'a {
        compactseq_select_items(&self.data, it)
    }
}

/// index for a branch node
#[derive(Debug, Clone)]
pub struct BranchIndex<T> {
    // number of events
    pub count: u64,
    // level of the tree node
    pub level: u32,
    // block is sealed
    pub sealed: bool,
    // link to the branch node
    pub cid: Cid,
    // extra data
    pub data: T,
}

impl<T: CompactSeq> BranchIndex<T> {
    pub fn items<'a>(&'a self) -> impl Iterator<Item = T::Item> + 'a {
        compactseq_items(&self.data)
    }

    pub fn select_data<'a>(
        &'a self,
        it: impl Iterator<Item = bool> + 'a,
    ) -> impl Iterator<Item = (u64, T::Item)> + 'a {
        compactseq_select_items(&self.data, it)
    }
}

#[derive(Debug, Clone, Serialize)]
struct IndexW<'a, T> {
    // number of events
    count: Option<u64>,
    // level of the tree node
    level: Option<u32>,
    // block is sealed
    sealed: bool,
    // link to the branch node
    cid: &'a Cid,
    // extra data
    data: &'a T,
}

impl<'a, T> From<&'a Index<T>> for IndexW<'a, T> {
    fn from(value: &'a Index<T>) -> Self {
        match value {
            Index::Branch(i) => Self {
                count: Some(i.count),
                sealed: i.sealed,
                level: Some(i.level),
                cid: &i.cid,
                data: &i.data,
            },
            Index::Leaf(i) => Self {
                count: None,
                sealed: i.sealed,
                level: None,
                cid: &i.cid,
                data: &i.data,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct IndexR<T> {
    // number of events
    count: Option<u64>,
    // level of the tree node
    level: Option<u32>,
    // block is sealed
    sealed: bool,
    // link to the branch node
    cid: Cid,
    // extra data
    data: T,
}

impl<T> From<IndexR<T>> for Index<T> {
    fn from(v: IndexR<T>) -> Self {
        if let (Some(level), Some(count)) = (v.level, v.count) {
            BranchIndex {
                cid: v.cid,
                data: v.data,
                sealed: v.sealed,
                count,
                level,
            }
            .into()
        } else {
            LeafIndex {
                cid: v.cid,
                data: v.data,
                sealed: v.sealed,
            }
            .into()
        }
    }
}

/// index
#[derive(Debug, Clone, From)]
pub enum Index<T> {
    Leaf(LeafIndex<T>),
    Branch(BranchIndex<T>),
}

use std::result;

impl<T: Serialize> Serialize for Index<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> result::Result<S::Ok, S::Error> {
        IndexW::<T>::from(self).serialize(serializer)
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for Index<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> result::Result<Index<T>, D::Error> {
        IndexR::<T>::deserialize(deserializer).map(Into::into)
    }
}

impl<T> Index<T> {
    fn data(&self) -> &T {
        match self {
            Index::Leaf(x) => &x.data,
            Index::Branch(x) => &x.data,
        }
    }
}

impl<T: CompactSeq> Index<T> {
    fn cid(&self) -> &Cid {
        match self {
            Index::Leaf(x) => &x.cid,
            Index::Branch(x) => &x.cid,
        }
    }
    fn count(&self) -> u64 {
        match self {
            Index::Leaf(x) => x.data.count(),
            Index::Branch(x) => x.count,
        }
    }
    fn sealed(&self) -> bool {
        match self {
            Index::Leaf(x) => x.sealed,
            Index::Branch(x) => x.sealed,
        }
    }
    fn level(&self) -> u32 {
        match self {
            Index::Leaf(x) => 0,
            Index::Branch(x) => x.level,
        }
    }
}

fn err(text: &str) -> crate::Error {
    Box::new(std::io::Error::new(std::io::ErrorKind::Other, text))
}

#[derive(Debug, Clone)]
/// fully in memory representation of a branch node
struct Branch<T> {
    // index data for the children
    children: Vec<Index<T>>,
}

impl<T: Clone> Branch<T> {
    fn new(children: Vec<Index<T>>) -> Self {
        assert!(!children.is_empty());
        Self { children }
    }
    fn last_child(&mut self) -> Index<T> {
        self.children
            .last()
            .expect("branch can never have 0 children")
            .clone()
    }
    fn last_child_mut(&mut self) -> &mut Index<T> {
        self.children
            .last_mut()
            .expect("branch can never have 0 children")
    }
}

/// fully in memory representation of a leaf node
#[derive(Debug)]
struct Leaf {
    items: ZstdArrayBuilder,
}

impl Leaf {
    fn new(items: ZstdArrayBuilder) -> Self {
        Self { items }
    }
}

enum Node<T> {
    Branch(Branch<T>),
    Leaf(Leaf),
}

enum NodeInfo<'a, T> {
    Branch(&'a BranchIndex<T>, Branch<T>),
    Leaf(&'a LeafIndex<T>, Leaf),
}

impl Leaf {
    fn child_at<T: DeserializeOwned>(&self, offset: u64) -> Result<T> {
        self.items
            .as_ref()
            .select((0..=offset).map(|x| x == offset))?
            .into_iter()
            .last()
            .ok_or_else(|| err("nope").into())
    }
}

pub struct Tree<T: TreeTypes, V> {
    root: Option<Index<T::Seq>>,
    forest: Arc<Forest<T>>,
    _t: PhantomData<V>,
}

impl<T: TreeTypes, V> fmt::Debug for Tree<T, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.root {
            Some(root) => write!(f, "Tree {}", root.cid()),
            None => write!(f, "empty tree"),
        }
    }
}

pub trait Query<T: TreeTypes> {
    /// the iterator type
    type IndexIterator: Iterator<Item = bool>;
    /// checks whether a single item that could be a combination of multiple values can possibly match the query
    fn intersects(&self, x: &T::Key) -> bool;
    /// checks wether an individual item does match the query
    fn contains(&self, x: &T::Key) -> bool;
    /// an iterator returning x.count() elements, where each value is a bool indicating if the query does match
    fn containing(&self, offset: u64, x: &LeafIndex<T::Seq>) -> Self::IndexIterator;
    /// an iterator returning x.count() elements, where each value is a bool indicating if the query can match
    fn intersecting(&self, offset: u64, x: &BranchIndex<T::Seq>) -> Self::IndexIterator;
}

impl<V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static, T: TreeTypes>
    Tree<T, V>
{
    pub fn empty(forest: Arc<Forest<T>>) -> Self {
        Self {
            root: None,
            forest,
            _t: PhantomData,
        }
    }

    pub async fn dump(&self) -> Result<()> {
        match &self.root {
            Some(index) => self.forest.dump0(index, "").await,
            None => Ok(()),
        }
    }

    /// append a single element
    pub async fn push(&mut self, key: &T::Key, value: &V) -> Result<()> {
        self.root = Some(match &self.root {
            Some(index) => {
                if !index.sealed() {
                    self.forest.push0(index, key, &value).await?
                } else {
                    let index = self.forest.single_branch(index.clone()).await?.into();
                    self.forest.push0(&index, key, &value).await?
                }
            }
            None => self.forest.single_leaf(key, &value).await?.into(),
        });
        Ok(())
    }

    /// element at index
    pub async fn get(&self, offset: u64) -> Result<Option<(T::Key, V)>> {
        Ok(match &self.root {
            Some(index) => Some(self.forest.get0(index, offset).await?),
            None => None,
        })
    }

    pub fn stream<'a>(&'a self) -> impl Stream<Item = Result<(T::Key, V)>> + 'a {
        match &self.root {
            Some(index) => self.forest.stream0(index.clone()).left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    pub fn stream_filtered<'a, Q: Query<T>>(
        &'a self,
        query: &'a Q,
    ) -> impl Stream<Item = Result<(u64, T::Key, V)>> + 'a {
        match &self.root {
            Some(index) => self
                .forest
                .stream_filtered0(0, query, index.clone())
                .left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn count(&self) -> u64 {
        self.root.as_ref().map(|x| x.count()).unwrap_or(0)
    }
}

fn zip_with_offset<'a, I: Iterator<Item = Index<T>> + 'a, T: CompactSeq + 'a>(
    value: I,
    offset: u64,
) -> impl Iterator<Item = (Index<T>, u64)> + 'a {
    value.scan(offset, |offset, x| {
        *offset += x.count();
        Some((x, *offset))
    })
}

pub trait TreeTypes {
    /// key type. This also doubles as the type for a combination (union) of keys
    type Key: Semigroup + Debug;
    /// compact sequence type to be used for indices
    type Seq: CompactSeq<Item = Self::Key> + Serialize + DeserializeOwned + Clone + Debug;
}

/// a number of trees that are grouped together, sharing common caches
pub struct Forest<T: TreeTypes> {
    store: ArcStore,
    config: Config,
    branch_cache: RwLock<lru::LruCache<Cid, Branch<T::Seq>>>,
    leaf_cache: RwLock<lru::LruCache<Cid, Leaf>>,
    _tt: PhantomData<T>,
}

/// basic random access append only async tree
impl<T> Forest<T>
where
    T: TreeTypes,
{
    /// predicate to determine if a leaf is sealed
    fn leaf_sealed(&self, bytes: u64, count: u64) -> bool {
        bytes >= self.config.max_leaf_size || count >= self.config.max_leaf_count
    }

    // /// create a new leaf index given a cid and some data
    // fn new_leaf_index(&self, cid: Cid, data: &[u8]) -> Result<LeafIndex<T::Seq>> {
    //     let count = CborZstdArrayRef::<IgnoredAny>::new(data).items()?.len() as u64;
    //     let sealed = self.leaf_sealed(data.len() as u64, count);
    //     Ok(LeafIndex { cid, count, sealed })
    // }

    /// load a leaf given a leaf index
    ///
    /// for now this just loads from scratch, but in the future this will load from a cache
    /// of hot leaf nodes.
    async fn load_leaf(&self, index: &LeafIndex<T::Seq>) -> Result<Leaf> {
        let bytes = self.store.get(&index.cid).await?;
        let items = ZstdArrayBuilder::init(bytes.as_ref(), self.config.zstd_level)?;
        Ok(Leaf::new(items))
    }

    async fn load_leaf_cached(&self, index: &LeafIndex<T::Seq>) -> Result<Leaf> {
        match self.leaf_cache.write().unwrap().pop(&index.cid) {
            Some(leaf) => Ok(leaf),
            None => self.load_leaf(index).await,
        }
    }

    /// Creates a tree containing a single item, and returns the index of that tree
    async fn single_leaf<V: Serialize + Debug>(
        &self,
        key: &T::Key,
        value: &V,
    ) -> Result<LeafIndex<T::Seq>> {
        let items = ZstdArrayBuilder::new(self.config.zstd_level)?.push(value)?;
        let cid = self.store.put(items.as_ref().raw()).await?;
        let index = LeafIndex {
            cid,
            sealed: self.leaf_sealed(items.as_ref().raw().len() as u64, 1),
            data: T::Seq::single(key),
        };
        Ok(index)
    }

    /// predicate to determine if a leaf is sealed
    fn branch_sealed(&self, bytes: u64, items: &[Index<T::Seq>], level: u32) -> bool {
        // a branch with less than 2 children is never considered sealed.
        // if we ever get this a branch that is too large despite having just 1 child,
        // we should just panic.
        if let Some(last_child) = items.last() {
            items.len() > 1
                && (last_child.sealed() && last_child.level() >= level - 1)
                && ((bytes >= self.config.max_branch_size)
                    || items.len() as u64 >= self.config.max_branch_count)
        } else {
            false
        }
    }

    // /// create a branch index given a cid and some data
    // fn new_branch_index(&self, cid: Cid, data: &[u8], children: &[Index<T::Seq>]) -> BranchIndex<T::Seq> {
    //     let count = children.iter().map(|c| c.count()).sum();
    //     let level = children.iter().fold(1, |l, c| l.max(c.level() + 1));
    //     let sealed = self.branch_sealed(data.len() as u64, children);
    //     BranchIndex {
    //         cid,
    //         count,
    //         level,
    //         sealed,
    //     }
    // }

    /// load a branch given a branch index
    ///
    /// for now this just loads from scratch, but in the future this will load from a cache
    /// of hot branch nodes.
    async fn load_branch(&self, index: &BranchIndex<T::Seq>) -> Result<Branch<T::Seq>> {
        let bytes = self.store.get(&index.cid).await?;
        let children: Vec<_> = serde_cbor::from_slice(&bytes)?;
        // let children = CborZstdArrayRef::new(bytes.as_ref()).items()?;
        Ok(Branch::<T::Seq>::new(children))
    }

    async fn load_branch_cached(&self, index: &BranchIndex<T::Seq>) -> Result<Branch<T::Seq>> {
        match self.branch_cache.write().unwrap().pop(&index.cid) {
            Some(branch) => Ok(branch),
            None => self.load_branch(index).await,
        }
    }

    async fn single_branch(&self, value: Index<T::Seq>) -> Result<BranchIndex<T::Seq>> {
        // let children: CborZstdArrayBuilder<Index<T::Seq>> =
        //     CborZstdArrayBuilder::<Index<T::Seq>>::new(self.config.zstd_level)?;
        // let children = children.push(&value)?;
        let cbor = serde_cbor::to_vec(&vec![&value])?;
        let cid = self.store.put(&cbor).await?;
        let index = BranchIndex {
            level: value.level() + 1,
            count: value.count(),
            sealed: false,
            cid,
            data: T::Seq::single(&value.data().summarize()),
        };
        Ok(index)
    }

    async fn load_node<'a>(&self, index: &'a Index<T::Seq>) -> Result<NodeInfo<'a, T::Seq>> {
        Ok(match index {
            Index::Branch(index) => NodeInfo::Branch(index, self.load_branch_cached(index).await?),
            Index::Leaf(index) => NodeInfo::Leaf(index, self.load_leaf_cached(index).await?),
        })
    }

    fn store_leaf(&self, index: LeafIndex<T::Seq>, node: Leaf) -> Index<T::Seq> {
        self.leaf_cache
            .write()
            .unwrap()
            .put(index.cid.clone(), node);
        index.into()
    }

    fn store_branch(&self, index: BranchIndex<T::Seq>, node: Branch<T::Seq>) -> Index<T::Seq> {
        self.branch_cache
            .write()
            .unwrap()
            .put(index.cid.clone(), node);
        index.into()
    }

    fn pushr<'a, V: Serialize + Debug>(
        &'a self,
        node: &'a Index<T::Seq>,
        key: &'a T::Key,
        value: &'a V,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Index<T::Seq>>> + 'a>> {
        Box::pin(self.push0(node, key, value))
    }

    /// note: you could make this more efficient by passing in a mut &Index, but then
    /// you would have to be really careful to do all modification after anything that
    /// can fail, otherwise you might end up with an inconsistent state.
    ///
    /// The functional way of threading the index through the call and returning it is
    /// safer to get correct.
    async fn push0<V: Serialize + Debug>(
        &self,
        index: &Index<T::Seq>,
        key: &T::Key,
        value: &V,
    ) -> Result<Index<T::Seq>> {
        // calling push0 for a sealed node makes no sense and should not happen!
        assert!(!index.sealed());
        match self.load_node(index).await? {
            NodeInfo::Leaf(index, mut leaf) => {
                leaf.items = leaf.items.push(&value)?;
                let mut index = index.clone();
                // update the index data
                index.data.push(key);
                index.sealed = self.leaf_sealed(leaf.items.raw().len() as u64, index.data.count());
                index.cid = self.store.put(leaf.items.raw()).await?;
                Ok(self.store_leaf(index, leaf))
            }
            NodeInfo::Branch(index, mut branch) => {
                let child_index = branch.last_child();
                let mut index = index.clone();
                if !child_index.sealed() {
                    // there is room in the child. Just push it down and update us
                    index.data.extend(&key);
                    *branch.last_child_mut() = self.pushr(&child_index, key, value).await?;
                } else if child_index.level() < index.level - 1 {
                    // there is room for another tree node. Create a new one and push down to it
                    index.data.extend(&key);
                    let child_index = self.single_branch(child_index).await?;
                    *branch.last_child_mut() = self.pushr(&child_index.into(), key, value).await?;
                } else {
                    // all our children are full, we need to append
                    let child_index = self.single_leaf(key, &value).await?;
                    // add new index element wiht child summary
                    index.data.push(&child_index.data.summarize());
                    // add actual new child
                    branch.children.push(child_index.into());
                }
                let cbor = serde_cbor::to_vec(&branch.children)?;
                let cid = self.store.put(&cbor).await?;
                // let data = CborZstdArrayBuilder::<Index<T::Seq>>::new(self.config.zstd_level)?;
                // let data = data.push_items(branch.children.iter().cloned())?;
                // let cid = self.store.put(data.buffer()).await?;
                index.count += 1;
                index.sealed = self.branch_sealed(cbor.len() as u64, &branch.children, index.level);
                index.cid = cid;
                Ok(self.store_branch(index, branch))
            }
        }
    }

    fn getr<'a, V: DeserializeOwned + 'a>(
        &'a self,
        node: &'a Index<T::Seq>,
        offset: u64,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(T::Key, V)>> + 'a>> {
        Box::pin(self.get0(node, offset))
    }

    async fn get0<V: DeserializeOwned>(
        &self,
        index: &Index<T::Seq>,
        mut offset: u64,
    ) -> Result<(T::Key, V)> {
        assert!(offset < index.count());
        match self.load_node(index).await? {
            NodeInfo::Branch(index, node) => {
                for child in node.children.iter() {
                    if offset < child.count() {
                        return self.getr(child, offset).await;
                    } else {
                        offset -= child.count();
                    }
                }
                Err(err("index out of bounds").into())
            }
            NodeInfo::Leaf(index, node) => {
                let v = node.child_at::<V>(offset)?;
                let k = index.data.get(offset).unwrap();
                Ok((k, v))
            }
        }
    }

    fn stream0<'a, V: DeserializeOwned>(
        &'a self,
        index: Index<T::Seq>,
    ) -> LocalBoxStream<'a, Result<(T::Key, V)>> {
        let s = async move {
            Ok(match self.load_node(&index).await? {
                NodeInfo::Leaf(index, node) => {
                    let keys = index.items();
                    let elems: Vec<V> = node.items.items()?;
                    let pairs = keys.zip(elems).collect::<Vec<_>>();
                    stream::iter(pairs).map(|x| Ok(x)).left_stream()
                }
                NodeInfo::Branch(index, node) => stream::iter(node.children)
                    .map(move |child| self.stream0(child))
                    .flatten()
                    .right_stream(),
            })
        }
        .try_flatten_stream();
        Box::pin(s)
    }

    fn stream_filtered0<'a, V: DeserializeOwned, Q: Query<T>>(
        &'a self,
        offset: u64,
        query: &'a Q,
        index: Index<T::Seq>,
    ) -> LocalBoxStream<'a, Result<(u64, T::Key, V)>> {
        let s = async move {
            Ok(match self.load_node(&index).await? {
                NodeInfo::Leaf(index, node) => {
                    let matching = query.containing(offset, index).collect::<Vec<_>>();
                    let keys = index.select(matching.iter().cloned());
                    let elems: Vec<V> = node.items.as_ref().select(matching.iter().cloned())?;
                    let pairs = keys
                        .zip(elems)
                        .map(|((o, k), v)| (o + offset, k, v))
                        .collect::<Vec<_>>();
                    stream::iter(pairs).map(|x| Ok(x)).left_stream()
                }
                NodeInfo::Branch(index, node) => {
                    let matching = query.intersecting(offset, index);
                    let offsets = zip_with_offset(node.children.into_iter(), offset);
                    let children = matching
                        .zip(offsets)
                        .filter_map(|(m, c)| if m { Some(c) } else { None });
                    stream::iter(children)
                        .map(move |(child, offset)| self.stream_filtered0(offset, query, child))
                        .flatten()
                        .right_stream()
                }
            })
        }
        .try_flatten_stream();
        Box::pin(s)
    }

    fn dumpr<'a>(
        &'a self,
        index: &'a Index<T::Seq>,
        prefix: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(self.dump0(index, prefix))
    }

    async fn dump0(&self, index: &Index<T::Seq>, prefix: &str) -> Result<()> {
        match self.load_node(index).await? {
            NodeInfo::Leaf(index, leaf) => {
                println!("{}Leaf({}, {})", prefix, index.data.count(), index.sealed);
            }
            NodeInfo::Branch(index, branch) => {
                println!(
                    "{}Branch({}, {} {:?})",
                    prefix, index.count, index.sealed, index.data
                );
                let prefix = prefix.to_string() + "  ";
                for x in branch.children.iter() {
                    self.dumpr(x, &prefix).await?;
                }
            }
        };
        Ok(())
    }

    /// creates a new forest
    pub fn new(store: ArcStore) -> Self {
        let branch_cache = RwLock::new(lru::LruCache::<Cid, Branch<T::Seq>>::new(1000));
        let leaf_cache = RwLock::new(lru::LruCache::<Cid, Leaf>::new(1000));
        Self {
            store,
            config: Config::debug(),
            branch_cache,
            leaf_cache,
            _tt: PhantomData,
        }
    }
}
