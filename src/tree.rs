use crate::czaa::*;
use derive_more::From;
use futures::{future::BoxFuture, prelude::*, stream::{BoxStream, LocalBoxStream}};
use multihash::{Code, Multihash, Sha2_256};
use serde::{
    de::{DeserializeOwned, IgnoredAny},
    Deserialize, Serialize,
};
use std::{
    collections::HashMap,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, RwLock},
};
use tracing::{debug, info, trace};
use derive_more::Display;

type ArcStore = Arc<dyn Store + Send + Sync + 'static>;
type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
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

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct Cid(Vec<u8>);

/// index for a leaf of n events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafIndex {
    // number of events
    count: u64,
    // block is sealed
    sealed: bool,
    // link to the block
    cid: Cid,
}

/// index for a branch node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchIndex {
    // number of events
    count: u64,
    // level of the tree node
    level: u32,
    // block is sealed
    sealed: bool,
    // link to the branch node
    cid: Cid,
}

/// index
#[derive(Debug, Clone, Serialize, Deserialize, From)]
pub enum Index {
    Leaf(LeafIndex),
    Branch(BranchIndex),
}

impl Index {
    fn cid(&self) -> &Cid {
        match self {
            Index::Leaf(x) => &x.cid,
            Index::Branch(x) => &x.cid,
        }
    }
    fn count(&self) -> u64 {
        match self {
            Index::Leaf(x) => x.count,
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

pub trait Store {
    fn put(&self, data: &[u8]) -> BoxFuture<std::io::Result<Cid>>;
    fn get(&self, cid: &Cid) -> BoxFuture<std::io::Result<Arc<[u8]>>>;
}

pub struct TestStore(Arc<RwLock<HashMap<Cid, Arc<[u8]>>>>);

impl TestStore {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
}

impl Store for TestStore {
    fn put(&self, data: &[u8]) -> BoxFuture<std::io::Result<Cid>> {
        let cid = Cid(Sha2_256::digest(data).digest().to_vec());
        self.0
            .as_ref()
            .write()
            .unwrap()
            .insert(cid.clone(), data.into());
        future::ok(cid).boxed()
    }
    fn get(&self, cid: &Cid) -> BoxFuture<std::io::Result<Arc<[u8]>>> {
        let x = self.0.as_ref().read().unwrap();
        if let Some(value) = x.get(cid) {
            future::ok(value.clone()).boxed()
        } else {
            future::err(err("not there")).boxed()
        }
    }
}

fn err(text: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, text)
}

#[derive(Debug, Clone)]
/// fully in memory representation of a branch node
struct Branch {
    // index data for the children
    children: Vec<Index>,
}

impl Branch {
    fn new(children: Vec<Index>) -> Self {
        assert!(!children.is_empty());
        Self { children }
    }
    fn last_child(&mut self) -> Index {
        self.children
            .last()
            .expect("branch can never have 0 children")
            .clone()
    }
    fn last_child_mut(&mut self) -> &mut Index {
        self.children
            .last_mut()
            .expect("branch can never have 0 children")
    }
}

/// fully in memory representation of a leaf node
#[derive(Debug)]
struct Leaf<T> {
    items: CborZstdArrayBuilder<T>,
}

impl<T> Clone for Leaf<T> {
    fn clone(&self) -> Self {
        Self {
            items: self.items.clone(),
        }
    }
}

impl<T> Leaf<T> {
    fn new(items: CborZstdArrayBuilder<T>) -> Self {
        Self { items }
    }
}

enum Node<T> {
    Branch(Branch),
    Leaf(Leaf<T>),
}

enum NodeInfo<'a, T> {
    Branch(&'a BranchIndex, Branch),
    Leaf(&'a LeafIndex, Leaf<T>),
}

impl<T: Serialize + DeserializeOwned + Clone> Leaf<T> {
    fn child_at(&self, offset: u64) -> Result<T> {
        self.items
            .data()
            .items()?
            .get(offset as usize)
            .map(|x| x.clone())
            .ok_or_else(|| err("nope").into())
    }
}

/// A handle for a tree, consisting of the root and some data to access the store
pub struct Tree<T> {
    root: Option<Index>,
    store: ArcStore,
    config: Config,
    branch_cache: RwLock<lru::LruCache<Cid, Branch>>,
    leaf_cache: RwLock<lru::LruCache<Cid, Leaf<T>>>,
    _t: PhantomData<T>,
}

/// basic random access append only async tree
impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> Tree<T> {
    /// predicate to determine if a leaf is sealed
    fn leaf_sealed(&self, bytes: u64, count: u64) -> bool {
        bytes >= self.config.max_leaf_size || count >= self.config.max_leaf_count
    }

    /// create a new leaf index given a cid and some data
    fn new_leaf_index(&self, cid: Cid, data: &[u8]) -> Result<LeafIndex> {
        let count = CborZstdArrayRef::<IgnoredAny>::new(data).items()?.len() as u64;
        let sealed = self.leaf_sealed(data.len() as u64, count);
        Ok(LeafIndex { cid, count, sealed })
    }

    /// load a leaf given a leaf index
    ///
    /// for now this just loads from scratch, but in the future this will load from a cache
    /// of hot leaf nodes.
    async fn load_leaf(&self, index: &LeafIndex) -> Result<Leaf<T>> {
        let bytes = self.store.get(&index.cid).await?;
        let items = CborZstdArrayBuilder::<T>::init(bytes.as_ref(), self.config.zstd_level)?;
        Ok(Leaf::new(items))
    }

    async fn load_leaf_cached(&self, index: &LeafIndex) -> Result<Leaf<T>> {
        match self.leaf_cache.write().unwrap().pop(&index.cid) {
            Some(leaf) => Ok(leaf),
            None => self.load_leaf(index).await,
        }
    }

    /// Creates a tree containing a single item, and returns the index of that tree
    async fn single_leaf(&self, value: &T) -> Result<LeafIndex> {
        let items = CborZstdArrayBuilder::new(self.config.zstd_level)?.push(value)?;
        let cid = self.store.put(items.buffer()).await?;
        let index = LeafIndex {
            cid,
            count: 1,
            sealed: self.leaf_sealed(items.buffer().len() as u64, 1),
        };
        Ok(index)
    }

    /// predicate to determine if a leaf is sealed
    fn branch_sealed(&self, bytes: u64, items: &[Index]) -> bool {
        // a branch with less than 1 children is not considered sealed.
        // if we ever get this situation, we should just panic.
        items.len() > 1
            && items.last().unwrap().sealed()
            && ((bytes >= self.config.max_branch_size)
                || items.len() as u64 >= self.config.max_branch_count)
    }

    /// create a branch index given a cid and some data
    fn new_branch_index(&self, cid: Cid, data: &[u8], children: &[Index]) -> BranchIndex {
        let count = children.iter().map(|c| c.count()).sum();
        let level = children.iter().fold(1, |l, c| l.max(c.level() + 1));
        let sealed = self.branch_sealed(data.len() as u64, children);
        BranchIndex {
            cid,
            count,
            level,
            sealed,
        }
    }

    /// load a branch given a branch index
    ///
    /// for now this just loads from scratch, but in the future this will load from a cache
    /// of hot branch nodes.
    async fn load_branch(&self, index: &BranchIndex) -> Result<Branch> {
        let bytes = self.store.get(&index.cid).await?;
        let children = CborZstdArrayRef::new(bytes.as_ref()).items()?;
        Ok(Branch::new(children))
    }

    async fn load_branch_cached(&self, index: &BranchIndex) -> Result<Branch> {
        match self.branch_cache.write().unwrap().pop(&index.cid) {
            Some(branch) => Ok(branch),
            None => self.load_branch(index).await,
        }
    }

    async fn single_branch(&self, value: Index) -> Result<BranchIndex> {
        let children: CborZstdArrayBuilder<Index> =
            CborZstdArrayBuilder::<Index>::new(self.config.zstd_level)?;
        let children = children.push(&value)?;
        let cid = self.store.put(children.buffer()).await?;
        let index = BranchIndex {
            level: value.level() + 1,
            count: value.count(),
            sealed: false,
            cid,
        };
        Ok(index)
    }

    async fn load_node<'a>(&self, index: &'a Index) -> Result<NodeInfo<'a, T>> {
        Ok(match index {
            Index::Branch(index) => NodeInfo::Branch(index, self.load_branch_cached(index).await?),
            Index::Leaf(index) => NodeInfo::Leaf(index, self.load_leaf_cached(index).await?),
        })
    }

    fn store_leaf(&self, index: LeafIndex, node: Leaf<T>) -> Index {
        self.leaf_cache.write().unwrap().put(index.cid.clone(), node);
        index.into()
    }

    fn store_branch(&self, index: BranchIndex, node: Branch) -> Index {
        self.branch_cache.write().unwrap().put(index.cid.clone(), node);
        index.into()
    }

    fn pushr<'a>(
        &'a self,
        node: &'a Index,
        value: &'a T,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Index>> + 'a>> {
        Box::pin(self.push0(node, value))
    }

    async fn push0(&self, index: &Index, value: &T) -> Result<Index> {
        // calling push0 for a sealed node makes no sense and should not happen!
        assert!(!index.sealed());
        match self.load_node(index).await? {
            NodeInfo::Leaf(index, mut leaf) => {
                leaf.items = leaf.items.push(&value)?;
                let mut index = index.clone();
                // update the index data
                index.count += 1;
                index.sealed =
                    self.leaf_sealed(leaf.items.buffer().len() as u64, index.count);
                index.cid = self.store.put(leaf.items.buffer()).await?;
                Ok(self.store_leaf(index, leaf))
            }
            NodeInfo::Branch(index, mut branch) => {
                let child_index = branch.last_child();
                if !child_index.sealed() {
                    // there is room in the child. Just push it down and update us
                    *branch.last_child_mut() = self.pushr(&child_index, value).await?;
                } else if child_index.level() < index.level - 1 {
                    // there is room for another tree node. Create a new one and push down to it
                    let child_index = self.single_branch(child_index).await?;
                    *branch.last_child_mut() = self.pushr(&child_index.into(), value).await?;
                } else {
                    // all our children are full, we need to append
                    let child_index = self.single_leaf(&value).await?;
                    branch.children.push(child_index.into());
                }
                let data = CborZstdArrayBuilder::<Index>::new(self.config.zstd_level)?;
                let data = data.push_items(branch.children.iter().cloned())?;
                let cid = self.store.put(data.buffer()).await?;
                let mut index = index.clone();
                index.count += 1;
                index.sealed =
                    self.branch_sealed(data.buffer().len() as u64, &branch.children);
                index.cid = cid;
                Ok(self.store_branch(index, branch))
            }
        }
    }

    fn getr<'a>(
        &'a self,
        node: &'a Index,
        offset: u64,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<T>> + 'a>> {
        Box::pin(self.get0(node, offset))
    }

    async fn get0(&self, index: &Index, mut offset: u64) -> Result<T> {
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
            NodeInfo::Leaf(index, node) => node.child_at(offset),
        }
    }

    /// append a single element
    pub async fn push(&mut self, value: &T) -> Result<()> {
        self.root = Some(match &self.root {
            Some(index) => {
                if !index.sealed() {
                    self.push0(index, value).await?
                } else {
                    let index = self.single_branch(index.clone()).await?.into();
                    self.push0(&index, value).await?
                }
            }
            None => self.single_leaf(value).await?.into(),
        });
        Ok(())
    }

    /// element at index
    pub async fn get(&self, offset: u64) -> Result<Option<T>> {
        Ok(match &self.root {
            Some(index) => Some(self.get0(index, offset).await?),
            None => None,
        })
    }

    fn stream0<'a>(&'a self, index: Index) -> LocalBoxStream<'a, Result<T>> {
        let s = async move {
            Ok(match self.load_node(&index).await? {
                NodeInfo::Leaf(index, node) => {
                    let elems: Vec<T> = node.items.data().items()?;
                    stream::iter(elems).map(|x| Ok(x)).left_stream()
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

    pub fn stream<'a>(&'a self) -> impl Stream<Item = Result<T>> + 'a {
        match &self.root {
            Some(index) => self.stream0(index.clone()).left_stream(),
            None => stream::empty().right_stream(),
        }
    }

    fn dumpr<'a>(
        &'a self,
        index: &'a Index,
        prefix: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(self.dump0(index, prefix))
    }

    async fn dump0(&self, index: &Index, prefix: &str) -> Result<()> {
        match self.load_node(index).await? {
            NodeInfo::Leaf(index, leaf) => {
                println!("{}Leaf({}, {})", prefix, index.count, index.sealed);
            }
            NodeInfo::Branch(index, branch) => {
                println!("{}Branch({}, {})", prefix, index.count, index.sealed);
                let prefix = prefix.to_string() + "  ";
                for x in branch.children.iter() {
                    self.dumpr(x, &prefix).await?;
                }
            }
        };
        Ok(())
    }

    pub async fn dump(&self) -> Result<()> {
        match &self.root {
            Some(index) => self.dump0(index, "").await,
            None => {
                println!("empty");
                Ok(())
            }
        }
    }

    /// creates a new tree
    pub fn new(store: ArcStore) -> Self {
        Self::load(store, None)
    }

    /// loads the tree from the store, given a secret and a root cid
    pub fn load(store: ArcStore, root: Option<Index>) -> Self {
        let branch_cache = RwLock::new(lru::LruCache::<Cid, Branch>::new(1000));
        let leaf_cache = RwLock::new(lru::LruCache::<Cid, Leaf<T>>::new(1000));
        Self {                        
            store,
            root,
            config: Config::debug(),
            branch_cache,
            leaf_cache,
            _t: PhantomData,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn count(&self) -> u64 {
        self.root.as_ref().map(|x| x.count()).unwrap_or(0)
    }
}
