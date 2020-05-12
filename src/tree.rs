use crate::czaa::*;
use futures::{prelude::*, future::BoxFuture};
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
use multihash::{Multihash, Code, Sha2_256};
use tracing::{debug, info, trace};

const MAX_LEAF_SIZE: usize = 1000; // 1 << 16;
const MAX_BRANCH_SIZE: usize = 100; // 1 << 12;
const MAX_BRANCH_COUNT: usize = 32;
const ZSTD_LEVEL: i32 = 10;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
        self.0.as_ref().write().unwrap().insert(cid.clone(), data.into());
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

type BoxStore = Box<dyn Store>;
type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
fn err(text: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, text)
}

/// fully in memory representation of a branch node
struct Branch {
    // index data for this branch
    index: BranchIndex,
    // index data for the children
    children: Vec<Index>,
}

impl Branch {
    fn new(index: BranchIndex, children: Vec<Index>) -> Self {
        assert!(!children.is_empty());
        Self { index, children }
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
struct Leaf<T> {
    index: LeafIndex,
    items: CborZstdArrayBuilder<T>,
}

/// A handle for a tree, consisting of the root and some data to access the store
pub struct Tree<T> {
    root: Option<Index>,
    store: Box<dyn Store>,
    _t: PhantomData<T>,
}

/// basic random access append only async tree
impl<T: Serialize + DeserializeOwned> Tree<T> {
    fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// predicate to determine if a leaf is sealed
    fn leaf_sealed(&self, bytes: usize, count: u64) -> bool {
        bytes >= MAX_LEAF_SIZE
    }

    /// create a new leaf index given a cid and some data
    fn new_leaf_index(&self, cid: Cid, data: &[u8]) -> Result<LeafIndex> {
        let count = CborZstdArrayRef::<IgnoredAny>::new(data).items()?.len() as u64;
        let sealed = self.leaf_sealed(data.len(), count);
        Ok(LeafIndex { cid, count, sealed })
    }

    /// load a leaf given a leaf index
    ///
    /// for now this just loads from scratch, but in the future this will load from a cache
    /// of hot leaf nodes.
    async fn load_leaf(&self, index: LeafIndex) -> Result<Leaf<T>> {
        let bytes = self.store.get(&index.cid).await?;
        let items = CborZstdArrayBuilder::<T>::init(bytes.as_ref(), ZSTD_LEVEL)?;
        Ok(Leaf { index, items })
    }

    /// Creates a tree containing a single item, and returns the index of that tree
    async fn single_leaf(&self, value: &T) -> Result<Leaf<T>> {
        let items = CborZstdArrayBuilder::new(ZSTD_LEVEL)?.push(value)?;
        let cid = self.store.put(items.buffer()).await?;
        let index = LeafIndex {
            cid,
            count: 1,
            sealed: self.leaf_sealed(items.buffer().len(), 1),
        };
        Ok(Leaf { items, index })
    }

    /// predicate to determine if a leaf is sealed
    fn branch_sealed(&self, bytes: usize, items: &[Index]) -> bool {
        // a branch with less than 1 children is not considered sealed.
        // if we ever get this situation, we should just panic.
        items.len() > 1
            && items.last().unwrap().sealed()
            && ((bytes >= MAX_BRANCH_SIZE) || items.len() >= MAX_BRANCH_COUNT)
    }

    /// create a branch index given a cid and some data
    fn new_branch_index(&self, cid: Cid, data: &[u8], children: &[Index]) -> BranchIndex {
        let count = children.iter().map(|c| c.count()).sum();
        let level = children.iter().fold(1, |l, c| l.max(c.level() + 1));
        let sealed = self.branch_sealed(data.len(), children);
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
    async fn load_branch(&self, index: BranchIndex) -> Result<Branch> {
        let bytes = self.store.get(&index.cid).await?;
        let children = CborZstdArrayRef::new(bytes.as_ref()).items()?;
        Ok(Branch { index, children })
    }

    async fn single_branch(&self, value: Index) -> Result<Branch> {
        let children: CborZstdArrayBuilder<Index> = CborZstdArrayBuilder::<Index>::new(ZSTD_LEVEL)?;
        let children = children.push(&value)?;
        let cid = self.store.put(children.buffer()).await?;
        let index = BranchIndex {
            level: value.level() + 1,
            count: value.count(),
            sealed: false,
            cid,
        };
        Ok(Branch {
            children: vec![value],
            index,
        })
    }

    fn pushr<'a>(
        &'a self,
        node: Index,
        value: &'a T,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Index>> + 'a>> {
        Box::pin(self.push0(node, value))
    }

    async fn push0(&self, node: Index, value: &T) -> Result<Index> {
        // calling push for a sealed node makes no sense and should not happen!
        assert!(!node.sealed());
        match node {
            Index::Leaf(data) => {
                println!("extending leaf node {:?}", data.count);
                let mut leaf = self.load_leaf(data).await?;
                leaf.items = leaf.items.push(&value)?;
                // update the index data
                leaf.index.count += 1;
                leaf.index.sealed = self.leaf_sealed(leaf.items.buffer().len(), leaf.index.count);
                leaf.index.cid = self.store.put(leaf.items.buffer()).await?;
                println!("extending leaf node {} {}", leaf.index.count, leaf.index.sealed);
                Ok(Index::Leaf(leaf.index))
            }
            Index::Branch(data) => {
                let mut branch = self.load_branch(data).await?;
                let child_index = branch.last_child();
                if !child_index.sealed() {
                    println!("extend child");
                    // there is room in the child. Just push it down and update us
                    *branch.last_child_mut() = self.pushr(child_index, value).await?;
                } else if child_index.level() < branch.index.level - 1 {
                    // there is room for another tree node. Create a new one and push down to it
                    let child = self.single_branch(child_index).await?;
                    *branch.last_child_mut() =
                        self.pushr(Index::Branch(child.index), value).await?;
                } else {
                    // all our children are full, we need to append
                    let child = self.single_leaf(&value).await?;
                    branch.children.push(Index::Leaf(child.index));
                }
                let data = CborZstdArrayBuilder::<Index>::new(ZSTD_LEVEL)?;
                let data = data.push_items(branch.children.iter().cloned())?;
                let cid = self.store.put(data.buffer()).await?;
                branch.index.count += 1;
                branch.index.sealed = self.branch_sealed(data.buffer().len(), &branch.children);
                branch.index.cid = cid;
                Ok(Index::Branch(branch.index))
            }
        }
    }

    /// append an element
    pub async fn push(&mut self, value: &T) -> Result<()> {
        self.root = Some(match &self.root {
            Some(index) => 
                if !index.sealed() {
                    self.push0(index.clone(), value).await?
                } else {
                    let index = Index::Branch(self.single_branch(index.clone()).await?.index);
                    self.push0(index.clone(), value).await?                    
                }
            None => Index::Leaf(self.single_leaf(value).await?.index),
        });
        Ok(())
    }

    /// element at index
    pub async fn at(index: u64) -> Result<Option<T>> {
        Ok(None)
    }

    /// creates a new tree
    pub fn new(store: Box<dyn Store>) -> Self {
        Self::load(store, None)
    }

    /// loads the tree from the store, given a secret and a root cid
    pub fn load(store: Box<dyn Store>, root: Option<Index>) -> Self {
        Self {
            store,
            root,
            _t: PhantomData,
        }
    }

    /// saves the tree in the store
    pub async fn save() -> Result<Cid> {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "unable to save").into())
    }
}
