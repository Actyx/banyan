use crate::czaa::*;
use futures::future::BoxFuture;
use serde::{
    de::{DeserializeOwned, IgnoredAny},
    Deserialize, Serialize,
};
use std::marker::PhantomData;

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct Cid([u8; 32]);

/// index for a block of n events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockIndex {
    // number of events
    count: u64,
    // block is sealed
    sealed: bool,
    // link to the block
    cid: Cid,
}

const MAX_LEAF_SIZE: usize = 1 << 16;

impl BlockIndex {
    fn new(cid: Cid, data: &[u8]) -> Result<Self> {
        let count = CborZstdArrayRef::<IgnoredAny>::new(data).items()?.len() as u64;
        let sealed = data.len() > MAX_LEAF_SIZE;
        Ok(Self { cid, count, sealed })
    }
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

impl BranchIndex {
    fn new(cid: Cid, data: &[u8], children: &[Index]) -> Self {
        let count = children.iter().map(|c| c.count()).sum();
        let level = children.iter().fold(1, |l, c| l.max(c.level() + 1));
        let sealed = children.iter().all(|c| c.sealed()) && children.len() >= 16;
        Self {
            cid,
            count,
            level,
            sealed,
        }
    }
}

/// index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Index {
    Block(BlockIndex),
    Branch(BranchIndex),
}

impl Index {
    fn cid(&self) -> &Cid {
        match self {
            Index::Block(x) => &x.cid,
            Index::Branch(x) => &x.cid,
        }
    }
    fn count(&self) -> u64 {
        match self {
            Index::Block(x) => x.count,
            Index::Branch(x) => x.count,
        }
    }
    fn sealed(&self) -> bool {
        match self {
            Index::Block(x) => x.sealed,
            Index::Branch(x) => x.sealed,
        }
    }
    fn level(&self) -> u32 {
        match self {
            Index::Block(x) => 0,
            Index::Branch(x) => x.level,
        }
    }
}

pub trait Store {
    fn put(&self, data: &[u8]) -> BoxFuture<std::io::Result<Cid>>;
    fn get(&self, cid: &Cid) -> BoxFuture<std::io::Result<&[u8]>>;
}

type BoxStore = Box<dyn Store>;
type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

/// fully in memory representation of a branch node
struct Branch {
    // index data for this branch
    index: BranchIndex,
    // index data for the children
    children: Vec<Index>,
}

pub struct Tree<T> {
    root: Option<Index>,
    store: Box<dyn Store>,
    _t: PhantomData<T>,
}

/// basic random access append only async tree
impl<T: Serialize + DeserializeOwned> Tree<T> {
    async fn extend<X: Serialize + DeserializeOwned>(
        &self,
        cid: &Cid,
        item: &X,
    ) -> Result<Option<Cid>> {
        let data = self.store.get(&cid).await?;
        let builder = CborZstdArrayBuilder::<X>::init(data, 10)?;
        let builder = builder.push(item)?;
        if builder.buffer().len() < 100000 && builder.len() < 32 {
            let stored = self.store.put(builder.buffer()).await?;
            Ok(Some(stored))
        } else {
            // does not fit
            Ok(None)
        }
    }

    async fn single_leaf(&self, value: &T) -> Result<Index> {
        let builder: CborZstdArrayBuilder<T> = CborZstdArrayBuilder::<T>::new(10)?;
        let builder = builder.push(&value)?;
        let cid = self.store.put(builder.buffer()).await?;
        let index = Index::Block(BlockIndex {
            count: 1,
            sealed: false,
            cid,
        });
        Ok(index)
    }

    async fn single_branch(&self, value: Index) -> Result<Index> {
        let builder: CborZstdArrayBuilder<Index> = CborZstdArrayBuilder::<Index>::new(10)?;
        let builder = builder.push(&value)?;
        let cid = self.store.put(builder.buffer()).await?;
        let index = Index::Branch(BranchIndex {
            level: 1,
            count: value.count(),
            sealed: false,
            cid,
        });
        Ok(index)
    }

    async fn load_branch(&self, index: BranchIndex) -> Result<Branch> {
        let bytes = self.store.get(&index.cid).await?;
        let children = CborZstdArrayRef::new(bytes).items()?;
        Ok(Branch { index, children })
    }

    async fn push_impl(&self, index: &mut Index, value: &T) -> Result<bool> {
        unimplemented!()
    }

    async fn single(&self, value: &T) -> Result<Index> {
        let bytes = CborZstdArrayBuilder::new(10)?.push(value)?;
        let cid = self.store.put(bytes.buffer()).await?;
        Ok(Index::Block(BlockIndex {
            cid,
            count: 1,
            sealed: false,
        }))
    }

    fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// append an element
    pub async fn push(&mut self, value: &T) -> Result<()> {
        if self.is_empty() {
            self.root = Some(self.single(value).await?);
        } else {
        }
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
