
use futures::{future::BoxFuture};
use serde::{de::DeserializeOwned, Serialize, Deserialize};
use std::marker::PhantomData;
use crate::czaa::*;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cid {}

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

/// index for a branch node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchIndex {
    // number of events
    count: u64,
    // block is sealed
    sealed: bool,
    // link to the branch node
    cid: Cid,
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
}

pub trait Store {
    fn put(&self, data: &[u8]) -> BoxFuture<std::io::Result<Cid>>;
    fn get(&self, cid: &Cid) -> BoxFuture<std::io::Result<&[u8]>>;
}

type BoxStore = Box<dyn Store>;

pub struct Tree<T> {
    root: Cid,
    store: Box<dyn Store>,
    _t: PhantomData<T>,
}

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

/// fully in memory representation of a branch node
struct Branch {
    // index data for this branch
    index: BranchIndex,
    // index data for the children
    children: Vec<Index>,
}

/// basic random access append only async tree
impl<T: Serialize + DeserializeOwned> Tree<T> {        

    async fn extend<X: Serialize + DeserializeOwned>(&self, cid: &Cid, item: &X) -> Result<Option<Cid>> {
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
            count: value.count(),
            sealed: false,
            cid,
        });
        Ok(index)
    }

    async fn load_branch(&self, index: BranchIndex) -> Result<Branch> {
        let bytes = self.store.get(&index.cid).await?;
        let children = CborZstdArrayRef::new(bytes).items()?;
        Ok(Branch {
            index,
            children,
        })
    }
    
    async fn push_impl(&self, index: &mut Index, value: &T) -> Result<bool> {        
        unimplemented!()
    }

    /// append an element
    pub async fn push(&mut self, value: &T) -> Result<()> {
        let mut this = Branch::new(self.store.get(&self.root).await?)?;        
        let index = if root.children.is_empty() {
            self.single_leaf(&value).await?
        } else {
            unimplemented!()
        };
        root.children.push(index);
        // root.push(value, self.store)?;
        Ok(())
    }

    /// element at index
    pub async fn at(index: u64) -> Result<Option<T>> {
        Ok(None)
    }

    /// creates a new, empty tree
    pub async fn new(store: Box<dyn Store>) -> Result<Self> {
        let empty_hash = store.put(&[]).await?;
        Ok(Self {
            store,
            root: empty_hash,
            _t: PhantomData
        })
    }

    /// loads the tree from the store, given a secret and a root cid
    pub async fn load(store: Box<dyn Store>, root: Cid) -> Result<Self> {
        Ok(Self {
            store,
            root,
            _t: PhantomData
        })
    }

    /// saves the tree in the store
    pub async fn save() -> Result<Cid> {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "unable to save").into())
    }
}
