//! creation and traversal of banyan trees
use super::index::*;
use crate::store::{BlockWriter, ReadOnlyStore};
use anyhow::Result;
use core::{fmt::Debug, hash::Hash, iter::FromIterator, marker::PhantomData, ops::Range};
use futures::future::BoxFuture;
use libipld::cbor::DagCbor;
use lru::LruCache;
use rand::RngCore;
use std::{
    fmt::Display,
    sync::{Arc, Mutex},
};
mod read;
mod stream;
mod write;
pub(crate) use read::ForestIter;

pub type FutureResult<'a, T> = BoxFuture<'a, Result<T>>;

#[derive(Debug, Clone)]
pub struct BranchCache<T: TreeTypes>(Arc<Mutex<lru::LruCache<T::Link, Branch<T>>>>);

impl<T: TreeTypes> BranchCache<T> {
    pub fn new(capacity: usize) -> Self {
        Self(Arc::new(Mutex::new(LruCache::new(capacity))))
    }

    pub fn get<'a>(&'a self, link: &'a T::Link) -> Option<Branch<T>> {
        self.0.lock().unwrap().get(link).cloned()
    }

    pub fn put(&self, link: T::Link, branch: Branch<T>) {
        self.0.lock().unwrap().put(link, branch);
    }
}

/// Trees can be parametrized with the key type and the sequence type. Also, to avoid a dependency
/// on a link type with all its baggage, we parameterize the link type.
///
/// There might be more types in the future, so this essentially acts as a module for the entire
/// code base.
pub trait TreeTypes: Debug + Send + Sync + Clone + 'static {
    /// key type
    type Key: Debug + PartialEq + Send;
    /// Type for a summary of keys. In some cases this can be the same type as the key type.
    type Summary: Debug + PartialEq + Send;
    /// compact sequence type to be used for indices
    type KeySeq: CompactSeq<Item = Self::Key>
        + DagCbor
        + Clone
        + Debug
        + FromIterator<Self::Key>
        + Send
        + Sync
        + Summarizable<Self::Summary>;
    /// compact sequence type to be used for indices
    type SummarySeq: CompactSeq<Item = Self::Summary>
        + DagCbor
        + Clone
        + Debug
        + FromIterator<Self::Summary>
        + Send
        + Sync
        + Summarizable<Self::Summary>;
    /// link type to use over block boundaries
    type Link: Display + Debug + Hash + Eq + Clone + Copy + Send + Sync + DagCbor;
}

/// Everything that is needed to read trees
#[derive(Debug)]
pub struct ForestInner<T: TreeTypes, V, R> {
    pub(crate) store: R,
    pub(crate) branch_cache: BranchCache<T>,
    pub(crate) crypto_config: CryptoConfig,
    pub(crate) config: Config,
    pub(crate) _tt: PhantomData<(T, V, R)>,
}

#[derive(Debug)]
pub struct Forest<TT: TreeTypes, V, R>(Arc<ForestInner<TT, V, R>>);

impl<TT: TreeTypes, V, R> Clone for Forest<TT, V, R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<TT: TreeTypes, V, R: Clone> Forest<TT, V, R> {
    pub fn new(
        store: R,
        branch_cache: BranchCache<TT>,
        crypto_config: CryptoConfig,
        config: Config,
    ) -> Self {
        Self(Arc::new(ForestInner {
            store,
            branch_cache,
            crypto_config,
            config,
            _tt: PhantomData,
        }))
    }

    pub fn transaction<W: BlockWriter<TT::Link>>(
        &self,
        f: impl FnOnce(R) -> (R, W),
    ) -> Transaction<TT, V, R, W> {
        let (reader, writer) = f(self.0.as_ref().store.clone());
        Transaction {
            read: Self::new(
                reader,
                self.branch_cache.clone(),
                self.crypto_config,
                self.config,
            ),
            writer,
        }
    }
}

impl<T: TreeTypes, V, R> std::ops::Deref for Forest<T, V, R> {
    type Target = ForestInner<T, V, R>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Everything that is needed to write trees. To write trees, you also have to read trees.
pub struct Transaction<T: TreeTypes, V, R, W> {
    read: Forest<T, V, R>,
    writer: W,
}

impl<T: TreeTypes, V, R, W> Transaction<T, V, R, W> {
    /// Get the writer of the transaction.
    ///
    /// This can be used to finally commit the transaction or manually store the content.
    pub fn into_writer(self) -> W {
        self.writer
    }

    pub fn writer(&self) -> &W {
        &self.writer
    }
}

impl<T: TreeTypes, V, R, W> Transaction<T, V, R, W>
where
    R: ReadOnlyStore<T::Link> + Clone + Send + Sync + 'static,
    W: BlockWriter<T::Link> + 'static,
{
    /// create a new transaction.
    ///
    /// It is up to the caller to ensure that the reader reads the writes of the writer,
    /// if complex operations that require that should be performed in the transaction.
    pub fn new(read: Forest<T, V, R>, writer: W) -> Self {
        Self { read, writer }
    }
}

impl<T: TreeTypes, V, R, W> std::ops::Deref for Transaction<T, V, R, W> {
    type Target = Forest<T, V, R>;

    fn deref(&self) -> &Self::Target {
        &self.read
    }
}

#[derive(Debug, Copy, Clone)]
pub struct CryptoConfig {
    /// salsa20 key to decrypt index nodes
    pub index_key: salsa20::Key,
    /// salsa20 key to decrypt value nodes
    pub value_key: salsa20::Key,
}

impl CryptoConfig {
    pub fn random_key() -> salsa20::Key {
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        key.into()
    }
}

impl Default for CryptoConfig {
    fn default() -> Self {
        Self {
            index_key: [0; 32].into(),
            value_key: [0; 32].into(),
        }
    }
}

#[derive(Debug, Copy, Clone)]
/// Configuration for a forest. Includes settings for when a node is considered full
pub struct Config {
    /// maximum number of values in a leaf
    pub max_leaf_count: u64,
    /// maximum number of children in a branch
    pub max_branch_count: u64,
    /// zstd level to use for compression
    pub zstd_level: i32,
    /// rough maximum compressed bytes of a leaf. If a node has more bytes than this, it is considered full.
    ///
    /// note that this might overshoot due to the fact that the zstd encoder has internal state, and it is not possible
    /// to flush after each value without losing compression efficiency. The overshoot is bounded though.
    pub target_leaf_size: u64,
    /// Maximum uncompressed size of leafs. This is limited to prevent accidentally creating
    /// decompression bombs.
    pub max_uncompressed_leaf_size: u64,
}

impl Config {
    /// config that will produce complex tree structures with few values
    ///
    /// keys are hardcoded to 0
    pub fn debug() -> Self {
        Self {
            target_leaf_size: 10000,
            max_leaf_count: 10,
            max_branch_count: 4,
            zstd_level: 10,
            max_uncompressed_leaf_size: 16 * 1024 * 1024,
        }
    }

    /// config that will produce efficient trees
    pub fn debug_fast() -> Self {
        Self {
            target_leaf_size: 1 << 14,
            max_leaf_count: 1 << 14,
            max_branch_count: 32,
            zstd_level: 10,
            max_uncompressed_leaf_size: 16 * 1024 * 1024,
        }
    }

    /// predicate to determine if a leaf is sealed, based on the config
    pub fn branch_sealed<T: TreeTypes>(&self, items: &[Index<T>], level: u32) -> bool {
        // a branch with less than 2 children is never considered sealed.
        // if we ever get this a branch that is too large despite having just 1 child,
        // we should just panic.
        // must have at least 2 children
        if items.len() < 2 {
            return false;
        }
        // all children must be sealed
        if items.iter().any(|x| !x.sealed()) {
            return false;
        }
        // all children must be one level below us
        if items.iter().any(|x| x.level() != level - 1) {
            return false;
        }
        // we must be full
        items.len() as u64 >= self.max_branch_count
    }
}

/// Helper enum for finding a valid branch in a sequence of nodes
pub(crate) enum BranchResult {
    /// We found a sealed branch at the start.
    Sealed(usize),
    /// We found an unsealed branch that goes all the way to the end.
    Unsealed(usize),
    /// We found neither and have to skip a number of elements and recurse.
    Skip(usize),
}

/// Create mode for methods that create branch nodes.
/// This only controls how strict the assertions are.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub(crate) enum CreateMode {
    /// node is created packed, so strict invariant checking should be used
    Packed,
    /// node is created unpacked, so loose invariant checking should be used
    Unpacked,
}

/// A filtered chunk.
/// Contains both data and information about the offsets the data resulted from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredChunk<T: TreeTypes, V, E> {
    /// index range for this chunk
    pub range: Range<u64>,

    /// filtered data (offset, key, value)
    /// Note that this is always in ascending order, even when traversing a tree
    /// in descending order.
    ///
    /// If no elements were filtered, this will have the same number of elements
    /// as the range.
    pub data: Vec<(u64, T::Key, V)>,

    /// arbitrary extra data computed from the leaf or branch index.
    /// If you don't need this you can just pass a fn that returns ()
    pub extra: E,
}
