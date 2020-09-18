//! creation and traversal of banyan trees
use super::index::*;
use crate::{store::ArcBlockWriter, store::ArcReadOnlyStore};
use anyhow::Result;
use core::{fmt::Debug, hash::Hash, iter::FromIterator, marker::PhantomData, ops::Range};
use futures::future::BoxFuture;
use rand::RngCore;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    io,
    sync::{Arc, RwLock},
};
mod read;
mod stream;
mod write;

pub type FutureResult<'a, T> = BoxFuture<'a, Result<T>>;
pub type BranchCache<T: TreeTypes> = Arc<RwLock<lru::LruCache<T::Link, Branch<T>>>>;

/// Trees can be parametrized with the key type and the sequence type. Also, to avoid a dependency
/// on a link type with all its baggage, we parameterize the link type.
///
/// There might be more types in the future, so this essentially acts as a module for the entire
/// code base.
pub trait TreeTypes: Debug + Send + Sync {
    /// key type. This also doubles as the type for a combination (union) of keys
    type Key: Debug + Eq + Send;
    /// compact sequence type to be used for indices
    type Seq: CompactSeq<Item = Self::Key>
        + Serialize
        + DeserializeOwned
        + Clone
        + Debug
        + FromIterator<Self::Key>
        + Send
        + Sync;
    /// link type to use over block boundaries
    type Link: ToString + Hash + Eq + Clone + Debug + Send + Sync;

    fn serialize_branch(
        links: &[&Self::Link],
        data: Vec<u8>,
        w: impl io::Write,
    ) -> anyhow::Result<()>;
    fn deserialize_branch(reader: impl io::Read) -> anyhow::Result<(Vec<Self::Link>, Vec<u8>)>;
}

/// Everything that is needed to read trees
pub struct ReadForest<T: TreeTypes, V> {
    pub(crate) store: ArcReadOnlyStore<T::Link>,
    pub(crate) branch_cache: BranchCache<T>,
    pub(crate) crypto_config: CryptoConfig,
    pub(crate) config: Config,
    pub(crate) _tt: PhantomData<(T, V)>,
}

/// Everything that is needed to write trees. To write trees, you also have to read trees.
pub struct Forest<T: TreeTypes, V> {
    pub(crate) read: Arc<ReadForest<T, V>>,
    pub(crate) writer: ArcBlockWriter<T::Link>,
}

impl<T: TreeTypes, V> std::ops::Deref for Forest<T, V> {
    type Target = ReadForest<T, V>;

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
        }
    }

    /// config that will produce efficient trees
    pub fn debug_fast() -> Self {
        Self {
            target_leaf_size: 1 << 14,
            max_leaf_count: 1 << 14,
            max_branch_count: 32,
            zstd_level: 10,
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

    /// predicate to determine if a leaf is sealed, based on the config
    pub fn leaf_sealed(&self, bytes: u64, count: u64) -> bool {
        bytes >= self.target_leaf_size || count >= self.max_leaf_count
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
    // filtered data (offset, key, value)
    pub data: Vec<(u64, T::Key, V)>,
    // arbitrary extra data computed from the leaf or branch index.
    // If you don't need this you can just pass a fn that returns ()
    pub extra: E,
}
