//! interface to a content-addressed store
use anyhow::Result;
use libipld::cbor::DagCbor;
mod branch_cache;
mod mem_cache;
mod mem_store;
mod thread_local_zstd;
mod zstd_dag_cbor_seq;

pub use branch_cache::BranchCache;
pub use mem_cache::{MemCache, MemWriter};
pub use mem_store::MemStore;
use std::{convert::TryInto, fmt};
pub(crate) use thread_local_zstd::decompress_and_transform;
pub use zstd_dag_cbor_seq::ZstdDagCborSeq;

pub trait BanyanValue: DagCbor + Send + 'static {}

impl<T: DagCbor + Send + Sync + 'static> BanyanValue for T {}

pub type StreamId = u128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, libipld::DagCbor)]
pub struct LocalLink(u64, u32);

impl LocalLink {
    pub fn new(offset: u64, len: usize) -> anyhow::Result<Self> {
        Ok(Self(offset, len.try_into()?))
    }

    pub fn offset(&self) -> u64 {
        self.0
    }

    pub fn len(&self) -> usize {
        self.1 as usize
    }
}

impl fmt::Display for LocalLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::str::FromStr for LocalLink {
    type Err = anyhow::Error;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        anyhow::bail!("not yet implemented")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GlobalLink(StreamId, LocalLink);

impl GlobalLink {
    pub fn new(stream: StreamId, link: LocalLink) -> Self {
        Self(stream, link)
    }

    pub fn stream_id(&self) -> StreamId {
        self.0
    }

    pub fn link(&self) -> LocalLink {
        self.1
    }
}


impl fmt::Display for GlobalLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::str::FromStr for GlobalLink {
    type Err = anyhow::Error;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        anyhow::bail!("not yet implemented")
    }
}

pub trait BlockWriter: Send + Sync + 'static {
    /// adds a block to a temporary staging area
    ///
    /// We might have to do this async at some point, but let's keep it sync for now.
    fn put(&self, stream_id: StreamId, offset: u64, data: Vec<u8>) -> Result<()>;
}

pub trait ReadOnlyStore: Clone + Send + Sync + 'static {
    fn get(&self, link: GlobalLink) -> Result<Box<[u8]>>;
}
