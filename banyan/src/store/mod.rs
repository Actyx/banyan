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
pub(crate) use thread_local_zstd::decompress_and_transform;
pub use zstd_dag_cbor_seq::ZstdDagCborSeq;

pub trait BanyanValue: DagCbor + Send + 'static {}

impl<T: DagCbor + Send + Sync + 'static> BanyanValue for T {}

pub type LocalLink = (u64, u64);
pub type GlobalLink = (u128, u64, u64);

pub trait BlockWriter: Send + Sync + 'static {
    /// adds a block to a temporary staging area
    ///
    /// We might have to do this async at some point, but let's keep it sync for now.
    fn put(&self, stream_id: u128, offset: u64, data: Vec<u8>) -> Result<()>;
}

pub trait ReadOnlyStore: Clone + Send + Sync + 'static {
    fn get(&self, stream_id: u128, link: (u64, u64)) -> Result<Box<[u8]>>;
}
