//! interface to a content-addressed store

mod branch_cache;
mod mem_cache;
mod mem_store;
mod thread_local_zstd;
mod zstd_dag_cbor_seq;

use crate::error::Error;
pub use branch_cache::BranchCache;
pub use mem_cache::{MemCache, MemWriter};
pub use mem_store::MemStore;
pub(crate) use thread_local_zstd::decompress_and_transform;
pub use zstd_dag_cbor_seq::ZstdDagCborSeq;

use cbor_data::codec::ReadCbor;
use cbor_data::codec::WriteCbor;

pub trait BanyanValue: ReadCbor + WriteCbor + Send + 'static {}

impl<T: ReadCbor + WriteCbor + Send + Sync + 'static> BanyanValue for T {}

pub trait BlockWriter<L>: Send + Sync + 'static {
    /// adds a block to a temporary staging area
    ///
    /// We might have to do this async at some point, but let's keep it sync for now.
    fn put(&mut self, data: Vec<u8>) -> Result<L, Error>;
}

pub trait ReadOnlyStore<L>: Clone + Send + Sync + 'static {
    fn get(&self, link: &L) -> Result<Box<[u8]>, Error>;
}
