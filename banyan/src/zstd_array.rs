//! Utilities to work with zstd compressed arrays of cbor values
use crate::thread_local_zstd::decompress_and_transform;
use anyhow::Result;
use core::fmt;
use serde::{
    de::{DeserializeOwned, IgnoredAny},
    Deserialize, Serialize,
};
use std::{
    io::{prelude::*, Cursor, Write},
    sync::Arc,
};
use tracing::*;
use zstd::stream::write::Encoder;

/// An array of zstd compressed data
pub struct ZstdArray {
    data: Arc<[u8]>,
}

impl From<ZstdArray> for Arc<[u8]> {
    fn from(value: ZstdArray) -> Self {
        value.data
    }
}

impl ZstdArray {
    pub fn new(data: Arc<[u8]>) -> Self {
        Self { data }
    }

    /// create ZStdArray from a single serializable item
    pub fn single<T: Serialize>(value: &T, level: i32) -> Result<Self> {
        let mut encoder = Encoder::new(Vec::new(), level)?;
        serde_cbor::to_writer(&mut encoder, value)?;
        // call finish to write the zstd frame
        let data = encoder.finish()?;
        // box into an arc
        Ok(Self::new(data.into()))
    }

    /// create a ZStdArray by filling from an iterator
    pub fn fill<T: Serialize>(
        compressed: &[u8],
        mut from: impl FnMut() -> Option<T>,
        level: i32,
        compressed_size: u64,
    ) -> Result<Self> {
        let mut encoder = Encoder::new(Vec::new(), level)?;
        // decompress into the encoder, if necessary
        if !compressed.is_empty() {
            // the first ? is to handle the io error from decompress_and_transform, the second to handle the inner io error from write_all
            decompress_and_transform(compressed, &mut |decompressed| {
                encoder.write_all(decompressed)
            })??;
        }
        // fill until rough size goal exceeded
        while (encoder.get_ref().len() as u64) < compressed_size {
            if let Some(value) = from() {
                serde_cbor::to_writer(&mut encoder, &value)?;
            } else {
                break;
            }
        }
        // call finish to write the zstd frame
        let data = encoder.finish()?;
        // box into an arc
        Ok(Self::new(data.into()))
    }

    /// Get the compressed data
    pub fn compressed(&self) -> &[u8] {
        &self.data
    }

    pub fn items<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        info!("compressed length {}", self.compressed().len());

        decompress_and_transform(self.compressed(), &mut |uncompressed| {
            info!("uncompressed length {}", uncompressed.len());
            let mut result = Vec::new();
            let mut r = Cursor::new(&uncompressed);
            while r.position() < uncompressed.len() as u64 {
                let mut deserializer = serde_cbor::Deserializer::from_reader(r.by_ref());
                result.push(T::deserialize(&mut deserializer)?);
            }
            Ok(result)
        })?
    }

    pub fn count(&self) -> Result<u64> {
        Ok(self.items::<serde::de::IgnoredAny>()?.len() as u64)
    }

    /// select the items marked by the iterator and deserialize them into a vec.
    ///
    /// Other items will be skipped when deserializing, saving some unnecessary work.
    pub fn get<T: DeserializeOwned>(&self, index: u64) -> Result<Option<T>> {
        decompress_and_transform(self.compressed(), &mut |uncompressed| {
            let mut r = Cursor::new(&uncompressed);
            let mut remaining = index;
            while r.position() < uncompressed.len() as u64 {
                let mut deserializer = serde_cbor::Deserializer::from_reader(r.by_ref());
                if remaining > 0 {
                    IgnoredAny::deserialize(&mut deserializer)?;
                    remaining -= 1;
                } else {
                    return Ok(Some(T::deserialize(&mut deserializer)?));
                }
            }
            Ok(None)
        })?
    }

    /// select the items marked by the iterator and deserialize them into a vec.
    ///
    /// Other items will be skipped when deserializing, saving some unnecessary work.
    pub fn select<T: DeserializeOwned>(&self, take: &[bool]) -> Result<Vec<T>> {
        decompress_and_transform(self.compressed(), &mut |uncompressed| {
            let mut result: Vec<T> = Vec::new();
            let mut r = Cursor::new(&uncompressed);
            let mut i: usize = 0;
            while r.position() < uncompressed.len() as u64 {
                if i < take.len() {
                    let mut deserializer = serde_cbor::Deserializer::from_reader(r.by_ref());
                    if take[i] {
                        result.push(T::deserialize(&mut deserializer)?);
                    } else {
                        IgnoredAny::deserialize(&mut deserializer)?;
                    }
                    i += 1;
                } else {
                    break;
                }
            }
            Ok(result)
        })?
    }
}

impl fmt::Debug for ZstdArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ZstdArray")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::quickcheck;
    use std::collections::VecDeque;

    /// basic test to ensure that the decompress works and properly clears the thread local buffer
    #[quickcheck]
    fn zstd_array_fill_roundtrip(first: Vec<u8>, data: Vec<Vec<u8>>) -> anyhow::Result<bool> {
        let bytes = data.iter().map(|x| x.len()).sum::<usize>() as u64;
        let target_size = bytes / 2;
        let initial = ZstdArray::single(&first, 0)?;
        let mut x: VecDeque<Vec<u8>> = data.clone().into();
        let za = ZstdArray::fill(&initial.compressed(), || x.pop_front(), 0, target_size)?;
        // println!("compressed={} n={} bytes={}", za.compressed().len(), data.len(), bytes);
        let mut decompressed = za.items::<Vec<u8>>()?;
        let first1 = decompressed
            .splice(0..1, std::iter::empty())
            .collect::<Vec<_>>();
        // first item must always be included
        if first != first1[0] {
            return Ok(false);
        }
        // remaining items must match input up to where they fit in
        if decompressed[..] != data[..decompressed.len()] {
            return Ok(false);
        }
        //
        if decompressed.len() < data.len() && (za.compressed().len() as u64) < target_size {
            return Ok(false);
        }
        Ok(true)
    }
}
