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
    iter,
    sync::Arc,
    time::Instant,
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
    pub fn fill<K, V: Serialize>(
        compressed: &[u8],
        from: &mut iter::Peekable<impl Iterator<Item = (K, V)>>,
        zstd_level: i32,
        compressed_size: u64,
        uncompressed_size: u64,
        keys: &mut Vec<K>,
        max_keys: usize,
    ) -> Result<(Self, bool)> {
        let t0 = Instant::now();
        let compressed_size = compressed_size as usize;
        let uncompressed_size = uncompressed_size as usize;
        let mut encoder = Encoder::new(Vec::new(), zstd_level)?;
        // decompress into the encoder, if necessary
        //
        // also init decompressed size
        let mut size = if !compressed.is_empty() {
            // the first ? is to handle the io error from decompress_and_transform, the second to handle the inner io error from write_all
            let (size, data) = decompress_and_transform(compressed, &mut |decompressed| {
                encoder.write_all(decompressed)
            })?;
            data?;
            size
        } else {
            0
        };
        let mut full = false;
        // fill until rough size goal exceeded
        while let Some((_, value)) = from.peek() {
            // do this check here, in case somebody calls us with an already full keys vec
            if keys.len() >= max_keys {
                break;
            }
            let bytes = serde_cbor::to_vec(value)?;
            // if a single item is too big, bail out
            anyhow::ensure!(bytes.len() <= uncompressed_size, "single item too large!");
            // check that we don't exceed the uncompressed_size goal before adding
            if size + bytes.len() > uncompressed_size {
                // we know that the next item does not fit, so we are full even if
                // there is some space left.
                full = true;
                break;
            }
            // this is guaranteed to work because of the peek above.
            // Now we are committed to add the item.
            let (key, _) = from.next().unwrap();
            size += bytes.len();
            encoder.write_all(&bytes)?;
            keys.push(key);
            if encoder.get_ref().len() >= compressed_size {
                break;
            }
        }
        // call finish to write the zstd frame
        let data = encoder.finish()?;
        // log elapsed time and compression rate
        tracing::debug!(
            "ZstdArray::fill elapsed={} compressed={} uncompressed={}",
            t0.elapsed().as_secs_f64(),
            data.len(),
            size
        );
        full |= data.len() >= compressed_size;
        full |= keys.len() >= max_keys;
        full |= size >= uncompressed_size;
        // box into an arc
        Ok((Self::new(data.into()), full))
    }

    /// Get the compressed data
    pub fn compressed(&self) -> &[u8] {
        &self.data
    }

    pub fn items<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        info!("compressed length {}", self.compressed().len());

        let (_, data) = decompress_and_transform(self.compressed(), &mut |uncompressed| {
            info!("uncompressed length {}", uncompressed.len());
            let mut result = Vec::new();
            let mut r = Cursor::new(&uncompressed);
            while r.position() < uncompressed.len() as u64 {
                let mut deserializer = serde_cbor::Deserializer::from_reader(r.by_ref());
                result.push(T::deserialize(&mut deserializer)?);
            }
            Ok(result)
        })?;
        data
    }

    pub fn count(&self) -> Result<u64> {
        Ok(self.items::<serde::de::IgnoredAny>()?.len() as u64)
    }

    /// select the items marked by the iterator and deserialize them into a vec.
    ///
    /// Other items will be skipped when deserializing, saving some unnecessary work.
    pub fn get<T: DeserializeOwned>(&self, index: u64) -> Result<Option<T>> {
        let (_, data) = decompress_and_transform(self.compressed(), &mut |uncompressed| {
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
        })?;
        data
    }

    /// select the items marked by the iterator and deserialize them into a vec.
    ///
    /// Other items will be skipped when deserializing, saving some unnecessary work.
    pub fn select<T: DeserializeOwned>(&self, take: &[bool]) -> Result<Vec<T>> {
        let (_, data) = decompress_and_transform(self.compressed(), &mut |uncompressed| {
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
        })?;
        data
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

    #[test]
    fn zstd_array_fill_oversized() -> anyhow::Result<()> {
        // one byte too large. Does not fit!
        let mut items = vec![(1u8, vec![0u8; 10001])].into_iter().peekable();
        let mut keys = Vec::new();
        let res = ZstdArray::fill(
            &[],
            &mut items,
            10,
            1000,
            10002, // one byte too small
            &mut keys,
            1000,
        );
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().to_string(),
            "single item too large!".to_string()
        );
        assert!(items.peek().is_some());
        // fits exactly
        let mut items = vec![(1usize, vec![0u8; 10000])].into_iter().peekable();
        let mut keys = Vec::new();
        let (_, full) = ZstdArray::fill(
            &[],
            &mut items,
            10,
            1000,
            10003, // exactly the right size
            &mut keys,
            1000,
        )?;
        assert!(full);
        Ok(())
    }

    #[test]
    fn zstd_array_fill_keys() -> anyhow::Result<()> {
        let mut items = vec![
            (1u8, vec![0u8; 1]),
            (2u8, vec![0u8; 1]),
            (3u8, vec![0u8; 1]),
            (4u8, vec![0u8; 1]),
        ]
        .into_iter()
        .peekable();
        let mut keys = Vec::new();
        let (_, full) = ZstdArray::fill(
            &[],
            &mut items,
            10,
            1000,
            10002, // one byte too small
            &mut keys,
            2,
        )?;
        // has reported full
        assert!(full);
        // has taken 2 keys
        assert_eq!(keys.len(), 2);
        // 3 is the first elemeent that is left
        assert_eq!(items.peek().unwrap().0, 3u8);
        Ok(())
    }

    /// basic test to ensure that the decompress works and properly clears the thread local buffer
    #[quickcheck]
    fn zstd_array_fill_roundtrip(first: Vec<u8>, data: Vec<Vec<u8>>) -> anyhow::Result<bool> {
        let bytes = data.iter().map(|x| x.len()).sum::<usize>() as u64;
        let target_size = bytes / 2;
        let initial = ZstdArray::single(&first, 0)?;
        let mut iter = data.iter().cloned().enumerate().peekable();
        let mut keys = Vec::new();
        let (za, _) = ZstdArray::fill(
            &initial.compressed(),
            &mut iter,
            0,
            target_size,
            1024 * 1024 * 4,
            &mut keys,
            usize::max_value(),
        )?;
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
