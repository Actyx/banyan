//! This module provides some utilities to work with zstd compressed arrays of cbor values
use anyhow::Result;
use serde::{
    de::{DeserializeOwned, IgnoredAny},
    Deserialize, Serialize,
};
use std::{
    io::{prelude::*, Cursor, Write},
    sync::Arc,
};
use tracing::*;
use zstd::stream::{
    raw::{Decoder as ZDecoder, Operation, OutBuffer},
    write::Encoder,
};

/// An array of zstd compressed data
pub struct ZstdArray {
    data: Arc<[u8]>,
}

impl ZstdArray {
    pub fn new(data: Arc<[u8]>) -> Self {
        Self { data }
    }

    pub fn as_ref<'a>(&'a self) -> ZstdArrayRef<'a> {
        ZstdArrayRef { data: &self.data }
    }
}

impl std::fmt::Debug for ZstdArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ZstdArray")
    }
}

pub struct ZstdArrayRef<'a> {
    data: &'a [u8],
}

impl<'a> ZstdArrayRef<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// Get the compressed data
    pub fn raw(&self) -> &'a [u8] {
        self.data
    }

    #[allow(dead_code)]
    fn decompress_into_broken(&self, mut uncompressed: Vec<u8>) -> Result<Vec<u8>> {
        let data = self.raw();
        let mut c = Cursor::new(data);
        while c.position() < data.len() as u64 {
            let mut reader = zstd::stream::read::Decoder::new(c.by_ref())?.single_frame();
            reader.read_to_end(&mut uncompressed)?;
        }
        Ok(uncompressed)
    }

    fn decompress_into(&self, uncompressed: Vec<u8>) -> Result<Vec<u8>> {
        // let data = self.raw();
        // let mut writer = zstd::stream::write::Decoder::new(uncompressed.by_ref())?;
        // writer.write_all(data)?;
        // writer.flush()?;
        // Ok(uncompressed)
        self.decompress_into_lowlevel(uncompressed)
    }

    #[allow(dead_code)]
    fn decompress_into_lowlevel(&self, mut uncompressed: Vec<u8>) -> Result<Vec<u8>> {
        // let mut cipher = (self.mk_cipher)();
        // todo: avoid cloning the whole thing, but have a buffer for stream apply and decompression source
        let data = self.data.to_vec();
        // cipher.apply_keystream(&mut data);
        let mut src = zstd::stream::raw::InBuffer::around(&data);
        // todo: thread local buffers that grow dynamically
        let mut tmp = [0u8; 4096 * 100];
        let mut decompressor = ZDecoder::new()?;
        // decompress until input is consumed
        loop {
            let mut out = OutBuffer::around(&mut tmp);
            let _ = decompressor.run(&mut src, &mut out)?;
            let n = out.pos;
            uncompressed.extend_from_slice(&tmp[..n]);
            if src.pos == src.src.len() {
                break;
            }
        }
        loop {
            let mut out = OutBuffer::around(&mut tmp);
            let remaining = decompressor.flush(&mut out)?;
            let n = out.pos;
            uncompressed.extend_from_slice(&tmp[..n]);
            if remaining == 0 {
                break;
            }
        }
        Ok(uncompressed)
    }

    pub fn items<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        info!("compressed length {}", self.raw().len());
        let uncompressed = self.decompress_into(Vec::new())?;
        info!("uncompressed length {}", uncompressed.len());
        let mut result = Vec::new();
        let mut r = Cursor::new(&uncompressed);
        while r.position() < uncompressed.len() as u64 {
            let mut deserializer = serde_cbor::Deserializer::from_reader(r.by_ref());
            result.push(T::deserialize(&mut deserializer)?);
        }
        Ok(result)
    }

    /// select the items marked by the iterator and deserialize them into a vec.
    ///
    /// Other items will be skipped when deserializing, saving some unnecessary work.
    pub fn get<T: DeserializeOwned>(&self, index: u64) -> Result<Option<T>> {
        let uncompressed = self.decompress_into(Vec::new())?;
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
    }

    /// select the items marked by the iterator and deserialize them into a vec.
    ///
    /// Other items will be skipped when deserializing, saving some unnecessary work.
    pub fn select<T: DeserializeOwned>(
        &self,
        mut take: impl Iterator<Item = bool>,
    ) -> Result<Vec<T>> {
        let uncompressed = self.decompress_into(Vec::new())?;
        let mut result: Vec<T> = Vec::new();
        let mut r = Cursor::new(&uncompressed);
        while r.position() < uncompressed.len() as u64 {
            if let Some(p) = take.next() {
                let mut deserializer = serde_cbor::Deserializer::from_reader(r.by_ref());
                if p {
                    result.push(T::deserialize(&mut deserializer)?);
                } else {
                    IgnoredAny::deserialize(&mut deserializer)?;
                }
            } else {
                break;
            }
        }
        Ok(result)
    }
}

// struct CborIterator<'a, T, P> {
//     reader: Cursor<&'a [u8]>,
//     take: P,
//     _p: PhantomData<T>,
// }

// impl<'a, T: DeserializeOwned, P: FnMut(usize) -> bool> Iterator for CborIterator<'a, T, P> {
//     type Item = T;
//     fn next(&mut self) -> Option<T> {
//         while self.reader.position() < self.reader.get_ref().len() as u64 {
//             if self.take() {

//             }
//         }
//         None
//     }
// }

pub struct ZstdArrayBuilder {
    encoder: zstd::stream::write::Encoder<Vec<u8>>,
}

impl std::fmt::Debug for ZstdArrayBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ZstdArrayBuilder")
    }
}

impl ZstdArrayBuilder {
    pub fn init(data: &[u8], level: i32) -> Result<Self> {
        let decompressed = ZstdArrayRef::new(data).decompress_into(Vec::new())?;
        let res = Self::new(level)?;
        let res = res.push_bytes(&decompressed)?;
        Ok(res)
    }

    pub fn new(level: i32) -> std::io::Result<Self> {
        Ok(Self {
            encoder: Encoder::new(Vec::new(), level)?,
        })
    }

    pub fn as_ref<'a>(&'a self) -> ZstdArrayRef<'a> {
        ZstdArrayRef::new(self.encoder.get_ref().as_ref())
    }

    pub fn raw(&self) -> &[u8] {
        self.as_ref().raw()
    }

    pub fn is_empty(&self) -> bool {
        self.raw().is_empty()
    }

    pub fn items<T: DeserializeOwned>(&self) -> Result<Vec<T>> {
        self.as_ref().items()
    }

    /// Writes some data and makes sure the zstd encoder state is flushed.
    //
    pub fn push_bytes(mut self, value: &[u8]) -> Result<Self> {
        self.encoder.write_all(value)?;
        self.encoder.flush()?;
        Ok(self)
    }

    /// Writes some data and makes sure the zstd encoder state is flushed.
    //
    pub fn push<T: Serialize>(mut self, value: &T) -> Result<Self> {
        serde_cbor::to_writer(&mut self.encoder, value)?;
        self.encoder.flush()?;
        Ok(self)
    }

    /// fill the array from a source, until the compressed size exceeds the given size.
    ///
    /// note that the encoder will be flushed just once at the end of the fill op, so the size might be
    /// significantly above the target size.
    pub fn fill<T: Serialize>(
        mut self,
        mut from: impl FnMut() -> Option<T>,
        compressed_size: u64,
    ) -> Result<Self> {
        while (self.raw().len() as u64) < compressed_size {
            if let Some(value) = from() {
                serde_cbor::to_writer(&mut self.encoder, &value)?;
            } else {
                break;
            }
        }
        self.encoder.flush()?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_build() -> Result<()> {
        let mut w = ZstdArrayBuilder::new(10)?;
        let mut expected: Vec<u64> = Vec::new();
        for i in 0u64..10 {
            w = w.push(&i)?;
            expected.push(i);
            println!(
                "xxx {} {}",
                w.as_ref().raw().len(),
                hex::encode(w.as_ref().raw())
            );
            let items: Vec<u64> = w.as_ref().items()?;
            assert_eq!(items, expected);
        }
        Ok(())
    }

    #[test]
    fn read_builder() -> Result<()> {
        let mut w = ZstdArrayBuilder::new(10)?;
        let mut expected: Vec<u64> = Vec::new();
        for i in 0u64..100 {
            w = w.push(&i)?;
            expected.push(i);
            w = ZstdArrayBuilder::init(w.as_ref().raw(), 10)?;
            let items: Vec<u64> = w.as_ref().items()?;
            assert_eq!(items, expected);
        }
        Ok(())
    }
}
