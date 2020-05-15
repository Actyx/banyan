use std::io::prelude::*;
use std::io::{Cursor, Write};
use std::sync::Arc;
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use crate::tree::{Result};
use zstd::stream::raw::{Decoder as ZDecoder, Encoder as ZEncoder, InBuffer, Operation, OutBuffer};

use zstd::stream::read::Decoder;
use zstd::stream::write::Encoder;

/// An array of zstd compressed data
pub struct ZstdArray {
    data: Arc<[u8]>,
}

impl ZstdArray {
    pub fn new(data: Arc<[u8]>) -> Self {
        Self {
            data,
        }
    }

    pub fn as_ref<'a>(&'a self) -> ZstdArrayRef<'a> {
        ZstdArrayRef {
            data: &self.data,
        }
    }
}

pub struct ZstdArrayRef<'a> {
    data: &'a [u8],
}

const CBOR_ARRAY_START: u8 = (4 << 5) | 31;
const CBOR_BREAK: u8 = 255;

impl<'a> ZstdArrayRef<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
        }
    }

    /// Get the compressed data
    pub fn raw(&self) -> &'a [u8] {
        self.data
    }

    fn decompress_into(&self, mut uncompressed: Vec<u8>) -> Result<Vec<u8>> {
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
        println!("items!!!!");
        let mut uncompressed = Vec::<u8>::new();
        uncompressed.push(CBOR_ARRAY_START);
        let mut uncompressed = self.decompress_into(uncompressed)?;
        uncompressed.push(CBOR_BREAK);
        Ok(serde_cbor::from_slice(&uncompressed)?)
    }
}

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

    pub fn as_ref<'a>(&'a self) -> ZstdArrayRef<'a>
    {
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
        self.encoder.get_mut().flush()?;
        Ok(self)
    }

    /// Writes some data and makes sure the zstd encoder state is flushed.
    //
    pub fn push<T: Serialize>(mut self, value: &T) -> Result<Self> {
        serde_cbor::to_writer(&mut self.encoder, value)?;
        self.encoder.flush()?;
        self.encoder.get_mut().flush()?;
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
            println!("xxx {} {}", w.as_ref().raw().len(), hex::encode(w.as_ref().raw()));
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
