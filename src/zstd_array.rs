use std::io::prelude::*;
use std::io::{Cursor, Write};
use std::sync::Arc;

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

impl<'a> ZstdArrayRef<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
        }
    }

    /// Get the compressed data
    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    /// get a reader for the uncompressed data
    pub fn reader(&self) -> std::io::Result<impl std::io::Read + 'a> {
        Decoder::new(Cursor::new(self.data))
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
    pub fn init(data: &[u8], level: i32) -> std::io::Result<Self> {
        let mut reader = ZstdArrayRef::new(data).reader()?;
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        let res = ZstdArrayBuilder::new(level)?;
        res.push(&data)
    }
}

impl ZstdArrayBuilder {
    pub fn new(level: i32) -> std::io::Result<Self> {
        Ok(Self {
            encoder: Encoder::new(Vec::new(), level)?,
        })
    }

    pub fn data<'a>(&'a self) -> ZstdArrayRef<'a>
    {
        ZstdArrayRef::new(self.encoder.get_ref().as_ref())
    }

    pub fn is_empty(&self) -> bool {
        self.data().data().is_empty()
    }

    /// Writes some data and makes sure the zstd encoder state is flushed.
    //
    pub fn push(mut self, value: &[u8]) -> std::io::Result<Self> {
        self.encoder.write_all(value)?;
        self.encoder.flush()?;
        Ok(self)
    }
}
