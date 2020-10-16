//! # ZStd decompressor that uses thread local buffers to prevent allocations
use std::cell::RefCell;
use zstd::block::Decompressor;

/// minimum size of the buffer
const MIN_CAPACITY: usize = 1024 * 16;
/// max capacity the buffer will grow to.
const MAX_CAPACITY: usize = 1024 * 1024 * 4;

/// thread-local decompression state
pub(crate) struct DecompressionState {
    /// reused zstd decompressor
    decompressor: Decompressor,
    /// buffer that can grow up to MAX_CAPACITY
    buffer: Vec<u8>,
}

impl DecompressionState {
    fn new() -> Self {
        let buffer: Vec<u8> = Vec::with_capacity(1024);
        Self {
            decompressor: Decompressor::new(),
            buffer,
        }
    }

    fn decompress(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let mut cap = MIN_CAPACITY;
        loop {
            self.buffer.resize(cap, 0);
            let res = self
                .decompressor
                .decompress_to_buffer(data, &mut self.buffer);
            if res.is_ok() || cap >= MAX_CAPACITY {
                return res;
            } else {
                cap *= 2;
            }
        }
    }

    fn decompress_and_transform<F, R>(&mut self, compressed: &[u8], f: &mut F) -> std::io::Result<R>
    where
        F: FnMut(&[u8]) -> R,
    {
        let len = self.decompress(compressed)?;
        let result = f(&self.buffer[0..len]);
        self.buffer.truncate(MIN_CAPACITY);
        self.buffer.shrink_to_fit();
        Ok(result)
    }
}

thread_local!(static DECOMPRESSOR: RefCell<DecompressionState> = RefCell::new(DecompressionState::new()));

/// decompress some data into an internal thread-local buffer, and, on success, applies a transform to the buffer
pub fn decompress_and_transform<F, R>(compressed: &[u8], f: &mut F) -> std::io::Result<R>
where
    F: FnMut(&[u8]) -> R,
{
    DECOMPRESSOR.with(|d| d.borrow_mut().decompress_and_transform(compressed, f))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::quickcheck;
    use std::io::Cursor;

    /// basic test to ensure that the decompress works and properly clears the thread local buffer
    #[quickcheck]
    fn thread_local_compression_decompression(data: Vec<u8>) -> anyhow::Result<bool> {
        let cursor = Cursor::new(&data);
        let compressed = zstd::encode_all(cursor, 0)?;
        let decompressed = decompress_and_transform(&compressed, &mut |x| x.to_vec())?;
        Ok(data == decompressed)
    }
}
