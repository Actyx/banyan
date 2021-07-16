//! # ZStd decompressor that uses thread local buffers to prevent allocations
use std::cell::RefCell;
use zstd::block::Decompressor;

/// The size of the thread local buffer
const MIN_CAPACITY: usize = 1024 * 1024 * 4;

/// Max capacity we are going to allocate.
const MAX_CAPACITY: usize = 1024 * 1024 * 16;

/// thread-local decompression state
pub(crate) struct DecompressionState {
    /// reused zstd decompressor
    decompressor: Decompressor,
    buffer: Vec<u8>,
}

impl DecompressionState {
    fn new() -> Self {
        Self {
            decompressor: Decompressor::new(),
            buffer: vec![0u8; MIN_CAPACITY],
        }
    }

    /// Decompress some data and apply a transform to it, e.g. deserialization.
    ///
    /// Returns the result of the transform and the uncompressed size.
    fn decompress_and_transform<F, R>(
        &mut self,
        compressed: &[u8],
        f: &mut F,
    ) -> std::io::Result<(usize, R)>
    where
        F: FnMut(&[u8]) -> R,
    {
        let capacity = Decompressor::upper_bound(compressed)
            .unwrap_or(MAX_CAPACITY)
            .min(MAX_CAPACITY);
        let mut tmp = Vec::new();
        let buffer = if capacity <= MIN_CAPACITY {
            &mut self.buffer[..]
        } else {
            tmp.resize(capacity, 0u8);
            &mut tmp
        };

        let span = tracing::debug_span!("decompress_and_transform");
        let _entered = span.enter();
        let len = self.decompressor.decompress_to_buffer(compressed, buffer)?;
        let result = f(&buffer[0..len]);
        Ok((len, result))
    }
}

thread_local!(static DECOMPRESSOR: RefCell<DecompressionState> = RefCell::new(DecompressionState::new()));

/// decompress some data into an internal thread-local buffer, and, on success, applies a transform to the buffer
///
/// returns the result of the function call and the size of the
pub fn decompress_and_transform<F, R>(compressed: &[u8], f: &mut F) -> std::io::Result<(usize, R)>
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
        let (size, decompressed) = decompress_and_transform(&compressed, &mut |x| x.to_vec())?;
        Ok(size == decompressed.len() && data == decompressed)
    }
}
