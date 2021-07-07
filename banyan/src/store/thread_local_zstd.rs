//! # ZStd decompressor that uses thread local buffers to prevent allocations
use std::{cell::RefCell, time::Instant};
use zstd::block::Decompressor;

/// minimum size of the buffer.
///
/// The buffer will shrink back to this capacity after each use, so this should be
/// large enough for the vast majority of cases, otherwise it defeats the purpose of
/// having a thread local buffer.
///
/// The maximum memory size is MIN_CAPACITY times the maximum number of threads doing
/// decompression.
///
/// Note that this whole contraption will be worth it mostly when decompressing very
/// small things. When decompressing larger things the overhead of allocating a buffer
/// will be negligible.
const MIN_CAPACITY: usize = 1024 * 1024 * 4;
/// max capacity the buffer will grow to.
///
/// The maximum decompressed size the buffer can grow to before giving up.
const MAX_CAPACITY: usize = 1024 * 1024 * 16;

/// thread-local decompression state
pub(crate) struct DecompressionState {
    /// reused zstd decompressor
    decompressor: Decompressor,
    /// buffer that can grow up to MAX_CAPACITY
    buffer: Vec<u8>,
}

impl DecompressionState {
    fn new() -> Self {
        let buffer: Vec<u8> = Vec::with_capacity(MIN_CAPACITY);
        Self {
            decompressor: Decompressor::new(),
            buffer,
        }
    }

    fn decompress(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let mut cap = MIN_CAPACITY;
        // todo: do not resize but use a temp buffer as soon as we are above min_capacity
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
        let span = tracing::debug_span!("decompress_and_transform");
        let _entered = span.enter();
        let t0 = Instant::now();
        let len = self.decompress(compressed)?;
        let result = f(&self.buffer[0..len]);
        self.buffer.truncate(MIN_CAPACITY);
        self.buffer.shrink_to_fit();
        let dt = t0.elapsed();
        tracing::debug!("decompress_and_transform took {}", dt.as_secs_f64());
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
