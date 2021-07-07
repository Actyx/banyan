//! # ZStd decompressor that uses thread local buffers to prevent allocations
use smallvec::SmallVec;
use std::cell::RefCell;
use zstd::block::Decompressor;

/// max capacity we are going to allocate.
///
/// The maximum decompressed size the buffer can grow to before giving up.
const MAX_CAPACITY: usize = 1024 * 1024 * 16;

/// thread-local decompression state
pub(crate) struct DecompressionState {
    /// reused zstd decompressor
    decompressor: Decompressor,
}

impl DecompressionState {
    fn new() -> Self {
        Self {
            decompressor: Decompressor::new(),
        }
    }

    // fn decompress(&mut self, data: &[u8]) -> std::io::Result<usize> {
    //     if let Some(bound) = Decompressor::upper_bound(data) {
    //         if bound <= 1024 {
    //             let buffer = [0u8; 1024];
    //             let res = self
    //                 .decompressor
    //                 .decompress_to_buffer(data, &mut self.buffer);
    //         }
    //     }
    //     let mut cap = MIN_CAPACITY;
    //     // todo: do not resize but use a temp buffer as soon as we are above min_capacity
    //     loop {
    //         let t0 = Instant::now();
    //         self.buffer = vec![0u8; cap];
    //         tracing::info!("resize buffer {}", t0.elapsed().as_secs_f64());
    //         let t0 = Instant::now();
    //         let res = self
    //             .decompressor
    //             .decompress_to_buffer(data, &mut self.buffer);
    //         if res.is_ok() || cap >= MAX_CAPACITY {
    //             tracing::info!("decompress inne {} {}", t0.elapsed().as_secs_f64(), res.is_ok());
    //             return res;
    //         } else {
    //             tracing::info!("had to resize");
    //             cap *= 2;
    //         }
    //     }
    // }

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
        let capacity = Decompressor::upper_bound(compressed).unwrap_or(MAX_CAPACITY);
        // use a smallvec so in case capacity is very small, it all happens on the stack.
        let mut buffer: SmallVec<[u8; 1024]> = smallvec::smallvec![0u8; capacity];
        let span = tracing::debug_span!("decompress_and_transform");
        let _entered = span.enter();
        let len = self
            .decompressor
            .decompress_to_buffer(compressed, buffer.as_mut())?;
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
