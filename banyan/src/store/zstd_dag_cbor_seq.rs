//! Compressed and encrypted cbor seq with links.
//!
//! This is the block format for both branches and leaves. Basically it is a dag-cbor
//! 2-tuple, containing an encrypted and zstd compressed blob which again is a sequence
//! of dag-cbor items, and a number of links.
//!
//! The links are unencrypted so they are visible for tools that do not have the encryption
//! keys.
//!
//! They are typically extracted from the content, but there is no strict rule that
//! all links in the links section must be extracted from the content. It might be useful to
//! add additional links to e.g. accelerate traversal of very long chains or to attach additional
//! data.
//!
//! To have links that are not supposed to be visible to the outside, simply omit the cbor tag that
//! marks them as links. However, be aware that this will prevent sync mechanisms that do not have the
//! decompression keys from properly syncing the data.
//!
//! The blob is encrypted with the chacha20 symmetric cipher, with a 24 byte nonce that is
//! appended to the blob.
//!
//! https://github.com/ipld/specs/blob/master/block-layer/codecs/dag-cbor.md
//! https://tools.ietf.org/html/rfc8742
use cbor_data::{
    codec::{ReadCbor, WriteCbor},
    Cbor, CborBuilder, ItemKind, Visitor,
};
use chacha20::{
    cipher::{NewCipher, StreamCipher, StreamCipherSeek},
    XChaCha20,
};
use libipld::{
    cbor::DagCborCodec,
    codec::Codec,
    prelude::{Decode, Encode},
    Cid, DagCbor, Ipld,
};
use std::{
    collections::BTreeSet,
    convert::TryFrom,
    fmt,
    io::{Cursor, ErrorKind, Write},
    iter,
    ops::Range,
    time::Instant,
};

use crate::{error::Error, store::decompress_and_transform, stream_builder::CipherOffset};

#[derive(Clone, PartialEq, Eq)]
pub struct ZstdDagCborSeq {
    /// ZStd compressed sequence of cbor items, see https://tools.ietf.org/html/rfc8742
    data: Vec<u8>,
    /// Links that will be persisted unencrypted, typically extracted from the content
    links: Vec<Cid>,
}

impl ZstdDagCborSeq {
    pub(crate) fn new(data: Vec<u8>, links: Vec<Cid>) -> Self {
        Self { data, links }
    }

    /// create ZStdArray from a sequence of serializable items
    pub fn from_iter<'a, I, T>(iter: I, zstd_level: i32) -> Result<Self, Error>
    where
        I: IntoIterator<Item = &'a T> + 'a,
        T: WriteCbor + 'a,
    {
        let t0 = Instant::now();
        let mut encoder = zstd::Encoder::new(Vec::new(), zstd_level)?;
        let mut links = BTreeSet::new();
        let mut size: usize = 0;
        let mut encoded = Vec::new();
        for item in iter.into_iter() {
            encoded.clear();
            item.write_cbor(CborBuilder::append_to(&mut encoded));
            size += encoded.len();
            scrape_links(encoded.as_ref(), &mut links)?;
            encoder.write_all(encoded.as_ref())?;
        }
        // call finish to write the zstd frame
        let data = encoder.finish()?;
        tracing::trace!(
            "ZstdArray::from_iter elapsed={} compressed={} uncompressed={}",
            t0.elapsed().as_secs_f64(),
            data.len(),
            size
        );
        // box into an arc
        Ok(Self::new(data, links.into_iter().collect()))
    }

    /// create ZStdArray from a single serializable item
    pub fn from_iter_ipld<'a, I, T>(iter: I, zstd_level: i32) -> Result<Self, Error>
    where
        I: IntoIterator<Item = &'a T> + 'a,
        T: Encode<DagCborCodec> + 'a,
    {
        let t0 = Instant::now();
        let mut encoder = zstd::Encoder::new(Vec::new(), zstd_level)?;
        let mut links = BTreeSet::new();
        let mut size: usize = 0;
        for item in iter.into_iter() {
            let encoded = DagCborCodec.encode(item)?;
            size += encoded.len();
            scrape_links(encoded.as_ref(), &mut links)?;
            encoder.write_all(encoded.as_ref())?;
        }
        // call finish to write the zstd frame
        let data = encoder.finish()?;
        tracing::trace!(
            "ZstdArray::from_iter elapsed={} compressed={} uncompressed={}",
            t0.elapsed().as_secs_f64(),
            data.len(),
            size
        );
        // box into an arc
        Ok(Self::new(data, links.into_iter().collect()))
    }

    /// create ZStdArray from a single serializable item
    pub fn single<T: WriteCbor>(value: &T, zstd_level: i32) -> Result<Self, Error> {
        Self::from_iter(std::iter::once(value), zstd_level)
    }

    /// create ZStdArray from a single serializable item
    pub fn single_ipld<T: Encode<DagCborCodec>>(value: &T, zstd_level: i32) -> Result<Self, Error> {
        Self::from_iter_ipld(std::iter::once(value), zstd_level)
    }

    /// create a ZStdArray by filling from an iterator
    ///
    /// Takes the compressed data `compressed`, and extends it by pulling from the peekable
    /// iterator `from`. Uses compression level `zstd_level`. There are three target criteria
    /// after which it will stop pulling from the iterator: `compressed_size`, `uncompressed_size`,
    /// and `max_keys`. `max_keys` is the maximum number of keys.
    ///
    /// Only values will be compressed. Keys will be pushed on the `keys` vec.
    ///
    /// IPLD links in V will be scraped and put into the links. So on success, links will contain
    /// all CBOR links in both `compressed` and the added `V`s.
    ///
    /// On success, returns a tuple consisting of the `ZstdDagCborSeq` and a boolean indicating if
    /// the result is full.
    pub fn fill<K, V: WriteCbor>(
        compressed: &[u8],
        from: &mut iter::Peekable<impl Iterator<Item = (K, V)>>,
        keys: &mut Vec<K>,
        zstd_level: i32,
        compressed_size: usize,
        uncompressed_size: usize,
        max_keys: usize,
    ) -> Result<(Self, bool), Error> {
        let mut links = BTreeSet::new();
        let t0 = Instant::now();
        let mut encoder = zstd::Encoder::new(Vec::new(), zstd_level)?;
        // decompress into the encoder, if necessary
        //
        // also init decompressed size
        let mut size = if !compressed.is_empty() {
            // the first ? is to handle the io error from decompress_and_transform, the second to handle the inner io error from write_all
            let (size, data) =
                decompress_and_transform(compressed, &mut |decompressed| -> Result<(), Error> {
                    scrape_links(decompressed, &mut links)?;
                    encoder.write_all(decompressed)?;
                    Ok(())
                })?;
            data?;
            size
        } else {
            0
        };
        let mut full = false;
        let mut bytes = Vec::new();
        // fill until rough size goal exceeded
        while let Some((_, value)) = from.peek() {
            // do this check here, in case somebody calls us with an already full keys vec
            if keys.len() >= max_keys {
                break;
            }
            bytes.clear();
            value.write_cbor(CborBuilder::append_to(&mut bytes));
            // if a single item is too big, bail out
            if bytes.len() > uncompressed_size {
                return Err(Error::ItemTooLarge);
            }
            // check that we don't exceed the uncompressed_size goal before adding
            if size + bytes.len() > uncompressed_size {
                // we know that the next item does not fit, so we are full even if
                // there is some space left.
                full = true;
                break;
            }
            // scrape links from the new item
            scrape_links(bytes.as_ref(), &mut links)?;
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
        tracing::trace!(
            "ZstdArray::fill elapsed={} compressed={} uncompressed={}",
            t0.elapsed().as_secs_f64(),
            data.len(),
            size
        );
        full |= data.len() >= compressed_size;
        full |= keys.len() >= max_keys;
        full |= size >= uncompressed_size;
        Ok((Self::new(data, links.into_iter().collect()), full))
    }

    /// create a ZStdArray by filling from an iterator
    ///
    /// Takes the compressed data `compressed`, and extends it by pulling from the peekable
    /// iterator `from`. Uses compression level `zstd_level`. There are three target criteria
    /// after which it will stop pulling from the iterator: `compressed_size`, `uncompressed_size`,
    /// and `max_keys`. `max_keys` is the maximum number of keys.
    ///
    /// Only values will be compressed. Keys will be pushed on the `keys` vec.
    ///
    /// IPLD links in V will be scraped and put into the links. So on success, links will contain
    /// all CBOR links in both `compressed` and the added `V`s.
    ///
    /// On success, returns a tuple consisting of the `ZstdDagCborSeq` and a boolean indicating if
    /// the result is full.
    pub fn fill_ipld<K, V: Encode<DagCborCodec>>(
        compressed: &[u8],
        from: &mut iter::Peekable<impl Iterator<Item = (K, V)>>,
        keys: &mut Vec<K>,
        zstd_level: i32,
        compressed_size: usize,
        uncompressed_size: usize,
        max_keys: usize,
    ) -> Result<(Self, bool), Error> {
        let mut links = BTreeSet::new();
        let t0 = Instant::now();
        let mut encoder = zstd::Encoder::new(Vec::new(), zstd_level)?;
        // decompress into the encoder, if necessary
        //
        // also init decompressed size
        let mut size = if !compressed.is_empty() {
            // the first ? is to handle the io error from decompress_and_transform, the second to handle the inner io error from write_all
            let (size, data) =
                decompress_and_transform(compressed, &mut |decompressed| -> Result<(), Error> {
                    scrape_links(decompressed, &mut links)?;
                    encoder.write_all(decompressed)?;
                    Ok(())
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
            let bytes = DagCborCodec.encode(value)?;
            // if a single item is too big, bail out
            if !(bytes.len() <= uncompressed_size) {
                return Err(Error::ItemTooLarge);
            }
            // check that we don't exceed the uncompressed_size goal before adding
            if size + bytes.len() > uncompressed_size {
                // we know that the next item does not fit, so we are full even if
                // there is some space left.
                full = true;
                break;
            }
            // scrape links from the new item
            scrape_links(bytes.as_ref(), &mut links)?;
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
        tracing::trace!(
            "ZstdArray::fill elapsed={} compressed={} uncompressed={}",
            t0.elapsed().as_secs_f64(),
            data.len(),
            size
        );
        full |= data.len() >= compressed_size;
        full |= keys.len() >= max_keys;
        full |= size >= uncompressed_size;
        Ok((Self::new(data, links.into_iter().collect()), full))
    }

    /// Get the compressed data
    pub fn compressed(&self) -> &[u8] {
        &self.data
    }

    /// Computes the number of cbor items in the cbor seq
    pub fn count(&self) -> Result<u64, Error> {
        decompress_and_transform(self.compressed(), &mut |uncompressed| {
            count_cbor_items(uncompressed)
        })?
        .1
    }

    /// returns all items as a vec
    pub fn items<T: ReadCbor>(&self) -> Result<Vec<T>, Error> {
        let (_, data) = decompress_and_transform(self.compressed(), &mut |mut uncompressed| {
            let mut result = Vec::new();
            while !uncompressed.is_empty() {
                let (cbor, rest) = Cbor::checked_prefix(uncompressed)?;
                result.push(T::read_cbor(cbor)?);
                uncompressed = rest;
            }
            Ok(result)
        })?;
        data
    }

    /// returns all items as a vec
    pub fn items_ipld<T: Decode<DagCborCodec>>(&self) -> Result<Vec<T>, Error> {
        let (_, data) = decompress_and_transform(self.compressed(), &mut |uncompressed| {
            let mut result = Vec::new();
            let mut r = Cursor::new(&uncompressed);
            let len = u64::try_from(uncompressed.len())?;
            while r.position() < len {
                result.push(T::decode(DagCborCodec, &mut r)?);
            }
            Ok(result)
        })?;
        data
    }

    /// Decompress and decode a single item
    pub fn get<T: ReadCbor>(&self, index: u64) -> Result<Option<T>, Error> {
        let (_, data) = decompress_and_transform(self.compressed(), &mut |uncompressed| {
            let mut remaining = index;
            let mut bytes = uncompressed;
            while !bytes.is_empty() {
                // we cannot use ipld since the contained data may be arbitrary CBOR, not just IPLD-CBOR
                let (cbor, rest) = Cbor::checked_prefix(bytes)?;
                if remaining > 0 {
                    remaining -= 1;
                    bytes = rest;
                } else {
                    return Ok(Some(T::read_cbor(cbor)?));
                }
            }
            Ok(None)
        })?;
        data
    }

    /// select the items marked by the bool slice and deserialize them into a vec.
    ///
    /// Other items will be skipped when deserializing, saving some unnecessary work.
    pub fn select<T: ReadCbor>(&self, take: &[bool]) -> Result<Vec<T>, Error> {
        // shrink take so we don't needlessly decode stuff after the last match
        let take = shrink_to_fit(take);
        // this is not as useful as it looks, since usually we will only hit this if some upper
        // level logic already knows that there is something to be found.
        if take.is_empty() {
            return Ok(Vec::new());
        }
        let (_, data) = decompress_and_transform(self.compressed(), &mut |uncompressed| {
            let mut result: Vec<T> = Vec::new();
            let mut bytes = uncompressed;
            for take in take.iter().cloned() {
                if bytes.is_empty() {
                    break;
                }
                // we cannot use ipld since the contained data may be arbitrary CBOR, not just IPLD-CBOR
                let (cbor, rest) = Cbor::checked_prefix(bytes)?;
                bytes = rest;
                if take {
                    result.push(T::read_cbor(cbor)?);
                }
            }
            Ok(result)
        })?;
        data
    }

    /// encrypt using the given key and nonce
    pub fn encrypt(
        &self,
        key: &chacha20::Key,
        nonce: &chacha20::XNonce,
        offset: u64,
    ) -> Result<Vec<u8>, Error> {
        let mut state = CipherOffset::new(offset);
        self.clone().into_encrypted(key, nonce, &mut state)
    }

    /// convert into an encrypted blob, using the given key and nonce
    pub(crate) fn into_encrypted(
        self,
        key: &chacha20::Key,
        nonce: &chacha20::XNonce,
        state: &mut CipherOffset,
    ) -> Result<Vec<u8>, Error> {
        let Self { mut data, links } = self;
        // encrypt in place with the key and nonce
        let mut chacha20 = XChaCha20::new(key, nonce);
        let offset = state.reserve(data.len());
        chacha20.seek(offset);
        chacha20.apply_keystream(&mut data);
        // encode via IpldNode
        let result = DagCborCodec.encode(&IpldNode::new(links, data, offset))?;
        Ok(result)
    }

    /// decrypt using the given key
    pub fn decrypt(
        data: &[u8],
        key: &chacha20::Key,
        nonce: &chacha20::XNonce,
    ) -> Result<(Self, Range<u64>), Error> {
        let (offset, links, mut encrypted) = DagCborCodec.decode::<IpldNode>(data)?.into_data()?;
        let mut cipher = XChaCha20::new(key, nonce);
        let end_offset = offset
            .checked_add(encrypted.len() as u64)
            .ok_or(Error::SeekOffsetWraparound)?;
        cipher.seek(offset);
        cipher.apply_keystream(&mut encrypted);
        let decrypted = encrypted;
        Ok((Self::new(decrypted, links), offset..end_offset))
    }
}

/// utility struct for encoding and decoding
#[derive(DagCbor)]
struct IpldNode(u64, Vec<Cid>, Ipld);

impl IpldNode {
    fn new(links: Vec<Cid>, data: impl Into<Vec<u8>>, offset: u64) -> Self {
        Self(offset, links, Ipld::Bytes(data.into()))
    }

    fn into_data(self) -> Result<(u64, Vec<Cid>, Vec<u8>), Error> {
        if let Ipld::Bytes(data) = self.2 {
            Ok((self.0, self.1, data))
        } else {
            Err(Error::ExpectedIpldBytes)
        }
    }
}

/// shrink a bool slice so that the last true bool is at the end
fn shrink_to_fit(slice: &[bool]) -> &[bool] {
    for i in (0..slice.len()).rev() {
        if slice[i] {
            return &slice[0..=i];
        }
    }
    &[]
}

impl fmt::Debug for ZstdDagCborSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ZstdDagCborSeq")
    }
}

/// count the number of items in a dag cbor seq
fn count_cbor_items(data: &[u8]) -> Result<u64, Error> {
    let mut count = 0;
    let mut bytes = data;
    while !bytes.is_empty() {
        // we cannot use ipld since the contained data may be arbitrary CBOR, not just IPLD-CBOR
        let (_cbor, rest) = Cbor::checked_prefix(bytes)?;
        bytes = rest;
        count += 1;
    }
    Ok(count)
}

/// scrape references from a dag cbor seq using cbor-data
fn scrape_links<C: Extend<Cid>>(data: &[u8], c: &mut C) -> Result<(), Error> {
    let mut bytes = data;
    struct V<'a, C>(&'a mut C);
    impl<'a, C: Extend<Cid>> Visitor<'a, cid::Error> for V<'a, C> {
        fn visit_simple(&mut self, item: cbor_data::TaggedItem<'a>) -> Result<(), cid::Error> {
            if let (Some(42), ItemKind::Bytes(b)) = (item.tags().single(), item.kind()) {
                let b = b.as_cow();
                if !b.is_empty() && b[0] == 0 {
                    self.0.extend([Cid::read_bytes(&b[1..])?]);
                } else {
                    return Err(cid::Error::Io(std::io::Error::new(
                        ErrorKind::Other,
                        format!("invalid Link: {:?}", b),
                    )));
                }
            }
            Ok(())
        }
    }
    let mut visitor = V(c);
    while !bytes.is_empty() {
        // we cannot use ipld since the contained data may be arbitrary CBOR, not just IPLD-CBOR
        let (cbor, rest) = Cbor::checked_prefix(bytes)?;
        bytes = rest;
        cbor.visit(&mut visitor)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cbor_data::{codec::CodecError, Writer};
    use quickcheck::quickcheck;
    use rand::{Rng, RngCore, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use std::{collections::HashSet, convert::TryFrom, io::Cursor};

    #[test]
    fn zstd_array_fill_oversized() -> Result<(), Error> {
        // one byte too large. Does not fit!
        let mut items = vec![(1u8, vec![0u8; 10001])].into_iter().peekable();
        let mut keys = Vec::new();
        let res = ZstdDagCborSeq::fill(
            &[],
            &mut items,
            &mut keys,
            10,
            1000,
            10002, // one byte too small
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
        let (_, full) = ZstdDagCborSeq::fill(
            &[],
            &mut items,
            &mut keys,
            10,
            1000,
            10003, // exactly the right size
            1000,
        )?;
        assert!(full);
        Ok(())
    }

    #[test]
    fn zstd_array_fill_keys() -> Result<(), Error> {
        let mut items = vec![
            (1u8, vec![0u8; 1]),
            (2u8, vec![0u8; 1]),
            (3u8, vec![0u8; 1]),
            (4u8, vec![0u8; 1]),
        ]
        .into_iter()
        .peekable();
        let mut keys = Vec::new();
        let (_, full) = ZstdDagCborSeq::fill(
            &[],
            &mut items,
            &mut keys,
            10,
            1000,
            10002, // one byte too small
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

    struct MyCid(Cid);

    impl WriteCbor for MyCid {
        fn write_cbor<W: Writer>(&self, w: W) -> W::Output {
            let mut bytes = Vec::new();
            self.0.write_bytes(&mut bytes).expect("writing to SmallVec");
            w.write_bytes_chunked([&[0][..], &*bytes], [42])
        }
    }

    impl ReadCbor for MyCid {
        fn fmt(f: &mut impl fmt::Write) -> std::fmt::Result {
            write!(f, "Cid")
        }

        fn read_cbor(cbor: &Cbor) -> cbor_data::codec::Result<Self>
        where
            Self: Sized,
        {
            let decoded = cbor.tagged_item();
            if let (Some(42), ItemKind::Bytes(b)) = (decoded.tags().single(), decoded.kind()) {
                let b = b.as_cow();
                if b.is_empty() {
                    Err(CodecError::str("Cid cannot be empty"))
                } else if b[0] != 0 {
                    Err(CodecError::str("Cid must use identity encoding"))
                } else {
                    Ok(MyCid(Cid::read_bytes(&b[1..]).map_err(CodecError::custom)?))
                }
            } else {
                Err(CodecError::type_error("Cid", &decoded))
            }
        }
    }

    #[test]
    fn zstd_array_link_extract() -> Result<(), Error> {
        use std::str::FromStr;
        let cids = [
            Cid::from_str("bafyreihtx752fmf3zafbys5dtr4jxohb53yi3qtzfzf6wd5274jwtn5agu")?,
            Cid::from_str("bafyreiabupkdnos4dswptmc7ujczszggedg6pyijf2zrnhsx7sv73x2umq")?,
            Cid::from_str("bafyreifs65kond7gvtknoqe2vehsy3rhdmhxwnuwc2xxkoti3hwpjaeadu")?,
            Cid::from_str("bafyreidiz2cvkda77gzi6ljsow6sssypt2kgdvou5wbmyi534styi5hgku")?,
        ];
        let data = |i: usize| vec![MyCid(cids[i % cids.len()])];
        let items = (0..100).map(data).collect::<Vec<_>>();
        let za = ZstdDagCborSeq::single(&items, 10)?;
        // test that links are deduped
        assert_eq!(za.links.len(), 4);
        assert_eq!(
            za.links.iter().collect::<HashSet<_>>(),
            cids.iter().collect::<HashSet<_>>()
        );
        Ok(())
    }

    #[test]
    fn zstd_array_link_extract_ipld() -> Result<(), Error> {
        use std::str::FromStr;
        let cids = [
            Cid::from_str("bafyreihtx752fmf3zafbys5dtr4jxohb53yi3qtzfzf6wd5274jwtn5agu")?,
            Cid::from_str("bafyreiabupkdnos4dswptmc7ujczszggedg6pyijf2zrnhsx7sv73x2umq")?,
            Cid::from_str("bafyreifs65kond7gvtknoqe2vehsy3rhdmhxwnuwc2xxkoti3hwpjaeadu")?,
            Cid::from_str("bafyreidiz2cvkda77gzi6ljsow6sssypt2kgdvou5wbmyi534styi5hgku")?,
        ];
        let data = |i: usize| Ipld::List(vec![Ipld::Link(cids[i % cids.len()])]);
        let items = (0..100).map(data).collect::<Vec<_>>();
        let za = ZstdDagCborSeq::single_ipld(&items, 10)?;
        // test that links are deduped
        assert_eq!(za.links.len(), 4);
        assert_eq!(
            za.links.iter().collect::<HashSet<_>>(),
            cids.iter().collect::<HashSet<_>>()
        );
        Ok(())
    }

    /// basic test to ensure that the decompress works and properly clears the thread local buffer
    fn do_zstd_array_fill_roundtrip(
        first: Vec<u8>,
        data: Vec<Vec<u8>>,
        seed: u64,
    ) -> Result<bool, Error> {
        let bytes = data.iter().map(|x| x.len()).sum::<usize>();
        let target_size = bytes / 2;
        let initial = ZstdDagCborSeq::single(&first, 0)?;
        let mut iter = data.iter().cloned().enumerate().peekable();
        let mut keys = Vec::new();
        let (za, _) = ZstdDagCborSeq::fill(
            initial.compressed(),
            &mut iter,
            &mut keys,
            0,
            target_size,
            1024 * 1024 * 4,
            usize::max_value(),
        )?;
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let len = za.compressed().len() as u64;
        // do not test offsets that would wrap around!
        let offset = rng.next_u64().saturating_add(len).saturating_sub(len);
        let key: chacha20::Key = rng.gen::<[u8; 32]>().into();
        let nonce: chacha20::XNonce = rng.gen::<[u8; 24]>().into();
        let encrypted = za.encrypt(&key, &nonce, offset)?;
        let (za2, byte_range) = ZstdDagCborSeq::decrypt(&encrypted, &key, &nonce)?;
        if za != za2 {
            return Ok(false);
        }
        if (offset..offset + za.compressed().len() as u64) != byte_range {
            return Ok(false);
        }
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
        if decompressed.len() < data.len() && za.compressed().len() < target_size {
            return Ok(false);
        }
        Ok(true)
    }
    #[quickcheck]
    fn zstd_array_fill_roundtrip(
        first: Vec<u8>,
        data: Vec<Vec<u8>>,
        seed: u64,
    ) -> Result<bool, Error> {
        do_zstd_array_fill_roundtrip(first, data, seed)
    }

    #[test]
    fn zstd_array_fill_roundtrip_1() {
        assert!(do_zstd_array_fill_roundtrip(vec![], vec![], 0).unwrap());
    }

    #[test]
    fn test_disk_format() -> Result<(), Error> {
        let data = vec![1u64, 2, 3, 4];
        let key: chacha20::Key = [0u8; 32].into();
        let nonce: chacha20::XNonce = [0u8; 24].into();
        let offset = 7u64;

        let res = ZstdDagCborSeq::single(&data, 10)?;
        let bytes = res.encrypt(&key, &nonce, offset)?;

        // do not exactly check the compressed and encrypted part, since the exact
        // bytes depend on zstd details and might be fragile.
        assert_eq!(
            bytes[0..3],
            vec![
                0x83, // list 0x80 of length 3
                0x07, // offset, unsigned(7)
                0x80, // array of links, size 0 (no links)
            ]
        );
        let items: Vec<Ipld> = DagCborCodec.decode(&bytes)?;
        assert_eq!(items.len(), 3);
        assert_eq!(items[1], Ipld::List(vec![]));
        if let (Ipld::Integer(offset1), Ipld::Bytes(encrypted)) = (&items[0], &items[2]) {
            let offset1 = u64::try_from(*offset1)?;
            assert_eq!(offset1, offset);
            // once decrypted, must be valid zstd
            let mut decrypted = encrypted.to_vec();
            let mut chacha = XChaCha20::new(&key, &nonce);
            chacha.seek(offset);
            chacha.apply_keystream(&mut decrypted);
            let decompressed = zstd::decode_all(Cursor::new(decrypted))?;
            // finally, compare with the original data
            let data1: Vec<u64> = DagCborCodec.decode(&decompressed)?;
            assert_eq!(data1, data);
        } else {
            panic!();
        }
        Ok(())
    }
}
