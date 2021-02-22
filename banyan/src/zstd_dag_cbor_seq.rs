use std::{
    fmt,
    io::{Cursor, Write},
    iter,
    time::Instant,
};

use crate::thread_local_zstd::decompress_and_transform;
use fmt::Result;
use fnv::FnvHashSet;
use libipld::{
    cbor::DagCborCodec,
    codec::{Codec, Decode, Encode, References},
    Cid, DagCbor, Ipld,
};
use salsa20::{
    cipher::{NewStreamCipher, SyncStreamCipher},
    XSalsa20,
};

#[derive(Clone, PartialEq, Eq)]
pub struct ZstdDagCborSeq {
    data: Vec<u8>,
    links: Vec<Cid>,
}

impl ZstdDagCborSeq {
    pub(crate) fn new(data: Vec<u8>, links: Vec<Cid>) -> Self {
        Self { data, links }
    }

    /// create ZStdArray from a single serializable item
    pub fn from_iter<'a, I, T>(iter: I, level: i32) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = &'a T> + 'a,
        T: Encode<DagCborCodec> + 'a,
    {
        let mut encoder = zstd::Encoder::new(Vec::new(), level)?;
        let mut links = FnvHashSet::default();
        for item in iter.into_iter() {
            let encoded = DagCborCodec.encode(item)?;
            scrape_links(encoded.as_ref(), &mut links)?;
            encoder.write_all(encoded.as_ref())?;
        }
        // call finish to write the zstd frame
        let data = encoder.finish()?;
        // box into an arc
        Ok(Self::new(data.into(), links.into_iter().collect()))
    }

    /// create ZStdArray from a single serializable item
    pub fn single<T: Encode<DagCborCodec>>(value: &T, level: i32) -> anyhow::Result<Self> {
        Self::from_iter(std::iter::once(value), level)
    }

    /// create a ZStdArray by filling from an iterator
    pub fn fill<K, V: Encode<DagCborCodec>>(
        compressed: &[u8],
        from: &mut iter::Peekable<impl Iterator<Item = (K, V)>>,
        zstd_level: i32,
        compressed_size: u64,
        uncompressed_size: u64,
        keys: &mut Vec<K>,
        max_keys: usize,
    ) -> anyhow::Result<(Self, bool)> {
        let mut links = FnvHashSet::default();
        let t0 = Instant::now();
        let compressed_size = compressed_size as usize;
        let uncompressed_size = uncompressed_size as usize;
        let mut encoder = zstd::Encoder::new(Vec::new(), zstd_level)?;
        // decompress into the encoder, if necessary
        //
        // also init decompressed size
        let mut size = if !compressed.is_empty() {
            // the first ? is to handle the io error from decompress_and_transform, the second to handle the inner io error from write_all
            let (size, data) =
                decompress_and_transform(compressed, &mut |decompressed| -> anyhow::Result<()> {
                    scrape_links(decompressed.as_ref(), &mut links)?;
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
        Ok((Self::new(data.into(), links.into_iter().collect()), full))
    }

    /// Get the compressed data
    pub fn compressed(&self) -> &[u8] {
        &self.data
    }

    pub fn count(&self) -> anyhow::Result<u64> {
        decompress_and_transform(self.compressed(), &mut |uncompressed| {
            Ok(count_cbor_items(uncompressed)?)
        })?
        .1
    }

    pub fn items<T: Decode<DagCborCodec>>(&self) -> anyhow::Result<Vec<T>> {
        let (_, data) = decompress_and_transform(self.compressed(), &mut |uncompressed| {
            let mut result = Vec::new();
            let mut r = Cursor::new(&uncompressed);
            while r.position() < uncompressed.len() as u64 {
                result.push(T::decode(DagCborCodec, &mut r)?);
            }
            Ok(result)
        })?;
        data
    }

    /// select the items marked by the iterator and deserialize them into a vec.
    ///
    /// Other items will be skipped when deserializing, saving some unnecessary work.
    pub fn get<T: Decode<DagCborCodec>>(&self, index: u64) -> anyhow::Result<Option<T>> {
        let (_, data) = decompress_and_transform(self.compressed(), &mut |uncompressed| {
            let mut r = Cursor::new(uncompressed);
            let mut remaining = index;
            while r.position() < uncompressed.len() as u64 {
                if remaining > 0 {
                    // decode, but ignore the result.
                    // TODO: replace with something similar to serde IgnoredAny
                    T::decode(DagCborCodec, &mut r)?;
                    remaining -= 1;
                } else {
                    return Ok(Some(T::decode(DagCborCodec, &mut r)?));
                }
            }
            Ok(None)
        })?;
        data
    }

    /// select the items marked by the iterator and deserialize them into a vec.
    ///
    /// Other items will be skipped when deserializing, saving some unnecessary work.
    pub fn select<T: Decode<DagCborCodec>>(&self, take: &[bool]) -> anyhow::Result<Vec<T>> {
        let (_, data) = decompress_and_transform(self.compressed(), &mut |uncompressed| {
            let mut result: Vec<T> = Vec::new();
            let mut r = Cursor::new(uncompressed);
            let mut i: usize = 0;
            while r.position() < uncompressed.len() as u64 {
                if i < take.len() {
                    if take[i] {
                        result.push(T::decode(DagCborCodec, &mut r)?);
                    } else {
                        // decode, but ignore the result.
                        // TODO: replace with something similar to serde IgnoredAny
                        T::decode(DagCborCodec, &mut r)?;
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

    pub fn encrypt(&self, nonce: &salsa20::XNonce, key: &salsa20::Key) -> anyhow::Result<Vec<u8>> {
        self.clone().into_encrypted(nonce, key)
    }

    pub fn into_encrypted(
        self,
        nonce: &salsa20::XNonce,
        key: &salsa20::Key,
    ) -> anyhow::Result<Vec<u8>> {
        let Self { mut data, links } = self;
        // encrypt in place with the key and nonce
        XSalsa20::new(key, nonce).apply_keystream(&mut data);
        // add the nonce
        data.extend(nonce.as_slice());
        // encode via IpldNode
        let result = DagCborCodec.encode(&IpldNode::new(links, data))?;
        Ok(result)
    }

    pub fn decrypt(data: &[u8], key: &salsa20::Key) -> anyhow::Result<Self> {
        let (links, mut encrypted) = DagCborCodec.decode::<IpldNode>(data)?.into_data()?;
        let len = encrypted.len();
        anyhow::ensure!(len >= 24);
        let (compressed, nonce) = encrypted.split_at_mut(len - 24);
        XSalsa20::new(key, (&*nonce).into()).apply_keystream(compressed);
        // just remove the nonce, but don't use a new vec.
        let mut decrypted = encrypted;
        decrypted.drain(len - 24..);
        Ok(Self::new(decrypted, links))
    }
}

#[derive(DagCbor)]
struct IpldNode(Vec<Cid>, Ipld);

impl IpldNode {
    fn new(links: Vec<Cid>, data: impl Into<Vec<u8>>) -> Self {
        Self(links, Ipld::Bytes(data.into()))
    }

    fn into_data(self) -> anyhow::Result<(Vec<Cid>, Vec<u8>)> {
        if let Ipld::Bytes(data) = self.1 {
            Ok((self.0, data))
        } else {
            Err(anyhow::anyhow!("expected ipld bytes"))
        }
    }
}

impl fmt::Debug for ZstdDagCborSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ZstdDagCborSeq")
    }
}

fn count_cbor_items(data: &[u8]) -> anyhow::Result<u64> {
    let mut cursor = Cursor::new(data);
    let mut count = 0;
    let mut tmp = Vec::new();
    let size = data.len() as u64;
    while cursor.position() < size {
        // todo: use skip / IgnoredAny
        <Ipld as References<DagCborCodec>>::references(DagCborCodec, &mut cursor, &mut tmp)?;
        count += 1;
    }
    Ok(count)
}

fn scrape_links<C: Extend<Cid>>(data: &[u8], c: &mut C) -> anyhow::Result<()> {
    let mut cursor = Cursor::new(data);
    let size = data.len() as u64;
    while cursor.position() < size {
        <Ipld as References<DagCborCodec>>::references(DagCborCodec, &mut cursor, c)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use libipld::cid::multibase::Result;
    use quickcheck::quickcheck;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn zstd_array_fill_oversized() -> anyhow::Result<()> {
        // one byte too large. Does not fit!
        let mut items = vec![(1u8, vec![0u8; 10001])].into_iter().peekable();
        let mut keys = Vec::new();
        let res = ZstdDagCborSeq::fill(
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
        let (_, full) = ZstdDagCborSeq::fill(
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
        let (_, full) = ZstdDagCborSeq::fill(
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

    #[test]
    fn zstd_array_link_extract() -> anyhow::Result<()> {
        use std::str::FromStr;
        let cids = [
            Cid::from_str("bafyreihtx752fmf3zafbys5dtr4jxohb53yi3qtzfzf6wd5274jwtn5agu")?,
            Cid::from_str("bafyreiabupkdnos4dswptmc7ujczszggedg6pyijf2zrnhsx7sv73x2umq")?,
            Cid::from_str("bafyreifs65kond7gvtknoqe2vehsy3rhdmhxwnuwc2xxkoti3hwpjaeadu")?,
            Cid::from_str("bafyreidiz2cvkda77gzi6ljsow6sssypt2kgdvou5wbmyi534styi5hgku")?,
        ];
        let data = |i: usize| Ipld::List(vec![Ipld::Link(cids[i % cids.len()])]);
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

    /// basic test to ensure that the decompress works and properly clears the thread local buffer
    #[quickcheck]
    fn zstd_array_fill_roundtrip(
        first: Vec<u8>,
        data: Vec<Vec<u8>>,
        seed: u64,
    ) -> anyhow::Result<bool> {
        let bytes = data.iter().map(|x| x.len()).sum::<usize>() as u64;
        let target_size = bytes / 2;
        let initial = ZstdDagCborSeq::single(&first, 0)?;
        let mut iter = data.iter().cloned().enumerate().peekable();
        let mut keys = Vec::new();
        let (za, _) = ZstdDagCborSeq::fill(
            &initial.compressed(),
            &mut iter,
            0,
            target_size,
            1024 * 1024 * 4,
            &mut keys,
            usize::max_value(),
        )?;
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let nonce: salsa20::XNonce = rng.gen::<[u8; 24]>().into();
        let key: salsa20::Key = rng.gen::<[u8; 32]>().into();
        let encrypted = za.encrypt(&nonce, &key)?;
        let za2 = ZstdDagCborSeq::decrypt(&encrypted, &key)?;
        if za != za2 {
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
        if decompressed.len() < data.len() && (za.compressed().len() as u64) < target_size {
            return Ok(false);
        }
        Ok(true)
    }

    #[test]
    fn test_disk_format() -> anyhow::Result<()> {
        let data = vec![1u32,2,3,4];
        let nonce: salsa20::XNonce = [0u8;24].into();
        let key: salsa20::Key = [0u8;32].into();

        let res = ZstdDagCborSeq::single(&data, 10)?;
        let bytes = res.encrypt(&nonce, &key)?;

        // do not exactly check the compressed and encrypted part, since the exact
        // bytes depend on zstd details and might be fragile.
        assert_eq!(
            bytes[0..2],
            vec![
                0x82, // list 0x80 of length 2
                0x80, // array of links, size 0 (no links)
            ]
        );
        let items: Vec<Ipld> = DagCborCodec.decode(&bytes)?;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], Ipld::List(vec![]));
        if let Ipld::Bytes(bytes) = &items[1] {
            use std::ops::Deref;
            let len = bytes.len();
            assert!(len >= 24);
            // nonce should be stored last
            let (encrypted, nonce1) = bytes.split_at(len - 24);
            assert_eq!(nonce1, nonce.deref());
            // once decrypted, must be valid zstd
            let mut decrypted = encrypted.to_vec();
            XSalsa20::new(&key, (&*nonce).into()).apply_keystream(&mut decrypted);
            let decompressed = zstd::decode_all(Cursor::new(decrypted))?;
            // finally, compare with the original data
            let data1: Vec<u32> = DagCborCodec.decode(&decompressed)?;
            assert_eq!(data1, data);
        } else {
            assert!(false);
        }
        Ok(())
    }
}
