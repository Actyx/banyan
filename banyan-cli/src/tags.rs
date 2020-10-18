use crate::{ipfs::Cid, tag_index::TagIndex, tag_index::TagSet};
use banyan::index::*;
use banyan::{forest::*, query::Query};
use serde::{Deserialize, Serialize};
use std::{convert::{TryFrom, TryInto}, fmt, io, iter::FromIterator, str::FromStr};

#[derive(Debug)]
pub struct TT {}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    pub fn new(data: &[u8]) -> Self {
        let mh = multihash::Sha2_256::digest(data);
        Sha256Digest(mh.digest().try_into().unwrap())
    }
}

impl From<Sha256Digest> for Cid {
    fn from(value: Sha256Digest) -> Self {
        let mh = multihash::wrap(multihash::Code::Sha2_256, &value.0);
        cid::Cid::new_v1(cid::Codec::DagCBOR, mh).into()
    }
}

impl FromStr for Sha256Digest {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let cid = Cid::from_str(s)?;
        cid.try_into()
    }
}

impl TryFrom<Cid> for Sha256Digest {
    type Error = anyhow::Error;
    fn try_from(value: Cid) -> Result<Self, Self::Error> {
        let cid: cid::Cid = value.into();
        if cid.version() != cid::Version::V1 {
            anyhow::bail!("version 0 multihash not supported!");
        }
        // if cid.codec() != cid::Codec::DagCBOR {
        //     anyhow::bail!("Must be DagCBOR codec");
        // }
        if cid.hash().algorithm() != multihash::Code::Sha2_256 {
            anyhow::bail!("hashes must be Sha256 encoded!");
        }
        if cid.hash().digest().len() != 32 {
            anyhow::bail!("Sha256 must have 256 bits digest");
        }
        Ok(Sha256Digest(cid.hash().digest().try_into()?))
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Cid::from(*self))
    }
}

impl TreeTypes for TT {
    type Key = Key;
    type Seq = KeySeq;
    type Link = Sha256Digest;
    fn serialize_branch(
        links: &[&Self::Link],
        data: Vec<u8>,
        w: impl io::Write,
    ) -> anyhow::Result<()> {
        let cids = links
            .into_iter()
            .map(|x| Cid::from(**x))
            .collect::<Vec<_>>();
        serde_cbor::to_writer(w, &(cids, serde_cbor::Value::Bytes(data)))
            .map_err(|e| anyhow::Error::new(e))
    }
    fn deserialize_branch(reader: impl io::Read) -> anyhow::Result<(Vec<Self::Link>, Vec<u8>)> {
        let (cids, data): (Vec<Cid>, serde_cbor::Value) = serde_cbor::from_reader(reader)?;
        let links = cids
            .into_iter()
            .map(Sha256Digest::try_from)
            .collect::<anyhow::Result<Vec<_>>>()?;
        if let serde_cbor::Value::Bytes(data) = data {
            Ok((links, data))
        } else {
            Err(anyhow::anyhow!("expected cbor bytes"))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Key {
    min_lamport: u64,
    min_time: u64,
    max_time: u64,
    tags: TagSet,
}

impl Key {
    pub fn single(lamport: u64, time: u64, tags: TagSet) -> Self {
        Self {
            min_lamport: lamport,
            min_time: time,
            max_time: time,
            tags,
        }
    }

    pub fn filter_tags(tags: TagSet) -> Self {
        Self {
            min_lamport: u64::MIN,
            min_time: u64::MIN,
            max_time: u64::MAX,
            tags,
        }
    }

    pub fn range(min_time: u64, max_time: u64, tags: TagSet) -> Self {
        Self {
            min_lamport: 0,
            min_time,
            max_time,
            tags,
        }
    }

    fn intersects(&self, that: &Key) -> bool {
        if self.max_time < that.min_time {
            return false;
        }
        if self.min_time > that.max_time {
            return false;
        }
        if self.tags.is_disjoint(&that.tags) {
            return false;
        }
        true
    }

    fn contains(&self, that: &Key) -> bool {
        if that.min_time < self.min_time {
            return false;
        }
        if that.max_time > self.max_time {
            return false;
        }
        if !that.tags.is_subset(&self.tags) {
            return false;
        }
        true
    }
}

impl Key {
    fn combine(&mut self, b: &Self) {
        self.min_lamport = self.min_lamport.min(b.min_lamport);
        self.min_time = self.min_time.min(b.min_time);
        self.max_time = self.max_time.max(b.max_time);
        self.tags.extend(b.tags.iter().cloned());
    }
}

#[derive(Debug)]
pub struct DnfQuery(pub Vec<Key>);

impl DnfQuery {
    fn intersects(&self, v: &Key) -> bool {
        self.0.iter().any(|x| x.intersects(v))
    }
    fn contains(&self, v: &Key) -> bool {
        self.0.iter().any(|x| x.contains(v))
    }
}

impl Query<TT> for DnfQuery {
    fn intersecting(&self, _: u64, x: &BranchIndex<TT>, matching: &mut [bool]) {        
        for i in 0 .. x.summaries.len().min(matching.len()) {
            if matching[i] {
                matching[i] = self.intersects(&x.summaries.get(i).unwrap());
            }
        }
    }
    fn containing(&self, _: u64, x: &LeafIndex<TT>, matching: &mut [bool]) {
        for i in 0 .. x.keys.len().min(matching.len()) {
            if matching[i] {
                matching[i] = self.contains(&x.keys.get(i).unwrap());
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeySeq {
    min_lamport: Vec<u64>,
    min_time: Vec<u64>,
    max_time: Vec<u64>,
    tags: TagIndex,
}

impl CompactSeq for KeySeq {
    type Item = Key;

    fn get(&self, index: usize) -> Option<Key> {
        if let (Some(min_lamport), Some(min_time), Some(max_time), Some(tags)) = (
            self.min_lamport.get(index),
            self.min_time.get(index),
            self.max_time.get(index),
            self.tags.get(index),
        ) {
            Some(Key {
                min_lamport: *min_lamport,
                min_time: *min_time,
                max_time: *max_time,
                tags,
            })
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.tags.elements.len()
    }

    fn summarize(&self) -> Key {
        let max_time = *self.max_time.iter().max().unwrap();
        let min_time = *self.min_time.iter().min().unwrap();
        let min_lamport = *self.min_lamport.iter().min().unwrap();
        let tags = self.tags.tags.clone();
        Key {
            max_time,
            min_time,
            min_lamport,
            tags,
        }
        // let mut result = self.get(0).unwrap();
        // for i in 1..self.tags.elements.len() {
        //     result.combine(&self.get(i).unwrap());
        // }
        // result
    }
}

impl FromIterator<Key> for KeySeq {
    fn from_iter<T: IntoIterator<Item = Key>>(iter: T) -> Self {
        let mut min_lamport = Vec::new();
        let mut min_time = Vec::new();
        let mut max_time = Vec::new();
        let mut tag_index = Vec::new();
        for value in iter.into_iter() {
            min_lamport.push(value.min_lamport);
            min_time.push(value.min_time);
            max_time.push(value.max_time);
            tag_index.push(value.tags);
        }
        let tag_index = TagIndex::from_elements(&tag_index);
        Self {
            min_lamport,
            min_time,
            max_time,
            tags: tag_index,
        }
    }
}
