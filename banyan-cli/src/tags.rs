use crate::{tag_index::map_to_index_set, tag_index::TagIndex, tag_index::TagSet};
use banyan::index::*;
use banyan::{forest::*, query::Query};
use libipld::{
    cbor::{decode::TryReadCbor, DagCborCodec},
    codec::{Decode, Encode},
    Cid,
    DagCbor,
};
use multihash::MultihashDigest;
use serde::{Deserialize, Serialize};
use std::{
    convert::{TryFrom, TryInto},
    fmt,
    io::{Read, Seek, Write},
    iter::FromIterator,
    str::FromStr,
};
use vec_collections::VecSet;

#[derive(Debug, Clone)]
pub struct TT {}

#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sha256Digest([u8; 32]);

impl Decode<DagCborCodec> for Sha256Digest {
    fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> anyhow::Result<Self> {
        Self::try_from(Cid::decode(c, r)?)
    }
}
impl Encode<DagCborCodec> for Sha256Digest {
    fn encode<W: Write>(&self, c: DagCborCodec, w: &mut W) -> anyhow::Result<()> {
        Cid::encode(&Cid::from(*self), c, w)
    }
}

impl TryReadCbor for Sha256Digest {
    fn try_read_cbor<R: Read + Seek>(r: &mut R, major: u8) -> anyhow::Result<Option<Self>> {
        if let Some(cid) = Cid::try_read_cbor(r, major)? {
            Ok(Some(Self::try_from(cid)?))
        } else {
            Ok(None)
        }
    }
}

impl Sha256Digest {
    pub fn new(data: &[u8]) -> Self {
        let mh = multihash::Code::Sha2_256.digest(data);
        Sha256Digest(mh.digest().try_into().unwrap())
    }
}

impl From<Sha256Digest> for Cid {
    fn from(value: Sha256Digest) -> Self {
        // https://github.com/multiformats/multicodec/blob/master/table.csv
        let mh = multihash::Multihash::wrap(0x12, &value.0).unwrap();
        Cid::new_v1(0x71, mh)
    }
}

impl TryFrom<Cid> for Sha256Digest {
    type Error = anyhow::Error;

    fn try_from(value: Cid) -> Result<Self, Self::Error> {
        anyhow::ensure!(value.codec() == 0x71, "Unexpected codec");
        anyhow::ensure!(value.hash().code() == 0x12, "Unexpected hash algorithm");
        let digest: [u8; 32] = value.hash().digest().try_into()?;
        Ok(Self(digest))
    }
}

impl FromStr for Sha256Digest {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let cid = Cid::from_str(s)?;
        cid.try_into()
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Cid::from(*self))
    }
}

impl fmt::Debug for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Cid::from(*self))
    }
}

impl TreeTypes for TT {
    type Key = Key;
    type KeySeq = KeySeq;
    type Summary = Key;
    type SummarySeq = KeySeq;
    type Link = Sha256Digest;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Key {
    time: TimeData,
    tags: TagSet,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeData {
    min_lamport: u64,
    min_time: u64,
    max_time: u64,
}

impl TimeData {
    fn intersects(&self, that: &Self) -> bool {
        if self.max_time < that.min_time {
            return false;
        }
        if self.min_time > that.max_time {
            return false;
        }
        true
    }

    fn contains(&self, that: &Self) -> bool {
        if that.min_time < self.min_time {
            return false;
        }
        if that.max_time > self.max_time {
            return false;
        }
        true
    }

    fn combine(&mut self, b: &Self) {
        self.min_lamport = self.min_lamport.min(b.min_lamport);
        self.min_time = self.min_time.min(b.min_time);
        self.max_time = self.max_time.max(b.max_time);
    }
}

impl Key {
    pub fn single(lamport: u64, time: u64, tags: TagSet) -> Self {
        Self {
            time: TimeData {
                min_lamport: lamport,
                min_time: time,
                max_time: time,
            },
            tags,
        }
    }

    pub fn filter_tags(tags: TagSet) -> Self {
        Self {
            time: TimeData {
                min_lamport: u64::MIN,
                min_time: u64::MIN,
                max_time: u64::MAX,
            },
            tags,
        }
    }

    pub fn range(min_time: u64, max_time: u64, tags: TagSet) -> Self {
        Self {
            time: TimeData {
                min_lamport: 0,
                min_time,
                max_time,
            },
            tags,
        }
    }

    fn intersects(&self, that: &Key) -> bool {
        self.time.intersects(&that.time) && !self.tags.is_disjoint(&that.tags)
    }

    fn contains(&self, that: &Key) -> bool {
        self.time.contains(&that.time) && self.tags.is_superset(&that.tags)
    }
}

impl Key {
    fn combine(&mut self, b: &Self) {
        self.time.combine(&b.time);
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
    fn map_into<'a>(&self, keyseq: &'a KeySeq) -> TranslatedDnfQuery<'a> {
        TranslatedDnfQuery {
            query: self
                .0
                .iter()
                .map(|key| {
                    map_to_index_set(&keyseq.tags.tags, &key.tags).map(|index_set| TranslatedKey {
                        index_set,
                        time: key.time,
                    })
                })
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default(),
            seq: keyseq,
        }
    }
}

type IndexSet = VecSet<[u32; 4]>;

struct TranslatedKey {
    index_set: IndexSet,
    time: TimeData,
}

struct TranslatedDnfQuery<'a> {
    query: Vec<TranslatedKey>,
    seq: &'a KeySeq,
}

impl<'a> TranslatedDnfQuery<'a> {
    fn intersects(&self, i: usize) -> bool {
        self.query.iter().any(|q| {
            q.time.intersects(&self.seq.time(i).unwrap())
                && !q.index_set.is_disjoint(&self.seq.tags.elements[i])
        })
    }

    fn contains(&self, i: usize) -> bool {
        self.query.iter().any(|q| {
            q.time.contains(&self.seq.time(i).unwrap())
                && q.index_set.is_superset(&self.seq.tags.elements[i])
        })
    }

    fn intersecting(&self, matching: &mut [bool]) {
        for i in 0..self.seq.len().min(matching.len()) {
            if matching[i] {
                matching[i] = self.intersects(i);
            }
        }
    }

    fn containing(&self, matching: &mut [bool]) {
        for i in 0..self.seq.len().min(matching.len()) {
            if matching[i] {
                matching[i] = self.contains(i);
            }
        }
    }
}

impl Query<TT> for DnfQuery {
    fn intersecting(&self, _: u64, x: &BranchIndex<TT>, matching: &mut [bool]) {
        self.map_into(&x.summaries).intersecting(matching);
        // for i in 0..x.summaries.len().min(matching.len()) {
        //     if matching[i] {
        //         matching[i] = self.intersects(&x.summaries.get(i).unwrap());
        //     }
        // }
    }
    fn containing(&self, _: u64, x: &LeafIndex<TT>, matching: &mut [bool]) {
        self.map_into(&x.keys).containing(matching);
        // for i in 0..x.keys.len().min(matching.len()) {
        //     if matching[i] {
        //         matching[i] = self.contains(&x.keys.get(i).unwrap());
        //     }
        // }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, DagCbor)]
pub struct KeySeq {
    min_lamport: Vec<u64>,
    min_time: Vec<u64>,
    max_time: Vec<u64>,
    tags: TagIndex,
}

impl KeySeq {
    fn time(&self, i: usize) -> Option<TimeData> {
        if i < self.min_lamport.len() {
            Some(TimeData {
                min_time: self.min_time[i],
                max_time: self.max_time[i],
                min_lamport: self.min_lamport[i],
            })
        } else {
            None
        }
    }
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
                time: TimeData {
                    min_lamport: *min_lamport,
                    min_time: *min_time,
                    max_time: *max_time,
                },
                tags,
            })
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.tags.elements.len()
    }
}

impl Summarizable<Key> for KeySeq {
    fn summarize(&self) -> Key {
        let max_time = *self.max_time.iter().max().unwrap();
        let min_time = *self.min_time.iter().min().unwrap();
        let min_lamport = *self.min_lamport.iter().min().unwrap();
        let tags = self.tags.tags.clone();
        Key {
            time: TimeData {
                max_time,
                min_time,
                min_lamport,
            },
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
            min_lamport.push(value.time.min_lamport);
            min_time.push(value.time.min_time);
            max_time.push(value.time.max_time);
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
