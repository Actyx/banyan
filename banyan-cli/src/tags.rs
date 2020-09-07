use crate::ipfs::Cid;
use banyan::index::*;
use banyan::{forest::*, query::Query};
use bitvec::prelude::*;
use maplit::btreeset;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, io, iter::FromIterator, sync::Arc};

#[derive(Debug)]
pub struct TT {}

impl TreeTypes for TT {
    type Key = Key;
    type Seq = KeySeq;
    type Link = Cid;
    fn serialize_branch(
        links: &[&Self::Link],
        data: Vec<u8>,
        w: impl io::Write,
    ) -> anyhow::Result<()> {
        serde_cbor::to_writer(w, &(links, serde_cbor::Value::Bytes(data)))
            .map_err(|e| anyhow::Error::new(e))
    }
    fn deserialize_branch(reader: impl io::Read) -> anyhow::Result<(Vec<Self::Link>, Vec<u8>)> {
        let (links, data): (Vec<Self::Link>, serde_cbor::Value) = serde_cbor::from_reader(reader)?;
        if let serde_cbor::Value::Bytes(data) = data {
            Ok((links, data))
        } else {
            Err(anyhow::anyhow!("expected cbor bytes"))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Ord, Eq)]
pub struct Tag(Arc<str>);

impl Tag {
    pub fn new(text: &str) -> Self {
        Self(text.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tags(pub BTreeSet<Tag>);

impl Tags {
    fn empty() -> Self {
        Self(BTreeSet::new())
    }
    pub fn single(text: &str) -> Self {
        Self(btreeset! { Tag(text.into()) })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Key {
    min_lamport: u64,
    min_time: u64,
    max_time: u64,
    tags: Tags,
}

impl Key {
    pub fn single(lamport: u64, time: u64, tags: Tags) -> Self {
        Self {
            min_lamport: lamport,
            min_time: time,
            max_time: time,
            tags,
        }
    }

    pub fn filter_tags(tags: Tags) -> Self {
        Self {
            min_lamport: u64::MIN,
            min_time: u64::MIN,
            max_time: u64::MAX,
            tags,
        }
    }

    pub fn range(min_time: u64, max_time: u64, tags: Tags) -> Self {
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
        if self.tags.0.is_disjoint(&that.tags.0) {
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
        if !that.tags.0.is_subset(&self.tags.0) {
            return false;
        }
        true
    }
}

impl Semigroup for Key {
    fn combine(&mut self, b: &Self) {
        self.min_lamport = self.min_lamport.min(b.min_lamport);
        self.min_time = self.min_time.min(b.min_time);
        self.max_time = self.max_time.max(b.max_time);
        self.tags.0.extend(b.tags.0.iter().cloned());
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
    fn intersecting(&self, _: u64, x: &BranchIndex<TT>, matching: &mut BitVec) {
        for (i, s) in x.summaries().take(matching.len()).enumerate() {
            if matching[i] {
                matching.set(i, self.intersects(&s));
            }
        }
    }
    fn containing(&self, _: u64, x: &LeafIndex<TT>, matching: &mut BitVec) {
        for (i, s) in x.keys().take(matching.len()).enumerate() {
            if matching[i] {
                matching.set(i, self.contains(&s));
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeySeq {
    min_lamport: Vec<u64>,
    min_time: Vec<u64>,
    max_time: Vec<u64>,
    tags: Vec<Tags>,
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
                tags: tags.clone(),
            })
        } else {
            None
        }
    }

    fn count(&self) -> u64 {
        self.tags.len() as u64
    }

    fn summarize(&self) -> Key {
        let mut result = self.get(0).unwrap();
        for i in 1..self.tags.len() {
            result.combine(&self.get(i).unwrap());
        }
        result
    }
}

impl FromIterator<Key> for KeySeq {
    fn from_iter<T: IntoIterator<Item = Key>>(iter: T) -> Self {
        let mut min_lamport = Vec::new();
        let mut min_time = Vec::new();
        let mut max_time = Vec::new();
        let mut tags = Vec::new();
        for value in iter.into_iter() {
            min_lamport.push(value.min_lamport);
            min_time.push(value.min_time);
            max_time.push(value.max_time);
            tags.push(value.tags.clone());
        }
        Self {
            min_lamport,
            min_time,
            max_time,
            tags,
        }
    }
}
