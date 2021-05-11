#![allow(clippy::upper_case_acronyms)]
//! helper methods for the tests
use banyan::{
    forest::{BranchCache, Config, CryptoConfig, Forest, Transaction, TreeTypes},
    index::{CompactSeq, Summarizable},
    memstore::MemStore,
    tree::Tree,
};
use futures::Future;
use libipld::{
    cbor::DagCborCodec,
    codec::{Decode, Encode},
    Cid, DagCbor,
};
use quickcheck::{Arbitrary, Gen, TestResult};
use sha2::{Digest, Sha256};
use std::{
    convert::{TryFrom, TryInto},
    fmt,
    io::{Read, Seek, Write},
    iter::FromIterator,
};

#[allow(dead_code)]
pub type Txn = Transaction<TT, u64, MemStore<Sha256Digest>, MemStore<Sha256Digest>>;

#[derive(Debug, Clone)]
pub struct TT;

#[derive(Debug, Clone, Copy, PartialEq, Eq, DagCbor)]
pub struct Key(pub u64);

/// A trivial implementation of a CompactSeq as just a Seq.
///
/// This is useful mostly as a reference impl and for testing.
#[derive(Debug, Clone, DagCbor)]
pub struct KeySeq(pub Vec<Key>);

impl CompactSeq for KeySeq {
    type Item = Key;
    fn get(&self, index: usize) -> Option<Key> {
        self.0.get(index).cloned()
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl Summarizable<Key> for KeySeq {
    fn summarize(&self) -> Key {
        let mut res = self.0[0];
        for i in 1..self.0.len() {
            res.combine(&self.0[i]);
        }
        res
    }
}
impl FromIterator<Key> for KeySeq {
    fn from_iter<T: IntoIterator<Item = Key>>(iter: T) -> Self {
        KeySeq(iter.into_iter().collect())
    }
}

impl TreeTypes for TT {
    type Key = Key;
    type KeySeq = KeySeq;
    type Summary = Key;
    type SummarySeq = KeySeq;
    type Link = Sha256Digest;
}

impl Key {
    fn combine(&mut self, rhs: &Key) {
        self.0 |= rhs.0
    }
}

impl Arbitrary for Key {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(Arbitrary::arbitrary(g))
    }
}

#[allow(dead_code)]
pub fn txn(store: MemStore<Sha256Digest>, cache_cap: usize) -> Txn {
    let branch_cache = BranchCache::new(cache_cap);
    Txn::new(
        Forest::new(
            store.clone(),
            branch_cache,
        ),
        store,
    )
}

#[allow(dead_code)]
pub fn create_test_tree<I>(xs: I) -> anyhow::Result<(Tree<TT>, Txn)>
where
    I: IntoIterator<Item = (Key, u64)>,
    I::IntoIter: Send,
{
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let forest = txn(store, 1000);
    let mut tree = Tree::<TT>::debug();
    tree = forest.extend(&tree, xs)?;
    forest.assert_invariants(&tree)?;
    Ok((tree, forest))
}

#[allow(dead_code)]
pub async fn test<F: Future<Output = anyhow::Result<bool>>>(f: impl Fn() -> F) -> TestResult {
    match f().await {
        Ok(success) => TestResult::from_bool(success),
        Err(cause) => TestResult::error(cause.to_string()),
    }
}
/// For tests, we use a Sha2-256 digest as a link
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sha256Digest([u8; 32]);

impl Decode<DagCborCodec> for Sha256Digest {
    fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> anyhow::Result<Self> {
        Self::try_from(libipld::Cid::decode(c, r)?)
    }
}
impl Encode<DagCborCodec> for Sha256Digest {
    fn encode<W: Write>(&self, c: DagCborCodec, w: &mut W) -> anyhow::Result<()> {
        libipld::Cid::encode(&Cid::from(*self), c, w)
    }
}

impl Sha256Digest {
    pub fn digest(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        Sha256Digest(result.try_into().unwrap())
    }
    pub fn read(data: &[u8]) -> anyhow::Result<Self> {
        Ok(Self(data[0..32].try_into()?))
    }
}

impl AsRef<[u8]> for Sha256Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.as_ref()))
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

/// Some convenience fns so we don't have to depend on IterTools
pub(crate) trait IterExt<'a>
where
    Self: Iterator + Sized + Send + 'a,
{
    fn boxed(self) -> Box<dyn Iterator<Item = Self::Item> + Send + 'a> {
        Box::new(self)
    }
}

impl<'a, T: Iterator + Sized + Send + 'a> IterExt<'a> for T {}
