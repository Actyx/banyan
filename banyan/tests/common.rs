#![allow(clippy::upper_case_acronyms, dead_code, clippy::type_complexity)]
//! helper methods for the tests
use banyan::{
    index::{CompactSeq, Summarizable},
    query::AllQuery,
    store::{BranchCache, MemStore, ReadOnlyStore},
    StreamBuilder, Transaction, Tree, TreeTypes,
};
use futures::Future;
use libipld::{
    cbor::DagCborCodec,
    codec::{Codec, Decode, Encode},
    Cid, DagCbor, Ipld,
};
use quickcheck::{Arbitrary, Gen, TestResult};
use range_collections::RangeSet;
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    convert::{TryFrom, TryInto},
    fmt,
    io::{Read, Seek, Write},
    iter::FromIterator,
    ops::Range,
};

#[allow(dead_code)]
pub type Forest = banyan::Forest<TT, MemStore<Sha256Digest>>;

#[allow(dead_code)]
pub type Txn = Transaction<TT, MemStore<Sha256Digest>, MemStore<Sha256Digest>>;

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
    Txn::new(Forest::new(store.clone(), branch_cache), store)
}

/// A recipe for a tree, which will either be packed or unpacked
#[derive(Debug, Clone)]
pub enum TestTree {
    Unpacked(UnpackedTestTree),
    Packed(PackedTestTree),
}

impl TestTree {
    pub fn packed(xs: Vec<(Key, u64)>) -> Self {
        Self::Packed(PackedTestTree(xs))
    }

    pub fn unpacked(xss: Vec<Vec<(Key, u64)>>) -> Self {
        Self::Unpacked(UnpackedTestTree(xss))
    }

    pub fn tree(self) -> anyhow::Result<(Tree<TT, u64>, Txn, Vec<(Key, u64)>)> {
        let (b, txn, xs) = self.builder()?;
        Ok((b.snapshot(), txn, xs))
    }

    pub fn builder(self) -> anyhow::Result<(StreamBuilder<TT, u64>, Txn, Vec<(Key, u64)>)> {
        match self {
            Self::Unpacked(x) => x.builder(),
            Self::Packed(x) => x.builder(),
        }
    }
}

impl Arbitrary for TestTree {
    fn arbitrary(g: &mut Gen) -> Self {
        if Arbitrary::arbitrary(g) {
            Self::Packed(Arbitrary::arbitrary(g))
        } else {
            Self::Unpacked(Arbitrary::arbitrary(g))
        }
    }
}

/// Recipe for a packed test tree
#[derive(Debug, Clone)]
pub struct PackedTestTree(Vec<(Key, u64)>);

impl PackedTestTree {
    /// Convert this into an actual tree
    pub fn builder(self) -> anyhow::Result<(StreamBuilder<TT, u64>, Txn, Vec<(Key, u64)>)> {
        let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
        let txn = txn(store, 1 << 20);
        let mut builder = StreamBuilder::<TT, u64>::debug();
        let xs = self.0.clone();
        txn.extend(&mut builder, self.0)?;
        txn.assert_invariants(&builder)?;
        Ok((builder, txn, xs))
    }
}

impl Arbitrary for PackedTestTree {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(Arbitrary::arbitrary(g))
    }
}

/// Recipe for an unpacked test tree
#[derive(Debug, Clone)]
pub struct UnpackedTestTree(Vec<Vec<(Key, u64)>>);

impl UnpackedTestTree {
    /// Convert this into an actual tree
    pub fn builder(self) -> anyhow::Result<(StreamBuilder<TT, u64>, Txn, Vec<(Key, u64)>)> {
        let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
        let txn = txn(store, 1 << 20);
        let mut builder = StreamBuilder::<TT, u64>::debug();
        let xs = self.0.iter().cloned().flatten().collect();
        for xs in self.0 {
            txn.extend_unpacked(&mut builder, xs)?;
        }
        txn.assert_invariants(&builder)?;
        Ok((builder, txn, xs))
    }
}

impl Arbitrary for UnpackedTestTree {
    fn arbitrary(g: &mut Gen) -> Self {
        let xs: Vec<(Key, u64)> = Arbitrary::arbitrary(g);
        let mut cuts: Vec<usize> = Arbitrary::arbitrary(g);
        if !xs.is_empty() {
            for x in cuts.iter_mut() {
                *x %= xs.len();
            }
            cuts.push(0);
            cuts.push(xs.len());
            cuts.sort_unstable();
            cuts.dedup();
        } else {
            cuts.clear();
        }
        // cut xs in random places
        let res = cuts
            .iter()
            .zip(cuts.iter().skip(1))
            .map(|(start, end)| xs[*start..*end].to_vec())
            .collect::<Vec<_>>();
        UnpackedTestTree(res)
    }
}

/// Extract all links from a tree
pub fn links(
    forest: &Forest,
    tree: &Tree<TT, u64>,
    links: &mut HashSet<Sha256Digest>,
) -> anyhow::Result<()> {
    let link_opts = forest
        .iter_index(&tree, AllQuery)
        .map(|x| x.map(|x| x.link().as_ref().cloned()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    links.extend(link_opts.into_iter().flatten());
    Ok(())
}

fn same_secrets<X, Y>(a: &Tree<TT, X>, b: &Tree<TT, Y>) -> bool {
    match (a.secrets(), b.secrets()) {
        (Some(sa), Some(sb)) => {
            sa.index_key() == sb.index_key() || sa.value_key() == sb.value_key()
        }
        _ => {
            // return true here since the empty tree is "compatible" with all trees
            true
        }
    }
}

type OffsetSet = RangeSet<u64, [u64; 16]>;

/// utility struct for encoding and decoding - copied for the tests
#[derive(DagCbor)]
struct IpldNode(u64, Vec<Cid>, Ipld);

impl IpldNode {
    fn into_data(self) -> anyhow::Result<(u64, Vec<Cid>, Vec<u8>)> {
        if let Ipld::Bytes(data) = self.2 {
            Ok((self.0, self.1, data))
        } else {
            Err(anyhow::anyhow!("expected ipld bytes"))
        }
    }
}

fn block_range(forest: &Forest, hash: &Sha256Digest) -> anyhow::Result<Range<u64>> {
    let blob = forest.store().get(hash)?;
    let (offset, _, encrypted) = DagCborCodec.decode::<IpldNode>(&blob)?.into_data()?;
    let len = encrypted.len() as u64;
    let end_offset = offset
        .checked_add(len)
        .ok_or_else(|| anyhow::anyhow!("seek wraparound"))?;
    Ok(offset..end_offset)
}

/// check that for the given blocks, ranges do not overlap
fn no_range_overlap(forest: &Forest, hashes: HashSet<Sha256Digest>) -> anyhow::Result<bool> {
    let mut ranges = OffsetSet::empty();
    let mut result = true;
    for hash in hashes {
        let range = OffsetSet::from(block_range(forest, &hash)?);
        // println!("{} {:?}", hash, range);
        if !ranges.is_disjoint(&range) {
            result = false;
        }
        ranges |= range;
    }
    Ok(result)
}

#[allow(dead_code)]
pub fn no_offset_overlap<'a>(
    forest: &'a Forest,
    trees: impl IntoIterator<Item = &'a Tree<TT, u64>>,
) -> anyhow::Result<bool> {
    let mut trees = trees.into_iter().peekable();
    if let Some(first) = trees.peek().cloned() {
        let mut hashes = HashSet::new();
        for tree in trees {
            anyhow::ensure!(same_secrets(first, tree), "not the same secrets");
            links(forest, tree, &mut hashes)?;
        }
        no_range_overlap(forest, hashes)
    } else {
        Ok(true)
    }
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
