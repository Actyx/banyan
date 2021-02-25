use banyan::{
    forest::{BranchCache, CryptoConfig, Forest},
    forest::{Config, Transaction, TreeTypes},
    index::{BranchIndex, CompactSeq, Index, LeafIndex, Summarizable, VecSeq},
    memstore::MemStore,
    query::{AllQuery, OffsetRangeQuery},
    tree::Tree,
};
use fnv::FnvHashSet;
use futures::prelude::*;
use libipld::{
    cbor::DagCborCodec,
    codec::{Codec, Decode, Encode},
    ipld, Cid, DagCbor, Ipld,
};
use quickcheck::{quickcheck, Arbitrary, Gen, TestResult};
use std::convert::TryFrom;
use std::{convert::TryInto, iter, iter::FromIterator, ops::Range, str::FromStr};
use store::Sha256Digest;
use tracing::Value;
mod store;

type Txn = Transaction<TT, Payload, MemStore<Sha256Digest>, MemStore<Sha256Digest>>;

impl Arbitrary for Key {
    fn arbitrary(g: &mut Gen) -> Self {
        let t: u8 = Arbitrary::arbitrary(g);
        let ipld = if t < 200 {
            let x: u64 = Arbitrary::arbitrary(g);
            ipld! { x }
        } else {
            let cid = Cid::from_str("bafyreiewdnw5h3pdzohmxkwl22g6aqgnpdvs5vmiseymz22mjeti5jgvay")
                .unwrap();
            ipld! { cid }
        };
        Self(ipld)
    }
}

impl Arbitrary for Payload {
    fn arbitrary(g: &mut Gen) -> Self {
        let t: u8 = Arbitrary::arbitrary(g);
        let ipld = if t < 200 {
            let x: u64 = Arbitrary::arbitrary(g);
            ipld! { x }
        } else {
            let cid = Cid::from_str("bafyreih3ryqpylsmh4siyygdtplff46bgrzjro4xpofu2widxbifkyqgam")
                .unwrap();
            ipld! { cid }
        };
        Self(ipld)
    }
}

#[derive(Debug, Clone)]
struct TT;

#[derive(Debug, Clone, PartialEq, DagCbor)]
struct Key(Ipld);

#[derive(Debug, Clone, PartialEq, DagCbor)]
struct Payload(Ipld);
#[derive(Debug, Clone)]
pub struct UnitSeq(usize);

impl Encode<DagCborCodec> for UnitSeq {
    fn encode<W: std::io::Write>(&self, c: DagCborCodec, w: &mut W) -> anyhow::Result<()> {
        (self.0 as u64).encode(c, w)
    }
}

impl Decode<DagCborCodec> for UnitSeq {
    fn decode<R: std::io::Read + std::io::Seek>(
        c: DagCborCodec,
        r: &mut R,
    ) -> anyhow::Result<Self> {
        let t = u64::decode(c, r)?;
        Ok(Self(usize::try_from(t)?))
    }
}

impl CompactSeq for UnitSeq {
    type Item = ();
    fn get(&self, index: usize) -> Option<()> {
        if index < self.0 {
            Some(())
        } else {
            None
        }
    }
    fn len(&self) -> usize {
        self.0 as usize
    }
}

impl Summarizable<()> for UnitSeq {
    fn summarize(&self) {}
}

impl FromIterator<()> for UnitSeq {
    fn from_iter<T: IntoIterator<Item = ()>>(iter: T) -> Self {
        Self(iter.into_iter().count())
    }
}

impl TreeTypes for TT {
    type Key = Key;
    type KeySeq = VecSeq<Key>;
    type Summary = ();
    type SummarySeq = UnitSeq;
    type Link = Sha256Digest;
}

fn txn(store: MemStore<Sha256Digest>) -> Txn {
    let branch_cache = BranchCache::new(1000);
    Txn::new(
        Forest::new(
            store.clone(),
            branch_cache,
            CryptoConfig::default(),
            Config::debug(),
        ),
        store,
    )
}

fn create_test_tree<I>(xs: I) -> anyhow::Result<(Tree<TT>, Txn)>
where
    I: IntoIterator<Item = (Key, Payload)>,
    I::IntoIter: Send,
{
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let forest = txn(store);
    let mut tree = Tree::<TT>::empty();
    tree = forest.extend(&tree, xs)?;
    forest.assert_invariants(&tree)?;
    Ok((tree, forest))
}

async fn test<F: Future<Output = anyhow::Result<bool>>>(f: impl Fn() -> F) -> TestResult {
    match f().await {
        Ok(success) => TestResult::from_bool(success),
        Err(cause) => TestResult::error(cause.to_string()),
    }
}

fn links_get_properly_scraped(xs: Vec<(Key, Payload)>) -> anyhow::Result<bool> {
    let (_, txn) = create_test_tree(xs.clone())?;
    let store = txn.into_writer().into_inner()?;
    let mut references_from_blocks = FnvHashSet::default();
    let mut references_from_kv = FnvHashSet::default();
    // collect references from blocks
    for (_, v) in store {
        let t: Vec<Ipld> = DagCborCodec.decode(&v)?;
        anyhow::ensure!(t.len() == 2);
        t[0].references(&mut references_from_blocks);
    }
    // collect references from keys and values
    for (k, v) in &xs {
        k.0.references(&mut references_from_kv);
        v.0.references(&mut references_from_kv);
    }
    // check that at least all references from keys and values are in the blocks
    Ok(references_from_blocks.is_superset(&references_from_kv))
}

quickcheck! {
    fn tree_link_scraping(xs: Vec<(Key, Payload)>) -> anyhow::Result<bool> {
        links_get_properly_scraped(xs.clone())
    }
}

#[test]
fn tree_link_scraping_payload() -> anyhow::Result<()> {
    let xs = vec![(
        Key(ipld! { 1234 }),
        Payload(
            ipld! { Cid::from_str("bafyreih3ryqpylsmh4siyygdtplff46bgrzjro4xpofu2widxbifkyqgam")? },
        ),
    )];
    assert!(links_get_properly_scraped(xs)?);
    Ok(())
}

#[test]
fn tree_link_scraping_key() -> anyhow::Result<()> {
    let xs = vec![(
        Key(
            ipld! { Cid::from_str("bafyreih3ryqpylsmh4siyygdtplff46bgrzjro4xpofu2widxbifkyqgam")? },
        ),
        Payload(ipld! { 1234 }),
    )];
    assert!(links_get_properly_scraped(xs)?);
    Ok(())
}
