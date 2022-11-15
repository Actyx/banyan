#![allow(clippy::upper_case_acronyms)]
use banyan::{
    index::{UnitSeq, VecSeq},
    store::{BranchCache, MemStore},
    Forest, StreamBuilder, Transaction, Tree, TreeTypes,
};
use cbor_data::codec::{CodecError, ReadCbor, WriteCbor};
use common::Sha256Digest;
use fnv::FnvHashSet;
use libipld::{cbor::DagCborCodec, codec::Codec, ipld, prelude::Decode, Cid, DagCbor, Ipld};
use quickcheck::{quickcheck, Arbitrary, Gen};
use std::{io::Cursor, str::FromStr};

mod common;

type Txn = Transaction<TT, MemStore<Sha256Digest>, MemStore<Sha256Digest>>;

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

impl WriteCbor for Payload {
    fn write_cbor<W: cbor_data::Writer>(&self, w: W) -> W::Output {
        let bytes = DagCborCodec.encode(&self.0).unwrap();
        w.write_trusting(&*bytes)
    }
}

impl ReadCbor for Payload {
    fn fmt(f: &mut impl std::fmt::Write) -> std::fmt::Result {
        write!(f, "Payload")
    }

    fn read_cbor(cbor: &cbor_data::Cbor) -> cbor_data::codec::Result<Self>
    where
        Self: Sized,
    {
        Ipld::decode(DagCborCodec, &mut Cursor::new(cbor.as_slice()))
            .map(Payload)
            .map_err(|err| CodecError::Custom(err.into()))
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
    let branch_cache = BranchCache::default();
    Txn::new(Forest::new(store.clone(), branch_cache), store)
}

fn create_test_tree<I>(xs: I) -> anyhow::Result<(Tree<TT, Payload>, Txn)>
where
    I: IntoIterator<Item = (Key, Payload)>,
    I::IntoIter: Send,
{
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let mut forest = txn(store);
    let mut tree = StreamBuilder::<TT, Payload>::debug();
    forest.extend(&mut tree, xs)?;
    forest.assert_invariants(&tree)?;
    Ok((tree.snapshot(), forest))
}

fn links_get_properly_scraped(xs: Vec<(Key, Payload)>) -> anyhow::Result<bool> {
    let (_, txn) = create_test_tree(xs.clone())?;
    let store = txn.into_writer().into_inner()?;
    let mut references_from_blocks = FnvHashSet::default();
    let mut references_from_kv = FnvHashSet::default();
    // collect references from blocks
    for (_, v) in store {
        let t: Vec<Ipld> = DagCborCodec.decode(&v)?;
        anyhow::ensure!(t.len() == 3);
        t[1].references(&mut references_from_blocks);
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
        links_get_properly_scraped(xs)
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
