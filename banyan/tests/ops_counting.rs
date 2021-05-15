use banyan::{
    index::{BranchIndex, Index, LeafIndex},
    query::{AllQuery, EmptyQuery, OffsetRangeQuery},
    store::{BlockWriter, BranchCache, MemStore, ReadOnlyStore},
    Config, Forest, Secrets, StreamBuilder, Transaction,
};
use common::{create_test_tree, txn, IterExt, Key, KeySeq, Sha256Digest, TT};
use futures::prelude::*;
use libipld::{cbor::DagCborCodec, codec::Codec, Cid};
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::{
    convert::TryInto,
    iter,
    ops::Range,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
    usize,
};

use crate::common::no_offset_overlap;

mod common;

#[derive(Clone)]
struct OpsCountingStore {
    inner: MemStore<Sha256Digest>,
    reads: Arc<AtomicU64>,
    writes: Arc<AtomicU64>,
}

impl OpsCountingStore {
    fn new(inner: MemStore<Sha256Digest>) -> Self {
        Self {
            inner,
            reads: Arc::new(AtomicU64::default()),
            writes: Arc::new(AtomicU64::default()),
        }
    }

    fn reads(&self) -> u64 {
        self.reads.load(Ordering::SeqCst)
    }

    fn writes(&self) -> u64 {
        self.writes.load(Ordering::SeqCst)
    }
}

impl ReadOnlyStore<Sha256Digest> for OpsCountingStore {
    fn get(&self, link: &Sha256Digest) -> anyhow::Result<Box<[u8]>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(link)
    }
}

impl BlockWriter<Sha256Digest> for OpsCountingStore {
    fn put(&self, data: Vec<u8>) -> anyhow::Result<Sha256Digest> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.put(data)
    }
}

#[test]
fn ops_count_1() -> anyhow::Result<()> {
    let n = 1000000;
    let xs = (0..n).map(|i| (Key(i), i)).collect::<Vec<_>>();
    let store = OpsCountingStore::new(MemStore::new(usize::max_value(), Sha256Digest::digest));
    let branch_cache = BranchCache::<TT>::default();
    let txn = Transaction::new(Forest::new(store.clone(), branch_cache), store.clone());
    let mut builder = StreamBuilder::new(Config::debug_fast(), Secrets::default());
    txn.extend(&mut builder, xs)?;
    let tree = builder.snapshot();

    let t0 = Instant::now();
    let r0 = store.reads();
    let xs1 = txn.collect(&tree)?;
    let r_collect = store.reads() - r0;
    let t_collect = t0.elapsed();

    let t0 = Instant::now();
    let r0 = store.reads();
    let xs2: Vec<_> = txn.iter_filtered(&builder.snapshot(), AllQuery).collect();
    let r_iter = store.reads() - r0;
    let t_iter = t0.elapsed();

    let t0 = Instant::now();
    let r0 = store.reads();
    let xs3: Vec<_> = txn
        .iter_filtered(&builder.snapshot(), OffsetRangeQuery::from(0..n / 10))
        .collect();
    let r_iter_10 = store.reads() - r0;
    let t_iter_10 = t0.elapsed();

    assert!(xs1.len() as u64 == n);
    assert!(xs2.len() as u64 == n);
    assert!(xs3.len() as u64 == n / 10);

    println!("{} {} {}", r_collect, r_iter, r_iter_10);
    println!(
        "{} {} {}",
        t_collect.as_micros(),
        t_iter.as_micros(),
        t_iter_10.as_micros()
    );
    Ok(())
}
