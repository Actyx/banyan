use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use banyan::{
    query::{AllQuery, OffsetRangeQuery},
    store::{BlockWriter, BranchCache, MemStore, ReadOnlyStore},
    Config, Forest, Secrets, StreamBuilder, Transaction,
};
use banyan_utils::{
    tag_index::TagSet,
    tags::{Key, Sha256Digest, TT},
};

#[derive(Clone)]
struct OpsCountingStore<S> {
    inner: S,
    reads: Arc<AtomicU64>,
    writes: Arc<AtomicU64>,
}

impl<S> OpsCountingStore<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            reads: Arc::new(AtomicU64::default()),
            writes: Arc::new(AtomicU64::default()),
        }
    }

    fn reads(&self) -> u64 {
        self.reads.load(Ordering::SeqCst)
    }
}

impl<L, S: ReadOnlyStore<L>> ReadOnlyStore<L> for OpsCountingStore<S> {
    fn get(&self, link: &L) -> anyhow::Result<Box<[u8]>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(link)
    }
}

impl<L, S: BlockWriter<L> + Send + Sync> BlockWriter<L> for OpsCountingStore<S> {
    fn put(&self, data: Vec<u8>) -> anyhow::Result<L> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.put(data)
    }
}

#[test]
fn ops_count_1() -> anyhow::Result<()> {
    let n = 1000000;
    let capacity = 0;
    let xs = (0..n)
        .map(|i| (Key::single(i, i, TagSet::empty()), i))
        .collect::<Vec<_>>();
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let store = OpsCountingStore::new(store);
    let branch_cache = BranchCache::<TT>::new(capacity);
    let txn = Transaction::new(
        Forest::new(store.clone(), branch_cache.clone()),
        store.clone(),
    );
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
    let r_iter_small = store.reads() - r0;
    let t_iter_small = t0.elapsed();

    let t0 = Instant::now();
    let r0 = store.reads();
    let xs4: Vec<_> = txn
        .iter_filtered(&builder.snapshot(), OffsetRangeQuery::from(0..10))
        .collect();
    let r_iter_tiny = store.reads() - r0;
    let t_iter_tiny = t0.elapsed();

    assert!(xs1.len() as u64 == n);
    assert!(xs2.len() as u64 == n);
    assert!(xs3.len() as u64 == n / 10);
    assert!(xs4.len() as u64 == 10);

    assert_eq!(r_collect, 65);
    assert_eq!(r_iter, 65);
    assert_eq!(r_iter_small, 35);
    assert_eq!(r_iter_tiny, 35); // this seems weird...

    println!("{} {} {} {}", r_collect, r_iter, r_iter_small, r_iter_tiny);
    println!(
        "{} {} {} {}",
        t_collect.as_micros(),
        t_iter.as_micros(),
        t_iter_small.as_micros(),
        t_iter_tiny.as_micros(),
    );
    Ok(())
}
