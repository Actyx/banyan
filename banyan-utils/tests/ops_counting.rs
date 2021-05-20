use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use banyan::{
    query::{AllQuery, OffsetRangeQuery, Query},
    store::{BlockWriter, BranchCache, MemStore, ReadOnlyStore},
    Config, Forest, Secrets, StreamBuilder, Transaction, Tree,
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

#[allow(clippy::clippy::type_complexity)]
fn test_ops_count(
    name: &str,
    forest: &Forest<TT, OpsCountingStore<MemStore<Sha256Digest>>>,
    tree: &Tree<TT, u64>,
    query: impl Query<TT> + Clone + 'static,
) -> (Vec<anyhow::Result<(u64, Key, u64)>>, Duration, u64) {
    let r0 = forest.store().reads();
    let t0 = Instant::now();
    let xs: Vec<anyhow::Result<(u64, Key, u64)>> = forest.iter_filtered(&tree, query).collect();
    let dt = t0.elapsed();
    let dr = forest.store().reads() - r0;
    println!("{} {} {}", name, dr, dt.as_micros());
    (xs, dt, dr)
}

#[test]
fn ops_count_1() -> anyhow::Result<()> {
    let config = Config {
        target_leaf_size: 1 << 14,
        max_leaf_count: 1 << 14,
        max_summary_branches: 32,
        max_key_branches: 4,
        max_uncompressed_leaf_size: 16 * 1024 * 1024,
        zstd_level: 10,
    };
    let n = 1000000;
    // test with a rather large cache, but a new one on every test
    //
    // we are interested in the number of distinct blocks being accessed.
    let capacity = 16 << 20;
    let xs = (0..n)
        .map(|i| (Key::single(i, i, TagSet::empty()), i))
        .collect::<Vec<_>>();
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let store = OpsCountingStore::new(store);
    let branch_cache = BranchCache::<TT>::new(0);
    let txn = Transaction::new(Forest::new(store.clone(), branch_cache), store.clone());
    let mut builder = StreamBuilder::new(config, Secrets::default());
    txn.extend(&mut builder, xs)?;
    let tree = builder.snapshot();

    let t0 = Instant::now();
    let r0 = store.reads();
    let xs1 = txn.collect(&tree)?;
    let r_collect = store.reads() - r0;
    println!("collect {} {}", r_collect, t0.elapsed().as_micros());

    let (xs2, _, r_iter) = test_ops_count(
        "iter   ",
        &Forest::new(store.clone(), BranchCache::new(capacity)),
        &tree,
        AllQuery,
    );
    let (xs3, _, r_iter_small) = test_ops_count(
        "small  ",
        &Forest::new(store.clone(), BranchCache::new(capacity)),
        &tree,
        OffsetRangeQuery::from(0..n / 10),
    );
    let (xs4, _, r_iter_tiny) = test_ops_count(
        "tiny   ",
        &Forest::new(store.clone(), BranchCache::new(capacity)),
        &tree,
        OffsetRangeQuery::from(0..10),
    );

    assert!(xs1.len() as u64 == n);
    assert!(xs2.len() as u64 == n);
    assert!(xs3.len() as u64 == n / 10);
    assert!(xs4.len() as u64 == 10);

    assert_eq!(r_collect, 79);
    assert_eq!(r_iter, 79);
    assert_eq!(r_iter_small, 10);
    assert_eq!(r_iter_tiny, 3);

    Ok(())
}
