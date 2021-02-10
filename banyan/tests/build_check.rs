use banyan::{
    forest::{BranchCache, CryptoConfig, Forest},
    index::{CompactSeq, HasSummary},
};
use banyan::{
    forest::{Config, Transaction, TreeTypes},
    memstore::MemStore,
    query::{AllQuery, OffsetRangeQuery},
    tree::Tree,
};
use futures::prelude::*;
use quickcheck::{Arbitrary, Gen, TestResult};
use serde::{Deserialize, Serialize};
use std::{iter, iter::FromIterator, ops::Range};
use store::Sha256Digest;
mod store;

type Txn = Transaction<TT, u64, MemStore<Sha256Digest>, MemStore<Sha256Digest>>;

#[derive(Debug, Clone)]
struct TT;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Key(u64);
/// A trivial implementation of a CompactSeq as just a Seq.
///
/// This is useful mostly as a reference impl and for testing.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct KeySeq(Vec<Key>);

impl CompactSeq for KeySeq {
    type Item = Key;
    fn get(&self, index: usize) -> Option<Key> {
        self.0.get(index).cloned()
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl HasSummary<Key> for KeySeq {
    fn summarize(&self) -> Key {
        let mut res = self.0[0].clone();
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

async fn create_test_tree<I>(xs: I) -> anyhow::Result<(Tree<TT>, Txn)>
where
    I: IntoIterator<Item = (Key, u64)>,
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

#[quickcheck_async::tokio]
async fn build_stream(xs: Vec<(Key, u64)>) -> quickcheck::TestResult {
    test(|| async {
        let (tree, txn) = create_test_tree(xs.clone()).await?;
        let actual = txn
            .stream_filtered(&tree, AllQuery)
            .map_ok(|(_, k, v)| (k, v))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(actual == xs)
    })
    .await
}

/// checks that stream_filtered returns the same elements as filtering each element manually
async fn compare_filtered(xs: Vec<(Key, u64)>, range: Range<u64>) -> anyhow::Result<bool> {
    let range = range.clone();
    let (tree, txn) = create_test_tree(xs.clone()).await?;
    let actual = txn
        .stream_filtered(&tree, OffsetRangeQuery::from(range.clone()))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    let expected = xs
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, (k, v))| (i as u64, k, v))
        .filter(|(offset, _, _)| range.contains(offset))
        .collect::<Vec<_>>();
    Ok(actual == expected)
}

/// checks that stream_filtered_chunked returns the same elements as filtering each element manually
async fn compare_filtered_chunked(xs: Vec<(Key, u64)>, range: Range<u64>) -> anyhow::Result<bool> {
    let range = range.clone();
    let (tree, txn) = create_test_tree(xs.clone()).await?;
    let actual = txn
        .stream_filtered_chunked(&tree, OffsetRangeQuery::from(range.clone()), &|_| ())
        .map(|chunk_result| chunk_result.map(|chunk| stream::iter(chunk.data.into_iter().map(Ok))))
        .try_flatten()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    let expected = xs
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, (k, v))| (i as u64, k, v))
        .filter(|(offset, _, _)| range.contains(offset))
        .collect::<Vec<_>>();
    Ok(actual == expected)
}

/// checks that stream_filtered_chunked returns the same elements as filtering each element manually
async fn compare_filtered_chunked_reverse(
    xs: Vec<(Key, u64)>,
    range: Range<u64>,
) -> anyhow::Result<bool> {
    let range = range.clone();
    let (tree, txn) = create_test_tree(xs.clone()).await?;
    let actual = txn
        .stream_filtered_chunked_reverse(&tree, OffsetRangeQuery::from(range.clone()), &|_| ())
        .map(|chunk_result| chunk_result.map(|chunk| stream::iter(chunk.data.into_iter().map(Ok))))
        .try_flatten()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    let expected = xs
        .iter()
        .cloned()
        .enumerate()
        .rev()
        .map(|(i, (k, v))| (i as u64, k, v))
        .filter(|(offset, _, _)| range.contains(offset))
        .collect::<Vec<_>>();
    if actual != expected {
        println!("{:?} {:?}", actual, expected);
    }
    Ok(actual == expected)
}

/// checks that stream_filtered_chunked returns the same elements as filtering each element manually
async fn filtered_chunked_no_holes(xs: Vec<(Key, u64)>, range: Range<u64>) -> anyhow::Result<bool> {
    let range = range.clone();
    let (tree, txn) = create_test_tree(xs.clone()).await?;
    let chunks = txn
        .stream_filtered_chunked(&tree, OffsetRangeQuery::from(range.clone()), &|_| ())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    let max_offset = chunks.iter().fold(0, |offset, chunk| {
        if offset == chunk.range.start {
            chunk.range.end
        } else {
            offset
        }
    });
    Ok(max_offset == (xs.len() as u64))
}

#[quickcheck_async::tokio]
async fn build_stream_filtered(xs: Vec<(Key, u64)>, range: Range<u64>) -> quickcheck::TestResult {
    test(|| compare_filtered(xs.clone(), range.clone())).await
}

#[quickcheck_async::tokio]
async fn build_stream_filtered_chunked(
    xs: Vec<(Key, u64)>,
    range: Range<u64>,
) -> quickcheck::TestResult {
    test(|| compare_filtered_chunked(xs.clone(), range.clone())).await
}

#[quickcheck_async::tokio]
async fn build_stream_filtered_chunked_reverse(
    xs: Vec<(Key, u64)>,
    range: Range<u64>,
) -> quickcheck::TestResult {
    test(|| compare_filtered_chunked_reverse(xs.clone(), range.clone())).await
}

#[quickcheck_async::tokio]
async fn build_stream_filtered_chunked_no_holes(
    xs: Vec<(Key, u64)>,
    range: Range<u64>,
) -> quickcheck::TestResult {
    test(|| filtered_chunked_no_holes(xs.clone(), range.clone())).await
}

#[quickcheck_async::tokio]
async fn build_get(xs: Vec<(Key, u64)>) -> quickcheck::TestResult {
    test(|| async {
        let (tree, txn) = create_test_tree(xs.clone()).await?;
        let mut actual = Vec::new();
        for i in 0..xs.len() as u64 {
            actual.push(txn.get(&tree, i)?.unwrap());
        }
        Ok(actual == xs)
    })
    .await
}

#[quickcheck_async::tokio]
async fn build_pack(xss: Vec<Vec<(Key, u64)>>) -> quickcheck::TestResult {
    test(|| async {
        let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
        let forest = txn(store);
        let mut tree = Tree::<TT>::empty();
        // flattened xss for reference
        let xs = xss.iter().cloned().flatten().collect::<Vec<_>>();
        // build complex unbalanced tree
        for xs in xss.iter() {
            tree = forest.extend_unpacked(&tree, xs.clone()).unwrap();
        }
        // check that the unbalanced tree itself matches the elements
        let actual: Vec<_> = forest
            .collect(&tree)?
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .unwrap();
        let unpacked_matches = xs == actual;

        tree = forest.pack(&tree)?;
        assert!(forest.is_packed(&tree)?);
        let actual: Vec<_> = forest
            .collect(&tree)?
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .unwrap();
        let packed_matches = xs == actual;

        Ok(unpacked_matches && packed_matches)
    })
    .await
}

#[quickcheck_async::tokio]
async fn retain(xss: Vec<Vec<(Key, u64)>>) -> quickcheck::TestResult {
    test(|| async {
        let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
        let forest = txn(store);
        let mut tree = Tree::<TT>::empty();
        // flattened xss for reference
        let xs = xss.iter().cloned().flatten().collect::<Vec<_>>();
        // build complex unbalanced tree
        for xs in xss.iter() {
            tree = forest.extend_unpacked(&tree, xs.clone()).unwrap();
        }
        tree = forest.retain(&tree, &OffsetRangeQuery::from(xs.len() as u64..))?;
        forest.assert_invariants(&tree)?;
        tree = forest.pack(&tree)?;
        tree = forest.retain(&tree, &OffsetRangeQuery::from(xs.len() as u64..))?;
        forest.assert_invariants(&tree)?;
        Ok(true)
    })
    .await
}

#[tokio::test]
async fn filter_test_simple() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let res = compare_filtered(vec![(Key(1), 1), (Key(2), 2)], 0..1).await;
    assert_eq!(res.ok(), Some(true));
    Ok(())
}

#[tokio::test]
async fn stream_test_simple() -> anyhow::Result<()> {
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let forest = txn(store);
    let mut trees = Vec::new();
    for n in 1..=10u64 {
        let mut tree = Tree::<TT>::empty();
        tree = forest.extend(&tree, (0..n).map(|t| (Key(t), n)))?;
        forest.assert_invariants(&tree)?;
        trees.push(tree);
    }
    println!("{:?}", trees);
    let res = forest
        .read()
        .stream_trees(AllQuery, stream::iter(trees).boxed());
    let res = res.collect::<Vec<_>>().await;
    println!("{:?}", res);
    Ok(())
}

fn build(items: &mut Vec<u32>) {
    const MAX_BRANCH: usize = 8;
    if items.len() > 1 {
        let pos = items
            .iter()
            .position(|x| *x != items[0])
            .unwrap_or(items.len());
        if pos >= MAX_BRANCH || pos == items.len() || pos == items.len() - 1 {
            // a valid node can be built from the start
            items.splice(
                0..MAX_BRANCH.min(items.len()),
                vec![items[0].wrapping_add(1)],
            );
        } else {
            // temporarily remove the start and recurse
            let removed = items.splice(0..pos, iter::empty()).collect::<Vec<_>>();
            build(items);
            items.splice(0..0, removed);
        }
    }
}

#[test]
fn build_test() {
    let mut v = vec![
        1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    while v.len() > 1 {
        println!("{:?}", v);
        build(&mut v);
    }
    println!("{:?}", v);
}

#[quickcheck_async::tokio]
async fn build_converges(data: Vec<u32>) -> bool {
    let mut v = data;
    v.sort_unstable();
    v.reverse();
    while v.len() > 1 {
        build(&mut v);
    }
    true
}
