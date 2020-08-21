use banyan::index::{Semigroup, SimpleCompactSeq};
use banyan::{
    query::{AllQuery, OffsetRangeQuery},
    tree::{Config, Forest, Tree, TreeTypes},
};
use futures::prelude::*;
use ipfs::MemStore;
use quickcheck::{Arbitrary, Gen, TestResult};
use serde::{Deserialize, Serialize};
use std::{ops::Range, sync::Arc};
mod ipfs;

struct TT;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Key(u64);

impl TreeTypes for TT {
    type Key = Key;
    type Seq = SimpleCompactSeq<Key>;
    type Link = ipfs::Cid;
}

impl Semigroup for Key {
    fn combine(&mut self, rhs: &Key) {
        self.0 |= rhs.0;
    }
}

impl Arbitrary for Key {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        Self(Arbitrary::arbitrary(g))
    }
}

async fn create_test_tree(
    xs: impl IntoIterator<Item = (Key, u64)>,
) -> anyhow::Result<Tree<TT, u64>> {
    let store = Arc::new(MemStore::new());
    let forest = Arc::new(Forest::<TT>::new(store, Config::debug()));
    let mut tree = Tree::<TT, u64>::empty(forest);
    tree.extend(xs).await?;
    tree.assert_invariants().await?;
    Ok(tree)
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
        let tree = create_test_tree(xs.clone()).await?;
        let actual = tree
            .stream()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(actual == xs)
    })
    .await
}

async fn compare_filtered(xs: Vec<(Key, u64)>, range: Range<u64>) -> anyhow::Result<bool> {
    let range = range.clone();
    let tree = create_test_tree(xs.clone()).await?;
    let actual = tree
        .stream_filtered(&OffsetRangeQuery::from(range.clone()))
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

#[quickcheck_async::tokio]
async fn build_stream_filtered(xs: Vec<(Key, u64)>, range: Range<u64>) -> quickcheck::TestResult {
    test(|| compare_filtered(xs.clone(), range.clone())).await
}

#[quickcheck_async::tokio]
async fn build_get(xs: Vec<(Key, u64)>) -> quickcheck::TestResult {
    test(|| async {
        let tree = create_test_tree(xs.clone()).await?;
        let mut actual = Vec::new();
        for i in 0..xs.len() as u64 {
            actual.push(tree.get(i).await?.unwrap());
        }
        Ok(actual == xs)
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
    let store = Arc::new(MemStore::new());
    let forest = Arc::new(Forest::<TT>::new(store, Config::debug()));
    let mut trees = Vec::new();
    for n in 1..=10u64 {
        let mut tree = Tree::<TT, u64>::empty(forest.clone());
        tree.extend((0..n).map(|t| (Key(t), n))).await?;
        tree.assert_invariants().await?;
        trees.push(tree.root().unwrap());
    }
    println!("{:?}", trees);
    let res = banyan::stream::SourceStream(forest, AllQuery)
        .query::<u64>(stream::iter(trees).boxed_local());
    let res = res.collect::<Vec<_>>().await;
    println!("{:?}", res);
    Ok(())
}
