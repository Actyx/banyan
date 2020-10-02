use anyhow::bail;
use banyan::index::CompactSeq;
use banyan::{
    forest::{Config, Transaction, TreeTypes},
    query::{AllQuery, OffsetRangeQuery},
    tree::Tree,
};
use futures::prelude::*;
use quickcheck::{Arbitrary, Gen, TestResult};
use serde::{Deserialize, Serialize};
use std::{io, iter, iter::FromIterator, ops::Range, sync::Arc};
use store::{MemStore, Sha256Digest};
mod store;

#[derive(Debug)]
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

fn get_bytes(data: serde_cbor::Value) -> anyhow::Result<Vec<u8>> {
    if let serde_cbor::Value::Bytes(data) = data {
        Ok(data)
    } else {
        bail!("expected CBOR bytes!");
    }
}

impl TreeTypes for TT {
    type Key = Key;
    type Seq = KeySeq;
    type Link = Sha256Digest;

    fn serialize_branch(
        links: &[&Self::Link],
        data: Vec<u8>,
        w: impl io::Write,
    ) -> anyhow::Result<()> {
        let links = links
            .iter()
            .map(|x| serde_cbor::Value::Bytes(x.as_ref().to_vec()))
            .collect::<Vec<_>>();
        serde_cbor::to_writer(w, &(links, serde_cbor::Value::Bytes(data)))
            .map_err(|e| anyhow::Error::new(e))
    }
    fn deserialize_branch(reader: impl io::Read) -> anyhow::Result<(Vec<Self::Link>, Vec<u8>)> {
        let (links, data): (Vec<serde_cbor::Value>, serde_cbor::Value) =
            serde_cbor::from_reader(reader)?;
        let data = get_bytes(data)?;
        let links = links
            .into_iter()
            .map(get_bytes)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let links = links
            .into_iter()
            .map(|x| Sha256Digest::read(&x))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok((links, data))
    }
}

impl Key {
    fn combine(&mut self, rhs: &Key) {
        self.0 |= rhs.0
    }
}

impl Arbitrary for Key {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        Self(Arbitrary::arbitrary(g))
    }
}

async fn create_test_tree<I>(xs: I) -> anyhow::Result<Tree<TT, u64>>
where
    I: IntoIterator<Item = (Key, u64)>,
    I::IntoIter: Send,
{
    let store = Arc::new(MemStore::new());
    let forest = Arc::new(Transaction::<TT, u64>::new(
        store.clone(),
        store,
        Config::debug(),
        Default::default(),
    ));
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
            .stream_filtered(AllQuery)
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
    let tree = create_test_tree(xs.clone()).await?;
    let actual = tree
        .stream_filtered(OffsetRangeQuery::from(range.clone()))
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
    let tree = create_test_tree(xs.clone()).await?;
    let actual = tree
        .stream_filtered_chunked(OffsetRangeQuery::from(range.clone()), &|_| ())
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
    let tree = create_test_tree(xs.clone()).await?;
    let actual = tree
        .stream_filtered_chunked_reverse(OffsetRangeQuery::from(range.clone()), &|_| ())
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
    let tree = create_test_tree(xs.clone()).await?;
    let chunks = tree
        .stream_filtered_chunked(OffsetRangeQuery::from(range.clone()), &|_| ())
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
        let tree = create_test_tree(xs.clone()).await?;
        let mut actual = Vec::new();
        for i in 0..xs.len() as u64 {
            actual.push(tree.get(i).await?.unwrap());
        }
        Ok(actual == xs)
    })
    .await
}

#[quickcheck_async::tokio]
async fn build_pack(xss: Vec<Vec<(Key, u64)>>) -> quickcheck::TestResult {
    test(|| async {
        let store = Arc::new(MemStore::new());
        let forest = Arc::new(Transaction::<TT, u64>::new(
            store.clone(),
            store,
            Config::debug(),
            Default::default(),
        ));
        let mut tree = Tree::<TT, u64>::empty(forest);
        // flattened xss for reference
        let xs = xss.iter().cloned().flatten().collect::<Vec<_>>();
        // build complex unbalanced tree
        for xs in xss.iter() {
            tree.extend_unpacked(xs.clone()).await.unwrap();
        }
        // check that the unbalanced tree itself matches the elements
        let actual: Vec<_> = tree.collect().await?;
        let unpacked_matches = xs == actual;

        tree.pack().await?;
        assert!(tree.is_packed().await?);
        let actual: Vec<_> = tree.collect().await?;
        let packed_matches = xs == actual;

        Ok(unpacked_matches && packed_matches)
    })
    .await
}

#[quickcheck_async::tokio]
async fn retain(xss: Vec<Vec<(Key, u64)>>) -> quickcheck::TestResult {
    test(|| async {
        let store = Arc::new(MemStore::new());
        let forest = Arc::new(Transaction::<TT, u64>::new(
            store.clone(),
            store,
            Config::debug(),
            Default::default(),
        ));
        let mut tree = Tree::<TT, u64>::empty(forest);
        // flattened xss for reference
        let xs = xss.iter().cloned().flatten().collect::<Vec<_>>();
        // build complex unbalanced tree
        for xs in xss.iter() {
            tree.extend_unpacked(xs.clone()).await.unwrap();
        }
        tree.retain(&OffsetRangeQuery::from(xs.len() as u64..))
            .await?;
        tree.assert_invariants().await?;
        tree.pack().await?;
        tree.retain(&OffsetRangeQuery::from(xs.len() as u64..))
            .await?;
        tree.assert_invariants().await?;
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
    let store = Arc::new(MemStore::new());
    let forest = Arc::new(Transaction::<TT, u64>::new(
        store.clone(),
        store,
        Config::debug(),
        Default::default(),
    ));
    let mut trees = Vec::new();
    for n in 1..=10u64 {
        let mut tree = Tree::<TT, u64>::empty(forest.clone());
        tree.extend((0..n).map(|t| (Key(t), n))).await?;
        tree.assert_invariants().await?;
        trees.push(tree.root().cloned().unwrap());
    }
    println!("{:?}", trees);
    let res = forest
        .read()
        .clone()
        .stream_roots(AllQuery, stream::iter(trees).boxed());
    let res = res.collect::<Vec<_>>().await;
    println!("{:?}", res);
    Ok(())
}

fn build(items: &mut Vec<u32>) {
    const MAX_BRANCH: usize = 8;
    if items.len() <= 1 {
        // nothing we can do
        return;
    } else {
        let pos = items
            .iter()
            .position(|x| *x != items[0])
            .unwrap_or(items.len());
        if pos >= MAX_BRANCH || pos == items.len() || pos == items.len() - 1 {
            // a valid node can be built from the start
            items.splice(0..MAX_BRANCH.min(items.len()), vec![items[0] + 1]);
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
    v.sort();
    v.reverse();
    while v.len() > 1 {
        build(&mut v);
    }
    true
}
