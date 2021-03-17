use banyan::{
    index::{BranchIndex, Index, LeafIndex},
    memstore::MemStore,
    query::{AllQuery, OffsetRangeQuery},
    tree::Tree,
};
use futures::prelude::*;
use libipld::{cbor::DagCborCodec, codec::Codec, Cid};
use std::{convert::TryInto, iter, ops::Range, str::FromStr};

use common::{create_test_tree, test, txn, Key, KeySeq, Sha256Digest, TT};

mod common;

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
/// checks that stream_filtered_chunked returns the same elements as stream_filtered_chunked_reverse
async fn compare_filtered_chunked_with_reverse(
    xs: Vec<(Key, u64)>,
    range: Range<u64>,
) -> anyhow::Result<bool> {
    let range = range.clone();
    let (tree, txn) = create_test_tree(xs.clone()).await?;
    let reverse = txn
        .stream_filtered_chunked_reverse(&tree, OffsetRangeQuery::from(range.clone()), &|_| ())
        .map(|chunk_result| chunk_result.map(|chunk| stream::iter(chunk.data.into_iter().map(Ok))))
        .try_flatten()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .rev()
        .collect::<anyhow::Result<Vec<_>>>()?;

    let forward = txn
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
    Ok(reverse == forward && forward == expected)
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
async fn build_stream_filtered_chunked_forward_and_reverse(
    xs: Vec<(Key, u64)>,
    range: Range<u64>,
) -> quickcheck::TestResult {
    test(|| compare_filtered_chunked_with_reverse(xs.clone(), range.clone())).await
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
        let forest = txn(store, 1000);
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
        let forest = txn(store, 1000);
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
    let forest = txn(store, 1000);
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

// parses hex from cbor.me format
fn from_cbor_me(text: &str) -> anyhow::Result<Vec<u8>> {
    let parts = text
        .split('\n')
        .filter_map(|x| x.split('#').next())
        .flat_map(|x| x.split_whitespace())
        .collect::<String>();
    Ok(hex::decode(parts)?)
}

#[test]
fn leaf_index_wire_format() -> anyhow::Result<()> {
    let index: Index<TT> = Index::Leaf(LeafIndex {
        sealed: true,
        value_bytes: 1234,
        keys: KeySeq(vec![Key(1), Key(2)]),
        link: Some(
            Cid::from_str("bafyreihtx752fmf3zafbys5dtr4jxohb53yi3qtzfzf6wd5274jwtn5agu")?
                .try_into()?,
        ),
    });
    let serialized = DagCborCodec.encode(&index)?;
    let expected = from_cbor_me(
        r#"
A4                                      # map(4)
   64                                   # text(4)
      6B657973                          # "keys"
   81                                   # array(1)
      82                                # array(2)
         81                             # array(1)
            01                          # unsigned(1)
         81                             # array(1)
            02                          # unsigned(2)
   64                                   # text(4)
      6C696E6B                          # "link"
   D8 2A                                # tag(42)
      58 25                             # bytes(37)
         0001711220F3BFFBA2B0BBC80A1C4BA39C789BB8E1EEF08DC2792E4BEB0FBAFF1369B7A035 # "\x00\x01q\x12 \xF3\xBF\xFB\xA2\xB0\xBB\xC8\n\x1CK\xA3\x9Cx\x9B\xB8\xE1\xEE\xF0\x8D\xC2y.K\xEB\x0F\xBA\xFF\x13i\xB7\xA05"
   66                                   # text(6)
      7365616C6564                      # "sealed"
   F5                                   # primitive(21)
   6B                                   # text(11)
      76616C75655F6279746573            # "value_bytes"
   19 04D2                              # unsigned(1234)
"#,
    )?;
    // println!("{}", hex::encode(&serialized));
    assert_eq!(serialized, expected);
    Ok(())
}

#[test]
fn branch_index_wire_format() -> anyhow::Result<()> {
    let index: Index<TT> = Index::Branch(BranchIndex {
        count: 36784,
        level: 3,
        sealed: true,
        key_bytes: 67834,
        value_bytes: 123478912,
        summaries: KeySeq(vec![Key(1), Key(2)]),
        link: Some(
            Cid::from_str("bafyreihtx752fmf3zafbys5dtr4jxohb53yi3qtzfzf6wd5274jwtn5agu")?
                .try_into()?,
        ),
    });
    let serialized = DagCborCodec.encode(&index)?;
    let expected = from_cbor_me(
        r#"
A7                                      # map(7)
   65                                   # text(5)
      636F756E74                        # "count"
   19 8FB0                              # unsigned(36784)
   69                                   # text(9)
      6B65795F6279746573                # "key_bytes"
   1A 000108FA                          # unsigned(67834)
   65                                   # text(5)
      6C6576656C                        # "level"
   03                                   # unsigned(3)
   64                                   # text(4)
      6C696E6B                          # "link"
   D8 2A                                # tag(42)
      58 25                             # bytes(37)
         0001711220F3BFFBA2B0BBC80A1C4BA39C789BB8E1EEF08DC2792E4BEB0FBAFF1369B7A035 # "\x00\x01q\x12 \xF3\xBF\xFB\xA2\xB0\xBB\xC8\n\x1CK\xA3\x9Cx\x9B\xB8\xE1\xEE\xF0\x8D\xC2y.K\xEB\x0F\xBA\xFF\x13i\xB7\xA05"
   66                                   # text(6)
      7365616C6564                      # "sealed"
   F5                                   # primitive(21)
   69                                   # text(9)
      73756D6D6172696573                # "summaries"
   81                                   # array(1)
      82                                # array(2)
         81                             # array(1)
            01                          # unsigned(1)
         81                             # array(1)
            02                          # unsigned(2)
   6B                                   # text(11)
      76616C75655F6279746573            # "value_bytes"
   1A 075C2380                          # unsigned(123478912)

"#,
    )?;
    assert_eq!(serialized, expected);
    Ok(())
}
