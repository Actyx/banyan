use banyan::{
    index::{BranchIndex, Index, LeafIndex, VecSeq},
    query::{AllQuery, EmptyQuery, OffsetRangeQuery},
    store::{BranchCache, MemStore},
    Config, Forest, Secrets, StreamBuilder, Tree,
};
use common::{txn, IterExt, Key, KeyRange, KeySeq, Sha256Digest, TestFilter, TestTree, TT};
use futures::prelude::*;
use libipld::{cbor::DagCborCodec, codec::Codec, Cid};
use quickcheck::TestResult;
use quickcheck_macros::quickcheck;
use std::{convert::TryInto, iter, str::FromStr};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use crate::common::no_offset_overlap;

mod common;

#[quickcheck]
fn build_stream(t: TestTree) -> anyhow::Result<bool> {
    let (tree, txn, xs) = t.tree()?;
    let actual = txn
        .iter_filtered(&tree, AllQuery)
        .map(|res| res.map(|(_, k, v)| (k, v)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(actual == xs)
}

/// checks that stream_filtered returns the same elements as filtering each element manually
fn compare_filtered(t: TestTree, filter: TestFilter) -> anyhow::Result<bool> {
    let (tree, txn, xs) = t.tree()?;
    let actual = txn
        .iter_filtered(&tree, filter.query())
        .collect::<anyhow::Result<Vec<_>>>()?;
    let expected = xs
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, (k, v))| (i as u64, k, v))
        .filter(|triple| filter.contains(triple))
        .collect::<Vec<_>>();
    if actual != expected {
        println!("{:?} {:?}", actual, expected);
    }
    Ok(actual == expected)
}

/// checks that stream_filtered_chunked returns the same elements as filtering each element manually
fn compare_filtered_chunked(t: TestTree, filter: TestFilter) -> anyhow::Result<bool> {
    let (tree, txn, xs) = t.tree()?;
    let actual = txn
        .iter_filtered_chunked(&tree, filter.query(), &|_| ())
        .flat_map(|chunk_result| match chunk_result {
            Ok(chunk) => chunk.data.into_iter().map(Ok).boxed(),
            Err(cause) => iter::once(Err(cause)).boxed(),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let expected = xs
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, (k, v))| (i as u64, k, v))
        .filter(|triple| filter.contains(triple))
        .collect::<Vec<_>>();
    Ok(actual == expected)
}
/// checks that stream_filtered_chunked returns the same elements as stream_filtered_chunked_reverse
fn compare_filtered_chunked_with_reverse(t: TestTree, filter: TestFilter) -> anyhow::Result<bool> {
    let (tree, txn, xs) = t.tree()?;
    let mut reverse = txn
        .iter_filtered_chunked_reverse(&tree, filter.query(), &|_| ())
        .flat_map(|chunk_result| match chunk_result {
            Ok(chunk) => chunk.data.into_iter().rev().map(Ok).boxed(),
            Err(cause) => iter::once(Err(cause)).boxed(),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    reverse.reverse();

    let forward = txn
        .iter_filtered_chunked(&tree, filter.query(), &|_| ())
        .flat_map(|chunk_result| match chunk_result {
            Ok(chunk) => chunk.data.into_iter().map(Ok).boxed(),
            Err(cause) => iter::once(Err(cause)).boxed(),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let expected = xs
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, (k, v))| (i as u64, k, v))
        .filter(|triple| filter.contains(triple))
        .collect::<Vec<_>>();
    Ok(reverse == forward && forward == expected)
}

/// checks that stream_filtered_chunked returns the same elements as filtering each element manually
fn compare_filtered_chunked_reverse(t: TestTree, filter: TestFilter) -> anyhow::Result<bool> {
    let (tree, txn, xs) = t.tree()?;
    let actual = txn
        .iter_filtered_chunked_reverse(&tree, filter.query(), &|_| ())
        .flat_map(|chunk_result| match chunk_result {
            Ok(chunk) => chunk.data.into_iter().rev().map(Ok).boxed(),
            Err(cause) => iter::once(Err(cause)).boxed(),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let expected = xs
        .iter()
        .cloned()
        .enumerate()
        .rev()
        .map(|(i, (k, v))| (i as u64, k, v))
        .filter(|triple| filter.contains(triple))
        .collect::<Vec<_>>();
    if actual != expected {
        println!("{:?} {:?}", actual, expected);
    }
    Ok(actual == expected)
}

/// checks that stream_filtered_chunked returns the same elements as filtering each element manually
fn filtered_chunked_no_holes(t: TestTree, filter: TestFilter) -> anyhow::Result<bool> {
    let (tree, txn, xs) = t.tree()?;
    let chunks = txn
        .iter_filtered_chunked(&tree, filter.query(), &|_| ())
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

/// checks that stream_filtered_chunked returns the same elements as filtering each element manually
fn filtered_chunked_reverse_no_holes(t: TestTree, filter: TestFilter) -> anyhow::Result<bool> {
    let (tree, txn, _xs) = t.tree()?;
    let chunks = txn
        .iter_filtered_chunked_reverse(&tree, filter.query(), &|_| ())
        .collect::<anyhow::Result<Vec<_>>>()?;
    let min_offset = chunks.iter().fold(tree.count(), |offset, chunk| {
        if offset == chunk.range.end {
            chunk.range.start
        } else {
            offset
        }
    });
    Ok(min_offset == 0)
}

fn iter_index(t: TestTree) -> anyhow::Result<bool> {
    let (tree, txn, xs) = t.tree()?;
    let len = xs.len() as u64;
    let actual = txn
        .iter_index(&tree, AllQuery)
        .collect::<anyhow::Result<Vec<_>>>()?;

    // should visit all indices
    let cnt = actual.iter().fold(0, |acc, idx| {
        if let Index::Leaf(l) = idx {
            acc + l.keys().count() as u64
        } else {
            acc
        }
    });
    Ok(cnt == len)
}

#[quickcheck]
fn build_iter_index(t: TestTree) -> anyhow::Result<bool> {
    iter_index(t)
}

#[quickcheck]
fn build_stream_filtered(t: TestTree, filter: TestFilter) -> anyhow::Result<bool> {
    compare_filtered(t, filter)
}

#[quickcheck]
fn build_stream_filtered_chunked(t: TestTree, filter: TestFilter) -> anyhow::Result<bool> {
    compare_filtered_chunked(t, filter)
}

#[quickcheck]
fn build_stream_filtered_chunked_forward_and_reverse(
    t: TestTree,
    filter: TestFilter,
) -> anyhow::Result<bool> {
    compare_filtered_chunked_with_reverse(t, filter)
}

#[quickcheck]
fn build_stream_filtered_chunked_reverse(t: TestTree, filter: TestFilter) -> anyhow::Result<bool> {
    compare_filtered_chunked_reverse(t, filter)
}

#[quickcheck]
fn build_stream_filtered_chunked_no_holes(t: TestTree, filter: TestFilter) -> anyhow::Result<bool> {
    filtered_chunked_no_holes(t, filter)
}

#[quickcheck]
fn build_stream_filtered_chunked_reverse_no_holes(
    t: TestTree,
    filter: TestFilter,
) -> anyhow::Result<bool> {
    filtered_chunked_reverse_no_holes(t, filter)
}

#[quickcheck]
fn build_get(t: TestTree) -> anyhow::Result<bool> {
    let (tree, txn, xs) = t.tree()?;
    let mut actual = Vec::new();
    for i in 0..xs.len() as u64 {
        actual.push(txn.get(&tree, i)?.unwrap());
    }
    Ok(actual == xs)
}

fn do_build_pack(xss: Vec<Vec<(Key, u64)>>) -> anyhow::Result<bool> {
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let mut forest = txn(store, 1000);
    let mut builder = StreamBuilder::<TT, u64>::debug();

    // flattened xss for reference
    let xs = xss.iter().flatten().cloned().collect::<Vec<_>>();
    // build complex unbalanced tree
    for xs in xss.iter() {
        forest.extend_unpacked(&mut builder, xs.clone()).unwrap();
    }
    let unpacked_tree = builder.snapshot();
    // check that the unbalanced tree itself matches the elements
    let actual: Vec<_> = forest
        .collect(&unpacked_tree)?
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .unwrap();
    let unpacked_matches = xs == actual;

    forest.pack(&mut builder)?;
    let packed_tree = builder.snapshot();
    assert!(forest.is_packed(&packed_tree)?);
    let actual: Vec<_> = forest
        .collect(&packed_tree)?
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .unwrap();
    let packed_matches = xs == actual;
    let offsets_ok = no_offset_overlap(&forest, &[unpacked_tree, packed_tree])?;
    Ok(unpacked_matches && packed_matches && offsets_ok)
}

#[quickcheck]
fn build_pack(xss: Vec<Vec<(Key, u64)>>) -> anyhow::Result<bool> {
    do_build_pack(xss)
}

#[test]
fn build_pack_1() {
    let xss = vec![vec![(Key(8702892647260624503), 0)]];
    assert!(do_build_pack(xss).unwrap());
}

#[test]
fn build_pack_2() {
    try_init_logging();
    let xss = (0..200)
        .map(|i| (0..200).map(|j| (Key(i), j)).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert!(do_build_pack(xss).unwrap());
}

fn do_retain(t: TestTree) -> anyhow::Result<bool> {
    let (mut builder, mut txn, xs) = t.builder()?;
    let tree0 = builder.snapshot();
    txn.retain(&mut builder, &OffsetRangeQuery::from(xs.len() as u64..))?;
    let tree1 = builder.snapshot();
    txn.assert_invariants(&builder)?;
    txn.pack(&mut builder)?;
    let tree2 = builder.snapshot();
    txn.retain(&mut builder, &OffsetRangeQuery::from(xs.len() as u64..))?;
    txn.assert_invariants(&builder)?;
    let tree3 = builder.snapshot();
    let offsets_ok = no_offset_overlap(&txn, &[tree0, tree1, tree2, tree3])?;
    Ok(offsets_ok)
}

#[quickcheck]
fn retain(t: TestTree) -> anyhow::Result<bool> {
    do_retain(t)
}

#[quickcheck]
fn iter_from_should_return_all_items(t: TestTree) -> anyhow::Result<bool> {
    let (builder, txn, xs) = t.builder()?;
    let actual = txn
        .iter_from(&builder.snapshot())
        .collect::<anyhow::Result<Vec<_>>>()?;
    let expected = xs
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, (k, v))| (i as u64, k, v))
        .collect::<Vec<_>>();
    if expected != actual {
        println!("{:?} {:?}", expected, actual);
    }
    Ok(expected == actual)
}

#[test]
fn filter_test_simple() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let ok = compare_filtered(
        TestTree::packed(vec![(Key(1), 1), (Key(2), 2)]),
        TestFilter::offset_range(0..1),
    )?;
    assert!(ok);
    Ok(())
}

#[test]
fn filter_test_simple_2() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let ok = compare_filtered(
        TestTree::unpacked(vec![vec![(Key(1), 1)], vec![(Key(2), 2)]]),
        TestFilter::offset_range(0..1),
    )?;
    assert!(ok);
    Ok(())
}

#[test]
fn transaction_1() -> anyhow::Result<()> {
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let mut forest = txn(store, 1000);
    let mut builder = StreamBuilder::<TT, u64>::debug();
    forest.extend(&mut builder, vec![(Key(1), 1)])?;
    // transaction that is dropped without committing
    let mut txn = builder.transaction();
    forest.extend(&mut txn, vec![(Key(1), 1)])?;
    assert_eq!(txn.count(), 2);
    drop(txn);
    assert_eq!(builder.count(), 1);

    // transaction that is dropped after committing
    let mut txn = builder.transaction();
    forest.extend(&mut txn, vec![(Key(2), 2)])?;
    assert_eq!(txn.count(), 2);
    txn.commit();
    assert_eq!(builder.count(), 2);
    Ok(())
}

#[tokio::test]
async fn stream_test_simple() -> anyhow::Result<()> {
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let mut forest = txn(store, 1000);
    let mut trees = Vec::new();
    for n in 1..=10u64 {
        let mut builder = StreamBuilder::<TT, u64>::debug();
        forest.extend(&mut builder, (0..n).map(|t| (Key(t), n)))?;
        forest.assert_invariants(&builder)?;
        trees.push(builder.snapshot());
    }
    // println!("{:?}", trees);
    let res = forest
        .read()
        .stream_trees(AllQuery, stream::iter(trees).boxed());
    let _res = res.collect::<Vec<_>>().await;
    // println!("{:?}", res);
    Ok(())
}

#[quickcheck_async::tokio]
async fn stream_trees_chunked_should_honour_an_inclusive_upper_bound(
    t: TestTree,
) -> anyhow::Result<TestResult> {
    let (builder, forest, xs) = t.builder()?;
    let len = xs.len() as u64;
    if len == 0 {
        return Ok(TestResult::discard());
    }
    let trees = stream::once(async move { builder.snapshot() }).chain(stream::pending());

    let actual = forest
        .stream_trees_chunked(AllQuery, trees, 0u64..=(len - 1), &|_| ())
        .map_ok(move |chunk| stream::iter(chunk.data))
        .take_while(|x| future::ready(x.is_ok()))
        .filter_map(|x| future::ready(x.ok()))
        .flatten()
        .collect::<Vec<_>>()
        .await;

    let expected = xs
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, (k, v))| (i as u64, k, v))
        .collect::<Vec<_>>();
    if expected != actual {
        println!("{:?} {:?}", expected, actual);
    }
    Ok(TestResult::from_bool(expected == actual))
}

#[tokio::test]
async fn stream_trees_chunked_reverse_should_honour_an_inclusive_upper_bound_1(
) -> anyhow::Result<()> {
    let t = TestTree::unpacked(vec![vec![(Key(0), 0)], vec![(Key(1), 1)]]);
    let res = do_stream_trees_chunked_reverse_should_honour_an_inclusive_upper_bound(t).await?;
    assert!(!res.is_failure());
    Ok(())
}

#[quickcheck_async::tokio]
async fn stream_trees_chunked_reverse_should_honour_an_inclusive_upper_bound(
    t: TestTree,
) -> anyhow::Result<TestResult> {
    do_stream_trees_chunked_reverse_should_honour_an_inclusive_upper_bound(t).await
}

async fn do_stream_trees_chunked_reverse_should_honour_an_inclusive_upper_bound(
    t: TestTree,
) -> anyhow::Result<TestResult> {
    let (builder, forest, xs) = t.builder()?;
    let len = xs.len() as u64;
    if len == 0 {
        return Ok(TestResult::discard());
    }
    let trees = stream::once(async move { builder.snapshot() }).chain(stream::pending());
    let actual = forest
        .stream_trees_chunked_reverse(AllQuery, trees, 0u64..=(len - 1), &|_| ())
        .map_ok(move |chunk| {
            // reverse the chunk
            stream::iter(chunk.data.into_iter().rev())
        })
        .take_while(|x| future::ready(x.is_ok()))
        .filter_map(|x| future::ready(x.ok()))
        .flatten()
        .collect::<Vec<_>>()
        .await;

    let expected = xs
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, (k, v))| (i as u64, k, v))
        .rev()
        .collect::<Vec<_>>();

    if expected != actual {
        println!("expected {:?} actual {:?}", expected, actual);
    }
    Ok(TestResult::from_bool(expected == actual))
}

#[tokio::test]
async fn stream_trees_chunked_reverse_should_complete() {
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let mut forest = txn(store, 1000);
    let secrets = Secrets::default();
    let config = Config::debug();
    let mut builder = StreamBuilder::<TT, u64>::new(config, secrets);
    forest
        .extend_unpacked(&mut builder, vec![(Key(0), 0)])
        .unwrap();
    let trees = stream::once(async move { builder.snapshot() }).chain(stream::pending());
    let _ = forest
        .stream_trees_chunked_reverse(EmptyQuery, trees, 0u64..=0, &|_| ())
        .map_ok(move |chunk| stream::iter(chunk.data))
        .take_while(|x| future::ready(x.is_ok()))
        .filter_map(|x| future::ready(x.ok()))
        .flatten()
        .collect::<Vec<_>>()
        .await;
}

#[tokio::test]
async fn stream_trees_chunked_should_complete() {
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let mut forest = txn(store, 1000);
    let secrets = Secrets::default();
    let config = Config::debug();
    let mut builder = StreamBuilder::<TT, u64>::new(config, secrets);
    forest
        .extend_unpacked(&mut builder, vec![(Key(0), 0)])
        .unwrap();
    let trees = stream::once(async move { builder.snapshot() }).chain(stream::pending());
    let _ = forest
        .stream_trees_chunked(EmptyQuery, trees, 0u64..=0, &|_| ())
        .map_ok(move |chunk| stream::iter(chunk.data))
        .take_while(|x| future::ready(x.is_ok()))
        .filter_map(|x| future::ready(x.ok()))
        .flatten()
        .collect::<Vec<_>>()
        .await;
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
fn deep_tree_traversal_no_stack_overflow() -> anyhow::Result<()> {
    // traverse a tree on a thread with a tiny stack
    // this would fail with recursive traversal
    let handle = std::thread::Builder::new()
        .name("stack-overflow-test".into())
        .stack_size(65536)
        .spawn(|| {
            let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
            let mut forest = txn(store, 1000);
            let mut builder = StreamBuilder::<TT, u64>::debug();
            let elems = (0u64..100).map(|i| (i, Key(i), i)).collect::<Vec<_>>();
            for (_offset, k, v) in &elems {
                forest
                    .extend_unpacked(&mut builder, vec![(*k, *v)])
                    .unwrap();
            }
            let elems1 = forest
                .iter_filtered(&builder.snapshot(), AllQuery)
                .collect::<anyhow::Result<Vec<_>>>()
                .unwrap();
            let mut elems2 = forest
                .iter_filtered_reverse(&builder.snapshot(), AllQuery)
                .collect::<anyhow::Result<Vec<_>>>()
                .unwrap();
            elems2.reverse();
            assert_eq!(elems, elems1);
            assert_eq!(elems, elems2);
        })?;
    handle.join().unwrap();
    Ok(())
}

#[test]
fn leaf_index_wire_format() -> anyhow::Result<()> {
    let index: Index<TT> = LeafIndex {
        sealed: true,
        value_bytes: 1234,
        keys: KeySeq(vec![Key(1), Key(2)]),
        link: Some(
            Cid::from_str("bafyreihtx752fmf3zafbys5dtr4jxohb53yi3qtzfzf6wd5274jwtn5agu")?
                .try_into()?,
        ),
    }
    .into();
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
    let index: Index<TT> = BranchIndex {
        count: 36784,
        level: 3,
        sealed: true,
        key_bytes: 67834,
        value_bytes: 123478912,
        summaries: vec![KeyRange(0, 1), KeyRange(1, 2)]
            .into_iter()
            .collect::<VecSeq<_>>(),
        link: Some(
            Cid::from_str("bafyreihtx752fmf3zafbys5dtr4jxohb53yi3qtzfzf6wd5274jwtn5agu")?
                .try_into()?,
        ),
    }
    .into();
    let serialized = DagCborCodec.encode(&index)?;
    let expected = from_cbor_me(
        r#"

A7                                   # map(7)
64                                   # text(4)
   6C696E6B                          # "link"
D8 2A                                # tag(42)
   58 25                             # bytes(37)
      0001711220F3BFFBA2B0BBC80A1C4BA39C789BB8E1EEF08DC2792E4BEB0FBAFF1369B7A035 # "\x00\x01q\x12 \xF3\xBF\xFB\xA2\xB0\xBB\xC8\n\x1CK\xA3\x9Cx\x9B\xB8\xE1\xEE\xF0\x8D\xC2y.K\xEB\x0F\xBA\xFF\x13i\xB7\xA05"
65                                   # text(5)
   636F756E74                        # "count"
19 8FB0                              # unsigned(36784)
65                                   # text(5)
   6C6576656C                        # "level"
03                                   # unsigned(3)
66                                   # text(6)
   7365616C6564                      # "sealed"
F5                                   # primitive(21)
69                                   # text(9)
   6B65795F6279746573                # "key_bytes"
1A 000108FA                          # unsigned(67834)
69                                   # text(9)
   73756D6D6172696573                # "summaries"
81                                   # array(1)
   82                                # array(2)
      82                             # array(2)
         00                          # unsigned(0)
         01                          # unsigned(1)
      82                             # array(2)
         01                          # unsigned(1)
         02                          # unsigned(2)
6B                                   # text(11)
   76616C75655F6279746573            # "value_bytes"
1A 075C2380                          # unsigned(123478912
"#,
    )?;
    println!("{}", hex::encode(&serialized));
    assert_eq!(serialized, expected);

    // check that the old format (which didn’t sort by map key length) is still accepted
    let decoded = DagCborCodec.decode::<Index<TT>>(&from_cbor_me(r#"

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
          82                             # array(2)
             00                          # unsigned(0)
             01                          # unsigned(1)
          82                             # array(2)
             01                          # unsigned(1)
             02                          # unsigned(2)
    6B                                   # text(11)
       76616C75655F6279746573            # "value_bytes"
    1A 075C2380                          # unsigned(123478912
    "#)?
    )?;
    assert_eq!(DagCborCodec.encode(&decoded)?, expected);

    Ok(())
}

#[test]
fn retain1() -> anyhow::Result<()> {
    let xs = (0..10).map(|i| (Key(i), i)).collect::<Vec<_>>();
    let ok = do_retain(TestTree::packed(xs))?;
    assert!(ok);
    Ok(())
}

type TreeFixture = (Forest<TT, MemStore<Sha256Digest>>, Vec<u64>, Tree<TT, u64>);
fn create_interesting_tree(n: usize) -> anyhow::Result<TreeFixture> {
    let config = Config {
        target_leaf_size: 10000,
        max_leaf_count: 10,
        max_key_branches: 4,
        max_summary_branches: 4,
        zstd_level: 10,
        max_uncompressed_leaf_size: 16 * 1024 * 1024,
    };
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let forest = Forest::new(store.clone(), BranchCache::new(1 << 20));
    let mut txn = txn(store, 1 << 20);
    let secrets = Secrets::default();
    let events: Vec<_> = (0..n)
        .into_iter()
        .map(|i| (Key(i as u64), i as u64))
        .collect();
    let payloads = events.iter().map(|(_, v)| *v).collect::<Vec<_>>();
    let mut builder = StreamBuilder::<TT, u64>::new(config, secrets);
    txn.extend(&mut builder, events)?;
    Ok((forest, payloads, builder.snapshot()))
}

/// Test all possible offset ranges for stream of trees, created from a simple tree
#[tokio::test]
async fn offset_range_test_stream() -> anyhow::Result<()> {
    let n = 100;
    let (forest, payloads, tree) = create_interesting_tree(n)?;
    let trees = forest.left_roots(&tree)?;
    let ranges = (0..n)
        .flat_map(|start| (start..n).map(move |end| (start, end)))
        .collect::<Vec<_>>();
    for (start, end) in ranges {
        let trees = stream::iter(trees.clone()).chain(stream::pending());
        let range = start as u64..=end as u64;
        let res = forest
            .stream_trees_chunked(AllQuery, trees, range, &|_| ())
            .map_ok(move |chunk| {
                stream::iter(
                    chunk
                        .data
                        .into_iter()
                        .map(move |(_, _, payload)| payload)
                        .collect::<Vec<_>>(),
                )
            })
            .take_while(|x| future::ready(x.is_ok()))
            .filter_map(|x| future::ready(x.ok()))
            .flatten()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(res.into_iter().collect::<Vec<_>>(), payloads[start..=end]);
    }

    Ok(())
}

/// Test all possible offset ranges for a single tree
#[test]
fn offset_range_test_simple() -> anyhow::Result<()> {
    let n = 100;
    let (forest, payloads, tree) = create_interesting_tree(n)?;
    let ranges = (0..n)
        .flat_map(|start| (start..n).map(move |end| (start, end)))
        .collect::<Vec<_>>();
    for (start, end) in ranges {
        let tree = tree.clone();
        let range = start as u64..=end as u64;
        let res = forest
            .iter_filtered_chunked(&tree, OffsetRangeQuery::from(range), &|_| ())
            .flat_map(move |item| {
                item.map(|chunk| {
                    chunk
                        .data
                        .into_iter()
                        .map(move |(_, _, payload)| payload)
                        .collect::<Vec<_>>()
                })
            })
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(res, payloads[start..=end]);
    }

    Ok(())
}

#[test]
fn retain2() -> anyhow::Result<()> {
    let xs = vec![
        vec![
            (Key(0), 0),
            (Key(1), 0),
            (Key(2), 0),
            (Key(3), 0),
            (Key(4), 0),
            (Key(5), 0),
            (Key(6), 0),
            (Key(7), 0),
            (Key(8), 0),
            (Key(9), 0),
        ],
        vec![
            (Key(0), 0),
            (Key(1), 0),
            (Key(2), 0),
            (Key(3), 0),
            (Key(4), 0),
            (Key(5), 0),
            (Key(6), 0),
            (Key(7), 0),
            (Key(8), 0),
            (Key(9), 0),
            (Key(10), 0),
            (Key(11), 0),
            (Key(12), 0),
            (Key(13), 0),
            (Key(14), 0),
            (Key(15), 0),
            (Key(16), 0),
            (Key(17), 0),
            (Key(18), 0),
            (Key(19), 0),
            (Key(20), 0),
            (Key(21), 0),
            (Key(22), 0),
            (Key(23), 0),
            (Key(24), 0),
            (Key(25), 0),
            (Key(26), 0),
            (Key(27), 0),
            (Key(28), 0),
            (Key(29), 0),
            (Key(30), 0),
            (Key(31), 0),
            (Key(32), 0),
            (Key(33), 0),
            (Key(34), 0),
            (Key(35), 0),
            (Key(36), 0),
            (Key(37), 0),
            (Key(38), 0),
            (Key(39), 0),
        ],
    ];
    let ok = do_retain(TestTree::unpacked(xs))?;
    assert!(ok);
    Ok(())
}

#[test]
fn build1() -> anyhow::Result<()> {
    let xs = (0..10).map(|i| (Key(i), i)).collect::<Vec<_>>();
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let mut forest = txn(store, 1000);
    let mut tree = StreamBuilder::debug();
    forest.extend(&mut tree, xs)?;
    forest.dump(&tree.snapshot())?;
    // let foo = Tree::empty()
    Ok(())
}

fn try_init_logging() {
    let builder = tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env());

    let _ = builder.try_init();
}
