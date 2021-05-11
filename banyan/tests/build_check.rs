use banyan::{forest::{Config, CryptoConfig}, index::{BranchIndex, Index, LeafIndex}, memstore::MemStore, query::{AllQuery, EmptyQuery, OffsetRangeQuery}, tree::{StreamBuilderState, Tree}};
use common::{create_test_tree, txn, IterExt, Key, KeySeq, Sha256Digest, TT};
use futures::prelude::*;
use libipld::{cbor::DagCborCodec, codec::Codec, Cid};
use quickcheck_macros::quickcheck;
use std::{convert::TryInto, iter, ops::Range, str::FromStr};

mod common;

#[quickcheck]
fn build_stream(xs: Vec<(Key, u64)>) -> anyhow::Result<bool> {
    let (tree, txn) = create_test_tree(xs.clone())?;
    let actual = txn
        .iter_filtered(&tree, AllQuery)
        .map(|res| res.map(|(_, k, v)| (k, v)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(actual == xs)
}

/// checks that stream_filtered returns the same elements as filtering each element manually
fn compare_filtered(xs: Vec<(Key, u64)>, range: Range<u64>) -> anyhow::Result<bool> {
    let (tree, txn) = create_test_tree(xs.clone())?;
    let actual = txn
        .iter_filtered(&tree, OffsetRangeQuery::from(range.clone()))
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
fn compare_filtered_chunked(xs: Vec<(Key, u64)>, range: Range<u64>) -> anyhow::Result<bool> {
    let (tree, txn) = create_test_tree(xs.clone())?;
    let actual = txn
        .iter_filtered_chunked(&tree, OffsetRangeQuery::from(range.clone()), &|_| ())
        .map(|chunk_result| match chunk_result {
            Ok(chunk) => chunk.data.into_iter().map(Ok).boxed(),
            Err(cause) => iter::once(Err(cause)).boxed(),
        })
        .flatten()
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
fn compare_filtered_chunked_with_reverse(
    xs: Vec<(Key, u64)>,
    range: Range<u64>,
) -> anyhow::Result<bool> {
    let (tree, txn) = create_test_tree(xs.clone())?;
    let mut reverse = txn
        .iter_filtered_chunked_reverse(&tree, OffsetRangeQuery::from(range.clone()), &|_| ())
        .map(|chunk_result| match chunk_result {
            Ok(chunk) => chunk.data.into_iter().rev().map(Ok).boxed(),
            Err(cause) => iter::once(Err(cause)).boxed(),
        })
        .flatten()
        .collect::<anyhow::Result<Vec<_>>>()?;
    reverse.reverse();

    let forward = txn
        .iter_filtered_chunked(&tree, OffsetRangeQuery::from(range.clone()), &|_| ())
        .map(|chunk_result| match chunk_result {
            Ok(chunk) => chunk.data.into_iter().map(Ok).boxed(),
            Err(cause) => iter::once(Err(cause)).boxed(),
        })
        .flatten()
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
fn compare_filtered_chunked_reverse(
    xs: Vec<(Key, u64)>,
    range: Range<u64>,
) -> anyhow::Result<bool> {
    let (tree, txn) = create_test_tree(xs.clone())?;
    let actual = txn
        .iter_filtered_chunked_reverse(&tree, OffsetRangeQuery::from(range.clone()), &|_| ())
        .map(|chunk_result| match chunk_result {
            Ok(chunk) => chunk.data.into_iter().rev().map(Ok).boxed(),
            Err(cause) => iter::once(Err(cause)).boxed(),
        })
        .flatten()
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
fn filtered_chunked_no_holes(xs: Vec<(Key, u64)>, range: Range<u64>) -> anyhow::Result<bool> {
    let (tree, txn) = create_test_tree(xs.clone())?;
    let chunks = txn
        .iter_filtered_chunked(&tree, OffsetRangeQuery::from(range), &|_| ())
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

fn iter_index(xs: Vec<(Key, u64)>) -> anyhow::Result<bool> {
    let len = xs.len() as u64;
    let (tree, txn) = create_test_tree(xs)?;
    let actual = txn
        .iter_index(&tree, AllQuery)
        .collect::<anyhow::Result<Vec<_>>>()?;

    // should visit all indices
    let cnt = actual.iter().fold(0, |acc, idx| {
        if let Index::Leaf(l) = idx.as_ref() {
            acc + l.keys().count() as u64
        } else {
            acc
        }
    });
    Ok(cnt == len)
}

#[quickcheck]
fn build_iter_index(xs: Vec<(Key, u64)>) -> anyhow::Result<bool> {
    iter_index(xs)
}

#[quickcheck]
fn build_stream_filtered(xs: Vec<(Key, u64)>, range: Range<u64>) -> anyhow::Result<bool> {
    compare_filtered(xs, range)
}

#[quickcheck]
fn build_stream_filtered_chunked(xs: Vec<(Key, u64)>, range: Range<u64>) -> anyhow::Result<bool> {
    compare_filtered_chunked(xs, range)
}

#[quickcheck]
fn build_stream_filtered_chunked_forward_and_reverse(
    xs: Vec<(Key, u64)>,
    range: Range<u64>,
) -> anyhow::Result<bool> {
    compare_filtered_chunked_with_reverse(xs, range)
}

#[quickcheck]
fn build_stream_filtered_chunked_reverse(
    xs: Vec<(Key, u64)>,
    range: Range<u64>,
) -> anyhow::Result<bool> {
    compare_filtered_chunked_reverse(xs, range)
}

#[quickcheck]
fn build_stream_filtered_chunked_no_holes(
    xs: Vec<(Key, u64)>,
    range: Range<u64>,
) -> anyhow::Result<bool> {
    filtered_chunked_no_holes(xs, range)
}

#[quickcheck]
fn build_get(xs: Vec<(Key, u64)>) -> anyhow::Result<bool> {
    let (tree, txn) = create_test_tree(xs.clone())?;
    let mut actual = Vec::new();
    for i in 0..xs.len() as u64 {
        actual.push(txn.get(&tree, i)?.unwrap());
    }
    Ok(actual == xs)
}

#[quickcheck]
fn build_pack(xss: Vec<Vec<(Key, u64)>>) -> anyhow::Result<bool> {
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let forest = txn(store, 1000);
    let mut tree = Tree::<TT>::debug();

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
}

fn do_retain(xss: Vec<Vec<(Key, u64)>>) -> anyhow::Result<bool> {
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let forest = txn(store, 1000);
    let mut tree = Tree::<TT>::debug();
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
}

#[quickcheck]
fn retain(xss: Vec<Vec<(Key, u64)>>) -> anyhow::Result<bool> {
    do_retain(xss)
}

#[quickcheck]
fn iter_from_should_return_all_items(xs: Vec<(Key, u64)>) -> anyhow::Result<bool> {
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let forest = txn(store, 1000);
    let mut tree = Tree::<TT>::debug();
    tree = forest.extend(&tree, xs.clone().into_iter())?;
    forest.assert_invariants(&tree)?;
    let actual = forest
        .iter_from(&tree)
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
    let ok = compare_filtered(vec![(Key(1), 1), (Key(2), 2)], 0..1)?;
    assert!(ok);
    Ok(())
}

#[tokio::test]
async fn stream_test_simple() -> anyhow::Result<()> {
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let forest = txn(store, 1000);
    let stream = StreamBuilderState::new(0, CryptoConfig::default(), Config::debug());
    let mut trees = Vec::new();
    for n in 1..=10u64 {
        let mut tree = Tree::<TT>::debug();
        tree = forest.extend(&tree, (0..n).map(|t| (Key(t), n)))?;
        forest.assert_invariants(&tree)?;
        trees.push(tree);
    }
    println!("{:?}", trees);
    let res = forest
        .read()
        .stream_trees(stream, AllQuery, stream::iter(trees).boxed());
    let res = res.collect::<Vec<_>>().await;
    println!("{:?}", res);
    Ok(())
}

#[tokio::test]
async fn stream_trees_chunked_reverse_should_complete() {
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let forest = txn(store, 1000);
    let stream = StreamBuilderState::new(0, CryptoConfig::default(), Config::debug());
    let mut tree = Tree::<TT>::empty(stream.config().clone(), stream.crypto_config().clone());
    tree = forest.extend_unpacked(&tree, vec![(Key(0), 0)]).unwrap();
    let trees = stream::once(async move { tree }).chain(stream::pending());
    let _ = forest
        .stream_trees_chunked_reverse(stream, EmptyQuery, trees, 0u64..=0, &|_| ())
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
    let forest = txn(store, 1000);
    let stream = StreamBuilderState::new(0, CryptoConfig::default(), Config::debug());
    let mut tree = Tree::<TT>::empty(stream.config().clone(), stream.crypto_config().clone());
    tree = forest.extend_unpacked(&tree, vec![(Key(0), 0)]).unwrap();
    let trees = stream::once(async move { tree }).chain(stream::pending());
    let _ = forest
        .stream_trees_chunked(stream, EmptyQuery, trees, 0u64..=0, &|_| ())
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
            let forest = txn(store, 1000);
            let mut tree = Tree::<TT>::debug();
            let elems = (0u64..100).map(|i| (i, Key(i), i)).collect::<Vec<_>>();
            for (_offset, k, v) in &elems {
                tree = forest.extend_unpacked(&tree, vec![(*k, *v)]).unwrap();
            }
            let elems1 = forest
                .iter_filtered(&tree, AllQuery)
                .collect::<anyhow::Result<Vec<_>>>()
                .unwrap();
            let mut elems2 = forest
                .iter_filtered_reverse(&tree, AllQuery)
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

#[test]
fn retain1() -> anyhow::Result<()> {
    let xs = (0..10).map(|i| (Key(i), i)).collect::<Vec<_>>();
    let ok = do_retain(vec![xs])?;
    assert!(ok);
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
    let ok = do_retain(xs)?;
    assert!(ok);
    Ok(())
}

#[test]
fn build1() -> anyhow::Result<()> {
    let xs = (0..10).map(|i| (Key(i), i)).collect::<Vec<_>>();
    let store = MemStore::new(usize::max_value(), Sha256Digest::digest);
    let forest = txn(store, 1000);
    let tree = forest.extend(&Tree::debug(), xs)?;
    forest.dump(&tree)?;
    // let foo = Tree::empty()
    Ok(())
}
