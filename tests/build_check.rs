use banyan::index::{Semigroup, SimpleCompactSeq};
use banyan::store::MemStore;
use banyan::tree::{Config, Forest, Query, Tree, TreeTypes};
use futures::prelude::*;
use quickcheck::{Arbitrary, Gen, TestResult};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

struct TT;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Key(u64);

impl TreeTypes for TT {
    type Key = Key;
    type Seq = SimpleCompactSeq<Key>;
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

fn reverse<T: Clone>(xs: &[T]) -> Vec<T> {
    let mut rev = vec![];
    for x in xs {
        rev.insert(0, x.clone())
    }
    rev
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

struct TrueQuery;

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

// #[quickcheck_async::tokio]
// async fn build_stream_filtered(xs: Vec<(Key, u64)>) -> quickcheck::TestResult {
//     test(|| async {
//         let tree = create_test_tree(xs.clone()).await?;
//         let actual = tree
//             .stream_filtered(&TrueQuery)
//             .collect::<Vec<_>>()
//             .await
//             .into_iter()
//             .collect::<anyhow::Result<Vec<_>>>()?;
//         let expected = xs.iter().cloned().enumerate().map(|(i, (k, v))| (i as u64, k, v)).collect::<Vec<_>>();
//         Ok(actual == expected)
//     })
//     .await
// }

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
