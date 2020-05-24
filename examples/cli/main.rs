use anyhow::anyhow;
use clap::{App, Arg, SubCommand};
use futures::prelude::*;
use maplit::btreeset;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, VecDeque},
    str::FromStr,
    sync::Arc,
};
use tracing::Level;
use tracing_subscriber;

mod ipfs;

use czaa::index::*;
use czaa::{ipfs::Cid, store::MemStore, tree::*};
use ipfs::IpfsStore;

pub type Error = anyhow::Error;
pub type Result<T> = anyhow::Result<T>;

struct TT {}

impl TreeTypes for TT {
    type Key = Value;
    type Seq = ValueSeq;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Ord, Eq)]
struct Tag(Arc<str>);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tags(BTreeSet<Tag>);

impl Tags {
    fn empty() -> Self {
        Self(BTreeSet::new())
    }
    fn single(text: &str) -> Self {
        Self(btreeset! { Tag(text.into()) })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Value {
    min_lamport: u64,
    min_time: u64,
    max_time: u64,
    tags: Tags,
}

impl Value {
    fn single(lamport: u64, time: u64, tags: Tags) -> Self {
        Self {
            min_lamport: lamport,
            min_time: time,
            max_time: time,
            tags,
        }
    }

    fn range(min_time: u64, max_time: u64, tags: Tags) -> Self {
        Self {
            min_lamport: 0,
            min_time,
            max_time,
            tags,
        }
    }

    fn intersects(&self, that: &Value) -> bool {
        if self.max_time < that.min_time {
            return false;
        }
        if self.min_time > that.max_time {
            return false;
        }
        if self.tags.0.is_disjoint(&that.tags.0) {
            return false;
        }
        true
    }

    fn contains(&self, that: &Value) -> bool {
        if that.min_time < self.min_time {
            return false;
        }
        if that.max_time > self.max_time {
            return false;
        }
        if !that.tags.0.is_subset(&self.tags.0) {
            return false;
        }
        true
    }
}

impl Semigroup for Value {
    fn combine(&mut self, b: &Value) {
        self.min_lamport = self.min_lamport.min(b.min_lamport);
        self.min_time = self.min_time.min(b.min_time);
        self.max_time = self.max_time.max(b.max_time);
        self.tags.0.extend(b.tags.0.iter().cloned());
    }
}

struct DnfQuery(Vec<Value>);

impl Query<TT> for DnfQuery {
    type IndexIterator = std::vec::IntoIter<bool>;
    fn intersects(&self, v: &Value) -> bool {
        self.0.iter().any(|x| x.intersects(v))
    }
    fn contains(&self, v: &Value) -> bool {
        self.0.iter().any(|x| x.contains(v))
    }
    fn intersecting(&self, _: u64, x: &BranchIndex<ValueSeq>) -> Self::IndexIterator {
        let bools = x.items().map(|x| self.intersects(&x)).collect::<Vec<_>>();
        bools.into_iter()
    }
    fn containing(&self, _: u64, x: &LeafIndex<ValueSeq>) -> Self::IndexIterator {
        let bools = x.items().map(|x| self.contains(&x)).collect::<Vec<_>>();
        bools.into_iter()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ValueSeq {
    min_lamport: Vec<u64>,
    min_time: Vec<u64>,
    max_time: Vec<u64>,
    tags: Vec<Tags>,
}

impl CompactSeq for ValueSeq {
    type Item = Value;

    fn single(value: &Value) -> Self {
        Self {
            min_lamport: vec![value.min_lamport],
            min_time: vec![value.min_time],
            max_time: vec![value.max_time],
            tags: vec![value.tags.clone()],
        }
    }

    fn push(&mut self, value: &Value) {
        self.min_lamport.push(value.min_lamport);
        self.min_time.push(value.min_time);
        self.max_time.push(value.max_time);
        self.tags.push(value.tags.clone());
    }

    fn extend(&mut self, value: &Value) {
        let min_lamport = self.min_lamport.last_mut().unwrap();
        let min_time = self.min_time.last_mut().unwrap();
        let max_time = self.max_time.last_mut().unwrap();
        let tags = self.tags.last_mut().unwrap();
        *min_lamport = value.min_lamport.min(*min_lamport);
        *min_time = value.min_time.min(*min_time);
        *max_time = value.max_time.min(*max_time);
        tags.0.extend(value.tags.0.iter().cloned());
    }

    fn get(&self, index: u64) -> Option<Value> {
        let index = index as usize;
        if let (Some(min_lamport), Some(min_time), Some(max_time), Some(tags)) = (
            self.min_lamport.get(index),
            self.min_time.get(index),
            self.max_time.get(index),
            self.tags.get(index),
        ) {
            Some(Value {
                min_lamport: *min_lamport,
                min_time: *min_time,
                max_time: *max_time,
                tags: tags.clone(),
            })
        } else {
            None
        }
    }

    fn count(&self) -> u64 {
        self.tags.len() as u64
    }

    fn summarize(&self) -> Value {
        let mut result = self.get(0).unwrap();
        for i in 1..self.tags.len() as u64 {
            result.combine(&self.get(i).unwrap());
        }
        result
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // a builder for `FmtSubscriber`.
    tracing_subscriber::fmt()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::INFO)
        // completes the builder and sets the constructed `Subscriber` as the default.
        .init();

    let store = Arc::new(IpfsStore::new());
    let forest = Arc::new(Forest::<TT>::new(store, Config::debug()));
    let matches = App::new("banyan-cli")
        .version("0.1")
        .author("Rüdiger Klaehn")
        .about("CLI to work with large banyan trees on ipfs")
        .subcommand(
            SubCommand::with_name("dump").about("Dump a tree").arg(
                Arg::with_name("root")
                    .long("root")
                    .required(true)
                    .takes_value(true)
                    .help("The root hash to use"),
            ),
        )
        .subcommand(
            SubCommand::with_name("stream").about("Stream a tree").arg(
                Arg::with_name("root")
                    .long("root")
                    .required(true)
                    .takes_value(true)
                    .help("The root hash to use"),
            ),
        )
        .subcommand(
            SubCommand::with_name("build").about("Build a tree").arg(
                Arg::with_name("count")
                    .long("count")
                    .required(true)
                    .takes_value(true)
                    .help("The number of values"),
            ),
        )
        .get_matches();
    if let Some(matches) = matches.subcommand_matches("dump") {
        let root = Cid::from_str(
            matches
                .value_of("root")
                .ok_or(anyhow!("root must be provided"))?,
        )?;
        let tree = Tree::<TT, serde_cbor::Value>::new(root, forest).await?;
        tree.dump().await?;
        return Ok(());
    } else if let Some(matches) = matches.subcommand_matches("stream") {
        let root = Cid::from_str(
            matches
                .value_of("root")
                .ok_or(anyhow!("root must be provided"))?,
        )?;
        let tree = Tree::<TT, serde_cbor::Value>::new(root, forest).await?;
        let mut stream = tree.stream().enumerate();
        while let Some((i, Ok(v))) = stream.next().await {
            if i % 1000 == 0 {
                println!("{:?}", v);
            }
        }
        return Ok(());
    } else if let Some(matches) = matches.subcommand_matches("build") {
        let n: u64 = matches
            .value_of("count")
            .ok_or(anyhow!("required arg count not provided"))?
            .parse()?;
        println!("{:?}", matches);
        println!("building a tree");
        // let store = TestStore::new();
        let store = Arc::new(IpfsStore::new());
        let forest = Arc::new(Forest::new(store, Config::default()));
        let mut tree = Tree::<TT, u64>::empty(forest.clone());
        tree.push(&Value::single(0, 0, Tags::empty()), &0u64)
            .await?;
        println!("{:?}", tree.get(0).await?);

        // let n = 100;
        // let mut tree = Tree::<TT, u64>::empty(forest.clone());
        // for i in 0..n {
        //     println!("{}", i);
        //     tree.push(&Value::single(i, i, Tags::single("foo")), &i)
        //         .await?;
        // }

        // tree.dump().await?;

        // for i in 0..n {
        //     println!("{:?}", tree.get(i).await?);
        // }

        let mut stream = tree.stream().enumerate();
        while let Some((i, Ok(v))) = stream.next().await {
            if i % 1000 == 0 {
                println!("{:?}", v);
            }
        }

        println!("filtered iteration!");
        let query = DnfQuery(vec![Value::range(0, 50, Tags::single("foo"))]);
        let mut stream = tree.stream_filtered(&query);
        while let Some(Ok(v)) = stream.next().await {
            println!("{:?}", v);
        }

        println!("filtered iteration - brute force!");
        let mut stream = tree.stream().try_filter_map(|(k, v)| {
            future::ok(if query.contains(&k) {
                Some((k, v))
            } else {
                None
            })
        });
        while let Some(Ok(v)) = stream.next().await {
            println!("{:?}", v);
        }

        println!("{:?}", tree);

        let mut tree2 = Tree::<TT, u64>::empty(forest);
        let tags = Tags::single("foo");
        let mut v = (0..n)
            .map(|i| {
                println!("{}", i);
                (Value::single(i, i, tags.clone()), i)
            })
            .collect::<VecDeque<_>>();
        tree2.extend(&mut v).await?;
        tree2.dump().await?;
        println!("{:?}", tree2);

        // czaa::flat_tree::test();
    }
    Ok(())
}
