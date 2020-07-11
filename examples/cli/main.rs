use anyhow::anyhow;
use clap::{App, Arg, SubCommand};
use futures::prelude::*;
use maplit::btreeset;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};
use tracing::Level;
use tracing_subscriber;

mod ipfs;

use banyan::index::*;
use banyan::{
    ipfs::Cid,
    query::{OffsetRangeQuery, Query},
    tree::*,
};
use ipfs::IpfsStore;

use bitvec::prelude::*;

pub type Error = anyhow::Error;
pub type Result<T> = anyhow::Result<T>;

struct TT {}

impl TreeTypes for TT {
    type Key = Key;
    type Seq = KeySeq;
    type Link = Cid;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Ord, Eq)]
struct Tag(Arc<str>);

impl Tag {
    pub fn new(text: &str) -> Self {
        Self(text.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Tags(BTreeSet<Tag>);

impl Tags {
    fn empty() -> Self {
        Self(BTreeSet::new())
    }
    fn single(text: &str) -> Self {
        Self(btreeset! { Tag(text.into()) })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Key {
    min_lamport: u64,
    min_time: u64,
    max_time: u64,
    tags: Tags,
}

impl Key {
    fn single(lamport: u64, time: u64, tags: Tags) -> Self {
        Self {
            min_lamport: lamport,
            min_time: time,
            max_time: time,
            tags,
        }
    }

    fn filter_tags(tags: Tags) -> Self {
        Self {
            min_lamport: u64::MIN,
            min_time: u64::MIN,
            max_time: u64::MAX,
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

    fn intersects(&self, that: &Key) -> bool {
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

    fn contains(&self, that: &Key) -> bool {
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

impl Semigroup for Key {
    fn combine(&mut self, b: &Self) {
        self.min_lamport = self.min_lamport.min(b.min_lamport);
        self.min_time = self.min_time.min(b.min_time);
        self.max_time = self.max_time.max(b.max_time);
        self.tags.0.extend(b.tags.0.iter().cloned());
    }
}

#[derive(Debug)]
struct DnfQuery(Vec<Key>);

impl DnfQuery {
    fn intersects(&self, v: &Key) -> bool {
        self.0.iter().any(|x| x.intersects(v))
    }
    fn contains(&self, v: &Key) -> bool {
        self.0.iter().any(|x| x.contains(v))
    }
}

impl Query<TT> for DnfQuery {
    fn intersecting(&self, _: u64, x: &BranchIndex<Cid, KeySeq>, matching: &mut BitVec) {
        for (i, s) in x.summaries().take(matching.len()).enumerate() {
            if matching[i] {
                matching.set(i, self.intersects(&s));
            }
        }
    }
    fn containing(&self, _: u64, x: &LeafIndex<Cid, KeySeq>, matching: &mut BitVec) {
        for (i, s) in x.keys().take(matching.len()).enumerate() {
            if matching[i] {
                matching.set(i, self.contains(&s));
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct KeySeq {
    min_lamport: Vec<u64>,
    min_time: Vec<u64>,
    max_time: Vec<u64>,
    tags: Vec<Tags>,
}

impl CompactSeq for KeySeq {
    type Item = Key;

    fn empty() -> Self {
        Self {
            min_lamport: Vec::new(),
            min_time: Vec::new(),
            max_time: Vec::new(),
            tags: Vec::new(),
        }
    }

    fn single(value: &Key) -> Self {
        Self {
            min_lamport: vec![value.min_lamport],
            min_time: vec![value.min_time],
            max_time: vec![value.max_time],
            tags: vec![value.tags.clone()],
        }
    }

    fn push(&mut self, value: &Key) {
        self.min_lamport.push(value.min_lamport);
        self.min_time.push(value.min_time);
        self.max_time.push(value.max_time);
        self.tags.push(value.tags.clone());
    }

    fn extend(&mut self, value: &Key) {
        let min_lamport = self.min_lamport.last_mut().unwrap();
        let min_time = self.min_time.last_mut().unwrap();
        let max_time = self.max_time.last_mut().unwrap();
        let tags = self.tags.last_mut().unwrap();
        *min_lamport = value.min_lamport.min(*min_lamport);
        *min_time = value.min_time.min(*min_time);
        *max_time = value.max_time.min(*max_time);
        tags.0.extend(value.tags.0.iter().cloned());
    }

    fn get(&self, index: usize) -> Option<Key> {
        if let (Some(min_lamport), Some(min_time), Some(max_time), Some(tags)) = (
            self.min_lamport.get(index),
            self.min_time.get(index),
            self.max_time.get(index),
            self.tags.get(index),
        ) {
            Some(Key {
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

    fn summarize(&self) -> Key {
        let mut result = self.get(0).unwrap();
        for i in 1..self.tags.len() {
            result.combine(&self.get(i).unwrap());
        }
        result
    }
}

fn app() -> clap::App<'static, 'static> {
    let root_arg = || {
        Arg::with_name("root")
            .required(true)
            .takes_value(true)
            .help("The root hash to use")
    };
    let index_pass_arg = || {
        Arg::with_name("index_pass")
            .long("index_pass")
            .required(false)
            .takes_value(true)
            .help("An index password to use")
    };
    let value_pass_arg = || {
        Arg::with_name("value_pass")
            .long("value_pass")
            .required(false)
            .takes_value(true)
            .help("A value password to use")
    };
    App::new("banyan-cli")
        .version("0.1")
        .author("Rüdiger Klaehn")
        .about("CLI to work with large banyan trees on ipfs")
        .arg(index_pass_arg())
        .arg(value_pass_arg())
        .subcommand(
            SubCommand::with_name("dump")
                .about("Dump a tree")
                .arg(root_arg()),
        )
        .subcommand(
            SubCommand::with_name("stream")
                .about("Stream a tree")
                .arg(root_arg()),
        )
        .subcommand(
            SubCommand::with_name("build")
                .about("Build a tree")
                .arg(
                    Arg::with_name("count")
                        .long("count")
                        .required(true)
                        .takes_value(true)
                        .help("The number of values per batch"),
                )
                .arg(
                    Arg::with_name("batches")
                        .long("batches")
                        .takes_value(true)
                        .default_value("1")
                        .help("The number of batches"),
                )
                .arg(
                    Arg::with_name("unbalanced")
                        .long("unbalanced")
                        .takes_value(false)
                        .help("Do not balance while building"),
                ),
        )
        .subcommand(
            SubCommand::with_name("filter")
                .about("Stream a tree, filtered")
                .arg(root_arg())
                .arg(
                    Arg::with_name("tag")
                        .long("tag")
                        .required(true)
                        .multiple(true)
                        .takes_value(true)
                        .help("Tags to filter"),
                ),
        )
        .subcommand(
            SubCommand::with_name("pack")
                .about("Pack a tree")
                .arg(root_arg()),
        )
        .subcommand(
            SubCommand::with_name("repair")
                .about("Repair a tree")
                .arg(root_arg()),
        )
        .subcommand(
            SubCommand::with_name("forget")
                .about("Forget data from a tree")
                .arg(root_arg())
                .arg(
                    Arg::with_name("before")
                        .long("before")
                        .required(true)
                        .takes_value(true)
                        .help("The offset before which to forget data"),
                ),
        )
        .subcommand(SubCommand::with_name("demo").about("Do some stuff"))
}

struct Tagger(BTreeMap<&'static str, Tag>);

impl Tagger {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn tag(&mut self, name: &'static str) -> Tag {
        self.0.entry(name).or_insert_with(|| Tag::new(name)).clone()
    }

    pub fn tags(&mut self, names: &[&'static str]) -> Tags {
        Tags(
            names
                .into_iter()
                .map(|name| self.tag(name))
                .collect::<BTreeSet<_>>(),
        )
    }
}

fn create_salsa_key(text: &str) -> salsa20::Key {
    let mut key = [0u8; 32];
    for (i, v) in text.as_bytes().iter().take(32).enumerate() {
        key[i] = *v;
    }
    key.into()
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

    let mut tagger = Tagger::new();
    // function to add some arbitrary tags to test out tag querying and compression
    let mut tags_from_offset = |i: u64| -> Tags {
        let fizz = i % 3 == 0;
        let buzz = i % 5 == 0;
        if fizz && buzz {
            tagger.tags(&["fizzbuzz"])
        } else if fizz {
            tagger.tags(&["fizz"])
        } else if buzz {
            tagger.tags(&["buzz"])
        } else {
            tagger.tags(&["we.like.long.identifiers.because.they.seem.professional"])
        }
    };

    let store = Arc::new(IpfsStore::new());
    let matches = app().get_matches();
    let index_key: salsa20::Key = matches
        .value_of("index_pass")
        .map(create_salsa_key)
        .unwrap_or_default();
    let value_key: salsa20::Key = matches
        .value_of("value_pass")
        .map(create_salsa_key)
        .unwrap_or_default();
    let mut config = Config::debug();
    config.index_key = index_key;
    config.value_key = value_key;
    let forest = Arc::new(Forest::<TT>::new(store, config));
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
        let count: u64 = matches
            .value_of("count")
            .ok_or(anyhow!("required arg count not provided"))?
            .parse()?;
        let batches: u64 = matches
            .value_of("batches")
            .ok_or(anyhow!("required arg count not provided"))?
            .parse()?;
        let unbalanced = matches.is_present("unbalanced");
        println!(
            "building a tree with {} batches of {} values, unbalanced: {}",
            batches, count, unbalanced
        );
        let mut tree = Tree::<TT, String>::empty(forest);
        let mut offset: u64 = 0;
        for _ in 0..batches {
            let v = (0..count)
                .map(|_| {
                    if offset % 1000 == 0 {
                        println!("{}", offset);
                    }
                    let result = (
                        Key::single(offset, offset, tags_from_offset(offset)),
                        offset.to_string(),
                    );
                    offset += 1;
                    result
                })
                .collect::<Vec<_>>();
            if unbalanced {
                tree.extend_unbalanced(v).await?;
                tree.assert_invariants().await?;
            } else {
                tree.extend(v).await?;
                tree.assert_invariants().await?;
            }
        }
        tree.dump().await?;
        println!("{:?}", tree);
        println!("{}", tree);
    } else if let Some(matches) = matches.subcommand_matches("pack") {
        let root = Cid::from_str(
            matches
                .value_of("root")
                .ok_or(anyhow!("root must be provided"))?,
        )?;
        let tree = Tree::<TT, serde_cbor::Value>::new(root, forest).await?;
        tree.dump().await?;
        let tree = tree.pack().await?;
        tree.assert_invariants().await?;
        assert!(tree.is_packed().await?);
        tree.dump().await?;
        println!("{:?}", tree);
    } else if let Some(matches) = matches.subcommand_matches("filter") {
        let root = Cid::from_str(
            matches
                .value_of("root")
                .ok_or(anyhow!("root must be provided"))?,
        )?;
        let tags = matches
            .values_of("tag")
            .ok_or(anyhow!("at least one tag must be provided"))?
            .map(|tag| Key::filter_tags(Tags(btreeset! {Tag::new(tag)})))
            .collect::<Vec<_>>();
        let query = DnfQuery(tags);
        let tree = Tree::<TT, serde_cbor::Value>::new(root, forest).await?;
        tree.dump().await?;
        let mut stream = tree.stream_filtered(&query).enumerate();
        while let Some((i, Ok(v))) = stream.next().await {
            if i % 1000 == 0 {
                println!("{:?}", v);
            }
        }
    } else if let Some(matches) = matches.subcommand_matches("repair") {
        let root = Cid::from_str(
            matches
                .value_of("root")
                .ok_or(anyhow!("root must be provided"))?,
        )?;
        let mut tree = Tree::<TT, serde_cbor::Value>::new(root, forest).await?;
        tree.repair().await?;
        tree.dump().await?;
        println!("{:?}", tree);
    } else if let Some(matches) = matches.subcommand_matches("forget") {
        let root = Cid::from_str(
            matches
                .value_of("root")
                .ok_or(anyhow!("root must be provided"))?,
        )?;
        let offset: u64 = matches
            .value_of("before")
            .ok_or(anyhow!("required arg before not provided"))?
            .parse()?;
        let mut tree = Tree::<TT, serde_cbor::Value>::new(root, forest).await?;
        tree.forget_except(&OffsetRangeQuery::from(offset..))
            .await?;
        tree.dump().await?;
        println!("{:?}", tree);
    } else {
        app().print_long_help()?;
        println!();
    }
    Ok(())
}
