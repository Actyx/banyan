use futures::future::poll_fn;
use futures::prelude::*;
use sqlite::SqliteStore;
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};
use structopt::StructOpt;
use tag_index::{Tag, TagSet};
use tracing::Level;

mod ipfs;
mod sqlite;
mod tag_index;
mod tags;

use banyan::{
    forest::*,
    query::{AllQuery, OffsetRangeQuery, QueryExt},
    store::{ArcBlockWriter, ArcReadOnlyStore},
    tree::*,
};
use ipfs::{pubsub_pub, pubsub_sub, IpfsStore};
use tags::{DnfQuery, Key, Sha256Digest, TT};

#[cfg(target_env = "musl")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub type Error = anyhow::Error;
pub type Result<T> = anyhow::Result<T>;

type Txn = Transaction<TT, String, ArcReadOnlyStore<Sha256Digest>, ArcBlockWriter<Sha256Digest>>;

#[derive(StructOpt)]
#[structopt(about = "CLI to work with large banyan trees on ipfs")]
struct Opts {
    #[structopt(long, global = true)]
    /// An index password to use
    index_pass: Option<String>,
    #[structopt(long, global = true)]
    /// A value password to use
    value_pass: Option<String>,
    #[structopt(short, parse(from_occurrences = set_log_level), global = true)]
    #[allow(dead_code)] // log level will bet set in [`set_log_level`]
    /// Increase verbosity
    verbosity: u64,
    #[structopt(subcommand)]
    cmd: Command,
}

fn set_log_level(verbosity: u64) -> u64 {
    let level = match verbosity {
        0 => Level::ERROR,
        1 => Level::INFO,
        2 => Level::DEBUG,
        _ => Level::TRACE,
    };
    tracing_subscriber::fmt().with_max_level(level).init();
    verbosity
}

#[derive(StructOpt)]
enum Command {
    /// Benchmark
    Bench {
        #[structopt(long)]
        /// The number of values per batch
        count: u64,
    },
    /// Build a tree
    Build {
        #[structopt(long)]
        /// The number of values per batch
        count: u64,
        #[structopt(long, default_value = "1")]
        /// The number of batches
        batches: u64,
        #[structopt(long)]
        /// Do not balance while building
        unbalanced: bool,
        #[structopt(long)]
        /// Base on which to build
        base: Sha256Digest,
    },
    /// Dump a tree
    Dump {
        #[structopt(long)]
        /// The root hash to use
        root: Sha256Digest,
    },
    /// Stream a tree, filtered
    Filter {
        #[structopt(long)]
        /// The root hash to use
        root: Sha256Digest,
        #[structopt(long)]
        /// Tags to filter
        tag: Vec<String>,
    },
    /// Forget data from a tree
    Forget {
        #[structopt(long)]
        /// The root hash to use
        root: Sha256Digest,
        #[structopt(long)]
        /// The offset before which to forget data
        before: u64,
    },
    /// Pack a tree
    Pack {
        #[structopt(long)]
        /// The root hash to use
        root: Sha256Digest,
    },
    /// Receive a stream
    RecvStream {
        #[structopt(long)]
        /// Topic to receive data over
        topic: String,
    },
    /// Repair a tree
    Repair {
        #[structopt(long)]
        /// The root hash to use
        root: Sha256Digest,
    },
    /// Send a stream
    SendStream {
        #[structopt(long)]
        /// Topic to send data over
        topic: String,
    },
    /// Stream a tree
    Stream {
        #[structopt(long)]
        /// The root hash to use
        root: Sha256Digest,
    },
}

struct Tagger(BTreeMap<&'static str, Tag>);

impl Tagger {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn tag(&mut self, name: &'static str) -> Tag {
        self.0.entry(name).or_insert_with(|| name.into()).clone()
    }

    pub fn tags(&mut self, names: &[&'static str]) -> TagSet {
        names.iter().map(|name| self.tag(name)).collect::<TagSet>()
    }
}

fn create_salsa_key(text: String) -> salsa20::Key {
    let mut key = [0u8; 32];
    for (i, v) in text.as_bytes().iter().take(32).enumerate() {
        key[i] = *v;
    }
    key.into()
}

async fn build_tree(
    forest: &Txn,
    base: Option<Sha256Digest>,
    batches: u64,
    count: u64,
    unbalanced: bool,
    print_every: u64,
) -> anyhow::Result<Tree<TT>> {
    let mut tagger = Tagger::new();
    // function to add some arbitrary tags to test out tag querying and compression
    let mut tags_from_offset = |i: u64| -> TagSet {
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
    let mut tree = match base {
        Some(root) => forest.load_tree(root)?,
        None => Tree::<TT>::empty(),
    };
    let mut offset: u64 = 0;
    for _ in 0..batches {
        let v = (0..count)
            .map(|_| {
                if offset % print_every == 0 {
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
            tree = forest.extend_unpacked(&tree, v)?;
            forest.assert_invariants(&tree)?;
        } else {
            tree = forest.extend(&tree, v)?;
            forest.assert_invariants(&tree)?;
        }
    }
    Ok(tree)
}

async fn bench_build(
    forest: &Txn,
    base: Option<Sha256Digest>,
    batches: u64,
    count: u64,
    unbalanced: bool,
) -> anyhow::Result<(Tree<TT>, std::time::Duration)> {
    let mut tagger = Tagger::new();
    // function to add some arbitrary tags to test out tag querying and compression
    let mut tags_from_offset = |i: u64| -> TagSet {
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
    let mut tree = match base {
        Some(root) => forest.load_tree(root)?,
        None => Tree::<TT>::empty(),
    };
    let mut offset: u64 = 0;
    let data = (0..batches)
        .map(|_b| {
            (0..count)
                .map(|_| {
                    let result = (
                        Key::single(offset, offset, tags_from_offset(offset)),
                        offset.to_string(),
                    );
                    offset += 1;
                    result
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let t0 = std::time::Instant::now();
    for v in data {
        if unbalanced {
            tree = forest.extend_unpacked(&tree, v)?;
            forest.assert_invariants(&tree)?;
        } else {
            tree = forest.extend(&tree, v)?;
            forest.assert_invariants(&tree)?;
        }
    }
    let t1 = std::time::Instant::now();
    Ok((tree, t1 - t0))
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::from_args();
    let mut tagger = Tagger::new();
    // function to add some arbitrary tags to test out tag querying and compression
    let mut tags_from_offset = |i: u64| -> TagSet {
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

    let store = Arc::new(IpfsStore::new()?);
    let index_key: salsa20::Key = opts.index_pass.map(create_salsa_key).unwrap_or_default();
    let value_key: salsa20::Key = opts.value_pass.map(create_salsa_key).unwrap_or_default();
    let config = Config::debug_fast();
    let crypto_config = CryptoConfig {
        index_key,
        value_key,
    };
    let txn = || {
        let branch_cache = BranchCache::new(1000);
        Txn::new(
            Forest::new(store.clone(), branch_cache, crypto_config, config),
            store.clone(),
        )
    };
    let forest = txn();
    match opts.cmd {
        Command::Dump { root } => {
            let tree = forest.load_tree(root)?;
            forest.dump(&tree)?;
        }
        Command::Stream { root } => {
            let tree = forest.load_tree(root)?;
            let mut stream = forest.stream_filtered(&tree, AllQuery).enumerate();
            while let Some((i, Ok(v))) = stream.next().await {
                if i % 1000 == 0 {
                    println!("{:?}", v);
                }
            }
        }
        Command::Build {
            base,
            batches,
            count,
            unbalanced,
        } => {
            println!(
                "building a tree with {} batches of {} values, unbalanced: {}",
                batches, count, unbalanced
            );
            let tree = build_tree(&forest, Some(base), batches, count, unbalanced, 1000).await?;
            forest.dump(&tree)?;
            let roots = forest.roots(&tree)?;
            let levels = roots.iter().map(|x| x.level()).collect::<Vec<_>>();
            let _tree2 = forest.tree_from_roots(roots)?;
            println!("{:?}", tree);
            println!("{}", tree);
            println!("{:?}", levels);
            forest.dump(&tree)?;
        }
        Command::Bench { count } => {
            let store = Arc::new(SqliteStore::memory()?);
            let config = Config::debug_fast();
            let crypto_config = CryptoConfig {
                index_key,
                value_key,
            };
            let branch_cache = BranchCache::new(1000);
            let forest = Txn::new(
                Forest::new(store.clone(), branch_cache, crypto_config, config),
                store,
            );
            let _t0 = std::time::Instant::now();
            let base = None;
            let batches = 1;
            let unbalanced = false;
            let (tree, tcreate) = bench_build(&forest, base, batches, count, unbalanced).await?;
            let t0 = std::time::Instant::now();
            let _values: Vec<_> = forest.collect(&tree)?;
            let t1 = std::time::Instant::now();
            let tcollect = t1 - t0;
            let t0 = std::time::Instant::now();
            let tags = vec![Key::range(
                0,
                u64::max_value(),
                TagSet::single(Tag::from("fizz")),
            )];
            let query = DnfQuery(tags).boxed();
            let values: Vec<_> = forest
                .stream_filtered(&tree, query)
                .map_ok(|(_, k, v)| (k, v))
                .collect::<Vec<_>>()
                .await;
            println!("{}", values.len());
            let t1 = std::time::Instant::now();
            let tfilter_common = t1 - t0;
            let t0 = std::time::Instant::now();
            let tags = vec![Key::range(
                0,
                count / 10,
                TagSet::single(Tag::from("fizzbuzz")),
            )];
            let query = DnfQuery(tags).boxed();
            let values: Vec<_> = forest
                .stream_filtered(&tree, query)
                .map_ok(|(_, k, v)| (k, v))
                .collect::<Vec<_>>()
                .await;
            println!("{}", values.len());
            let t1 = std::time::Instant::now();
            let tfilter_rare = t1 - t0;
            println!("create {}", (tcreate.as_micros() as f64) / 1000000.0);
            println!("collect {}", (tcollect.as_micros() as f64) / 1000000.0);
            println!(
                "filter_common {}",
                (tfilter_common.as_micros() as f64) / 1000000.0
            );
            println!(
                "filter_rare {}",
                (tfilter_rare.as_micros() as f64) / 1000000.0
            );
        }
        Command::Filter { tag, root } => {
            let tags = tag
                .into_iter()
                .map(|tag| Key::filter_tags(TagSet::single(Tag::from(tag))))
                .collect::<Vec<_>>();
            let query = DnfQuery(tags).boxed();
            let tree = forest.load_tree(root)?;
            forest.dump(&tree)?;
            let mut stream = forest.stream_filtered(&tree, query).enumerate();
            while let Some((i, Ok(v))) = stream.next().await {
                if i % 1000 == 0 {
                    println!("{:?}", v);
                }
            }
        }
        Command::Forget { root, before } => {
            let mut tree = forest.load_tree(root)?;
            tree = forest.retain(&tree, &OffsetRangeQuery::from(before..))?;
            forest.dump(&tree)?;
            println!("{:?}", tree);
        }
        Command::Pack { root } => {
            let mut tree = forest.load_tree(root)?;
            forest.dump(&tree)?;
            tree = forest.pack(&tree)?;
            forest.assert_invariants(&tree)?;
            assert!(forest.is_packed(&tree)?);
            forest.dump(&tree)?;
            println!("{:?}", tree);
        }
        Command::RecvStream { topic } => {
            let stream = pubsub_sub(&*topic)?
                .map_err(anyhow::Error::new)
                .and_then(|data| future::ready(String::from_utf8(data).map_err(anyhow::Error::new)))
                .and_then(|data| future::ready(Sha256Digest::from_str(&data)));
            let forest2 = forest.clone();
            let trees = stream.filter_map(move |x| {
                let forest = forest2.clone();
                future::ready(x.and_then(move |link| forest.load_tree(link)).ok())
            });
            let mut stream = forest.read().stream_trees(AllQuery, trees).boxed_local();
            while let Some(ev) = stream.next().await {
                println!("{:?}", ev);
            }
        }
        Command::Repair { root } => {
            let tree = forest.load_tree(root)?;
            let (tree, _) = forest.repair(&tree)?;
            forest.dump(&tree)?;
            println!("{:?}", tree);
        }
        Command::SendStream { topic } => {
            let mut ticks = tokio::time::interval(Duration::from_secs(1));
            let mut tree = Tree::<TT>::empty();
            let mut offset = 0;
            loop {
                poll_fn(|cx| ticks.poll_tick(cx)).await;
                let key = Key::single(offset, offset, tags_from_offset(offset));
                tree = forest.extend_unpacked(&tree, Some((key, "xxx".into())))?;
                if tree.level() > 100 {
                    println!("packing the tree");
                    tree = forest.pack(&tree)?;
                }
                offset += 1;
                if let Some(cid) = tree.link() {
                    println!("publishing {} to {}", cid, topic);
                    pubsub_pub(&*topic, cid.to_string().as_bytes()).await?;
                }
            }
        }
    }

    Ok(())
}
