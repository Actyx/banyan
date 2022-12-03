use banyan::TreeTypes;
use futures::future::poll_fn;
use futures::prelude::*;
use ipfs_sqlite_block_store::BlockStore;

use std::{collections::BTreeMap, convert::TryFrom, str::FromStr, time::Duration};
use structopt::StructOpt;
use tracing::Level;

use banyan::{
    query::{AllQuery, OffsetRangeQuery, QueryExt},
    store::{BlockWriter, BranchCache, MemStore, ReadOnlyStore},
    Config, Forest, Secrets, StreamBuilder, Transaction, Tree,
};
use banyan_utils::{
    create_chacha_key, dump,
    ipfs::{pubsub_pub, pubsub_sub, IpfsStore},
    sqlite::SqliteStore,
    tag_index::{Tag, TagSet},
    tags::{DnfQuery, Key, Sha256Digest, TT},
};
use libipld::DefaultParams;

#[cfg(target_env = "musl")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub type Error = anyhow::Error;
pub type Result<T> = anyhow::Result<T>;

type Txn = Transaction<TT, Storage, Storage>;

#[derive(Clone)]
enum Storage {
    Memory(MemStore<Sha256Digest>),
    Ipfs(IpfsStore),
    Sqlite(SqliteStore<DefaultParams>),
}
impl ReadOnlyStore<Sha256Digest> for Storage {
    fn get(&self, link: &Sha256Digest) -> Result<Box<[u8]>> {
        match self {
            Self::Memory(m) => m.get(link),
            Storage::Ipfs(i) => i.get(link),
            Storage::Sqlite(s) => s.get(link),
        }
    }
}

impl BlockWriter<Sha256Digest> for Storage {
    fn put(&mut self, data: Vec<u8>) -> Result<Sha256Digest> {
        match self {
            Self::Memory(m) => m.put(data),
            Storage::Ipfs(i) => i.put(data),
            Storage::Sqlite(s) => s.put(data),
        }
    }
}
impl FromStr for Storage {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let s = match s {
            "memory" => Self::Memory(MemStore::new(usize::max_value(), Sha256Digest::new)),
            "ipfs" => Self::Ipfs(IpfsStore::default()),
            x => Self::Sqlite(SqliteStore::new(BlockStore::open(
                x,
                ipfs_sqlite_block_store::Config::default(),
            )?)),
        };
        Ok(s)
    }
}
#[derive(StructOpt)]
#[structopt(about = "CLI to work with large banyan trees")]
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
    #[structopt(long, default_value = "memory", global = true)]
    /// Storage, possible options "memory", "ipfs" (uses local ipfs gw via HTTP),
    /// or a path to a sqlite database (will create it if it doesn't exist)
    storage: Storage,
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
        base: Option<Sha256Digest>,
    },
    /// Traverse a tree and dump its output as dot. Can be piped directly:
    /// `banyan-cli graph --root <..> | dot -Tpng output.png`.
    /// Branches are depicted as rectangles, leafs as circles. A sealed node is
    /// greyed out.
    Graph {
        #[structopt(long)]
        /// The root hash to use
        root: Sha256Digest,
    },
    /// Dump a tree
    Dump {
        #[structopt(long)]
        /// The root hash to use
        root: Sha256Digest,
    },
    /// Dump a block as json to stdout
    DumpBlock {
        #[structopt(long)]
        /// The root hash to use
        hash: Sha256Digest,
        #[structopt(long)]
        cbor: bool,
    },
    /// Dumps all values of a tree as json to stdout, newline separated
    DumpValues {
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

async fn build_tree(
    forest: &mut Txn,
    base: Option<Sha256Digest>,
    batches: u64,
    count: u64,
    unbalanced: bool,
    print_every: u64,
) -> anyhow::Result<StreamBuilder<TT, String>> {
    let secrets = Secrets::default();
    let config = Config::debug();
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
        Some(root) => forest.load_stream_builder(secrets, config, root)?,
        None => StreamBuilder::<TT, String>::new(config, secrets),
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
            forest.extend_unpacked(&mut tree, v)?;
        } else {
            forest.extend(&mut tree, v)?;
        }
        forest.assert_invariants(&tree)?;
    }
    Ok(tree)
}

async fn bench_build(
    forest: &mut Txn,
    base: Option<Sha256Digest>,
    batches: u64,
    count: u64,
    unbalanced: bool,
    secrets: Secrets,
    config: Config,
) -> anyhow::Result<(Tree<TT, String>, std::time::Duration)> {
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
        Some(link) => forest.load_stream_builder(secrets, config, link)?,
        None => StreamBuilder::<TT, String>::new(config, secrets),
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
            forest.extend_unpacked(&mut tree, v)?;
        } else {
            forest.extend(&mut tree, v)?;
        }
        forest.assert_invariants(&tree)?;
    }
    let t1 = std::time::Instant::now();
    Ok((tree.snapshot(), t1 - t0))
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

    let store = opts.storage;
    let index_key: chacha20::Key = opts.index_pass.map(create_chacha_key).unwrap_or_default();
    let value_key: chacha20::Key = opts.value_pass.map(create_chacha_key).unwrap_or_default();
    let config = Config::debug_fast();
    let secrets = Secrets::new(index_key, value_key);
    let txn = || {
        Txn::new(
            Forest::new(store.clone(), BranchCache::default()),
            store.clone(),
        )
    };
    let mut forest = txn();
    match opts.cmd {
        Command::Graph { root } => {
            let tree = forest.load_tree::<String>(secrets, root)?;
            let mut stdout = std::io::stdout();
            dump::graph(&forest, &tree, &mut stdout)?;
        }
        Command::Dump { root } => {
            let tree = forest.load_tree::<String>(secrets, root)?;
            forest.dump(&tree)?;
        }
        Command::DumpValues { root } => {
            let tree = forest.load_tree::<String>(secrets, root)?;
            let iter = forest.iter_from(&tree);
            for res in iter {
                let (i, k, v) = res?;
                println!("{:?} {:?} {:?}", i, k, v);
            }
        }
        Command::DumpBlock { hash, cbor } => {
            let nonce = <&chacha20::XNonce>::try_from(TT::NONCE).unwrap();
            if cbor {
                dump::dump_cbor(store, hash, &value_key, nonce, &mut std::io::stdout())?;
            } else {
                dump::dump_json(store, hash, &value_key, nonce, &mut std::io::stdout())?;
            }
        }
        Command::Stream { root } => {
            let tree = forest.load_tree::<String>(secrets, root)?;
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
            let tree = build_tree(&mut forest, base, batches, count, unbalanced, 1000).await?;
            forest.dump(&tree.snapshot())?;
        }
        Command::Bench { count } => {
            let config = Config::debug_fast();
            let secrets = Secrets::new(index_key, value_key);
            let branch_cache = BranchCache::default();
            let mut forest = Txn::new(Forest::new(store.clone(), branch_cache), store);
            let _t0 = std::time::Instant::now();
            let base = None;
            let batches = 1;
            let unbalanced = false;
            let (tree, tcreate) = bench_build(
                &mut forest,
                base,
                batches,
                count,
                unbalanced,
                secrets,
                config,
            )
            .await?;
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
            let values = forest.iter_filtered(&tree, query).count();
            println!("{}", values);
            let t1 = std::time::Instant::now();
            let tfilter_common = t1 - t0;
            let t0 = std::time::Instant::now();
            let tags = vec![Key::range(
                0,
                count / 10,
                TagSet::single(Tag::from("fizzbuzz")),
            )];
            let query = DnfQuery(tags).boxed();
            let values = forest.iter_filtered(&tree, query).count();
            println!("{}", values);
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
            let secrets = Secrets::default();
            let config = Config::debug();
            let tree = forest.load_stream_builder::<String>(secrets, config, root)?;
            forest.dump(&tree.snapshot())?;
            let mut stream = forest.stream_filtered(&tree.snapshot(), query).enumerate();
            while let Some((i, Ok(v))) = stream.next().await {
                if i % 1000 == 0 {
                    println!("{:?}", v);
                }
            }
        }
        Command::Forget { root, before } => {
            let secrets = Secrets::default();
            let config = Config::debug();
            let mut tree = forest.load_stream_builder::<String>(secrets, config, root)?;
            forest.retain(&mut tree, &OffsetRangeQuery::from(before..))?;
            forest.dump(&tree.snapshot())?;
            println!("{:?}", tree);
        }
        Command::Pack { root } => {
            let secrets = Secrets::default();
            let config = Config::debug();
            let mut tree = forest.load_stream_builder::<String>(secrets, config, root)?;
            forest.dump(&tree.snapshot())?;
            forest.pack(&mut tree)?;
            forest.assert_invariants(&tree)?;
            assert!(forest.is_packed(&tree.snapshot())?);
            forest.dump(&tree.snapshot())?;
            println!("{:?}", tree);
        }
        Command::RecvStream { topic } => {
            let secrets = Secrets::default();
            let links = pubsub_sub(&topic)?
                .map_err(anyhow::Error::new)
                .and_then(|data| future::ready(String::from_utf8(data).map_err(anyhow::Error::new)))
                .and_then(|data| future::ready(Sha256Digest::from_str(&data)));
            let forest2 = forest.clone();
            let trees = links.filter_map(move |link| {
                let forest = forest2.clone();
                let secrets = secrets.clone();
                future::ready(
                    link.and_then(move |link| forest.load_tree::<String>(secrets, link))
                        .ok(),
                )
            });
            let mut s = forest.read().stream_trees(AllQuery, trees).boxed_local();
            while let Some(ev) = s.next().await {
                println!("{:?}", ev);
            }
        }
        Command::Repair { root } => {
            let mut tree = forest.load_stream_builder::<String>(secrets, config, root)?;
            let _ = forest.repair(&mut tree)?;
            forest.dump(&tree.snapshot())?;
            println!("{:?}", tree);
        }
        Command::SendStream { topic } => {
            let mut ticks = tokio::time::interval(Duration::from_secs(1));
            let mut tree = StreamBuilder::<TT, String>::new(config, secrets);
            let mut offset = 0;
            loop {
                poll_fn(|cx| ticks.poll_tick(cx)).await;
                let key = Key::single(offset, offset, tags_from_offset(offset));
                forest.extend_unpacked(&mut tree, Some((key, "xxx".into())))?;
                if tree.level() > 100 {
                    println!("packing the tree");
                    forest.pack(&mut tree)?;
                }
                offset += 1;
                if let Some(cid) = tree.link() {
                    println!("publishing {} to {}", cid, topic);
                    pubsub_pub(&topic, cid.to_string().as_bytes()).await?;
                }
            }
        }
    }

    Ok(())
}
