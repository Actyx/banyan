use anyhow::anyhow;
use clap::{App, Arg, SubCommand};
use futures::prelude::*;
use maplit::btreeset;
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tracing::Level;
use tracing_subscriber;

mod ipfs;
mod tags;

use banyan::{
    forest::*,
    query::{AllQuery, OffsetRangeQuery, Query},
    tree::*,
};
use ipfs::{pubsub_pub, pubsub_sub, Cid, IpfsStore, MemStore};
use tags::{DnfQuery, Key, Tag, Tags, TT};

pub type Error = anyhow::Error;
pub type Result<T> = anyhow::Result<T>;

fn app() -> clap::App<'static, 'static> {
    let root_arg = || {
        Arg::with_name("root")
            .required(true)
            .takes_value(true)
            .help("The root hash to use")
    };
    let topic_arg = || {
        Arg::with_name("topic")
            .long("topic")
            .required(true)
            .takes_value(true)
            .help("The topic to send/recv data over")
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
    let verbose_arg = || {
        Arg::with_name("verbose")
            .short("v")
            .multiple(true)
            .help("Sets the level of verbosity")
    };
    App::new("banyan-cli")
        .version("0.1")
        .author("Rüdiger Klaehn")
        .about("CLI to work with large banyan trees on ipfs")
        .arg(index_pass_arg())
        .arg(value_pass_arg())
        .arg(verbose_arg())
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
                )
                .arg(
                    Arg::with_name("base")
                        .long("base")
                        .takes_value(true)
                        .help("Base on which to build"),
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
            SubCommand::with_name("send_stream")
                .about("Send a stream")
                .arg(topic_arg()),
        )
        .subcommand(
            SubCommand::with_name("recv_stream")
                .about("Receive a stream")
                .arg(topic_arg()),
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
        .subcommand(
            SubCommand::with_name("bench").about("Benchmark").arg(
                Arg::with_name("count")
                    .long("count")
                    .required(true)
                    .takes_value(true)
                    .help("The number of values per batch"),
            ),
        )
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

async fn build_tree(
    forest: Arc<Forest<TT, String>>,
    base: Option<Cid>,
    batches: u64,
    count: u64,
    unbalanced: bool,
    print_every: u64,
) -> anyhow::Result<Tree<TT, String>> {
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
    let mut tree = match base {
        Some(root) => Tree::<TT, String>::from_link(root, forest.clone()).await?,
        None => Tree::<TT, String>::empty(forest.clone()),
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
            tree.extend_unpacked(v).await?;
            tree.assert_invariants().await?;
        } else {
            tree.extend(v).await?;
            tree.assert_invariants().await?;
        }
    }
    Ok(tree)
}

async fn bench_build(
    forest: Arc<Forest<TT, String>>,
    base: Option<Cid>,
    batches: u64,
    count: u64,
    unbalanced: bool,
) -> anyhow::Result<(Tree<TT, String>, std::time::Duration)> {
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
    let mut tree = match base {
        Some(root) => Tree::<TT, String>::from_link(root, forest.clone()).await?,
        None => Tree::<TT, String>::empty(forest.clone()),
    };
    let mut offset: u64 = 0;
    let data = (0..batches)
        .map(|_b| {
            let v = (0..count)
                .map(|_| {
                    let result = (
                        Key::single(offset, offset, tags_from_offset(offset)),
                        offset.to_string(),
                    );
                    offset += 1;
                    result
                })
                .collect::<Vec<_>>();
            v
        })
        .collect::<Vec<_>>();
    let t0 = std::time::Instant::now();
    for v in data {
        if unbalanced {
            tree.extend_unpacked(v).await?;
            tree.assert_invariants().await?;
        } else {
            tree.extend(v).await?;
            tree.assert_invariants().await?;
        }
    }
    let t1 = std::time::Instant::now();
    Ok((tree, t1 - t0))
}

#[tokio::main]
async fn main() -> Result<()> {
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
    let verbosity = matches.occurrences_of("verbose");
    let level = match verbosity {
        0 => Level::ERROR,
        1 => Level::INFO,
        2 => Level::DEBUG,
        _ => Level::TRACE,
    };
    tracing_subscriber::fmt().with_max_level(level).init();
    let mut config = Config::debug_fast();
    config.index_key = index_key;
    config.value_key = value_key;
    let forest = Arc::new(Forest::<TT, String>::new(store, config));
    if let Some(matches) = matches.subcommand_matches("dump") {
        let root = Cid::from_str(
            matches
                .value_of("root")
                .ok_or(anyhow!("root must be provided"))?,
        )?;
        let tree = Tree::<TT, String>::from_link(root, forest).await?;
        tree.dump().await?;
        return Ok(());
    } else if let Some(matches) = matches.subcommand_matches("stream") {
        let root = Cid::from_str(
            matches
                .value_of("root")
                .ok_or(anyhow!("root must be provided"))?,
        )?;
        let tree = Tree::<TT, String>::from_link(root, forest).await?;
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
        let base = matches.value_of("base").map(Cid::from_str).transpose()?;
        println!(
            "building a tree with {} batches of {} values, unbalanced: {}",
            batches, count, unbalanced
        );
        let tree = build_tree(forest.clone(), base, batches, count, unbalanced, 1000).await?;
        tree.dump().await?;
        let roots = tree.roots().await?;
        let levels = roots.iter().map(|x| x.level()).collect::<Vec<_>>();
        let tree2 = Tree::<TT, String>::from_roots(forest, roots).await?;
        println!("{:?}", tree);
        println!("{}", tree);
        println!("{:?}", levels);
        tree2.dump().await?;
    } else if let Some(matches) = matches.subcommand_matches("pack") {
        let root = Cid::from_str(
            matches
                .value_of("root")
                .ok_or(anyhow!("root must be provided"))?,
        )?;
        let mut tree = Tree::<TT, String>::from_link(root, forest).await?;
        tree.dump().await?;
        tree.pack().await?;
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
        let tree = Tree::<TT, String>::from_link(root, forest).await?;
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
        let mut tree = Tree::<TT, String>::from_link(root, forest).await?;
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
        let mut tree = Tree::<TT, String>::from_link(root, forest).await?;
        tree.retain(&OffsetRangeQuery::from(offset..)).await?;
        tree.dump().await?;
        println!("{:?}", tree);
    } else if let Some(matches) = matches.subcommand_matches("send_stream") {
        let topic = matches
            .value_of("topic")
            .ok_or(anyhow!("topic must be provided"))?;
        let mut ticks = tokio::time::interval(Duration::from_secs(1));
        let mut tree = Tree::<TT, String>::empty(forest);
        let mut offset = 0;
        while let Some(_) = ticks.next().await {
            let key = Key::single(offset, offset, tags_from_offset(offset));
            tree.extend_unpacked(Some((key, "xxx".into()))).await?;
            if tree.level() > 100 {
                println!("packing the tree");
                tree.pack().await?;
            }
            offset += 1;
            if let Some(cid) = tree.link() {
                println!("publishing {} to {}", cid, topic);
                pubsub_pub(topic, cid.to_string().as_bytes()).await?;
            }
        }
    } else if let Some(matches) = matches.subcommand_matches("recv_stream") {
        let topic = matches
            .value_of("topic")
            .ok_or(anyhow!("topic must be provided"))?;
        let stream = pubsub_sub(topic)?
            .map_err(anyhow::Error::new)
            .and_then(|data| future::ready(String::from_utf8(data).map_err(anyhow::Error::new)))
            .and_then(|data| future::ready(Cid::from_str(&data).map_err(anyhow::Error::new)));
        let cids = stream.filter_map(|x| future::ready(x.ok()));
        let mut stream = forest
            .stream_roots(AllQuery, cids.boxed_local())
            .boxed_local();
        while let Some(ev) = stream.next().await {
            println!("{:?}", ev);
        }
    } else if let Some(matches) = matches.subcommand_matches("bench") {
        let store = Arc::new(MemStore::new());
        let mut config = Config::debug_fast();
        config.index_key = index_key;
        config.value_key = value_key;
        let forest = Arc::new(Forest::<TT, String>::new(store, config));
        let _t0 = std::time::Instant::now();
        let base = None;
        let batches = 1;
        let count: u64 = matches
            .value_of("count")
            .ok_or(anyhow!("required arg count not provided"))?
            .parse()?;
        let unbalanced = false;
        let (tree, tcreate) = bench_build(forest.clone(), base, batches, count, unbalanced).await?;
        let t0 = std::time::Instant::now();
        let _values: Vec<_> = tree.collect().await?;
        let t1 = std::time::Instant::now();
        let tcollect = t1 - t0;
        let t0 = std::time::Instant::now();
        let tags = vec![Key::range(0, u64::max_value(), Tags::single("fizz"))];
        let query: Arc<dyn Query<TT>> = Arc::new(DnfQuery(tags));
        let values: Vec<_> = tree
            .clone()
            .stream_filtered_static(query)
            .map_ok(|(_, k, v)| (k, v))
            .collect::<Vec<_>>()
            .await;
        println!("{}", values.len());
        let t1 = std::time::Instant::now();
        let tfilter_common = t1 - t0;
        let t0 = std::time::Instant::now();
        let tags = vec![Key::range(0, count / 10, Tags::single("fizzbuzz"))];
        let query: Arc<dyn Query<TT>> = Arc::new(DnfQuery(tags));
        let values: Vec<_> = tree
            .stream_filtered_static(query)
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
    } else {
        app().print_long_help()?;
        println!();
    }
    Ok(())
}
