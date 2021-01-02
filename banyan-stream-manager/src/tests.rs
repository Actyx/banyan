use std::{collections::BTreeMap, convert::TryInto};

use crate::{
    sqlite::SqliteStore,
    tag_index::{Tag, TagSet},
    tags::Key,
    Config, Link, StreamManager,
};
use futures::prelude::*;
use libipld::Cid;
use std::str::FromStr;

struct Tagger(BTreeMap<&'static str, Tag>);

impl Tagger {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn tag(&mut self, name: &'static str) -> Tag {
        self.0.entry(name).or_insert_with(|| name.into()).clone()
    }

    pub fn tags(&mut self, names: &[&'static str]) -> TagSet {
        names
            .into_iter()
            .map(|name| self.tag(name))
            .collect::<TagSet>()
    }
}

fn cids_to_string(cids: Vec<Cid>) -> String {
    cids.iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[tokio::test]
async fn smoke() -> anyhow::Result<()> {
    let mut tagger = Tagger::new();
    let ev = (
        Key::single(0, 0, tagger.tags(&["tag1"])),
        serde_cbor::Value::Null,
    );
    let stream_nr = 1;
    let other_stream_id = "other1".try_into()?;
    let store = SqliteStore::memory()?;
    let (mgr, task, publish, wanted) = StreamManager::new(store.clone(), Config::test());
    tokio::task::spawn(task);
    mgr.append(stream_nr, vec![ev.clone()]).await?;
    mgr.append(stream_nr, vec![ev]).await?;
    mgr.pack(stream_nr).await?;
    let pu = publish.take(3).collect::<Vec<_>>().await;
    println!("{:?}", pu);
    println!("{:?}", cids_to_string(store.lock().get_known_cids()?));

    mgr.update_root(
        other_stream_id,
        Link::from_str("bafyreifas2ujpq77aiqhgnixkhs7m5bb2ubv7axgtcicdjwonfhlias6au")?,
    )?;
    let wu = wanted.take(1).collect::<Vec<_>>().await;
    println!("{:?}", wu);
    Ok(())
}
