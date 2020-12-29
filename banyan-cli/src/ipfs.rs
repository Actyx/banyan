//! helper methods to work with ipfs/ipld
use anyhow::{anyhow, Result};
use banyan::store::{BlockWriter, ReadOnlyStore};
use derive_more::{Display, From, FromStr, Into};
use futures::{future::BoxFuture, prelude::*};
use libipld::Cid;
use multihash::Sha2_256;
use reqwest::multipart::Part;
use serde::{
    de::IgnoredAny, de::Visitor, ser::SerializeStruct, Deserialize, Deserializer, Serialize,
    Serializer,
};
use serde_cbor::tags::Tagged;
use std::{convert::TryFrom, convert::TryInto, fmt, result, str::FromStr, sync::Arc};

use crate::tags::Sha256Digest;

pub(crate) async fn block_get(key: &Cid) -> Result<Arc<[u8]>> {
    let url = reqwest::Url::parse_with_params(
        "http://localhost:5001/api/v0/block/get",
        &[("arg", format!("{}", key))],
    )?;
    let client = reqwest::Client::new();
    let data: Vec<u8> = client.post(url).send().await?.bytes().await?.to_vec();
    Ok(data.into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Base64Blob(pub Vec<u8>);

impl Serialize for Base64Blob {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(base64::encode(&self.0).as_ref())
    }
}

impl<'de> Deserialize<'de> for Base64Blob {
    fn deserialize<D>(deserializer: D) -> Result<Base64Blob, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MyVisitor();

        impl<'de> Visitor<'de> for MyVisitor {
            type Value = Base64Blob;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                base64::decode(v).map(Base64Blob).map_err(|err| {
                    serde::de::Error::custom(format!("Error decoding base64 string: {}", err))
                })
            }
        }

        deserializer.deserialize_any(MyVisitor())
    }
}

#[derive(Debug, Clone, Deserialize)]
struct IpfsPubsubEventIo {
    // We only need this field, skip deserializing everything else. We still keep the rest of the
    // fields to ensure that we are parsing the right thing.
    data: Base64Blob,
    #[serde(rename = "from")]
    _from: IgnoredAny,
    #[serde(rename = "seqno")]
    _seqno: IgnoredAny,
    #[serde(rename = "topicIDs")]
    _topic_ids: IgnoredAny,
}

pub(crate) fn pubsub_sub(topic: &str) -> Result<impl Stream<Item = reqwest::Result<Vec<u8>>>> {
    let url = reqwest::Url::parse_with_params(
        "http://localhost:5001/api/v0/pubsub/sub",
        &[("arg", topic)],
    )?;
    let client = reqwest::Client::new();
    let data = client
        .post(url)
        .send()
        .map(|response| match response {
            Ok(response) => response
                .bytes_stream()
                .map_ok(|bytes| {
                    let data: IpfsPubsubEventIo = serde_json::from_slice(&bytes).unwrap();
                    data.data.0
                })
                .left_stream(),
            Err(cause) => futures::stream::once(future::err(cause)).right_stream(),
        })
        .flatten_stream();
    Ok(data)
}

pub(crate) async fn pubsub_pub(topic: &str, data: &[u8]) -> Result<()> {
    use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
    let topic = percent_encode(&topic.as_bytes(), NON_ALPHANUMERIC).to_string();
    let data = percent_encode(&data, NON_ALPHANUMERIC).to_string();
    let url = reqwest::Url::parse(&format!(
        "http://localhost:5001/api/v0/pubsub/pub?arg={}&arg={}",
        topic, data
    ))?;
    let client = reqwest::Client::new();
    let _ = client.post(url).send().await?.bytes().await?.to_vec();
    Ok(())
}

fn format_codec(codec: u64) -> Result<&'static str> {
    match codec {
        0x71 => Ok("cbor"),
        0x70 => Ok("protobuf"),
        0x55 => Ok("raw"),
        _ => Err(anyhow!("unsupported codec {}", codec)),
    }
}

pub(crate) async fn block_put(data: &[u8], codec: u64, pin: bool) -> Result<Cid> {
    let url = reqwest::Url::parse_with_params(
        "http://localhost:5001/api/v0/block/put",
        &[("format", format_codec(codec)?), ("pin", &pin.to_string())],
    )?;
    let client = reqwest::Client::new();
    let form = reqwest::multipart::Form::new().part("file", Part::bytes(data.to_vec()));
    let res: IpfsBlockPutResponseIo = client
        .post(url)
        .multipart(form)
        .send()
        .await?
        .json()
        .await?;
    let cid = Cid::from_str(&res.key)?;
    Ok(cid)
}

#[derive(Clone)]
pub struct IpfsStore {}

impl IpfsStore {
    pub fn new() -> Self {
        Self {}
    }
}

impl ReadOnlyStore<Sha256Digest> for IpfsStore {
    fn get(&self, link: &Sha256Digest) -> BoxFuture<Result<Arc<[u8]>>> {
        let cid: Cid = (*link).into();
        async move { crate::ipfs::block_get(&cid).await }.boxed()
    }
}

impl BlockWriter<Sha256Digest> for IpfsStore {
    fn put(&self, data: &[u8]) -> BoxFuture<Result<Sha256Digest>> {
        let data = data.to_vec();
        async move {
            let cid = crate::ipfs::block_put(&data, 0x71, false).await?;
            assert!(cid.hash().code() == 0x12);
            assert!(cid.hash().digest().len() == 32);
            cid.try_into()
        }
        .boxed()
    }
}

#[derive(Deserialize)]
struct IpfsBlockPutResponseIo {
    #[serde(rename = "Key")]
    key: String,
}
