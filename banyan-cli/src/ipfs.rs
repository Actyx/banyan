//! helper methods to work with ipfs/ipld
use anyhow::{anyhow, Result};
use banyan::store::{BlockWriter, ReadOnlyStore};
use derive_more::{Display, From, FromStr, Into};
use futures::{future::BoxFuture, prelude::*};
use multihash::Sha2_256;
use reqwest::multipart::Part;
use serde::{
    de::IgnoredAny, de::Visitor, ser::SerializeStruct, Deserialize, Deserializer, Serialize,
    Serializer,
};
use serde_cbor::tags::Tagged;
use std::{
    collections::HashMap,
    convert::TryFrom,
    convert::TryInto,
    fmt, result,
    str::FromStr,
    sync::{Arc, RwLock},
};

use crate::tags::Sha256Digest;

#[derive(Clone, Hash, PartialEq, Eq, Display, From, Into, FromStr)]
pub struct Cid(cid::Cid);

impl Cid {
    pub fn new(data: &[u8], codec: cid::Codec) -> Self {
        Self(cid::Cid::new_v1(codec, Sha2_256::digest(data)))
    }
}

impl fmt::Debug for Cid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for Cid {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            let mut state = serializer.serialize_struct("", 1)?;
            state.serialize_field("/", &self.0.to_string())?;
            state.end()
        } else {
            Tagged::new(Some(42), &CidIo(self.0.clone())).serialize(serializer)
        }
    }
}

struct CidVisitor;

impl<'de> Visitor<'de> for CidVisitor {
    type Value = Cid;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("CID")
    }

    /// if we have serde_cbor in tag enabled mode, we will get a visit_newtype_struct when we hit a tag.
    /// we just ignore the tag and delegate to the inner serializer.
    ///
    /// Without this, it will fail as soon as it hits a tag!
    fn visit_newtype_struct<D: Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> result::Result<Self::Value, D::Error> {
        Self::Value::deserialize(deserializer)
    }

    // JSON variant
    fn visit_str<E>(self, string: &str) -> result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        cid::Cid::from_str(string)
            .map(Cid)
            .map_err(serde::de::Error::custom)
    }

    // CBOR variant
    fn visit_bytes<E>(self, value: &[u8]) -> result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // We need to drop the first byte, since it's just a 0 padding
        cid::Cid::try_from(&value[1..])
            .map(Cid)
            .map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for Cid {
    fn deserialize<D>(deserializer: D) -> result::Result<Cid, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            deserializer.deserialize_str(CidVisitor)
        } else {
            deserializer.deserialize_bytes(CidVisitor)
        }
    }
}

/// helper struct to serialize a cid in an dag-cbor compliant way
struct CidIo(cid::Cid);

impl From<CidIo> for Cid {
    fn from(value: CidIo) -> Self {
        Self(value.0)
    }
}

impl Serialize for CidIo {
    fn serialize<S: Serializer>(&self, serializer: S) -> result::Result<S::Ok, S::Error> {
        let mut bytes = self.0.to_bytes();
        // for some miraculous reason we need to pad with a single byte here to be compatible with go-ipfs
        bytes.insert(0, 0u8);
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for CidIo {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> result::Result<Self, D::Error> {
        struct CidVisitor;

        impl<'de> Visitor<'de> for CidVisitor {
            type Value = CidIo;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("CID")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // for some miraculous reason we need to strip a single byte here to be compatible with go-ipfs
                cid::Cid::try_from(&value[1..])
                    .map(CidIo)
                    .map_err(serde::de::Error::custom)
            }
        }
        deserializer.deserialize_any(CidVisitor)
    }
}

pub struct MemStore(Arc<RwLock<HashMap<Sha256Digest, Arc<[u8]>>>>);

impl MemStore {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
}

impl ReadOnlyStore<Sha256Digest> for MemStore {
    fn get(&self, link: &Sha256Digest) -> BoxFuture<Result<Arc<[u8]>>> {
        let x = self.0.as_ref().read().unwrap();
        if let Some(value) = x.get(link) {
            future::ok(value.clone()).boxed()
        } else {
            future::err(anyhow!("not there")).boxed()
        }
    }
}

impl BlockWriter<Sha256Digest> for MemStore {
    fn put(&self, data: &[u8], level: u32) -> BoxFuture<Result<Sha256Digest>> {
        let digest = Sha256Digest::new(data);
        self.0.as_ref().write().unwrap().insert(digest, data.into());
        future::ok(digest).boxed()
    }
}

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

fn format_codec(codec: cid::Codec) -> Result<&'static str> {
    match codec {
        cid::Codec::DagCBOR => Ok("cbor"),
        cid::Codec::DagProtobuf => Ok("protobuf"),
        cid::Codec::Raw => Ok("raw"),
        _ => Err(anyhow!("unsupported codec {:?}", codec)),
    }
}

pub(crate) async fn block_put(data: &[u8], codec: cid::Codec, pin: bool) -> Result<Cid> {
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
    let cid = cid::Cid::from_str(&res.key)?;
    Ok(cid.into())
}

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
    fn put(&self, data: &[u8], level: u32) -> BoxFuture<Result<Sha256Digest>> {
        let data = data.to_vec();
        async move {
            let cid = crate::ipfs::block_put(&data, cid::Codec::DagCBOR, false).await?;
            assert!(cid.0.hash().algorithm() == multihash::Code::Sha2_256);
            assert!(cid.0.hash().digest().len() == 32);
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
