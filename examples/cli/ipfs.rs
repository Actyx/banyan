//! helper methods to work with ipfs/ipld
use anyhow::anyhow;
use anyhow::Result;
use banyan::ipfs::Cid;
use banyan::store::Store;
use futures::{future::BoxFuture, prelude::*};
use reqwest::multipart::Part;
use serde::Deserialize;
use std::{str::FromStr, sync::Arc};

pub(crate) async fn block_get(key: &Cid) -> Result<Arc<[u8]>> {
    let url = format!("http://localhost:5001/api/v0/block/get?arg={}", key);
    let data: Vec<u8> = reqwest::get(url.as_str()).await?.bytes().await?.to_vec();
    Ok(data.into())
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
    let url = format!(
        "http://localhost:5001/api/v0/block/put?format={}&pin={}",
        format_codec(codec)?,
        pin
    );
    let client = reqwest::Client::new();
    let form = reqwest::multipart::Form::new().part("file", Part::bytes(data.to_vec()));
    let res: IpfsBlockPutResponseIo = client
        .post(url.as_str())
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

impl Store for IpfsStore {
    fn put(&self, data: &[u8], codec: cid::Codec) -> BoxFuture<Result<Cid>> {
        let data = data.to_vec();
        async move { crate::ipfs::block_put(&data, codec, false).await }.boxed()
    }

    fn get(&self, cid: &Cid) -> BoxFuture<Result<Arc<[u8]>>> {
        let cid = cid.clone();
        async move { crate::ipfs::block_get(&cid).await }.boxed()
    }
}

#[derive(Deserialize)]
struct IpfsBlockPutResponseIo {
    #[serde(rename = "Key")]
    key: String,
}
