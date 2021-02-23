//! helper methods for the tests
use libipld::{
    cbor::DagCborCodec,
    codec::{Decode, Encode},
    Cid,
};
use sha2::{Digest, Sha256};
use std::{
    convert::{TryFrom, TryInto},
    fmt,
    io::{Read, Seek, Write},
};

/// For tests, we use a Sha2-256 digest as a link
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sha256Digest([u8; 32]);

impl Decode<DagCborCodec> for Sha256Digest {
    fn decode<R: Read + Seek>(c: DagCborCodec, r: &mut R) -> anyhow::Result<Self> {
        Self::try_from(libipld::Cid::decode(c, r)?)
    }
}
impl Encode<DagCborCodec> for Sha256Digest {
    fn encode<W: Write>(&self, c: DagCborCodec, w: &mut W) -> anyhow::Result<()> {
        libipld::Cid::encode(&Cid::from(*self), c, w)
    }
}

impl Sha256Digest {
    pub fn digest(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        Sha256Digest(result.try_into().unwrap())
    }
    pub fn read(data: &[u8]) -> anyhow::Result<Self> {
        Ok(Self(data[0..32].try_into()?))
    }
}

impl AsRef<[u8]> for Sha256Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.as_ref()))
    }
}

impl From<Sha256Digest> for Cid {
    fn from(value: Sha256Digest) -> Self {
        // https://github.com/multiformats/multicodec/blob/master/table.csv
        let mh = multihash::Multihash::wrap(0x12, &value.0).unwrap();
        Cid::new_v1(0x71, mh)
    }
}

impl TryFrom<Cid> for Sha256Digest {
    type Error = anyhow::Error;

    fn try_from(value: Cid) -> Result<Self, Self::Error> {
        anyhow::ensure!(value.codec() == 0x71, "Unexpected codec");
        anyhow::ensure!(value.hash().code() == 0x12, "Unexpected hash algorithm");
        let digest: [u8; 32] = value.hash().digest().try_into()?;
        Ok(Self(digest))
    }
}
