#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Tree must not be empty")]
    TreeMustNotBeEmpty,

    #[error("Index out of bounds: {}, length: {}", .tried, .length)]
    IndexOutOfBounds { length: usize, tried: u64 },

    #[error("must have more than 1 element when extending")]
    MustHaveMoreThanOneElement,

    #[error("Invalid: {}", .0)]
    Invalid(&'static str),

    #[error("Single item too large")]
    ItemTooLarge,

    #[error("Found purged data")]
    FoundPurgedData,

    #[error("Max size exceeded")]
    MaxSizeExceeded,

    #[error("Not there")]
    NotThere,

    #[error("Multiple strong references")]
    MultipleStrongRef,

    #[error("seek offset wraparound")]
    SeekOffsetWraparound,

    #[error("expected ipld bytes")]
    ExpectedIpldBytes,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Ipld(#[from] libipld::error::Error),

    #[error(transparent)]
    CBorParse(#[from] cbor_data::ParseError),

    #[error(transparent)]
    CBorCodec(#[from] cbor_data::codec::CodecError),

    #[error(transparent)]
    Cid(#[from] cid::Error),

    #[error(transparent)]
    FromInt(#[from] std::num::TryFromIntError),

    #[cfg(feature = "metrics")]
    #[error(transparent)]
    Prometheus(#[from] prometheus::Error),
}
