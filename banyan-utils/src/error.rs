#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Banyan(#[from] banyan::error::Error),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error("unsupported codec {}", .0)]
    UnsupportedCodec(u64),

    #[error("join error")]
    JoinError,
}
