use lazy_static::lazy_static;
use prometheus::{exponential_buckets, Histogram, HistogramOpts, Registry};

lazy_static! {
    pub static ref LEAF_LOAD_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("leaf_load_time", "Complete time to load leafs",)
            .namespace("banyan")
            .buckets(exponential_buckets(0.00001, 2.0, 17).unwrap()),
    )
    .unwrap();
    pub static ref BRANCH_LOAD_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("branch_load_time", "Complete time to load branches",)
            .namespace("banyan")
            .buckets(exponential_buckets(0.00001, 2.0, 17).unwrap()),
    )
    .unwrap();
    pub static ref LEAF_STORE_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("leaf_store_time", "Complete time to store leafs",)
            .namespace("banyan")
            .buckets(exponential_buckets(0.00001, 2.0, 17).unwrap()),
    )
    .unwrap();
    pub static ref BRANCH_STORE_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("branch_store_time", "Complete time to store branches",)
            .namespace("banyan")
            .buckets(exponential_buckets(0.00001, 2.0, 17).unwrap()),
    )
    .unwrap();
    pub static ref BLOCK_PUT_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("block_put_time", "Complete time spent in block put",)
            .namespace("banyan")
            .buckets(exponential_buckets(0.00001, 2.0, 17).unwrap()),
    )
    .unwrap();
    pub static ref BLOCK_GET_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("block_get_time", "Complete time spent in block get",)
            .namespace("banyan")
            .buckets(exponential_buckets(0.00001, 2.0, 17).unwrap()),
    )
    .unwrap();
    pub static ref BLOCK_PUT_SIZE_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("block_put_size", "Size of blocks being written",)
            .namespace("banyan")
            .buckets(exponential_buckets(64.0, 2.0, 16).unwrap()),
    )
    .unwrap();
    pub static ref BLOCK_GET_SIZE_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("block_get_size", "Size of blocks being read",)
            .namespace("banyan")
            .buckets(exponential_buckets(64.0, 2.0, 16).unwrap()),
    )
    .unwrap();
}

pub(crate) fn register(registry: &Registry) -> anyhow::Result<()> {
    registry.register(Box::new(LEAF_LOAD_HIST.clone()))?;
    registry.register(Box::new(BRANCH_LOAD_HIST.clone()))?;
    registry.register(Box::new(LEAF_STORE_HIST.clone()))?;
    registry.register(Box::new(BRANCH_STORE_HIST.clone()))?;
    registry.register(Box::new(BLOCK_PUT_HIST.clone()))?;
    registry.register(Box::new(BLOCK_GET_HIST.clone()))?;
    registry.register(Box::new(BLOCK_PUT_SIZE_HIST.clone()))?;
    registry.register(Box::new(BLOCK_GET_SIZE_HIST.clone()))?;
    Ok(())
}
