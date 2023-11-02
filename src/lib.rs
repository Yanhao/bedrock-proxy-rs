#![feature(result_option_inspect)]
#![feature(impl_trait_in_assoc_type)]
#![feature(async_closure)]
#![feature(pattern)]

use tracing::info;

pub mod config;
pub mod ds_client;
pub mod handler;
pub mod ms_client;
pub mod shard_range;
pub mod tso;
pub mod utils;

use utils::A;

pub async fn start_background_tasks() {
    info!("startting background tasks ...");

    shard_range::SHARD_RANGE.s({
        let mut shard_ranger = shard_range::ShardRangeCache::new().await.unwrap();
        shard_ranger.start().await.unwrap();
        shard_ranger
    });

    tso::TSO.s(tso::Tso::new());

    info!("background tasks start finished");
}
