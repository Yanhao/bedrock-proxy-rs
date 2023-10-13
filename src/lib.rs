#![feature(result_option_inspect)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(impl_trait_in_assoc_type)]
#![feature(async_closure)]

use tracing::info;

pub mod config;
pub mod ds_client;
pub mod handler;
pub mod ms_client;
pub mod shard_range;
pub mod shard_route;
pub mod tso;
pub mod tx_table;
pub mod utils;

use utils::A;

pub async fn start_background_tasks() {
    info!("startting background tasks ...");

    shard_route::SHARD_ROUTER.s({
        let mut shard_router = shard_route::ShardRouter::new().await.unwrap();
        shard_router.start().await.unwrap();
        shard_router
    });

    shard_range::SHARD_RANGE.s({
        let mut shard_ranger = shard_range::ShardRangeCache::new().await.unwrap();
        shard_ranger.start().await.unwrap();
        shard_ranger
    });

    tso::TSO.s(tso::Tso::new());

    info!("background tasks start finished");
}
