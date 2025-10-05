#![feature(impl_trait_in_assoc_type)]
#![feature(pattern)]

pub mod config;
pub mod ds_client;
pub mod handler;
pub mod ms_client;
pub mod route_manager;
pub mod shard_route;
pub mod tso;
pub mod utils;

use anyhow::Result;
use tracing::info;

pub async fn start_background_tasks() -> Result<()> {
    info!("startting background tasks ...");

    info!("background tasks start finished");
    Ok(())
}
