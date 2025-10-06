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
use ds_client::DS_CLIENTS;
use ms_client::MS_CLIENT;
use tracing::info;

pub async fn start_background_tasks() -> Result<()> {
    info!("starting background tasks ...");

    _ = *MS_CLIENT;
    _ = *DS_CLIENTS;

    info!("background tasks start finished");
    Ok(())
}
