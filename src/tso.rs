use std::sync::atomic;

use anyhow::Result;
use arc_swap::{access::Access, ArcSwapOption};
use once_cell::sync::Lazy;

use idl_gen::metaserver::AllocateTxidsRequest;
use tonic::Status;
use tracing::info;

use crate::{ms_client::get_ms_client, utils::R};

pub static TSO: Lazy<ArcSwapOption<Tso>> = Lazy::new(|| None.into());

pub struct Tso {
    mtx: tokio::sync::Mutex<()>,
    current: atomic::AtomicU64,
    limit: atomic::AtomicU64,
}

impl Tso {
    pub fn new() -> Self {
        Self {
            mtx: tokio::sync::Mutex::new(()),
            current: 0.into(),
            limit: 0.into(),
        }
    }

    pub async fn alloc_txid() -> std::result::Result<u64, tonic::Status> {
        Ok(TSO
            .load()
            .r()
            .do_allocate_txid()
            .await
            .map_err(|e| Status::internal(format!("allcoate txid failed, err: {e}")))?)
    }

    async fn do_allocate_txid(&self) -> Result<u64> {
        let _guard = self.mtx.lock().await;

        if self.current.load(atomic::Ordering::Relaxed) < self.limit.load(atomic::Ordering::Relaxed)
        {
            return Ok(self.current.fetch_add(1, atomic::Ordering::Relaxed) + 1);
        }

        let resp = get_ms_client()
            .await
            .allocate_txids(AllocateTxidsRequest { count: 10000 })
            .await?;

        info!(
            "allocated txids, range: [{}, {})",
            resp.txid_range_start, resp.txid_range_end
        );

        let limit = resp.txid_range_end;
        self.limit.store(limit, atomic::Ordering::Relaxed);

        Ok(self.current.fetch_add(1, atomic::Ordering::Relaxed) + 1)
    }
}
