use std::sync::atomic;

use anyhow::Result;
use once_cell::sync::Lazy;
use tonic::Status;
use tracing::info;

use idl_gen::metaserver::AllocateTxidsRequest;

use crate::ms_client::get_ms_client;

pub static TSO: Lazy<Tso> = Lazy::new(Tso::new);

pub struct Tso {
    mtx: tokio::sync::Mutex<()>,
    current: atomic::AtomicU64,
    limit: atomic::AtomicU64,
}

impl Default for Tso {
    fn default() -> Self {
        Self::new()
    }
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
        TSO.do_allocate_txid()
            .await
            .map_err(|e| Status::internal(format!("allcoate txid failed, err: {e}")))
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
