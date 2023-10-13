use std::sync::atomic;

use anyhow::Result;
use arc_swap::ArcSwapOption;
use once_cell::sync::Lazy;

use idl_gen::metaserver::AllocateTxidsRequest;

use crate::ms_client::MS_CLIENT;

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

    pub async fn allocate_txid(&self) -> Result<u64> {
        let _guard = self.mtx.lock().await;

        if self.current.load(atomic::Ordering::Relaxed) < self.limit.load(atomic::Ordering::Relaxed)
        {
            return Ok(self.current.fetch_add(1, atomic::Ordering::Relaxed) + 1);
        }

        let resp = MS_CLIENT
            .allocate_txids(AllocateTxidsRequest { count: 10000 })
            .await?;

        let limit = *resp.txids.last().unwrap();
        self.limit.store(limit, atomic::Ordering::Relaxed);

        Ok(self.current.fetch_add(1, atomic::Ordering::Relaxed) + 1)
    }
}
