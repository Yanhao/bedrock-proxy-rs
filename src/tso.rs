use std::sync::atomic;

use anyhow::Result;
use arc_swap::ArcSwapOption;
use once_cell::sync::Lazy;
use tonic::transport::Channel;

use idl_gen::metaserver::{meta_service_client::MetaServiceClient, AllocateTxidsRequest};

pub static TSO: Lazy<ArcSwapOption<Tso>> = Lazy::new(|| None.into());

pub struct Tso {
    ms_client: MetaServiceClient<Channel>,

    mtx: tokio::sync::Mutex<()>,
    current: atomic::AtomicU64,
    limit: atomic::AtomicU64,
}

impl Tso {
    pub async fn new() -> Result<Self> {
        let ms_client = MetaServiceClient::connect("").await?;

        Ok(Self {
            ms_client,

            mtx: tokio::sync::Mutex::new(()),
            current: 0.into(),
            limit: 0.into(),
        })
    }

    pub async fn allocate_txid(&self) -> Result<u64> {
        let _guard = self.mtx.lock().await;

        if self.current.load(atomic::Ordering::Relaxed) < self.limit.load(atomic::Ordering::Relaxed)
        {
            return Ok(self.current.fetch_add(1, atomic::Ordering::Relaxed) + 1);
        }

        let mut ms_client = self.ms_client.clone();
        let resp = ms_client
            .allocate_txids(AllocateTxidsRequest { count: 10000 })
            .await?
            .into_inner();

        let limit = *resp.txids.last().unwrap();
        self.limit.store(limit, atomic::Ordering::Relaxed);

        Ok(self.current.fetch_add(1, atomic::Ordering::Relaxed) + 1)
    }
}
