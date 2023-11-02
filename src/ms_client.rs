use std::collections::HashMap;
use std::str::pattern::Pattern;
use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwapOption;
use tokio::sync::OnceCell;
use tokio::{select, sync::mpsc, time::MissedTickBehavior};
use tonic::transport::Channel;

use crate::config;
use crate::utils::{A, R};

use idl_gen::metaserver::{
    meta_service_client::MetaServiceClient, AllocateTxidsRequest, AllocateTxidsResponse,
    InfoRequest, ScanShardRangeRequest, ScanShardRangeResponse,
};

static MS_CLIENT: OnceCell<MsClient> = OnceCell::const_new();

pub async fn get_ms_client() -> &'static MsClient {
    MS_CLIENT
        .get_or_init(|| async {
            let mut ms_client = MsClient::new();
            ms_client.start().await.unwrap();

            ms_client
        })
        .await
}

pub struct MsClient {
    leader_conn: Arc<ArcSwapOption<(String, MetaServiceClient<Channel>)>>,
    follower_conns: Arc<parking_lot::RwLock<HashMap<String, MetaServiceClient<Channel>>>>,

    stop_ch: Option<mpsc::Sender<()>>,
}

impl MsClient {
    pub fn new() -> Self {
        Self {
            leader_conn: Arc::new(None.into()),
            follower_conns: Arc::new(parking_lot::RwLock::new(HashMap::new())),

            stop_ch: None,
        }
    }

    async fn update_ms_conns(
        leader_conn: Arc<ArcSwapOption<(String, MetaServiceClient<Channel>)>>,
        follower_conns: Arc<parking_lot::RwLock<HashMap<String, MetaServiceClient<Channel>>>>,
    ) -> Result<()> {
        if leader_conn.load().is_none() {
            let ms_addr = config::get_config().metaserver_url.clone();
            let conn = MetaServiceClient::connect(ms_addr.clone()).await?;
            leader_conn.s((ms_addr, conn));

            return Ok(());
        }

        let (url, mut conn) = (*leader_conn.load().r().clone()).clone();
        let resp = conn.info(InfoRequest {}).await?.into_inner();

        let leader_url = if "http://".is_prefix_of(&resp.leader_addr) {
            resp.leader_addr.clone()
        } else {
            format!("http://{}", resp.leader_addr)
        };

        if leader_url != url {
            let conn = MetaServiceClient::connect(resp.leader_addr).await?;
            leader_conn.s((leader_url, conn));
        }

        let mut new_follower_conns = follower_conns
            .read()
            .clone()
            .into_iter()
            .filter(|(addr, _)| resp.follower_addrs.contains(&addr.to_string()))
            .collect::<HashMap<_, _>>();

        for url in resp.follower_addrs.into_iter() {
            let follower_url = if "http://".is_prefix_of(&url) {
                url.clone()
            } else {
                format!("http://{}", url)
            };
            if !new_follower_conns.contains_key(&follower_url) {
                new_follower_conns.insert(follower_url, MetaServiceClient::connect(url).await?);
            }
        }

        *follower_conns.write() = new_follower_conns;

        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        {
            let ms_addr = config::get_config().metaserver_url.clone();
            let conn = MetaServiceClient::connect(ms_addr.clone()).await?;
            self.leader_conn.s((ms_addr, conn));
        }

        let leader_conn = self.leader_conn.clone();
        let follower_conns = self.follower_conns.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await; // make sure ms_client init successful first

            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        let _ = Self::update_ms_conns(leader_conn.clone(), follower_conns.clone()).await;
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.send(()).await?;
        }

        Ok(())
    }
}

impl MsClient {
    pub async fn scan_shard_range(
        &self,
        req: ScanShardRangeRequest,
    ) -> Result<ScanShardRangeResponse> {
        let (_, mut cli) = (*self.leader_conn.load().r().clone()).clone();

        Ok(cli.scan_shard_range(req).await?.into_inner())
    }

    pub async fn allocate_txids(&self, req: AllocateTxidsRequest) -> Result<AllocateTxidsResponse> {
        let (_, mut cli) = (*self.leader_conn.load().r().clone()).clone();

        Ok(cli.allocate_txids(req).await?.into_inner())
    }
}
