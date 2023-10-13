use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwapOption;
use once_cell::sync::Lazy;
use tokio::{select, sync::mpsc, time::MissedTickBehavior};
use tonic::transport::Channel;

use crate::utils::{A, R};

use idl_gen::metaserver::{
    meta_service_client::MetaServiceClient, AllocateTxidsRequest, AllocateTxidsResponse,
    GetShardRouteRequest, GetShardRouteResponse, InfoRequest, ScanShardRangeRequest,
    ScanShardRangeResponse,
};

pub static MS_CLIENT: Lazy<MsClient> = Lazy::new(|| MsClient::new());

pub struct MsClient {
    leader_conn: Arc<ArcSwapOption<(SocketAddr, MetaServiceClient<Channel>)>>,
    follower_conns: Arc<parking_lot::RwLock<HashMap<SocketAddr, MetaServiceClient<Channel>>>>,

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
        leader_conn: Arc<ArcSwapOption<(SocketAddr, MetaServiceClient<Channel>)>>,
        follower_conns: Arc<parking_lot::RwLock<HashMap<SocketAddr, MetaServiceClient<Channel>>>>,
    ) -> Result<()> {
        let (addr, mut conn) = (*leader_conn.load().r().clone()).clone();
        let resp = conn.info(InfoRequest {}).await?.into_inner();

        let leader_addr = resp.leader_addr.parse().unwrap();
        if leader_addr != addr {
            let conn = MetaServiceClient::connect(resp.leader_addr).await?;
            leader_conn.s((leader_addr, conn));
        }

        let mut new_follower_conns = follower_conns
            .read()
            .clone()
            .into_iter()
            .filter(|(addr, _)| resp.follower_addrs.contains(&addr.to_string()))
            .collect::<HashMap<_, _>>();

        for addr in resp.follower_addrs.into_iter() {
            let socket = addr.parse().unwrap();
            if !new_follower_conns.contains_key(&socket) {
                new_follower_conns.insert(socket, MetaServiceClient::connect(addr).await?);
            }
        }

        *follower_conns.write() = new_follower_conns;

        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        let leader_conn = self.leader_conn.clone();
        let follower_conns = self.follower_conns.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

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

    pub async fn get_shard_route(
        &self,
        req: GetShardRouteRequest,
    ) -> Result<GetShardRouteResponse> {
        let (_, mut cli) = (*self.leader_conn.load().r().clone()).clone();

        Ok(cli.get_shard_route(req).await?.into_inner())
    }

    pub async fn allocate_txids(&self, req: AllocateTxidsRequest) -> Result<AllocateTxidsResponse> {
        let (_, mut cli) = (*self.leader_conn.load().r().clone()).clone();

        Ok(cli.allocate_txids(req).await?.into_inner())
    }
}
