use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use chrono::prelude::*;
use tokio::sync::OnceCell;
use tokio::{select, sync::mpsc, time::MissedTickBehavior};
use tonic::transport::Channel;

use idl_gen::{
    dataserver::data_service_client::DataServiceClient,
    dataserver::{
        CommitTxRequest, CommitTxResponse, KvDelRequest, KvDelResponse, KvGetRequest,
        KvGetResponse, KvScanRequest, KvScanResponse, KvSetRequest, KvSetResponse,
        PrepareTxRequest, PrepareTxResponse,
    },
};

static DS_CLIENT: OnceCell<DsClient> = OnceCell::const_new();
pub async fn get_ds_client() -> &'static DsClient {
    DS_CLIENT
        .get_or_init(|| async {
            let mut ds_client = DsClient::new();
            ds_client.start().await.unwrap();

            ds_client
        })
        .await
}

#[derive(Clone)]
struct ClientWrapper {
    ds_client: DataServiceClient<Channel>,
    expire_at: DateTime<Utc>,
}

pub struct DsClient {
    conns: Arc<tokio::sync::RwLock<im::HashMap<SocketAddr, ClientWrapper>>>,

    stop_ch: Option<mpsc::Sender<()>>,
}

impl DsClient {
    pub fn new() -> Self {
        Self {
            conns: Arc::new(tokio::sync::RwLock::new(im::HashMap::new())),
            stop_ch: None,
        }
    }

    async fn check_expires(
        conns: Arc<tokio::sync::RwLock<im::HashMap<SocketAddr, ClientWrapper>>>,
    ) -> Result<()> {
        let conns_copy = conns.read().await.clone();

        for (addr, client) in conns_copy.iter() {
            if client.expire_at < Utc::now() {
                conns.write().await.remove(addr);
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        let conns = self.conns.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        let _ = Self::check_expires(conns.clone());
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

impl DsClient {
    async fn get_conn(&self, addr: &SocketAddr) -> Result<DataServiceClient<Channel>> {
        let mut g = self.conns.write().await;
        if let Some(cli) = g.get(addr) {
            return Ok(cli.ds_client.clone());
        }

        let cli = DataServiceClient::connect(addr.to_string()).await?;

        g.insert(
            addr.clone(),
            ClientWrapper {
                ds_client: cli.clone(),
                expire_at: Utc::now() + chrono::Duration::seconds(10),
            },
        );

        return Ok(cli);
    }

    pub async fn kv_get(&self, addr: &SocketAddr, req: KvGetRequest) -> Result<KvGetResponse> {
        let mut cli = self.get_conn(addr).await?;

        Ok(cli.kv_get(req).await?.into_inner())
    }

    pub async fn kv_set(&self, addr: &SocketAddr, req: KvSetRequest) -> Result<KvSetResponse> {
        let mut cli = self.get_conn(addr).await?;

        Ok(cli.kv_set(req).await?.into_inner())
    }

    pub async fn kv_del(&self, addr: &SocketAddr, req: KvDelRequest) -> Result<KvDelResponse> {
        let mut cli = self.get_conn(addr).await?;

        Ok(cli.kv_del(req).await?.into_inner())
    }

    pub async fn kv_scan(&self, addr: &SocketAddr, req: KvScanRequest) -> Result<KvScanResponse> {
        let mut cli = self.get_conn(addr).await?;

        Ok(cli.kv_scan(req).await?.into_inner())
    }

    pub async fn prepare_tx(
        &self,
        addr: &SocketAddr,
        req: PrepareTxRequest,
    ) -> Result<PrepareTxResponse> {
        let mut cli = self.get_conn(addr).await?;

        Ok(cli.prepare_tx(req).await?.into_inner())
    }

    pub async fn commit_tx(
        &self,
        addr: &SocketAddr,
        req: CommitTxRequest,
    ) -> Result<CommitTxResponse> {
        let mut cli = self.get_conn(addr).await?;

        Ok(cli.commit_tx(req).await?.into_inner())
    }
}
