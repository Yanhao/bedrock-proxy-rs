use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use arc_swap::ArcSwapOption;
use chrono::prelude::*;
use once_cell::sync::Lazy;
use tokio::{select, sync::mpsc, time::MissedTickBehavior};

use idl_gen::metaserver::{meta_service_client::MetaServiceClient, GetShardRouteRequest};
use tonic::transport::Channel;

pub static SHARD_ROUTER: Lazy<ArcSwapOption<ShardRouter>> = Lazy::new(|| None.into());

#[derive(Clone)]
pub struct Shard {
    pub(crate) shard_id: ShardID,
    #[allow(unused)]
    pub(crate) replicates: Vec<SocketAddr>,
    pub(crate) leader: SocketAddr,
    expire_at: DateTime<Utc>,
}

impl Shard {
    fn is_expired(&self) -> bool {
        self.expire_at < Utc::now()
    }
}

type ShardID = u64;

pub struct ShardRouter {
    shard_cache: Arc<parking_lot::RwLock<im::HashMap<ShardID, Shard>>>,

    sfg: async_singleflight::Group<Shard, anyhow::Error>,
    ms_client: MetaServiceClient<Channel>,
    expire_duration: chrono::Duration,

    stop_ch: Option<mpsc::Sender<()>>,
}

impl ShardRouter {
    pub async fn new() -> Result<Self> {
        let ms_client = MetaServiceClient::connect("").await?;

        Ok(Self {
            shard_cache: Arc::new(parking_lot::RwLock::new(im::HashMap::new())),
            sfg: async_singleflight::Group::new(),
            ms_client,
            expire_duration: chrono::Duration::seconds(5),
            stop_ch: None,
        })
    }

    pub async fn get_shard(&self, shard_id: ShardID) -> Result<Shard> {
        if let Some(shard) = self.shard_cache.read().get(&shard_id) {
            return Ok(shard.clone());
        }

        let mut ms_client = self.ms_client.clone();
        let expire_duration = self.expire_duration.clone();
        let shard_cache = self.shard_cache.clone();

        let (value, err, _owner) = self
            .sfg
            .work(
                &shard_id.to_string(),
                (|| async move {
                    let a = ms_client
                        .get_shard_route(GetShardRouteRequest {
                            timestamp: Some(prost_types::Timestamp {
                                seconds: 0,
                                nanos: 0,
                            }),
                            shard_ids: vec![],
                        })
                        .await?
                        .into_inner();

                    let shard_record = a.routes.first().unwrap();
                    let shard = Shard {
                        shard_id: shard_record.shard_id,
                        expire_at: Utc::now() + expire_duration,
                        leader: shard_record.leader_addr.parse().unwrap(),
                        replicates: shard_record
                            .addrs
                            .iter()
                            .map(|x| x.parse().unwrap())
                            .collect::<Vec<_>>(),
                    };

                    shard_cache.write().insert(shard_id, shard.clone());

                    Ok(shard)
                })(),
            )
            .await;

        if let Some(err) = err {
            return Err(err);
        }

        return Ok(value.unwrap());
    }

    fn check_expired(shard_cache: Arc<parking_lot::RwLock<im::HashMap<ShardID, Shard>>>) {
        let cache_copy = shard_cache.read().clone();

        for (shard_id, shard) in cache_copy.iter() {
            if shard.is_expired() {
                shard_cache.write().remove(shard_id);
            }
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        let shard_cache = self.shard_cache.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(60));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        Self::check_expired(shard_cache.clone());
                    }
                }
            }
        });

        Ok(())
    }
}
