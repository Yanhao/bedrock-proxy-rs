use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use arc_swap::ArcSwapOption;
use chrono::prelude::*;
use im::HashMap;
use itertools::Itertools;
use once_cell::sync::Lazy;
use tokio::{select, sync::mpsc, time::MissedTickBehavior};

use idl_gen::metaserver::GetShardRouteRequest;

use crate::ms_client::get_ms_client;

pub static SHARD_ROUTER: Lazy<ArcSwapOption<ShardRouter>> = Lazy::new(|| None.into());

type ShardID = u64;

#[derive(Clone, Debug)]
pub struct ShardRoute {
    pub(crate) shard_id: ShardID,
    #[allow(unused)]
    pub(crate) replicates: Vec<SocketAddr>,
    pub(crate) leader: SocketAddr,
    pub(crate) updated_at: DateTime<Utc>,
}

impl ShardRoute {
    fn is_expired(&self, expire_duration: chrono::Duration) -> bool {
        self.updated_at + expire_duration < Utc::now()
    }
}

pub struct ShardRouter {
    shard_cache: Arc<parking_lot::RwLock<im::HashMap<ShardID, ShardRoute>>>,

    sfg: async_singleflight::Group<ShardRoute, anyhow::Error>,
    expire_duration: chrono::Duration,

    stop_ch: Option<mpsc::Sender<()>>,
}

impl ShardRouter {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            shard_cache: Arc::new(parking_lot::RwLock::new(im::HashMap::new())),
            sfg: async_singleflight::Group::new(),
            expire_duration: chrono::Duration::seconds(5),
            stop_ch: None,
        })
    }

    pub async fn get_shard(&self, shard_id: ShardID) -> Result<ShardRoute> {
        if let Some(shard) = self.shard_cache.read().get(&shard_id) {
            return Ok(shard.clone());
        }

        let shard_cache = self.shard_cache.clone();

        let (value, err, _owner) = self
            .sfg
            .work(
                &shard_id.to_string(),
                (|| async move {
                    let a = get_ms_client()
                        .await
                        .get_shard_route(GetShardRouteRequest {
                            timestamp: Some(prost_types::Timestamp {
                                seconds: 0,
                                nanos: 0,
                            }),
                            shard_ids: vec![shard_id],
                        })
                        .await?;

                    let shard_record = a.routes.first().unwrap();
                    let shard = ShardRoute {
                        shard_id: shard_record.shard_id,
                        updated_at: Utc::now(),
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

    async fn update_expired(
        expire_duration: chrono::Duration,
        shard_cache: Arc<parking_lot::RwLock<im::HashMap<ShardID, ShardRoute>>>,
    ) {
        let cache_copy = shard_cache.read().clone();

        let outdated_routes = cache_copy
            .iter()
            .filter(|(_shard_id, shard)| shard.is_expired(expire_duration))
            .take(1000)
            .collect_vec();

        let minimum_ts = outdated_routes
            .iter()
            .map(|(_, r)| r.updated_at)
            .min()
            .unwrap_or(DateTime::from_timestamp(0, 0).unwrap());

        let shard_ids = outdated_routes
            .iter()
            .map(|(_, r)| r.shard_id)
            .collect_vec();

        let ts = Utc::now();

        let Ok(resp) = get_ms_client()
            .await
            .get_shard_route(GetShardRouteRequest {
                timestamp: Some(prost_types::Timestamp {
                    seconds: minimum_ts.second() as i64,
                    nanos: minimum_ts.nanosecond() as i32,
                }),
                shard_ids,
            })
            .await
        else {
            return;
        };

        let mut updated_routes = outdated_routes
            .into_iter()
            .map(|(shard_id, shard)| {
                (*shard_id, {
                    let mut a = shard.clone();
                    a.updated_at = ts;
                    a
                })
            })
            .collect::<HashMap<_, _>>();

        for r in resp.routes {
            updated_routes.insert(
                r.shard_id,
                ShardRoute {
                    shard_id: r.shard_id,
                    replicates: r.addrs.iter().map(|x| x.parse().unwrap()).collect_vec(),
                    leader: r.leader_addr.parse().unwrap(),
                    updated_at: ts,
                },
            );
        }

        for (shard_id, routes) in updated_routes.into_iter() {
            shard_cache.write().insert(shard_id, routes);
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        let shard_cache = self.shard_cache.clone();
        let expire_duration = self.expire_duration.clone();

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
                        Self::update_expired(
                            expire_duration.clone(),
                            shard_cache.clone()
                        ).await;
                    }
                }
            }
        });

        Ok(())
    }
}
