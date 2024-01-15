use std::{net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Result};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use chrono::prelude::*;
use once_cell::sync::Lazy;
use rand::seq::SliceRandom;
use tokio::{select, sync::mpsc, time::MissedTickBehavior};
use tracing::{debug, error, info};

use idl_gen::metaserver::ScanShardRangeRequest;

use crate::{config::get_config, ms_client::get_ms_client};

pub static SHARD_RANGE: Lazy<ArcSwapOption<ShardRangeCache>> = Lazy::new(|| None.into());

pub const MAX_KEY: [u8; 128] = [0xff; 128];

#[derive(Clone)]
pub struct ShardRange {
    pub(crate) shard_id: u64,
    range_start: Vec<u8>,
    range_end: Vec<u8>,
    pub(crate) replicates: Vec<SocketAddr>,
    pub(crate) leader: SocketAddr,
    update_at: DateTime<Utc>,
}

impl ShardRange {
    pub fn select_address(&self, need_leader: bool) -> Option<SocketAddr> {
        if need_leader {
            return Some(self.leader.clone());
        }

        let mut rng = rand::thread_rng();
        Some(self.replicates.choose(&mut rng)?.to_owned())
    }
}

impl std::fmt::Debug for ShardRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        s.push_str("Shard: id: ");
        s.push_str(&format!("0x{:016x}", self.shard_id));
        s.push_str(", range: [");
        s.push_str(&hex::encode(&self.range_start));
        s.push_str(", ");
        s.push_str(&hex::encode(&self.range_end));
        s.push_str("), ");
        s.push_str(&format!(
            "replicates: {:?}, leader: {}, update_at: {}",
            self.replicates, self.leader, self.update_at
        ));

        f.write_str(&s)
    }
}

pub struct ShardRangeCache {
    shard_ranges: Arc<parking_lot::RwLock<im::OrdMap<Bytes, ShardRange>>>,
    stop_ch: Option<mpsc::Sender<()>>,
}

impl ShardRangeCache {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            shard_ranges: Arc::new(parking_lot::RwLock::new(im::OrdMap::new())),
            stop_ch: None,
        })
    }

    pub async fn get_shard_range(&self, key: Bytes, sync: bool) -> Result<ShardRange> {
        if sync {
            self.update_partial_ranges(
                key.clone(),
                Bytes::from(MAX_KEY.to_vec()),
                Some(get_config().update_range_count),
            )
            .await?;
        }

        let guard = self.shard_ranges.read();
        let (_, sr) = guard
            .get_prev(key.as_ref())
            .ok_or(anyhow!("get prev failed"))?;

        Ok(sr.clone())
    }

    pub fn scan_shard_range(&self, start: Bytes, end: Bytes) -> Result<Vec<ShardRange>> {
        let ranges_copy = self.shard_ranges.read().clone();
        let mut r = ranges_copy
            .range(start.clone()..end)
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();

        if Bytes::from(r.first().unwrap().range_start.clone()) != start {
            r.insert(0, ranges_copy.get_prev(&start).unwrap().1.clone());
        }

        Ok(r)
    }

    pub async fn update_partial_ranges(
        &self,
        start: Bytes,
        end: Bytes,
        max_count: Option<u32>,
    ) -> Result<()> {
        Self::update_ranges(self.shard_ranges.clone(), start, end, max_count).await?;

        Ok(())
    }

    async fn update_ranges(
        shard_ranges: Arc<parking_lot::RwLock<im::OrdMap<Bytes, ShardRange>>>,
        range_start: Bytes,
        range_end: Bytes,
        max_count: Option<u32>,
    ) -> Result<()> {
        let mut range_start: Vec<u8> = range_start.into();

        let (max_count, mut count) = (max_count.unwrap_or(std::u32::MAX), 0);

        loop {
            let resp = get_ms_client()
                .await
                .scan_shard_range(ScanShardRangeRequest {
                    storage_id: get_config().storage_id,
                    range_start: range_start.clone(),
                    range_count: get_config().update_range_count,
                })
                .await?;

            if resp.is_end {
                break;
            }

            let ranges_copy = shard_ranges.write().clone();

            let (first_key, last_key) = (
                Bytes::from(resp.ranges.first().unwrap().range_start.clone()),
                Bytes::from(resp.ranges.last().unwrap().range_end.clone()),
            );

            let ranges_to_remove = {
                let mut r = ranges_copy
                    .range(first_key.clone()..last_key.clone())
                    .map(|(range_start, _)| range_start.clone())
                    .collect::<Vec<_>>();
                if r.is_empty() {
                    vec![]
                } else {
                    if first_key != *(r.first().unwrap()) {
                        if let Some((range_start, _)) = ranges_copy.get_prev(&first_key) {
                            r.insert(0, range_start.clone());
                        }
                    }
                    r
                }
            };

            for range_key in ranges_to_remove.iter() {
                shard_ranges.write().remove(range_key);
            }

            for sr in resp.ranges.iter() {
                shard_ranges.write().insert(
                    Bytes::from(sr.range_start.clone()),
                    ShardRange {
                        shard_id: sr.shard_id,
                        range_start: sr.range_start.clone(),
                        range_end: sr.range_end.clone(),
                        leader: sr.leader_addr.parse().unwrap(),
                        replicates: sr
                            .addrs
                            .iter()
                            .map(|x| x.parse().unwrap())
                            .collect::<Vec<_>>(),
                        update_at: Utc::now(),
                    },
                );
            }

            range_start = last_key.into();

            if range_start >= range_end {
                break;
            }

            count += get_config().update_range_count;
            if count >= max_count {
                break;
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        let shard_ranges = self.shard_ranges.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(1));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await; // make sure ms_client init successful first

            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        match Self::update_ranges(shard_ranges.clone(), Bytes::new(), Bytes::new(), None).await {
                            Err(e) => error!("update ranges failed, err: {e}"),
                            Ok(_) => debug!("update ranges successfully"),
                        };
                    }
                }
            }

            info!("shard router stopped ...");
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
