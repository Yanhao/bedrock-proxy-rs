use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use chrono::prelude::*;
use rand::seq::SliceRandom;
use tokio::{select, sync::mpsc, time::MissedTickBehavior};
use tracing::{error, info};

use idl_gen::metaserver::ScanShardRangeRequest;

use crate::{config::get_config, ms_client::MS_CLIENT};

pub const MAX_KEY: [u8; 128] = [0xff; 128];

#[derive(Clone)]
pub struct ShardInfo {
    pub(crate) shard_id: u64,
    range_start: Vec<u8>,
    range_end: Vec<u8>,
    pub(crate) replicates: Vec<SocketAddr>,
    pub(crate) leader: SocketAddr,
    update_at: DateTime<Utc>,
}

impl ShardInfo {
    pub fn select_address(&self, need_leader: bool) -> Option<SocketAddr> {
        if need_leader {
            return Some(self.leader);
        }

        let mut rng = rand::thread_rng();
        Some(self.replicates.choose(&mut rng)?.to_owned())
    }
}

impl std::fmt::Debug for ShardInfo {
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

pub struct ShardRouter {
    shard_ranges: Arc<parking_lot::RwLock<im::OrdMap<Bytes, ShardInfo>>>,
    storage_id: u32,

    stop_ch: Option<mpsc::Sender<()>>,
}

impl ShardRouter {
    pub fn new(storage_id: u32) -> Self {
        Self {
            shard_ranges: Arc::new(parking_lot::RwLock::new(im::OrdMap::new())),
            storage_id,
            stop_ch: None,
        }
    }

    pub async fn get_shard_range(&self, key: Bytes, sync: bool) -> Result<ShardInfo> {
        if sync {
            self.update_partial_ranges(
                key.clone(),
                Bytes::from(MAX_KEY.to_vec()),
                Some(get_config().update_range_count),
            )
            .await?;
        }

        let guard = self.shard_ranges.read();
        if guard.is_empty() {
            return Err(anyhow!("shard route is empty"));
        }

        if guard.len() == 1 {
            let (_, ret) = guard.iter().next().unwrap();
            return Ok(ret.clone());
        }

        let (_, sr) = guard
            .get_prev(key.as_ref())
            .ok_or(anyhow!("get prev failed"))?;

        Ok(sr.clone())
    }

    pub fn scan_shard_range(&self, start: Bytes, end: Bytes) -> Result<Vec<ShardInfo>> {
        let ranges_copy = self.shard_ranges.read().clone();
        let mut r = ranges_copy
            .range(start.clone()..end)
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();

        if r.first().unwrap().range_start.clone() != start {
            r.insert(0, ranges_copy.get_prev(&start).unwrap().1.clone());
        }

        Ok(r)
    }

    async fn update_partial_ranges(
        &self,
        start: Bytes,
        end: Bytes,
        max_count: Option<u32>,
    ) -> Result<()> {
        Self::update_ranges(
            self.storage_id,
            self.shard_ranges.clone(),
            start,
            end,
            max_count,
        )
        .await?;

        Ok(())
    }

    async fn update_ranges(
        storage_id: u32,
        shard_ranges: Arc<parking_lot::RwLock<im::OrdMap<Bytes, ShardInfo>>>,
        range_start: Bytes,
        range_end: Bytes,
        max_count: Option<u32>,
    ) -> Result<()> {
        let mut range_start: Vec<u8> = range_start.into();

        let (max_count, mut count) = (max_count.unwrap_or(std::u32::MAX), 0);

        loop {
            let resp = MS_CLIENT
                .scan_shard_range({
                    ScanShardRangeRequest {
                        storage_id,
                        range_start: range_start.clone(),
                        range_count: get_config().update_range_count,
                    }
                })
                .await?;

            let mut ranges_copy = shard_ranges.write().clone();

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
                ranges_copy.remove(range_key);
            }

            for sr in resp.ranges.iter() {
                ranges_copy.insert(
                    Bytes::from(sr.range_start.clone()),
                    ShardInfo {
                        shard_id: sr.shard_id,
                        range_start: sr.range_start.clone(),
                        range_end: sr.range_end.clone(),
                        leader: sr.leader_addr.to_socket_addrs().unwrap().next().unwrap(),
                        replicates: sr
                            .addrs
                            .iter()
                            .map(|x| x.to_socket_addrs().unwrap().next().unwrap())
                            .collect::<Vec<_>>(),
                        update_at: Utc::now(),
                    },
                );
            }
            *shard_ranges.write() = ranges_copy;

            if resp.is_end {
                break;
            }
            range_start = last_key.into();
            if range_start >= range_end {
                break;
            }

            count += get_config().update_range_count;
            if count >= max_count {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        let shard_ranges = self.shard_ranges.clone();
        let storage_id = self.storage_id;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(5));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await; // make sure ms_client init successful first

            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        match Self::update_ranges(storage_id, shard_ranges.clone(), Bytes::new(), Bytes::new(), None).await {
                            Err(e) => error!("update ranges failed, err: {e}"),
                            Ok(_) => info!("update ranges for starogeid: {storage_id} successfully"),
                        };
                    }
                }
            }

            info!("shard router for storageid: {storage_id} started...");
        });

        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.send(()).await?;
        }

        Ok(())
    }
}
