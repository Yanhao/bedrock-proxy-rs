use std::sync::Arc;

use anyhow::{anyhow, Result};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use chrono::prelude::*;
use once_cell::sync::Lazy;
use tokio::{select, sync::mpsc, time::MissedTickBehavior};
use tracing::info;

use idl_gen::metaserver::ScanShardRangeRequest;

use crate::ms_client::get_ms_client;

pub static SHARD_RANGE: Lazy<ArcSwapOption<ShardRangeCache>> = Lazy::new(|| None.into());

#[derive(Clone)]
pub struct ShardRange {
    pub(crate) shard_id: u64,
    range_start: Vec<u8>,
    #[allow(dead_code)]
    range_end: Vec<u8>,
    #[allow(dead_code)]
    update_time: DateTime<Utc>,
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

    pub fn get_shard_range(&self, key: Bytes) -> Result<ShardRange> {
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
        storage_id: u32,
        start: Bytes,
        end: Bytes,
    ) -> Result<()> {
        Self::update_ranges(storage_id, self.shard_ranges.clone(), start, end).await?;

        Ok(())
    }

    pub async fn update_ranges(
        storage_id: u32,
        shard_ranges: Arc<parking_lot::RwLock<im::OrdMap<Bytes, ShardRange>>>,
        start: Bytes,
        end: Bytes,
    ) -> Result<()> {
        let mut range_start = vec![];
        loop {
            let resp = get_ms_client()
                .await
                .scan_shard_range(ScanShardRangeRequest {
                    storage_id,
                    range_start: range_start.clone(),
                    range_count: 10,
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

                if first_key != *(r.first().unwrap()) {
                    if let Some((range_start, _)) = ranges_copy.get_prev(&first_key) {
                        r.insert(0, range_start.clone());
                    }
                }
                r
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
                        update_time: Utc::now(),
                    },
                );
            }

            range_start = last_key.into();
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

            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        Self::update_ranges(1,  shard_ranges.clone(), Bytes::new(), Bytes::new())
                        .await
                        .unwrap();
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
