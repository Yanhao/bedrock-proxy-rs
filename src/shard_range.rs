use std::{ops::Range, sync::Arc};

use anyhow::{anyhow, Result};
use arc_swap::ArcSwapOption;
use chrono::prelude::*;
use once_cell::sync::Lazy;
use tokio::{select, sync::mpsc};
use tonic::transport::Channel;
use tracing::info;

use idl_gen::metaserver::{meta_service_client::MetaServiceClient, ScanStorageShardsRequest};

use crate::utils::R;

pub static SHARD_RANGE: Lazy<ArcSwapOption<ShardRangeCache>> = Lazy::new(|| None.into());

#[derive(Clone)]
pub struct ShardRange {
    pub(crate) shard_id: u64,
    range_start: Vec<u8>,
    range_end: Vec<u8>,
    update_time: DateTime<Utc>,
}

#[derive(Default)]
pub struct ShardRangeCache {
    shard_ranges: Arc<parking_lot::RwLock<im::OrdMap<String, ShardRange>>>,
    stop_ch: Option<mpsc::Sender<()>>,
}

impl ShardRangeCache {
    pub fn new() -> Self {
        Self {
            shard_ranges: Arc::new(parking_lot::RwLock::new(im::OrdMap::new())),
            stop_ch: None,
        }
    }

    pub fn get_shard_range(&self, key: impl AsRef<str>) -> Result<ShardRange> {
        let guard = self.shard_ranges.read();
        let range = guard
            .get_prev(key.as_ref())
            .ok_or(anyhow!("get prev failed"))?;

        Ok(range.1.clone())
    }

    pub fn scan_shard_range(&self, start: String, end: String) -> Result<Vec<ShardRange>> {
        let ranges_copy = self.shard_ranges.read().clone();
        let mut r = ranges_copy
            .range(start.clone()..end)
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();

        if unsafe { String::from_utf8_unchecked(r.first().unwrap().range_start.clone()) } != start {
            r.insert(0, ranges_copy.get_prev(&start).unwrap().1.clone());
        }

        Ok(r)
    }

    pub async fn update_ranges(
        storage_id: u32,
        mut ms_client: MetaServiceClient<Channel>,
        shard_ranges: Arc<parking_lot::RwLock<im::OrdMap<String, ShardRange>>>,
    ) -> Result<()> {
        let mut range_start = vec![];
        loop {
            let a = ms_client
                .scan_storage_shards(ScanStorageShardsRequest {
                    storage_id,
                    range_start: range_start.clone(),
                })
                .await?
                .into_inner();

            if a.is_end {
                break;
            }

            let mut guard = shard_ranges.write();

            let ranges_copy = guard.clone();

            let (first_key, last_key) = unsafe {
                (
                    String::from_utf8_unchecked(a.shards.first().unwrap().range_start.clone()),
                    String::from_utf8_unchecked(a.shards.last().unwrap().range_end.clone()),
                )
            };

            let mut ranges_to_remove = ranges_copy
                .range(first_key.clone()..last_key.clone())
                .collect::<Vec<_>>();

            if first_key != *(ranges_to_remove.first().unwrap().0) {
                if let Some(a) = ranges_copy.get_prev(&first_key) {
                    ranges_to_remove.insert(0, a);
                }
            }

            for i in ranges_to_remove.iter() {
                guard.remove(i.0);
            }

            for s in a.shards.iter() {
                unsafe {
                    guard.insert(
                        String::from_utf8_unchecked(s.range_start.clone()),
                        ShardRange {
                            shard_id: s.shard_id,
                            range_start: s.range_start.clone(),
                            range_end: s.range_end.clone(),
                            update_time: Utc::now(),
                        },
                    );
                }
            }

            range_start = a.shards.last().unwrap().range_end.clone();
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let ms_client = MetaServiceClient::connect("").await?;

        let (tx, mut rx) = mpsc::channel(1);
        self.stop_ch.replace(tx);

        let shard_ranges = self.shard_ranges.clone();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = rx.recv() => {
                        break;
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    }
                }
            }

            Self::update_ranges(1, ms_client.clone(), shard_ranges.clone())
                .await
                .unwrap();

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
