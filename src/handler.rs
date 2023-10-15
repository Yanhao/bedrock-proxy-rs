use std::pin::Pin;

use async_stream::stream;
use bytes::Bytes;
use futures_util::StreamExt;
use itertools::Itertools;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use idl_gen::dataserver;
use idl_gen::proxy::proxy_service_server::ProxyService;
use idl_gen::proxy::{
    self, BatchRequest, BatchResponse, KvDeleteRequest, KvDeleteResponse, KvGetRequest,
    KvGetResponse, KvScanRequest, KvScanResponse, KvSetRequest, KvSetResponse, PredicateOp,
};

use crate::ds_client::get_ds_client;
use crate::shard_range::SHARD_RANGE;
use crate::shard_route::{ShardRoute, SHARD_ROUTER};
use crate::tso::Tso;
use crate::utils::R;

#[derive(Debug, Default)]
pub struct ProxyServer {}

#[tonic::async_trait]
impl ProxyService for ProxyServer {
    async fn kv_set(
        &self,
        request: Request<KvSetRequest>,
    ) -> Result<Response<KvSetResponse>, Status> {
        let request = request.into_inner();
        let txid = Tso::alloc_txid().await?;

        let shard = Self::shard_route(request.storage_id, request.key.clone().into())
            .await
            .map_err(|_| Status::internal("get shard route failed"))?;

        let _ = get_ds_client()
            .await
            .kv_set(
                &shard.leader,
                dataserver::KvSetRequest {
                    txid,
                    shard_id: shard.shard_id,
                    key: request.key,
                    value: request.value,
                },
            )
            .await
            .map_err(|e| Status::internal(format!("err: {e}")))?;

        Ok(Response::new(KvSetResponse {
            err: proxy::Error::Ok as i32,
        }))
    }

    async fn kv_get(
        &self,
        request: Request<KvGetRequest>,
    ) -> Result<Response<KvGetResponse>, Status> {
        let request = request.into_inner();
        let txid = Tso::alloc_txid().await?;

        let shard = Self::shard_route(request.storage_id, request.key.clone().into())
            .await
            .map_err(|_| Status::internal("get shard route failed"))?;

        let resp = get_ds_client()
            .await
            .kv_get(
                &shard.leader,
                dataserver::KvGetRequest {
                    txid,
                    shard_id: shard.shard_id,
                    key: request.key,
                    need_lock: false,
                    need_unlock: false,
                },
            )
            .await
            .map_err(|e| Status::internal(format!("err: {e}")))?;

        Ok(Response::new(KvGetResponse {
            value: resp.value,
            err: proxy::Error::Ok as i32,
        }))
    }

    async fn kv_delete(
        &self,
        request: Request<KvDeleteRequest>,
    ) -> Result<Response<KvDeleteResponse>, Status> {
        let request = request.into_inner();
        let txid = Tso::alloc_txid().await?;

        let shard = Self::shard_route(request.storage_id, request.key.clone().into())
            .await
            .map_err(|_| Status::internal("get shard route failed"))?;

        let _ = get_ds_client()
            .await
            .kv_del(
                &shard.leader,
                dataserver::KvDelRequest {
                    txid,
                    shard_id: shard.shard_id,
                    key: request.key,
                },
            )
            .await
            .map_err(|e| Status::internal(format!("err: {e}")))?;

        Ok(Response::new(KvDeleteResponse {
            err: proxy::Error::Ok as i32,
        }))
    }

    type KvScanStream = Pin<Box<dyn Stream<Item = Result<KvScanResponse, Status>> + Send>>;
    async fn kv_scan(
        &self,
        request: Request<KvScanRequest>,
    ) -> Result<Response<Self::KvScanStream>, Status> {
        let request = request.into_inner();
        let txid = Tso::alloc_txid().await?;

        let (start_key, limit) = (Bytes::from(request.prefix.clone()), request.limit);

        let stream = stream! {
            let (mut start_key, mut limit) = (start_key, limit);

            while limit > 0 {
                let shard = Self::shard_route(request.storage_id, start_key.clone())
                .await
                .map_err(|_| Status::internal("get shard route failed"))?;

                let resp = get_ds_client().await.kv_scan(
                    &shard.leader,
                    dataserver::KvScanRequest {
                        txid,
                        shard_id: shard.shard_id,
                        prefix: start_key.clone().into(),
                        limit: 128, // TODO: default count, need to be configurable
                    }
                )
                .await
                .map_err(|_| Status::internal("get shard route failed"))?;

                if resp.kvs.is_empty() {
                    break;
                }

                yield Ok(KvScanResponse {
                    err: proxy::Error::Ok as i32,
                    kvs: resp.kvs.iter().map(
                        |kv| proxy::KeyValue{ key: kv.key.clone(), value: kv.value.clone() },
                    ).collect_vec(),
                });

                start_key = resp.kvs.last().unwrap().key.clone().into();
                limit -= resp.kvs.len() as u32;
            }
        }
        .boxed();

        Ok(Response::new(stream.into()))
    }

    async fn batch(
        &self,
        request: Request<BatchRequest>,
    ) -> Result<Response<BatchResponse>, Status> {
        let request = request.into_inner();
        let txid = Tso::alloc_txid().await?;

        if !self
            .check_predicates(request.storage_id, txid, request.predicates)
            .await
            .map_err(|e| Status::internal(format!("err: {e}")))?
        {
            return Ok(Response::new(BatchResponse {
                err: proxy::Error::Ok as i32,
            }));
        }

        for kv in request.kvs.iter() {
            let shard = Self::shard_route(request.storage_id, kv.key.clone().into())
                .await
                .map_err(|_| Status::internal("get shard route failed"))?;

            let _ = get_ds_client()
                .await
                .prepare_tx(
                    &shard.leader,
                    dataserver::PrepareTxRequest {
                        txid,
                        shard_id: shard.shard_id,
                        kvs: vec![dataserver::KeyValue {
                            key: kv.key.clone(),
                            value: kv.value.clone(),
                        }],
                        need_lock: true,
                    },
                )
                .await
                .map_err(|_| Status::internal(""))?;
        }

        for kv in request.kvs.into_iter() {
            let shard = Self::shard_route(request.storage_id, kv.key.into())
                .await
                .map_err(|_| Status::internal("get shard route failed"))?;

            let _ = get_ds_client()
                .await
                .commit_tx(
                    &shard.leader,
                    dataserver::CommitTxRequest {
                        txid,
                        shard_id: shard.shard_id,
                    },
                )
                .await
                .map_err(|e| Status::internal(format!("err: {e}")))?;
        }

        Ok(Response::new(BatchResponse {
            err: proxy::Error::Ok as i32,
        }))
    }
}

impl ProxyServer {
    async fn check_predicates(
        &self,
        storage_id: u32,
        txid: u64,
        predicates: Vec<proxy::Predicate>,
    ) -> anyhow::Result<bool> {
        for p in predicates.into_iter() {
            let shard = Self::shard_route(storage_id, p.key.clone().into()).await?;
            let resp = get_ds_client()
                .await
                .kv_get(
                    &shard.leader,
                    dataserver::KvGetRequest {
                        txid,
                        shard_id: shard.shard_id,
                        need_lock: true,
                        need_unlock: false,
                        key: p.key,
                    },
                )
                .await?;

            let result = match PredicateOp::from(OpI32(p.op)) {
                PredicateOp::Equal => p.value == resp.value,
                PredicateOp::NotEqual => p.value != resp.value,
            };

            if !result {
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn shard_route(_storage_id: u32, key: Bytes) -> anyhow::Result<ShardRoute> {
        let sr = SHARD_RANGE.load().r().get_shard_range(key)?;
        Ok(SHARD_ROUTER.load().r().get_shard(sr.shard_id).await?)
    }
}

struct OpI32(i32);
impl From<OpI32> for PredicateOp {
    fn from(value: OpI32) -> Self {
        match value.0 {
            0 => PredicateOp::Equal,
            1 => PredicateOp::NotEqual,
            _ => unreachable!(),
        }
    }
}
