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

use crate::connections::CONNS;
use crate::shard_range::SHARD_RANGE;
use crate::shard_route::SHARD_ROUTER;
use crate::tso::TSO;
use crate::utils::R;

#[derive(Debug, Default)]
pub struct ProxyServer {}

#[tonic::async_trait]
impl ProxyService for ProxyServer {
    async fn kv_set(
        &self,
        request: Request<KvSetRequest>,
    ) -> Result<Response<KvSetResponse>, Status> {
        let txid = TSO
            .load()
            .r()
            .allocate_txid()
            .await
            .map_err(|_| Status::internal(""))?;

        let sr = SHARD_RANGE
            .load()
            .r()
            .get_shard_range(Bytes::from(request.get_ref().key.clone()))
            .map_err(|_| Status::internal(""))?;

        let shard = SHARD_ROUTER
            .load()
            .r()
            .get_shard(sr.shard_id)
            .await
            .map_err(|_| Status::internal(""))?;

        let mut conn = CONNS
            .get_conn(shard.leader)
            .await
            .map_err(|_| Status::internal(""))?;

        let _ = conn
            .kv_set(dataserver::KvSetRequest {
                txid,
                shard_id: shard.shard_id,
                key: request.get_ref().key.clone(),
                value: request.get_ref().value.clone(),
            })
            .await
            .map_err(|_| Status::internal(""))?;

        Ok(Response::new(KvSetResponse {
            err: proxy::Error::Ok as i32,
        }))
    }

    async fn kv_get(
        &self,
        request: Request<KvGetRequest>,
    ) -> Result<Response<KvGetResponse>, Status> {
        let txid = TSO
            .load()
            .r()
            .allocate_txid()
            .await
            .map_err(|_| Status::internal(""))?;

        let sr = SHARD_RANGE
            .load()
            .r()
            .get_shard_range(Bytes::from(request.get_ref().key.clone()))
            .map_err(|_| Status::internal(""))?;

        let shard = SHARD_ROUTER
            .load()
            .r()
            .get_shard(sr.shard_id)
            .await
            .map_err(|_| Status::internal(""))?;

        let mut conn = CONNS
            .get_conn(shard.leader)
            .await
            .map_err(|_| Status::internal(""))?;

        let resp = conn
            .kv_get(dataserver::KvGetRequest {
                txid,
                shard_id: shard.shard_id,
                key: request.get_ref().key.clone(),
                need_lock: false,
                need_unlock: false,
            })
            .await
            .map_err(|_| Status::internal(""))?;

        Ok(Response::new(KvGetResponse {
            value: resp.into_inner().value,
            err: proxy::Error::Ok as i32,
        }))
    }

    async fn kv_delete(
        &self,
        request: Request<KvDeleteRequest>,
    ) -> Result<Response<KvDeleteResponse>, Status> {
        let txid = TSO
            .load()
            .r()
            .allocate_txid()
            .await
            .map_err(|_| Status::internal(""))?;

        let sr = SHARD_RANGE
            .load()
            .r()
            .get_shard_range(Bytes::from(request.get_ref().key.clone()))
            .map_err(|_| Status::internal(""))?;

        let shard = SHARD_ROUTER
            .load()
            .r()
            .get_shard(sr.shard_id)
            .await
            .map_err(|_| Status::internal(""))?;

        let mut conn = CONNS
            .get_conn(shard.leader)
            .await
            .map_err(|_| Status::internal(""))?;

        let _ = conn
            .kv_del(dataserver::KvDelRequest {
                txid,
                shard_id: shard.shard_id,
                key: request.get_ref().key.clone(),
            })
            .await
            .map_err(|_| Status::internal(""))?;

        Ok(Response::new(KvDeleteResponse {
            err: proxy::Error::Ok as i32,
        }))
    }

    type KvScanStream = Pin<Box<dyn Stream<Item = Result<KvScanResponse, Status>> + Send>>;
    async fn kv_scan(
        &self,
        request: Request<KvScanRequest>,
    ) -> Result<Response<Self::KvScanStream>, Status> {
        let txid = TSO
            .load()
            .r()
            .allocate_txid()
            .await
            .map_err(|_| Status::internal(""))?;
        let start_key = Bytes::from(request.get_ref().prefix.clone());
        let limit = request.get_ref().limit;

        let stream = stream! {
            let mut start_key = start_key;
            let mut limit = limit;

            while limit > 0 {
                let sr = SHARD_RANGE
                    .load()
                    .r()
                    .get_shard_range(start_key.clone())
                    .map_err(|_| Status::internal(""))?;

                let shard = SHARD_ROUTER
                    .load()
                    .r()
                    .get_shard(sr.shard_id)
                    .await
                    .map_err(|_| Status::internal(""))?;

                let mut conn = CONNS
                    .get_conn(shard.leader)
                    .await
                    .map_err(|_| Status::internal(""))?;

                let resp = conn
                    .kv_scan(dataserver::KvScanRequest {
                        txid,
                        shard_id: shard.shard_id,
                        prefix: start_key.clone().into(),
                        limit: 10,
                    })
                    .await
                    .map_err(|_| Status::internal(""))?
                    .into_inner();

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
        let txid = TSO
            .load()
            .r()
            .allocate_txid()
            .await
            .map_err(|_| Status::internal(""))?;

        let predicates = request.get_ref().predicates.clone();

        if !self
            .check_predicates(txid, predicates)
            .await
            .map_err(|_| Status::internal(""))?
        {
            return Ok(Response::new(BatchResponse {
                err: proxy::Error::Ok as i32,
            }));
        }

        let kvs = request.get_ref().kvs.clone();
        for kv in kvs.iter() {
            let sr = SHARD_RANGE
                .load()
                .r()
                .get_shard_range(Bytes::from(kv.key.clone()))
                .map_err(|_| Status::internal(""))?;

            let shard = SHARD_ROUTER
                .load()
                .r()
                .get_shard(sr.shard_id)
                .await
                .map_err(|_| Status::internal(""))?;

            let mut conn = CONNS
                .get_conn(shard.leader)
                .await
                .map_err(|_| Status::internal(""))?;

            let _ = conn
                .prepare_tx(dataserver::PrepareTxRequest {
                    txid,
                    shard_id: sr.shard_id,
                    kvs: vec![dataserver::KeyValue {
                        key: kv.key.clone(),
                        value: kv.value.clone(),
                    }],
                    need_lock: true,
                })
                .await
                .map_err(|_| Status::internal(""))?;
        }

        for kv in kvs.iter() {
            let sr = SHARD_RANGE
                .load()
                .r()
                .get_shard_range(Bytes::from(kv.key.clone()))
                .map_err(|_| Status::internal(""))?;

            let shard = SHARD_ROUTER
                .load()
                .r()
                .get_shard(sr.shard_id)
                .await
                .map_err(|_| Status::internal(""))?;

            let mut conn = CONNS
                .get_conn(shard.leader)
                .await
                .map_err(|_| Status::internal(""))?;

            let _ = conn
                .commit_tx(dataserver::CommitTxRequest {
                    txid,
                    shard_id: sr.shard_id,
                })
                .await
                .map_err(|_| Status::internal(""))?;
        }

        Ok(Response::new(BatchResponse {
            err: proxy::Error::Ok as i32,
        }))
    }
}

impl ProxyServer {
    async fn check_predicates(
        &self,
        txid: u64,
        predicates: Vec<proxy::Predicate>,
    ) -> anyhow::Result<bool> {
        for p in predicates.iter() {
            let sr = SHARD_RANGE
                .load()
                .r()
                .get_shard_range(Bytes::from(p.key.clone()))
                .map_err(|_| Status::internal(""))?;

            let shard = SHARD_ROUTER
                .load()
                .r()
                .get_shard(sr.shard_id)
                .await
                .map_err(|_| Status::internal(""))?;

            let mut conn = CONNS
                .get_conn(shard.leader)
                .await
                .map_err(|_| Status::internal(""))?;

            let resp = conn
                .kv_get(dataserver::KvGetRequest {
                    txid,
                    shard_id: sr.shard_id,
                    need_lock: true,
                    need_unlock: false,
                    key: p.key.clone(),
                })
                .await
                .map_err(|_| Status::internal(""))?;

            let value = resp.into_inner().value;

            let result = match PredicateOp::from(OpI32(p.op)) {
                PredicateOp::Equal => p.value == value,
                PredicateOp::NotEqual => p.value != value,
            };

            if !result {
                return Ok(false);
            }
        }

        Ok(true)
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
