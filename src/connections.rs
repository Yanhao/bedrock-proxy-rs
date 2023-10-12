use std::net::SocketAddr;

use anyhow::Result;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use tonic::transport::Channel;

use idl_gen::service_pb::data_service_client::DataServiceClient;

pub static CONNS: Lazy<Connections> = Lazy::new(|| Default::default());

#[derive(Default)]
pub struct Connections {
    conns: DashMap<SocketAddr, DataServiceClient<Channel>>,
}

impl Connections {
    pub async fn get_conn(&self, addr: SocketAddr) -> Result<DataServiceClient<Channel>> {
        Ok(self
            .conns
            .entry(addr)
            .or_insert(DataServiceClient::connect(addr.to_string()).await?)
            .clone())
    }
}
