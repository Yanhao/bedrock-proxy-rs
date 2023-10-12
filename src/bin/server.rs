#![feature(result_option_inspect)]

use std::fs;

use clap::Parser;
use tokio::signal;
use tonic::transport::Server as GrpcServer;
use tracing::{error, info};

use idl_gen::proxy::proxy_service_server::ProxyServiceServer;

use bedrock_proxy_rs::{
    config::{self, DEFAULT_CONFIG_FILE},
    handler::ProxyServer,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = DEFAULT_CONFIG_FILE.to_string())]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    config::init_config(args.config).inspect_err(|e| error!("init config failed, err: {e}"))?;

    let grpc_server =
        GrpcServer::builder().add_service(ProxyServiceServer::new(ProxyServer::default()));

    let rpc_server_addr = config::get_config().rpc_server_addr.unwrap().parse()?;
    tokio::spawn(async move {
        info!("starting proxy server...");
        grpc_server.serve(rpc_server_addr).await.unwrap();
        info!("stop proxy server");
    });

    signal::ctrl_c().await?;

    info!("ctrl-c pressed");
    if let Err(e) = fs::remove_file(format!(
        "{}/LOCK",
        config::get_config()
            .work_dir
            .as_ref()
            .unwrap_or(&String::from("."))
    )) {
        error!("failed to remove lock file, err {}", e);
    }

    Ok(())
}
