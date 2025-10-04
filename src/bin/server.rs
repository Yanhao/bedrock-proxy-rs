use std::{fs, path::Path};

use clap::Parser;
use tokio::signal;
use tonic::transport::Server as GrpcServer;
use tracing::{error, info};

use idl_gen::proxy::proxy_service_server::ProxyServiceServer;

use bedrock_proxy_rs::{
    config::{self, get_config, DEFAULT_CONFIG_FILE},
    handler::ProxyServer,
    start_background_tasks,
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

    create_lock_file(get_config().work_dir.unwrap_or(String::from(".")))
        .inspect_err(|e| error!("failed to create lock file, err: {e}"))?;

    start_background_tasks().await?;

    let grpc_server =
        GrpcServer::builder().add_service(ProxyServiceServer::new(ProxyServer::default()));

    let rpc_server_addr = config::get_config().rpc_server_addr.unwrap().parse()?;
    tokio::spawn(async move {
        info!("starting proxy server...");
        grpc_server.serve(rpc_server_addr).await.unwrap();
        info!("stop proxy server");
    });

    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("ctrl-c pressed");
        },
        _ = sigterm.recv() => {
            info!("SIGTERM received");
        },
    };
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

fn create_lock_file(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let lock_path = path.as_ref().to_path_buf().join("LOCK");
    info!("create lock file: {:?}", lock_path);

    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(lock_path)?;

    Ok(())
}
