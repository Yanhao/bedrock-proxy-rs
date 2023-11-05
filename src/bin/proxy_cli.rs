#![feature(result_option_inspect)]

use clap::{arg, Command};
use idl_gen::proxy::{
    proxy_service_client::ProxyServiceClient, KvDeleteRequest, KvGetRequest, KvSetRequest,
};

fn cli() -> Command {
    Command::new("proxy-cli")
        .about("proxy command line tool")
        .subcommand_required(true)
        .subcommand(
            Command::new("kvset")
                .about("kv set")
                .arg(arg!(<KEY> "key"))
                .arg(arg!(<VALUE> "value"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("kvget")
                .about("kv get")
                .arg(arg!(<KEY> "key"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("kvdel")
                .about("kv delete")
                .arg(arg!(<KEY> "key"))
                .arg_required_else_help(true),
        )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = cli().get_matches();

    let Ok(proxy_url) = std::env::var("PROXY_URL") else {
        println!("RPOXY_URL was not set!");
        std::process::exit(-1);
    };

    let Ok(storage_id) = std::env::var("STORAGE_ID").unwrap().parse::<u32>() else {
        println!("parse STORAGE_ID failed");
        std::process::exit(-1);
    };

    let mut proxy_cli = match ProxyServiceClient::connect(proxy_url).await {
        Ok(v) => v,
        Err(e) => {
            eprint!("connect failed, err: {e}\n");
            std::process::exit(-1);
        }
    };

    match matches.subcommand() {
        Some(("kvset", subcommand)) => {
            let key = subcommand.get_one::<String>("KEY").expect("requred");
            let value = subcommand.get_one::<String>("VALUE").expect("requred");

            if let Err(e) = proxy_cli
                .kv_set(KvSetRequest {
                    storage_id,
                    key: key.as_bytes().to_vec(),
                    value: value.as_bytes().to_vec(),
                })
                .await
            {
                eprint!("kvset failed, err: {e}\n");
            }
        }
        Some(("kvget", subcommand)) => {
            let key = subcommand.get_one::<String>("KEY").expect("requred");

            match proxy_cli
                .kv_get(KvGetRequest {
                    storage_id,
                    key: key.as_bytes().to_vec(),
                    read_latest: false,
                })
                .await
            {
                Err(e) => {
                    eprint!("kvget failed, err: {e}\n");
                }
                Ok(v) => {
                    print!(
                        "value: {}\n",
                        String::from_utf8_lossy(&v.into_inner().value)
                    )
                }
            }
        }
        Some(("kvdel", subcommand)) => {
            let key = subcommand.get_one::<String>("KEY").expect("requred");

            if let Err(e) = proxy_cli
                .kv_delete(KvDeleteRequest {
                    storage_id,
                    key: key.as_bytes().to_vec(),
                })
                .await
            {
                eprint!("kvdel failed, err: {e}\n");
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}
