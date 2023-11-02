use std::{fs::read_to_string, path::Path};

use anyhow::Result;
use once_cell::sync::Lazy;
use serde::Deserialize;
use tracing::{debug, error, info};

pub const DEFAULT_CONFIG_FILE: &str = "/etc/bedrock/proxy.toml";

pub static CONFIG: Lazy<parking_lot::RwLock<Configuration>> = Lazy::new(Default::default);

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Configuration {
    pub rpc_server_addr: Option<String>,
    pub work_dir: Option<String>,
    pub metaserver_url: String,
    pub storage_id: u32,
    pub update_range_count: u32,
}

impl Configuration {
    pub fn parse_config_file(file: &Path) -> Result<Configuration> {
        info!("parsing configuration file: {}", file.to_str().unwrap());

        let file_contents = read_to_string(file)?;

        Ok(toml::from_str(&file_contents)?)
    }

    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

pub fn init_config(config_file: impl AsRef<Path>) -> Result<()> {
    let conf = Configuration::parse_config_file(config_file.as_ref())
        .inspect_err(|e| error!("failed to initialize config module, err: {e}"))?;

    conf.validate()?;

    *CONFIG.write() = conf;

    info!("successfully initialized config module");
    debug!("configuration: {:?}", *CONFIG.read());

    Ok(())
}

pub fn get_config() -> Configuration {
    (*CONFIG.read()).clone()
}
