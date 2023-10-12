use std::path::Path;

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
}

impl Configuration {
    pub fn parse_config_file(_file: &Path) -> Result<Configuration> {
        todo!()
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
