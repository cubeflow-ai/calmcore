pub mod network;
pub mod times;

use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub cluster: String,
    pub manager_addr: String,
    pub log: LogConfig,
    pub data_path: String,
    pub host: Option<String>,
    pub http_port: u32,
    pub grpc_port: u32,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct LogConfig {
    pub path: String,
    pub level: String,
}

pub fn load_config(config_path: &str) -> Config {
    let data = match std::fs::read(config_path) {
        Ok(data) => data,
        Err(err) => {
            panic!("Failed to load config file:{} err:{}", config_path, err);
        }
    };

    match toml::from_str::<Config>(std::str::from_utf8(&data).unwrap()) {
        Ok(config) => config,
        Err(err) => {
            panic!("Failed to parse config file: {}", err);
        }
    }
}
