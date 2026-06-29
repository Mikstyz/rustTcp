use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use std::fs::File;
use std::io::BufReader;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct ProxyConfig {
    pub _name: String,
    pub _addr: String,
}

#[serde_as]
#[derive(Deserialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct Config {
    _proxies: Vec<ProxyConfig>,
    #[serde_as(as = "DisplayFromStr")]
    _timeout_ms: u64,
    #[serde_as(as = "DisplayFromStr")]
    _reconnect_attempts: u8,
}

impl Config {
    pub fn load(path: String) -> Self {
        let file = File::open(&path)
            .unwrap_or_else(|err| panic!("Failed to open config file {}: {}", path, err));
        let reader = BufReader::new(file);
        let config: Self = serde_json::from_reader(reader)
            .unwrap_or_else(|err| panic!("Failed to parse config JSON {}: {}", path, err));
        config
    }

    pub fn proxies(&self) -> &[ProxyConfig] {
        &self._proxies
    }

    pub fn timeout_ms(&self) -> u64 {
        self._timeout_ms
    }

    pub fn reconnect_attempts(&self) -> u8 {
        self._reconnect_attempts
    }
}
