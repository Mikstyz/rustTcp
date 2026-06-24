use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use std::fs::File;
use std::io::BufReader;

#[serde_as]
#[derive(Deserialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct Config {
    _name: String,
    _addr: String,
    _password: String,

    #[serde_as(as = "DisplayFromStr")]
    _timeout_second: usize,

    #[serde_as(as = "DisplayFromStr")]
    _update_time_second: usize,

    #[serde_as(as = "DisplayFromStr")]
    _worker_concurrency: usize,

    _backends: Vec<String>,
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

    pub fn addr(&self) -> &str {
        &self._addr
    }

    pub fn name(&self) -> &str {
        &self._name
    }

    pub fn password(&self) -> &str {
        &self._password
    }

    pub fn timeout_second(&self) -> usize {
        self._timeout_second
    }

    pub fn update_time_second(&self) -> usize {
        self._update_time_second
    }

    pub fn worker_concurrency(&self) -> usize {
        self._worker_concurrency
    }

    pub fn backends(&self) -> &Vec<String> {
        &self._backends
    }
}
