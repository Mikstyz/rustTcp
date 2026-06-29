use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use std::fs::File;
use std::io::BufReader;

#[serde_as]
#[derive(Deserialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct Config {
    _addr: String,
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
}
