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
    _timeout_second: u8,

    #[serde_as(as = "DisplayFromStr")]
    _update_time_second: u8,
}

impl Config {
    pub fn load(path: String) -> Self {
        let file =
            File::open(&path).unwrap_or_else(|err| panic!("failed to open file {}: {}", path, err));

        let reader = BufReader::new(file);

        let config: Self = serde_json::from_reader(reader)
            .unwrap_or_else(|err| panic!("error parsing JSON in file {}: {}", path, err));

        config
    }
}
