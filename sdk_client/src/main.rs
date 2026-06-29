use tracing::debug;

mod config {
    pub mod config;
}
mod service {
    pub mod connectoin;
    pub mod proxy_enty;
    pub mod proxy_pool;
}

const CONFIG_DIR: &str = "SdkClientConfig.json";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = config::config::Config::load(CONFIG_DIR.to_string());
    debug!("config: \n{:?}", config)
}
