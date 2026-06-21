// use tokio::time::error;
use tracing::debug; //error, info};

pub mod service {
    pub mod server;
}

pub mod config {
    pub mod config;
}

pub mod test {
    pub mod server_test;
}

const CONFIG_DIR: &str = "ServerConfig.json";

#[tokio::main]
async fn main() {
    //loging
    tracing_subscriber::fmt::init();

    //config load
    let config = config::config::Config::load(CONFIG_DIR.to_string());
    debug!("config: \n{:?}", config);
    //run server
}
