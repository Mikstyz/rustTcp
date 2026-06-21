// use tokio::time::error;
use tracing::debug; //, error, info};
//
pub mod service {
    pub mod clinet;
}

pub mod config {
    pub mod config;
}

pub mod test {
    pub mod client_test;
}

const CONFIG_DIR: &str = "ClientConfig";

#[tokio::main]
async fn main() {
    //log
    tracing_subscriber::fmt::init();

    //config
    let config = config::config::Config::load(CONFIG_DIR.to_string());
    debug!("config: \n{:?}", config);
}
