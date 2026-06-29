use std::sync::Arc;

// use tokio::time::error;
use tracing::debug; //error, info};

pub mod service {
    pub mod connection;
    pub mod events;
    pub mod listener;
    pub mod router;
}

pub mod entities {
    pub mod enum_task;
}

pub mod config {
    pub mod config;
}

const CONFIG_DIR: &str = "TcpProxyConfig.json";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = config::config::Config::load(CONFIG_DIR.to_string());
    debug!("config: \n{:?}", config);

    let router = Arc::new(service::router::Router::new());

    // Add all backends from config
    for backend_addr in config.backends() {
        if !router.add_rout_server(backend_addr).await {
            tracing::warn!("Backend {} unavailable at startup", backend_addr);
        }
    }

    let pool = service::events::InterestPool::new(
        config.timeout_second(),
        config.update_time_second(),
        config.worker_concurrency(),
        config.backend_is_life_second(),
        Arc::clone(&router),
    );

    let server =
        service::listener::TcpServer::new(config.addr(), config.name(), config.password(), pool);

    Arc::clone(&router).healthcheck();

    if let Err(e) = server.initialization_async().await {
        tracing::error!("Server error: {}", e);
    }
}
