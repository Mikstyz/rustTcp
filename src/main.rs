use tokio::time::error;
use tracing::{debug, error, info};

pub mod cli {
    pub mod command;
}

pub mod tcp {
    pub mod clinet;
    pub mod connection;
    pub mod server;
}

pub mod tools {
    pub mod time;
}

pub mod service {
    pub mod console;
}

pub mod config {
    pub mod config;
}

const SERVER_NAME: &str = "root_server";
const SERVER_IP: &str = "127.0.0.1:1234";
const SERVER_PASSWORD: &str = "password";

// create async run time Tokio
#[tokio::main]
async fn main() {
    //loging
    tracing_subscriber::fmt::init();

    tracing::info!("initialization for server {}...", SERVER_NAME);

    //create copy server
    let server = tcp::server::TcpServer::new(SERVER_NAME, SERVER_IP, SERVER_PASSWORD);

    tracing::info!("server running for addr {}...", SERVER_IP);

    //run server async
    if let Err(e) = server.run_async().await {
        error!("server error: {}", e);
    }

    debug!("server stoped");
}
