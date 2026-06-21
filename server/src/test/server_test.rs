use crate::service;
use common::test_constants;

pub struct TestServer {
    _server: service::server::TcpServer,
}

impl TestServer {
    pub async fn new() -> Self {
        tracing::info!(
            "initialization for server {}...",
            test_constants::SERVER_NAME
        );

        let server = service::server::TcpServer::new(
            test_constants::SERVER_NAME,
            test_constants::SERVER_ADDR,
            test_constants::SERVER_PASSWORD,
            test_constants::TIMEOUT_SECOND,
            test_constants::UPDATE_TIME_SECOND,
        );

        tracing::info!("server running for addr {}...", test_constants::SERVER_ADDR);

        if let Err(e) = server.initialization_async().await {
            tracing::info!("server error: {}", e);
        }

        tracing::info!("server stoped");

        Self { _server: server }
    }
}
