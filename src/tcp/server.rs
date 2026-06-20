use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info};

use crate::tcp::connection;

pub struct TcpServer {
    name: String,
    addr: String,
    password: String,

    connectoins: Arc<Mutex<connection::ConnectionManager>>,
}

// connection lifetime
const TIMEOUT_SECOND: u64 = 20;

// number of connection updates
const UPDATE_TIME_SECOND: u64 = 60;

impl TcpServer {
    pub fn new(name: &str, addr: &str, password: &str) -> Self {
        debug!(
            "name: {} \nip: {} \npassword: {}\nTimeOutSecond: {} \nUpdateTimeSecond: {} \n",
            name.to_string(),
            addr,
            password.to_string(),
            TIMEOUT_SECOND,
            UPDATE_TIME_SECOND
        );

        let manager = connection::ConnectionManager::new(TIMEOUT_SECOND, UPDATE_TIME_SECOND);

        Self {
            name: name.to_string(),
            addr: addr.to_string(),
            password: password.to_string(),
            connectoins: Arc::new(Mutex::new(manager)),
        }
    }

    //Слушать клиента или отправлять ему сообщения
    async fn handle_client(
        mut stream: TcpStream,
        addr: std::net::SocketAddr,
        manager: Arc<Mutex<connection::ConnectionManager>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    pub async fn run_async(&self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("arc clone connections");
        let cleanup_manager = Arc::clone(&self.connectoins);

        debug!("tokio spawn cleanup task");
        tokio::spawn(async move {
            loop {
                debug!("thread sleep: {}", UPDATE_TIME_SECOND);
                tokio::time::sleep(Duration::from_secs(UPDATE_TIME_SECOND)).await;

                debug!("Clear die conn");
                let mut mgr = cleanup_manager.lock().unwrap();

                let deleted = mgr.update();

                debug!(
                    "Active connections: {}. Deleted this turn: {}",
                    mgr.len(),
                    deleted
                );
            }
        });

        let listener = TcpListener::bind(&self.addr).unwrap();
        info!("{} TCP server run - addr: {}", self.name, self.addr);

        debug!("writing tcp flow");

        loop {
            match listener.accept() {
                Ok((stream, socket_addr)) => {
                    debug!("new connection from: {}", socket_addr);

                    let mgr_clone = Arc::clone(&self.connectoins);

                    //async task on client
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_client(stream, socket_addr, mgr_clone).await {
                            tracing::error!("Error handling client {}: {}", socket_addr, e);
                        }
                    });
                }
                Err(e) => {
                    debug!("Error at accept client connection: {}", e);
                }
            }
        }
    }
}
