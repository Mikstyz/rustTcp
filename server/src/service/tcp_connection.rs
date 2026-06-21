use crate::service::connection;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info};

pub struct TcpServer {
    _addr: String,
    _name: String,
    _password: String,

    _connectoins: Arc<Mutex<connection::ConnectionManager>>,
    _timeout_secont: u16,
    _update_time_second: u8,
}

impl TcpServer {
    pub fn new(
        addr: &str,
        name: &str,
        password: &str,
        timeout_secont: u16,
        update_time_second: u8,
    ) -> Self {
        debug!(
            "ip: {} \nname: {} \npassword: {}\nTimeOutSecond: {} \nUpdateTimeSecond: {} \n",
            addr,
            name.to_string(),
            password.to_string(),
            timeout_secont,
            update_time_second
        );

        let manager = connection::ConnectionManager::new(timeout_secont, update_time_second);

        Self {
            _name: name.to_string(),
            _addr: addr.to_string(),
            _password: password.to_string(),
            _connectoins: Arc::new(Mutex::new(manager)),
            _timeout_secont: timeout_secont,
            _update_time_second: update_time_second,
        }
    }

    //liste client and send data
    async fn handle_client(
        mut _stream: TcpStream,
        _addr: std::net::SocketAddr,
        _manager: Arc<Mutex<connection::ConnectionManager>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    pub async fn initialization_async(&self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("arc clone connections");

        let update_time = self._update_time_second.clone();
        let cleanup_manager = Arc::clone(&self._connectoins);

        debug!("tokio spawn cleanup task");
        tokio::spawn(async move {
            loop {
                //UPDATE_TIME_SECOND
                debug!("thread sleep: {}", update_time);
                tokio::time::sleep(Duration::from_secs(update_time as u64)).await;

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

        let listener = TcpListener::bind(&self._addr).unwrap();
        info!("{} TCP server run - addr: {}", self._name, self._addr);

        debug!("writing tcp flow");

        loop {
            match listener.accept() {
                Ok((stream, socket_addr)) => {
                    debug!("new connection from: {}", socket_addr);

                    let mgr_clone = Arc::clone(&self._connectoins);

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
