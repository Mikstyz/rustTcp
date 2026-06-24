use crate::service::connection;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info};

pub struct TcpServer {
    _addr: String,
    _name: String,
    _password: String,
}

impl TcpServer {
    pub fn new(addr: &str, name: &str, password: &str) -> Self {
        debug!(
            "ip: {} \nname: {} \npassword: {}\n",
            addr,
            name.to_string(),
            password.to_string(), 
        );

        Self {
            _name: name.to_string(),
            _addr: addr.to_string(),
            _password: password.to_string(),
        }
    }

    //liste client and send data
    async fn handle_client(
        mut _stream: TcpStream,
        _addr: std::net::SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    pub async fn initialization_async(&self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("arc clone connections");

        debug!("tokio spawn cleanup task");
        tokio::spawn(async move {
            loop {
                debug!("Clear die conn");
            }
        });

        let listener = TcpListener::bind(&self._addr).unwrap();
        info!("{} TCP server run - addr: {}", self._name, self._addr);

        debug!("writing tcp flow");

        loop {
            match listener.accept() {
                Ok((stream, socket_addr)) => {
                    debug!("new connection from: {}", socket_addr);
                }
                Err(e) => {
                    debug!("Error at accept client connection: {}", e);
                }
            }
        }
    }
}
