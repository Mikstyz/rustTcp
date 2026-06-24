use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, interval};
use tracing::{debug, error, info};

const BUFFER: [u8; 1024] = [0; 1024];

pub struct Client {
    _id: u64,
    _server_addr: String,
}

impl Client {
    pub fn new(_id: u64, addr: &str) -> Self {
        Self {
            _id,
            _server_addr: addr.to_string(),
        }
    }

    pub async fn initialization_on_server_async(
        &self,
        delay_ping: u16,
    ) -> Result<(), Box<dyn Error>> {
        debug!("connection for server: {}", self._server_addr);
        let mut stream = TcpStream::connect(&self._server_addr).await?;

        debug!(
            "Ok - connection for server - send user id for server: {}",
            self._id
        );
        stream.write_all(&self._id.to_be_bytes()).await?;

        let mut buf = BUFFER.clone();

        debug!("ping_interval: {}s", delay_ping);
        let mut ping_interval = interval(Duration::from_secs(delay_ping as u64));

        // miss first tick interval() ticket in start
        ping_interval.tick().await;

        loop {
            tokio::select! {
                // send ping for server on timer
                _ = ping_interval.tick() => {
                    if let Err(e) = stream.write_all(b"PING").await {
                        error!("Error sending ping: {}", e);
                        break;
                    }
                    debug!("Ping sent successfully");
                }

                // read data from server
                read_result = stream.read(&mut buf) => {
                    match read_result {
                        Ok(0) => {
                            info!("Server closed connection");
                            break;
                        }

                        Ok(n) => {
                            let data = &buf[..n];

                            //action on meesage
                            if let Err(e) = Self::processing_raw_data(data) {
                                error!("Failed to procces raw data: {}", e)
                            }
                        }

                        Err(e) => {
                            error!("Error reading from socket: {}", e);
                            break;
                        }
                    }
                }
            }
        }

        info!("Client stopped");
        Ok(())
    }

    fn processing_raw_data(raw_data: &[u8]) -> Result<(), Box<dyn Error>> {
        Self::print_data_for_server(raw_data);
        Ok(())
    }

    fn print_data_for_server(raw_data: &[u8]) {
        debug!("New message from server: {:?}", raw_data);
    }

    pub async fn send_message_for_server(&self, _stream: &mut TcpStream, data: &[u8]) {
        debug!("new send data for server: {:?}", data)
        //byte array
    }

    pub fn close() {}

    //print byte array for console
}
