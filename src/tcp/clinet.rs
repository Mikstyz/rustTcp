use std::error::Error;
// Используем асинхронный TcpStream из tokio
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, interval};
use tracing::{debug, error, info};

const DELAY_PING: u64 = 10;
const BUFFER: [u8; 1024] = [0; 1024];

pub struct Client {
    id: u64,
    server_addr: String,
}

impl Client {
    pub fn new(id: u64, addr: String) -> Self {
        Self {
            id,
            server_addr: addr,
        }
    }

    pub async fn run_async(&self, timeout: u64) -> Result<(), Box<dyn Error>> {
        debug!("connection for server: {}", self.server_addr);
        let mut stream = TcpStream::connect(&self.server_addr).await?;

        debug!(
            "Ok - connection for server - send user id for server: {}",
            self.id
        );
        stream.write_all(&self.id.to_be_bytes()).await?;

        let mut buf = BUFFER.clone();

        // count interval on ping
        let ping_i = if timeout > DELAY_PING {
            timeout - DELAY_PING
        } else {
            1
        };

        debug!("ping_interval: {}s", ping_i);
        let mut ping_interval = interval(Duration::from_secs(ping_i));

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
