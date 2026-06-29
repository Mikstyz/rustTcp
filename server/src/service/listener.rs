use crate::service::events::InterestPool;
use crate::{entities::enum_task, service::connection};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{debug, info};

const COLLECTOR_TIMEOUT_SECS: u64 = 1;
const WRITER_TIMESTEP_CONN_MS: u64 = 10;

pub struct TcpServer {
    _addr: String,
    _name: String,
    _password: String,

    _pool: Arc<RwLock<InterestPool>>,
}

impl TcpServer {
    pub fn new(addr: &str, name: &str, password: &str, pool: InterestPool) -> Self {
        debug!("ip: {} \nname: {} \npassword: {}\n", addr, name, password,);

        Self {
            _name: name.to_string(),
            _addr: addr.to_string(),
            _password: password.to_string(),
            _pool: Arc::new(RwLock::new(pool)),
        }
    }

    pub async fn initialization_async(&self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("start dead connection collector");
        let collerctor_afk_connectoin = Arc::clone(&self._pool);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(COLLECTOR_TIMEOUT_SECS)).await;
                debug!("clean die connection");
                let pool_write = collerctor_afk_connectoin.write().await;
                pool_write.collector();
                drop(pool_write);
            }
        });

        let listener = TcpListener::bind(&self._addr).await?;
        info!("{} tcp server run - addr: {}", self._name, self._addr);
        debug!("writing tcp flow");

        loop {
            match listener.accept().await {
                Ok((stream, socket_addr)) => {
                    debug!("new connection from: {}", socket_addr);

                    if let Err(e) = stream.set_nodelay(true) {
                        tracing::error!("Failed to set NO_DELAY: {}", e);
                    }

                    let stream = Arc::new(stream);
                    let spy_stream = Arc::clone(&stream);

                    let conn = connection::Connection::new(socket_addr, Arc::clone(&stream));
                    let pool_clone = Arc::clone(&self._pool);

                    let conn_id = {
                        let mut pool_guard = pool_clone.write().await;
                        pool_guard.add_connection(conn)
                    };

                    let w_pool_clone = Arc::clone(&self._pool);

                    tokio::spawn(async move {
                        loop {
                            match spy_stream.readable().await {
                                Ok(()) => {
                                    // Peek to verify data actually arrived
                                    // readable() fires spuriously on Windows
                                    let mut peek = [0u8; 1];
                                    match spy_stream.peek(&mut peek).await {
                                        Ok(0) => {
                                            // Client closed connection gracefully
                                            debug!("Socket {} closed by client", conn_id);
                                            break;
                                        }
                                        Ok(_) => {
                                            // Data is available — check alive and fire event
                                            let pool_guard = w_pool_clone.read().await;
                                            let is_alive = pool_guard.contains_connection(conn_id);
                                            drop(pool_guard);

                                            if is_alive {
                                                let pool_guard = w_pool_clone.read().await;
                                                pool_guard.new_event(enum_task::Task::ReadData {
                                                    conn_id,
                                                });
                                                drop(pool_guard);

                                                tokio::time::sleep(
                                                    tokio::time::Duration::from_millis(
                                                        WRITER_TIMESTEP_CONN_MS,
                                                    ),
                                                )
                                                .await;
                                            } else {
                                                debug!(
                                                    "Socket watcher for ID {} stopped (conn deleted)",
                                                    conn_id
                                                );
                                                break;
                                            }
                                        }
                                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                            // Spurious wake — no data yet, keep waiting
                                            continue;
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "Peek error on socket id {}: {}",
                                                conn_id,
                                                e
                                            );
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("error reading socket id {}: {}", conn_id, e);
                                    break;
                                }
                            }
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
