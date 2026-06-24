use parking_lot::RwLock;
use slab::Slab;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::info;

const TIMEOUT_SERVER_MS: u64 = 200;
const MAX_POOL_CONNECTIONS: usize = 10;
const POOL_WARMUP_COUNT: usize = 2;

#[derive(Clone, Copy, PartialEq)]
pub enum BackendStatus {
    Online,
    Offline,
}

pub struct BackendPool {
    pub _addr: String,
    pub _id: usize,
    pub _status: BackendStatus,
    pub _latency_ms: u32,
    _connections: Mutex<VecDeque<TcpStream>>,
    _max_connections: usize,
}

impl BackendPool {
    pub fn new(addr: String, id: usize, status: BackendStatus, latency_ms: u32) -> Self {
        Self {
            _addr: addr,
            _id: id,
            _status: status,
            _latency_ms: latency_ms,
            _connections: Mutex::new(VecDeque::new()),
            _max_connections: MAX_POOL_CONNECTIONS,
        }
    }

    // Pre-fill the pool with ready connections on startup
    pub async fn warmup(&self, count: usize) {
        let mut pool = self._connections.lock().await;
        for _ in 0..count {
            match TcpStream::connect(&self._addr).await {
                Ok(conn) => {
                    let _ = conn.set_nodelay(true);
                    pool.push_back(conn);
                }
                Err(e) => {
                    tracing::warn!("Warmup connect failed for {}: {}", self._addr, e);
                }
            }
        }
        tracing::debug!(
            "Pool warmed up with {} connections for {}",
            pool.len(),
            self._addr
        );
    }

    // Take an existing connection from the pool, or create a new one
    pub async fn acquire(&self) -> Option<TcpStream> {
        {
            let mut pool = self._connections.lock().await;
            if let Some(conn) = pool.pop_front() {
                tracing::debug!("Reusing pooled connection to {}", self._addr);
                return Some(conn);
            }
        }

        tracing::debug!("Pool empty, creating new connection to {}", self._addr);
        match TcpStream::connect(&self._addr).await {
            Ok(conn) => {
                let _ = conn.set_nodelay(true);
                Some(conn)
            }
            Err(e) => {
                tracing::error!("Failed to connect to {}: {}", self._addr, e);
                None
            }
        }
    }

    // Return a healthy connection back to the pool after use
    pub async fn release(&self, conn: TcpStream) {
        let mut pool = self._connections.lock().await;
        if pool.len() < self._max_connections {
            pool.push_back(conn);
            tracing::debug!("Connection returned to pool for {}", self._addr);
        }
        // Pool is full — drop the connection
    }
}

pub struct Router {
    _upstreams: Arc<RwLock<Slab<Arc<BackendPool>>>>,
    _rr_index: AtomicUsize,
}

impl Router {
    pub fn new() -> Self {
        Self {
            _upstreams: Arc::new(RwLock::new(Slab::new())),
            _rr_index: AtomicUsize::new(0),
        }
    }

    // Round-Robin: pick the next online backend address
    fn choose_next(&self) -> Option<Arc<BackendPool>> {
        let pool = self._upstreams.read();

        if pool.is_empty() {
            return None;
        }

        let start_idx = self._rr_index.fetch_add(1, Ordering::Relaxed);
        let capacity = pool.capacity();

        for i in 0..capacity {
            let target_id = (start_idx + i) % capacity;
            if let Some(backend) = pool.get(target_id) {
                if backend._status == BackendStatus::Online {
                    return Some(Arc::clone(backend));
                }
            }
        }

        None
    }

    // TCP ping to check if a backend is reachable and measure latency
    pub async fn ping(&self, addr: &str) -> (BackendStatus, u32) {
        let start = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_millis(TIMEOUT_SERVER_MS),
            TcpStream::connect(addr),
        )
        .await;

        match result {
            Ok(Ok(_)) => {
                let latency = start.elapsed().as_millis() as u32;
                (BackendStatus::Online, latency)
            }
            _ => (BackendStatus::Offline, 0),
        }
    }

    // Forward raw bytes from a client to a backend using the connection pool.
    // Retries with failover if the chosen backend is unreachable.
    pub fn to_backend(self: Arc<Self>, conn_id: usize, raw_bytes: Vec<u8>) -> bool {
        let Some(backend) = self.choose_next() else {
            tracing::error!("Cannot route packet: all backend servers are OFFLINE!");
            return false;
        };

        let router_clone = Arc::clone(&self);

        tokio::spawn(async move {
            let mut current_backend = backend;
            let mut data_sent = false;

            while !data_sent {
                tracing::debug!(
                    "Forwarding packet from client {} to backend {}",
                    conn_id,
                    current_backend._addr
                );

                // Acquire a pooled or new connection
                let Some(mut conn) = current_backend.acquire().await else {
                    tracing::error!("No connection available for {}", current_backend._addr);
                    // Try to find another backend
                    match router_clone.choose_next() {
                        Some(next) => {
                            current_backend = next;
                            continue;
                        }
                        None => {
                            tracing::error!(
                                "CRITICAL: No fallback upstreams. Packet from client {} lost.",
                                conn_id
                            );
                            break;
                        }
                    }
                };

                // Build payload: [conn_id: 8 bytes][data: N bytes]
                use tokio::io::AsyncWriteExt;
                let mut payload = Vec::with_capacity(8 + raw_bytes.len());
                payload.extend_from_slice(&conn_id.to_be_bytes());
                payload.extend_from_slice(&raw_bytes);

                match conn.write_all(&payload).await {
                    Ok(_) => {
                        tracing::debug!(
                            "Packet from client {} delivered to {}",
                            conn_id,
                            current_backend._addr
                        );
                        current_backend.release(conn).await;
                        data_sent = true;
                    }
                    Err(e) => {
                        // Connection is broken — drop it, run express health check
                        tracing::error!("Write failed to {}: {}", current_backend._addr, e);

                        tracing::warn!(
                            "Running express health check for {}",
                            current_backend._addr
                        );

                        let mut is_alive = false;
                        for _attempt in 1..=2 {
                            let (status, _) = router_clone.ping(&current_backend._addr).await;
                            if status == BackendStatus::Online {
                                is_alive = true;
                                break;
                            }
                        }

                        if !is_alive {
                            // Mark backend as offline and switch to fallback
                            tracing::error!(
                                "Backend {} is dead. Marking Offline.",
                                current_backend._addr
                            );
                            {
                                let mut upstreams = router_clone._upstreams.write();
                                if let Some((id, backend)) = upstreams
                                    .iter_mut()
                                    .find(|(_, b)| b._addr == current_backend._addr)
                                {
                                    // Rebuild with Offline status since BackendPool fields are not mut
                                    let updated = Arc::new(BackendPool::new(
                                        backend._addr.clone(),
                                        id,
                                        BackendStatus::Offline,
                                        0,
                                    ));
                                    *backend = updated;
                                }
                            }

                            match router_clone.choose_next() {
                                Some(next) => current_backend = next,
                                None => {
                                    tracing::error!(
                                        "CRITICAL: No fallback upstreams. Packet from client {} lost.",
                                        conn_id
                                    );
                                    break;
                                }
                            }
                        } else {
                            // Server recovered — short pause, retry same backend
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        });

        true
    }

    // Ping the address, then add it to the upstream pool if online
    pub async fn add_rout_server(&self, addr: &str) -> bool {
        let (status, latency_ms) = self.ping(addr).await;

        if status == BackendStatus::Offline {
            tracing::error!("Failed to add server {}: backend is OFFLINE", addr);
            return false;
        }

        let mut pool = self._upstreams.write();
        let entry = pool.vacant_entry();
        let id = entry.key();

        let backend = Arc::new(BackendPool::new(addr.to_string(), id, status, latency_ms));

        // Warm up pool in background so we don't block here
        let backend_clone = Arc::clone(&backend);
        tokio::spawn(async move {
            backend_clone.warmup(POOL_WARMUP_COUNT).await;
        });

        entry.insert(backend);
        info!("New backend added to pool - id: {}, addr: {}", id, addr);

        true
    }

    // Remove a backend server from the pool by ID
    pub fn delete_rout_server(&self, id: usize) -> bool {
        let mut pool = self._upstreams.write();

        if pool.contains(id) {
            pool.remove(id);
            info!("Backend server with id: {} successfully removed", id);
            true
        } else {
            tracing::warn!("Failed to delete server: id {} not found", id);
            false
        }
    }
}
