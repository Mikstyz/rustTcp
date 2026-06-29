use crate::entities::enum_task;
use crate::service::connection::{self};
use crate::service::router::Router;
use parking_lot::RwLock;
use slab::Slab;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::usize;
use tokio::sync::{Notify, mpsc};

const CONNECTION_LIFETIME_SECS: u16 = 10;

pub struct InterestPool {
    _connection_pool: Arc<RwLock<Slab<connection::Connection>>>,
    _waiting_pool: WaitingPool,
    _timeout_second: usize,
    _update_time_second: usize,
    _backend_is_life_second: usize,
}

impl InterestPool {
    pub fn new(
        timeout_second: usize,
        update_time_second: usize,
        concurrency: usize,
        backend_is_life_second: usize,
        //
        router: Arc<Router>,
    ) -> Self {
        let mut interest_pool = Self {
            _connection_pool: Arc::new(RwLock::new(Slab::new())),
            _waiting_pool: WaitingPool::new(),
            _timeout_second: timeout_second,
            _update_time_second: update_time_second,
            _backend_is_life_second: backend_is_life_second,
        };

        tracing::info!("clone connection pool");
        let interest_pool_clone = Arc::clone(&interest_pool._connection_pool);

        tracing::info!("start workers: {}", concurrency);

        // Pass tx into run_loop so workers can give it to router.to_backend()
        let tx = interest_pool._waiting_pool.sender();
        interest_pool
            ._waiting_pool
            .run_loop(interest_pool_clone, concurrency, router, tx);

        interest_pool
    }

    pub fn new_event(&self, task: enum_task::Task) {
        self._waiting_pool.defrost();

        let tx = self._waiting_pool.task_router();

        tokio::spawn(async move {
            if let Err(e) = tx.send(task).await {
                tracing::error!("Failed to send task to workers: {}", e);
            }
        });
    }

    pub fn add_connection(&mut self, mut conn: connection::Connection) -> usize {
        let mut pool_guard = self._connection_pool.write();

        let entry = pool_guard.vacant_entry();
        let conn_id = entry.key();

        conn.update_id(conn_id);

        tracing::info!("New connection id: {}", conn_id);
        entry.insert(conn);

        conn_id
    }

    pub fn remove_connection(&mut self, conn: connection::Connection) {
        let conn_id = conn.get_id();
        tracing::info!("Removing connection id: {}", conn_id);

        let mut pool_guard = self._connection_pool.write();

        if pool_guard.contains(conn_id) {
            pool_guard.remove(conn_id);
        } else {
            tracing::warn!(id = conn_id, "Connection already removed or invalid");
        }
    }

    pub fn contains_connection(&self, conn_id: usize) -> bool {
        let pool_guard = self._connection_pool.read();
        pool_guard.contains(conn_id)
    }

    pub fn collector(&self) {
        let has_dead_conn = {
            let pool_read = &self._connection_pool.read();
            pool_read
                .iter()
                .any(|(_, conn)| !conn.is_life(CONNECTION_LIFETIME_SECS))
        };

        if has_dead_conn {
            let mut pool_write = self._connection_pool.write();
            pool_write.retain(|_, conn| {
                if !conn.is_life(CONNECTION_LIFETIME_SECS) {
                    tracing::warn!("Closing connection due to AFK timeout");
                    false
                } else {
                    true
                }
            });
        }
    }
}

// ====================================
// WAITING
// ====================================

const WAITING_SIZE_TASK_BUFFER: usize = 1024;

struct WaitingPool {
    _rx: Option<mpsc::Receiver<enum_task::Task>>,
    _tx: mpsc::Sender<enum_task::Task>,
    _is_frozen: Arc<AtomicBool>,
    _freeze_notify: Arc<Notify>,
}

impl WaitingPool {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel(WAITING_SIZE_TASK_BUFFER);

        Self {
            _rx: Some(rx),
            _tx: tx,
            _is_frozen: Arc::new(AtomicBool::new(false)),
            _freeze_notify: Arc::new(Notify::new()),
        }
    }

    fn defrost(&self) {
        tracing::info!("defrost waiting loop...");
        self._is_frozen.store(false, Ordering::Relaxed);
        self._freeze_notify.notify_one();
    }

    fn task_router(&self) -> mpsc::Sender<enum_task::Task> {
        self._tx.clone()
    }

    // Expose tx so InterestPool::new() can pass it into run_loop
    fn sender(&self) -> mpsc::Sender<enum_task::Task> {
        self._tx.clone()
    }

    fn run_loop(
        &mut self,
        interest_pool: Arc<RwLock<Slab<connection::Connection>>>,
        concurrency: usize,
        router: Arc<Router>,
        tx: mpsc::Sender<enum_task::Task>, // used by router.to_backend() to send responses back
    ) {
        let is_frozen = Arc::clone(&self._is_frozen);
        let freeze_notify = Arc::clone(&self._freeze_notify);

        let rx = match self._rx.take() {
            Some(r) => Arc::new(tokio::sync::Mutex::new(r)),
            None => {
                tracing::warn!("waiting loop is already running");
                return;
            }
        };

        tracing::info!("Running {} task workers", concurrency);

        for worker_id in 0..concurrency {
            let is_frozen = Arc::clone(&is_frozen);
            let freeze_notify = Arc::clone(&freeze_notify);
            let interest_pool = Arc::clone(&interest_pool);
            let rx = Arc::clone(&rx);
            let router = Arc::clone(&router);
            let tx = tx.clone(); // each worker gets its own clone of tx

            tokio::spawn(async move {
                tracing::info!("Worker #{} started", worker_id);

                loop {
                    // Pause if the loop is frozen
                    if is_frozen.load(Ordering::Relaxed) {
                        freeze_notify.notified().await;
                    }

                    // Pull one task from the shared channel
                    let task_option: Option<enum_task::Task> = {
                        let mut rx_guard = rx.lock().await;
                        rx_guard.recv().await
                    };

                    // Channel closed — shut down worker
                    let Some(task) = task_option else {
                        break;
                    };

                    match task {
                        enum_task::Task::ReadData { conn_id } => {
                            let raw_bytes = {
                                let pool_guard = interest_pool.read();
                                if let Some(conn) = pool_guard.get(conn_id) {
                                    match conn.read_to_buffer() {
                                        Ok(0) => {
                                            tracing::info!("Client {} disconnected", conn_id);
                                            None
                                        }
                                        Ok(n) => {
                                            tracing::info!(
                                                "Read {} bytes from conn_id: {}",
                                                n,
                                                conn_id
                                            );
                                            let bytes = conn._socket.lock().read_buffer.to_vec();
                                            conn._socket.lock().read_buffer.clear();
                                            Some(bytes)
                                        }
                                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                            // Spurious wake — no data ready yet
                                            None
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "Read error on conn_id {}: {}",
                                                conn_id,
                                                e
                                            );
                                            // Remove dead connection from pool
                                            drop(pool_guard);
                                            let mut pool_write = interest_pool.write();
                                            if pool_write.contains(conn_id) {
                                                pool_write.remove(conn_id);
                                                tracing::info!("Removed dead conn_id: {}", conn_id);
                                            }
                                            None
                                        }
                                    }
                                } else {
                                    None
                                }
                            }; // pool read lock released here

                            // Forward bytes to backend, pass tx so router can send response back
                            if let Some(bytes) = raw_bytes {
                                if !bytes.is_empty() {
                                    Arc::clone(&router).to_backend(conn_id, bytes, tx.clone());
                                }
                            }
                        }

                        // Triggered by router.from_backend() when backend sends a response
                        enum_task::Task::SendData { conn_id, payload } => {
                            let pool_guard = interest_pool.read();
                            if let Some(conn) = pool_guard.get(conn_id) {
                                match conn.write_to_buffer(&payload) {
                                    Ok(n) => {
                                        tracing::info!("Sent {} bytes to conn_id: {}", n, conn_id);
                                    }
                                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                        tracing::warn!(
                                            "Write would block for conn_id: {}",
                                            conn_id
                                        );
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "Write error for conn_id {}: {}",
                                            conn_id,
                                            e
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                tracing::info!("Worker #{} stopped", worker_id);
            });
        }
    }
}
