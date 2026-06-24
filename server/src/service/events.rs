use crate::entities::enum_task;
use crate::service::connection::{self};
use parking_lot::RwLock;
use slab::Slab;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::usize;
use tokio::sync::{Notify, mpsc};

// ==================================================================================
// 1. Create interested connectoin list
// 2. If connection send data that | interested connectoin -> waiting connection |
// 3. loop waiting connection and performance
// 4. if waiting connectoin list is empty that freze loop
// connection life on interested list: 10 - 300 second
// ==================================================================================
//
// ====================================
// Events
// ====================================
pub struct InterestPool {
    //interest (all open connection)
    _connection_pool: Arc<RwLock<Slab<connection::Connection>>>,

    // waiting (only active and thow waiting answer)
    _waiting_pool: WaitingPool,

    //time out setting
    _timeout_second: usize,
    _update_time_second: usize,
}

impl InterestPool {
    pub fn new(timeout_second: usize, update_time_second: usize, concurrency: usize) -> Self {
        let mut interest_pool = Self {
            //pool
            _connection_pool: Arc::new(RwLock::new(Slab::new())),
            _waiting_pool: WaitingPool::new(),
            //time
            _timeout_second: timeout_second,
            _update_time_second: update_time_second,
        };

        tracing::info!("clone connectoin pool");
        let interest_pool_clone = Arc::clone(&interest_pool._connection_pool);

        tracing::info!("start workers: {}", concurrency);

        interest_pool
            ._waiting_pool
            .run_loop(interest_pool_clone, concurrency);

        interest_pool
    }

    //server to clinet
    pub fn new_event(&self, task: enum_task::Task) {
        //defrost
        self._waiting_pool.defrost();

        let tx = self._waiting_pool.task_router();

        tokio::spawn(async move {
            if let Err(e) = tx.send(task).await {
                tracing::error!("Failed to send task to workers: {}", e);
            }
        });
    }

    pub fn run_waiting() {}

    pub fn add_connection(&mut self, mut conn: connection::Connection) {
        let mut pool_guard = self._connection_pool.write();

        let entry = pool_guard.vacant_entry();
        let conn_id = entry.key();

        conn.update_id(conn_id);

        tracing::info!("New connection id: {}", conn_id);
        entry.insert(conn);
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

    // ====================================
    // end note
    // ====================================
}

// ====================================
// WAITING
// ====================================
const WAITING_SIZE_TASK_BUFFER: usize = 1024;

struct WaitingPool {
    //waiting
    _rx: Option<mpsc::Receiver<enum_task::Task>>, // queue task
    _tx: mpsc::Sender<enum_task::Task>,           // transmitter

    //frozen
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

    //
    //
    //

    //loop freze
    // fn freeze(&self) {
    //     tracing::info!("freze waiting loop...");
    //     self._is_frozen.store(true, Ordering::Relaxed);
    // }

    //loop defrost
    fn defrost(&self) {
        tracing::info!("defrost waiting loop...");
        self._is_frozen.store(false, Ordering::Relaxed);
        self._freeze_notify.notify_one(); //to wake up async loop if on sleep
    }

    //
    //
    //

    fn task_router(&self) -> mpsc::Sender<enum_task::Task> {
        self._tx.clone()
    }

    fn run_loop(
        &mut self,
        interest_pool: std::sync::Arc<parking_lot::RwLock<slab::Slab<connection::Connection>>>,
        concurrency: usize,
    ) {
        //clone point on other tread
        let is_frozen = Arc::clone(&self._is_frozen);
        let freeze_notify = Arc::clone(&self._freeze_notify);

        //get rx to on Arc<Mutex> (reading all worker)
        let rx = match self._rx.take() {
            Some(r) => Arc::new(tokio::sync::Mutex::new(r)),
            None => {
                tracing::warn!("waiting loop is running");
                return;
            }
        };

        //working task
        tracing::info!("Running {} task worker", concurrency);

        for worker_id in 0..concurrency {
            let is_fronzen = Arc::clone(&is_frozen);
            let freeze_notify = Arc::clone(&freeze_notify);
            let interest_pool = Arc::clone(&interest_pool);
            let rx = Arc::clone(&rx);

            tokio::spawn(async move {
                tracing::info!("Worker #{} started", worker_id);

                loop {
                    //check is freeze loop
                    if is_fronzen.load(Ordering::Relaxed) {
                        freeze_notify.notified().await;
                    }

                    //take one task in pool
                    let task_option: Option<enum_task::Task> = {
                        let mut rx_guard = rx.lock().await;
                        rx_guard.recv().await
                    }; //die rx guard

                    // if chanel is close -> die worker
                    let Some(task) = task_option else {
                        break;
                    };

                    match task {
                        enum_task::Task::ReadData { conn_id } => {
                            let pool_guard = interest_pool.read();
                            if let Some(conn) = pool_guard.get(conn_id) {
                                // Read data
                            }
                        }

                        enum_task::Task::SendData { conn_id, payload } => {
                            let pool_guard = interest_pool.read();
                            if let Some(conn) = pool_guard.get(conn_id) {
                                // send data
                            }
                        }
                    }
                }
                tracing::info!("Worker #{} stopped", worker_id);
            });
        }
    }
}
// ====================================
//
// ====================================
