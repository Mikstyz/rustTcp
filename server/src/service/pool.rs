use crate::entities::enum_task;
use crate::service::connection::{self};
use crate::service::pool;
use parking_lot::RwLock;
use slab::Slab;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::usize;
use tokio::sync::{Notify, mpsc};
use tracing::info;

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
    _interest_connection_pool: Arc<RwLock<Slab<connection::Connection>>>,

    // waiting (only active and thow waiting answer)
    _waiting_pool: WaitingPool,

    //time out setting
    _timeout_second: usize,
    _update_time_second: usize,
}

impl InterestPool {
    pub fn new(timeout_second: usize, update_time_second: usize) -> Self {
        Self {
            //pool
            _interest_connection_pool: Arc::new(RwLock::new(Slab::new())),
            _waiting_pool: WaitingPool::new(),
            //time
            _timeout_second: timeout_second,
            _update_time_second: update_time_second,
        }
    }

    //server to clinet
    pub fn send_data_client(&mut self, conn_id: usize, data: Option<&[u8]>) {
        //

        //defrost
        self._waiting_pool.defrost();
    }

    //client to server
    pub fn get_data_client(&mut self, conn_id: usize) -> Option<&[u8]> {
        //

        //defrost
        self._waiting_pool.defrost();

        None
    }

    pub fn add_connection(&mut self, mut conn: connection::Connection) {
        let mut pool_guard = self._interest_connection_pool.write();

        let entry = pool_guard.vacant_entry();
        let conn_id = entry.key();

        conn.update_id(conn_id);

        tracing::info!("New connection id: {}", conn_id);
        entry.insert(conn);
    }

    pub fn remove_connection(&mut self, conn: connection::Connection) {
        let conn_id = conn.get_id();
        tracing::info!("Removing connection id: {}", conn_id);

        let mut pool_guard = self._interest_connection_pool.write();

        if pool_guard.contains(conn_id) {
            pool_guard.remove(conn_id);
        } else {
            tracing::warn!(id = conn_id, "Connection already removed or invalid");
        }
    }

    pub fn contains_connection(&self, conn_id: usize) -> bool {
        let pool_guard = self._interest_connection_pool.read();
        pool_guard.contains(conn_id)
    }

    // ====================================
    // end note
    // ====================================
}

// ====================================
// WAITING
// ====================================
const WAITING_TASK_BUFFER: usize = 1024;

pub struct WaitingPool {
    //waiting
    _rx: Option<mpsc::Receiver<enum_task::Task>>, // queue task
    _tx: mpsc::Sender<enum_task::Task>,           // transmitter

    //frozen
    _is_frozen: Arc<AtomicBool>,
    _freeze_notify: Arc<Notify>,
}

impl WaitingPool {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(WAITING_TASK_BUFFER);

        Self {
            _rx: Some(rx),
            _tx: tx,
            _is_frozen: Arc::new(AtomicBool::new(false)),
            _freeze_notify: Arc::new(Notify::new()),
        }
    }

    //loop freze
    fn freeze(&self) {
        tracing::info!("freze waiting loop...");
        self._is_frozen.store(true, Ordering::Relaxed);
    }

    //loop defrost
    fn defrost(&self) {
        tracing::info!("defrost waiting loop...");
        self._is_frozen.store(false, Ordering::Relaxed);
        self._freeze_notify.notify_one(); //to wake up async loop if on sleep
    }

    pub fn run_loop(
        &mut self,
        interest_pool: std::sync::Arc<parking_lot::RwLock<slab::Slab<connection::Connection>>>,
    ) {
        //clone point on other tread
        let is_frozen = Arc::clone(&self._is_frozen);
        let freeze_notify = Arc::clone(&self._freeze_notify);

        //get transmitter to None
        let mut rx = match self._rx.take() {
            Some(r) => r,
            None => {
                tracing::warn!("Waiting loop is runing");
                return;
            }
        };

        tracing::info!("Runing loop task Waiting");

        tokio::spawn(async move {
            while let Some(task) = rx.recv().await {
                //check freze
                if is_frozen.load(Ordering::Relaxed) {
                    tracing::debug!("loop freze...");
                    freeze_notify.notified().await;
                    tracing::debug!("loop defrost");
                }

                //working task
                tracing::info!("task running");

                match task {
                    enum_task::Task::ReadData { conn_id } => {
                        info!("ReadData: conn_id: {}", conn_id);
                        //
                        let pool_guard = interest_pool.read();

                        if let Some(conn) = pool_guard.get(conn_id) {
                            //read
                        }
                    }

                    enum_task::Task::SendData { conn_id, payload } => {
                        info!(
                            "SendData: conn_id: {}, len-payload: {}",
                            conn_id,
                            payload.len()
                        );
                        //
                        let pool_guard = interest_pool.read();

                        if let Some(conn) = pool_guard.get(conn_id) {
                            //send
                        }
                    }
                }

                //auto unlock mutex
                //clear clab
            }
        });
    }
}
// ====================================
//
// ====================================
