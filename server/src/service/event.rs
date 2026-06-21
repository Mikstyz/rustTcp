use crate::service::connection;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tracing::{debug, info};
use tracing_subscriber::fmt::time;

// ==================================================================================
// 1. Create interested connectoin list
// 2. If connection send data that | interested connectoin -> waiting connection |
// 3. loop waiting connection and performance
// 4. if waiting connectoin list is empty that freze loop
// connection life on interested list: 10 - 300 second
// ==================================================================================

const WAITING_LOOP_TIMOUT_MILLIS: u64 = 10;

pub struct Ivent {
    //interest (all open connection)
    _interest_pool: Vec<connection::Connection>,

    // waiting (only active and thow waiting answer)
    _waiting_pool: Vec<usize>,

    //time out setting
    _timeout_second: u16,
    _update_time_second: u8,

    //frozen
    _is_frozen: Arc<AtomicBool>,
    _freeze_notify: Arc<Notify>,
}

impl Ivent {
    pub fn new(timeout_second: u16, update_time_second: u8) -> Self {
        Self {
            _interest_pool: Vec::new(),
            _waiting_pool: Vec::new(),
            //
            _timeout_second: timeout_second,
            _update_time_second: update_time_second,
            //
            _is_frozen: Arc::new(AtomicBool::new(false)),
            _freeze_notify: Arc::new(Notify::new()),
        }
    }

    //loop freze
    fn waiting_freeze(&self) {
        tracing::info!("freze waiting loop...");
        self._is_frozen.store(true, Ordering::Relaxed);
    }

    //loop defrost
    fn waiting_defrost(&self) {
        tracing::info!("defrost waiting loop...");
        self._is_frozen.store(false, Ordering::Relaxed);
        self._freeze_notify.notify_one(); //to wake up async loop if on sleep
    }

    pub fn run(&self) {
        //clone point on other tread
        let is_frozen = Arc::clone(&self._is_frozen);
        let freeze_notify = Arc::clone(&self._freeze_notify);

        tracing::info!(
            "run waiting loop, Actinve connection: {}",
            &self._waiting_pool.len()
        );

        debug!("tokio spawn");
        tokio::spawn(async move {
            loop {
                //check freze
                if is_frozen.load(Ordering::Relaxed) {
                    tracing::debug!("Loop freze, waiting defrost()...");
                    freeze_notify.notified().await;
                    tracing::debug!("Loop defrost");
                }

                //
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    WAITING_LOOP_TIMOUT_MILLIS,
                ))
                .await;
            }
        });
    }
}
