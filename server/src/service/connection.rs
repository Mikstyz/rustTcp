use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::debug;
//use tokio::net::TcpStream;
use common::time;
use tokio::sync::mpsc;

const STATUS_DIE: u8 = 0;
//const STATUS_SLEEP: u8 = 1;
const STATUS_LIFE: u8 = 2;

pub struct Connection {
    _user_id: u64,
    _tx: mpsc::Sender<String>,
    _client_endpoint: SocketAddr,

    //connection time
    _time_stamp_connection: u64,
    _status: u8,
}

impl Connection {
    //create new connection from server
    pub fn new(user_id: u64, client_endpoint: SocketAddr, tx: mpsc::Sender<String>) -> Self {
        Self {
            _user_id: user_id,
            _client_endpoint: client_endpoint,
            _tx: tx,

            _time_stamp_connection: (time::timestamp()),
            _status: STATUS_LIFE,
        }
    }

    //print info for connectoin
    pub fn print(&self) {
        println!(
            "Connection -> Endpoint: {}, Timestamp: {}, Status: {}",
            self._client_endpoint, self._time_stamp_connection, self._status
        );
    }

    //==========================================================
    //GET
    //==========================================================

    // get id connection
    pub fn get_id(&self) -> &u64 {
        debug!("id: {}", &self._user_id);
        &self._user_id
    }

    // get xt connection
    pub fn get_tx(&self) -> &mpsc::Sender<String> {
        &self._tx
    }

    // get name connection
    pub fn get_client_endpoint(&self) -> SocketAddr {
        debug!("{}", self._client_endpoint);
        self._client_endpoint
    }

    // get creation time connection
    pub fn get_time_stamp(&self) -> &u64 {
        debug!("{}", &self._time_stamp_connection);
        &self._time_stamp_connection
    }

    //is life connectoin
    pub fn is_life(&self, lifetime: u16) -> bool {
        let is_life = (self.get_time_stamp() + lifetime as u64) > time::timestamp();
        debug!("{}", is_life);
        is_life
    }

    //==========================================================
    //Update
    //==========================================================

    //Update the lifetime if there was interaction from the connection
    pub fn update_time_stamp(&mut self) {
        self._time_stamp_connection = time::timestamp();

        if self._status < STATUS_LIFE {
            self._status = STATUS_LIFE;
        }
    }

    //download connectoin status
    pub fn downgrade_status(&mut self) -> bool {
        match self._status {
            STATUS_DIE => false,
            _ => {
                self._status -= 1;
                true
            }
        }
    }
}
