use common::time;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tracing::debug;

const STATUS_DIE: u8 = 0;
//const STATUS_SLEEP: u8 = 1;
const STATUS_LIFE: u8 = 2;

pub struct Connection {
    //connection id
    _id: usize,

    //clinet ip + port
    _endpoint: SocketAddr, //ip + port

    //clinet chanel
    _tx: mpsc::Sender<String>, // cline chanel

    //clinet life
    _time_stamp: u64, //clinet connectoin time
    _status: u8,                 //status (life, sleep, die)
}

impl Connection {
    //create new connection from server
    pub fn new(client_endpoint: SocketAddr, tx: mpsc::Sender<String>) -> Self {
        Self {
            _id: 0,
            _endpoint: client_endpoint,
            _tx: tx,
            _time_stamp: (time::timestamp()),
            _status: STATUS_LIFE,
        }
    }

    //print info for connectoin
    pub fn print(&self) {
        println!(
            "Connection -> _connectoin_id: {},  Endpoint: {}, Timestamp: {}, Status: {}",
            self._id, self._endpoint, self._time_stamp, self._status
        );
    }

    //==========================================================
    //GET
    //==========================================================

    //get id connection
    pub fn get_id(&self) -> usize {
        self._id
    }

    // get xt connection
    pub fn get_tx(&self) -> &mpsc::Sender<String> {
        &self._tx
    }

    // get name connection
    pub fn get_connection_endpoint(&self) -> SocketAddr {
        debug!("{}", self._endpoint);
        self._endpoint
    }

    // get creation time connection
    pub fn get_time_stamp(&self) -> &u64 {
        debug!("{}", &self._time_stamp);
        &self._time_stamp
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
        self._time_stamp = time::timestamp();

        if self._status < STATUS_LIFE {
            self._status = STATUS_LIFE;
        }
    }

    //create id in interest pool
    pub fn update_id(&mut self, id: usize) {
        self._id = id;
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
