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
    //fast send nudes this clinent
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

//note
//
//изменить user на структуру какую-то или по типу
//
//

pub struct ConnectionManager {
    // connections for server
    // key - ip
    // res - conn
    connections: HashMap<String, Connection>,

    // life time connection
    timeout_second: u16,

    // check life for connections
    update_time_second: u8,

    //len conn
    len_life_conn: u32,
}

impl ConnectionManager {
    // connections manager
    pub fn new(timeout_second: u16, update_time_second: u8) -> Self {
        Self {
            connections: HashMap::new(),
            timeout_second: timeout_second,
            update_time_second: update_time_second,
            len_life_conn: 0,
        }
    }

    pub fn print(&self) {
        println!("=== CONNECTION MANAGER ===");
        println!("TimeOut (second): {}", self.timeout_second);
        println!("interval update (second): {}", self.update_time_second);
        println!("Len life connectoin: {}", self.len_life_conn);
        println!("All table recordings: {}", self.connections.len());
        println!("-----------------------------------");

        if self.connections.is_empty() {
            println!("connectoin list no have empty");
        } else {
            for (ip, conn) in &self.connections {
                // Выводим ключ HashMap и вызываем функцию print() самого соединения
                print!("IP: [{}] -> ", ip);
                conn.print();
            }
        }
        println!("===================================");
    }

    // get len life connection
    pub fn len(&self) -> u32 {
        debug!("{}", self.len_life_conn);
        self.len_life_conn
    }

    // add new connection
    pub fn add(&mut self, ip: String, new_conn: Connection) -> Result<&'static str, &'static str> {
        // check, if this user founf for connections
        if self.connections.contains_key(&ip) {
            // if rewriting found, counter len_life_conn increase no need
            self.connections.insert(ip, new_conn);
            debug!("connectoin update");
            Ok("connectoin update")
        } else {
            // if this new connection
            self.connections.insert(ip, new_conn);
            self.len_life_conn += 1;
            debug!("connectoin added");
            Ok("connectoin added")
        }
    }

    // delete connection
    pub fn delete(&mut self, ip: &str) -> Result<&'static str, &'static str> {
        // .remove() return Some(connection), if element found, or  None, if elem not found
        if self.connections.remove(ip).is_some() {
            // decrease counter only if real delete
            if self.len_life_conn > 0 {
                self.len_life_conn -= 1;
            }
            debug!("connectoin delete");
            Ok("connectoin delete")
        } else {
            // if user not dound for HashMap
            debug!("connectoin not found");
            Err("connection not found")
        }
    }

    pub fn update(&mut self) -> u32 {
        // create vector for connection which need to delete
        let mut ips_to_delete = Vec::new();

        // at first riding map
        for (ip, connection) in self.connections.iter_mut() {
            if !connection.is_life(self.timeout_second) {
                // if status not downgrate means connectoin die - we are planning delete this connection
                if !connection.downgrade_status() {
                    // clone ip to delete later
                    ips_to_delete.push(ip.clone());
                }
            }
        }

        //delete connectoin count
        let mut deleted_count = 0;

        //get for ip whict should be removed
        for ip in ips_to_delete {
            if self.delete(&ip).is_ok() {
                deleted_count += 1;
            }
        }

        // return count remove connection
        deleted_count
    }
}
