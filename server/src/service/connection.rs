use bytes::BytesMut;
use common::time;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tracing::debug;

const STATUS_DIE: u8 = 0;
const STATUS_LIFE: u8 = 2;

//4kb memory
const READ_BUFFER_SIZE: usize = 4096;
const WRITE_BUFFER_SIZE: usize = 4096;

const CHUNK_SIZE: usize = 1024;

pub struct Connection {
    _id: usize,
    _endpoint: SocketAddr,

    pub stream: TcpStream,

    pub read_buffer: BytesMut,
    pub write_buffer: BytesMut,

    _time_stamp: u64,
    _status: u8,
}

impl Connection {
    pub fn new(client_endpoint: SocketAddr, stream: TcpStream) -> Self {
        Self {
            _id: 0,
            _endpoint: client_endpoint,
            stream,

            read_buffer: BytesMut::with_capacity(READ_BUFFER_SIZE),
            write_buffer: BytesMut::with_capacity(WRITE_BUFFER_SIZE),
            _time_stamp: time::timestamp(),
            _status: STATUS_LIFE,
        }
    }

    pub fn print(&self) {
        println!(
            "Connection -> _connection_id: {},  Endpoint: {}, Timestamp: {}, Status: {}",
            self._id, self._endpoint, self._time_stamp, self._status
        );
    }

    //==========================================================
    // NETWORK OPERATIONS
    //==========================================================

    pub fn read_to_buffer(&mut self) -> std::io::Result<usize> {
        let mut chunk = [0u8; CHUNK_SIZE];

        match self.stream.try_read(&mut chunk) {
            Ok(n) if n > 0 => {
                self.read_buffer.extend_from_slice(&chunk[..n]);
                self.update_time_stamp();
                Ok(n)
            }
            Ok(n) => Ok(n), // client close connection
            Err(e) => Err(e),
        }
    }

    pub fn write_to_buffer(&mut self) -> std::io::Result<usize> {
        if self.write_buffer.is_empty() {
            return Ok(0);
        }

        match self.stream.try_write(&self.write_buffer) {
            Ok(n) => {
                let _ = self.write_buffer.split_to(n);
                self.update_time_stamp();
                Ok(n)
            }
            Err(e) => Err(e),
        }
    }

    //==========================================================
    // GETTERS
    //==========================================================

    pub fn get_id(&self) -> usize {
        self._id
    }

    pub fn get_connection_endpoint(&self) -> SocketAddr {
        debug!("{}", self._endpoint);
        self._endpoint
    }

    pub fn get_time_stamp(&self) -> u64 {
        self._time_stamp
    }

    pub fn is_life(&self, lifetime: u16) -> bool {
        let is_life = (self.get_time_stamp() + lifetime as u64) > time::timestamp();
        debug!("{}", is_life);
        is_life
    }

    //==========================================================
    // UPDATES
    //==========================================================

    pub fn update_time_stamp(&mut self) {
        self._time_stamp = time::timestamp();
        if self._status < STATUS_LIFE {
            self._status = STATUS_LIFE;
        }
    }

    pub fn update_id(&mut self, id: usize) {
        self._id = id;
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
