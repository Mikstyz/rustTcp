use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

const READ_BUFFER_SIZE: usize = 4096;

pub struct Ethernet {
    _stream: TcpStream,
}

impl Ethernet {}
