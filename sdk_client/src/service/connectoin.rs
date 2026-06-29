use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

const READ_BUFFER_SIZE: usize = 4096;

// =============================================
//         Frame format: [size: 4 bytes][data: N bytes]
// =============================================

async fn read_frame(stream: &mut (impl AsyncReadExt + Unpin)) -> Result<Vec<u8>, std::io::Error> {
    // Read exactly 4 bytes to get frame size
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = u32::from_be_bytes(size_buf) as usize;

    // Read exactly size bytes
    let mut data = vec![0u8; size];
    stream.read_exact(&mut data).await?;

    Ok(data)
}

async fn write_frame(
    stream: &mut (impl AsyncWriteExt + Unpin),
    data: &[u8],
) -> Result<(), std::io::Error> {
    // Write size as 4 bytes then data
    let size = (data.len() as u32).to_be_bytes();
    stream.write_all(&size).await?;
    stream.write_all(data).await?;
    Ok(())
}

// =============================================
//         Sender half after split()
// =============================================

pub struct ConnectionSender {
    _stream: Arc<Mutex<TcpStream>>,
}

impl ConnectionSender {
    // Send raw bytes wrapped in a frame
    pub async fn send(&self, bytes: &[u8]) -> Result<(), std::io::Error> {
        let mut stream = self._stream.lock().await;
        write_frame(&mut *stream, bytes).await?;
        tracing::debug!("Sent frame: {} bytes", bytes.len());
        Ok(())
    }
}

// =============================================
//              Connection
// =============================================

pub struct Connection {
    _stream: TcpStream,
}

impl Connection {
    // Connect to proxy
    pub async fn connect(addr: &str) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        tracing::info!("Connected to proxy at {}", addr);
        Ok(Self { _stream: stream })
    }

    // Ping proxy — returns latency in ms or None if unreachable
    pub async fn ping(addr: &str, timeout_ms: u64) -> Option<u64> {
        let start = std::time::Instant::now();
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            TcpStream::connect(addr),
        )
        .await;

        match result {
            Ok(Ok(_)) => {
                let latency = start.elapsed().as_millis() as u64;
                tracing::debug!("Ping to {} — {}ms", addr, latency);
                Some(latency)
            }
            _ => {
                tracing::warn!("Ping failed to {}", addr);
                None
            }
        }
    }

    // Send raw bytes wrapped in a frame
    pub async fn send(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> {
        write_frame(&mut self._stream, bytes).await?;
        tracing::debug!("Sent frame: {} bytes", bytes.len());
        Ok(())
    }

    // Read one complete frame — blocks until full frame arrives
    pub async fn recv(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let data = read_frame(&mut self._stream).await?;
        tracing::debug!("Received frame: {} bytes", data.len());
        Ok(data)
    }

    // Graceful disconnect
    pub async fn disconnect(mut self) -> bool {
        match self._stream.shutdown().await {
            Ok(_) => {
                tracing::info!("Disconnected from proxy");
                true
            }
            Err(e) => {
                tracing::error!("Disconnect error: {}", e);
                false
            }
        }
    }

    // Split into sender + receiver channel for simultaneous send and receive
    pub fn split(self) -> (ConnectionSender, mpsc::Receiver<Vec<u8>>) {
        let stream = Arc::new(Mutex::new(self._stream));
        let read_stream = Arc::clone(&stream);

        let sender = ConnectionSender { _stream: stream };

        let (tx, rx) = mpsc::channel(256);

        tokio::spawn(async move {
            loop {
                // Lock only for the duration of reading one frame
                let result = {
                    let mut s = read_stream.lock().await;
                    read_frame(&mut *s).await
                };

                match result {
                    Ok(data) => {
                        tracing::debug!("Split received frame: {} bytes", data.len());
                        if tx.send(data).await.is_err() {
                            // Receiver dropped — stop listening
                            break;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        tracing::info!("Proxy closed connection");
                        break;
                    }
                    Err(e) => {
                        tracing::error!("Split read error: {}", e);
                        break;
                    }
                }
            }
        });

        (sender, rx)
    }
}
