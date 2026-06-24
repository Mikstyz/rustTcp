use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let mut stream = TcpStream::connect("127.0.0.1:8080").await.unwrap();
    tracing::info!("Connected to proxy at 127.0.0.1:8080");

    // Send test messages
    let messages = vec!["Hello from client!", "Second message", "Third message"];

    for msg in messages {
        tracing::info!("Sending: {}", msg);

        if let Err(e) = stream.write_all(msg.as_bytes()).await {
            tracing::error!("Failed to send: {}", e);
            break;
        }

        // Small pause between sends
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    // Keep alive and read any responses
    let mut buf = vec![0u8; 4096];
    loop {
        match stream.read(&mut buf).await {
            Ok(0) => {
                tracing::info!("Server closed connection");
                break;
            }
            Ok(n) => {
                tracing::info!("Received: {}", String::from_utf8_lossy(&buf[..n]));
            }
            Err(e) => {
                tracing::error!("Read error: {}", e);
                break;
            }
        }
    }
}
