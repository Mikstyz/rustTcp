pub struct Server {
    _addr: String,
    _proxy_addr: String,
}

impl Server {
    pub fn new(addr: &str, proxy_addr: &str, buffer_size: usize) -> Self {
        Self {
            _addr: addr.to_string(),
            _proxy_addr: proxy_addr.to_string(),
        }
    }
}
