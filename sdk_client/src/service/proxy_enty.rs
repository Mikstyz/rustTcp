pub struct ProxyEntry {
    pub name: String,
    pub addr: String,
    pub status: bool,
    pub latency_ms: Option<u64>,
}

impl ProxyEntry {
    pub fn new(name: String, addr: String) -> Self {
        Self {
            name,
            addr,
            status: false,
            latency_ms: None,
        }
    }
}
