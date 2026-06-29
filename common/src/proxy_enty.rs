pub struct ProxyEntry {
    pub _name: String,
    pub _addr: String,
    pub _status: bool,
    pub _latency_ms: Option<u64>,
}

impl ProxyEntry {
    pub fn new(_name: String, _addr: String) -> Self {
        Self {
            _name,
            _addr,
            _status: false,
            _latency_ms: None,
        }
    }
}
