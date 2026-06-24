pub struct Router {
    _processing_servers_addr: String,
}

impl Router {
    pub fn new() -> Self {
        Self {
            _processing_servers_addr: "".to_string(),
        }
    }
}
