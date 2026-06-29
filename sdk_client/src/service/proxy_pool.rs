use crate::config::config::Config;
use crate::service::proxy_enty;
use std::time::Duration;
use tokio::net::TcpStream;

pub struct ProxyPool {
    _proxies: Vec<proxy_enty::ProxyEntry>,
    _timeout_ms: u64,
}

impl ProxyPool {
    pub fn from_config(config: &Config) -> Self {
        let proxies = config
            .proxies()
            .iter()
            .map(|p| proxy_enty::ProxyEntry::new(p.name.clone(), p.addr.clone()))
            .collect();

        Self {
            _proxies: proxies,
            _timeout_ms: config.timeout_ms(),
        }
    }

    // Ping a single proxy by name — updates its status and latency
    pub async fn ping_one(&mut self, name: &str) -> Option<u64> {
        let proxy = self._proxies.iter_mut().find(|p| p.name == name)?;

        let start = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_millis(self._timeout_ms),
            TcpStream::connect(&proxy.addr),
        )
        .await;

        match result {
            Ok(Ok(_)) => {
                let latency = start.elapsed().as_millis() as u64;
                proxy.status = true;
                proxy.latency_ms = Some(latency);
                tracing::info!("Proxy {} [{}] — {}ms", proxy.name, proxy.addr, latency);
                Some(latency)
            }
            _ => {
                proxy.status = false;
                proxy.latency_ms = None;
                tracing::warn!("Proxy {} [{}] — OFFLINE", proxy.name, proxy.addr);
                None
            }
        }
    }

    // Ping all proxies simultaneously — updates all statuses and latencies
    pub async fn ping_all(&mut self) {
        tracing::info!("Pinging {} proxies...", self._proxies.len());

        // Collect addr + name to avoid borrow issues
        let targets: Vec<(String, String, u64)> = self
            ._proxies
            .iter()
            .map(|p| (p.name.clone(), p.addr.clone(), self._timeout_ms))
            .collect();

        // Ping all simultaneously
        let results = futures::future::join_all(targets.iter().map(|(name, addr, timeout_ms)| {
            let addr = addr.clone();
            let timeout_ms = *timeout_ms;
            async move {
                let start = std::time::Instant::now();
                let result = tokio::time::timeout(
                    Duration::from_millis(timeout_ms),
                    TcpStream::connect(&addr),
                )
                .await;

                match result {
                    Ok(Ok(_)) => {
                        let latency = start.elapsed().as_millis() as u64;
                        (name.clone(), true, Some(latency))
                    }
                    _ => (name.clone(), false, None),
                }
            }
        }))
        .await;

        // Update statuses
        for (name, status, latency) in results {
            if let Some(proxy) = self._proxies.iter_mut().find(|p| p.name == name) {
                proxy.status = status;
                proxy.latency_ms = latency;

                match latency {
                    Some(ms) => tracing::info!("Proxy {} — {}ms", name, ms),
                    None => tracing::warn!("Proxy {} — OFFLINE", name),
                }
            }
        }
    }

    // Return name of the fastest online proxy
    pub fn fastest(&self) -> Option<&str> {
        self._proxies
            .iter()
            .filter(|p| p.status)
            .min_by_key(|p| p.latency_ms.unwrap_or(u64::MAX))
            .map(|p| p.name.as_str())
    }

    // Return addr of proxy by name
    pub fn addr_of(&self, name: &str) -> Option<&str> {
        self._proxies
            .iter()
            .find(|p| p.name == name)
            .map(|p| p.addr.as_str())
    }

    // Connect to a specific proxy by name — returns its addr for Connection::connect()
    pub fn connect_to(&self, name: &str) -> Result<&str, String> {
        match self._proxies.iter().find(|p| p.name == name) {
            Some(proxy) if proxy.status => Ok(&proxy.addr),
            Some(proxy) => Err(format!("Proxy {} is OFFLINE", proxy.name)),
            None => Err(format!("Proxy {} not found", name)),
        }
    }

    // Ping all then return addr of the fastest online proxy
    pub async fn connect_fastest(&mut self) -> Result<&str, String> {
        self.ping_all().await;

        match self.fastest() {
            Some(name) => {
                let addr = self
                    ._proxies
                    .iter()
                    .find(|p| p.name == name)
                    .map(|p| p.addr.as_str())
                    .unwrap();
                tracing::info!("Fastest proxy: {} [{}]", name, addr);
                Ok(addr)
            }
            None => Err("All proxies are OFFLINE".to_string()),
        }
    }

    // Ping all and return addr of fastest proxy excluding the failed one
    pub async fn fallback(&mut self, failed_name: &str) -> Result<&str, String> {
        tracing::warn!("Proxy {} failed — looking for fallback", failed_name);
        self.ping_all().await;

        let result = self
            ._proxies
            .iter()
            .filter(|p| p.status && p.name != failed_name)
            .min_by_key(|p| p.latency_ms.unwrap_or(u64::MAX))
            .map(|p| p.addr.as_str());

        match result {
            Some(addr) => Ok(addr),
            None => Err("All proxies are OFFLINE".to_string()),
        }
    }

    // Print current status of all proxies
    pub fn status(&self) {
        tracing::info!("=== Proxy Pool Status ===");
        for p in &self._proxies {
            match p.latency_ms {
                Some(ms) => tracing::info!("[Online]  {} [{}] — {}ms", p.name, p.addr, ms),
                None => tracing::info!("[Offline] {} [{}]", p.name, p.addr),
            }
        }
    }
}
