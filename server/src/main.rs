// use tokio::time::error;
use tracing::debug; //error, info};

pub mod service {
    pub mod connection;
    pub mod events;
    pub mod router;
    pub mod listener;
}

pub mod entities {
    pub mod enum_task;
}

pub mod config {
    pub mod config;
}

pub mod test {
    pub mod server_test;
}

const CONFIG_DIR: &str = "ServerConfig.json";

#[tokio::main]
async fn main() {
    //loging
    tracing_subscriber::fmt::init();

    //config load
    let config = config::config::Config::load(CONFIG_DIR.to_string());
    debug!("config: \n{:?}", config);
    //run server
}

// ==================================================================
//                                NOTE
// =================================================================
//
// 1. Прописать tcp server
//  1. работа с events
//
// 2. Прописать router
//  1.1 для выведения данных на другие сервисы
//  1.2 добавление серверов обработчиков в пулл из конфига
//  1.3
//
// =================================================================
