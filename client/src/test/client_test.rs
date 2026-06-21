use crate::service::clinet::Client;
use tracing::{debug, error};

//use common::time;

const DELAY_PING: u16 = 10;

pub struct TestClinet {
    _client: Client,
}

impl TestClinet {
    pub fn new(id: u64, server_addr: &str) -> Self {
        //create copy clinet
        tracing::info!("create clinet id: {}...", id);
        let ini_clinet = Client::new(id, server_addr);

        Self {
            _client: ini_clinet,
        }
    }

    pub async fn test(&self) -> u8 {
        let mut bad_test: u8 = 0;
        let data_from_server: &[u8] = &[];

        bad_test += self.iniializatoin_on_server().await;

        bad_test += self.send_message_from_server(data_from_server).await;

        bad_test += self.get_message_from_server().await;

        bad_test
    }

    async fn iniializatoin_on_server(&self) -> u8 {
        //iniializatoin clinet
        tracing::info!("iniializatoin clinet");
        if let Err(e) = &self
            ._client
            .initialization_on_server_async(DELAY_PING)
            .await
        {
            error!("error client: {}", e);
            return 1;
        }

        0
    }

    async fn send_message_from_server(&self, data: &[u8]) -> u8 {
        debug!("len data: {}", data.len());
        0
    }

    async fn get_message_from_server(&self) -> u8 {
        let data: &[u8] = &[];

        tracing::info!("data from server: {:?}", data);

        0
    }
}
