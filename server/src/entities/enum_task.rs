pub enum Task {
    ReadData { conn_id: usize },

    SendData { conn_id: usize, payload: Vec<u8> },
}
