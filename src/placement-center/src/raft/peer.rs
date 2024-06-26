use clients::{placement::placement::call::send_raft_message, poll::ClientPool};
use common_base::log::{debug_meta, error_meta, info_meta};
use protocol::placement_center::generate::placement::SendRaftMessageRequest;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct PeerMessage {
    pub to: String,
    pub data: Vec<u8>,
}

pub struct PeersManager {
    peer_message_recv: mpsc::Receiver<PeerMessage>,
    client_poll: Arc<ClientPool>,
}

impl PeersManager {
    pub fn new(
        peer_message_recv: mpsc::Receiver<PeerMessage>,
        client_poll: Arc<ClientPool>,
    ) -> PeersManager {
        let pm = PeersManager {
            peer_message_recv,
            client_poll,
        };
        return pm;
    }

    pub async fn start(&mut self) {
        info_meta(&format!(
            "Starts the thread that sends Raft messages to other nodes"
        ));
        loop {
            if let Some(data) = self.peer_message_recv.recv().await {
                let addr = data.to;
                let request = SendRaftMessageRequest { message: data.data };
                match send_raft_message(self.client_poll.clone(), vec![addr.clone()], request).await
                {
                    Ok(_) => debug_meta(&format!("Send Raft message to node {} Successful.", addr)),
                    Err(e) => error_meta(&format!(
                        "Failed to send data to {}, error message: {}",
                        addr,
                        e.to_string()
                    )),
                }
            }
        }
    }
}
