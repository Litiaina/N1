use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::broadcast;
use tracing::info;
use crate::websocket::ws_structs::{StorageEvent, StorageEventPayload};

const CHANNEL_CAPACITY: usize = 10_000;

#[derive(Clone)]
pub struct WsConnectionManager {
    sessions: Arc<DashMap<String, broadcast::Sender<StorageEvent>>>,
}

impl WsConnectionManager {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
        }
    }

    pub fn subscribe(&self, uid: &str) -> broadcast::Receiver<StorageEvent> {
        let entry = self.sessions.entry(uid.to_string()).or_insert_with(|| {
            info!("Creating new broadcast channel for user: {}", uid);
            let (tx, _rx) = broadcast::channel(CHANNEL_CAPACITY);
            tx
        });
        
        entry.subscribe()
    }

    pub fn publish(&self, uid: &str, event_payload: StorageEventPayload) -> usize {
        if let Some(sender) = self.sessions.get(uid) {
            if sender.receiver_count() == 0 {
                drop(sender); 
                self.sessions.remove(uid);
                return 0;
            }

            let event = StorageEvent {
                timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
                payload: event_payload,
            };

            match sender.send(event) {
                Ok(receiver_count) => receiver_count,
                Err(_) => {
                    0
                }
            }
        } else {
            0
        }
    }

    pub fn broadcast_system_wide(&self, payload: StorageEventPayload) {
        let event = StorageEvent {
            timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
            payload,
        };
        
        for r in self.sessions.iter() {
            let _ = r.value().send(event.clone());
        }
    }

    pub fn broadcast_announcement(&self, message: &str, severity: &str) {
        self.broadcast_system_wide(StorageEventPayload::SystemMessage {
            message: message.to_string(),
            severity: severity.to_string(),
        });
    }
}
