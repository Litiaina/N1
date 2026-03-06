use crate::websocket::ws_structs::{StorageEvent, StorageEventPayload};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::broadcast;
use tracing::info;

const CHANNEL_CAPACITY: usize = 10_000;
static GLOBAL_WS_MANAGER: OnceLock<WsConnectionManager> = OnceLock::new();

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

    pub fn cleanup_connection(&self, uid: &str) {
        if let Some(entry) = self.sessions.get(uid) {
            if entry.receiver_count() == 0 {
                info!("Cleaning up empty session for user: {}", uid);
                drop(entry);
                self.sessions.remove(uid);
            }
        }
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
                Err(_) => 0,
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

        self.sessions.retain(|_, sender| {
            if sender.receiver_count() > 0 {
                let _ = sender.send(event.clone());
                true
            } else {
                false
            }
        });
    }

    pub fn broadcast_announcement(&self, message: &str, severity: &str) {
        self.broadcast_system_wide(StorageEventPayload::SystemMessage {
            message: message.to_string(),
            severity: severity.to_string(),
        });
    }
}

pub fn register_global_ws_manager(manager: WsConnectionManager) {
    let _ = GLOBAL_WS_MANAGER.set(manager);
}
