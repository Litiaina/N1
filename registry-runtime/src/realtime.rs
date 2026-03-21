use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{info, warn};
use uuid::Uuid;

use crate::auth::RegistryClaims;
use crate::models::{FragmentBindingRecord, ObjectRecord, UserRecord};

const OWNER_CHANNEL_CAPACITY: usize = 4_096;
const ADMIN_CHANNEL_CAPACITY: usize = 4_096;
const OUTBOUND_BUFFER_CAPACITY: usize = 256;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Deserialize)]
pub struct WsAuthParams {
    pub token: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "op", content = "d")]
pub enum ServerMessage {
    #[serde(rename = "HELLO")]
    Hello {
        connection_id: String,
        is_admin: bool,
    },

    #[serde(rename = "PONG")]
    Pong,

    #[serde(rename = "EVENT")]
    Event(RegistryRealtimeEvent),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", content = "d")]
pub enum ClientMessage {
    #[serde(rename = "PING")]
    Ping,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeleteUserNotice {
    pub uid: String,
    pub deleted_fragments: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RenameObjectNotice {
    pub source_fragment: String,
    pub source_key: String,
    pub destination_fragment: String,
    pub destination_key: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeleteObjectNotice {
    pub fragment: String,
    pub object_key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StateInvalidatedNotice {
    pub fragment: Option<String>,
    pub scope: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionNotice {
    pub uid: String,
    pub email: String,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TwoFactorNotice {
    pub uid: String,
    pub email: String,
    pub pending: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FragmentNotice {
    pub uid: String,
    pub fragment: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum RegistryRealtimePayload {
    #[serde(rename = "USER_CREATED")]
    UserCreated(UserRecord),

    #[serde(rename = "USER_UPDATED")]
    UserUpdated(UserRecord),

    #[serde(rename = "USER_DELETED")]
    UserDeleted(DeleteUserNotice),

    #[serde(rename = "ACCOUNT_CREATED")]
    AccountCreated(UserRecord),

    #[serde(rename = "ACCOUNT_UPDATED")]
    AccountUpdated(UserRecord),

    #[serde(rename = "ACCOUNT_2FA_SETUP")]
    AccountTwoFactorSetup(TwoFactorNotice),

    #[serde(rename = "ACCOUNT_SESSION_UPDATED")]
    AccountSessionUpdated(SessionNotice),

    #[serde(rename = "FRAGMENTS_INVALIDATED")]
    FragmentsInvalidated(FragmentNotice),

    #[serde(rename = "FRAGMENT_LINKED")]
    FragmentLinked(FragmentBindingRecord),

    #[serde(rename = "FILE_REGISTERED")]
    FileRegistered(ObjectRecord),

    #[serde(rename = "FILE_UPLOADED")]
    FileUploaded(ObjectRecord),

    #[serde(rename = "FILE_DELETED")]
    FileDeleted(DeleteObjectNotice),

    #[serde(rename = "FILE_RENAMED")]
    FileRenamed(RenameObjectNotice),

    #[serde(rename = "STATE_INVALIDATED")]
    StateInvalidated(StateInvalidatedNotice),
}

#[derive(Debug, Clone, Serialize)]
pub struct RegistryRealtimeEvent {
    pub timestamp_ms: u64,
    pub seq: u64,
    #[serde(flatten)]
    pub payload: RegistryRealtimePayload,
}

#[derive(Clone)]
pub struct RegistryRealtimeHub {
    owner_channels: Arc<DashMap<String, broadcast::Sender<RegistryRealtimeEvent>>>,
    admin_channel: broadcast::Sender<RegistryRealtimeEvent>,
    sequence: Arc<AtomicU64>,
    active_connections: Arc<AtomicUsize>,
}

impl RegistryRealtimeHub {
    pub fn new() -> Self {
        let (admin_tx, _admin_rx) = broadcast::channel(ADMIN_CHANNEL_CAPACITY);
        Self {
            owner_channels: Arc::new(DashMap::new()),
            admin_channel: admin_tx,
            sequence: Arc::new(AtomicU64::new(0)),
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn subscribe_owner(&self, uid: &str) -> broadcast::Receiver<RegistryRealtimeEvent> {
        let entry = self
            .owner_channels
            .entry(uid.to_string())
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(OWNER_CHANNEL_CAPACITY);
                tx
            });
        entry.subscribe()
    }

    pub fn subscribe_admin(&self) -> broadcast::Receiver<RegistryRealtimeEvent> {
        self.admin_channel.subscribe()
    }

    pub fn connection_opened(&self) -> usize {
        self.active_connections.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn connection_closed(&self) -> usize {
        self.active_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            })
            .map(|previous| previous.saturating_sub(1))
            .unwrap_or(0)
    }

    pub fn cleanup_owner(&self, uid: &str) {
        if let Some(entry) = self.owner_channels.get(uid) {
            if entry.receiver_count() == 0 {
                drop(entry);
                self.owner_channels.remove(uid);
            }
        }
    }

    pub fn publish_owner(&self, uid: &str, payload: RegistryRealtimePayload) {
        let event = self.event(payload);
        self.publish_admin_event(event.clone());

        if let Some(sender) = self.owner_channels.get(uid) {
            if sender.receiver_count() == 0 {
                drop(sender);
                self.owner_channels.remove(uid);
                return;
            }
            let _ = sender.send(event);
        }
    }

    pub fn publish_admin(&self, payload: RegistryRealtimePayload) {
        let event = self.event(payload);
        self.publish_admin_event(event);
    }

    fn publish_admin_event(&self, event: RegistryRealtimeEvent) {
        if self.admin_channel.receiver_count() > 0 {
            let _ = self.admin_channel.send(event);
        }
    }

    fn event(&self, payload: RegistryRealtimePayload) -> RegistryRealtimeEvent {
        RegistryRealtimeEvent {
            timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
            seq: self.sequence.fetch_add(1, Ordering::Relaxed) + 1,
            payload,
        }
    }
}

pub async fn handle_socket(socket: WebSocket, hub: RegistryRealtimeHub, claims: RegistryClaims) {
    let connection_id = Uuid::new_v4().to_string();
    let scope = if claims.is_admin { "admin" } else { "user" };
    let _ = hub.connection_opened();
    info!(
        uid = %claims.sub,
        email = %claims.email,
        scope,
        connection_id = %connection_id,
        "Registry websocket connected"
    );

    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(OUTBOUND_BUFFER_CAPACITY);
    let mut broadcast_rx = if claims.is_admin {
        hub.subscribe_admin()
    } else {
        hub.subscribe_owner(&claims.sub)
    };

    let _ = tx
        .send(ServerMessage::Hello {
            connection_id: connection_id.clone(),
            is_admin: claims.is_admin,
        })
        .await;

    let mut write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let json = match serde_json::to_string(&msg) {
                Ok(value) => value,
                Err(err) => {
                    warn!("Failed to serialize registry websocket message: {}", err);
                    continue;
                }
            };

            if sender.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
        let _ = sender.close().await;
    });

    let tx_logic = tx.clone();
    let hub_logic = hub.clone();
    let user_uid = claims.sub.clone();
    let mut logic_task = tokio::spawn(async move {
        let mut hb_interval = interval(HEARTBEAT_INTERVAL);
        hb_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut last_heartbeat = Instant::now();

        loop {
            tokio::select! {
                event_res = broadcast_rx.recv() => {
                    match event_res {
                        Ok(event) => {
                            if tx_logic.try_send(ServerMessage::Event(event)).is_err() {
                                warn!(uid = %user_uid, "Registry websocket client buffer full; disconnecting slow consumer");
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(uid = %user_uid, skipped, "Registry websocket client lagged; sending invalidation");
                            let invalidation = hub_logic.event(RegistryRealtimePayload::StateInvalidated(
                                StateInvalidatedNotice {
                                    fragment: None,
                                    scope: "full".to_string(),
                                },
                            ));
                            if tx_logic.try_send(ServerMessage::Event(invalidation)).is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = hb_interval.tick() => {
                    if last_heartbeat.elapsed() > CLIENT_TIMEOUT {
                        warn!(uid = %user_uid, "Registry websocket client timed out");
                        break;
                    }
                }
                msg = receiver.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            last_heartbeat = Instant::now();
                            if let Ok(ClientMessage::Ping) = serde_json::from_str::<ClientMessage>(&text) {
                                let _ = tx_logic.try_send(ServerMessage::Pong);
                            }
                        }
                        Some(Ok(Message::Close(_))) => break,
                        Some(Ok(_)) => {
                            last_heartbeat = Instant::now();
                        }
                        Some(Err(err)) => {
                            warn!(uid = %user_uid, "Registry websocket read error: {}", err);
                            break;
                        }
                        None => break,
                    }
                }
            }
        }
    });

    tokio::select! {
        _ = &mut write_task => logic_task.abort(),
        _ = &mut logic_task => write_task.abort(),
    }

    let remaining = hub.connection_closed();
    if !claims.is_admin {
        hub.cleanup_owner(&claims.sub);
    }
    info!(
        uid = %claims.sub,
        email = %claims.email,
        scope,
        connection_id = %connection_id,
        remaining_connections = remaining,
        "Registry websocket disconnected"
    );
}
