use std::sync::Arc;

use axum::{
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::IntoResponse,
};
use futures_util::StreamExt;
use jsonwebtoken::{Validation, decode};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::{
    middleware::auth_middleware::JWT_KEYS, routes::storage_routes::AppState,
    structs::auth_structs::Claims, websocket::ws_structs::WsAuthParams,
};

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<WsAuthParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let decoding_key = &JWT_KEYS.decoding;
    let validation = Validation::default();

    let claims = match decode::<Claims>(&params.token, decoding_key, &validation) {
        Ok(c) => c.claims,
        Err(e) => {
            warn!("Websocket auth failed: {}", e);
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    ws.on_upgrade(move |socket| handle_socket(socket, state, claims))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, claims: Claims) {
    let uid = claims.uid.clone();
    info!("Websocket connected: client {}", uid);

    let mut rx = {
        let entry = state.user_channels.entry(uid.clone()).or_insert_with(|| {
            let (tx, _rx) = broadcast::channel(4096);
            tx
        });
        entry.subscribe()
    };

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        if let Ok(json) = serde_json::to_string(&event.payload) {
                            if let Err(e) = socket.send(Message::Text(json.into())).await {
                                warn!("Websocket send error for {}: {}", uid, e);
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {      
                        warn!("Websocket client {} lagged and skipped {} messages", uid, skipped);
                    }
                    Err(_) => break,
                }
            }
            
            msg = socket.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => {
                        info!("Websocket disconnected: client {}", uid);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
}
