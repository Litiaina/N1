use axum::body::Body;
use axum::extract::ws::{CloseFrame, Message as AxumWsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{OriginalUri, State};
use axum::http::{Response, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;
use tokio_tungstenite::Connector;
use tokio_tungstenite::tungstenite::Message as UpstreamWsMessage;

use crate::proxy::text_response;
use crate::state::GatewayState;

use super::best_node;

pub(super) async fn websocket_proxy(
    State(state): State<Arc<GatewayState>>,
    OriginalUri(uri): OriginalUri,
    ws: WebSocketUpgrade,
) -> Response<Body> {
    let node = match best_node(&state).await {
        Ok(node) => node,
        Err(response) => return response,
    };
    let upstream_url = match websocket_upstream_url(&node.http_url, &uri) {
        Ok(url) => url,
        Err(message) => return text_response(StatusCode::BAD_GATEWAY, message),
    };
    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyUpstreamCertificate))
        .with_no_client_auth();
    let upstream = match tokio_tungstenite::connect_async_tls_with_config(
        upstream_url,
        None,
        true,
        Some(Connector::Rustls(Arc::new(tls))),
    )
    .await
    {
        Ok((socket, _)) => socket,
        Err(error) => {
            state.nodes.mark_failure(&node.id).await;
            return text_response(
                StatusCode::BAD_GATEWAY,
                format!("gateway websocket upstream {} failed: {error}", node.id),
            );
        }
    };

    state.nodes.mark_route(&node.id).await;
    ws.on_upgrade(move |client| relay_websocket(client, upstream))
        .into_response()
}

pub(super) fn websocket_upstream_url(
    http_url: &str,
    uri: &axum::http::Uri,
) -> Result<String, String> {
    let base = if let Some(rest) = http_url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = http_url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        return Err(format!("unsupported upstream URL scheme: {http_url}"));
    };
    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/noa/v1/ws");
    Ok(format!("{}{path_and_query}", base.trim_end_matches('/')))
}

async fn relay_websocket(
    client: WebSocket,
    upstream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    use futures_util::{SinkExt, StreamExt};

    let (mut client_sink, mut client_stream) = client.split();
    let (mut upstream_sink, mut upstream_stream) = upstream.split();

    loop {
        tokio::select! {
            incoming = client_stream.next() => {
                match incoming {
                    Some(Ok(message)) => {
                        let close = matches!(message, AxumWsMessage::Close(_));
                        if upstream_sink.send(axum_to_upstream_message(message)).await.is_err() || close {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        tracing::debug!("gateway websocket client receive failed: {error}");
                        break;
                    }
                    None => break,
                }
            }
            incoming = upstream_stream.next() => {
                match incoming {
                    Some(Ok(message)) => {
                        let Some(message) = upstream_to_axum_message(message) else {
                            continue;
                        };
                        let close = matches!(message, AxumWsMessage::Close(_));
                        if client_sink.send(message).await.is_err() || close {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        tracing::debug!("gateway websocket upstream receive failed: {error}");
                        break;
                    }
                    None => break,
                }
            }
        }
    }
    let _ = upstream_sink.close().await;
    let _ = client_sink.close().await;
}

pub(super) fn axum_to_upstream_message(message: AxumWsMessage) -> UpstreamWsMessage {
    match message {
        AxumWsMessage::Text(value) => UpstreamWsMessage::Text(value.to_string().into()),
        AxumWsMessage::Binary(value) => UpstreamWsMessage::Binary(value),
        AxumWsMessage::Ping(value) => UpstreamWsMessage::Ping(value),
        AxumWsMessage::Pong(value) => UpstreamWsMessage::Pong(value),
        AxumWsMessage::Close(frame) => UpstreamWsMessage::Close(frame.map(|frame| {
            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: frame.code.into(),
                reason: frame.reason.to_string().into(),
            }
        })),
    }
}

pub(super) fn upstream_to_axum_message(message: UpstreamWsMessage) -> Option<AxumWsMessage> {
    match message {
        UpstreamWsMessage::Text(value) => Some(AxumWsMessage::Text(value.to_string().into())),
        UpstreamWsMessage::Binary(value) => Some(AxumWsMessage::Binary(value)),
        UpstreamWsMessage::Ping(value) => Some(AxumWsMessage::Ping(value)),
        UpstreamWsMessage::Pong(value) => Some(AxumWsMessage::Pong(value)),
        UpstreamWsMessage::Close(frame) => {
            Some(AxumWsMessage::Close(frame.map(|frame| CloseFrame {
                code: frame.code.into(),
                reason: frame.reason.to_string().into(),
            })))
        }
        UpstreamWsMessage::Frame(_) => None,
    }
}

#[derive(Debug)]
struct AcceptAnyUpstreamCertificate;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyUpstreamCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme::*;
        vec![
            RSA_PKCS1_SHA1,
            ECDSA_SHA1_Legacy,
            RSA_PKCS1_SHA256,
            ECDSA_NISTP256_SHA256,
            RSA_PKCS1_SHA384,
            ECDSA_NISTP384_SHA384,
            RSA_PKCS1_SHA512,
            ECDSA_NISTP521_SHA512,
            RSA_PSS_SHA256,
            RSA_PSS_SHA384,
            RSA_PSS_SHA512,
            ED25519,
            ED448,
        ]
    }
}
