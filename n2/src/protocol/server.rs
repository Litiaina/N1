use crate::distribution_core::api::{
    get_n2_cluster_view, get_n2_cluster_view_public, get_n2_governance_state, get_n2_membership,
    put_n2_membership, resolve_n2_placement, rollback_n2_membership,
};
use crate::distribution_core::data_plane::{
    delete_n2_object, get_n2_object, list_n2_object_versions, list_n2_objects,
    put_n2_object_stream, reconcile_n2_fragment, rename_n2_object,
};
use crate::distribution_core::health::{
    get_n2_cluster_health, get_n2_fragment_health, get_n2_node_health, get_n2_object_health,
    repair_n2_object,
};
use crate::distribution_core::model::{
    ClusterHealthQuery, ClusterViewQuery, DeleteN2ObjectQuery, FragmentHealthQuery, GetObjectQuery,
    ListN2ObjectsQuery, ListN2VersionsQuery, ObjectHealthQuery, PutObjectQuery,
    ReconcileFragmentQuery, RenameN2ObjectRequest, RepairN2ObjectQuery, ResolvePlacementQuery,
    UpsertMembershipRequest,
};
use crate::enums::app_error_enum::AppError;
use crate::fs::fs_constants::MAX_FILE_SIZE;
use crate::middleware::auth_middleware::authorize_fragment_core;
use crate::structs::auth_structs::AuthorizationRequest;
use axum::Json as AxumJson;
use axum::body::to_bytes;
use axum::extract::{Path, Query};
use axum::http::{
    HeaderMap, HeaderValue,
    header::{CONTENT_LENGTH, CONTENT_TYPE, RANGE},
};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use n1::protocol::auth::NativeSession;
use n1::protocol::frame::{read_request_body, read_request_control, write_response};
use n1::protocol::types::{
    NativeAuthFragmentRequest, NativeBearerRequest, NativeOpResponse, NativeRequest, NativeResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::io::ReaderStream;
use tracing::{debug, info, warn};

const N2_NATIVE_PROTOCOL_VERSION: &str = "n2/0.1";
const NATIVE_RESPONSE_COLLECT_LIMIT: usize = 16 * 1024 * 1024;
const NATIVE_UPLOAD_DRAIN_BUFFER_BYTES: usize = 256 * 1024;
const NATIVE_UPLOAD_READER_BUFFER_BYTES: usize = 256 * 1024;
const N2_NATIVE_MAX_UPLOAD_BODY_BYTES: u64 = MAX_FILE_SIZE as u64;

#[derive(Debug)]
struct NativeExecutionError(NativeOpResponse, Vec<u8>);

impl std::fmt::Display for NativeExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "native n2 operation failed with status {}",
            self.0.status
        )
    }
}

#[derive(Debug, serde::Deserialize)]
struct FragmentObjectPayload {
    fragment: String,
    object_key: String,
    #[serde(default)]
    version_id: Option<String>,
    #[serde(default)]
    range: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ListObjectsPayload {
    fragment: String,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    continuation: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    delimiter: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct PutObjectPayload {
    fragment: String,
    object_key: String,
    #[serde(default)]
    uid: Option<String>,
    #[serde(default)]
    version_id: Option<String>,
    #[serde(default)]
    expected_epoch: Option<u64>,
    #[serde(default)]
    kc: Option<u8>,
    #[serde(default)]
    mc: Option<u8>,
    #[serde(default)]
    kd: Option<u8>,
    #[serde(default)]
    md: Option<u8>,
    #[serde(default)]
    stripe_block_size: Option<usize>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    expect_continue: bool,
}

#[derive(Debug, serde::Deserialize)]
struct DeleteObjectPayload {
    fragment: String,
    object_key: String,
    #[serde(default)]
    version_id: Option<String>,
    #[serde(default)]
    expected_epoch: Option<u64>,
    #[serde(default)]
    purge_shards: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
struct ReconcilePayload {
    fragment: String,
    #[serde(default)]
    max_objects: Option<usize>,
    #[serde(default)]
    dry_run: Option<bool>,
    #[serde(default)]
    expected_epoch: Option<u64>,
    #[serde(default)]
    cleanup_orphans: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
struct ClusterHealthPayload {
    #[serde(default)]
    expected_epoch: Option<u64>,
    #[serde(default)]
    deep: Option<bool>,
    #[serde(default)]
    max_fragments: Option<usize>,
    #[serde(default)]
    max_objects_per_fragment: Option<usize>,
}

#[derive(Debug, serde::Deserialize)]
struct FragmentHealthPayload {
    fragment: String,
    #[serde(default)]
    max_objects: Option<usize>,
}

#[derive(Debug, serde::Deserialize)]
struct RepairObjectPayload {
    fragment: String,
    object_key: String,
    #[serde(default)]
    version_id: Option<String>,
    #[serde(default)]
    expected_epoch: Option<u64>,
    #[serde(default)]
    dry_run: Option<bool>,
    #[serde(default)]
    cleanup_orphans: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
struct RollbackMembershipPayload {
    epoch: u64,
}

pub async fn bind_native_protocol(addr: SocketAddr) -> io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

pub async fn serve_native_protocol(listener: TcpListener, addr: SocketAddr) {
    info!("Native protocol => n2://{}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                if let Err(err) = stream.set_nodelay(true) {
                    warn!(
                        "N2 native connection {} failed to set TCP_NODELAY: {}",
                        peer, err
                    );
                }
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, peer).await {
                        debug!("N2 native connection {} closed: {}", peer, err);
                    }
                });
            }
            Err(err) => warn!("N2 native accept failed: {}", err),
        }
    }
}

async fn handle_connection(mut stream: TcpStream, peer: SocketAddr) -> Result<(), String> {
    let mut session = NativeSession::default();
    loop {
        let Some(request) = read_request_control(&mut stream)
            .await
            .map_err(|err| format!("read frame failed: {err}"))?
        else {
            return Ok(());
        };

        match handle_request(&mut session, peer, request, &mut stream).await {
            NativeRequestOutcome::Response(response) => {
                write_response(&mut stream, &response)
                    .await
                    .map_err(|err| format!("write frame failed: {err}"))?;
            }
            NativeRequestOutcome::Written => {}
        }
    }
}

enum NativeRequestOutcome {
    Response(NativeResponse),
    Written,
}

async fn handle_request(
    session: &mut NativeSession,
    _peer: SocketAddr,
    request: NativeRequest,
    stream: &mut TcpStream,
) -> NativeRequestOutcome {
    let id = request.id.clone();
    let response = match request.op.as_str() {
        "ping" => NativeResponse::ok(
            id,
            json!({
                "protocol": N2_NATIVE_PROTOCOL_VERSION,
                "server": "Litiaina N2",
                "transport": "n2://",
            }),
        ),
        "auth.clear" | "logout" => {
            session.clear();
            NativeResponse::ok(id, json!({"authenticated": false}))
        }
        "auth.bearer" => match payload_as::<NativeBearerRequest>(request.payload.as_deref()) {
            Ok(payload) => match session.set_bearer(payload.token) {
                Ok(claims) => NativeResponse::ok(
                    id,
                    json!({
                        "authenticated": true,
                        "fragment": claims.fragment,
                        "read": claims.read,
                        "write": claims.write,
                        "modify": claims.modify,
                        "delete": claims.delete,
                        "expires_at": claims.exp,
                    }),
                ),
                Err(err) => NativeResponse::err(id, "auth.invalid_token", err),
            },
            Err(err) => NativeResponse::err(id, "bad_request", err.to_string()),
        },
        "auth.fragment" => {
            match payload_as::<NativeAuthFragmentRequest>(request.payload.as_deref()) {
                Ok(payload) => auth_fragment(session, &id, payload).await,
                Err(err) => NativeResponse::err(id, "bad_request", err.to_string()),
            }
        }
        "session.info" => {
            let payload = match session.claims() {
                Some(claims) => json!({
                    "authenticated": true,
                    "fragment": claims.fragment,
                    "read": claims.read,
                    "write": claims.write,
                    "modify": claims.modify,
                    "delete": claims.delete,
                    "expires_at": claims.exp,
                }),
                None => json!({"authenticated": false}),
            };
            NativeResponse::ok(id, payload)
        }
        "object.get" | "object.download" => {
            return match execute_native_download(session, request.payload.as_deref(), stream, &id)
                .await
            {
                Ok(()) => NativeRequestOutcome::Written,
                Err(err) => {
                    NativeRequestOutcome::Response(NativeResponse::operation(id, err.0, err.1))
                }
            };
        }
        "object.put" => {
            return match execute_native_put(session, request, stream, &id).await {
                Ok(response) => NativeRequestOutcome::Response(response),
                Err(err) => {
                    NativeRequestOutcome::Response(NativeResponse::operation(id, err.0, err.1))
                }
            };
        }
        op => {
            let body = if let Some(body) = request.body {
                Some(body.into_vec())
            } else if let Some(body_len) = request.body_len.filter(|value| *value > 0) {
                match read_request_body(stream, body_len).await {
                    Ok(body) => Some(body),
                    Err(err) => {
                        return NativeRequestOutcome::Response(NativeResponse::err(
                            id,
                            "bad_request",
                            format!("read native body failed: {err}"),
                        ));
                    }
                }
            } else {
                None
            };
            match execute_native_control_op(session, op, request.payload.as_deref(), body).await {
                Ok((status, body)) => NativeResponse::operation(id, op_response(status), body),
                Err(err) => NativeResponse::operation(id, err.0, err.1),
            }
        }
    };
    NativeRequestOutcome::Response(response)
}

async fn execute_native_control_op(
    session: &NativeSession,
    op: &str,
    payload: Option<&str>,
    _body: Option<Vec<u8>>,
) -> Result<(u16, Vec<u8>), NativeExecutionError> {
    match op {
        "cluster.view.public" => {
            let query = payload_as::<ClusterViewQuery>(payload)?;
            let response = get_n2_cluster_view_public(Query(query)).await?;
            Ok(json_body(200, &response.0))
        }
        "cluster.view" => {
            let claims = require_claims(session)?;
            let query = payload_as::<ClusterViewQuery>(payload)?;
            let response = get_n2_cluster_view(claims, Query(query)).await?;
            Ok(json_body(200, &response.0))
        }
        "health.cluster" => {
            let claims = require_claims(session)?;
            let request = payload_as::<ClusterHealthPayload>(payload)?;
            let query = ClusterHealthQuery {
                expected_epoch: request.expected_epoch,
                deep: request.deep,
                max_fragments: request.max_fragments,
                max_objects_per_fragment: request.max_objects_per_fragment,
            };
            let response = get_n2_cluster_health(claims, Query(query)).await?;
            Ok(json_body(200, &response.0))
        }
        "health.node" => {
            let claims = require_claims(session)?;
            let response = get_n2_node_health(claims).await?;
            Ok(json_body(200, &response.0))
        }
        "health.fragment" => {
            let claims = require_claims(session)?;
            let request = payload_as::<FragmentHealthPayload>(payload)?;
            let query = FragmentHealthQuery {
                max_objects: request.max_objects,
            };
            let response =
                get_n2_fragment_health(claims, Path(request.fragment), Query(query)).await?;
            Ok(json_body(200, &response.0))
        }
        "health.object" | "object.health" | "object.status" => {
            let claims = require_claims(session)?;
            let request = payload_as::<FragmentObjectPayload>(payload)?;
            let query = ObjectHealthQuery {
                version_id: request.version_id,
            };
            let response = get_n2_object_health(
                claims,
                Path((request.fragment, request.object_key)),
                Query(query),
            )
            .await?;
            Ok(json_body(200, &response.0))
        }
        "governance.state" => {
            let claims = require_claims(session)?;
            let response = get_n2_governance_state(claims).await?;
            Ok(json_body(200, &response.0))
        }
        "membership.get" => {
            let claims = require_claims(session)?;
            let response = get_n2_membership(claims).await?;
            Ok(json_body(200, &response.0))
        }
        "membership.put" => {
            let claims = require_claims(session)?;
            let request = payload_as::<UpsertMembershipRequest>(payload)?;
            let response = put_n2_membership(claims, AxumJson(request)).await?;
            Ok(json_body(200, &response.0))
        }
        "membership.rollback" => {
            let claims = require_claims(session)?;
            let request = payload_as::<RollbackMembershipPayload>(payload)?;
            let response = rollback_n2_membership(claims, Path(request.epoch)).await?;
            Ok(json_body(200, &response.0))
        }
        "placement.resolve" => {
            let claims = require_claims(session)?;
            let request = payload_as::<FragmentObjectPayload>(payload)?;
            let query = ResolvePlacementQuery {
                uid: None,
                version_id: request.version_id,
                kc: None,
                mc: None,
            };
            let response = resolve_n2_placement(
                claims,
                Path((request.fragment, request.object_key)),
                Query(query),
            )
            .await?;
            Ok(json_body(200, &response.0))
        }
        "object.list" => {
            let claims = require_claims(session)?;
            let request = payload_as::<ListObjectsPayload>(payload)?;
            let query = ListN2ObjectsQuery {
                limit: request.limit,
                continuation: request.continuation,
                prefix: request.prefix,
                delimiter: request.delimiter,
            };
            let response = list_n2_objects(claims, Path(request.fragment), Query(query)).await?;
            Ok(json_body(200, &response.0))
        }
        "object.versions" => {
            let claims = require_claims(session)?;
            let request = payload_as::<FragmentObjectPayload>(payload)?;
            let query = ListN2VersionsQuery { limit: None };
            let response = list_n2_object_versions(
                claims,
                Path((request.fragment, request.object_key)),
                Query(query),
            )
            .await?;
            Ok(json_body(200, &response.0))
        }
        "object.delete" => {
            let claims = require_claims(session)?;
            let request = payload_as::<DeleteObjectPayload>(payload)?;
            let query = DeleteN2ObjectQuery {
                version_id: request.version_id,
                expected_epoch: request.expected_epoch,
                purge_shards: request.purge_shards,
            };
            let response = delete_n2_object(
                claims,
                Path((request.fragment, request.object_key)),
                Query(query),
            )
            .await?;
            Ok(json_body(200, &response.0))
        }
        "object.rename" => {
            let claims = require_claims(session)?;
            let request = payload_as::<RenameN2ObjectRequest>(payload)?;
            let response = rename_n2_object(claims, AxumJson(request)).await?;
            Ok(json_body(200, &response.0))
        }
        "reconcile.fragment" => {
            let claims = require_claims(session)?;
            let request = payload_as::<ReconcilePayload>(payload)?;
            let query = ReconcileFragmentQuery {
                max_objects: request.max_objects,
                dry_run: request.dry_run,
                expected_epoch: request.expected_epoch,
                cleanup_orphans: request.cleanup_orphans,
            };
            let response =
                reconcile_n2_fragment(claims, Path(request.fragment), Query(query)).await?;
            Ok(json_body(200, &response.0))
        }
        "repair.object" | "object.repair" => {
            let claims = require_claims(session)?;
            let request = payload_as::<RepairObjectPayload>(payload)?;
            let query = RepairN2ObjectQuery {
                version_id: request.version_id,
                expected_epoch: request.expected_epoch,
                dry_run: request.dry_run,
                cleanup_orphans: request.cleanup_orphans,
            };
            let response = repair_n2_object(
                claims,
                Path((request.fragment, request.object_key)),
                Query(query),
            )
            .await?;
            Ok(json_body(200, &response.0))
        }
        _ => Err(native_error(404, format!("unsupported n2:// op '{op}'"))),
    }
}

async fn execute_native_put(
    session: &NativeSession,
    request: NativeRequest,
    stream: &mut TcpStream,
    request_id: &str,
) -> Result<NativeResponse, NativeExecutionError> {
    let claims = require_claims(session)?;
    let payload = payload_as::<PutObjectPayload>(request.payload.as_deref())?;
    let body_len = request.body_len.unwrap_or_else(|| {
        request
            .body
            .as_ref()
            .map(|body| body.len() as u64)
            .unwrap_or(0)
    });
    if body_len > N2_NATIVE_MAX_UPLOAD_BODY_BYTES {
        return Err(native_error(
            413,
            format!("native upload body exceeds {N2_NATIVE_MAX_UPLOAD_BODY_BYTES} bytes"),
        ));
    }

    if payload.expect_continue {
        let continue_response =
            NativeResponse::operation(request_id.to_string(), op_response(100), Vec::new());
        write_response(stream, &continue_response)
            .await
            .map_err(|err| native_error(500, format!("native continue write failed: {err}")))?;
    }

    let mut headers = HeaderMap::new();
    if let Some(content_type) = payload.content_type.as_ref() {
        if let Ok(value) = HeaderValue::from_str(content_type) {
            headers.insert(CONTENT_TYPE, value);
        }
    }

    let query = PutObjectQuery {
        uid: payload.uid,
        version_id: payload.version_id,
        expected_epoch: payload.expected_epoch,
        kc: payload.kc,
        mc: payload.mc,
        kd: payload.kd,
        md: payload.md,
        stripe_block_size: payload.stripe_block_size,
        content_type: payload.content_type,
    };

    let fragment = payload.fragment;
    let object_key = payload.object_key;

    let result = if let Some(body) = request.body {
        let body_stream = stream::iter(vec![Ok(Bytes::from(body.into_vec()))]);
        put_n2_object_stream(claims, headers, fragment, object_key, query, body_stream).await
    } else if body_len > 0 {
        let (result, remaining) = {
            let mut body_reader = NativeSocketReader {
                stream,
                remaining: body_len,
            };
            let body_stream =
                ReaderStream::with_capacity(&mut body_reader, NATIVE_UPLOAD_READER_BUFFER_BYTES)
                    .map(|chunk_result| {
                        chunk_result.map_err(|err| {
                            AppError::Io(err, "read native n2 upload body".to_string())
                        })
                    });
            let result =
                put_n2_object_stream(claims, headers, fragment, object_key, query, body_stream)
                    .await;
            (result, body_reader.remaining)
        };
        if remaining > 0 {
            drain_native_remaining(stream, remaining)
                .await
                .map_err(|err| {
                    native_error(400, format!("drain native upload body failed: {err}"))
                })?;
        }
        result
    } else {
        let body_stream = stream::empty::<Result<Bytes, AppError>>();
        put_n2_object_stream(claims, headers, fragment, object_key, query, body_stream).await
    };

    let response = result?;
    Ok(NativeResponse::operation(
        request_id.to_string(),
        op_response(200),
        native_body_bytes(&response.0),
    ))
}

async fn execute_native_download(
    session: &NativeSession,
    payload: Option<&str>,
    stream: &mut TcpStream,
    request_id: &str,
) -> Result<(), NativeExecutionError> {
    let claims = require_claims(session)?;
    let request = payload_as::<FragmentObjectPayload>(payload)?;
    let mut headers = HeaderMap::new();
    if let Some(range) = request.range.as_ref() {
        if let Ok(value) = HeaderValue::from_str(range) {
            headers.insert(RANGE, value);
        }
    }
    let query = GetObjectQuery {
        version_id: request.version_id,
    };
    let response = get_n2_object(
        claims,
        headers,
        Path((request.fragment, request.object_key)),
        Query(query),
    )
    .await?
    .into_response();
    stream_response(response, stream, request_id).await
}

async fn stream_response(
    response: Response,
    stream: &mut TcpStream,
    request_id: &str,
) -> Result<(), NativeExecutionError> {
    let status = response.status().as_u16();
    let mut payload_headers = serde_json::Map::new();
    for (name, value) in response.headers() {
        if let Ok(value) = value.to_str() {
            payload_headers.insert(name.as_str().to_string(), Value::String(value.to_string()));
        }
    }

    let body_len = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let payload = json!({
        "headers": payload_headers,
    });

    if let Some(body_len) = body_len {
        let native_response = NativeResponse {
            id: request_id.to_string(),
            ok: status < 400,
            status: Some(status),
            payload: Some(serde_json::to_string(&payload).unwrap_or_else(|_| "{}".into())),
            body: None,
            body_len: Some(body_len),
            error: None,
        };
        write_response(stream, &native_response)
            .await
            .map_err(|err| {
                native_error(500, format!("native download control write failed: {err}"))
            })?;
        let mut body_stream = response.into_body().into_data_stream();
        while let Some(chunk_result) = body_stream.next().await {
            let chunk = chunk_result.map_err(|err| {
                native_error(500, format!("native download body read failed: {err}"))
            })?;
            if !chunk.is_empty() {
                stream.write_all(&chunk).await.map_err(|err| {
                    native_error(500, format!("native download socket write failed: {err}"))
                })?;
            }
        }
        stream.flush().await.map_err(|err| {
            native_error(500, format!("native download socket flush failed: {err}"))
        })?;
        Ok(())
    } else {
        let bytes = to_bytes(response.into_body(), NATIVE_RESPONSE_COLLECT_LIMIT)
            .await
            .map_err(|err| native_error(500, format!("native body collect failed: {err}")))?;
        let native_response = NativeResponse {
            id: request_id.to_string(),
            ok: status < 400,
            status: Some(status),
            payload: Some(serde_json::to_string(&payload).unwrap_or_else(|_| "{}".into())),
            body: Some(serde_bytes::ByteBuf::from(bytes.to_vec())),
            body_len: None,
            error: None,
        };
        write_response(stream, &native_response)
            .await
            .map_err(|err| {
                native_error(500, format!("native download response write failed: {err}"))
            })
    }
}

struct NativeSocketReader<'a> {
    stream: &'a mut TcpStream,
    remaining: u64,
}

impl AsyncRead for NativeSocketReader<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        let cap = buf.remaining();
        if cap == 0 {
            return Poll::Ready(Ok(()));
        }

        let allowed = this.remaining.min(cap as u64) as usize;
        let mut limited = buf.take(allowed);
        match Pin::new(&mut *this.stream).poll_read(cx, &mut limited) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                let read = limited.filled().len();
                if read == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "native n2 upload body ended early",
                    )));
                }
                unsafe {
                    buf.assume_init(read);
                }
                buf.advance(read);
                this.remaining = this.remaining.saturating_sub(read as u64);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }
}

async fn drain_native_remaining(stream: &mut TcpStream, mut remaining: u64) -> io::Result<()> {
    let mut buffer = vec![0u8; NATIVE_UPLOAD_DRAIN_BUFFER_BYTES];
    while remaining > 0 {
        let wanted = (remaining as usize).min(buffer.len());
        let read = stream.read(&mut buffer[..wanted]).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "native n2 upload body ended early",
            ));
        }
        remaining = remaining.saturating_sub(read as u64);
    }
    Ok(())
}

fn require_claims(
    session: &NativeSession,
) -> Result<crate::structs::auth_structs::Claims, NativeExecutionError> {
    session
        .claims()
        .cloned()
        .ok_or_else(|| native_error(401, "native n2 operation requires authentication"))
}

async fn auth_fragment(
    session: &mut NativeSession,
    id: &str,
    payload: NativeAuthFragmentRequest,
) -> NativeResponse {
    let body = match authorize_fragment_core(AuthorizationRequest {
        fragment: payload.fragment,
        secret: payload.secret,
        read: payload.read,
        write: payload.write,
        modify: payload.modify,
        delete: payload.delete,
        expires_in: payload.expires_in,
    })
    .await
    {
        Ok(body) => body,
        Err(err) => {
            return NativeResponse::operation(
                id,
                op_response(401),
                native_body_bytes(&json!({"error": format!("{err:?}")})),
            );
        }
    };
    let token = body.access_token.clone();
    if let Err(err) = session.set_bearer(token) {
        return NativeResponse::err(id, "auth.invalid_token", err);
    }
    NativeResponse::operation(id, op_response(200), native_body_bytes(&body))
}

fn payload_value(payload: Option<&str>) -> Result<Value, NativeExecutionError> {
    match payload {
        Some(payload) if !payload.trim().is_empty() => serde_json::from_str(payload)
            .map_err(|err| native_error(400, format!("invalid payload json: {err}"))),
        _ => Ok(Value::Null),
    }
}

fn payload_as<T>(payload: Option<&str>) -> Result<T, NativeExecutionError>
where
    T: DeserializeOwned,
{
    serde_json::from_value(payload_value(payload)?).map_err(|err| native_error(400, err))
}

fn json_body<T: Serialize>(status: u16, value: &T) -> (u16, Vec<u8>) {
    (status, native_body_bytes(value))
}

fn native_body_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    rmp_serde::to_vec(value).unwrap_or_else(|_| Vec::new())
}

fn op_response(status: u16) -> NativeOpResponse {
    NativeOpResponse { status }
}

fn native_error(status: u16, error: impl std::fmt::Display) -> NativeExecutionError {
    NativeExecutionError(
        op_response(status),
        native_body_bytes(&json!({"error": error.to_string()})),
    )
}

impl From<AppError> for NativeExecutionError {
    fn from(err: AppError) -> Self {
        let status = match err {
            AppError::BadRequest(_) => 400,
            AppError::Unauthorized => 401,
            AppError::Forbidden(_) => 403,
            AppError::NotFound => 404,
            AppError::Conflict(_) => 409,
            AppError::PayloadTooLarge(_) => 413,
            AppError::RangeNotSatisfiable => 416,
            AppError::InsufficientStorage => 507,
            AppError::ServiceUnavailable(_) => 503,
            AppError::Timeout(_) => 408,
            AppError::Io(_, _) | AppError::Multipart(_) | AppError::InternalServerError(_) => 500,
        };
        native_error(status, err)
    }
}

impl From<serde_json::Error> for NativeExecutionError {
    fn from(err: serde_json::Error) -> Self {
        native_error(400, err)
    }
}

impl From<rmp_serde::encode::Error> for NativeExecutionError {
    fn from(err: rmp_serde::encode::Error) -> Self {
        native_error(500, err)
    }
}
