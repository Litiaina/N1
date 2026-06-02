use crate::core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::core::disk_manager::{ObjectPathManager, ShardInfo};
use crate::enums::app_error_enum::AppError;
use crate::protocol::frame::{read_response_body, read_response_control, write_request_control};
use crate::protocol::types::{NativeRequest, NativeResponse};
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::put;
use axum::{Json, Router};
use bytes::Bytes;
use crc32fast::Hasher;
use futures_util::StreamExt;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use serde_json::json;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, DuplexStream};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};
use tokio_util::io::ReaderStream;
use ulid::Ulid;

use crate::routes::storage_routes::AppState;

static PEER_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("build N2 peer HTTP client")
});

const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_READ_TIMEOUT: Duration = Duration::from_secs(20);
const PEER_WRITE_TIMEOUT: Duration = Duration::from_secs(20);

pub(crate) fn peer_http_client() -> Result<reqwest::Client, AppError> {
    Ok(PEER_HTTP_CLIENT.clone())
}

fn endpoint_is_native(endpoint: &str) -> bool {
    endpoint.trim_start().starts_with("n2://")
}

fn native_socket_addr(endpoint: &str) -> Result<String, AppError> {
    let trimmed = endpoint.trim();
    let Some(addr) = trimmed.strip_prefix("n2://") else {
        return Err(AppError::BadRequest(format!(
            "N2 native peer endpoint must start with n2://, got {trimmed}"
        )));
    };
    if addr.trim().is_empty() {
        return Err(AppError::BadRequest(
            "N2 native peer endpoint is empty".into(),
        ));
    }
    Ok(addr.to_string())
}

#[derive(Clone, Debug)]
pub struct PeerShardTarget {
    pub node_id: String,
    pub endpoint: String,
    pub fragment: String,
    pub version_id: String,
    pub part_index: u64,
    pub shard_index: usize,
    pub is_parity: bool,
    pub expected_len: u64,
}

#[derive(Debug, Deserialize)]
pub struct PeerShardQuery {
    #[serde(default)]
    pub parity: Option<bool>,
    #[serde(default)]
    pub expected_len: Option<u64>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub length: Option<usize>,
}

pub struct RemoteShardWriter {
    target: PeerShardTarget,
    writer: Option<DuplexStream>,
    handle: tokio::task::JoinHandle<Result<ShardInfo, AppError>>,
}

impl RemoteShardWriter {
    pub fn spawn(target: PeerShardTarget) -> Self {
        let (writer, reader) = tokio::io::duplex(1024 * 1024);
        let upload_target = target.clone();
        let handle = tokio::spawn(async move {
            if endpoint_is_native(&upload_target.endpoint) {
                native_put_peer_shard(&upload_target, reader).await
            } else {
                http_put_peer_shard(&upload_target, reader).await
            }
        });
        Self {
            target,
            writer: Some(writer),
            handle,
        }
    }

    pub async fn finish(mut self) -> Result<ShardInfo, AppError> {
        if let Some(mut writer) = self.writer.take() {
            writer
                .shutdown()
                .await
                .map_err(|e| AppError::Io(e, "shutdown remote N2 shard upload stream".into()))?;
        }
        let mut info = self.handle.await.map_err(|e| {
            AppError::InternalServerError(format!("remote N2 shard writer join failed: {e}"))
        })??;
        info.absolute_path = encode_peer_shard_locator(&self.target);
        Ok(info)
    }
}

async fn http_put_peer_shard(
    target: &PeerShardTarget,
    reader: DuplexStream,
) -> Result<ShardInfo, AppError> {
    let url = peer_shard_url(target);
    let client = peer_http_client()?;
    let body = reqwest::Body::wrap_stream(ReaderStream::new(reader));
    let mut req = client.put(url).body(body);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = req
        .send()
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("peer shard upload failed: {e}")))?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "peer shard upload returned {}: {}",
            status, text
        )));
    }
    response
        .json::<ShardInfo>()
        .await
        .map_err(|e| AppError::InternalServerError(format!("decode peer shard response: {e}")))
}

async fn native_put_peer_shard(
    target: &PeerShardTarget,
    mut reader: DuplexStream,
) -> Result<ShardInfo, AppError> {
    let addr = native_socket_addr(&target.endpoint)?;
    let mut stream = TcpStream::connect(&addr).await.map_err(|e| {
        AppError::ServiceUnavailable(format!(
            "connect N2 native peer {} at {} failed: {}",
            target.node_id, addr, e
        ))
    })?;
    let _ = stream.set_nodelay(true);
    let payload = NativePeerShardPayload {
        secret: cluster_secret(),
        fragment: target.fragment.clone(),
        version_id: target.version_id.clone(),
        part_index: target.part_index,
        shard_index: target.shard_index,
        parity: target.is_parity,
        expected_len: Some(target.expected_len),
        offset: None,
        length: None,
    };
    let request = NativeRequest {
        id: format!("peer-put-{}", Ulid::new()),
        op: "peer.shard.put".to_string(),
        payload: Some(serde_json::to_string(&payload).map_err(|e| {
            AppError::InternalServerError(format!("encode native peer put payload: {e}"))
        })?),
        body: None,
        body_len: Some(target.expected_len),
    };
    write_request_control(&mut stream, &request)
        .await
        .map_err(|e| {
            AppError::ServiceUnavailable(format!("write N2 native peer put control failed: {e}"))
        })?;
    tokio::io::copy(&mut reader, &mut stream)
        .await
        .map_err(|e| {
            AppError::ServiceUnavailable(format!("stream N2 native peer shard failed: {e}"))
        })?;
    stream.shutdown().await.map_err(|e| {
        AppError::ServiceUnavailable(format!("shutdown N2 native peer shard stream failed: {e}"))
    })?;
    let response = read_native_peer_response(&mut stream).await?;
    if !response.ok {
        return Err(native_peer_response_error("put", target, &response));
    }
    let payload = response.payload.as_deref().ok_or_else(|| {
        AppError::InternalServerError("N2 native peer put response missing payload".into())
    })?;
    serde_json::from_str::<ShardInfo>(payload).map_err(|e| {
        AppError::InternalServerError(format!("decode N2 native peer put response: {e}"))
    })
}

impl AsyncWrite for RemoteShardWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "peer closed"))?;
        Pin::new(writer).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "peer closed"))?;
        Pin::new(writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(writer) = self.writer.as_mut() {
            Pin::new(writer).poll_shutdown(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

pub(crate) fn cluster_secret() -> Option<String> {
    std::env::var("N2_CLUSTER_SECRET")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn authorize_peer(headers: &HeaderMap) -> Result<(), AppError> {
    let Some(secret) = cluster_secret() else {
        return Ok(());
    };
    let provided = headers
        .get("x-n2-cluster-secret")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if provided == secret {
        Ok(())
    } else {
        Err(AppError::Forbidden("N2 peer secret mismatch".into()))
    }
}

fn encode_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn decode_segment(value: &str) -> Option<String> {
    percent_decode_str(value)
        .decode_utf8()
        .ok()
        .map(|v| v.to_string())
}

#[derive(Clone, Debug)]
pub struct PeerShardLocator {
    pub node_id: String,
    pub endpoint: String,
    pub fragment: String,
    pub version_id: String,
    pub part_index: u64,
    pub shard_index: usize,
    pub is_parity: bool,
}

pub fn encode_peer_shard_locator(target: &PeerShardTarget) -> String {
    format!(
        "n2-peer|{}|{}|{}|{}|{}|{}|{}",
        encode_segment(&target.node_id),
        encode_segment(&target.endpoint),
        encode_segment(&target.fragment),
        encode_segment(&target.version_id),
        target.part_index,
        target.shard_index,
        target.is_parity
    )
}

pub fn parse_peer_shard_locator(value: &str) -> Option<PeerShardLocator> {
    let mut parts = value.split('|');
    if parts.next()? != "n2-peer" {
        return None;
    }
    let node_id = decode_segment(parts.next()?)?;
    let endpoint = decode_segment(parts.next()?)?;
    let fragment = decode_segment(parts.next()?)?;
    let version_id = decode_segment(parts.next()?)?;
    let part_index = parts.next()?.parse().ok()?;
    let shard_index = parts.next()?.parse().ok()?;
    let is_parity = parts.next()?.parse().ok()?;
    Some(PeerShardLocator {
        node_id,
        endpoint,
        fragment,
        version_id,
        part_index,
        shard_index,
        is_parity,
    })
}

fn peer_shard_url(target: &PeerShardTarget) -> String {
    format!(
        "{}/n2/peer/shards/{}/{}/{}/{}?parity={}&expected_len={}",
        target.endpoint.trim_end_matches('/'),
        encode_segment(&target.fragment),
        encode_segment(&target.version_id),
        target.part_index,
        target.shard_index,
        target.is_parity,
        target.expected_len
    )
}

fn peer_shard_read_url(locator: &PeerShardLocator, offset: u64, length: usize) -> String {
    format!(
        "{}/n2/peer/shards/{}/{}/{}/{}?parity={}&offset={}&length={}",
        locator.endpoint.trim_end_matches('/'),
        encode_segment(&locator.fragment),
        encode_segment(&locator.version_id),
        locator.part_index,
        locator.shard_index,
        locator.is_parity,
        offset,
        length
    )
}

pub async fn fetch_peer_shard_chunk(
    locator: &PeerShardLocator,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, AppError> {
    if endpoint_is_native(&locator.endpoint) {
        return native_get_peer_shard(locator, offset, length).await;
    }
    let client = peer_http_client()?;
    let mut req = client.get(peer_shard_read_url(locator, offset, length));
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = req.timeout(PEER_READ_TIMEOUT).send().await.map_err(|e| {
        AppError::ServiceUnavailable(format!(
            "peer shard read failed from {}: {e}",
            locator.node_id
        ))
    })?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "peer shard read from {} returned {}: {}",
            locator.node_id, status, text
        )));
    }
    timeout(PEER_READ_TIMEOUT, response.bytes())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "peer shard body read timed out from {}",
                locator.node_id
            ))
        })?
        .map(|bytes| bytes.to_vec())
        .map_err(|e| AppError::ServiceUnavailable(format!("read peer shard body: {e}")))
}

async fn native_get_peer_shard(
    locator: &PeerShardLocator,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, AppError> {
    let addr = native_socket_addr(&locator.endpoint)?;
    let mut stream = timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(&addr))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "connect N2 native peer {} at {} timed out",
                locator.node_id, addr
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!(
                "connect N2 native peer {} at {} failed: {}",
                locator.node_id, addr, e
            ))
        })?;
    let _ = stream.set_nodelay(true);
    let payload = NativePeerShardPayload {
        secret: cluster_secret(),
        fragment: locator.fragment.clone(),
        version_id: locator.version_id.clone(),
        part_index: locator.part_index,
        shard_index: locator.shard_index,
        parity: locator.is_parity,
        expected_len: None,
        offset: Some(offset),
        length: Some(length),
    };
    let request = NativeRequest {
        id: format!("peer-get-{}", Ulid::new()),
        op: "peer.shard.get".to_string(),
        payload: Some(serde_json::to_string(&payload).map_err(|e| {
            AppError::InternalServerError(format!("encode native peer get payload: {e}"))
        })?),
        body: None,
        body_len: None,
    };
    timeout(PEER_WRITE_TIMEOUT, write_request_control(&mut stream, &request))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "write N2 native peer get control to {} timed out",
                locator.node_id
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!("write N2 native peer get control failed: {e}"))
        })?;
    let response = timeout(PEER_READ_TIMEOUT, read_native_peer_response(&mut stream))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "read N2 native peer get response from {} timed out",
                locator.node_id
            ))
        })??;
    if !response.ok {
        return Err(native_peer_locator_response_error(
            "get", locator, &response,
        ));
    }
    let body_len = response.body_len.unwrap_or(0);
    timeout(PEER_READ_TIMEOUT, read_response_body(&mut stream, body_len))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "read N2 native peer shard body from {} timed out",
                locator.node_id
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!("read N2 native peer shard body failed: {e}"))
        })
}

async fn read_native_peer_response(stream: &mut TcpStream) -> Result<NativeResponse, AppError> {
    read_response_control(stream)
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("read N2 native peer response: {e}")))?
        .ok_or_else(|| AppError::ServiceUnavailable("N2 native peer closed early".into()))
}

fn native_peer_response_error(
    op: &str,
    target: &PeerShardTarget,
    response: &NativeResponse,
) -> AppError {
    AppError::ServiceUnavailable(format!(
        "N2 native peer shard {op} returned status {:?} from {}: {}",
        response.status,
        target.node_id,
        response
            .error
            .as_ref()
            .map(|err| err.message.as_str())
            .unwrap_or("unknown native peer error")
    ))
}

fn native_peer_locator_response_error(
    op: &str,
    locator: &PeerShardLocator,
    response: &NativeResponse,
) -> AppError {
    AppError::ServiceUnavailable(format!(
        "N2 native peer shard {op} returned status {:?} from {}: {}",
        response.status,
        locator.node_id,
        response
            .error
            .as_ref()
            .map(|err| err.message.as_str())
            .unwrap_or("unknown native peer error")
    ))
}

fn stable_name(part_index: u64, shard_index: usize, is_parity: bool) -> String {
    let ext = if is_parity {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    };
    format!("part_{part_index}_shard_{shard_index}{ext}")
}

fn shard_dir(
    fragment: &str,
    version_id: &str,
    part_index: u64,
    shard_index: usize,
) -> Result<std::path::PathBuf, AppError> {
    let pm = ObjectPathManager::new();
    let roots = pm.get_blob_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No blob storage disks configured".into(),
        ));
    }
    let disk_idx = (part_index as usize + shard_index) % roots.len();
    Ok(pm.get_shard_dir_on_disk(&roots[disk_idx], fragment, version_id))
}

pub async fn peer_put_shard(
    headers: HeaderMap,
    Path((fragment, version_id, part_index, shard_index)): Path<(String, String, u64, usize)>,
    Query(query): Query<PeerShardQuery>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    let is_parity = query.parity.unwrap_or(false);
    let expected_len = query.expected_len.unwrap_or(0);
    let dir = shard_dir(&fragment, &version_id, part_index, shard_index)?;
    fs::create_dir_all(&dir)
        .await
        .map_err(|e| AppError::Io(e, format!("create peer shard dir {}", dir.display())))?;
    let file_name = stable_name(part_index, shard_index, is_parity);
    let tmp_path = dir.join(format!("{file_name}.tmp.{}", Ulid::new()));
    let stable_path = dir.join(&file_name);
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .await
        .map_err(|e| AppError::Io(e, format!("create peer shard {}", tmp_path.display())))?;
    let mut stream = body.into_data_stream();
    let mut hasher = Hasher::new();
    let mut written = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "read peer shard body".into(),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        written = written.saturating_add(chunk.len() as u64);
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|e| AppError::Io(e, format!("write peer shard {}", tmp_path.display())))?;
    }
    file.flush()
        .await
        .map_err(|e| AppError::Io(e, format!("flush peer shard {}", tmp_path.display())))?;
    drop(file);
    if expected_len > 0 && written != expected_len {
        let _ = fs::remove_file(&tmp_path).await;
        return Err(AppError::BadRequest(format!(
            "peer shard length mismatch: got {}, expected {}",
            written, expected_len
        )));
    }
    if fs::try_exists(&stable_path).await.unwrap_or(false) {
        let _ = fs::remove_file(&stable_path).await;
    }
    fs::rename(&tmp_path, &stable_path).await.map_err(|e| {
        AppError::Io(
            e,
            format!(
                "publish peer shard {} -> {}",
                tmp_path.display(),
                stable_path.display()
            ),
        )
    })?;
    let info = ShardInfo {
        shard_index,
        file_name,
        absolute_path: stable_path.to_string_lossy().to_string(),
        size_bytes: written,
        checksum: format!("{:08x}", hasher.finalize()),
        is_parity,
    };
    Ok((StatusCode::CREATED, Json(info)))
}

pub async fn peer_get_shard(
    headers: HeaderMap,
    Path((fragment, version_id, part_index, shard_index)): Path<(String, String, u64, usize)>,
    Query(query): Query<PeerShardQuery>,
) -> Result<Response, AppError> {
    authorize_peer(&headers)?;
    let dir = shard_dir(&fragment, &version_id, part_index, shard_index)?;
    let file_name = stable_name(part_index, shard_index, query.parity.unwrap_or(false));
    let path = dir.join(file_name);
    let mut file = fs::File::open(&path)
        .await
        .map_err(|e| AppError::Io(e, format!("open peer shard {}", path.display())))?;
    if let Some(length) = query.length {
        let offset = query.offset.unwrap_or(0);
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| AppError::Io(e, format!("seek peer shard {}", path.display())))?;
        let mut buf = vec![0u8; length];
        let mut read = 0usize;
        while read < length {
            let n = file.read(&mut buf[read..]).await.map_err(|e| {
                AppError::Io(e, format!("read peer shard range {}", path.display()))
            })?;
            if n == 0 {
                break;
            }
            read += n;
        }
        buf.truncate(read);
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(buf))
            .unwrap());
    }
    let stream = ReaderStream::new(file);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::from_stream(stream.map(|result| {
            result.map(Bytes::from).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("peer shard read: {e}"))
            })
        })))
        .unwrap())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NativePeerShardPayload {
    #[serde(default)]
    pub secret: Option<String>,
    pub fragment: String,
    pub version_id: String,
    pub part_index: u64,
    pub shard_index: usize,
    #[serde(default)]
    pub parity: bool,
    #[serde(default)]
    pub expected_len: Option<u64>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub length: Option<usize>,
}

fn authorize_peer_secret(secret: Option<&str>) -> Result<(), AppError> {
    let Some(expected) = cluster_secret() else {
        return Ok(());
    };
    if secret.unwrap_or_default() == expected {
        Ok(())
    } else {
        Err(AppError::Forbidden("N2 peer secret mismatch".into()))
    }
}

pub async fn native_peer_put_shard_reader<R>(
    payload: NativePeerShardPayload,
    reader: &mut R,
    body_len: u64,
) -> Result<ShardInfo, AppError>
where
    R: AsyncRead + Unpin,
{
    authorize_peer_secret(payload.secret.as_deref())?;
    let expected_len = payload.expected_len.unwrap_or(body_len);
    let dir = shard_dir(
        &payload.fragment,
        &payload.version_id,
        payload.part_index,
        payload.shard_index,
    )?;
    fs::create_dir_all(&dir)
        .await
        .map_err(|e| AppError::Io(e, format!("create native peer shard dir {}", dir.display())))?;
    let file_name = stable_name(payload.part_index, payload.shard_index, payload.parity);
    let tmp_path = dir.join(format!("{file_name}.tmp.{}", Ulid::new()));
    let stable_path = dir.join(&file_name);
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .await
        .map_err(|e| {
            AppError::Io(
                e,
                format!("create native peer shard {}", tmp_path.display()),
            )
        })?;
    let mut hasher = Hasher::new();
    let mut written = 0u64;
    let mut buf = vec![0u8; 256 * 1024];
    while written < body_len {
        let wanted = ((body_len - written) as usize).min(buf.len());
        let n = reader.read(&mut buf[..wanted]).await.map_err(|e| {
            AppError::Io(
                e,
                format!("read native peer shard body {}", tmp_path.display()),
            )
        })?;
        if n == 0 {
            break;
        }
        written = written.saturating_add(n as u64);
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n]).await.map_err(|e| {
            AppError::Io(e, format!("write native peer shard {}", tmp_path.display()))
        })?;
    }
    file.flush()
        .await
        .map_err(|e| AppError::Io(e, format!("flush native peer shard {}", tmp_path.display())))?;
    drop(file);
    if written != body_len || (expected_len > 0 && written != expected_len) {
        let _ = fs::remove_file(&tmp_path).await;
        return Err(AppError::BadRequest(format!(
            "native peer shard length mismatch: got {}, body_len {}, expected {}",
            written, body_len, expected_len
        )));
    }
    if fs::try_exists(&stable_path).await.unwrap_or(false) {
        let _ = fs::remove_file(&stable_path).await;
    }
    fs::rename(&tmp_path, &stable_path).await.map_err(|e| {
        AppError::Io(
            e,
            format!(
                "publish native peer shard {} -> {}",
                tmp_path.display(),
                stable_path.display()
            ),
        )
    })?;
    Ok(ShardInfo {
        shard_index: payload.shard_index,
        file_name,
        absolute_path: stable_path.to_string_lossy().to_string(),
        size_bytes: written,
        checksum: format!("{:08x}", hasher.finalize()),
        is_parity: payload.parity,
    })
}

pub async fn native_peer_get_shard_bytes(
    payload: NativePeerShardPayload,
) -> Result<Vec<u8>, AppError> {
    authorize_peer_secret(payload.secret.as_deref())?;
    let dir = shard_dir(
        &payload.fragment,
        &payload.version_id,
        payload.part_index,
        payload.shard_index,
    )?;
    let file_name = stable_name(payload.part_index, payload.shard_index, payload.parity);
    let path = dir.join(file_name);
    let mut file = fs::File::open(&path)
        .await
        .map_err(|e| AppError::Io(e, format!("open native peer shard {}", path.display())))?;
    if let Some(length) = payload.length {
        file.seek(SeekFrom::Start(payload.offset.unwrap_or(0)))
            .await
            .map_err(|e| AppError::Io(e, format!("seek native peer shard {}", path.display())))?;
        let mut buf = vec![0u8; length];
        let mut read = 0usize;
        while read < length {
            let n = file.read(&mut buf[read..]).await.map_err(|e| {
                AppError::Io(
                    e,
                    format!("read native peer shard range {}", path.display()),
                )
            })?;
            if n == 0 {
                break;
            }
            read += n;
        }
        buf.truncate(read);
        return Ok(buf);
    }
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .await
        .map_err(|e| AppError::Io(e, format!("read native peer shard {}", path.display())))?;
    Ok(buf)
}

pub fn native_peer_bytes_response(id: String, bytes: Vec<u8>) -> NativeResponse {
    NativeResponse {
        id,
        ok: true,
        status: Some(StatusCode::OK.as_u16()),
        payload: Some(json!({"bytes": bytes.len()}).to_string()),
        body_len: Some(bytes.len() as u64),
        body: Some(ByteBuf::from(bytes)),
        error: None,
    }
}

pub fn build_peer_routes() -> Router<Arc<AppState>> {
    Router::new().route(
        "/n2/peer/shards/{:fragment}/{:version_id}/{:part_index}/{:shard_index}",
        put(peer_put_shard).get(peer_get_shard),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_shard_locator_round_trips_endpoint_and_identity() {
        let target = PeerShardTarget {
            node_id: "node-a".into(),
            endpoint: "https://node-a.local:50001".into(),
            fragment: "frag".into(),
            version_id: "01KTEST".into(),
            part_index: 7,
            shard_index: 2,
            is_parity: true,
            expected_len: 128,
        };
        let encoded = encode_peer_shard_locator(&target);
        let decoded = parse_peer_shard_locator(&encoded).expect("locator");
        assert_eq!(decoded.node_id, target.node_id);
        assert_eq!(decoded.endpoint, target.endpoint);
        assert_eq!(decoded.fragment, target.fragment);
        assert_eq!(decoded.version_id, target.version_id);
        assert_eq!(decoded.part_index, target.part_index);
        assert_eq!(decoded.shard_index, target.shard_index);
        assert_eq!(decoded.is_parity, target.is_parity);
    }

    #[test]
    fn native_endpoint_detection_is_explicit() {
        assert!(endpoint_is_native("n2://127.0.0.1:50002"));
        assert!(!endpoint_is_native("https://127.0.0.1:50001"));
        assert_eq!(
            native_socket_addr("n2://node-a.local:50002").unwrap(),
            "node-a.local:50002"
        );
    }
}
