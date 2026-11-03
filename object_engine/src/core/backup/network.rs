//! Bounded native recovery-point transfer. The authenticated NOA connection is
//! consumed by this protocol; it never tunnels through the HTTP adapter.
use super::{
    Result,
    format::{BLOCK_BYTES, FORMAT, MAX_RECORD_BYTES, Snapshot},
    invalid,
    repository::Repository,
};
use serde::{Deserialize, Serialize};
use std::{io::ErrorKind, path::PathBuf, sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
const IDLE: Duration = Duration::from_secs(120);
mod control;
pub use control::{Destination, Request, completed_operation, run_async};
#[derive(Serialize, Deserialize)]
#[serde(tag = "message", deny_unknown_fields)]
enum Message {
    Begin {
        snapshot: Box<Snapshot>,
        key_id: String,
    },
    Offer {
        hash: String,
    },
    Data {
        length: usize,
    },
    Finish,
    Ready,
    Have,
    Send,
    Complete,
    Error {
        detail: String,
    },
}
async fn read_exact<R: AsyncRead + Unpin>(reader: &mut R, mut bytes: &mut [u8]) -> Result<()> {
    while !bytes.is_empty() {
        let take = bytes.len().min(64 * 1024);
        let n = tokio::time::timeout(IDLE, reader.read(&mut bytes[..take]))
            .await
            .map_err(|_| invalid("replication source stalled"))??;
        if n == 0 {
            return Err(invalid("truncated replication frame"));
        }
        bytes = &mut bytes[n..];
    }
    Ok(())
}
async fn write_all<W: AsyncWrite + Unpin>(writer: &mut W, bytes: &[u8]) -> Result<()> {
    for chunk in bytes.chunks(64 * 1024) {
        tokio::time::timeout(IDLE, writer.write_all(chunk))
            .await
            .map_err(|_| invalid("replication destination stalled"))??;
    }
    tokio::time::timeout(IDLE, writer.flush())
        .await
        .map_err(|_| invalid("replication flush stalled"))??;
    Ok(())
}
async fn write<S: AsyncWrite + Unpin>(stream: &mut S, message: &Message) -> Result<()> {
    let bytes = rmp_serde::to_vec_named(message)?;
    if bytes.is_empty() || bytes.len() > MAX_RECORD_BYTES {
        return Err(invalid("replication control exceeds limit"));
    }
    write_all(stream, &(bytes.len() as u32).to_be_bytes()).await?;
    write_all(stream, &bytes).await
}
async fn read<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Message> {
    let mut prefix = [0; 4];
    read_exact(stream, &mut prefix).await?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > MAX_RECORD_BYTES {
        return Err(invalid("replication control exceeds limit"));
    }
    let mut bytes = vec![0; length];
    read_exact(stream, &mut bytes).await?;
    let message: Message = rmp_serde::from_slice(&bytes)?;
    if let Message::Error { detail } = &message {
        return Err(invalid(format!(
            "replication peer rejected operation: {detail}"
        )));
    }
    Ok(message)
}
async fn work<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| invalid(format!("replication I/O worker failed: {e}")))?
}
fn missing(e: &super::Error) -> bool {
    e.downcast_ref::<std::io::Error>()
        .is_some_and(|e| e.kind() == ErrorKind::NotFound)
}
fn proposal(repo: &Repository, snapshot: &Snapshot) -> Result<PathBuf> {
    repo.ensure_writable()?;
    let pending = match repo.pending(&snapshot.id) {
        Ok(pending) => pending,
        Err(e)
            if e.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == ErrorKind::AlreadyExists) =>
        {
            let pending = repo.path.join("pending").join(&snapshot.id);
            if !std::fs::symlink_metadata(&pending)?.is_dir() {
                return Err(invalid("invalid pending replication directory"));
            }
            super::fs::private_dir(&pending.join("blocks"))?;
            pending
        }
        Err(e) => return Err(e),
    };
    let path = pending.join("proposal");
    let label = format!("{FORMAT}:proposal:{}", snapshot.id);
    let bytes = serde_json::to_vec(snapshot)?;
    match super::fs::read_regular(&path, MAX_RECORD_BYTES + 28) {
        Ok(old) => {
            if repo.key.open(&label, old)? != bytes {
                return Err(invalid("pending replication identity conflicts"));
            }
        }
        Err(e) if missing(&e) => super::fs::atomic_new(&path, &repo.key.seal(&label, &bytes)?)?,
        Err(e) => return Err(e),
    }
    Ok(pending)
}

pub async fn receive<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    repo: Arc<Repository>,
) -> Result<Snapshot> {
    let result = receive_inner(stream, repo).await;
    if let Err(e) = &result {
        let _ = write(
            stream,
            &Message::Error {
                detail: e.to_string(),
            },
        )
        .await;
    }
    result
}
async fn receive_inner<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    repo: Arc<Repository>,
) -> Result<Snapshot> {
    let Message::Begin { snapshot, key_id } = read(stream).await? else {
        return Err(invalid("replication requires a begin frame"));
    };
    super::format::validate_snapshot(&snapshot)?;
    if key_id != repo.key.id {
        return Err(invalid(
            "remote recovery repositories require the same backup encryption key",
        ));
    }
    let snapshot: Arc<Snapshot> = snapshot.into();
    let repository = repo.clone();
    let point = snapshot.clone();
    let existing = work(move || match repository.snapshot(&point.id) {
        Ok(existing) => {
            if serde_json::to_value(&existing)? != serde_json::to_value(&*point)? {
                return Err(invalid("existing recovery point conflicts"));
            }
            repository.verify(&existing)?;
            repository.sync_publication(&point.id)?;
            Ok(true)
        }
        Err(e) if missing(&e) => Ok(false),
        Err(e) => Err(e),
    })
    .await?;
    if existing {
        write(stream, &Message::Complete).await?;
        return Ok((*snapshot).clone());
    }
    let repository = repo.clone();
    let point = snapshot.clone();
    let pending = work(move || proposal(&repository, &point)).await?;
    write(stream, &Message::Ready).await?;
    loop {
        match read(stream).await? {
            Message::Offer { hash } => {
                super::fs::safe_hash(&hash)?;
                let repository = repo.clone();
                let wanted = hash.clone();
                let directory = pending.clone();
                let have = work(move || match repository.get(&wanted) {
                    Ok(_) => {
                        super::fs::sync_parent(&repository.block_path(&wanted)?)?;
                        repository.pin(&wanted, &directory)?;
                        Ok(true)
                    }
                    Err(e) if missing(&e) => Ok(false),
                    Err(e) => Err(e),
                })
                .await?;
                if have {
                    write(stream, &Message::Have).await?;
                    continue;
                }
                write(stream, &Message::Send).await?;
                let Message::Data { length } = read(stream).await? else {
                    return Err(invalid("expected encrypted replication data"));
                };
                if !(28..=BLOCK_BYTES + 28).contains(&length) {
                    return Err(invalid("replication block exceeds limit"));
                }
                let mut encrypted = vec![0; length];
                read_exact(stream, &mut encrypted).await?;
                let repository = repo.clone();
                let directory = pending.clone();
                work(move || repository.import_raw(&hash, &encrypted, &directory)).await?;
                write(stream, &Message::Have).await?;
            }
            Message::Finish => {
                let repository = repo.clone();
                let point = snapshot.clone();
                work(move || {
                    repository.verify(&point)?;
                    repository.publish(&point, &pending)
                })
                .await?;
                write(stream, &Message::Complete).await?;
                return Ok((*snapshot).clone());
            }
            _ => return Err(invalid("unexpected replication frame")),
        }
    }
}

pub async fn send<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    repo: Arc<Repository>,
    id: String,
) -> Result<Snapshot> {
    let repository = repo.clone();
    let snapshot = Arc::new(work(move || repository.snapshot(&id)).await?);
    write(
        stream,
        &Message::Begin {
            snapshot: Box::new((*snapshot).clone()),
            key_id: repo.key.id.clone(),
        },
    )
    .await?;
    match read(stream).await? {
        Message::Complete => return Ok((*snapshot).clone()),
        Message::Ready => {}
        _ => return Err(invalid("replication begin rejected")),
    }
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let source = repo.clone();
    let point = snapshot.clone();
    // Only bounded hash identifiers cross this channel. Transfer admission at
    // the service limits the number of waiting manifest producers.
    let producer = tokio::task::spawn_blocking(move || {
        source.manifest_hashes(&point, |hash| {
            tx.blocking_send(hash.to_owned())
                .map_err(|_| invalid("replication sender disconnected"))
        })?;
        source.records(&point, |record| {
            tx.blocking_send(record.hash)
                .map_err(|_| invalid("replication sender disconnected"))
        })
    });
    let transferred = async {
        while let Some(hash) = rx.recv().await {
            write(stream, &Message::Offer { hash: hash.clone() }).await?;
            match read(stream).await? {
                Message::Have => {}
                Message::Send => {
                    let repository = repo.clone();
                    let encrypted = work(move || repository.raw_block(&hash)).await?;
                    write(
                        stream,
                        &Message::Data {
                            length: encrypted.len(),
                        },
                    )
                    .await?;
                    write_all(stream, &encrypted).await?;
                    if !matches!(read(stream).await?, Message::Have) {
                        return Err(invalid("replication block not acknowledged"));
                    }
                }
                _ => return Err(invalid("invalid replication offer response")),
            }
        }
        Ok::<(), super::Error>(())
    }
    .await;
    drop(rx);
    producer
        .await
        .map_err(|e| invalid(format!("replication manifest producer failed: {e}")))??;
    transferred?;
    write(stream, &Message::Finish).await?;
    if !matches!(read(stream).await?, Message::Complete) {
        return Err(invalid("replication point not acknowledged"));
    }
    Ok((*snapshot).clone())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRepository {
    pub path: PathBuf,
    pub key_file: PathBuf,
}
pub fn valid_alias(alias: &str) -> bool {
    !alias.is_empty()
        && alias.len() <= 64
        && alias
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

/// Secondary HTTP/WebSocket transport uses the identical recovery protocol.
/// Binary frames are bounded; the in-memory duplex supplies backpressure.
pub async fn websocket_server(
    mut socket: axum::extract::ws::WebSocket,
    mut local: tokio::io::DuplexStream,
) -> Result<()> {
    use axum::extract::ws::Message as W;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        tokio::select! {
            read=tokio::time::timeout(IDLE,local.read(&mut buffer))=>{
                let n=read.map_err(|_|invalid("replication adapter stalled"))??;if n==0{let _=socket.send(W::Close(None)).await;return Ok(());}
                tokio::time::timeout(IDLE,socket.send(W::Binary(buffer[..n].to_vec().into()))).await.map_err(|_|invalid("replication HTTP peer stalled"))??;
            },
            frame=tokio::time::timeout(IDLE,socket.recv())=>match frame.map_err(|_|invalid("replication HTTP peer stalled"))?{
                Some(Ok(W::Binary(data)))=>{if data.len()>64*1024{return Err(invalid("replication websocket frame exceeds limit"));}write_all(&mut local,&data).await?;},
                Some(Ok(W::Ping(_)|W::Pong(_)))=>{},Some(Ok(W::Close(_)))|None=>return Ok(()),Some(Ok(_))=>return Err(invalid("replication requires binary frames")),Some(Err(e))=>return Err(e.into()),
            },
        }
    }
}
async fn websocket_client<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: tokio_tungstenite::WebSocketStream<S>,
    mut local: tokio::io::DuplexStream,
) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message as W;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        tokio::select! {
            read=tokio::time::timeout(IDLE,local.read(&mut buffer))=>{
                let n=read.map_err(|_|invalid("replication adapter stalled"))??;if n==0{let _=socket.close(None).await;return Ok(());}
                tokio::time::timeout(IDLE,socket.send(W::Binary(buffer[..n].to_vec().into()))).await.map_err(|_|invalid("replication HTTP peer stalled"))??;
            },
            frame=tokio::time::timeout(IDLE,socket.next())=>match frame.map_err(|_|invalid("replication HTTP peer stalled"))?{
                Some(Ok(W::Binary(data)))=>{if data.len()>64*1024{return Err(invalid("replication websocket frame exceeds limit"));}write_all(&mut local,&data).await?;},
                Some(Ok(W::Ping(_)|W::Pong(_)))=>{},Some(Ok(W::Close(_)))|None=>return Ok(()),Some(Ok(_))=>return Err(invalid("replication requires binary frames")),Some(Err(e))=>return Err(e.into()),
            },
        }
    }
}
pub async fn replicate_http(
    origin: &str,
    alias: &str,
    credentials: cli_core::Credentials,
    ca: Option<&std::path::Path>,
    source: Arc<Repository>,
    snapshot: String,
) -> Result<Snapshot> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    if !valid_alias(alias) {
        return Err(invalid("invalid recovery repository alias"));
    }
    let mut url = reqwest::Url::parse(origin)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(invalid(
            "replication HTTP destination must be an HTTP(S) origin",
        ));
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut client = reqwest::Client::builder().connect_timeout(Duration::from_secs(15));
    if let Some(ca) = ca {
        client = client.add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(ca)?)?);
    }
    let token = if let Some(token) = credentials.token {
        token
    } else {
        let request = n1_link::noa::Request {
            id: 1,
            body_len: 0,
            command: n1_link::noa::Command::Login {
                fragment: credentials
                    .fragment
                    .ok_or_else(|| invalid("replication fragment is required"))?,
                secret: credentials
                    .secret
                    .ok_or_else(|| invalid("replication secret is required"))?,
                permissions: None,
                ttl_seconds: Some(86400),
            },
        };
        let response: n1_link::noa::Response = client
            .build()?
            .post(url.join("/noa")?)
            .json(&request)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        response
            .result
            .and_then(|value| value["token"].as_str().map(str::to_owned))
            .ok_or_else(|| invalid("replication login did not return a token"))?
    };
    let secure = url.scheme() == "https";
    url.set_scheme(if secure { "wss" } else { "ws" })
        .map_err(|_| invalid("invalid websocket scheme"))?;
    url.set_path(&format!("/noa/backup/receive/{alias}"));
    let mut request = url.as_str().into_client_request()?;
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse()?);
    let connector = ca
        .map(|path| cli_core::client_tls(path).map(tokio_tungstenite::Connector::Rustls))
        .transpose()?;
    let limits = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(64 * 1024))
        .max_frame_size(Some(64 * 1024));
    let (socket, _) = tokio::time::timeout(
        Duration::from_secs(15),
        tokio_tungstenite::connect_async_tls_with_config(request, Some(limits), true, connector),
    )
    .await
    .map_err(|_| invalid("replication websocket connection timed out"))??;
    let (bridge, mut protocol) = tokio::io::duplex(128 * 1024);
    let (sent, forwarded) = tokio::join!(
        async move { send(&mut protocol, source, snapshot).await },
        websocket_client(socket, bridge)
    );
    let point = sent?;
    forwarded?;
    Ok(point)
}
