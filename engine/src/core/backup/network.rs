//! N1-owned operator/replication endpoint. Shell delegates here; no SSH
//! process, remote command execution, storage engine, or external server required.
//! The existing authenticated snapshot protocol runs unchanged over bounded
//! HTTP WebSocket bodies or an N1 native streaming operation.
use super::{Result, invalid, repository::Repository};
use crate::protocol::{
    frame::{read_request_control_with_limit, write_response},
    types::{NativeRequest, NativeResponse},
};
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Query, State,
        ws::{Message, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixStream},
    sync::Semaphore,
};

const CHUNK: usize = 64 * 1024;
const IDLE: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Destination {
    pub url: String,
    /// Destination's configured repository alias, never a remote filesystem path.
    pub repository: String,
    pub token_file: PathBuf,
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    /// Explicit opt-in for isolated/trusted HTTP or n1:// networks only.
    #[serde(default)]
    pub allow_plaintext: bool,
}
impl Destination {
    pub fn validate(&self) -> Result<()> {
        url(self)?;
        token(&self.token_file)?;
        if let Some(ca) = &self.ca_file {
            client_tls(ca)?;
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Store {
    pub path: PathBuf,
    pub key_file: PathBuf,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub bind: String,
    #[serde(default)]
    pub native_bind: Option<String>,
    pub token_file: PathBuf,
    #[serde(default)]
    pub tls_cert: Option<PathBuf>,
    #[serde(default)]
    pub tls_key: Option<PathBuf>,
    #[serde(default)]
    pub allow_plaintext: bool,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    pub repositories: BTreeMap<String, Store>,
    #[serde(default)]
    pub engine_config: Option<PathBuf>,
    #[serde(default)]
    pub restore_targets: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub replicas: BTreeMap<String, Destination>,
    /// Already-configured supervised jobs; no caller-supplied paths/commands.
    #[serde(default)]
    pub jobs: BTreeMap<String, PathBuf>,
    /// Running N1's capture endpoint; this separate operator never reads live roots.
    #[serde(default)]
    pub online_source: Option<Destination>,
    #[serde(default = "default_boundary_seconds")]
    pub capture_boundary_seconds: u64,
    #[serde(default = "default_capture_seconds")]
    pub capture_timeout_seconds: u64,
}

fn default_boundary_seconds() -> u64 {
    30
}
fn default_capture_seconds() -> u64 {
    3600
}

fn validate_config(config: &Config) -> Result<()> {
    if config.repositories.is_empty()
        || config.repositories.len() > 32
        || config
            .repositories
            .keys()
            .chain(config.replicas.keys())
            .chain(config.restore_targets.keys())
            .chain(config.jobs.keys())
            .any(|k| !alias(k))
        || !(1..=300).contains(&config.capture_boundary_seconds)
        || !(1..=604800).contains(&config.capture_timeout_seconds)
    {
        return Err(invalid("invalid operator aliases or capture limits"));
    }
    for store in config.repositories.values() {
        if !store.path.is_absolute() || !store.key_file.is_absolute() {
            return Err(invalid("repository/key paths must be absolute"));
        }
    }
    if let Some(destination) = &config.online_source {
        destination.validate()?;
    }
    Ok(())
}

fn private_file(path: &Path, limit: usize) -> Result<Vec<u8>> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::symlink_metadata(path)?;
    if !meta.is_file() || meta.permissions().mode() & 0o077 != 0 {
        return Err(invalid(
            "operator config/token must be an owner-only regular file",
        ));
    }
    super::fs::read_regular(path, limit)
}
fn token(path: &Path) -> Result<String> {
    let value = String::from_utf8(private_file(path, 1024)?)?
        .trim()
        .to_owned();
    if value.len() < 32 || !value.bytes().all(|c| c.is_ascii_graphic()) {
        return Err(invalid(
            "operator token must contain at least 32 printable non-space characters",
        ));
    }
    Ok(value)
}
pub fn generate_token(path: &Path) -> Result<()> {
    use rand::RngCore;
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(super::crypto::digest(&bytes).as_bytes())?;
    file.sync_all()?;
    super::fs::sync_parent(path)
}
fn alias(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}
fn url(destination: &Destination) -> Result<reqwest::Url> {
    let u = reqwest::Url::parse(&destination.url)?;
    if !matches!(u.scheme(), "http" | "https" | "n1" | "n1s")
        || u.host_str().is_none()
        || !u.username().is_empty()
        || u.password().is_some()
        || u.query().is_some()
        || u.fragment().is_some()
        || !matches!(u.path(), "" | "/")
        || !alias(&destination.repository)
    {
        return Err(invalid(
            "destination must be an HTTP(S)/N1(S) origin and configured repository alias",
        ));
    }
    if matches!(u.scheme(), "http" | "n1") && !destination.allow_plaintext {
        return Err(invalid(
            "plaintext replication requires explicit --allow-plaintext on a trusted network",
        ));
    }
    Ok(u)
}
fn client_tls(ca: &Path) -> Result<Arc<rustls::ClientConfig>> {
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls::pki_types::CertificateDer::pem_slice_iter(&std::fs::read(ca)?) {
        roots.add(cert?)?;
    }
    if roots.is_empty() {
        return Err(invalid("CA file contains no certificates"));
    }
    Ok(Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    ))
}

/// Copies the protocol stream with bounded buffers and per-I/O inactivity limits.
async fn pipe<A, B>(mut a: A, mut b: B) -> Result<()>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    // Owning both endpoints closes the Unix peer on error/cancellation, waking
    // the blocking sender/verifier rather than leaving it waiting for IDLE.
    let (mut ar, mut aw) = tokio::io::split(&mut a);
    let (mut br, mut bw) = tokio::io::split(&mut b);
    async fn copy<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(r: &mut R, w: &mut W) -> Result<()> {
        let mut buffer = vec![0; CHUNK];
        loop {
            let n = tokio::time::timeout(IDLE, r.read(&mut buffer)).await??;
            if n == 0 {
                w.shutdown().await?;
                return Ok(());
            }
            tokio::time::timeout(IDLE, w.write_all(&buffer[..n])).await??;
            // TLS can accept plaintext while encrypted records remain buffered.
            // Drain them before waiting for the peer's protocol acknowledgement.
            tokio::time::timeout(IDLE, w.flush()).await??;
        }
    }
    tokio::try_join!(copy(&mut ar, &mut bw), copy(&mut br, &mut aw))?;
    Ok(())
}
fn pair() -> Result<(std::os::unix::net::UnixStream, UnixStream)> {
    let (a, b) = std::os::unix::net::UnixStream::pair()?;
    a.set_read_timeout(Some(IDLE))?;
    a.set_write_timeout(Some(IDLE))?;
    b.set_nonblocking(true)?;
    Ok((a, UnixStream::from_std(b)?))
}
async fn sender(
    store: Store,
    snapshot: String,
    wire: std::os::unix::net::UnixStream,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let repo = Repository::open(&store.path, &store.key_file, false)?;
        super::transfer::send(
            &repo,
            &repo.snapshot(&snapshot)?,
            &mut std::io::BufReader::new(&wire),
            &mut &wire,
        )
    })
    .await?
}
async fn open_receiver(store: Store) -> Result<Repository> {
    tokio::task::spawn_blocking(move || Repository::open(&store.path, &store.key_file, true))
        .await?
}
async fn receiver(repo: Repository, wire: std::os::unix::net::UnixStream) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        super::transfer::receive(&repo, &mut std::io::BufReader::new(&wire), &mut &wire)
    })
    .await?
}
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

async fn native_connect(destination: &Destination) -> Result<Box<dyn Io>> {
    let u = url(destination)?;
    let host = u.host_str().ok_or_else(|| invalid("missing host"))?;
    let stream = tokio::time::timeout(
        Duration::from_secs(15),
        TcpStream::connect((
            host,
            u.port()
                .ok_or_else(|| invalid("native destination needs a port"))?,
        )),
    )
    .await??;
    stream.set_nodelay(true)?;
    if u.scheme() == "n1s" {
        let ca = destination
            .ca_file
            .as_ref()
            .ok_or_else(|| invalid("n1s requires a trusted --ca-file"))?;
        let connector = tokio_rustls::TlsConnector::from(client_tls(ca)?);
        let name = rustls::pki_types::ServerName::try_from(host.to_owned())?;
        Ok(Box::new(
            tokio::time::timeout(Duration::from_secs(15), connector.connect(name, stream))
                .await??,
        ))
    } else {
        Ok(Box::new(stream))
    }
}
async fn native_request(
    stream: &mut Box<dyn Io>,
    operation: &str,
    payload: Value,
) -> Result<NativeResponse> {
    let request = NativeRequest {
        id: ulid::Ulid::new().to_string(),
        op: operation.into(),
        payload: Some(payload.to_string()),
        body: None,
        body_len: None,
    };
    let bytes = rmp_serde::to_vec(&request)?;
    tokio::time::timeout(IDLE, async {
        stream
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(&bytes).await?;
        stream.flush().await
    })
    .await??;
    let length = tokio::time::timeout(IDLE, stream.read_u32()).await?? as usize;
    if length == 0 || length > CHUNK {
        return Err(invalid("invalid native operator response"));
    }
    let mut bytes = vec![0; length];
    tokio::time::timeout(IDLE, stream.read_exact(&mut bytes)).await??;
    let mut response: NativeResponse = rmp_serde::from_slice(&bytes)?;
    if response.body.is_none() && response.body_len.unwrap_or(0) > 0 {
        let length = usize::try_from(response.body_len.unwrap())?;
        if length > CHUNK {
            return Err(invalid("native operator body exceeds control limit"));
        }
        let mut body = vec![0; length];
        tokio::time::timeout(IDLE, stream.read_exact(&mut body)).await??;
        response.body = Some(body.into());
    }
    if response.id != request.id || !response.ok {
        return Err(invalid(
            response
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "native operator rejected request".into()),
        ));
    }
    Ok(response)
}

pub async fn replicate(source: Store, snapshot: &str, destination: &Destination) -> Result<()> {
    let u = url(destination)?;
    let secret = token(&destination.token_file)?;
    let (blocking, local) = pair()?;
    if matches!(u.scheme(), "n1" | "n1s") {
        let mut remote = native_connect(destination).await?;
        native_request(
            &mut remote,
            "backup.receive",
            json!({"token":secret,"repository":destination.repository}),
        )
        .await?;
        let (sent, bridged) = tokio::join!(
            sender(source, snapshot.into(), blocking),
            pipe(local, remote)
        );
        sent?;
        bridged?;
    } else {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut ws_url = u.clone();
        ws_url
            .set_scheme(if u.scheme() == "https" { "wss" } else { "ws" })
            .map_err(|_| invalid("invalid websocket scheme"))?;
        ws_url.set_path("/noa/v1/maintenance/backups/receive");
        ws_url
            .query_pairs_mut()
            .append_pair("repository", &destination.repository);
        let mut request = ws_url.as_str().into_client_request()?;
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {secret}").parse()?);
        let connector = destination
            .ca_file
            .as_ref()
            .map(|p| client_tls(p).map(tokio_tungstenite::Connector::Rustls))
            .transpose()?;
        let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .max_message_size(Some(CHUNK))
            .max_frame_size(Some(CHUNK));
        let (socket, _) = tokio::time::timeout(
            Duration::from_secs(15),
            tokio_tungstenite::connect_async_tls_with_config(
                request,
                Some(config),
                true,
                connector,
            ),
        )
        .await??;
        let (sent, bridged) = tokio::join!(
            sender(source, snapshot.into(), blocking),
            bridge_client(socket, local)
        );
        sent?;
        bridged?;
    }
    Ok(())
}
async fn bridge_client<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: tokio_tungstenite::WebSocketStream<S>,
    mut local: UnixStream,
) -> Result<()> {
    use tokio_tungstenite::tungstenite::Message as M;
    let mut buffer = vec![0; CHUNK];
    loop {
        tokio::select! {
            n = tokio::time::timeout(IDLE, local.read(&mut buffer)) => {
                let n = n??;
                if n == 0 { let _ = socket.close(None).await; return Ok(()); }
                tokio::time::timeout(IDLE, socket.send(M::Binary(buffer[..n].to_vec().into()))).await??;
            }
            frame = tokio::time::timeout(IDLE, socket.next()) => match frame? {
                Some(Ok(M::Binary(data))) => { tokio::time::timeout(IDLE, local.write_all(&data)).await??; },
                Some(Ok(M::Close(_))) | None => { local.shutdown().await?; return Ok(()); },
                Some(Ok(M::Ping(_) | M::Pong(_))) => {},
                Some(Ok(_)) => return Err(invalid("replication requires binary stream frames")),
                Some(Err(e)) => return Err(e.into()),
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub operation: String,
    #[serde(default)]
    pub repository: String,
    #[serde(default)]
    pub snapshot: String,
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub job: String,
    #[serde(default)]
    pub confirm: bool,
    #[serde(default)]
    pub resume: bool,
    #[serde(default)]
    pub keep_last: usize,
    #[serde(default)]
    pub cursor: String,
    #[serde(default)]
    pub limit: usize,
}
#[derive(Clone, Serialize)]
struct Job {
    id: String,
    operation: String,
    state: String,
    result: Option<Value>,
    error: Option<String>,
}
struct Host {
    config: Config,
    secret: String,
    transfers: Arc<Semaphore>,
    jobs: Mutex<VecDeque<Job>>,
    busy: Arc<Semaphore>,
    live_source: bool,
}
static LIVE_SOURCE: std::sync::OnceLock<Arc<Host>> = std::sync::OnceLock::new();

/// Opt-in engine capability. Only preset repositories and a dedicated token;
/// neither arbitrary filesystem paths nor fragment credentials grant access.
pub fn initialize_live_source() -> Result<()> {
    let Some(path) = std::env::var_os("N1_BACKUP_OPERATOR_CONFIG") else {
        return Ok(());
    };
    let config: Config = serde_json::from_slice(&private_file(Path::new(&path), 65536)?)?;
    validate_config(&config)?;
    if config.engine_config.is_none() {
        return Err(invalid("online source requires engine_config"));
    }
    let secret = token(&config.token_file)?;
    LIVE_SOURCE
        .set(Arc::new(Host {
            config,
            secret,
            transfers: Arc::new(Semaphore::new(4)),
            jobs: Mutex::new(VecDeque::new()),
            busy: Arc::new(Semaphore::new(1)),
            live_source: true,
        }))
        .map_err(|_| invalid("online source initialized twice"))
}

pub async fn live_operation(headers: HeaderMap, Json(request): Json<Request>) -> impl IntoResponse {
    let Some(host) = LIVE_SOURCE.get().cloned() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"online backup source is not configured"})),
        )
            .into_response();
    };
    operation(State(host), headers, Json(request))
        .await
        .into_response()
}

pub async fn live_native_operation(mut payload: Value) -> Result<Value> {
    let host = LIVE_SOURCE
        .get()
        .cloned()
        .ok_or_else(|| invalid("online backup source is not configured"))?;
    let supplied = payload.as_object_mut().and_then(|p| p.remove("token"));
    if !supplied
        .as_ref()
        .and_then(Value::as_str)
        .is_some_and(|t| host.authorized(t))
    {
        return Err(invalid("operator authentication rejected"));
    }
    dispatch(host, serde_json::from_value(payload)?).await
}
impl Host {
    fn authorized(&self, supplied: &str) -> bool {
        let a = super::crypto::digest(supplied.as_bytes());
        let b = super::crypto::digest(self.secret.as_bytes());
        a.bytes().zip(b.bytes()).fold(0u8, |v, (a, b)| v | (a ^ b)) == 0
    }
    fn store(&self, name: &str) -> Result<Store> {
        self.config
            .repositories
            .get(name)
            .cloned()
            .ok_or_else(|| invalid("unknown repository alias"))
    }
}
fn authenticate(host: &Host, headers: &HeaderMap) -> std::result::Result<(), StatusCode> {
    if let Some(origin) = headers.get("origin") {
        if !origin.to_str().ok().is_some_and(|o| {
            host.config
                .allowed_origins
                .iter()
                .any(|allowed| allowed == o)
        }) {
            return Err(StatusCode::FORBIDDEN);
        }
    }
    let value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if host.authorized(value) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}
fn execute(config: &Config, request: &Request, live_source: bool) -> Result<Value> {
    let store = || {
        config
            .repositories
            .get(&request.repository)
            .ok_or_else(|| invalid("unknown repository alias"))
    };
    let repo = |writable| {
        let s = store()?;
        Repository::open(&s.path, &s.key_file, writable)
    };
    match request.operation.as_str() {
        "list" => {
            if !request.cursor.is_empty() {
                super::fs::safe_id(&request.cursor)?;
            }
            let limit = if request.limit == 0 {
                20
            } else {
                request.limit.min(100)
            };
            let r = repo(false)?;
            // Keep only a bounded page of names, then decode one manifest at a time.
            let mut names = std::collections::BTreeSet::new();
            for entry in std::fs::read_dir(r.path.join("snapshots"))? {
                let entry = entry?;
                let id = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| invalid("invalid snapshot name"))?;
                super::fs::safe_id(&id)?;
                if request.cursor.is_empty() || id < request.cursor {
                    names.insert(id);
                    if names.len() > limit + 1 {
                        names.pop_first();
                    }
                }
            }
            let has_more = names.len() > limit;
            if has_more {
                names.pop_first();
            }
            let mut list = Vec::with_capacity(names.len());
            let mut cursor = None;
            for id in names.into_iter().rev() {
                let s = r.snapshot(&id)?;
                cursor = Some(id);
                list.push(json!({"snapshot":s.id,"created_at":s.created_at,"members":s.roots.len(),"source_bytes":s.source_bytes,"records":s.records,"capture_mode":s.capture_mode.as_deref().unwrap_or("offline_full")}));
            }
            Ok(
                json!({"snapshots":list,"has_more":has_more,"cursor":if has_more {cursor}else{None}}),
            )
        }
        "init" if request.confirm => {
            let s = store()?;
            Repository::initialize(&s.path, &s.key_file)?;
            Ok(json!({"initialized":true}))
        }
        "create" if request.confirm => {
            let config = config
                .engine_config
                .as_ref()
                .ok_or_else(|| invalid("no source engine config is allowed"))?;
            let s = super::capture::create(
                &repo(true)?,
                super::capture::roots_from_config(config)?,
                4,
            )?;
            Ok(json!({"snapshot":s.id,"source_bytes":s.source_bytes,"records":s.records}))
        }
        "create-online" if request.confirm => {
            if live_source {
                super::online::capture(
                    &repo(true)?,
                    config
                        .engine_config
                        .as_deref()
                        .ok_or_else(|| invalid("no source engine config"))?,
                    config.capture_boundary_seconds,
                )
            } else {
                let destination = config
                    .online_source
                    .as_ref()
                    .ok_or_else(|| invalid("online source is not configured"))?;
                if request.repository != destination.repository {
                    return Err(invalid(
                        "online capture is limited to the configured source repository alias",
                    ));
                }
                let mut forwarded = request.clone();
                forwarded.repository = destination.repository.clone();
                let result = run_async(completed_operation(
                    destination,
                    forwarded,
                    config.capture_timeout_seconds,
                ))?;
                super::online::validate_result(&result)?;
                Ok(result)
            }
        }
        "verify" => {
            let r = repo(false)?;
            super::restore::verify(&r, &r.snapshot(&request.snapshot)?)?;
            Ok(json!({"verified":request.snapshot}))
        }
        "replicate" => {
            let destination = config
                .replicas
                .get(&request.target)
                .ok_or_else(|| invalid("unknown replica alias"))?;
            run_async(replicate(store()?.clone(), &request.snapshot, destination))?;
            Ok(json!({"replicated":request.snapshot,"target":request.target}))
        }
        "restore" if request.confirm => {
            let target = config
                .restore_targets
                .get(&request.target)
                .ok_or_else(|| invalid("unknown restore target alias"))?;
            let r = repo(false)?;
            super::restore::restore(
                &r,
                &r.snapshot(&request.snapshot)?,
                super::capture::roots_from_config(target)?,
                request.resume,
            )?;
            Ok(json!({"restored":request.snapshot,"target":request.target}))
        }
        "forget" if request.confirm => {
            super::retention::forget(&repo(true)?, &request.snapshot)?;
            Ok(json!({"forgotten":request.snapshot}))
        }
        "gc" if request.confirm => {
            Ok(json!({"reclaimed_bytes":super::retention::gc(&repo(true)?, false)?}))
        }
        "prune" if request.confirm => {
            super::retention::prune(&repo(true)?, request.keep_last, &request.snapshot)?;
            Ok(json!({"retained":true}))
        }
        "run-job" if request.confirm => {
            let path = config
                .jobs
                .get(&request.job)
                .ok_or_else(|| invalid("unknown supervised job alias"))?;
            super::automation::run_enabled(path)
        }
        "job-status" => {
            let path = config
                .jobs
                .get(&request.job)
                .ok_or_else(|| invalid("unknown supervised job alias"))?;
            super::automation::status_value(path)
        }
        _ => Err(invalid(
            "unknown operation or explicit confirmation required",
        )),
    }
}
async fn dispatch(host: Arc<Host>, request: Request) -> Result<Value> {
    if host.live_source
        && !matches!(
            request.operation.as_str(),
            "capabilities" | "operations" | "create-online"
        )
    {
        return Err(invalid(
            "running N1 exposes committed online capture only; repository maintenance belongs to Shell",
        ));
    }
    if request.operation == "capabilities" {
        return Ok(
            json!({"protocol":"n1-backup-v1","repositories":host.config.repositories.keys().collect::<Vec<_>>(),"replicas":host.config.replicas.keys().collect::<Vec<_>>(),"restore_targets":host.config.restore_targets.keys().collect::<Vec<_>>(),"jobs":host.config.jobs.keys().collect::<Vec<_>>(),"capture_requires_stopped_engine":true,"online_committed_capture":host.live_source || host.config.online_source.is_some(),"capture_boundary_seconds":host.config.capture_boundary_seconds}),
        );
    }
    if request.operation == "operations" {
        return Ok(
            json!({"operations":host.jobs.lock().map_err(|_| invalid("job state poisoned"))?.iter().cloned().collect::<Vec<_>>() }),
        );
    }
    if matches!(request.operation.as_str(), "list" | "job-status") {
        let config = host.config.clone();
        return tokio::task::spawn_blocking(move || execute(&config, &request, false)).await?;
    }
    // Validate operation/aliases before accepting a job. Never execute browser-supplied paths.
    if !matches!(
        request.operation.as_str(),
        "init"
            | "create"
            | "create-online"
            | "verify"
            | "replicate"
            | "restore"
            | "forget"
            | "gc"
            | "prune"
            | "run-job"
    ) {
        return Err(invalid("unknown backup operation"));
    }
    if !matches!(request.operation.as_str(), "verify" | "replicate") && !request.confirm {
        return Err(invalid("explicit confirmation required"));
    }
    if request.operation != "run-job" {
        host.store(&request.repository)?;
    }
    if matches!(
        request.operation.as_str(),
        "verify" | "replicate" | "restore" | "forget" | "prune"
    ) {
        super::fs::safe_id(&request.snapshot)?;
    }
    let permit = host
        .busy
        .clone()
        .try_acquire_owned()
        .map_err(|_| invalid("another operator job is running"))?;
    let id = ulid::Ulid::new().to_string();
    let job = Job {
        id: id.clone(),
        operation: request.operation.clone(),
        state: "running".into(),
        result: None,
        error: None,
    };
    {
        let mut jobs = host
            .jobs
            .lock()
            .map_err(|_| invalid("job state poisoned"))?;
        if jobs.len() >= 32 {
            jobs.pop_front();
        }
        jobs.push_back(job);
    }
    let job_id = id.clone();
    tokio::spawn(async move {
        let config = host.config.clone();
        let live_source = host.live_source;
        let outcome =
            tokio::task::spawn_blocking(move || execute(&config, &request, live_source)).await;
        if let Ok(mut jobs) = host.jobs.lock() {
            if let Some(job) = jobs.iter_mut().find(|j| j.id == job_id) {
                match outcome {
                    Ok(Ok(value)) => {
                        job.state = "complete".into();
                        job.result = Some(value);
                    }
                    other => {
                        job.state = "failed".into();
                        job.error = Some(
                            match other {
                                Ok(Err(e)) => e.to_string(),
                                Err(e) => e.to_string(),
                                _ => unreachable!(),
                            }
                            .chars()
                            .take(4096)
                            .collect(),
                        );
                    }
                }
            }
        }
        drop(permit);
    });
    Ok(json!({"operation_id":id,"state":"running"}))
}
async fn operation(
    State(host): State<Arc<Host>>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> impl IntoResponse {
    if let Err(status) = authenticate(&host, &headers) {
        return (
            status,
            Json(json!({"error":"operator authentication/origin rejected"})),
        );
    }
    match dispatch(host, request).await {
        Ok(value) => (StatusCode::OK, Json(value)),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":e.to_string()})),
        ),
    }
}
#[derive(Deserialize)]
struct ReceiveQuery {
    repository: String,
}
async fn receive_http(
    State(host): State<Arc<Host>>,
    headers: HeaderMap,
    Query(query): Query<ReceiveQuery>,
    upgrade: WebSocketUpgrade,
) -> axum::response::Response {
    if let Err(status) = authenticate(&host, &headers) {
        return status.into_response();
    }
    let Ok(store) = host.store(&query.repository) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(permit) = host.transfers.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let repo = match open_receiver(store).await {
        Ok(repo) => repo,
        Err(error) => {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error":error.to_string()})),
            )
                .into_response();
        }
    };
    upgrade.max_message_size(CHUNK).max_frame_size(CHUNK).on_upgrade(move |mut socket| async move {
        let _permit = permit;
        let Ok((blocking, mut local)) = pair() else { return; };
        let bridge = async move {
            let mut buffer = vec![0; CHUNK];
            loop {
                tokio::select! {
                    n = tokio::time::timeout(IDLE, local.read(&mut buffer)) => { let n = n??; if n == 0 { let _ = socket.close().await; return Ok::<_, super::Error>(()); } tokio::time::timeout(IDLE, socket.send(Message::Binary(buffer[..n].to_vec().into()))).await??; },
                    message = tokio::time::timeout(IDLE, socket.recv()) => match message? {
                        Some(Ok(Message::Binary(data))) => { tokio::time::timeout(IDLE, local.write_all(&data)).await??; },
                        Some(Ok(Message::Close(_))) | None => { local.shutdown().await?; return Ok(()); },
                        Some(Ok(Message::Ping(_) | Message::Pong(_))) => {},
                        _ => return Err(invalid("invalid replication frame")),
                    }
                }
            }
        };
        let (received, streamed) = tokio::join!(receiver(repo, blocking), bridge);
        if let Err(e) = received.and(streamed) { eprintln!("N1 backup receive failed: {e}"); }
    }).into_response()
}
async fn native_connection(host: Arc<Host>, mut stream: Box<dyn Io>) -> Result<()> {
    let request = tokio::time::timeout(
        Duration::from_secs(15),
        read_request_control_with_limit(&mut stream, CHUNK),
    )
    .await??
    .ok_or_else(|| invalid("missing native operator request"))?;
    let mut payload: Value = serde_json::from_str(request.payload.as_deref().unwrap_or("{}"))?;
    if request.body.is_some()
        || request.body_len.is_some()
        || !host.authorized(payload["token"].as_str().unwrap_or(""))
    {
        write_response(
            &mut stream,
            &NativeResponse::err(
                request.id,
                "unauthorized",
                "operator authentication rejected",
            ),
        )
        .await?;
        return Ok(());
    }
    if request.op == "backup.receive" {
        let repo = match host
            .store(payload["repository"].as_str().unwrap_or(""))
            .map(|store| open_receiver(store))
        {
            Ok(open) => match open.await {
                Ok(repo) => repo,
                Err(error) => {
                    write_response(
                        &mut stream,
                        &NativeResponse::err(
                            request.id,
                            "repository_unavailable",
                            error.to_string(),
                        ),
                    )
                    .await?;
                    return Ok(());
                }
            },
            Err(error) => {
                write_response(
                    &mut stream,
                    &NativeResponse::err(request.id, "invalid_repository", error.to_string()),
                )
                .await?;
                return Ok(());
            }
        };
        let _permit = host
            .transfers
            .clone()
            .try_acquire_owned()
            .map_err(|_| invalid("receiver busy"))?;
        write_response(
            &mut stream,
            &NativeResponse::ok(request.id, json!({"ready":true})),
        )
        .await?;
        let (blocking, local) = pair()?;
        let (received, streamed) = tokio::join!(receiver(repo, blocking), pipe(stream, local));
        received?;
        streamed?;
    } else if request.op == "backup.operation" {
        payload
            .as_object_mut()
            .ok_or_else(|| invalid("invalid payload"))?
            .remove("token");
        let result = dispatch(host, serde_json::from_value(payload)?).await;
        let response = match result {
            Ok(v) => NativeResponse::ok(request.id, v),
            Err(e) => NativeResponse::err(request.id, "bad_request", e.to_string()),
        };
        write_response(&mut stream, &response).await?;
    } else {
        write_response(
            &mut stream,
            &NativeResponse::err(request.id, "unsupported", "unknown operator operation"),
        )
        .await?;
    }
    Ok(())
}
pub async fn remote_operation(destination: &Destination, request: Request) -> Result<Value> {
    let u = url(destination)?;
    let secret = token(&destination.token_file)?;
    if matches!(u.scheme(), "n1" | "n1s") {
        let mut stream = native_connect(destination).await?;
        let mut payload = serde_json::to_value(request)?;
        payload["token"] = json!(secret);
        let response = native_request(&mut stream, "backup.operation", payload).await?;
        match (response.payload, response.body) {
            (Some(payload), _) => Ok(serde_json::from_str(&payload)?),
            (None, Some(body)) => Ok(rmp_serde::from_slice(&body)?),
            _ => Err(invalid("native operator returned no control result")),
        }
    } else {
        let mut builder = reqwest::Client::builder()
            .timeout(IDLE)
            .redirect(reqwest::redirect::Policy::none());
        if let Some(ca) = &destination.ca_file {
            builder =
                builder.add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(ca)?)?);
        }
        let response = builder
            .build()?
            .post(u.join("/noa/v1/maintenance/backups/operation")?)
            .bearer_auth(secret)
            .json(&request)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(invalid(format!(
                "remote operator rejected operation ({})",
                response.status()
            )));
        }
        Ok(response.json().await?)
    }
}
pub async fn completed_operation(
    destination: &Destination,
    request: Request,
    seconds: u64,
) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(seconds), async {
        let value = remote_operation(destination, request).await?;
        let Some(id) = value["operation_id"].as_str() else {
            return Ok(value);
        };
        loop {
            let status = remote_operation(
                destination,
                Request {
                    operation: "operations".into(),
                    ..Default::default()
                },
            )
            .await?;
            let job = status["operations"]
                .as_array()
                .and_then(|list| list.iter().find(|j| j["id"] == id))
                .ok_or_else(|| invalid("remote job disappeared; outcome unconfirmed"))?;
            match job["state"].as_str() {
                Some("complete") => return Ok(job["result"].clone()),
                Some("failed") => {
                    return Err(invalid(
                        job["error"].as_str().unwrap_or("remote job failed"),
                    ));
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
    })
    .await?
}
pub fn run_async<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    // Offline entrypoints run before the storage engine initializes its TLS provider.
    let _ = rustls::crypto::ring::default_provider().install_default();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(future)
}
pub fn recover_operator(path: &Path) -> Result<()> {
    let config: Config = serde_json::from_slice(&private_file(path, 65536)?)?;
    let mut failures = Vec::new();
    for (name, path) in config.jobs {
        if let Err(error) = super::automation::recover_configured(&path) {
            failures.push(format!("{name}: {error}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(invalid(failures.join("; ")))
    }
}
pub fn serve(path: &Path) -> Result<()> {
    recover_operator(path)?;
    let config: Config = serde_json::from_slice(&private_file(path, 65536)?)?;
    validate_config(&config)?;
    let secret = token(&config.token_file)?;
    if config.repositories.is_empty()
        || config.repositories.len() > 32
        || config
            .repositories
            .keys()
            .chain(config.replicas.keys())
            .chain(config.restore_targets.keys())
            .chain(config.jobs.keys())
            .any(|k| !alias(k))
    {
        return Err(invalid("invalid configured operator aliases"));
    }
    let bind: std::net::SocketAddr = config.bind.parse()?;
    let native = config
        .native_bind
        .as_ref()
        .map(|s| s.parse::<std::net::SocketAddr>())
        .transpose()?;
    if config.tls_cert.is_some() != config.tls_key.is_some() {
        return Err(invalid("both TLS certificate and key are required"));
    }
    if config.tls_cert.is_none() && !config.allow_plaintext {
        return Err(invalid(
            "operator listener requires TLS or explicit allow_plaintext for a trusted network",
        ));
    }
    for store in config.repositories.values() {
        if !store.path.is_absolute() || !store.key_file.is_absolute() {
            return Err(invalid("configured repository/key paths must be absolute"));
        }
    }
    run_async(async move {
        let tls = match (&config.tls_cert, &config.tls_key) {
            (Some(c), Some(k)) => {
                Some(axum_server::tls_rustls::RustlsConfig::from_pem_file(c, k).await?)
            }
            _ => None,
        };
        let host = Arc::new(Host {
            config,
            secret,
            transfers: Arc::new(Semaphore::new(4)),
            jobs: Mutex::new(VecDeque::new()),
            busy: Arc::new(Semaphore::new(1)),
            live_source: false,
        });
        let app = Router::new()
            .route("/noa/v1/maintenance/backups/operation", post(operation))
            .route("/noa/v1/maintenance/backups/receive", get(receive_http))
            .layer(DefaultBodyLimit::max(CHUNK))
            .with_state(host.clone());
        // Explicit exact-origin CORS; never send authentication tokens to redirects.
        let allowed = host
            .config
            .allowed_origins
            .iter()
            .map(|o| o.parse())
            .collect::<std::result::Result<Vec<axum::http::HeaderValue>, _>>()?;
        let app = app.layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(allowed)
                .allow_methods([axum::http::Method::POST])
                .allow_headers([
                    axum::http::header::AUTHORIZATION,
                    axum::http::header::CONTENT_TYPE,
                ]),
        );
        if let Some(addr) = native {
            let listener = TcpListener::bind(addr).await?;
            let host = host.clone();
            let tls = tls.clone();
            tokio::spawn(async move {
                let connections = Arc::new(Semaphore::new(8));
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    let Ok(permit) = connections.clone().try_acquire_owned() else {
                        continue;
                    };
                    let host = host.clone();
                    let tls = tls.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _ = stream.set_nodelay(true);
                        let stream: Box<dyn Io> = if let Some(tls) = tls {
                            let acceptor = tokio_rustls::TlsAcceptor::from(tls.get_inner());
                            match tokio::time::timeout(
                                Duration::from_secs(15),
                                acceptor.accept(stream),
                            )
                            .await
                            {
                                Ok(Ok(s)) => Box::new(s),
                                _ => return,
                            }
                        } else {
                            Box::new(stream)
                        };
                        if let Err(e) = native_connection(host, stream).await {
                            tracing::warn!("N1 operator connection failed: {e}");
                        }
                    });
                }
            });
        }
        println!(
            "N1-owned backup operator endpoint: {}://{bind}",
            if tls.is_some() { "https" } else { "http" }
        );
        if let Some(tls) = tls {
            axum_server::bind_rustls(bind, tls)
                .serve(app.into_make_service())
                .await?;
        } else {
            axum::serve(TcpListener::bind(bind).await?, app).await?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::super::{
        capture, crypto,
        format::{Kind, Root},
        restore,
    };
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt};
    #[tokio::test]
    async fn duplex_pipe_flushes_before_waiting_for_protocol_acknowledgements() {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        struct Observed {
            stream: tokio::io::DuplexStream,
            flushes: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl AsyncRead for Observed {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                Pin::new(&mut self.stream).poll_read(cx, buf)
            }
        }
        impl AsyncWrite for Observed {
            fn poll_write(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                Pin::new(&mut self.stream).poll_write(cx, buf)
            }
            fn poll_flush(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                self.flushes
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Pin::new(&mut self.stream).poll_flush(cx)
            }
            fn poll_shutdown(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Pin::new(&mut self.stream).poll_shutdown(cx)
            }
        }
        let (a, mut receiver) = tokio::io::duplex(64);
        let (b, mut sender) = tokio::io::duplex(64);
        let flushes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let copied = tokio::spawn(pipe(
            Observed {
                stream: a,
                flushes: flushes.clone(),
            },
            b,
        ));
        sender.write_all(b"offer").await.unwrap();
        let mut bytes = [0; 5];
        receiver.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"offer");
        assert!(flushes.load(std::sync::atomic::Ordering::Relaxed) > 0);
        receiver.write_all(b"have!").await.unwrap();
        sender.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"have!");
        copied.abort();
    }
    #[tokio::test]
    async fn native_control_consumes_the_engines_streamed_messagepack_body() {
        let fixture = Fixture::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request_control_with_limit(&mut stream, CHUNK)
                .await
                .unwrap()
                .unwrap();
            write_response(
                &mut stream,
                &NativeResponse::operation(
                    request.id,
                    crate::protocol::types::NativeOpResponse { status: 200 },
                    crate::protocol::ops::native_body_bytes(
                        &json!({"online_committed_capture":true}),
                    ),
                ),
            )
            .await
            .unwrap();
        });
        let destination = Destination {
            url: format!("n1://{address}"),
            repository: "replica".into(),
            token_file: fixture.token.clone(),
            ca_file: None,
            allow_plaintext: true,
        };
        let result = remote_operation(
            &destination,
            Request {
                operation: "capabilities".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(result["online_committed_capture"], true);
        server.await.unwrap();
    }
    struct Fixture {
        base: PathBuf,
        key: PathBuf,
        root: PathBuf,
        source: Store,
        target: Store,
        snapshot: String,
        token: PathBuf,
    }
    impl Fixture {
        fn new() -> Self {
            let base = std::env::var_os("N1_BACKUP_TEST_ROOT")
                .map(PathBuf::from)
                .unwrap_or_else(std::env::temp_dir)
                .join(format!("n1-owned-replication-{}", ulid::Ulid::new()));
            fs::create_dir(&base).unwrap();
            let key = base.join("key");
            crypto::create_key(&key).unwrap();
            let token = base.join("operator.token");
            fs::write(&token, "owned-operator-token-0123456789abcdef0123456789").unwrap();
            fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).unwrap();
            let root = base.join("source-root");
            fs::create_dir(&root).unwrap();
            drop(
                crate::core::instance_lease::InstanceLease::engine(std::slice::from_ref(&root))
                    .unwrap(),
            );
            // More than one transfer buffer and one backup block, deterministic bytes.
            let data = (0usize..(5 * 1024 * 1024 + 91))
                .map(|i| (i.wrapping_mul(31) % 251) as u8)
                .collect::<Vec<_>>();
            fs::write(root.join("data"), data).unwrap();
            let source = Store {
                path: base.join("source-repository"),
                key_file: key.clone(),
            };
            let target = Store {
                path: base.join("replica-repository"),
                key_file: key.clone(),
            };
            Repository::initialize(&source.path, &key).unwrap();
            Repository::initialize(&target.path, &key).unwrap();
            let repo = Repository::open(&source.path, &key, true).unwrap();
            let snapshot = capture::create(
                &repo,
                vec![Root {
                    path: root.to_string_lossy().into_owned(),
                    legacy_path: None,
                    index: true,
                    blob: true,
                    member: None,
                }],
                1,
            )
            .unwrap()
            .id;
            Self {
                base,
                key,
                root,
                source,
                target,
                snapshot,
                token,
            }
        }
        fn host(&self) -> Arc<Host> {
            Arc::new(Host {
                config: Config {
                    bind: "127.0.0.1:0".into(),
                    native_bind: None,
                    token_file: self.token.clone(),
                    tls_cert: None,
                    tls_key: None,
                    allow_plaintext: true,
                    allowed_origins: vec!["https://console.example".into()],
                    repositories: BTreeMap::from([("replica".into(), self.target.clone())]),
                    engine_config: None,
                    restore_targets: BTreeMap::new(),
                    replicas: BTreeMap::new(),
                    jobs: BTreeMap::new(),
                    online_source: None,
                    capture_boundary_seconds: 30,
                    capture_timeout_seconds: 3600,
                },
                secret: token(&self.token).unwrap(),
                transfers: Arc::new(Semaphore::new(4)),
                jobs: Mutex::new(VecDeque::new()),
                busy: Arc::new(Semaphore::new(1)),
                live_source: false,
            })
        }
        async fn endpoint(&self, native: bool) -> (Destination, tokio::task::JoinHandle<()>) {
            let host = self.host();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let task = if native {
                tokio::spawn(async move {
                    loop {
                        let (stream, _) = listener.accept().await.unwrap();
                        let host = host.clone();
                        tokio::spawn(async move {
                            let _ = native_connection(host, Box::new(stream)).await;
                        });
                    }
                })
            } else {
                let app = Router::new()
                    .route("/noa/v1/maintenance/backups/operation", post(operation))
                    .route("/noa/v1/maintenance/backups/receive", get(receive_http))
                    .layer(DefaultBodyLimit::max(CHUNK))
                    .with_state(host);
                tokio::spawn(async move {
                    axum::serve(listener, app).await.unwrap();
                })
            };
            (
                Destination {
                    url: format!("{}://{address}", if native { "n1" } else { "http" }),
                    repository: "replica".into(),
                    token_file: self.token.clone(),
                    ca_file: None,
                    allow_plaintext: true,
                },
                task,
            )
        }
        fn verify_restore(&self) {
            let repo = Repository::open(&self.target.path, &self.key, false).unwrap();
            let snapshot = repo.snapshot(&self.snapshot).unwrap();
            restore::verify(&repo, &snapshot).unwrap();
            let root = self.base.join("restored");
            fs::create_dir(&root).unwrap();
            restore::restore(
                &repo,
                &snapshot,
                vec![Root {
                    path: root.to_string_lossy().into_owned(),
                    legacy_path: None,
                    index: true,
                    blob: true,
                    member: None,
                }],
                false,
            )
            .unwrap();
            assert_eq!(
                fs::read(root.join("data")).unwrap(),
                fs::read(self.root.join("data")).unwrap()
            );
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.base);
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_and_native_replicate_restore_and_repeat_without_ssh() {
        for native in [false, true] {
            let f = Fixture::new();
            let (destination, task) = f.endpoint(native).await;
            replicate(f.source.clone(), &f.snapshot, &destination)
                .await
                .unwrap();
            let before = fs::metadata(
                f.target
                    .path
                    .join("snapshots")
                    .join(&f.snapshot)
                    .join("manifest"),
            )
            .unwrap()
            .modified()
            .unwrap();
            replicate(f.source.clone(), &f.snapshot, &destination)
                .await
                .unwrap();
            assert_eq!(
                before,
                fs::metadata(
                    f.target
                        .path
                        .join("snapshots")
                        .join(&f.snapshot)
                        .join("manifest")
                )
                .unwrap()
                .modified()
                .unwrap()
            );
            f.verify_restore();
            task.abort();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wrong_operator_token_origin_and_repository_are_rejected() {
        for native in [false, true] {
            let f = Fixture::new();
            let (mut destination, task) = f.endpoint(native).await;
            let wrong = f.base.join("wrong.token");
            fs::write(&wrong, "wrong-token-0123456789abcdef0123456789").unwrap();
            fs::set_permissions(&wrong, fs::Permissions::from_mode(0o600)).unwrap();
            destination.token_file = wrong;
            assert!(
                replicate(f.source.clone(), &f.snapshot, &destination)
                    .await
                    .is_err()
            );
            destination.token_file = f.token.clone();
            destination.repository = "../../outside".into();
            assert!(
                replicate(f.source.clone(), &f.snapshot, &destination)
                    .await
                    .is_err()
            );
            destination.repository = "unknown".into();
            assert!(
                replicate(f.source.clone(), &f.snapshot, &destination)
                    .await
                    .is_err()
            );
            assert_eq!(
                fs::read_dir(f.target.path.join("snapshots"))
                    .unwrap()
                    .count(),
                0
            );
            if !native {
                let response = reqwest::Client::new()
                    .post(format!(
                        "{}/noa/v1/maintenance/backups/operation",
                        destination.url
                    ))
                    .bearer_auth(token(&f.token).unwrap())
                    .header("origin", "https://evil.example")
                    .json(&json!({"operation":"capabilities"}))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::FORBIDDEN);
            }
            task.abort();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn corrupt_block_and_wrong_repository_key_never_publish_a_snapshot() {
        for native in [false, true] {
            let f = Fixture::new();
            let (destination, task) = f.endpoint(native).await;
            let repo = Repository::open(&f.source.path, &f.key, false).unwrap();
            let snapshot = repo.snapshot(&f.snapshot).unwrap();
            let mut hash = String::new();
            repo.records(&snapshot, |record| {
                if let Kind::File { blocks, .. } = record.kind {
                    hash = blocks[0].clone();
                }
                Ok(())
            })
            .unwrap();
            let path = repo.block_path(&hash).unwrap();
            let healthy = fs::read(&path).unwrap();
            drop(repo);
            fs::write(&path, b"corrupt-source-block").unwrap();
            assert!(
                replicate(f.source.clone(), &f.snapshot, &destination)
                    .await
                    .is_err()
            );
            assert!(!f.target.path.join("snapshots").join(&f.snapshot).exists());
            fs::write(&path, &healthy).unwrap();
            replicate(f.source.clone(), &f.snapshot, &destination)
                .await
                .unwrap();
            let wrong = f.base.join("wrong.key");
            crypto::create_key(&wrong).unwrap();
            assert!(Repository::open(&f.target.path, &wrong, false).is_err());
            f.verify_restore();
            task.abort();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn operator_jobs_require_confirmation_and_report_real_verification_failure() {
        let f = Fixture::new();
        let host = f.host();
        assert!(
            dispatch(
                host.clone(),
                Request {
                    operation: "restore".into(),
                    repository: "replica".into(),
                    snapshot: f.snapshot.clone(),
                    ..Default::default()
                }
            )
            .await
            .is_err()
        );
        assert!(
            dispatch(
                host.clone(),
                Request {
                    operation: "arbitrary-command".into(),
                    ..Default::default()
                }
            )
            .await
            .is_err()
        );
        let accepted = dispatch(
            host.clone(),
            Request {
                operation: "verify".into(),
                repository: "replica".into(),
                snapshot: f.snapshot.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        for _ in 0..100 {
            let jobs = dispatch(
                host.clone(),
                Request {
                    operation: "operations".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let job = jobs["operations"]
                .as_array()
                .unwrap()
                .iter()
                .find(|j| j["id"] == accepted["operation_id"])
                .unwrap();
            if job["state"] == "failed" {
                assert!(job["error"].is_string());
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("missing snapshot verification was not reported as failure");
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_network_disconnect_never_publishes_and_retry_reuses_verified_blocks() {
        for native in [false, true] {
            let f = Fixture::new();
            let (destination, server) = f.endpoint(native).await;
            let address = reqwest::Url::parse(&destination.url).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy_address = listener.local_addr().unwrap();
            let remote_address = format!(
                "{}:{}",
                address.host_str().unwrap(),
                address.port().unwrap()
            );
            let proxy = tokio::spawn(async move {
                let (mut client, _) = listener.accept().await.unwrap();
                let mut remote = TcpStream::connect(remote_address).await.unwrap();
                let (read, mut write) = tokio::io::split(&mut client);
                let (mut peer_read, mut peer_write) = tokio::io::split(&mut remote);
                let mut limited = read.take((4 * 1024 * 1024 + CHUNK) as u64);
                let sending = async {
                    let _ = tokio::io::copy(&mut limited, &mut peer_write).await;
                    let _ = peer_write.shutdown().await;
                };
                let replies = async {
                    let _ = tokio::io::copy(&mut peer_read, &mut write).await;
                    let _ = write.shutdown().await;
                };
                tokio::join!(sending, replies);
            });
            let mut interrupted = destination.clone();
            interrupted.url = format!("{}://{proxy_address}", if native { "n1" } else { "http" });
            assert!(
                tokio::time::timeout(
                    Duration::from_secs(30),
                    replicate(f.source.clone(), &f.snapshot, &interrupted)
                )
                .await
                .unwrap()
                .is_err()
            );
            proxy.await.unwrap();
            assert!(!f.target.path.join("snapshots").join(&f.snapshot).exists());
            let blocks = walkdir::WalkDir::new(f.target.path.join("blocks"))
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
                .map(|e| {
                    (
                        e.path().to_path_buf(),
                        fs::metadata(e.path()).unwrap().modified().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert!(
                blocks.len() >= 2,
                "interruption must follow real manifest/data publication"
            );
            replicate(f.source.clone(), &f.snapshot, &destination)
                .await
                .unwrap();
            for (path, time) in blocks {
                assert_eq!(
                    time,
                    fs::metadata(path).unwrap().modified().unwrap(),
                    "verified block was unnecessarily rewritten"
                );
            }
            f.verify_restore();
            server.abort();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn receiver_with_a_different_valid_encryption_key_rejects_both_transports() {
        for native in [false, true] {
            let mut f = Fixture::new();
            let wrong = f.base.join("different.key");
            crypto::create_key(&wrong).unwrap();
            let path = f.base.join("different-repository");
            Repository::initialize(&path, &wrong).unwrap();
            f.target = Store {
                path: path.clone(),
                key_file: wrong,
            };
            let (destination, server) = f.endpoint(native).await;
            assert!(
                replicate(f.source.clone(), &f.snapshot, &destination)
                    .await
                    .is_err()
            );
            assert_eq!(fs::read_dir(path.join("snapshots")).unwrap().count(), 0);
            server.abort();
        }
    }
    #[test]
    fn plaintext_requires_explicit_opt_in_and_urls_cannot_carry_credentials() {
        let f = Fixture::new();
        let mut d = Destination {
            url: "http://127.0.0.1:50902".into(),
            repository: "replica".into(),
            token_file: f.token.clone(),
            ca_file: None,
            allow_plaintext: false,
        };
        assert!(url(&d).is_err());
        d.allow_plaintext = true;
        assert!(url(&d).is_ok());
        for bad in [
            "http://user:password@host",
            "ssh://host",
            "http://host/path",
            "http://host?token=secret",
        ] {
            d.url = bad.into();
            assert!(url(&d).is_err());
        }
    }
}
