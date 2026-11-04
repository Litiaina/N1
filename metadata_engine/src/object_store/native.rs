use super::{
    ObjectFinalization, ObjectStat, ObjectStoreError, ObjectStoreResult, ObjectUpload,
    ObjectUploadStatus,
};
use n1_link::{PoolId, noa::Command};
use n1_native_client::{N1NativeClient, NativeClientError, Response};
use serde::de::DeserializeOwned;
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit, RwLock, Semaphore},
};

/// Internal service credentials, distinct from a user's logical fragment.
/// Authorization of logical names belongs to metadata, before this boundary.
/// No Debug implementation: this value holds a service secret.
pub struct ObjectStoreConfig {
    pub native_url: String,
    pub fragment: String,
    pub secret: String,
    pub max_connections: usize,
    pub tls: Option<Arc<rustls::ClientConfig>>,
}

struct Inner {
    config: ObjectStoreConfig,
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<CachedClient>>,
    session: Mutex<Option<ServiceSession>>,
    lifecycle: Arc<RwLock<()>>,
    logins: Semaphore,
}

#[derive(Clone)]
struct ServiceSession {
    token: String,
    expires_at: u64,
}

struct CachedClient {
    client: N1NativeClient,
    expires_at: u64,
    released_at: Instant,
}

// The independent native server closes an idle control connection after 120
// seconds. Leave a margin rather than retaining it until JWT expiry: a slow
// durable batch can outlive the other connections in this pool.
const MAX_IDLE_AGE: Duration = Duration::from_secs(90);

/// Bounded independent native connections; never one mutex around all object
/// I/O. A cancelled or incomplete exchange discards its connection. A complete
/// control error retains its known frame boundary and authenticated transport.
#[derive(Clone)]
pub struct NativeObjectStore(Arc<Inner>);

struct Connection {
    owner: Arc<Inner>,
    client: N1NativeClient,
    expires_at: u64,
    _permit: OwnedSemaphorePermit,
    _lifecycle: OwnedRwLockReadGuard<()>,
}

// A disconnected source can be retried only before the destination observed
// bytes. Destination failures and partially delivered streams are never replayed.
struct DownloadWriter<'a, W> {
    inner: &'a mut W,
    started: bool,
    failed: bool,
}
impl<W: AsyncWrite + Unpin> AsyncWrite for DownloadWriter<'_, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut *this.inner).poll_write(cx, bytes);
        match &result {
            Poll::Ready(Ok(n)) => this.started |= *n > 0,
            Poll::Ready(Err(_)) => this.failed = true,
            Poll::Pending => (),
        }
        result
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut *this.inner).poll_flush(cx);
        this.failed |= matches!(&result, Poll::Ready(Err(_)));
        result
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut *this.inner).poll_shutdown(cx);
        this.failed |= matches!(&result, Poll::Ready(Err(_)));
        result
    }
}
fn disconnected(error: &NativeClientError) -> bool {
    matches!(error, NativeClientError::Io(error) if matches!(error.kind(),
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset |
        io::ErrorKind::ConnectionAborted | io::ErrorKind::UnexpectedEof |
        io::ErrorKind::NotConnected))
}

impl Connection {
    fn release(self) {
        let mut idle = self.owner.idle.lock().unwrap_or_else(|e| e.into_inner());
        if !self.owner.permits.is_closed() {
            idle.push(CachedClient {
                client: self.client,
                expires_at: self.expires_at,
                released_at: Instant::now(),
            });
        }
    }
}

impl NativeObjectStore {
    /// Operator and health controls use the same authenticated native boundary.
    /// This is private service access, never a pass-through for user commands.
    pub async fn control(&self, command: Command) -> ObjectStoreResult<serde_json::Value> {
        self.command(command).await
    }
    pub fn new(config: ObjectStoreConfig) -> ObjectStoreResult<Self> {
        n1_native_client::parse_native_address(&config.native_url)?;
        if config.fragment.is_empty()
            || config.secret.is_empty()
            || !(1..=4096).contains(&config.max_connections)
        {
            return Err(ObjectStoreError::Invalid(
                "object endpoint requires credentials and 1–4096 connections",
            ));
        }
        Ok(Self(Arc::new(Inner {
            permits: Arc::new(Semaphore::new(config.max_connections)),
            idle: Mutex::new(Vec::new()),
            session: Mutex::new(None),
            lifecycle: Arc::new(RwLock::new(())),
            logins: Semaphore::new(1),
            config,
        })))
    }

    async fn connection(&self) -> ObjectStoreResult<Connection> {
        let lifecycle = self.0.lifecycle.clone().read_owned().await;
        let permit = self
            .0
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ObjectStoreError::Closed)?;
        let (client, expires_at) = self.transport(true).await?;
        Ok(Connection {
            owner: self.0.clone(),
            client,
            expires_at,
            _permit: permit,
            _lifecycle: lifecycle,
        })
    }

    async fn transport(&self, reuse_idle: bool) -> ObjectStoreResult<(N1NativeClient, u64)> {
        let cached = if reuse_idle {
            self.0.idle.lock().unwrap_or_else(|e| e.into_inner()).pop()
        } else {
            None
        };
        let now = unix_now()?;
        let cached = cached.filter(|cached| {
            cached.expires_at > now.saturating_add(30)
                && cached.released_at.elapsed() < MAX_IDLE_AGE
        });
        let (client, expires_at) = if let Some(cached) = cached {
            (cached.client, cached.expires_at)
        } else {
            // Authenticate new transports with the pool's existing service
            // session. Password verification and durable session creation are
            // needed once per renewal, not once per concurrent connection.
            // Neither authentication nor object quotas are bypassed.
            let _login = self
                .0
                .logins
                .acquire()
                .await
                .map_err(|_| ObjectStoreError::Closed)?;
            let mut client = N1NativeClient::connect_with_tls(
                &self.0.config.native_url,
                self.0.config.tls.clone(),
            )
            .await?;
            let now = unix_now()?;
            let session = self
                .0
                .session
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
                .filter(|session| session.expires_at > now.saturating_add(30));
            let response = match &session {
                Some(session) => client.auth_bearer(session.token.clone()).await,
                None => {
                    client
                        .auth_fragment(&self.0.config.fragment, &self.0.config.secret)
                        .await
                }
            }
            .map_err(|error| {
                if matches!(&error, NativeClientError::Native {code,..} if code=="401") {
                    self.invalidate_authorization();
                }
                ObjectStoreError::from(error)
            })?;
            let expires_at = response
                .result
                .as_ref()
                .and_then(|result| result.get("session"))
                .and_then(|session| session.get("exp"))
                .and_then(serde_json::Value::as_u64)
                .filter(|expiry| unix_now().is_ok_and(|now| *expiry > now.saturating_add(30)))
                .ok_or(ObjectStoreError::Contract(
                    "object login returned an invalid session lifetime",
                ))?;
            if session.is_none() {
                let token = response
                    .result
                    .as_ref()
                    .and_then(|r| r["token"].as_str())
                    .filter(|token| !token.is_empty() && token.len() <= 16384)
                    .ok_or(ObjectStoreError::Contract(
                        "object login returned no service token",
                    ))?;
                *self.0.session.lock().unwrap_or_else(|e| e.into_inner()) = Some(ServiceSession {
                    token: token.into(),
                    expires_at,
                });
            }
            (client, expires_at)
        };
        Ok((client, expires_at))
    }

    /// Stop new calls and close idle transports. Accepted calls retain their
    /// permits until done or cancelled; shutdown waits for those owners.
    pub async fn shutdown(&self) {
        self.0.permits.close();
        self.0
            .idle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        let _drained = self.0.lifecycle.write().await;
        self.0
            .session
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
    }

    fn invalidate_authorization(&self) {
        self.0
            .session
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        self.0
            .idle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    fn failed(&self, error: NativeClientError) -> ObjectStoreError {
        if matches!(&error, NativeClientError::Native {code,..} if code=="401") {
            self.invalidate_authorization();
        }
        error.into()
    }

    async fn command<T: DeserializeOwned>(&self, command: Command) -> ObjectStoreResult<T> {
        let mut connection = self.connection().await?;
        match connection.client.control(command).await {
            Ok(response) => {
                let value = decode(response)?;
                connection.release();
                Ok(value)
            }
            Err(error) => {
                // control() returns Native only after consuming and validating
                // the whole response with no body. In particular, an untouched
                // upload's 404 must not cause a new password login per page.
                // An unauthorized session still requires reauthentication.
                if matches!(&error, NativeClientError::Native { code, .. } if code != "401") {
                    connection.release();
                }
                Err(self.failed(error))
            }
        }
    }

    pub async fn init(
        &self,
        pool: Option<PoolId>,
        total_bytes: u64,
        part_bytes: u64,
        durable: bool,
        ttl_seconds: u64,
    ) -> ObjectStoreResult<ObjectUpload> {
        let upload: ObjectUpload = self
            .command(Command::UploadInit {
                pool,
                total_bytes,
                part_bytes,
                durable,
                ttl_seconds,
            })
            .await?;
        if upload.total_bytes != total_bytes
            || upload.part_bytes != part_bytes
            || upload.durable != durable
            || part_bytes == 0
            || upload.parts != total_bytes.div_ceil(part_bytes)
            || !upload.object_key.starts_with("N1A1.")
            || !upload.upload_key.starts_with("N1U1.")
        {
            return Err(ObjectStoreError::Contract(
                "object service returned a different upload plan",
            ));
        }
        Ok(upload)
    }

    pub async fn part<R: AsyncRead + Unpin>(
        &self,
        upload: &ObjectUpload,
        part: u64,
        length: u64,
        source: &mut R,
    ) -> ObjectStoreResult<()> {
        if part >= upload.parts || upload.part_bytes == 0 {
            return Err(ObjectStoreError::Invalid("part is outside the upload plan"));
        }
        let offset = part
            .checked_mul(upload.part_bytes)
            .ok_or(ObjectStoreError::Invalid("part offset overflow"))?;
        if length
            != upload
                .total_bytes
                .saturating_sub(offset)
                .min(upload.part_bytes)
        {
            return Err(ObjectStoreError::Invalid(
                "part length differs from the upload plan",
            ));
        }
        let mut connection = self.connection().await?;
        connection
            .client
            .upload_part(upload.upload_key.clone(), part, length, source)
            .await
            .map_err(|error| self.failed(error))?;
        connection.release();
        Ok(())
    }

    pub async fn put<R: AsyncRead + Unpin>(
        &self,
        upload: &ObjectUpload,
        source: &mut R,
    ) -> ObjectStoreResult<ObjectFinalization> {
        let mut connection = self.connection().await?;
        let result: ObjectFinalization = decode(
            connection
                .client
                .put(upload.upload_key.clone(), upload.total_bytes, source)
                .await
                .map_err(|error| self.failed(error))?,
        )?;
        check_finalization(upload, &result)?;
        connection.release();
        Ok(result)
    }

    pub async fn put_batch<R: AsyncRead + Unpin>(
        &self,
        uploads: &[ObjectUpload],
        source: &mut R,
    ) -> ObjectStoreResult<Vec<ObjectFinalization>> {
        if uploads.is_empty() || uploads.len() > 64 {
            return Err(ObjectStoreError::Invalid(
                "batch must contain 1 through 64 objects",
            ));
        }
        let length: u64 = uploads.iter().map(|u| u.total_bytes).sum();
        if length > 8 * 1024 * 1024
            || uploads
                .iter()
                .any(|u| u.total_bytes > 1024 * 1024 || u.parts > 1)
        {
            return Err(ObjectStoreError::Invalid(
                "batch exceeds bounded small-object geometry",
            ));
        }
        let mut connection = self.connection().await?;
        #[derive(serde::Deserialize)]
        struct BatchFinalizations {
            objects: Vec<ObjectFinalization>,
        }
        let result = decode::<BatchFinalizations>(
            connection
                .client
                .put_batch(
                    uploads.iter().map(|u| u.upload_key.clone()).collect(),
                    length,
                    source,
                )
                .await
                .map_err(|error| self.failed(error))?,
        )?
        .objects;
        if result.len() != uploads.len() {
            return Err(ObjectStoreError::Contract(
                "batch returned a different object count",
            ));
        }
        for (upload, finalized) in uploads.iter().zip(&result) {
            check_finalization(upload, finalized)?;
        }
        connection.release();
        Ok(result)
    }

    pub async fn status(
        &self,
        upload: &ObjectUpload,
        start: u64,
        limit: u64,
    ) -> ObjectStoreResult<ObjectUploadStatus> {
        if limit == 0 || limit > 4096 || start > upload.parts {
            return Err(ObjectStoreError::Invalid("invalid upload status page"));
        }
        let result = self
            .command(Command::UploadStatus {
                upload_key: upload.upload_key.clone(),
                start,
                limit,
            })
            .await;
        match result {
            Err(error) if error.status() == 404 => {
                // LINK init writes no file. An untouched valid ticket therefore
                // has no shard to inspect yet; metadata still knows its plan.
                let end = upload.parts.min(start.saturating_add(limit));
                Ok(ObjectUploadStatus {
                    finalized: false,
                    finalized_shards: 0,
                    parts: upload.parts,
                    checked_start: start,
                    checked_end: end,
                    missing_parts: (start..end).collect(),
                    unavailable_members: Vec::new(),
                })
            }
            result => result,
        }
    }

    pub async fn finalize(&self, upload: &ObjectUpload) -> ObjectStoreResult<ObjectFinalization> {
        let result: ObjectFinalization = self
            .command(Command::UploadFinalize {
                upload_key: upload.upload_key.clone(),
            })
            .await?;
        check_finalization(upload, &result)?;
        Ok(result)
    }

    pub async fn cancel(&self, upload: &ObjectUpload) -> ObjectStoreResult<serde_json::Value> {
        self.command(Command::UploadCancel {
            upload_key: upload.upload_key.clone(),
        })
        .await
    }

    pub async fn stat(&self, object_key: &str) -> ObjectStoreResult<ObjectStat> {
        self.command(Command::ObjectStat {
            object_key: object_key.into(),
        })
        .await
    }

    pub async fn download<W: AsyncWrite + Unpin>(
        &self,
        object_key: &str,
        offset: u64,
        length: Option<u64>,
        destination: &mut W,
    ) -> ObjectStoreResult<u64> {
        let mut connection = self.connection().await?;
        let mut output = DownloadWriter {
            inner: destination,
            started: false,
            failed: false,
        };
        let first = connection
            .client
            .download(object_key.into(), offset, length, &mut output)
            .await;
        let result = match first {
            Err(error) if !output.started && !output.failed && disconnected(&error) => {
                // Keep this operation's original admission/lifecycle guards.
                // A single fresh transport bypasses other stale idle sockets,
                // without another RPC on healthy reads or any write replay.
                let (client, expires_at) = self.transport(false).await?;
                connection.client = client;
                connection.expires_at = expires_at;
                connection
                    .client
                    .download(object_key.into(), offset, length, &mut output)
                    .await
            }
            result => result,
        };
        let (_, length) = result.map_err(|error| self.failed(error))?;
        connection.release();
        Ok(length)
    }

    pub async fn health(
        &self,
        object_key: &str,
        deep: bool,
    ) -> ObjectStoreResult<serde_json::Value> {
        self.command(Command::ObjectHealth {
            object_key: object_key.into(),
            deep,
        })
        .await
    }

    pub async fn repair(&self, object_key: &str) -> ObjectStoreResult<serde_json::Value> {
        self.command(Command::ObjectRepair {
            object_key: object_key.into(),
        })
        .await
    }

    /// Physical, permanent retirement. Logical trash never calls this directly.
    /// The metadata reclamation worker must first establish no remaining refs.
    pub async fn retire(&self, object_key: &str) -> ObjectStoreResult<serde_json::Value> {
        self.command(Command::ObjectDelete {
            object_key: object_key.into(),
        })
        .await
    }
}

fn unix_now() -> ObjectStoreResult<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|time| time.as_secs())
        .map_err(|_| ObjectStoreError::Invalid("system time precedes the Unix epoch"))
}

fn decode<T: DeserializeOwned>(response: Response) -> ObjectStoreResult<T> {
    serde_json::from_value(
        response
            .result
            .ok_or(ObjectStoreError::Contract("object response has no result"))?,
    )
    .map_err(ObjectStoreError::Response)
}

fn check_finalization(upload: &ObjectUpload, result: &ObjectFinalization) -> ObjectStoreResult<()> {
    if result.object_key != upload.object_key || result.total_bytes != upload.total_bytes {
        return Err(ObjectStoreError::Contract(
            "object finalization changed the immutable identity or length",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use n1_link::noa::Request;
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    async fn request(stream: &mut TcpStream) -> Request {
        let length = stream.read_u32().await.unwrap() as usize;
        assert!((1..=8192).contains(&length));
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await.unwrap();
        rmp_serde::from_slice(&bytes).unwrap()
    }
    async fn respond(stream: &mut TcpStream, response: Response) {
        let bytes = rmp_serde::to_vec_named(&response).unwrap();
        stream.write_u32(bytes.len() as u32).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
    }
    async fn authenticated_stat(stream: &mut TcpStream, password: bool) {
        let login = request(stream).await;
        if password {
            assert!(matches!(login.command, Command::Login { .. }));
        } else {
            assert!(
                matches!(login.command, Command::Bearer {token} if token=="service-test-token")
            );
        }
        respond(
            stream,
            Response::ok(
                login.id,
                json!({"token":"service-test-token","session":{"exp":9_999_999_999u64}}),
            ),
        )
        .await;
        let stat = request(stream).await;
        assert!(matches!(stat.command, Command::ObjectStat { .. }));
        respond(stream, Response::ok(stat.id, json!({"total_bytes":17,"data_shards":1,"parity_shards":1,"finalized_shards":2,"integrity_algorithm":"LINK-SHA256-tree-v2","integrity_root":"ab".repeat(32)}))).await;
    }

    #[tokio::test]
    async fn long_batch_idle_connections_are_discarded_before_server_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let store = NativeObjectStore::new(ObjectStoreConfig {
            native_url: format!("n1://{}", listener.local_addr().unwrap()),
            fragment: "private".into(),
            secret: "private-test-secret".into(),
            max_connections: 1,
            tls: None,
        })
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            authenticated_stat(&mut first, true).await;
            // The pool must close its aged transport, without sending another
            // operation on it, and authenticate a fresh independent transport.
            let mut byte = [0];
            assert_eq!(first.read(&mut byte).await.unwrap(), 0);
            let (mut second, _) = listener.accept().await.unwrap();
            authenticated_stat(&mut second, false).await;
            assert_eq!(second.read(&mut byte).await.unwrap(), 0);
            let (mut third, _) = listener.accept().await.unwrap();
            authenticated_stat(&mut third, true).await;
        });
        assert_eq!(store.stat("N1A1.test").await.unwrap().total_bytes, 17);
        {
            let mut idle = store.0.idle.lock().unwrap();
            assert_eq!(idle.len(), 1);
            idle[0].released_at = Instant::now() - Duration::from_secs(121);
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            assert_eq!(store.stat("N1A1.test").await.unwrap().total_bytes, 17);
            store.0.session.lock().unwrap().as_mut().unwrap().expires_at = 0;
            store.0.idle.lock().unwrap()[0].released_at = Instant::now() - Duration::from_secs(121);
            assert_eq!(store.stat("N1A1.renewed").await.unwrap().total_bytes, 17);
            server.await.unwrap();
        })
        .await
        .expect("aged native connection was reused or failed to reconnect");
        store.shutdown().await;
    }

    #[tokio::test]
    async fn revoked_bearer_is_reported_without_replaying_an_object_operation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let store = NativeObjectStore::new(ObjectStoreConfig {
            native_url: format!("n1://{}", listener.local_addr().unwrap()),
            fragment: "private".into(),
            secret: "private-test-secret".into(),
            max_connections: 1,
            tls: None,
        })
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            authenticated_stat(&mut first, true).await;
            let mut byte = [0];
            assert_eq!(first.read(&mut byte).await.unwrap(), 0);
            let (mut rejected, _) = listener.accept().await.unwrap();
            let auth = request(&mut rejected).await;
            assert!(matches!(auth.command, Command::Bearer { .. }));
            respond(
                &mut rejected,
                Response::error(auth.id, 401, "session revoked"),
            )
            .await;
            assert_eq!(
                rejected.read(&mut byte).await.unwrap(),
                0,
                "object command was sent after rejected authentication"
            );
            let (mut next, _) = listener.accept().await.unwrap();
            authenticated_stat(&mut next, true).await;
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            store.stat("N1A1.first").await.unwrap();
            store.0.idle.lock().unwrap()[0].released_at = Instant::now() - Duration::from_secs(121);
            assert_eq!(store.stat("N1A1.rejected").await.unwrap_err().status(), 401);
            // Only a new caller request starts a new password session.
            assert_eq!(
                store.stat("N1A1.next-request").await.unwrap().total_bytes,
                17
            );
            server.await.unwrap();
        })
        .await
        .unwrap();
        store.shutdown().await;
    }
}
