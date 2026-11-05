use crate::{
    Command, NativeClientError, NativeClientResult, Request, Response, frame,
    url::parse_native_address,
};
use n1_link::noa::MAX_BODY_BYTES;
use std::sync::Arc;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
trait NativeIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> NativeIo for T {}

pub struct N1NativeClient {
    stream: Box<dyn NativeIo>,
    next_id: u64,
    healthy: bool,
}
impl N1NativeClient {
    pub async fn connect(url: &str) -> NativeClientResult<Self> {
        Self::connect_with_tls(url, None).await
    }
    /// A custom verified CA configuration is useful for private installations.
    /// Secure URLs never fall back to plaintext or disable verification.
    pub async fn connect_with_tls(
        url: &str,
        tls: Option<Arc<rustls::ClientConfig>>,
    ) -> NativeClientResult<Self> {
        let address = parse_native_address(url)?;
        let stream = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            TcpStream::connect(&address.authority),
        )
        .await
        .map_err(|_| NativeClientError::protocol("native connection timed out"))??;
        stream.set_nodelay(true)?;
        let stream: Box<dyn NativeIo> = if address.tls {
            let config = match tls {
                Some(config) => config,
                None => {
                    let roots = rustls::RootCertStore::from_iter(
                        webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
                    );
                    Arc::new(
                        rustls::ClientConfig::builder_with_provider(Arc::new(
                            rustls::crypto::ring::default_provider(),
                        ))
                        .with_safe_default_protocol_versions()
                        .map_err(|e| NativeClientError::protocol(e.to_string()))?
                        .with_root_certificates(roots)
                        .with_no_client_auth(),
                    )
                }
            };
            let name = rustls::pki_types::ServerName::try_from(address.host)
                .map_err(|_| NativeClientError::protocol("invalid TLS server name"))?;
            let connector = tokio_rustls::TlsConnector::from(config);
            let stream = tokio::time::timeout(
                std::time::Duration::from_secs(15),
                connector.connect(name, stream),
            )
            .await
            .map_err(|_| NativeClientError::protocol("native TLS handshake timed out"))??;
            Box::new(stream)
        } else {
            Box::new(stream)
        };
        Ok(Self {
            stream,
            next_id: 0,
            healthy: true,
        })
    }
    fn request(&mut self, command: Command, body_len: u64) -> NativeClientResult<Request> {
        if !self.healthy {
            return Err(NativeClientError::protocol(
                "connection has an incomplete exchange; reconnect before retrying",
            ));
        }
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| NativeClientError::protocol("native request IDs exhausted"))?;
        // Cancellation or any transport failure leaves this false. The next
        // request cannot reuse a stream at an unknown frame/body boundary.
        self.healthy = false;
        Ok(Request {
            id: self.next_id,
            command,
            body_len,
        })
    }
    async fn response(&mut self, id: u64) -> NativeClientResult<Response> {
        let response = frame::read(&mut self.stream).await?;
        if response.id != id {
            return Err(NativeClientError::protocol("native response ID mismatch"));
        }
        if response.status != 200 && response.body_len != 0 {
            return Err(NativeClientError::protocol(
                "error response cannot carry object bytes",
            ));
        }
        Ok(response)
    }
    pub async fn control(&mut self, command: Command) -> NativeClientResult<Response> {
        let request = self.request(command, 0)?;
        frame::write(&mut self.stream, &request).await?;
        let response = self.response(request.id).await?;
        if response.body_len != 0 {
            return Err(NativeClientError::protocol(
                "use download for streaming responses",
            ));
        }
        self.healthy = true;
        ensure_ok(response)
    }
    pub async fn auth_fragment(
        &mut self,
        fragment: &str,
        secret: &str,
    ) -> NativeClientResult<Response> {
        self.control(Command::Login {
            fragment: fragment.into(),
            secret: secret.into(),
            permissions: None,
            ttl_seconds: None,
        })
        .await
    }
    pub async fn auth_bearer(&mut self, token: String) -> NativeClientResult<Response> {
        self.control(Command::Bearer { token }).await
    }
    pub async fn into_replication(
        mut self,
        repository: String,
    ) -> NativeClientResult<impl AsyncRead + AsyncWrite + Unpin + Send> {
        self.control(Command::BackupReceive { repository }).await?;
        Ok(self.stream)
    }
    pub async fn write_at<R: AsyncRead + Unpin>(
        &mut self,
        object_key: String,
        offset: u64,
        length: u64,
        source: &mut R,
    ) -> NativeClientResult<Response> {
        if length > MAX_BODY_BYTES {
            return Err(NativeClientError::protocol(
                "part exceeds native body limit",
            ));
        }
        self.upload(Command::UploadPart { object_key, offset }, length, source)
            .await
    }
    pub async fn put<R: AsyncRead + Unpin>(
        &mut self,
        object_key: String,
        length: u64,
        source: &mut R,
    ) -> NativeClientResult<Response> {
        if length > i64::MAX as u64 {
            return Err(NativeClientError::protocol(
                "object exceeds file offset limit",
            ));
        }
        self.upload(Command::UploadPut { object_key }, length, source)
            .await
    }
    pub async fn put_batch<R: AsyncRead + Unpin>(
        &mut self,
        object_keys: Vec<String>,
        length: u64,
        source: &mut R,
    ) -> NativeClientResult<Response> {
        if length > MAX_BODY_BYTES {
            return Err(NativeClientError::protocol(
                "batch exceeds native body limit",
            ));
        }
        self.upload(Command::UploadBatch { object_keys }, length, source)
            .await
    }
    async fn upload<R: AsyncRead + Unpin>(
        &mut self,
        command: Command,
        length: u64,
        source: &mut R,
    ) -> NativeClientResult<Response> {
        let request = self.request(command, length)?;
        frame::write(&mut self.stream, &request).await?;
        frame::copy_exact(source, &mut self.stream, length).await?;
        let response = self.response(request.id).await?;
        if response.body_len != 0 {
            return Err(NativeClientError::protocol(
                "upload response cannot carry raw bytes",
            ));
        }
        self.healthy = true;
        ensure_ok(response)
    }
    pub async fn download<W: AsyncWrite + Unpin>(
        &mut self,
        object_key: String,
        offset: u64,
        length: Option<u64>,
        destination: &mut W,
    ) -> NativeClientResult<(Response, u64)> {
        self.download_command(
            Command::ObjectGet {
                object_key,
                offset,
                length,
            },
            length,
            destination,
        )
        .await
    }
    pub async fn download_signed<W: AsyncWrite + Unpin>(
        &mut self,
        grant: n1_link::noa::ReadGrant,
        destination: &mut W,
    ) -> NativeClientResult<(Response, u64)> {
        let length = grant.length;
        self.download_command(Command::SignedGet { grant }, length, destination)
            .await
    }
    async fn download_command<W: AsyncWrite + Unpin>(
        &mut self,
        command: Command,
        length: Option<u64>,
        destination: &mut W,
    ) -> NativeClientResult<(Response, u64)> {
        let request = self.request(command, 0)?;
        frame::write(&mut self.stream, &request).await?;
        let response = self.response(request.id).await?;
        if response.status != 200 {
            self.healthy = true;
            return ensure_ok(response).map(|r| (r, 0));
        }
        if length.is_some_and(|length| length != response.body_len) {
            return Err(NativeClientError::protocol(
                "download range length differs from response",
            ));
        }
        let bytes = response.body_len;
        frame::copy_exact(&mut self.stream, destination, bytes).await?;
        self.healthy = true;
        Ok((response, bytes))
    }
}
fn ensure_ok(response: Response) -> NativeClientResult<Response> {
    if response.status == 200 {
        Ok(response)
    } else {
        Err(NativeClientError::Native {
            code: response.status.to_string(),
            message: response
                .error
                .unwrap_or_else(|| "native operation failed".into()),
        })
    }
}
