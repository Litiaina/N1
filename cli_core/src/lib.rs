//! Native LINK client configuration and allocation discovery shared by CLI tools.
pub mod monitoring;
pub mod settings;
#[cfg(target_os = "linux")]
pub mod system;
use n1_link::PoolId;
use n1_native_client::{Command, N1NativeClient, Response};
use serde::Deserialize;
use std::{path::Path, sync::Arc};
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Deserialize)]
pub struct Credentials {
    pub native_url: Option<String>,
    pub fragment: Option<String>,
    pub secret: Option<String>,
    pub token: Option<String>,
}
impl Credentials {
    pub fn load(path: &Path) -> Result<Self> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if file.metadata()?.permissions().mode() & 0o077 != 0 {
                return Err("credentials must be owner-only (chmod 600)".into());
            }
        }
        if !file.metadata()?.is_file() || file.metadata()?.len() > 64 * 1024 {
            return Err("credentials must be a small regular file".into());
        }
        Ok(serde_json::from_reader(file)?)
    }
    pub async fn connect(
        &self,
        endpoint: Option<&str>,
        ca: Option<&Path>,
    ) -> Result<N1NativeClient> {
        let endpoint = endpoint
            .or(self.native_url.as_deref())
            .ok_or("native endpoint is required")?;
        let tls = ca.map(client_tls).transpose()?;
        let mut client = N1NativeClient::connect_with_tls(endpoint, tls).await?;
        if let Some(token) = &self.token {
            client.auth_bearer(token.clone()).await?;
        } else {
            client
                .auth_fragment(
                    self.fragment.as_deref().ok_or("fragment is required")?,
                    self.secret.as_deref().ok_or("secret is required")?,
                )
                .await?;
        }
        Ok(client)
    }
}
pub fn client_tls(path: &Path) -> Result<Arc<rustls::ClientConfig>> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(path)? {
        roots.add(cert?)?;
    }
    if roots.is_empty() {
        return Err("CA file contains no certificates".into());
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
pub fn result(response: Response) -> Result<serde_json::Value> {
    response
        .result
        .ok_or_else(|| "operation returned no result".into())
}
pub async fn stripe_bytes(client: &mut N1NativeClient, pool: Option<PoolId>) -> Result<u64> {
    let pools = result(client.control(Command::Pools).await?)?;
    let pool = pools["pools"]
        .as_array()
        .ok_or("invalid pools response")?
        .iter()
        .rev()
        .find(|p| p["sealed"] == false && pool.is_none_or(|id| p["id"] == id.to_string()))
        .ok_or("no matching unsealed pool")?;
    Ok(pool["data_shards"].as_u64().ok_or("pool has no K")?
        * pool["block_bytes"]
            .as_u64()
            .ok_or("pool has no block size")?)
}
pub fn aligned_part(bytes: u64, stripe: u64) -> Result<u64> {
    if stripe == 0 {
        return Err("invalid pool stripe".into());
    }
    bytes
        .max(1)
        .div_ceil(stripe)
        .checked_mul(stripe)
        .ok_or_else(|| "part size overflow".into())
}
