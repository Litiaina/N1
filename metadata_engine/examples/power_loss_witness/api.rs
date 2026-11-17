//! Verified native TLS for N1; verified HTTPS for M1's public named API.
use super::*;
use n1_native_client::{Command, N1NativeClient, NativeClientError};
use std::{path::Path, sync::Arc, time::Duration};

pub enum Api {
    N1(N1NativeClient),
    M1 {
        client: reqwest::Client,
        base: String,
        token: String,
    },
}

fn text(value: &Value, field: &str) -> Result<String> {
    Ok(value[field]
        .as_str()
        .ok_or_else(|| format!("missing {field}"))?
        .into())
}

async fn response(response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    let value: Value = response.json().await?;
    if !status.is_success() {
        return Err(format!("named API returned {status}").into());
    }
    Ok(value)
}

pub fn tls(path: &Path) -> Result<Arc<rustls::ClientConfig>> {
    use rustls::pki_types::{CertificateDer, pem::PemObject};
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(path)? {
        roots.add(certificate?)?;
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

fn secret(config: &Config) -> Result<String> {
    let name = match config.backend {
        Backend::N1 => "AUTH_KEY",
        Backend::M1 => "MASTER_KEY",
    };
    fs::read_to_string(&config.secret_env)?
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name}=")).map(str::to_owned))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("missing {name} in private environment file").into())
}

impl Api {
    pub async fn connect(config: &Config) -> Result<Self> {
        let secret = secret(config)?;
        match config.backend {
            Backend::N1 => {
                if !config.url.starts_with("n1s://") {
                    return Err("N1 requires verified native TLS".into());
                }
                let mut client =
                    N1NativeClient::connect_with_tls(&config.url, Some(tls(&config.ca)?)).await?;
                client.auth_fragment(&config.fragment, &secret).await?;
                Ok(Self::N1(client))
            }
            Backend::M1 => {
                if !config.url.starts_with("https://") {
                    return Err("M1 requires verified HTTPS".into());
                }
                let client = reqwest::Client::builder()
                    .https_only(true)
                    .add_root_certificate(reqwest::Certificate::from_pem(&fs::read(&config.ca)?)?)
                    .connect_timeout(Duration::from_secs(15))
                    .timeout(Duration::from_secs(180))
                    .build()?;
                let base = format!("{}/noa/v1", config.url.trim_end_matches('/'));
                let login = response(
                    client
                        .post(format!("{base}/auth/fragment"))
                        .json(&json!({
                            "fragment":config.fragment,"secret":secret,"read":true,"write":true,
                            "modify":true,"delete":true,"expires_in":86400
                        }))
                        .send()
                        .await?,
                )
                .await?;
                Ok(Self::M1 {
                    client,
                    base,
                    token: text(&login, "access_token")?,
                })
            }
        }
    }

    pub async fn begin(&mut self, spec: &workload::Spec) -> Result<Value> {
        match self {
            Self::N1(client) => Ok(client.control(Command::UploadInit {
                pool:None,total_bytes:spec.total,part_bytes:spec.part_bytes,
                durable:spec.strict,ttl_seconds:86400,
            }).await?.result.ok_or("missing native plan")?),
            Self::M1 { client, base, token } => response(client.post(format!(
                "{base}/upload/{}/init/{}?durability={}", spec.route(),spec.path,spec.durability()
            )).bearer_auth(token.as_str()).json(&json!({"total_size_bytes":spec.total,
                "part_size_bytes":spec.part_bytes,"expected_parts":spec.parts(),"negotiate_part_size":false
            })).send().await?).await,
        }
    }

    pub fn validate_plan(&self, spec: &workload::Spec, plan: &Value) -> Result<()> {
        let valid = match self {
            Self::N1(_) => {
                plan["total_bytes"] == spec.total
                    && plan["part_bytes"] == spec.part_bytes
                    && plan["parts"] == spec.parts()
                    && plan["object_key"].is_string()
            }
            Self::M1 { .. } => {
                plan["total_size_bytes"] == spec.total
                    && plan["part_size_bytes"] == spec.part_bytes
                    && plan["expected_parts"] == spec.parts()
                    && plan["version_id"].is_string()
                    && plan["session_id"].is_string()
            }
        };
        if !valid {
            return Err("server changed explicit witness geometry or identity".into());
        }
        Ok(())
    }

    pub fn validate_finalization(&self, plan: &Value, reply: &Value) -> Result<()> {
        let field = match self {
            Self::N1(_) => "object_key",
            Self::M1 { .. } => "version_id",
        };
        if reply[field] != plan[field] {
            return Err("finalize reply changed the allocated immutable identity".into());
        }
        Ok(())
    }

    pub async fn part(&mut self, spec: &workload::Spec, plan: &Value, part: u64) -> Result<Value> {
        let bytes = spec.payload(part);
        match self {
            Self::N1(client) => Ok(client
                .write_at(
                    text(plan, "object_key")?,
                    part * spec.part_bytes,
                    bytes.len() as u64,
                    &mut bytes.as_slice(),
                )
                .await?
                .result
                .ok_or("missing part reply")?),
            Self::M1 {
                client,
                base,
                token,
            } => {
                response(
                    client
                        .put(format!(
                            "{base}/upload/{}/{}/{part}?durability={}",
                            spec.route(),
                            text(plan, "version_id")?,
                            spec.durability()
                        ))
                        .bearer_auth(token.as_str())
                        .body(bytes)
                        .send()
                        .await?,
                )
                .await
            }
        }
    }

    pub async fn finalize(&mut self, spec: &workload::Spec, plan: &Value) -> Result<Value> {
        match self {
            Self::N1(client) => Ok(client
                .control(Command::UploadFinalize {
                    object_key: text(plan, "object_key")?,
                })
                .await?
                .result
                .ok_or("missing finalize reply")?),
            Self::M1 {
                client,
                base,
                token,
            } => {
                response(
                    client
                        .post(format!(
                            "{base}/upload/{}/{}/{}/finalize",
                            spec.route(),
                            text(plan, "version_id")?,
                            text(plan, "session_id")?
                        ))
                        .bearer_auth(token.as_str())
                        .json(&json!({"total_size_bytes":spec.total,"expected_parts":spec.parts()}))
                        .send()
                        .await?,
                )
                .await
            }
        }
    }

    pub async fn delete(&mut self, spec: &workload::Spec, plan: &Value) -> Result<Value> {
        match self {
            Self::N1(client) => Ok(client
                .control(Command::ObjectDelete {
                    object_key: text(plan, "object_key")?,
                })
                .await?
                .result
                .ok_or("missing delete reply")?),
            Self::M1 {
                client,
                base,
                token,
            } => {
                response(
                    client
                        .delete(format!("{base}/objects/{}", spec.path))
                        .bearer_auth(token.as_str())
                        .send()
                        .await?,
                )
                .await
            }
        }
    }

    pub async fn missing(
        &mut self,
        spec: &workload::Spec,
        plan: &Value,
    ) -> Result<Option<Vec<u64>>> {
        let value = match self {
            Self::N1(client) => match client
                .control(Command::UploadStatus {
                    object_key: text(plan, "object_key")?,
                    start: 0,
                    limit: spec.parts(),
                })
                .await
            {
                Ok(response) => response.result.ok_or("missing status reply")?,
                Err(NativeClientError::Native { code, .. }) if code == "404" => return Ok(None),
                Err(error) => return Err(error.into()),
            },
            Self::M1 {
                client,
                base,
                token,
            } => {
                response(
                    client
                        .get(format!(
                            "{base}/upload/{}/{}",
                            spec.route(),
                            text(plan, "version_id")?
                        ))
                        .bearer_auth(token.as_str())
                        .send()
                        .await?,
                )
                .await?
            }
        };
        let field = if matches!(self, Self::N1(_)) {
            "missing_parts"
        } else {
            "uploaded_parts"
        };
        let parts: std::collections::BTreeSet<u64> = value[field]
            .as_array()
            .ok_or("missing status parts")?
            .iter()
            .map(|value| value.as_u64().ok_or("invalid status part"))
            .collect::<std::result::Result<_, _>>()?;
        if parts.iter().any(|part| *part >= spec.parts()) {
            return Err("out-of-range status part".into());
        }
        if matches!(self, Self::N1(_)) {
            if value["checked_start"] != 0
                || value["checked_end"] != spec.parts()
                || value["unavailable_members"]
                    .as_array()
                    .is_none_or(|members| !members.is_empty())
            {
                return Err("incomplete native status coverage".into());
            }
            Ok(Some(parts.into_iter().collect()))
        } else {
            Ok(Some(
                (0..spec.parts())
                    .filter(|part| !parts.contains(part))
                    .collect(),
            ))
        }
    }

    pub async fn absent(&mut self, spec: &workload::Spec, plan: &Value) -> Result<bool> {
        match self {
            Self::N1(client) => match client
                .control(Command::ObjectStat {
                    object_key: text(plan, "object_key")?,
                })
                .await
            {
                Err(NativeClientError::Native { code, .. }) if code == "404" => Ok(true),
                Ok(_) => Ok(false),
                Err(error) => Err(error.into()),
            },
            Self::M1 {
                client,
                base,
                token,
            } => {
                let response = client
                    .get(format!("{base}/objects/get/{}", spec.path))
                    .bearer_auth(token.as_str())
                    .send()
                    .await?;
                if response.status() == 404 {
                    Ok(true)
                } else if response.status() == 200 {
                    Ok(false)
                } else {
                    Err(format!("unexpected absence status {}", response.status()).into())
                }
            }
        }
    }

    pub async fn verify_bytes(&mut self, spec: &workload::Spec, plan: &Value) -> Result<()> {
        let mut sink = HashSink {
            hash: Sha256::new(),
            bytes: 0,
            limit: spec.total,
        };
        match self {
            Self::N1(client) => {
                client
                    .download(text(plan, "object_key")?, 0, None, &mut sink)
                    .await?;
            }
            Self::M1 {
                client,
                base,
                token,
            } => {
                let mut response = client
                    .get(format!("{base}/objects/get/{}", spec.path))
                    .bearer_auth(token.as_str())
                    .send()
                    .await?;
                if response.status() != 200
                    || response
                        .headers()
                        .get("etag")
                        .and_then(|value| value.to_str().ok())
                        .map(|value| value.trim_matches('"'))
                        != plan["version_id"].as_str()
                {
                    return Err("named read status/version mismatch".into());
                }
                while let Some(bytes) = response.chunk().await? {
                    sink.accept(&bytes)?;
                }
            }
        }
        if sink.bytes != spec.total || format!("{:x}", sink.hash.finalize()) != spec.sha256 {
            return Err(format!(
                "acknowledged object {} length/checksum mismatch",
                spec.index
            )
            .into());
        }
        Ok(())
    }
}

struct HashSink {
    hash: Sha256,
    bytes: u64,
    limit: u64,
}
impl HashSink {
    fn accept(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() as u64 > self.limit - self.bytes {
            return Err(std::io::Error::other("read exceeds expected length"));
        }
        self.hash.update(bytes);
        self.bytes += bytes.len() as u64;
        Ok(bytes.len())
    }
}
impl tokio::io::AsyncWrite for HashSink {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(self.accept(bytes))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
