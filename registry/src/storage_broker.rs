use std::sync::Arc;

use hex::encode as hex_encode;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::config::{RegistryConfig, normalize_backend_url};
use crate::error::RegistryError;
use crate::models::{FragmentBindingRecord, StorageDeployment, StorageSessionResponse};

#[derive(Clone)]
pub struct StorageBroker {
    strict_client: Client,
    insecure_client: Client,
    config: Arc<RegistryConfig>,
}

#[derive(Debug, Serialize)]
struct FragmentAuthRequest<'a> {
    fragment: &'a str,
    secret: &'a str,
    read: bool,
    write: bool,
    modify: bool,
    delete: bool,
    expires_in: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct EngineAuthResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
    refresh_token: Option<String>,
    refresh_expires_in: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct EngineErrorResponse {
    error: Option<String>,
    message: Option<String>,
}

#[derive(Debug, Serialize)]
struct CreateFragmentRequest<'a> {
    name: &'a str,
    secret: &'a str,
    key: &'a str,
}

#[derive(Debug, Deserialize)]
struct CreateFragmentResponse {
    fragment_hash: String,
}

#[derive(Debug, Serialize)]
struct UpdateFragmentRequest<'a> {
    new_name: &'a str,
    secret: &'a str,
}

#[derive(Debug, Deserialize)]
struct UpdateFragmentResponse {
    fragment_hash: String,
}

#[derive(Debug)]
struct ResolvedBackend {
    deployment: StorageDeployment,
    engine_url: String,
    master_key: String,
    insecure_tls: bool,
}

impl StorageBroker {
    pub fn new(config: Arc<RegistryConfig>) -> Result<Self, RegistryError> {
        let strict_client = Client::builder().use_rustls_tls().build().map_err(|err| {
            RegistryError::internal(format!("storage broker client failed: {}", err))
        })?;
        let insecure_client = Client::builder()
            .use_rustls_tls()
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .build()
            .map_err(|err| {
                RegistryError::internal(format!("storage broker insecure client failed: {}", err))
            })?;
        Ok(Self {
            strict_client,
            insecure_client,
            config,
        })
    }

    pub async fn issue_fragment_session(
        &self,
        binding: &FragmentBindingRecord,
        read: bool,
        write: bool,
        modify: bool,
        delete: bool,
    ) -> Result<StorageSessionResponse, RegistryError> {
        let backend = self.resolve_backend(binding)?;
        let payload = self
            .authenticate_fragment(&backend, &binding.fragment, read, write, modify, delete)
            .await?;

        Ok(StorageSessionResponse {
            fragment: binding.fragment.clone(),
            deployment: backend.deployment,
            engine_url: backend.engine_url,
            access_token: payload.access_token,
            token_type: payload.token_type,
            expires_in: payload.expires_in,
            refresh_token: payload.refresh_token,
            refresh_expires_in: payload.refresh_expires_in,
        })
    }

    pub async fn ensure_default_fragment(
        &self,
        fragment_name: &str,
    ) -> Result<(StorageDeployment, String), RegistryError> {
        let backend = self.resolve_default_backend()?;
        let fragment_hash = hash_fragment_name(fragment_name);
        let fragment_key = Uuid::new_v4().to_string();

        let response = self
            .client_for_backend(&backend)
            .post(format!("{}/v4/fragment/create", backend.engine_url))
            .json(&CreateFragmentRequest {
                name: fragment_name,
                secret: &backend.master_key,
                key: &fragment_key,
            })
            .send()
            .await
            .map_err(|err| {
                RegistryError::bad_gateway(format!(
                    "storage fragment create failed for {}: {}",
                    backend.engine_url, err
                ))
            })?;

        if response.status().is_success() {
            let payload = response
                .json::<CreateFragmentResponse>()
                .await
                .map_err(|err| {
                    RegistryError::bad_gateway(format!("invalid fragment create response: {}", err))
                })?;
            return Ok((backend.deployment, payload.fragment_hash));
        }

        if response.status() == reqwest::StatusCode::CONFLICT {
            return Ok((backend.deployment, fragment_hash));
        }

        let fallback = format!(
            "storage fragment create failed against {} backend",
            match backend.deployment {
                StorageDeployment::SingleNode => "single-node",
                StorageDeployment::MultiNode => "multi-node",
            }
        );
        let message = response
            .json::<EngineErrorResponse>()
            .await
            .ok()
            .and_then(|body| body.error.or(body.message))
            .unwrap_or(fallback);
        Err(RegistryError::bad_gateway(message))
    }

    pub async fn stream_manifest_archive(
        &self,
        binding: &FragmentBindingRecord,
        manifest_path: &Path,
        filename: Option<&str>,
    ) -> Result<reqwest::Response, RegistryError> {
        let backend = self.resolve_backend(binding)?;
        let payload = self
            .authenticate_fragment(&backend, &binding.fragment, true, false, false, false)
            .await?;

        let file = File::open(manifest_path).await.map_err(|err| {
            RegistryError::internal(format!(
                "failed to open archive manifest for streaming: {}",
                err
            ))
        })?;
        let body = reqwest::Body::wrap_stream(ReaderStream::new(file));
        let request = self
            .client_for_backend(&backend)
            .post(format!(
                "{}/v4/fragment/archive/manifest",
                backend.engine_url
            ))
            .bearer_auth(payload.access_token)
            .query(&[
                ("format", "tar"),
                ("filename", filename.unwrap_or("export")),
            ])
            .body(body);

        let response = request.send().await.map_err(|err| {
            RegistryError::bad_gateway(format!(
                "storage archive request failed for {}: {}",
                backend.engine_url, err
            ))
        });
        let _ = tokio::fs::remove_file(manifest_path).await;
        let response = response?;

        if !response.status().is_success() {
            let fallback = format!(
                "storage archive failed against {} backend",
                match backend.deployment {
                    StorageDeployment::SingleNode => "single-node",
                    StorageDeployment::MultiNode => "multi-node",
                }
            );
            let message = extract_engine_error_message(response, fallback).await;
            return Err(RegistryError::bad_gateway(message));
        }

        Ok(response)
    }

    pub async fn rename_fragment(
        &self,
        binding: &FragmentBindingRecord,
        new_name: &str,
    ) -> Result<String, RegistryError> {
        let backend = self.resolve_backend(binding)?;
        let payload = self
            .authenticate_fragment(&backend, &binding.fragment, true, false, true, false)
            .await?;
        let response = self
            .client_for_backend(&backend)
            .patch(format!(
                "{}/v4/fragment/modify/{}",
                backend.engine_url, binding.fragment
            ))
            .bearer_auth(payload.access_token)
            .json(&UpdateFragmentRequest {
                new_name,
                secret: &backend.master_key,
            })
            .send()
            .await
            .map_err(|err| {
                RegistryError::bad_gateway(format!(
                    "storage fragment rename failed for {}: {}",
                    backend.engine_url, err
                ))
            })?;

        if !response.status().is_success() {
            let fallback = format!(
                "storage fragment rename failed against {} backend",
                match backend.deployment {
                    StorageDeployment::SingleNode => "single-node",
                    StorageDeployment::MultiNode => "multi-node",
                }
            );
            let message = extract_engine_error_message(response, fallback).await;
            return Err(RegistryError::bad_gateway(message));
        }

        let updated = response
            .json::<UpdateFragmentResponse>()
            .await
            .map_err(|err| {
                RegistryError::bad_gateway(format!("invalid fragment rename response: {}", err))
            })?;
        Ok(updated.fragment_hash)
    }

    pub async fn delete_fragment(
        &self,
        binding: &FragmentBindingRecord,
    ) -> Result<(), RegistryError> {
        let backend = self.resolve_backend(binding)?;
        let payload = self
            .authenticate_fragment(&backend, &binding.fragment, false, false, true, true)
            .await?;
        let response = self
            .client_for_backend(&backend)
            .delete(format!(
                "{}/v4/fragment/delete/{}",
                backend.engine_url, binding.fragment
            ))
            .bearer_auth(payload.access_token)
            .send()
            .await
            .map_err(|err| {
                RegistryError::bad_gateway(format!(
                    "storage fragment delete failed for {}: {}",
                    backend.engine_url, err
                ))
            })?;
        if !response.status().is_success() {
            let fallback = format!(
                "storage fragment delete failed against {} backend",
                match backend.deployment {
                    StorageDeployment::SingleNode => "single-node",
                    StorageDeployment::MultiNode => "multi-node",
                }
            );
            let message = extract_engine_error_message(response, fallback).await;
            return Err(RegistryError::bad_gateway(message));
        }
        Ok(())
    }

    fn resolve_backend(
        &self,
        binding: &FragmentBindingRecord,
    ) -> Result<ResolvedBackend, RegistryError> {
        let (default_url, master_key, insecure_tls) = match binding.deployment {
            StorageDeployment::SingleNode => (
                self.config.single_node_url.clone(),
                self.config.single_node_master_key.clone(),
                self.config.single_node_insecure_tls,
            ),
            StorageDeployment::MultiNode => (
                self.config.multi_node_url.clone(),
                self.config.multi_node_master_key.clone(),
                self.config.multi_node_insecure_tls,
            ),
        };
        let engine_url = binding
            .engine_url_override
            .clone()
            .or(default_url)
            .ok_or_else(|| {
                RegistryError::conflict(match binding.deployment {
                    StorageDeployment::SingleNode => {
                        "single-node storage backend is not configured in the registry"
                    }
                    StorageDeployment::MultiNode => {
                        "multi-node storage backend is not configured in the registry"
                    }
                })
            })?;
        let engine_url = normalize_backend_url(&engine_url).map_err(|err| {
            RegistryError::bad_request(format!("invalid storage backend URL: {}", err))
        })?;
        let master_key = master_key.ok_or_else(|| {
            RegistryError::conflict(match binding.deployment {
                StorageDeployment::SingleNode => {
                    "single-node storage master key is not configured in the registry"
                }
                StorageDeployment::MultiNode => {
                    "multi-node storage master key is not configured in the registry"
                }
            })
        })?;

        Ok(ResolvedBackend {
            deployment: binding.deployment,
            engine_url,
            master_key,
            insecure_tls,
        })
    }

    fn resolve_default_backend(&self) -> Result<ResolvedBackend, RegistryError> {
        if self.config.single_node_url.is_some() && self.config.single_node_master_key.is_some() {
            let engine_url = normalize_backend_url(
                self.config
                    .single_node_url
                    .as_deref()
                    .expect("checked single node url"),
            )
            .map_err(|err| {
                RegistryError::bad_request(format!(
                    "invalid single-node storage backend URL: {}",
                    err
                ))
            })?;
            return Ok(ResolvedBackend {
                deployment: StorageDeployment::SingleNode,
                engine_url,
                master_key: self
                    .config
                    .single_node_master_key
                    .clone()
                    .expect("checked single node master key"),
                insecure_tls: self.config.single_node_insecure_tls,
            });
        }

        if self.config.multi_node_url.is_some() && self.config.multi_node_master_key.is_some() {
            let engine_url = normalize_backend_url(
                self.config
                    .multi_node_url
                    .as_deref()
                    .expect("checked multi node url"),
            )
            .map_err(|err| {
                RegistryError::bad_request(format!(
                    "invalid multi-node storage backend URL: {}",
                    err
                ))
            })?;
            return Ok(ResolvedBackend {
                deployment: StorageDeployment::MultiNode,
                engine_url,
                master_key: self
                    .config
                    .multi_node_master_key
                    .clone()
                    .expect("checked multi node master key"),
                insecure_tls: self.config.multi_node_insecure_tls,
            });
        }

        Err(RegistryError::conflict(
            "registry has no default storage backend configured for automatic fragment provisioning",
        ))
    }

    fn client_for_backend(&self, backend: &ResolvedBackend) -> &Client {
        if backend.insecure_tls || should_allow_insecure_tls_for_url(&backend.engine_url) {
            &self.insecure_client
        } else {
            &self.strict_client
        }
    }

    async fn authenticate_fragment(
        &self,
        backend: &ResolvedBackend,
        fragment: &str,
        read: bool,
        write: bool,
        modify: bool,
        delete: bool,
    ) -> Result<EngineAuthResponse, RegistryError> {
        let response = self
            .client_for_backend(backend)
            .post(format!("{}/authenticate/fragment", backend.engine_url))
            .json(&FragmentAuthRequest {
                fragment,
                secret: &backend.master_key,
                read,
                write,
                modify,
                delete,
                expires_in: Some(self.config.storage_session_ttl_secs),
            })
            .send()
            .await
            .map_err(|err| {
                RegistryError::bad_gateway(format!(
                    "storage auth request failed for {}: {}",
                    backend.engine_url, err
                ))
            })?;

        if !response.status().is_success() {
            let fallback = format!(
                "storage auth failed against {} backend",
                match backend.deployment {
                    StorageDeployment::SingleNode => "single-node",
                    StorageDeployment::MultiNode => "multi-node",
                }
            );
            let message = extract_engine_error_message(response, fallback).await;
            return Err(RegistryError::bad_gateway(
                normalize_engine_auth_error_message(message),
            ));
        }

        response.json::<EngineAuthResponse>().await.map_err(|err| {
            RegistryError::bad_gateway(format!("invalid storage auth response: {}", err))
        })
    }
}

fn normalize_engine_auth_error_message(message: String) -> String {
    let trimmed = message.trim();
    if trimmed.eq_ignore_ascii_case("An internal error occurred") {
        return "storage fragment auth failed inside N1; check that REGISTRY_SINGLE_NODE_MASTER_KEY or REGISTRY_MULTI_NODE_MASTER_KEY matches the engine MASTER_KEY, and that the fragment metadata exists and is readable".to_string();
    }
    trimmed.to_string()
}

async fn extract_engine_error_message(response: reqwest::Response, fallback: String) -> String {
    if let Ok(body) = response.json::<EngineErrorResponse>().await {
        if let Some(message) = body.error.or(body.message) {
            return message;
        }
    }
    fallback
}

fn hash_fragment_name(name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hex_encode(hasher.finalize())
}

fn should_allow_insecure_tls_for_url(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    if parsed.scheme() != "https" {
        return false;
    }
    let Some(host) = parsed.host_str() else {
        return false;
    };
    let host = host.trim_matches(&['[', ']'][..]);
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .map(|addr| addr.is_loopback())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{normalize_engine_auth_error_message, should_allow_insecure_tls_for_url};

    #[test]
    fn loopback_https_urls_are_allowed_for_insecure_tls() {
        assert!(should_allow_insecure_tls_for_url("https://localhost:50001"));
        assert!(should_allow_insecure_tls_for_url("https://127.0.0.1:50001"));
        assert!(should_allow_insecure_tls_for_url("https://[::1]:50001"));
    }

    #[test]
    fn non_loopback_or_non_https_urls_remain_strict() {
        assert!(!should_allow_insecure_tls_for_url(
            "https://storage.example.com:50001"
        ));
        assert!(!should_allow_insecure_tls_for_url("http://127.0.0.1:50001"));
        assert!(!should_allow_insecure_tls_for_url("not-a-url"));
    }

    #[test]
    fn expands_generic_engine_auth_error_into_registry_hint() {
        let message = normalize_engine_auth_error_message("An internal error occurred".to_string());
        assert!(message.contains("storage fragment auth failed inside N1"));
        assert!(message.contains("MASTER_KEY"));
        assert!(message.contains("fragment metadata"));
    }
}
