use std::sync::Arc;

use hex::encode as hex_encode;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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

#[derive(Debug)]
struct ResolvedBackend {
    deployment: StorageDeployment,
    engine_url: String,
    master_key: String,
    insecure_tls: bool,
}

impl StorageBroker {
    pub fn new(config: Arc<RegistryConfig>) -> Result<Self, RegistryError> {
        let strict_client = Client::builder()
            .use_rustls_tls()
            .build()
            .map_err(|err| RegistryError::internal(format!("storage broker client failed: {}", err)))?;
        let insecure_client = Client::builder()
            .use_rustls_tls()
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .build()
            .map_err(|err| RegistryError::internal(format!("storage broker insecure client failed: {}", err)))?;
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
        let response = self
            .client_for_backend(&backend)
            .post(format!("{}/authenticate/fragment", backend.engine_url))
            .json(&FragmentAuthRequest {
                fragment: &binding.fragment,
                secret: &backend.master_key,
                read,
                write,
                modify,
                delete,
                expires_in: Some(self.config.storage_session_ttl_secs),
            })
            .send()
            .await
            .map_err(|err| RegistryError::bad_gateway(format!(
                "storage auth request failed for {}: {}",
                backend.engine_url, err
            )))?;

        if !response.status().is_success() {
            let fallback = format!(
                "storage auth failed against {} backend",
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
            return Err(RegistryError::bad_gateway(
                normalize_engine_auth_error_message(message),
            ));
        }

        let payload = response
            .json::<EngineAuthResponse>()
            .await
            .map_err(|err| RegistryError::bad_gateway(format!("invalid storage auth response: {}", err)))?;

        Ok(StorageSessionResponse {
            fragment: binding.fragment.clone(),
            deployment: backend.deployment,
            engine_url: backend.engine_url,
            access_token: payload.access_token,
            token_type: payload.token_type,
            expires_in: payload.expires_in,
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
            .map_err(|err| RegistryError::bad_gateway(format!(
                "storage fragment create failed for {}: {}",
                backend.engine_url, err
            )))?;

        if response.status().is_success() {
            let payload = response
                .json::<CreateFragmentResponse>()
                .await
                .map_err(|err| RegistryError::bad_gateway(format!("invalid fragment create response: {}", err)))?;
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
        let engine_url = normalize_backend_url(&engine_url)
            .map_err(|err| RegistryError::bad_request(format!("invalid storage backend URL: {}", err)))?;
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
}

fn normalize_engine_auth_error_message(message: String) -> String {
    let trimmed = message.trim();
    if trimmed.eq_ignore_ascii_case("An internal error occurred") {
        return "storage fragment auth failed inside N1; check that REGISTRY_SINGLE_NODE_MASTER_KEY or REGISTRY_MULTI_NODE_MASTER_KEY matches the engine MASTER_KEY, and that the fragment metadata exists and is readable".to_string();
    }
    trimmed.to_string()
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
        assert!(!should_allow_insecure_tls_for_url("https://storage.example.com:50001"));
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
