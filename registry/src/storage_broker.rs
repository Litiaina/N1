use hex::encode as hex_encode;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempfile;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::io::SeekFrom;
use tokio_tar::{Builder as TarBuilder, Header as TarHeader};
use tokio_util::io::StreamReader;
use uuid::Uuid;

use crate::config::{RegistryConfig, normalize_backend_url};
use crate::error::RegistryError;
use crate::models::{
    FragmentBindingRecord, ObjectRecord, ShareDisposition, StorageDeployment,
    StorageSessionResponse,
};
use futures_util::TryStreamExt;

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

#[derive(Debug, Clone)]
pub struct EngineObjectState {
    pub current: Option<EngineObjectMetadata>,
    pub versions: Vec<EngineObjectMetadata>,
}

#[derive(Debug, Clone)]
pub struct EngineObjectMetadata {
    pub version_id: String,
    pub object_size_bytes: i64,
    pub content_type: Option<String>,
    pub created_at_unix: i64,
}

#[derive(Debug, Deserialize)]
struct EngineObjectSummary {
    version_id: String,
    content_type: String,
    total_size_bytes: u64,
    created_at: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineUploadSession {
    pub session_id: String,
    pub version_id: String,
    pub object_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EngineListUploadsResponse {
    sessions: Vec<EngineUploadSession>,
    total: usize,
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
            .post(format!("{}/n1/fragments", backend.engine_url))
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
            self.verify_fragment_ready(&backend, &payload.fragment_hash)
                .await?;
            return Ok((backend.deployment, payload.fragment_hash));
        }

        if response.status() == reqwest::StatusCode::CONFLICT {
            self.verify_fragment_ready(&backend, &fragment_hash).await?;
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

    pub async fn stream_logical_archive(
        &self,
        binding: &FragmentBindingRecord,
        objects: &[ObjectRecord],
    ) -> Result<File, RegistryError> {
        let backend = self.resolve_backend(binding)?;
        let payload = self
            .authenticate_fragment(&backend, &binding.fragment, true, false, false, false)
            .await?;

        let std_file = tempfile().map_err(|err| {
            RegistryError::internal(format!("failed to create archive temp file: {}", err))
        })?;
        let file = File::from_std(std_file);
        let mut builder = TarBuilder::new(file);

        for object in objects {
            let response = self
                .download_object_response(
                    &backend,
                    &payload.access_token,
                    &object.storage_object_id,
                )
                .await?;
            let size = response
                .content_length()
                .unwrap_or_else(|| object.object_size_bytes.max(0) as u64);
            let stream = response
                .bytes_stream()
                .map_err(|err| std::io::Error::other(err.to_string()));
            let reader = StreamReader::new(stream);

            let mut header = TarHeader::new_gnu();
            header.set_size(size);
            header.set_mode(0o644);
            header.set_mtime(parse_rfc3339_unix(&object.updated_at).unwrap_or_else(now_unix_u64));
            header.set_cksum();

            builder
                .append_data(
                    &mut header,
                    sanitize_archive_path(&object.object_key),
                    reader,
                )
                .await
                .map_err(|err| {
                    RegistryError::bad_gateway(format!(
                        "failed to build archive entry for '{}': {}",
                        object.object_key, err
                    ))
                })?;
        }

        let mut file = builder.into_inner().await.map_err(|err| {
            RegistryError::internal(format!("failed to finalize tar archive: {}", err))
        })?;
        file.seek(SeekFrom::Start(0)).await.map_err(|err| {
            RegistryError::internal(format!("failed to rewind tar archive: {}", err))
        })?;
        Ok(file)
    }

    pub async fn list_active_upload_sessions(
        &self,
        binding: &FragmentBindingRecord,
    ) -> Result<Vec<EngineUploadSession>, RegistryError> {
        let backend = self.resolve_backend(binding)?;
        let payload = self
            .authenticate_fragment(&backend, &binding.fragment, true, false, false, false)
            .await?;

        let mut sessions = Vec::new();
        let mut offset = 0usize;
        loop {
            let response = self
                .client_for_backend(&backend)
                .get(format!("{}/n1/upload/multipart", backend.engine_url))
                .bearer_auth(&payload.access_token)
                .query(&[("limit", 100usize), ("offset", offset)])
                .send()
                .await
                .map_err(|err| {
                    RegistryError::bad_gateway(format!(
                        "storage upload session listing failed for {}: {}",
                        backend.engine_url, err
                    ))
                })?;

            if !response.status().is_success() {
                let fallback = format!(
                    "storage upload session listing failed against {} backend",
                    match backend.deployment {
                        StorageDeployment::SingleNode => "single-node",
                        StorageDeployment::MultiNode => "multi-node",
                    }
                );
                let message = extract_engine_error_message(response, fallback).await;
                return Err(RegistryError::bad_gateway(message));
            }

            let payload = response
                .json::<EngineListUploadsResponse>()
                .await
                .map_err(|err| {
                    RegistryError::bad_gateway(format!(
                        "invalid storage upload session listing response: {}",
                        err
                    ))
                })?;
            let page_len = payload.sessions.len();
            sessions.extend(payload.sessions);
            offset = sessions.len();
            if page_len == 0 || offset >= payload.total {
                break;
            }
        }

        Ok(sessions)
    }

    pub async fn stream_shared_object(
        &self,
        binding: &FragmentBindingRecord,
        object_key: &str,
        disposition: ShareDisposition,
        request_headers: &axum::http::HeaderMap,
    ) -> Result<reqwest::Response, RegistryError> {
        let backend = self.resolve_backend(binding)?;
        let payload = self
            .authenticate_fragment(&backend, &binding.fragment, true, false, false, false)
            .await?;
        let client = self.client_for_backend(&backend);
        let mut request = if disposition.is_download() {
            client
                .get(format!("{}/n1/objects/download", backend.engine_url))
                .query(&[("object_key", object_key)])
        } else {
            client.get(format!(
                "{}/n1/objects/get/{}",
                backend.engine_url,
                encode_object_key_path(object_key)
            ))
        }
        .bearer_auth(&payload.access_token);

        for header_name in [
            axum::http::header::RANGE,
            axum::http::header::IF_RANGE,
            axum::http::header::IF_NONE_MATCH,
            axum::http::header::IF_MODIFIED_SINCE,
            axum::http::header::ACCEPT,
        ] {
            if let Some(value) = request_headers.get(&header_name) {
                if let Ok(value) = value.to_str() {
                    request = request.header(header_name.as_str(), value);
                }
            }
        }

        let response = request.send().await.map_err(|err| {
            RegistryError::bad_gateway(format!(
                "storage share request failed for {}: {}",
                backend.engine_url, err
            ))
        })?;

        if response.status().is_success() || response.status() == reqwest::StatusCode::NOT_MODIFIED
        {
            return Ok(response);
        }
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(RegistryError::not_found("shared object not found"));
        }

        let fallback = format!(
            "storage shared object failed against {} backend",
            match backend.deployment {
                StorageDeployment::SingleNode => "single-node",
                StorageDeployment::MultiNode => "multi-node",
            }
        );
        let message = extract_engine_error_message(response, fallback).await;
        Err(RegistryError::bad_gateway(message))
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
                "{}/n1/fragments/{}",
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
                "{}/n1/fragments/{}",
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

    pub async fn read_current_object_state(
        &self,
        binding: &FragmentBindingRecord,
        object_key: &str,
        include_versions: bool,
    ) -> Result<EngineObjectState, RegistryError> {
        let backend = self.resolve_backend(binding)?;
        let payload = self
            .authenticate_fragment(&backend, &binding.fragment, true, false, false, false)
            .await?;
        self.read_object_state_with_token(
            &backend,
            &payload.access_token,
            object_key,
            include_versions,
        )
        .await
    }

    pub async fn delete_object_version_and_read_state(
        &self,
        binding: &FragmentBindingRecord,
        object_key: &str,
        version_id: &str,
    ) -> Result<EngineObjectState, RegistryError> {
        let backend = self.resolve_backend(binding)?;
        let payload = self
            .authenticate_fragment(&backend, &binding.fragment, true, true, false, true)
            .await?;
        let encoded_key = encode_object_key_path(object_key);
        let encoded_version = encode_path_segment(version_id);
        let response = self
            .client_for_backend(&backend)
            .delete(format!(
                "{}/n1/objects/version/{}/{}",
                backend.engine_url, encoded_version, encoded_key
            ))
            .bearer_auth(&payload.access_token)
            .send()
            .await
            .map_err(|err| {
                RegistryError::bad_gateway(format!(
                    "storage version delete failed for {}: {}",
                    backend.engine_url, err
                ))
            })?;

        if !response.status().is_success() {
            let fallback = format!(
                "storage version delete failed against {} backend",
                match backend.deployment {
                    StorageDeployment::SingleNode => "single-node",
                    StorageDeployment::MultiNode => "multi-node",
                }
            );
            let message = extract_engine_error_message(response, fallback).await;
            return Err(RegistryError::bad_gateway(message));
        }

        self.read_object_state_with_token(&backend, &payload.access_token, object_key, true)
            .await
    }

    pub async fn permanently_delete_objects<I, S>(
        &self,
        binding: &FragmentBindingRecord,
        object_keys: I,
    ) -> Result<(), RegistryError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let backend = self.resolve_backend(binding)?;
        let payload = self
            .authenticate_fragment(&backend, &binding.fragment, false, false, false, true)
            .await?;

        let object_keys = object_keys
            .into_iter()
            .map(Into::into)
            .filter(|value| !value.trim().is_empty())
            .collect::<BTreeSet<_>>();
        if object_keys.is_empty() {
            return Ok(());
        }

        for object_key in object_keys {
            let encoded_key = encode_object_key_path(&object_key);
            let response = self
                .client_for_backend(&backend)
                .delete(format!(
                    "{}/n1/objects/permanent/{}",
                    backend.engine_url, encoded_key
                ))
                .bearer_auth(&payload.access_token)
                .send()
                .await
                .map_err(|err| {
                    RegistryError::bad_gateway(format!(
                        "storage permanent delete failed for {}: {}",
                        backend.engine_url, err
                    ))
                })?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                continue;
            }
            ensure_engine_success(response, &backend, "storage permanent delete failed").await?;
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

    async fn verify_fragment_ready(
        &self,
        backend: &ResolvedBackend,
        fragment: &str,
    ) -> Result<(), RegistryError> {
        let _ = self
            .authenticate_fragment(backend, fragment, true, false, false, false)
            .await?;
        Ok(())
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
            .post(format!("{}/n1/auth/fragment", backend.engine_url))
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

    async fn read_object_state_with_token(
        &self,
        backend: &ResolvedBackend,
        access_token: &str,
        object_key: &str,
        include_versions: bool,
    ) -> Result<EngineObjectState, RegistryError> {
        let current = self
            .fetch_object_metadata(backend, access_token, object_key)
            .await?;
        let versions = if include_versions {
            self.fetch_object_versions(backend, access_token, object_key)
                .await?
        } else {
            Vec::new()
        };
        Ok(EngineObjectState { current, versions })
    }

    async fn fetch_object_metadata(
        &self,
        backend: &ResolvedBackend,
        access_token: &str,
        object_key: &str,
    ) -> Result<Option<EngineObjectMetadata>, RegistryError> {
        let encoded_key = encode_object_key_path(object_key);
        let response = self
            .client_for_backend(backend)
            .get(format!(
                "{}/n1/objects/meta/{}",
                backend.engine_url, encoded_key
            ))
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|err| {
                RegistryError::bad_gateway(format!(
                    "storage object metadata request failed for {}: {}",
                    backend.engine_url, err
                ))
            })?;

        match response.status() {
            reqwest::StatusCode::OK => {
                let payload = response
                    .json::<EngineObjectSummary>()
                    .await
                    .map_err(|err| {
                        RegistryError::bad_gateway(format!(
                            "invalid storage metadata response: {}",
                            err
                        ))
                    })?;
                Ok(Some(payload.into()))
            }
            reqwest::StatusCode::NOT_FOUND => Ok(None),
            _ => {
                let fallback = format!(
                    "storage object metadata failed against {} backend",
                    match backend.deployment {
                        StorageDeployment::SingleNode => "single-node",
                        StorageDeployment::MultiNode => "multi-node",
                    }
                );
                let message = extract_engine_error_message(response, fallback).await;
                Err(RegistryError::bad_gateway(message))
            }
        }
    }

    async fn fetch_object_versions(
        &self,
        backend: &ResolvedBackend,
        access_token: &str,
        object_key: &str,
    ) -> Result<Vec<EngineObjectMetadata>, RegistryError> {
        let encoded_key = encode_object_key_path(object_key);
        let response = self
            .client_for_backend(backend)
            .get(format!(
                "{}/n1/objects/versions/{}",
                backend.engine_url, encoded_key
            ))
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|err| {
                RegistryError::bad_gateway(format!(
                    "storage object versions request failed for {}: {}",
                    backend.engine_url, err
                ))
            })?;

        match response.status() {
            reqwest::StatusCode::OK => {
                let payload = response
                    .json::<Vec<EngineObjectSummary>>()
                    .await
                    .map_err(|err| {
                        RegistryError::bad_gateway(format!(
                            "invalid storage versions response: {}",
                            err
                        ))
                    })?;
                Ok(payload.into_iter().map(Into::into).collect())
            }
            reqwest::StatusCode::NOT_FOUND => Ok(Vec::new()),
            _ => {
                let fallback = format!(
                    "storage object versions failed against {} backend",
                    match backend.deployment {
                        StorageDeployment::SingleNode => "single-node",
                        StorageDeployment::MultiNode => "multi-node",
                    }
                );
                let message = extract_engine_error_message(response, fallback).await;
                Err(RegistryError::bad_gateway(message))
            }
        }
    }

    async fn download_object_response(
        &self,
        backend: &ResolvedBackend,
        access_token: &str,
        object_key: &str,
    ) -> Result<reqwest::Response, RegistryError> {
        let response = self
            .client_for_backend(backend)
            .get(format!("{}/n1/objects/download", backend.engine_url))
            .query(&[("object_key", object_key)])
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|err| {
                RegistryError::bad_gateway(format!(
                    "storage object download request failed for {}: {}",
                    backend.engine_url, err
                ))
            })?;

        if response.status().is_success() {
            return Ok(response);
        }

        let fallback = format!(
            "storage object download failed against {} backend",
            match backend.deployment {
                StorageDeployment::SingleNode => "single-node",
                StorageDeployment::MultiNode => "multi-node",
            }
        );
        let message = extract_engine_error_message(response, fallback).await;
        Err(RegistryError::bad_gateway(message))
    }
}

const OBJECT_KEY_PATH_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'?')
    .add(b'[')
    .add(b']')
    .add(b'{')
    .add(b'}');

const PATH_SEGMENT_SET: &AsciiSet = &OBJECT_KEY_PATH_SET.add(b'/');

fn encode_object_key_path(object_key: &str) -> String {
    object_key
        .split('/')
        .map(|segment| utf8_percent_encode(segment, OBJECT_KEY_PATH_SET).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn encode_path_segment(value: &str) -> String {
    utf8_percent_encode(value, PATH_SEGMENT_SET).to_string()
}

fn sanitize_archive_path(path: &str) -> String {
    let trimmed = path.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        "export.bin".to_string()
    } else {
        trimmed.to_string()
    }
}

fn parse_rfc3339_unix(value: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|dt| u64::try_from(dt.timestamp()).ok())
}

fn now_unix_u64() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp()).unwrap_or(0)
}

impl From<EngineObjectSummary> for EngineObjectMetadata {
    fn from(value: EngineObjectSummary) -> Self {
        Self {
            version_id: value.version_id,
            object_size_bytes: i64::try_from(value.total_size_bytes).unwrap_or(i64::MAX),
            content_type: Some(value.content_type).filter(|entry| !entry.trim().is_empty()),
            created_at_unix: value.created_at,
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

async fn extract_engine_error_message(response: reqwest::Response, fallback: String) -> String {
    if let Ok(body) = response.json::<EngineErrorResponse>().await {
        if let Some(message) = body.error.or(body.message) {
            return message;
        }
    }
    fallback
}

async fn ensure_engine_success(
    response: reqwest::Response,
    backend: &ResolvedBackend,
    fallback_prefix: &str,
) -> Result<(), RegistryError> {
    if response.status().is_success() {
        return Ok(());
    }

    let fallback = format!(
        "{} against {} backend",
        fallback_prefix,
        match backend.deployment {
            StorageDeployment::SingleNode => "single-node",
            StorageDeployment::MultiNode => "multi-node",
        }
    );
    let message = extract_engine_error_message(response, fallback).await;
    Err(RegistryError::bad_gateway(message))
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
