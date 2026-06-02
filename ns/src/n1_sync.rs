use crate::core::disk_manager::{InitiateUploadParams, InitiateUploadResponse};
use crate::enums::app_error_enum::AppError;
use crate::metadata::{NasMetadataStore, NasShareRecord, SyncCursor};
use crate::settings::config::NsConfig;
use crate::structs::auth_structs::AuthBody;
use n1_native_client::N1NativeClient;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use serde::Serialize;
use sha2::{Digest as ShaDigest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;
use tokio_util::io::ReaderStream;
use tracing::{debug, info, warn};
use walkdir::WalkDir;

const SYNC_MULTIPART_THRESHOLD_BYTES: u64 = 16 * 1024 * 1024;
const SYNC_MULTIPART_PART_SIZE_BYTES: u64 = 16 * 1024 * 1024;

const URL_PATH_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b'%')
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}');

#[derive(Serialize)]
struct FragmentAuthRequest<'a> {
    fragment: &'a str,
    secret: &'a str,
    read: bool,
    write: bool,
    modify: bool,
    delete: bool,
    expires_in: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileSyncState {
    modified_at_unix_ms: i64,
    size_bytes: u64,
}

pub fn spawn_sync_worker(
    config: Arc<NsConfig>,
    metadata: NasMetadataStore,
) -> Option<JoinHandle<()>> {
    if !config.sync.enabled {
        return None;
    }

    Some(tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(config.sync.request_timeout_seconds))
            .danger_accept_invalid_certs(config.n1.accept_invalid_certs)
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                warn!("failed to build N1 sync client: {}", err);
                return;
            }
        };

        loop {
            let shares = match metadata.list_shares() {
                Ok(shares) => shares,
                Err(err) => {
                    warn!("failed to list dynamic NS shares for sync: {}", err);
                    tokio::time::sleep(Duration::from_secs(config.sync.scan_interval_seconds))
                        .await;
                    continue;
                }
            };
            for share in shares {
                if let Err(err) = sync_share(&client, &metadata, config.as_ref(), &share).await {
                    warn!("share sync failed for {}: {}", share.name, err);
                }
            }
            tokio::time::sleep(Duration::from_secs(config.sync.scan_interval_seconds)).await;
        }
    }))
}

pub async fn restore_share_file_from_n1(
    config: &NsConfig,
    share: &NasShareRecord,
    relative_key: &str,
) -> Result<u64, AppError> {
    let native_url = config.n1.native_url.as_deref().ok_or_else(|| {
        AppError::BadRequest("N1 native URL is required for NS restore".to_string())
    })?;
    let sync_profile = config
        .fragment_sync(share.fragment_hash.as_str())
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "share '{}' has no N1 sync profile for restore",
                share.name
            ))
        })?;
    let secret = sync_profile.secret.as_deref().ok_or_else(|| {
        AppError::BadRequest(format!(
            "share '{}' N1 restore profile is missing secret",
            share.name
        ))
    })?;
    let relative_path = safe_relative_path(relative_key)?;
    let object_key = if share.cold_n1_prefix.is_empty() {
        relative_key.trim_matches('/').to_string()
    } else {
        format!(
            "{}/{}",
            share.cold_n1_prefix.trim_matches('/'),
            relative_key.trim_matches('/')
        )
    };
    let destination = PathBuf::from(share.root_path.as_str()).join(relative_path);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|err| AppError::Io(err, format!("mkdir {}", parent.display())))?;
    }
    let tmp_path = destination.with_extension("ns-restore-partial");
    let mut native = N1NativeClient::connect(native_url)
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 native connect failed: {}", err)))?
        .with_request_prefix("ns-restore");
    native
        .auth_fragment(share.fragment_hash.as_str(), secret)
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 native auth failed: {}", err)))?;

    let mut output = std::fs::File::create(tmp_path.as_path())
        .map_err(|err| AppError::Io(err, format!("create {}", tmp_path.display())))?;
    let (response, bytes_written) = match native
        .stream_response(
            "object.download_stream",
            &serde_json::json!({ "object_key": object_key }),
            |chunk| {
                output.write_all(chunk)?;
                Ok(())
            },
        )
        .await
    {
        Ok(result) => result,
        Err(err) => {
            let _ = std::fs::remove_file(tmp_path.as_path());
            return Err(AppError::InternalServerError(format!(
                "N1 native restore failed: {}",
                err
            )));
        }
    };
    if !response.ok {
        let _ = std::fs::remove_file(tmp_path.as_path());
        let message = response
            .error
            .map(|err| err.message)
            .unwrap_or_else(|| "N1 native restore failed".to_string());
        return Err(AppError::InternalServerError(message));
    }
    output
        .sync_all()
        .map_err(|err| AppError::Io(err, format!("sync {}", tmp_path.display())))?;
    drop(output);
    fs::rename(tmp_path.as_path(), destination.as_path())
        .await
        .map_err(|err| AppError::Io(err, format!("rename {}", destination.display())))?;
    Ok(bytes_written)
}

async fn sync_share(
    client: &reqwest::Client,
    metadata: &NasMetadataStore,
    config: &NsConfig,
    share: &NasShareRecord,
) -> Result<(), AppError> {
    if !share.cold_sync_enabled {
        return Ok(());
    }
    let native_url = config.n1.native_url.as_deref();
    let base_url = share.n1_base_url.as_deref();
    if native_url.is_none() && base_url.is_none() {
        debug!(
            "cold sync skipped share='{}' because no N1 target URL is configured",
            share.name
        );
        return Ok(());
    }
    let token = if native_url.is_none() {
        ensure_fragment_token(client, metadata, config, share).await?
    } else {
        None
    };
    if native_url.is_none() && token.is_none() {
        debug!(
            "cold sync skipped share='{}' because no sync credential is configured",
            share.name
        );
        return Ok(());
    }

    let root_path = PathBuf::from(&share.root_path);
    let files = collect_files(root_path.as_path())?;
    let now_ms = now_unix_millis();
    for path in files {
        let relative = path
            .strip_prefix(root_path.as_path())
            .map_err(|err| AppError::InternalServerError(format!("strip_prefix: {}", err)))?;
        let relative_key = relative_to_object_key(relative);
        let Some(metadata_fs) = file_metadata_if_exists(path.as_path()).await? else {
            debug!(
                "cold sync skipped share='{}' path='{}' because it disappeared before stat",
                share.name, relative_key
            );
            continue;
        };
        let source_state = file_sync_state(&metadata_fs);
        if !metadata.sync_guard_allows_file(share.name.as_str(), relative_key.as_str())? {
            debug!(
                "cold sync deferred share='{}' path='{}' because SMB write guard is dirty or active",
                share.name, relative_key
            );
            continue;
        }
        if cursor_matches_state(
            metadata
                .sync_cursor(share.name.as_str(), relative_key.as_str())?
                .as_ref(),
            &source_state,
        ) {
            continue;
        }
        if !is_file_stable_for_sync(&source_state, now_ms, config.sync.min_stable_age_seconds) {
            debug!(
                "cold sync deferred share='{}' path='{}' because file is too recent (mtime_ms={} size={}B stable_after={}s)",
                share.name,
                relative_key,
                source_state.modified_at_unix_ms,
                source_state.size_bytes,
                config.sync.min_stable_age_seconds
            );
            continue;
        }

        let Some(snapshot_path) = create_sync_snapshot(
            config.storage.staging_root.as_path(),
            share.name.as_str(),
            relative_key.as_str(),
            path.as_path(),
            &source_state,
        )
        .await?
        else {
            debug!(
                "cold sync deferred share='{}' path='{}' because file changed while snapshotting",
                share.name, relative_key
            );
            continue;
        };

        let upload_result = if let Some(native_url) = native_url {
            upload_file_to_n1_native(
                native_url,
                config,
                share,
                relative_key.as_str(),
                path.as_path(),
                snapshot_path.as_path(),
                source_state.size_bytes,
            )
            .await
        } else {
            upload_file_to_n1(
                client,
                base_url.expect("base URL checked"),
                token.as_deref().expect("token checked"),
                share,
                relative_key.as_str(),
                path.as_path(),
                snapshot_path.as_path(),
                source_state.size_bytes,
            )
            .await
        };
        cleanup_sync_snapshot(snapshot_path.as_path()).await;
        upload_result?;

        let Some(current_metadata) = file_metadata_if_exists(path.as_path()).await? else {
            debug!(
                "cold sync left cursor pending share='{}' path='{}' because source disappeared during upload",
                share.name, relative_key
            );
            continue;
        };
        let current_state = file_sync_state(&current_metadata);
        if current_state != source_state {
            debug!(
                "cold sync left cursor pending share='{}' path='{}' because source changed during upload",
                share.name, relative_key
            );
            continue;
        }

        metadata.put_sync_cursor(
            share.name.as_str(),
            relative_key.as_str(),
            &SyncCursor {
                modified_at_unix_ms: source_state.modified_at_unix_ms,
                size_bytes: source_state.size_bytes,
            },
        )?;
        info!(
            "cold synced share='{}' path='{}' size={}B",
            share.name, relative_key, source_state.size_bytes
        );
    }

    Ok(())
}

async fn file_metadata_if_exists(path: &Path) -> Result<Option<std::fs::Metadata>, AppError> {
    match fs::metadata(path).await {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata)),
        Ok(_) => Ok(None),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(AppError::Io(err, format!("stat {}", path.display()))),
    }
}

async fn ensure_fragment_token(
    client: &reqwest::Client,
    metadata: &NasMetadataStore,
    config: &NsConfig,
    share: &NasShareRecord,
) -> Result<Option<String>, AppError> {
    if matches!(share.n1_access_expires_at_ms, Some(expires_at) if expires_at > now_unix_millis()) {
        if let Some(token) = share.n1_access_token.as_ref() {
            return Ok(Some(token.clone()));
        }
    }

    let Some(sync_profile) = config.fragment_sync(share.fragment_hash.as_str()) else {
        return Ok(None);
    };
    if !sync_profile.enabled {
        return Ok(None);
    }
    let Some(secret) = sync_profile.secret.as_deref() else {
        return Ok(None);
    };
    let base_url = sync_profile
        .n1_base_url
        .as_deref()
        .or(share.n1_base_url.as_deref())
        .ok_or_else(|| {
            AppError::InternalServerError(format!(
                "share '{}' is missing N1 base URL for cold sync",
                share.name
            ))
        })?;

    let auth = mint_fragment_token(client, base_url, share.fragment_hash.as_str(), secret).await?;
    let updated_share = metadata
        .update_share_auth_tokens(share.name.as_str(), &auth)?
        .ok_or_else(|| {
            AppError::InternalServerError(format!(
                "share '{}' disappeared while updating sync token",
                share.name
            ))
        })?;
    Ok(updated_share.n1_access_token)
}

async fn mint_fragment_token(
    client: &reqwest::Client,
    base_url: &str,
    fragment: &str,
    secret: &str,
) -> Result<AuthBody, AppError> {
    let url = format!("{}/n1/auth/fragment", base_url.trim_end_matches('/'));
    let response = client
        .post(url)
        .json(&FragmentAuthRequest {
            fragment,
            secret,
            read: true,
            write: true,
            modify: true,
            delete: true,
            expires_in: Some(60 * 60),
        })
        .send()
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 auth request failed: {}", err)))?;
    if !response.status().is_success() {
        return Err(AppError::InternalServerError(format!(
            "N1 auth failed with status {}",
            response.status()
        )));
    }
    response
        .json::<AuthBody>()
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 auth decode failed: {}", err)))
}

async fn upload_file_to_n1(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    share: &NasShareRecord,
    relative_key: &str,
    content_hint_path: &Path,
    upload_path: &Path,
    size_bytes: u64,
) -> Result<(), AppError> {
    let object_key = if share.cold_n1_prefix.is_empty() {
        relative_key.to_string()
    } else {
        format!(
            "{}/{}",
            share.cold_n1_prefix.trim_matches('/'),
            relative_key
        )
    };
    let content_type = mime_guess::from_path(content_hint_path)
        .first_or_octet_stream()
        .to_string();

    if size_bytes <= SYNC_MULTIPART_THRESHOLD_BYTES {
        match upload_file_to_n1_oneshot(
            client,
            base_url,
            token,
            share,
            relative_key,
            object_key.as_str(),
            upload_path,
            size_bytes,
            content_type.as_str(),
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(N1UploadAttemptError::PayloadTooLarge) => {
                debug!(
                    "N1 cold sync one-shot too large; falling back to multipart share='{}' path='{}' size={}B",
                    share.name, relative_key, size_bytes
                );
            }
            Err(N1UploadAttemptError::Failed(err)) => return Err(err),
        }
    }

    upload_file_to_n1_multipart(
        client,
        base_url,
        token,
        share,
        relative_key,
        object_key.as_str(),
        upload_path,
        size_bytes,
        content_type.as_str(),
    )
    .await
}

async fn upload_file_to_n1_native(
    native_url: &str,
    config: &NsConfig,
    share: &NasShareRecord,
    relative_key: &str,
    content_hint_path: &Path,
    upload_path: &Path,
    size_bytes: u64,
) -> Result<(), AppError> {
    let sync_profile = config
        .fragment_sync(share.fragment_hash.as_str())
        .ok_or_else(|| {
            AppError::InternalServerError(format!(
                "share '{}' has no native sync profile",
                share.name
            ))
        })?;
    let secret = sync_profile.secret.as_deref().ok_or_else(|| {
        AppError::InternalServerError(format!(
            "share '{}' native sync is missing secret",
            share.name
        ))
    })?;
    let object_key = if share.cold_n1_prefix.is_empty() {
        relative_key.to_string()
    } else {
        format!(
            "{}/{}",
            share.cold_n1_prefix.trim_matches('/'),
            relative_key
        )
    };
    let content_type = mime_guess::from_path(content_hint_path)
        .first_or_octet_stream()
        .to_string();
    let mut native = N1NativeClient::connect(native_url)
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 native connect failed: {}", err)))?
        .with_request_prefix("ns-cold-sync");
    native
        .auth_fragment(share.fragment_hash.as_str(), secret)
        .await
        .map_err(|err| AppError::InternalServerError(format!("N1 native auth failed: {}", err)))?;

    if size_bytes <= SYNC_MULTIPART_THRESHOLD_BYTES {
        let bytes = fs::read(upload_path)
            .await
            .map_err(|err| AppError::Io(err, format!("read {}", upload_path.display())))?;
        native
            .operation(
                "upload.one_shot",
                &serde_json::json!({
                    "object_key": object_key,
                    "content_type": content_type,
                    "total_size_bytes": size_bytes,
                }),
                Some(bytes),
            )
            .await
            .map_err(|err| {
                AppError::InternalServerError(format!("N1 native upload failed: {}", err))
            })?;
        return Ok(());
    }

    let expected_parts = sync_expected_parts(size_bytes, SYNC_MULTIPART_PART_SIZE_BYTES);
    let params = InitiateUploadParams {
        content_type: Some(content_type.clone()),
        total_size_bytes: size_bytes,
        expected_parts,
        part_size_bytes: Some(SYNC_MULTIPART_PART_SIZE_BYTES),
    };
    let init: InitiateUploadResponse = native
        .operation_json(
            "upload.multipart.init",
            &serde_json::json!({
                "object_key": object_key,
                "content_type": content_type,
                "total_size_bytes": size_bytes,
                "expected_parts": expected_parts,
                "part_size_bytes": SYNC_MULTIPART_PART_SIZE_BYTES,
            }),
            None,
        )
        .await
        .map_err(|err| {
            AppError::InternalServerError(format!("N1 native multipart init failed: {}", err))
        })?;

    let upload_result = upload_native_multipart_parts(
        &mut native,
        upload_path,
        &init,
        size_bytes,
        content_type.as_str(),
    )
    .await;
    if let Err(err) = upload_result {
        let _ = native
            .raw_json(
                "upload.multipart.cancel",
                &serde_json::json!({
                    "object_key": object_key,
                    "version_id": init.version_id,
                }),
                None,
            )
            .await;
        return Err(err);
    }

    native
        .operation(
            "upload.multipart.finalize",
            &serde_json::json!({
                "object_key": object_key,
                "version_id": init.version_id,
                "session_id": init.session_id,
                "content_type": params.content_type,
                "total_size_bytes": params.total_size_bytes,
                "expected_parts": params.expected_parts,
                "part_size_bytes": params.part_size_bytes,
            }),
            None,
        )
        .await
        .map_err(|err| {
            AppError::InternalServerError(format!("N1 native multipart finalize failed: {}", err))
        })?;
    Ok(())
}

async fn upload_native_multipart_parts(
    native: &mut N1NativeClient,
    upload_path: &Path,
    init: &InitiateUploadResponse,
    size_bytes: u64,
    content_type: &str,
) -> Result<(), AppError> {
    let mut file = fs::File::open(upload_path)
        .await
        .map_err(|err| AppError::Io(err, format!("open {}", upload_path.display())))?;

    for part_index in 0..sync_expected_parts(size_bytes, SYNC_MULTIPART_PART_SIZE_BYTES) {
        let part_size =
            sync_expected_part_size(size_bytes, SYNC_MULTIPART_PART_SIZE_BYTES, part_index)
                .ok_or_else(|| {
                    AppError::InternalServerError(format!(
                        "invalid native multipart part index {} for {}B file",
                        part_index, size_bytes
                    ))
                })?;
        let mut bytes = vec![0u8; part_size as usize];
        if part_size > 0 {
            file.read_exact(bytes.as_mut_slice())
                .await
                .map_err(|err| AppError::Io(err, format!("read {}", upload_path.display())))?;
        }
        native
            .operation(
                "upload.multipart.put_part",
                &serde_json::json!({
                    "version_id": init.version_id,
                    "part_index": part_index,
                    "content_type": content_type,
                    "content_length": part_size,
                }),
                Some(bytes),
            )
            .await
            .map_err(|err| {
                AppError::InternalServerError(format!(
                    "N1 native multipart part {} failed: {}",
                    part_index, err
                ))
            })?;
    }
    Ok(())
}

enum N1UploadAttemptError {
    PayloadTooLarge,
    Failed(AppError),
}

async fn upload_file_to_n1_oneshot(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    share: &NasShareRecord,
    relative_key: &str,
    object_key: &str,
    upload_path: &Path,
    size_bytes: u64,
    content_type: &str,
) -> Result<(), N1UploadAttemptError> {
    let encoded_key = encode_object_key(object_key);
    let url = format!(
        "{}/n1/upload/one-shot/{}",
        base_url.trim_end_matches('/'),
        encoded_key
    );
    let file = fs::File::open(upload_path).await.map_err(|err| {
        N1UploadAttemptError::Failed(AppError::Io(err, format!("open {}", upload_path.display())))
    })?;
    let stream = ReaderStream::new(file);
    let response = client
        .post(url)
        .header(AUTHORIZATION, format!("Bearer {}", token))
        .header(CONTENT_LENGTH, size_bytes.to_string())
        .header(CONTENT_TYPE, content_type)
        .body(reqwest::Body::wrap_stream(stream))
        .send()
        .await
        .map_err(|err| {
            N1UploadAttemptError::Failed(AppError::InternalServerError(format!(
                "N1 upload failed: {}",
                err
            )))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        debug!(
            "N1 cold sync upload rejected share='{}' path='{}' status={} body='{}'",
            share.name, relative_key, status, body
        );
        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            return Err(N1UploadAttemptError::PayloadTooLarge);
        }
        return Err(N1UploadAttemptError::Failed(AppError::InternalServerError(
            format!("N1 upload failed with status {}", status),
        )));
    }
    Ok(())
}

async fn upload_file_to_n1_multipart(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    share: &NasShareRecord,
    relative_key: &str,
    object_key: &str,
    upload_path: &Path,
    size_bytes: u64,
    content_type: &str,
) -> Result<(), AppError> {
    let expected_parts = sync_expected_parts(size_bytes, SYNC_MULTIPART_PART_SIZE_BYTES);
    let params = InitiateUploadParams {
        content_type: Some(content_type.to_string()),
        total_size_bytes: size_bytes,
        expected_parts,
        part_size_bytes: Some(SYNC_MULTIPART_PART_SIZE_BYTES),
    };
    let encoded_key = encode_object_key(object_key);
    let init_url = format!(
        "{}/n1/upload/multipart/init/{}",
        base_url.trim_end_matches('/'),
        encoded_key
    );
    let init_response = client
        .post(init_url)
        .header(AUTHORIZATION, format!("Bearer {}", token))
        .json(&params)
        .send()
        .await
        .map_err(|err| {
            AppError::InternalServerError(format!("N1 multipart init failed: {}", err))
        })?;
    if !init_response.status().is_success() {
        return Err(n1_rejected_response("N1 multipart init", init_response).await);
    }
    let init = init_response
        .json::<InitiateUploadResponse>()
        .await
        .map_err(|err| {
            AppError::InternalServerError(format!("N1 multipart init decode failed: {}", err))
        })?;

    let upload_result = upload_multipart_parts(
        client,
        base_url,
        token,
        upload_path,
        &init,
        size_bytes,
        content_type,
    )
    .await;
    if let Err(err) = upload_result {
        if let Err(cancel_err) = cancel_multipart_upload(
            client,
            base_url,
            token,
            object_key,
            init.version_id.as_str(),
        )
        .await
        {
            warn!(
                "failed to cancel rejected N1 multipart upload share='{}' path='{}' version='{}': {}",
                share.name, relative_key, init.version_id, cancel_err
            );
        }
        return Err(err);
    }

    let finalize_url = format!(
        "{}/n1/upload/multipart/{}/{}/finalize",
        base_url.trim_end_matches('/'),
        init.version_id,
        init.session_id
    );
    let finalize_response = client
        .post(finalize_url)
        .header(AUTHORIZATION, format!("Bearer {}", token))
        .query(&[("object_key", object_key)])
        .json(&params)
        .send()
        .await
        .map_err(|err| {
            AppError::InternalServerError(format!("N1 multipart finalize failed: {}", err))
        })?;
    if !finalize_response.status().is_success() {
        if let Err(cancel_err) = cancel_multipart_upload(
            client,
            base_url,
            token,
            object_key,
            init.version_id.as_str(),
        )
        .await
        {
            warn!(
                "failed to cancel unfinalized N1 multipart upload share='{}' path='{}' version='{}': {}",
                share.name, relative_key, init.version_id, cancel_err
            );
        }
        return Err(n1_rejected_response("N1 multipart finalize", finalize_response).await);
    }

    Ok(())
}

async fn upload_multipart_parts(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    upload_path: &Path,
    init: &InitiateUploadResponse,
    size_bytes: u64,
    content_type: &str,
) -> Result<(), AppError> {
    let mut file = fs::File::open(upload_path)
        .await
        .map_err(|err| AppError::Io(err, format!("open {}", upload_path.display())))?;

    for part_index in 0..sync_expected_parts(size_bytes, SYNC_MULTIPART_PART_SIZE_BYTES) {
        let part_size =
            sync_expected_part_size(size_bytes, SYNC_MULTIPART_PART_SIZE_BYTES, part_index)
                .ok_or_else(|| {
                    AppError::InternalServerError(format!(
                        "invalid multipart part index {} for {}B file",
                        part_index, size_bytes
                    ))
                })?;
        let mut bytes = vec![0u8; part_size as usize];
        if part_size > 0 {
            file.read_exact(bytes.as_mut_slice())
                .await
                .map_err(|err| AppError::Io(err, format!("read {}", upload_path.display())))?;
        }

        let part_url = format!(
            "{}/n1/upload/multipart/{}/{}",
            base_url.trim_end_matches('/'),
            init.version_id,
            part_index
        );
        let response = client
            .put(part_url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header(CONTENT_LENGTH, part_size.to_string())
            .header(CONTENT_TYPE, content_type)
            .body(bytes)
            .send()
            .await
            .map_err(|err| {
                AppError::InternalServerError(format!(
                    "N1 multipart part {} failed: {}",
                    part_index, err
                ))
            })?;
        if !response.status().is_success() {
            return Err(n1_rejected_response("N1 multipart part", response).await);
        }
    }

    Ok(())
}

async fn cancel_multipart_upload(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    object_key: &str,
    version_id: &str,
) -> Result<(), AppError> {
    let cancel_url = format!(
        "{}/n1/upload/multipart/{}",
        base_url.trim_end_matches('/'),
        version_id
    );
    let response = client
        .delete(cancel_url)
        .header(AUTHORIZATION, format!("Bearer {}", token))
        .query(&[("object_key", object_key)])
        .send()
        .await
        .map_err(|err| {
            AppError::InternalServerError(format!("N1 multipart cancel failed: {}", err))
        })?;
    if response.status().is_success() {
        return Ok(());
    }
    Err(n1_rejected_response("N1 multipart cancel", response).await)
}

async fn n1_rejected_response(context: &str, response: reqwest::Response) -> AppError {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    AppError::InternalServerError(if body.is_empty() {
        format!("{} failed with status {}", context, status)
    } else {
        format!("{} failed with status {}: {}", context, status, body)
    })
}

fn sync_expected_parts(total_size_bytes: u64, part_size_bytes: u64) -> u64 {
    if total_size_bytes == 0 || part_size_bytes == 0 {
        1
    } else {
        total_size_bytes.div_ceil(part_size_bytes).max(1)
    }
}

fn sync_expected_part_size(
    total_size_bytes: u64,
    part_size_bytes: u64,
    part_index: u64,
) -> Option<u64> {
    let expected_parts = sync_expected_parts(total_size_bytes, part_size_bytes);
    if part_index >= expected_parts {
        return None;
    }
    if total_size_bytes == 0 {
        return Some(0);
    }
    let start = part_index.checked_mul(part_size_bytes)?;
    Some(total_size_bytes.saturating_sub(start).min(part_size_bytes))
}

fn file_sync_state(metadata: &std::fs::Metadata) -> FileSyncState {
    let modified_at_unix_ms = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default();
    FileSyncState {
        modified_at_unix_ms,
        size_bytes: metadata.len(),
    }
}

fn cursor_matches_state(cursor: Option<&SyncCursor>, state: &FileSyncState) -> bool {
    matches!(
        cursor,
        Some(cursor)
            if cursor.modified_at_unix_ms == state.modified_at_unix_ms
                && cursor.size_bytes == state.size_bytes
    )
}

fn is_file_stable_for_sync(
    state: &FileSyncState,
    now_ms: u64,
    min_stable_age_seconds: u64,
) -> bool {
    if min_stable_age_seconds == 0 {
        return true;
    }
    let modified_at_ms = u64::try_from(state.modified_at_unix_ms).unwrap_or(0);
    now_ms.saturating_sub(modified_at_ms) >= min_stable_age_seconds.saturating_mul(1000)
}

fn sync_snapshot_path(
    staging_root: &Path,
    share_name: &str,
    relative_key: &str,
    state: &FileSyncState,
) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(share_name.as_bytes());
    hasher.update([0]);
    hasher.update(relative_key.as_bytes());
    hasher.update([0]);
    hasher.update(state.modified_at_unix_ms.to_le_bytes());
    hasher.update(state.size_bytes.to_le_bytes());
    let digest = hex::encode(hasher.finalize());
    staging_root
        .join("sync-snapshots")
        .join(share_name)
        .join(format!("{}.snapshot", digest))
}

async fn create_sync_snapshot(
    staging_root: &Path,
    share_name: &str,
    relative_key: &str,
    source_path: &Path,
    source_state: &FileSyncState,
) -> Result<Option<PathBuf>, AppError> {
    let snapshot_path = sync_snapshot_path(staging_root, share_name, relative_key, source_state);
    if let Some(parent) = snapshot_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|err| AppError::Io(err, format!("mkdir {}", parent.display())))?;
    }
    match fs::remove_file(&snapshot_path).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(AppError::Io(
                err,
                format!("remove {}", snapshot_path.display()),
            ));
        }
    }

    let copied_bytes = match fs::copy(source_path, &snapshot_path).await {
        Ok(value) => value,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(AppError::Io(err, format!("copy {}", source_path.display()))),
    };
    if copied_bytes != source_state.size_bytes {
        cleanup_sync_snapshot(snapshot_path.as_path()).await;
        return Ok(None);
    }

    let Some(current_metadata) = file_metadata_if_exists(source_path).await? else {
        cleanup_sync_snapshot(snapshot_path.as_path()).await;
        return Ok(None);
    };
    let current_state = file_sync_state(&current_metadata);
    if &current_state != source_state {
        cleanup_sync_snapshot(snapshot_path.as_path()).await;
        return Ok(None);
    }

    Ok(Some(snapshot_path))
}

async fn cleanup_sync_snapshot(snapshot_path: &Path) {
    match fs::remove_file(snapshot_path).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => warn!(
            "failed to remove NS sync snapshot '{}': {}",
            snapshot_path.display(),
            err
        ),
    }
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>, AppError> {
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }
    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        if entry.file_type().is_file() {
            out.push(entry.path().to_path_buf());
        }
    }
    out.sort();
    Ok(out)
}

fn relative_to_object_key(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            std::path::Component::Normal(value) => Some(value.to_string_lossy().to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn safe_relative_path(relative_key: &str) -> Result<PathBuf, AppError> {
    let trimmed = relative_key.trim_matches('/');
    if trimmed.is_empty() {
        return Err(AppError::BadRequest("restore path is required".to_string()));
    }
    let mut path = PathBuf::new();
    for segment in trimmed.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." || segment.contains('\\') {
            return Err(AppError::BadRequest(format!(
                "invalid restore path segment '{}'",
                segment
            )));
        }
        path.push(segment);
    }
    Ok(path)
}

fn encode_object_key(object_key: &str) -> String {
    object_key
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| utf8_percent_encode(segment, URL_PATH_ENCODE_SET).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::NasMetadataStore;
    use crate::settings::config::{
        FragmentSyncConfig, N1Config, NsAdminConfig, NsConfig, StorageConfig, SyncConfig,
    };
    use crate::smb::config::SmbConfig;
    use axum::extract::{Path as AxumPath, Query, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{delete, post, put};
    use axum::{Json, Router};
    use serde::Deserialize;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::net::TcpListener;

    #[derive(Clone)]
    struct MockAuthState {
        expected_fragment: String,
        expected_secret: String,
        hits: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct MockUploadState {
        expected_object_key: String,
        one_shot_hits: Arc<AtomicUsize>,
        init_hits: Arc<AtomicUsize>,
        part_hits: Arc<AtomicUsize>,
        finalize_hits: Arc<AtomicUsize>,
        cancel_hits: Arc<AtomicUsize>,
    }

    #[derive(Debug, Deserialize)]
    struct IncomingFragmentAuthRequest {
        fragment: String,
        secret: String,
        read: bool,
        write: bool,
        modify: bool,
        delete: bool,
        expires_in: Option<u64>,
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "litiaina-ns-sync-{}-{}-{}",
            test_name,
            std::process::id(),
            unique
        ))
    }

    fn fragment_hash(identifier: &str) -> String {
        let mut hasher = sha2::Sha256::new();
        use sha2::Digest as _;
        hasher.update(identifier.as_bytes());
        hex::encode(hasher.finalize())
    }

    fn test_config(root: &Path, base_url: &str) -> NsConfig {
        NsConfig {
            admin: NsAdminConfig {
                enabled: false,
                bind_addr: "127.0.0.1".to_string(),
                port: 0,
                admin_key: None,
            },
            smb: SmbConfig::default(),
            storage: StorageConfig {
                metadata_db_path: root.join("metadata.ndxdb"),
                staging_root: root.join("staging"),
                share_root: root.join("shares"),
                read_only_default: false,
            },
            n1: N1Config {
                base_url: Some(base_url.to_string()),
                native_url: None,
                accept_invalid_certs: true,
                index_roots: vec![root.join("n1-index")],
            },
            sync: SyncConfig {
                enabled: true,
                scan_interval_seconds: 300,
                request_timeout_seconds: 30,
                min_stable_age_seconds: 120,
                cold_n1_prefix: "nas".to_string(),
            },
            users: HashMap::new(),
            disks: HashMap::new(),
            shares: HashMap::new(),
            fragment_aliases: HashMap::new(),
            fragment_policies: HashMap::new(),
            fragment_syncs: HashMap::new(),
        }
    }

    async fn fragment_auth_handler(
        State(state): State<MockAuthState>,
        Json(request): Json<IncomingFragmentAuthRequest>,
    ) -> Json<AuthBody> {
        assert_eq!(request.fragment, state.expected_fragment);
        assert_eq!(request.secret, state.expected_secret);
        assert!(request.read);
        assert!(request.write);
        assert!(request.modify);
        assert!(request.delete);
        assert_eq!(request.expires_in, Some(60 * 60));
        state.hits.fetch_add(1, Ordering::SeqCst);

        Json(AuthBody {
            access_token: "token-123".to_string(),
            token_type: "Bearer".to_string(),
            expires_in: 3600,
            refresh_token: Some("refresh-123".to_string()),
            refresh_expires_in: Some(7200),
        })
    }

    async fn one_shot_payload_too_large_handler(
        State(state): State<MockUploadState>,
    ) -> (StatusCode, &'static str) {
        state.one_shot_hits.fetch_add(1, Ordering::SeqCst);
        (StatusCode::PAYLOAD_TOO_LARGE, "too large")
    }

    async fn multipart_init_handler(
        State(state): State<MockUploadState>,
        AxumPath(object_key): AxumPath<String>,
        Json(request): Json<InitiateUploadParams>,
    ) -> (StatusCode, Json<InitiateUploadResponse>) {
        assert_eq!(object_key, state.expected_object_key);
        assert_eq!(request.total_size_bytes, 15);
        assert_eq!(request.expected_parts, 1);
        assert_eq!(
            request.part_size_bytes,
            Some(SYNC_MULTIPART_PART_SIZE_BYTES)
        );
        state.init_hits.fetch_add(1, Ordering::SeqCst);
        (
            StatusCode::CREATED,
            Json(InitiateUploadResponse {
                session_id: "session-1".to_string(),
                version_id: "version-1".to_string(),
            }),
        )
    }

    async fn multipart_part_handler(
        State(state): State<MockUploadState>,
        AxumPath((version_id, part_index)): AxumPath<(String, u64)>,
        headers: HeaderMap,
        body: String,
    ) -> (StatusCode, &'static str) {
        assert_eq!(version_id, "version-1");
        assert_eq!(part_index, 0);
        assert_eq!(
            headers
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some("15")
        );
        assert_eq!(body.as_bytes(), b"hello multipart");
        state.part_hits.fetch_add(1, Ordering::SeqCst);
        (StatusCode::OK, "part ok")
    }

    async fn multipart_finalize_handler(
        State(state): State<MockUploadState>,
        AxumPath((version_id, session_id)): AxumPath<(String, String)>,
        Query(query): Query<HashMap<String, String>>,
        Json(request): Json<InitiateUploadParams>,
    ) -> (StatusCode, &'static str) {
        assert_eq!(version_id, "version-1");
        assert_eq!(session_id, "session-1");
        assert_eq!(
            query.get("object_key").map(String::as_str),
            Some(state.expected_object_key.as_str())
        );
        assert_eq!(request.total_size_bytes, 15);
        assert_eq!(request.expected_parts, 1);
        state.finalize_hits.fetch_add(1, Ordering::SeqCst);
        (StatusCode::OK, "finalized")
    }

    async fn multipart_cancel_handler(
        State(state): State<MockUploadState>,
    ) -> (StatusCode, &'static str) {
        state.cancel_hits.fetch_add(1, Ordering::SeqCst);
        (StatusCode::OK, "cancelled")
    }

    #[test]
    fn file_stability_guard_requires_minimum_quiet_time() {
        let state = FileSyncState {
            modified_at_unix_ms: 99_500,
            size_bytes: 64,
        };
        assert!(!is_file_stable_for_sync(&state, 100_000, 1));
        assert!(is_file_stable_for_sync(&state, 100_500, 1));
        assert!(is_file_stable_for_sync(&state, 100_000, 0));
    }

    #[test]
    fn cursor_match_requires_same_mtime_and_size() {
        let state = FileSyncState {
            modified_at_unix_ms: 123_456,
            size_bytes: 512,
        };
        let same = SyncCursor {
            modified_at_unix_ms: 123_456,
            size_bytes: 512,
        };
        let different_size = SyncCursor {
            modified_at_unix_ms: 123_456,
            size_bytes: 1024,
        };
        assert!(cursor_matches_state(Some(&same), &state));
        assert!(!cursor_matches_state(Some(&different_size), &state));
        assert!(!cursor_matches_state(None, &state));
    }

    #[test]
    fn restore_relative_path_rejects_traversal() {
        assert_eq!(
            safe_relative_path("docs/report.pdf").expect("valid path"),
            PathBuf::from("docs").join("report.pdf")
        );
        assert!(safe_relative_path("../secret.txt").is_err());
        assert!(safe_relative_path("docs/../secret.txt").is_err());
        assert!(safe_relative_path("").is_err());
    }

    #[tokio::test]
    async fn ensure_fragment_token_uses_stable_fragment_auth_and_persists_cache() {
        let root = temp_root("ensure-fragment-token");
        let fragment_hash = fragment_hash("my-fragment");
        let hits = Arc::new(AtomicUsize::new(0));

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind auth mock");
        let base_url = format!("http://{}", listener.local_addr().expect("listener addr"));
        let app = Router::new()
            .route("/n1/auth/fragment", post(fragment_auth_handler))
            .with_state(MockAuthState {
                expected_fragment: fragment_hash.clone(),
                expected_secret: "password".to_string(),
                hits: hits.clone(),
            });
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve auth mock");
        });

        let mut config = test_config(root.as_path(), base_url.as_str());
        config.fragment_syncs.insert(
            fragment_hash.clone(),
            FragmentSyncConfig {
                enabled: true,
                secret: Some("password".to_string()),
                n1_base_url: Some(base_url.clone()),
                cold_n1_prefix: "nas/backup".to_string(),
            },
        );

        let metadata = NasMetadataStore::open(config.storage.metadata_db_path.as_path())
            .expect("metadata opens");
        metadata.seed_from_config(&config).expect("seed config");
        let share = metadata
            .upsert_authenticated_fragment("my-fragment", fragment_hash.as_str(), &config)
            .expect("upsert authenticated fragment");
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("client builds");

        let token = ensure_fragment_token(&client, &metadata, &config, &share)
            .await
            .expect("ensure token")
            .expect("token present");
        assert_eq!(token, "token-123");
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        let persisted = metadata
            .share(share.name.as_str())
            .expect("metadata lookup")
            .expect("persisted share");
        assert_eq!(persisted.n1_access_token.as_deref(), Some("token-123"));
        assert_eq!(persisted.n1_refresh_token.as_deref(), Some("refresh-123"));
        assert!(persisted.n1_access_expires_at_ms.is_some());

        let cached_token = ensure_fragment_token(&client, &metadata, &config, &persisted)
            .await
            .expect("ensure cached token")
            .expect("cached token present");
        assert_eq!(cached_token, "token-123");
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        server.abort();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn upload_file_to_n1_falls_back_to_multipart_when_one_shot_is_too_large() {
        let root = temp_root("multipart-fallback");
        tokio::fs::create_dir_all(root.as_path())
            .await
            .expect("mkdir temp root");
        let file_path = root.join("sample.bin");
        tokio::fs::write(file_path.as_path(), b"hello multipart")
            .await
            .expect("write sample file");

        let state = MockUploadState {
            expected_object_key: "nas/main/folder/sample.bin".to_string(),
            one_shot_hits: Arc::new(AtomicUsize::new(0)),
            init_hits: Arc::new(AtomicUsize::new(0)),
            part_hits: Arc::new(AtomicUsize::new(0)),
            finalize_hits: Arc::new(AtomicUsize::new(0)),
            cancel_hits: Arc::new(AtomicUsize::new(0)),
        };
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upload mock");
        let base_url = format!("http://{}", listener.local_addr().expect("listener addr"));
        let app = Router::new()
            .route(
                "/n1/upload/one-shot/{*object_key}",
                post(one_shot_payload_too_large_handler),
            )
            .route(
                "/n1/upload/multipart/init/{*object_key}",
                post(multipart_init_handler),
            )
            .route(
                "/n1/upload/multipart/{version_id}/{part_index}",
                put(multipart_part_handler),
            )
            .route(
                "/n1/upload/multipart/{version_id}/{session_id}/finalize",
                post(multipart_finalize_handler),
            )
            .route(
                "/n1/upload/multipart/{version_id}",
                delete(multipart_cancel_handler),
            )
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve upload mock");
        });

        let share = NasShareRecord {
            name: "share-main".to_string(),
            fragment_hash: "fragment-main".to_string(),
            root_path: root.to_string_lossy().to_string(),
            read_only: false,
            n1_base_url: Some(base_url.clone()),
            cold_sync_enabled: true,
            cold_n1_prefix: "nas/main".to_string(),
            n1_access_token: None,
            n1_access_expires_at_ms: None,
            n1_refresh_token: None,
            n1_refresh_expires_at_ms: None,
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("client builds");

        upload_file_to_n1(
            &client,
            base_url.as_str(),
            "token-123",
            &share,
            "folder/sample.bin",
            file_path.as_path(),
            file_path.as_path(),
            15,
        )
        .await
        .expect("multipart fallback upload");

        assert_eq!(state.one_shot_hits.load(Ordering::SeqCst), 1);
        assert_eq!(state.init_hits.load(Ordering::SeqCst), 1);
        assert_eq!(state.part_hits.load(Ordering::SeqCst), 1);
        assert_eq!(state.finalize_hits.load(Ordering::SeqCst), 1);
        assert_eq!(state.cancel_hits.load(Ordering::SeqCst), 0);

        server.abort();
        let _ = std::fs::remove_dir_all(root);
    }
}
