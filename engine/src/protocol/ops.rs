use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::AsyncRead;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::core::disk_manager::{
    ChunkQuery, InitiateUploadParams, ObjectMetadata, UploadPartQuery,
};
use crate::core::fragment_manager::{
    DeleteFragmentRequest, UpdateFragmentRequest, create_fragment, delete_fragment, update_fragment,
};
use crate::core::garbage_collector::{
    get_gc_status, run_garbage_collector, stop_garbage_collector,
};
use crate::core::index;
use crate::core::metadata_parser::get_version_metadata;
use crate::core::monitoring::system_health_snapshot;
use crate::core::operation_gate::ensure_user_mutations_allowed;
use crate::core::redistribution::{
    get_redist_status, stop_redistribution_job_api, trigger_redistribution_job_api,
};
use crate::core::repair_manager::{
    get_repair_status, inspect_object_status, parse_check_mode, repair_object, stop_repair_job_api,
    trigger_repair_job_api,
};
use crate::core::retrieve::{get_object_metadata_direct, list_fragments};
use crate::core::storage_capacity_parser::{get_capacity_usage, get_fragment_usage};
use crate::core::sync::CreateFragmentRequest;
use crate::core::upload::{
    FinalizeUploadResult, cancel_upload_session_direct, finalize_object_upload_direct,
    get_upload_status, initiate_object_upload_direct, list_active_uploads,
    object_upload_part_direct, object_upload_part_reader_direct, object_upload_part_stream_direct,
    upload_object_one_shot_direct, upload_object_one_shot_reader_direct,
    upload_object_one_shot_stream_direct,
};
use crate::enums::app_error_enum::AppError;
use crate::middleware::auth_middleware::AuthError;
use crate::namespace::models::NamespaceEntryKind;
use crate::namespace::runtime::{global_namespace_runtime, logical_not_found};
use crate::posix::service as posix_service;
use crate::protocol::auth::NativeSession;
use crate::protocol::types::NativeOpResponse;
use crate::routes::storage_routes::{
    AppState, commit_logical_upload_from_meta, emit_logical_file_uploaded,
    find_reusable_pending_upload, resolve_logical_write_key,
};
use crate::structs::auth_structs::Claims;
use crate::validation::fragment_validation::does_fragment_exist;
use crate::websocket::ws_structs::{NamespaceChangeEvent, StorageEventPayload};

pub struct NativeExecutionError(pub NativeOpResponse, pub Vec<u8>);

const NATIVE_STREAM_DOWNLOAD_CHUNK_BYTES: usize = 4 * 1024 * 1024;

pub struct NativeDownloadPlan {
    pub payload: Value,
    pub body_len: u64,
    metadata: ObjectMetadata,
    offset: u64,
}

fn native_operation_requires_write_safe_topology(op: &str) -> bool {
    matches!(
        op,
        "namespace.mkdir"
            | "posix.mkdir"
            | "namespace.rename"
            | "posix.rename"
            | "posix.move"
            | "namespace.trash"
            | "posix.trash"
            | "namespace.recover"
            | "posix.recover"
            | "trash.recover_all"
            | "object.delete"
            | "object.recover"
            | "object.rename"
            | "object.bulk_delete"
            | "object.bulk.delete"
            | "object.bulk_recover"
            | "object.bulk.recover"
            | "upload.one_shot"
            | "object.upload.one_shot"
            | "multipart.init"
            | "upload.multipart.init"
            | "multipart.put_part"
            | "upload.multipart.put_part"
            | "multipart.finalize"
            | "upload.multipart.finalize"
            | "multipart.cancel"
            | "upload.multipart.cancel"
            | "maintenance.gc.start"
            | "maintenance.redistribution.start"
            | "fragment.create"
            | "fragment.update"
            | "fragment.delete"
    )
}

pub async fn execute_native_op(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    _body: Option<Vec<u8>>,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, NativeExecutionError> {
    let result = execute_native_op_inner(state, session, op, payload, _body).await;
    match result {
        Ok(value) => Ok(value),
        Err(err) => Err(native_app_error(err)),
    }
}

pub async fn execute_native_streaming_op(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    body: Body,
    body_len: u64,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, NativeExecutionError> {
    let body_stream = body.into_data_stream().map(|chunk_result| {
        chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "native stream read".into(),
            )
        })
    });
    execute_native_streaming_source(state, session, op, payload, body_stream, body_len).await
}

pub async fn execute_native_streaming_source<S>(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    body_stream: S,
    body_len: u64,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, NativeExecutionError>
where
    S: Stream<Item = Result<Bytes, AppError>> + Unpin,
{
    let result =
        execute_native_streaming_op_inner(state, session, op, payload, body_stream, body_len).await;
    match result {
        Ok(value) => Ok(value),
        Err(err) => Err(native_app_error(err)),
    }
}

pub async fn execute_native_streaming_reader<R>(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    reader: &mut R,
    body_len: u64,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, NativeExecutionError>
where
    R: AsyncRead + Unpin + Send,
{
    let result =
        execute_native_streaming_reader_inner(state, session, op, payload, reader, body_len).await;
    match result {
        Ok(value) => Ok(value),
        Err(err) => Err(native_app_error(err)),
    }
}

async fn execute_native_streaming_reader_inner<R>(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    reader: &mut R,
    body_len: u64,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, AppError>
where
    R: AsyncRead + Unpin + Send,
{
    let claims = require_claims(session)?;
    let response = match op {
        "upload.one_shot" | "object.upload.one_shot" => {
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let logical = required_string(&payload, "object_key")?;
            let physical = resolve_logical_write_key(&fragment, &logical).await?;
            let _upload_activity =
                crate::core::upload::UploadActivityGuard::begin(&fragment, &physical)?;
            let mut metadata = upload_object_one_shot_reader_direct(
                claims,
                fragment.clone(),
                physical.clone(),
                upload_part_query_from_payload(&payload)?,
                string_field(&payload, "content_type"),
                reader,
                body_len,
                true,
            )
            .await?;
            let committed_physical =
                commit_logical_upload_from_meta(&fragment, &logical, &physical, &metadata).await?;
            metadata.object_key = logical;
            emit_logical_file_uploaded(
                &state,
                &fragment,
                &metadata.object_key,
                &committed_physical,
                &metadata,
            );
            metadata.strip_internal_layout();
            Some((op_response(201), native_body_bytes(&metadata)))
        }
        "multipart.put_part" | "upload.multipart.put_part" => {
            require_write(&claims)?;
            let version_id = required_string(&payload, "version_id")?;
            let part_index = u64_field(&payload, "part_index")
                .ok_or_else(|| AppError::BadRequest("part_index is required".into()))?;
            object_upload_part_reader_direct(
                claims.clone(),
                claims.fragment.clone(),
                version_id.clone(),
                part_index,
                upload_part_query_from_payload(&payload)?,
                reader,
                body_len,
            )
            .await?;
            Some(native_body(json!({
                "version_id": version_id,
                "part_index": part_index,
                "uploaded": true,
            })))
        }
        _ => None,
    };

    if let Some(response) = response {
        return Ok(Some(response));
    }
    execute_native_op_inner(state, session, op, payload, None).await
}

async fn execute_native_streaming_op_inner<S>(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    body_stream: S,
    body_len: u64,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>> + Unpin,
{
    let claims = require_claims(session)?;
    let response = match op {
        "upload.one_shot" | "object.upload.one_shot" => {
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let logical = required_string(&payload, "object_key")?;
            let physical = resolve_logical_write_key(&fragment, &logical).await?;
            let _upload_activity =
                crate::core::upload::UploadActivityGuard::begin(&fragment, &physical)?;
            let mut metadata = upload_object_one_shot_stream_direct(
                claims,
                fragment.clone(),
                physical.clone(),
                upload_part_query_from_payload(&payload)?,
                string_field(&payload, "content_type"),
                body_stream,
                body_len,
                true,
            )
            .await?;
            let committed_physical =
                commit_logical_upload_from_meta(&fragment, &logical, &physical, &metadata).await?;
            metadata.object_key = logical;
            emit_logical_file_uploaded(
                &state,
                &fragment,
                &metadata.object_key,
                &committed_physical,
                &metadata,
            );
            metadata.strip_internal_layout();
            Some((op_response(201), native_body_bytes(&metadata)))
        }
        "multipart.put_part" | "upload.multipart.put_part" => {
            require_write(&claims)?;
            let version_id = required_string(&payload, "version_id")?;
            let part_index = u64_field(&payload, "part_index")
                .ok_or_else(|| AppError::BadRequest("part_index is required".into()))?;
            object_upload_part_stream_direct(
                claims.clone(),
                claims.fragment.clone(),
                version_id.clone(),
                part_index,
                upload_part_query_from_payload(&payload)?,
                body_stream,
                body_len,
            )
            .await?;
            Some(native_body(json!({
                "version_id": version_id,
                "part_index": part_index,
                "uploaded": true,
            })))
        }
        _ => None,
    };
    Ok(response)
}

async fn execute_native_op_inner(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    _body: Option<Vec<u8>>,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, AppError> {
    if native_operation_requires_write_safe_topology(op) {
        ensure_user_mutations_allowed()?;
    }
    let claims = match op {
        "fragment.create" | "fragment.hash.public" | "fragment.check" => None,
        _ => Some(require_claims(session)?),
    };

    let response = match op {
        "namespace.list" | "posix.list" => {
            let claims = claims_required(claims)?;
            native_body(
                posix_service::list_dir(
                    claims,
                    string_field(&payload, "path").unwrap_or_default(),
                    usize_field(&payload, "limit").unwrap_or(200),
                    string_field(&payload, "continuation_token"),
                    bool_field(&payload, "trash").unwrap_or(false),
                )
                .await?,
            )
        }
        "namespace.stat" | "posix.stat" => {
            let claims = claims_required(claims)?;
            native_body(
                posix_service::stat_path(
                    claims,
                    required_string(&payload, "path")?,
                    bool_field(&payload, "trash").unwrap_or(false),
                )
                .await?,
            )
        }
        "namespace.mkdir" | "posix.mkdir" => {
            let claims = claims_required(claims)?;
            native_body(
                posix_service::mkdir_path(state, claims, required_string(&payload, "path")?)
                    .await?,
            )
        }
        "namespace.rename" | "posix.rename" | "posix.move" => {
            let claims = claims_required(claims)?;
            native_body(
                posix_service::rename_path(
                    state,
                    claims,
                    required_string(&payload, "source_path")?,
                    required_string(&payload, "destination_path")?,
                )
                .await?,
            )
        }
        "namespace.trash" | "posix.trash" => {
            let claims = claims_required(claims)?;
            native_body(
                posix_service::trash_path(state, claims, required_string(&payload, "path")?)
                    .await?,
            )
        }
        "namespace.recover" | "posix.recover" => {
            let claims = claims_required(claims)?;
            native_body(
                posix_service::recover_path(state, claims, required_string(&payload, "path")?)
                    .await?,
            )
        }
        "object.list" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            native_body(
                global_namespace_runtime()
                    .list_objects_page(&claims.fragment, &params_from_payload(&payload), false)
                    .await?,
            )
        }
        "trash.list" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            native_body(
                global_namespace_runtime()
                    .list_objects_page(&claims.fragment, &params_from_payload(&payload), true)
                    .await?,
            )
        }
        "trash.recover_all" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let summary = global_namespace_runtime()
                .recover_all(&claims.fragment)
                .await?;
            state.emit(
                &claims.fragment,
                StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
                    &claims.fragment,
                    summary.clone(),
                )),
            );
            native_body(json!({
                "operation": summary.operation,
                "affected_paths": summary.affected_paths,
                "affected_objects": summary.affected_objects,
            }))
        }
        "object.status" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let object_key = required_string(&payload, "object_key")?;
            let physical = resolve_logical_read_key(&claims.fragment, &object_key).await?;
            let metadata = get_object_metadata_direct(&claims.fragment, &physical).await?;
            let mut status = inspect_object_status(&metadata, check_mode_payload(&payload)).await?;
            status.object_key = object_key.trim_start_matches('/').to_string();
            native_body(status)
        }
        "object.repair" => {
            let claims = claims_required(claims)?;
            require_write_modify(&claims)?;
            let object_key = required_string(&payload, "object_key")?;
            let physical = resolve_logical_read_key(&claims.fragment, &object_key).await?;
            let metadata = get_object_metadata_direct(&claims.fragment, &physical).await?;
            let mut status = repair_object(&metadata, check_mode_payload(&payload)).await?;
            status.object_key = object_key.trim_start_matches('/').to_string();
            native_body(status)
        }
        "object.delete" => {
            let claims = claims_required(claims)?;
            let path = required_string(&payload, "object_key")?;
            native_body(posix_service::trash_path(state, claims, path).await?)
        }
        "object.recover" => {
            let claims = claims_required(claims)?;
            let path = required_string(&payload, "object_key")?;
            native_body(posix_service::recover_path(state, claims, path).await?)
        }
        "object.rename" => {
            let claims = claims_required(claims)?;
            require_write_modify(&claims)?;
            let source = string_field(&payload, "source_key")
                .or_else(|| string_field(&payload, "source_path"))
                .ok_or_else(|| AppError::BadRequest("source_key is required".into()))?;
            let destination = string_field(&payload, "destination_key")
                .or_else(|| string_field(&payload, "destination_path"))
                .ok_or_else(|| AppError::BadRequest("destination_key is required".into()))?;
            native_body(posix_service::rename_path(state, claims, source, destination).await?)
        }
        "object.meta" | "object.get" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let logical = required_string(&payload, "object_key")?;
            let fragment = claims.fragment.clone();
            let physical = resolve_logical_read_key(&fragment, &logical).await?;
            let mut metadata = if let Some(version_id) = string_field(&payload, "version_id") {
                get_version_metadata(&fragment, &physical, &version_id, &fragment).await?
            } else {
                get_object_metadata_direct(&fragment, &physical).await?
            };
            metadata.object_key = logical.trim_start_matches('/').to_string();
            metadata.strip_internal_layout();
            native_body(metadata)
        }
        "object.versions" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let logical = required_string(&payload, "object_key")?;
            let fragment = claims.fragment.clone();
            let physical = resolve_logical_read_key(&fragment, &logical).await?;
            let mut versions =
                index::list_live_object_versions_metadata(&fragment, &physical).await?;
            versions.sort_by(|a, b| b.version_id.cmp(&a.version_id));
            for metadata in &mut versions {
                metadata.object_key = logical.trim_start_matches('/').to_string();
                metadata.strip_internal_layout();
            }
            native_body(versions)
        }
        "object.bulk_delete" | "object.bulk.delete" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            native_body(
                native_bulk_namespace_operation(
                    &state,
                    &claims.fragment,
                    string_array_field(&payload, "object_keys")?,
                    "bulk_delete",
                    BulkNamespaceMode::Trash,
                )
                .await?,
            )
        }
        "object.bulk_recover" | "object.bulk.recover" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            native_body(
                native_bulk_namespace_operation(
                    &state,
                    &claims.fragment,
                    string_array_field(&payload, "object_keys")?,
                    "bulk_recover",
                    BulkNamespaceMode::Recover,
                )
                .await?,
            )
        }
        "upload.one_shot" | "object.upload.one_shot" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let body =
                _body.ok_or_else(|| AppError::BadRequest("upload body is required".into()))?;
            let fragment = claims.fragment.clone();
            let logical = required_string(&payload, "object_key")?;
            let physical = resolve_logical_write_key(&fragment, &logical).await?;
            let _upload_activity =
                crate::core::upload::UploadActivityGuard::begin(&fragment, &physical)?;
            let mut metadata = upload_object_one_shot_direct(
                claims,
                fragment.clone(),
                physical.clone(),
                upload_part_query_from_payload(&payload)?,
                string_field(&payload, "content_type"),
                body,
                true,
            )
            .await?;
            let committed_physical =
                commit_logical_upload_from_meta(&fragment, &logical, &physical, &metadata).await?;
            metadata.object_key = logical;
            emit_logical_file_uploaded(
                &state,
                &fragment,
                &metadata.object_key,
                &committed_physical,
                &metadata,
            );
            metadata.strip_internal_layout();
            (op_response(201), native_body_bytes(&metadata))
        }
        "multipart.init" | "upload.multipart.init" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let logical = required_string(&payload, "object_key")?;
            let params: InitiateUploadParams = typed_payload(&payload)?;
            if let Some(existing) =
                find_reusable_pending_upload(&fragment, &logical, &params).await?
            {
                (op_response(201), native_body_bytes(&existing))
            } else {
                let physical = resolve_logical_write_key(&fragment, &logical).await?;
                let response = initiate_object_upload_direct(
                    claims,
                    fragment.clone(),
                    physical.clone(),
                    params,
                )
                .await?;
                global_namespace_runtime()
                    .bind_pending_upload(
                        &fragment,
                        &logical,
                        &physical,
                        &response.version_id,
                        &response.session_id,
                    )
                    .await?;
                (op_response(201), native_body_bytes(&response))
            }
        }
        "multipart.put_part" | "upload.multipart.put_part" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let body = _body.ok_or_else(|| AppError::BadRequest("part body is required".into()))?;
            let version_id = required_string(&payload, "version_id")?;
            let part_index = u64_field(&payload, "part_index")
                .ok_or_else(|| AppError::BadRequest("part_index is required".into()))?;
            object_upload_part_direct(
                claims.clone(),
                claims.fragment.clone(),
                version_id.clone(),
                part_index,
                upload_part_query_from_payload(&payload)?,
                body,
            )
            .await?;
            native_body(json!({
                "version_id": version_id,
                "part_index": part_index,
                "uploaded": true,
            }))
        }
        "multipart.finalize" | "upload.multipart.finalize" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let version_id = required_string(&payload, "version_id")?;
            let session_id = required_string(&payload, "session_id")?;
            let logical = required_string(&payload, "object_key")?;
            let params: InitiateUploadParams = typed_payload(&payload)?;
            let binding = global_namespace_runtime()
                .pending_upload(&fragment, &version_id)
                .await?;
            let physical = binding
                .as_ref()
                .map(|value| value.physical_object_id.clone())
                .unwrap_or_else(|| logical.clone());
            let _upload_activity =
                crate::core::upload::UploadActivityGuard::begin(&fragment, &physical)?;
            match finalize_object_upload_direct(
                claims,
                fragment.clone(),
                version_id,
                session_id,
                physical.clone(),
                params,
            )
            .await?
            {
                FinalizeUploadResult::Complete(mut metadata) => {
                    let logical_path = binding
                        .as_ref()
                        .map(|value| value.logical_path.clone())
                        .unwrap_or(logical);
                    let committed_physical = commit_logical_upload_from_meta(
                        &fragment,
                        &logical_path,
                        &physical,
                        &metadata,
                    )
                    .await?;
                    crate::core::index::remove_session(
                        &fragment,
                        &metadata.version_id,
                        "finalized",
                    )
                    .await?;
                    metadata.object_key = logical_path;
                    emit_logical_file_uploaded(
                        &state,
                        &fragment,
                        &metadata.object_key,
                        &committed_physical,
                        &metadata,
                    );
                    metadata.strip_internal_layout();
                    (op_response(200), native_body_bytes(&metadata))
                }
                FinalizeUploadResult::Incomplete(incomplete) => {
                    (op_response(409), native_body_bytes(&incomplete))
                }
            }
        }
        "multipart.cancel" | "upload.multipart.cancel" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let version_id = required_string(&payload, "version_id")?;
            let binding = global_namespace_runtime()
                .pending_upload(&fragment, &version_id)
                .await?;
            let object_key = binding
                .as_ref()
                .map(|value| value.physical_object_id.clone())
                .or_else(|| string_field(&payload, "object_key"))
                .unwrap_or_else(|| version_id.clone());
            cancel_upload_session_direct(claims, fragment.clone(), version_id.clone(), object_key)
                .await?;
            let _ = global_namespace_runtime()
                .clear_pending_upload(&fragment, &version_id)
                .await;
            (op_response(204), Vec::new())
        }
        "object.download_chunk" | "download.chunk" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let fragment = claims.fragment.clone();
            let logical = required_string(&payload, "object_key")?;
            let physical = resolve_logical_read_key(&fragment, &logical).await?;
            let metadata = if let Some(version_id) = string_field(&payload, "version_id") {
                get_version_metadata(&fragment, &physical, &version_id, &fragment).await?
            } else {
                get_object_metadata_direct(&fragment, &physical).await?
            };
            let offset = u64_field(&payload, "offset").unwrap_or(0);
            let chunk_size = usize_field(&payload, "chunk_size").unwrap_or(8 * 1024 * 1024);
            let (bytes, _) = crate::core::download::download_object_chunk_from_metadata(
                metadata,
                &ChunkQuery {
                    object_key: physical,
                    offset,
                    chunk_size,
                },
            )
            .await?;
            let status = if bytes.is_empty() { 204 } else { 200 };
            (op_response(status), bytes)
        }
        "object.download" | "object.stream" | "object.view" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let fragment = claims.fragment.clone();
            let logical = required_string(&payload, "object_key")?;
            let physical = resolve_logical_read_key(&fragment, &logical).await?;
            let metadata = if let Some(version_id) = string_field(&payload, "version_id") {
                get_version_metadata(&fragment, &physical, &version_id, &fragment).await?
            } else {
                get_object_metadata_direct(&fragment, &physical).await?
            };
            native_download_metadata(metadata, physical).await?
        }
        "maintenance.health" => {
            let claims = claims_required(claims)?;
            native_body(system_health_snapshot(state, claims, params_from_payload(&payload)).await?)
        }
        "maintenance.gc.status" => native_body(get_gc_status().await),
        "maintenance.repair.status" => native_body(get_repair_status().await),
        "maintenance.redistribution.status" => native_body(get_redist_status().await),
        "maintenance.gc.start" => {
            let claims = claims_required(claims)?;
            require_write_modify_delete(&claims)?;
            native_from_response(
                run_garbage_collector(State(state), Query(params_from_payload(&payload))).await,
            )
            .await
        }
        "maintenance.gc.stop" => {
            let claims = claims_required(claims)?;
            native_from_response(stop_garbage_collector(claims, State(state)).await).await
        }
        "maintenance.repair.start" => {
            let claims = claims_required(claims)?;
            let params = params_with_claim_fragment(&claims, &payload);
            native_from_response(trigger_repair_job_api(claims, State(state), Query(params)).await?)
                .await
        }
        "maintenance.repair.stop" => {
            let claims = claims_required(claims)?;
            native_from_response(stop_repair_job_api(claims, State(state)).await?).await
        }
        "maintenance.redistribution.start" => {
            let claims = claims_required(claims)?;
            let params = params_with_claim_fragment(&claims, &payload);
            native_from_response(
                trigger_redistribution_job_api(claims, State(state), Query(params)).await?,
            )
            .await
        }
        "maintenance.redistribution.stop" => {
            let claims = claims_required(claims)?;
            native_from_response(stop_redistribution_job_api(claims, State(state)).await?).await
        }
        "fragment.create" => {
            let req = CreateFragmentRequest {
                name: required_string(&payload, "name")?,
                key: required_string(&payload, "key")?,
                secret: required_string(&payload, "secret")?,
            };
            native_from_response(create_fragment(axum::Json(req)).await?).await
        }
        "fragment.list" => {
            let claims = claims_required(claims)?;
            native_from_response(list_fragments(claims).await?).await
        }
        "fragment.update" => {
            let claims = claims_required(claims)?;
            let fragment =
                string_field(&payload, "fragment").unwrap_or_else(|| claims.fragment.clone());
            let req: UpdateFragmentRequest = typed_payload(&payload)?;
            native_from_response(
                update_fragment(State(state), claims, Path(fragment), axum::Json(req)).await?,
            )
            .await
        }
        "fragment.delete" => {
            let claims = claims_required(claims)?;
            let fragment =
                string_field(&payload, "fragment").unwrap_or_else(|| claims.fragment.clone());
            let req: DeleteFragmentRequest = typed_payload(&payload)?;
            native_from_response(
                delete_fragment(State(state), claims, Path(fragment), axum::Json(req)).await?,
            )
            .await
        }
        "fragment.check" => {
            let fragment = required_string(&payload, "fragment")?;
            native_body(json!({
                "fragment": fragment,
                "exists": does_fragment_exist(&fragment).await,
            }))
        }
        "fragment.hash" | "fragment.hash.public" => {
            let fragment = required_string(&payload, "fragment")?;
            let hash = fragment_hash(&fragment);
            if !does_fragment_exist(&hash).await {
                return Err(AppError::NotFound);
            }
            native_body(json!({"fragment": fragment, "fragment_hash": hash}))
        }
        "fragment.usage" | "capacity.fragment" => {
            let claims = claims_required(claims)?;
            native_from_response(get_fragment_usage(claims).await?).await
        }
        "capacity.usage" => {
            let claims = claims_required(claims)?;
            let fragment = claims.fragment.clone();
            native_from_response(get_capacity_usage(claims, Path(fragment)).await?).await
        }
        "uploads.list" | "multipart.list" => {
            let claims = claims_required(claims)?;
            let fragment = claims.fragment.clone();
            native_from_response(
                list_active_uploads(claims, Path(fragment), Query(params_from_payload(&payload)))
                    .await?,
            )
            .await
        }
        "uploads.status" | "multipart.status" => {
            let claims = claims_required(claims)?;
            let fragment = claims.fragment.clone();
            let version_id = required_string(&payload, "version_id")?;
            native_from_response(get_upload_status(claims, Path((fragment, version_id))).await?)
                .await
        }
        _ => return Ok(None),
    };

    Ok(Some(response))
}

pub async fn prepare_native_download_stream(
    session: &NativeSession,
    payload: Value,
) -> Result<NativeDownloadPlan, AppError> {
    let claims = require_claims(session)?;
    require_read(&claims)?;
    let fragment = claims.fragment.clone();
    let logical = required_string(&payload, "object_key")?;
    let physical = resolve_logical_read_key(&fragment, &logical).await?;
    let metadata = if let Some(version_id) = string_field(&payload, "version_id") {
        get_version_metadata(&fragment, &physical, &version_id, &fragment).await?
    } else {
        get_object_metadata_direct(&fragment, &physical).await?
    };
    if metadata.is_delete_marker {
        return Err(AppError::NotFound);
    }

    let total_size = metadata.total_size_bytes;
    let offset = u64_field(&payload, "offset").unwrap_or(0).min(total_size);
    let remaining = total_size.saturating_sub(offset);
    let requested_len = u64_field(&payload, "length").unwrap_or(remaining);
    let body_len = remaining.min(requested_len);
    let requested_chunk_size = usize_field(&payload, "chunk_size")
        .unwrap_or(NATIVE_STREAM_DOWNLOAD_CHUNK_BYTES)
        .clamp(64 * 1024, 8 * 1024 * 1024);
    let chunk_size = requested_chunk_size.min(NATIVE_STREAM_DOWNLOAD_CHUNK_BYTES);

    let response_payload = json!({
        "object_key": logical.trim_start_matches('/'),
        "version_id": metadata.version_id,
        "content_type": metadata.content_type,
        "total_size_bytes": total_size,
        "offset": offset,
        "length": body_len,
        "chunk_size_bytes": chunk_size,
        "stream": true,
    });

    Ok(NativeDownloadPlan {
        payload: response_payload,
        body_len,
        metadata,
        offset,
    })
}

pub async fn stream_native_download_plan<W>(
    writer: &mut W,
    plan: NativeDownloadPlan,
) -> Result<u64, AppError>
where
    W: AsyncWrite + Unpin + Send,
{
    let written = crate::core::download::stream_object_range_to_writer(
        plan.metadata,
        plan.offset,
        plan.body_len,
        writer,
    )
    .await?;

    if written != plan.body_len {
        return Err(AppError::InternalServerError(format!(
            "native download stream ended early: wrote {} of {} bytes",
            written, plan.body_len
        )));
    }

    writer
        .flush()
        .await
        .map_err(|err| AppError::Io(err, "native download stream flush".to_string()))?;
    Ok(written)
}

pub fn op_response(status: u16) -> NativeOpResponse {
    NativeOpResponse { status }
}

fn native_body<T: Serialize>(value: T) -> (NativeOpResponse, Vec<u8>) {
    (op_response(200), native_body_bytes(&value))
}

fn typed_payload<T: DeserializeOwned>(payload: &Value) -> Result<T, AppError> {
    serde_json::from_value(payload.clone())
        .map_err(|err| AppError::BadRequest(format!("invalid native payload: {err}")))
}

fn upload_part_query_from_payload(payload: &Value) -> Result<UploadPartQuery, AppError> {
    typed_payload(payload)
}

#[derive(Clone, Copy)]
enum BulkNamespaceMode {
    Trash,
    Recover,
}

async fn native_bulk_namespace_operation(
    state: &Arc<AppState>,
    fragment: &str,
    object_keys: Vec<String>,
    event: &str,
    mode: BulkNamespaceMode,
) -> Result<Value, AppError> {
    let requested = object_keys.len();
    let unique_keys = object_keys
        .into_iter()
        .filter(|key| !key.trim().is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if unique_keys.is_empty() {
        return Err(AppError::BadRequest("object_keys must not be empty".into()));
    }

    let mut succeeded = 0usize;
    let mut not_found = 0usize;
    let mut failed = Vec::new();
    for object_key in &unique_keys {
        let result = match mode {
            BulkNamespaceMode::Trash => {
                global_namespace_runtime().trash(fragment, object_key).await
            }
            BulkNamespaceMode::Recover => {
                global_namespace_runtime()
                    .recover(fragment, object_key)
                    .await
            }
        };
        match result {
            Ok(_) => succeeded = succeeded.saturating_add(1),
            Err(AppError::NotFound) => not_found = not_found.saturating_add(1),
            Err(err) => failed.push(json!({
                "object_key": object_key,
                "error": err.to_string(),
            })),
        }
    }

    if succeeded > 0 {
        state.emit(
            fragment,
            StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::bulk(
                fragment, event, succeeded,
            )),
        );
    }

    Ok(json!({
        "requested": requested,
        "unique": unique_keys.len(),
        "succeeded": succeeded,
        "not_found": not_found,
        "failed": failed,
    }))
}

async fn native_download_metadata(
    metadata: ObjectMetadata,
    physical_object_key: String,
) -> Result<(NativeOpResponse, Vec<u8>), AppError> {
    let total_size = usize::try_from(metadata.total_size_bytes).map_err(|_| {
        AppError::PayloadTooLarge("native download size exceeds platform usize".into())
    })?;
    if total_size > crate::protocol::types::MAX_NATIVE_FULL_DOWNLOAD_BYTES {
        return Err(AppError::PayloadTooLarge(format!(
            "native full download exceeds {} bytes; use object.download_chunk",
            crate::protocol::types::MAX_NATIVE_FULL_DOWNLOAD_BYTES
        )));
    }

    let mut offset = 0u64;
    let mut out = Vec::with_capacity(total_size);
    while offset < metadata.total_size_bytes {
        let chunk_size = ((metadata.total_size_bytes - offset) as usize).min(8 * 1024 * 1024);
        let (chunk, _) = crate::core::download::download_object_chunk_from_metadata(
            metadata.clone(),
            &ChunkQuery {
                object_key: physical_object_key.clone(),
                offset,
                chunk_size,
            },
        )
        .await?;
        if chunk.is_empty() {
            break;
        }
        offset = offset.saturating_add(chunk.len() as u64);
        out.extend_from_slice(&chunk);
    }

    Ok((op_response(200), out))
}

async fn native_from_response(response: impl IntoResponse) -> (NativeOpResponse, Vec<u8>) {
    let response = response.into_response();
    let status = response.status().as_u16();
    let raw_body = to_bytes(
        response.into_body(),
        crate::protocol::types::MAX_NATIVE_FRAME_BYTES,
    )
    .await
    .map(|bytes| bytes.to_vec())
    .unwrap_or_default();
    let body = serde_json::from_slice::<Value>(&raw_body)
        .ok()
        .and_then(|value| rmp_serde::to_vec_named(&value).ok())
        .unwrap_or(raw_body);
    (op_response(status), body)
}

fn native_app_error(err: AppError) -> NativeExecutionError {
    let status = app_error_status(&err).as_u16();
    let body = native_body_bytes(&json!({"error": err.to_string()}));
    NativeExecutionError(op_response(status), body)
}

pub fn auth_error_response(err: &AuthError) -> NativeOpResponse {
    op_response(auth_error_status(err).as_u16())
}

pub fn native_body_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(value).unwrap_or_default()
}

pub fn auth_error_message(err: &AuthError) -> &'static str {
    match err {
        AuthError::WrongCredentials => "Wrong credentials",
        AuthError::MissingCredentials => "Missing credentials",
        AuthError::InvalidToken => "Invalid token",
        AuthError::InternalError => "An internal error occurred",
        AuthError::FragmentNotFound => "Fragment not found",
    }
}

fn auth_error_status(err: &AuthError) -> StatusCode {
    match err {
        AuthError::WrongCredentials | AuthError::InvalidToken => StatusCode::UNAUTHORIZED,
        AuthError::MissingCredentials | AuthError::FragmentNotFound => StatusCode::BAD_REQUEST,
        AuthError::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn app_error_status(err: &AppError) -> StatusCode {
    err.status_code()
}

fn require_claims(session: &NativeSession) -> Result<Claims, AppError> {
    session.claims().cloned().ok_or(AppError::Unauthorized)
}

fn claims_required(claims: Option<Claims>) -> Result<Claims, AppError> {
    claims.ok_or(AppError::Unauthorized)
}

fn require_read(claims: &Claims) -> Result<(), AppError> {
    if claims.read {
        Ok(())
    } else {
        Err(AppError::Forbidden("Read permission required".into()))
    }
}

fn require_write(claims: &Claims) -> Result<(), AppError> {
    if claims.write {
        Ok(())
    } else {
        Err(AppError::Forbidden("Write permission required".into()))
    }
}

fn require_write_modify(claims: &Claims) -> Result<(), AppError> {
    if claims.write && claims.modify {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "Write and Modify permission required".into(),
        ))
    }
}

fn require_write_modify_delete(claims: &Claims) -> Result<(), AppError> {
    if claims.write && claims.modify && claims.delete {
        Ok(())
    } else {
        Err(AppError::Forbidden(
            "Write, Modify and Delete permission required".into(),
        ))
    }
}

async fn resolve_logical_read_key(
    fragment: &str,
    requested_path: &str,
) -> Result<String, AppError> {
    let requested = requested_path.trim_start_matches('/').trim();
    if let Some(entry) = global_namespace_runtime()
        .entry(fragment, requested)
        .await?
    {
        if entry.kind != NamespaceEntryKind::File || !entry.lifecycle.is_live() {
            return Err(logical_not_found());
        }
        return entry.object_id.ok_or_else(|| {
            AppError::InternalServerError("logical file is missing physical object id".to_string())
        });
    }
    crate::core::index_store::normalize_object_id(requested).map_err(|_| logical_not_found())
}

fn params_from_payload(payload: &Value) -> HashMap<String, String> {
    payload
        .as_object()
        .map(|obj| {
            obj.iter()
                .filter_map(|(key, value)| value_to_string(value).map(|value| (key.clone(), value)))
                .collect()
        })
        .unwrap_or_default()
}

fn params_with_claim_fragment(claims: &Claims, payload: &Value) -> HashMap<String, String> {
    let mut params = params_from_payload(payload);
    params
        .entry("fragment".to_string())
        .or_insert_with(|| claims.fragment.clone());
    params
}

fn check_mode_payload(payload: &Value) -> crate::core::repair_manager::RepairCheckMode {
    parse_check_mode(&params_from_payload(payload))
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        _ => Some(value.to_string()),
    }
}

fn string_field(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(value_to_string)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn required_string(payload: &Value, key: &str) -> Result<String, AppError> {
    string_field(payload, key).ok_or_else(|| AppError::BadRequest(format!("{key} is required")))
}

fn string_array_field(payload: &Value, key: &str) -> Result<Vec<String>, AppError> {
    payload
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| AppError::BadRequest(format!("{key} must be an array")))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| AppError::BadRequest(format!("{key} must contain strings")))
        })
        .collect()
}

fn bool_field(payload: &Value, key: &str) -> Option<bool> {
    match payload.get(key)? {
        Value::Bool(value) => Some(*value),
        Value::String(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
        .then_some(true)
        .or_else(|| Some(false)),
        Value::Number(value) => value.as_u64().map(|value| value != 0),
        _ => None,
    }
}

fn usize_field(payload: &Value, key: &str) -> Option<usize> {
    payload.get(key).and_then(|value| match value {
        Value::Number(value) => value.as_u64().and_then(|value| usize::try_from(value).ok()),
        Value::String(value) => value.trim().parse::<usize>().ok(),
        _ => None,
    })
}

fn u64_field(payload: &Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(|value| match value {
        Value::Number(value) => value.as_u64(),
        Value::String(value) => value.trim().parse::<u64>().ok(),
        _ => None,
    })
}

fn fragment_hash(fragment: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(fragment.as_bytes());
    hex::encode(hasher.finalize())
}

#[allow(dead_code)]
fn encode_query(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}
