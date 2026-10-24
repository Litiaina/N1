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
use tokio_util::io::ReaderStream;

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
use crate::core::metadata_parser::get_version_metadata_by_id;
use crate::core::monitoring::system_health_snapshot;
use crate::core::redistribution::{
    get_redist_status, stop_redistribution_job_api, trigger_redistribution_job_api,
};
use crate::core::repair_manager::{
    get_repair_status, parse_check_mode, repair_object, stop_repair_job_api, trigger_repair_job_api,
};
use crate::core::retrieve::{get_object_metadata_by_id, list_fragments};
use crate::core::storage_capacity_parser::get_capacity_usage;
use crate::core::sync::CreateFragmentRequest;
use crate::core::upload::{
    FinalizeUploadResult, cancel_upload_session_direct, get_upload_status, list_active_uploads,
};
use crate::distribution::download::part_has_remote_shards;
use crate::distribution::health::cluster_health_snapshot;
use crate::distribution::membership::active_cluster_runtime;
use crate::distribution::monitor::{ClusterMonitorQuery, cluster_monitor_snapshot};
use crate::distribution::namespace::{
    NamespaceMutationKind, PeerNamespaceApplyRequest, commit_namespace_bulk,
    commit_namespace_summary,
};
use crate::distribution::rebalance::{
    ClusterRebalanceRequest, metadata_matches_current_layout, plan_rebalance, rebalance_status,
    repair_status, rewrite_object_to_current_layout, start_rebalance, start_repair, stop_rebalance,
    stop_repair,
};
use crate::distribution::repair::repair_policy;
use crate::distribution::resolve_configured_layout;
use crate::distribution::routes::cluster_usage_snapshot;
use crate::distribution::status::inspect_cluster_object_status;
use crate::distribution::upload::{
    cancel_standard_cluster_upload, complete_standard_cluster_upload, finalize_cluster_upload,
    finalize_standard_cluster_upload, find_reusable_standard_cluster_upload,
    get_standard_cluster_upload_status, initiate_cluster_upload, initiate_standard_cluster_upload,
    list_standard_cluster_uploads, put_cluster_part, put_standard_cluster_part,
    standard_upload_binding, upload_cluster_one_shot,
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

mod download;
mod helpers;
mod streaming;

pub use download::*;
pub use helpers::*;
pub use streaming::*;

pub struct NativeExecutionError(pub NativeOpResponse, pub Vec<u8>);

const NATIVE_STREAM_DOWNLOAD_CHUNK_BYTES: usize = 4 * 1024 * 1024;

pub struct NativeDownloadPlan {
    pub payload: Value,
    pub body_len: u64,
    metadata: ObjectMetadata,
    offset: u64,
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

pub(super) async fn execute_native_op_inner(
    state: Arc<AppState>,
    session: &NativeSession,
    op: &str,
    payload: Value,
    _body: Option<Vec<u8>>,
) -> Result<Option<(NativeOpResponse, Vec<u8>)>, AppError> {
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
                posix_service::rename_path_or_start(
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
                posix_service::trash_path_or_start(
                    state,
                    claims,
                    required_string(&payload, "path")?,
                )
                .await?,
            )
        }
        "namespace.recover" | "posix.recover" => {
            let claims = claims_required(claims)?;
            native_body(
                posix_service::recover_path_or_start(
                    state,
                    claims,
                    required_string(&payload, "path")?,
                )
                .await?,
            )
        }
        "namespace.operation.status" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            native_body(
                crate::posix::operations::namespace_operation_status(
                    &claims.fragment,
                    &required_string(&payload, "operation_id")?,
                )
                .await?,
            )
        }
        "namespace.operation.cancel" => {
            let claims = claims_required(claims)?;
            let operation_id = required_string(&payload, "operation_id")?;
            let operation = crate::posix::operations::namespace_operation_status(
                &claims.fragment,
                &operation_id,
            )
            .await?;
            if operation.operation == "rename" {
                if !claims.modify {
                    return Err(AppError::Forbidden("Modify permission required".into()));
                }
            } else {
                require_write(&claims)?;
            }
            native_body(
                crate::posix::operations::cancel_namespace_operation(
                    &claims.fragment,
                    &operation_id,
                )
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
        "object.status" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let object_key = required_string(&payload, "object_key")?;
            let physical = resolve_logical_read_key(&claims.fragment, &object_key).await?;
            let metadata = if let Some(version_id) = string_field(&payload, "version_id") {
                get_version_metadata_by_id(&claims.fragment, &physical, &version_id).await?
            } else {
                get_object_metadata_by_id(&claims.fragment, &physical).await?
            };
            let mut status =
                inspect_cluster_object_status(&metadata, check_mode_payload(&payload)).await?;
            status.object_key = object_key.trim_start_matches('/').to_string();
            native_body(status)
        }
        "object.repair" => {
            let claims = claims_required(claims)?;
            require_write_modify(&claims)?;
            let object_key = required_string(&payload, "object_key")?;
            let physical = resolve_logical_read_key(&claims.fragment, &object_key).await?;
            let metadata = get_object_metadata_by_id(&claims.fragment, &physical).await?;
            let check_mode = check_mode_payload(&payload);
            let mut status = if metadata.parts.iter().any(part_has_remote_shards) {
                let status = inspect_cluster_object_status(&metadata, check_mode).await?;
                if status.status == "healthy" {
                    status
                } else if status.repairable {
                    crate::core::repair_manager::repair_cluster_object_recorded_layout(
                        &metadata, check_mode,
                    )
                    .await?
                } else {
                    status
                }
            } else {
                repair_object(&metadata, check_mode).await?
            };
            status.object_key = object_key.trim_start_matches('/').to_string();
            native_body(status)
        }
        "object.redistribute" => {
            let claims = claims_required(claims)?;
            require_write_modify(&claims)?;
            let object_key = required_string(&payload, "object_key")?;
            let physical = resolve_logical_read_key(&claims.fragment, &object_key).await?;
            let requested_version_id = string_field(&payload, "version_id");
            let metadata = if let Some(version_id) = requested_version_id.as_deref() {
                get_version_metadata_by_id(&claims.fragment, &physical, version_id).await?
            } else {
                get_object_metadata_by_id(&claims.fragment, &physical).await?
            };
            let check_mode = check_mode_payload(&payload);
            let mut before = inspect_cluster_object_status(&metadata, check_mode).await?;
            before.object_key = object_key.trim_start_matches('/').to_string();
            let runtime = active_cluster_runtime()?;
            let dry_run = bool_field(&payload, "dry_run").unwrap_or(false);
            let force = bool_field(&payload, "force").unwrap_or(false);
            let already_current = metadata_matches_current_layout(&metadata, &runtime);
            if already_current && !force {
                let after = before.clone();
                native_body(json!({
                    "object_key": object_key.trim_start_matches('/'),
                    "physical_object_id": physical,
                    "requested_version_id": requested_version_id,
                    "dry_run": dry_run,
                    "force": force,
                    "rewritten": false,
                    "skipped": true,
                    "reason": "object version already uses the current topology",
                    "before": before,
                    "after": after,
                }))
            } else if dry_run {
                native_body(json!({
                    "object_key": object_key.trim_start_matches('/'),
                    "physical_object_id": physical,
                    "requested_version_id": requested_version_id,
                    "dry_run": dry_run,
                    "force": force,
                    "rewritten": false,
                    "skipped": false,
                    "reason": "dry_run: object version would be redistributed",
                    "before": before,
                    "after": null,
                }))
            } else {
                rewrite_object_to_current_layout(
                    object_key.trim_start_matches('/').to_string(),
                    physical.clone(),
                    metadata,
                    false,
                )
                .await?;
                let rewritten_metadata =
                    get_object_metadata_by_id(&claims.fragment, &physical).await?;
                let mut after =
                    inspect_cluster_object_status(&rewritten_metadata, check_mode).await?;
                after.object_key = object_key.trim_start_matches('/').to_string();
                native_body(json!({
                    "object_key": object_key.trim_start_matches('/'),
                    "physical_object_id": physical,
                    "requested_version_id": requested_version_id,
                    "dry_run": dry_run,
                    "force": force,
                    "rewritten": true,
                    "skipped": false,
                    "reason": null,
                    "before": before,
                    "after": after,
                }))
            }
        }
        "object.delete" => {
            let claims = claims_required(claims)?;
            let path = required_string(&payload, "object_key")?;
            native_body(posix_service::trash_path_or_start(state, claims, path).await?)
        }
        "object.recover" => {
            let claims = claims_required(claims)?;
            let path = required_string(&payload, "object_key")?;
            native_body(posix_service::recover_path_or_start(state, claims, path).await?)
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
            native_body(
                posix_service::rename_path_or_start(state, claims, source, destination).await?,
            )
        }
        "object.meta" | "object.get" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let logical = required_string(&payload, "object_key")?;
            let fragment = claims.fragment.clone();
            let physical = resolve_logical_read_key(&fragment, &logical).await?;
            let mut metadata = if let Some(version_id) = string_field(&payload, "version_id") {
                get_version_metadata_by_id(&fragment, &physical, &version_id).await?
            } else {
                get_object_metadata_by_id(&fragment, &physical).await?
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
            for metadata in &mut versions {
                metadata.object_key = logical.trim_start_matches('/').to_string();
                metadata.strip_internal_layout();
            }
            native_body(versions)
        }
        "object.version.recover" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let version_id = required_string(&payload, "version_id")?;
            let logical = required_string(&payload, "object_key")?;
            let physical = resolve_any_logical_object_id(&fragment, &logical).await?;
            let version_exists =
                match index::read_object_version(&fragment, &physical, &version_id).await {
                    Ok(_) => true,
                    Err(AppError::NotFound) => {
                        index::read_trashed_object_version(&fragment, &physical, &version_id)
                            .await
                            .is_ok()
                    }
                    Err(err) => return Err(err),
                };
            if !version_exists {
                return Err(AppError::NotFound);
            }
            let summary = commit_namespace_summary(PeerNamespaceApplyRequest {
                mutation_id: None,
                kind: NamespaceMutationKind::Recover,
                fragment: fragment.clone(),
                source_path: Some(logical.clone()),
                destination_path: None,
                object_id: None,
                version_id: Some(version_id.clone()),
                metadata: None,
            })
            .await?;
            state.emit(
                &fragment,
                StorageEventPayload::NamespaceChanged(NamespaceChangeEvent::from_summary(
                    &fragment,
                    summary.clone(),
                )),
            );
            native_body(json!({
                "operation": summary.operation,
                "object_key": logical,
                "physical_object_id": physical,
                "version_id": version_id,
                "affected_paths": summary.affected_paths,
                "affected_objects": summary.affected_objects,
            }))
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
            let body_len = body.len() as u64;
            let body_stream =
                futures_util::stream::once(async move { Ok::<Bytes, AppError>(Bytes::from(body)) });
            let mut metadata = upload_cluster_one_shot(
                claims,
                fragment.clone(),
                physical.clone(),
                crate::core::index_store::packed_extension_key(&logical),
                string_field(&payload, "content_type")
                    .unwrap_or_else(|| "application/octet-stream".to_string()),
                upload_part_query_from_payload(&payload)?,
                body_stream,
                body_len,
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
        "upload.multipart.standard.init" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let logical = required_string(&payload, "object_key")?;
            let mut params: InitiateUploadParams = typed_payload(&payload)?;
            if let Some(existing) =
                find_reusable_standard_cluster_upload(&fragment, &logical, &params)
            {
                (op_response(201), native_body_bytes(&existing))
            } else {
                params.packed_ext_u64 = crate::core::index_store::packed_extension_key(&logical);
                let physical = resolve_logical_write_key(&fragment, &logical).await?;
                let response =
                    initiate_standard_cluster_upload(claims, fragment, physical, logical, params)
                        .await?;
                (op_response(201), native_body_bytes(&response))
            }
        }
        "multipart.init" | "upload.multipart.init" | "upload.multipart.durable.init" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let logical = required_string(&payload, "object_key")?;
            let mut params: InitiateUploadParams = typed_payload(&payload)?;
            if let Some(existing) =
                find_reusable_pending_upload(&fragment, &logical, &params).await?
            {
                (op_response(201), native_body_bytes(&existing))
            } else {
                params.packed_ext_u64 = crate::core::index_store::packed_extension_key(&logical);
                let physical = resolve_logical_write_key(&fragment, &logical).await?;
                let response =
                    initiate_cluster_upload(claims, fragment.clone(), physical.clone(), params)
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
        "multipart.put_part"
        | "upload.multipart.put_part"
        | "upload.multipart.durable.put_part"
        | "upload.multipart.standard.put_part" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let body = _body.ok_or_else(|| AppError::BadRequest("part body is required".into()))?;
            let fragment = claims.fragment.clone();
            let version_id = required_string(&payload, "version_id")?;
            let part_index = u64_field(&payload, "part_index")
                .ok_or_else(|| AppError::BadRequest("part_index is required".into()))?;
            let body_len = body.len() as u64;
            let body_stream =
                futures_util::stream::once(async move { Ok::<Bytes, AppError>(Bytes::from(body)) });
            if op == "upload.multipart.standard.put_part" {
                put_standard_cluster_part(
                    claims,
                    fragment,
                    version_id.clone(),
                    part_index,
                    upload_part_query_from_payload(&payload)?,
                    body_stream,
                    body_len,
                )
                .await?;
            } else {
                put_cluster_part(
                    claims,
                    fragment,
                    version_id.clone(),
                    part_index,
                    upload_part_query_from_payload(&payload)?,
                    body_stream,
                    body_len,
                )
                .await?;
            }
            native_body(json!({
                "version_id": version_id,
                "part_index": part_index,
                "uploaded": true,
            }))
        }
        "upload.multipart.standard.finalize" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let version_id = required_string(&payload, "version_id")?;
            let session_id = required_string(&payload, "session_id")?;
            let (logical_path, physical) = standard_upload_binding(&fragment, &version_id)
                .ok_or_else(|| AppError::BadRequest("Standard upload session not found".into()))?;
            match finalize_standard_cluster_upload(
                claims,
                fragment.clone(),
                version_id.clone(),
                session_id,
            )
            .await?
            {
                FinalizeUploadResult::Complete(mut metadata) => {
                    let committed_physical = commit_logical_upload_from_meta(
                        &fragment,
                        &logical_path,
                        &physical,
                        &metadata,
                    )
                    .await?;
                    complete_standard_cluster_upload(&fragment, &version_id);
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
        "multipart.finalize"
        | "upload.multipart.finalize"
        | "upload.multipart.durable.finalize" => {
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
            match finalize_cluster_upload(
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
                        &metadata.object_key,
                        &metadata,
                    )
                    .await?;
                    index::remove_session(&fragment, &metadata.version_id, "finalized").await?;
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
        "upload.multipart.standard.cancel" => {
            let claims = claims_required(claims)?;
            require_write(&claims)?;
            let fragment = claims.fragment.clone();
            let version_id = required_string(&payload, "version_id")?;
            cancel_standard_cluster_upload(&claims, &fragment, &version_id).await?;
            (op_response(204), Vec::new())
        }
        "multipart.cancel" | "upload.multipart.cancel" | "upload.multipart.durable.cancel" => {
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
                get_version_metadata_by_id(&fragment, &physical, &version_id).await?
            } else {
                get_object_metadata_by_id(&fragment, &physical).await?
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
                get_version_metadata_by_id(&fragment, &physical, &version_id).await?
            } else {
                get_object_metadata_by_id(&fragment, &physical).await?
            };
            native_download_metadata(metadata, physical).await?
        }
        "maintenance.health" => {
            let claims = claims_required(claims)?;
            native_body(system_health_snapshot(state, claims, params_from_payload(&payload)).await?)
        }
        "cluster.membership" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let runtime = active_cluster_runtime()?;
            native_body(json!({
                "local_node_id": runtime.local_node_id,
                "membership_path": runtime.membership_path,
                "membership": runtime.membership,
            }))
        }
        "cluster.layout" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let runtime = active_cluster_runtime()?;
            let health = cluster_health_snapshot(&runtime).await?;
            let configured = runtime
                .membership
                .newest_writable_pool()
                .map(|pool| pool.node_ids.len())
                .unwrap_or_default();
            let layout = resolve_configured_layout(configured, &runtime.layout_policy)
                .map_err(AppError::BadRequest)?;
            let data_reconstruction_writable =
                health.data_plane_writable && health.ncnx_survival_writable;
            let survival_writable = health.effective_client_writable;
            let fully_protected =
                health.data_plane_fully_protected && health.ncnx_strictly_writable;
            native_body(json!({
                "membership_epoch": runtime.membership.epoch,
                "policy": runtime.layout_policy,
                "active_write_nodes": health.active_write_nodes,
                "layout": layout,
                "configured_layout": layout,
                "data_reconstruction_writable": data_reconstruction_writable,
                "write_authority_proven": health.write_authority_proven,
                "survival_writable": survival_writable,
                "fully_protected": fully_protected,
                "degraded_write_layout": survival_writable && !fully_protected,
                "bounded_object_erasure_set": true,
                "static_server_pool_ec": true,
                "old_object_layouts_remain_valid": true,
                "automatic_rebalance": runtime.auto_rebalance,
            }))
        }
        "cluster.health" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let runtime = active_cluster_runtime()?;
            native_body(cluster_health_snapshot(&runtime).await?)
        }
        "cluster.monitor" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let query =
                serde_json::from_value::<ClusterMonitorQuery>(payload.clone()).unwrap_or_default();
            native_body(cluster_monitor_snapshot(state, claims, query).await?)
        }
        "cluster.usage" => {
            let claims = claims_required(claims)?;
            native_body(
                cluster_usage_snapshot(
                    claims,
                    string_field(&payload, "fragment"),
                    bool_field(&payload, "refresh").unwrap_or(false),
                )
                .await?,
            )
        }
        "cluster.repair.status" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            native_body(repair_status())
        }
        "cluster.repair.plan" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let runtime = active_cluster_runtime()?;
            native_body(repair_policy(&runtime))
        }
        "cluster.repair.start" => {
            let claims = claims_required(claims)?;
            require_write_modify(&claims)?;
            let request: ClusterRebalanceRequest = typed_payload(&payload)?;
            native_body(start_repair(request)?)
        }
        "cluster.repair.stop" => {
            let claims = claims_required(claims)?;
            require_write_modify(&claims)?;
            native_body(stop_repair())
        }
        "cluster.rebalance.status" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            native_body(rebalance_status())
        }
        "cluster.rebalance.plan" => {
            let claims = claims_required(claims)?;
            require_read(&claims)?;
            let runtime = active_cluster_runtime()?;
            native_body(plan_rebalance(&runtime)?)
        }
        "cluster.rebalance.start" => {
            let claims = claims_required(claims)?;
            require_write_modify(&claims)?;
            let request: ClusterRebalanceRequest = typed_payload(&payload)?;
            native_body(start_rebalance(request)?)
        }
        "cluster.rebalance.stop" => {
            let claims = claims_required(claims)?;
            require_write_modify(&claims)?;
            native_body(stop_rebalance())
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
        "capacity.usage" => {
            let claims = claims_required(claims)?;
            let fragment = claims.fragment.clone();
            native_from_response(get_capacity_usage(claims, Path(fragment)).await?).await
        }
        "uploads.standard.list" | "multipart.standard.list" => {
            let claims = claims_required(claims)?;
            let fragment = claims.fragment.clone();
            let params = params_from_payload(&payload);
            let limit = params
                .get("limit")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(50)
                .min(100);
            let offset = params
                .get("offset")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            native_body(list_standard_cluster_uploads(
                &claims, &fragment, limit, offset,
            )?)
        }
        "uploads.list" | "multipart.list" | "uploads.durable.list" => {
            let claims = claims_required(claims)?;
            let fragment = claims.fragment.clone();
            native_from_response(
                list_active_uploads(claims, Path(fragment), Query(params_from_payload(&payload)))
                    .await?,
            )
            .await
        }
        "uploads.standard.status" | "multipart.standard.status" => {
            let claims = claims_required(claims)?;
            let fragment = claims.fragment.clone();
            let version_id = required_string(&payload, "version_id")?;
            native_body(get_standard_cluster_upload_status(
                &claims,
                &fragment,
                &version_id,
            )?)
        }
        "uploads.status" | "multipart.status" | "uploads.durable.status" => {
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
