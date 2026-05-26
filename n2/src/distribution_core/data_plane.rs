use crate::distribution_core::cluster_shards::{
    abort_coordinator_shard_session, append_coordinator_shard_data,
    begin_local_coordinator_shard_session, begin_remote_cluster_shard_session,
    collect_readable_cluster_shard_refs, commit_coordinator_shard_session,
    delete_local_cluster_shard, delete_remote_cluster_shard, flush_coordinator_shard_data,
    open_local_cluster_shard_stream, open_remote_cluster_shard_stream,
    reconstruct_object_stream_from_cluster_sources, store_cluster_shard_from_available_sources,
};
#[cfg(test)]
use crate::distribution_core::cluster_shards::{
    read_local_cluster_shard, store_local_cluster_shard_from_bytes,
};
use crate::distribution_core::cluster_topology::{
    is_admin_claim, is_local_target, shard_matches_target,
};
#[cfg(test)]
use crate::distribution_core::codec::reconstruct_cluster_shard_from_available;
use crate::distribution_core::codec::{
    encode_cluster_stripe_blocks, normalize_n2_stripe_block_size,
};
use crate::distribution_core::config::{
    N2AutoMaintenanceMode, effective_cross_params, n2_runtime_config,
};
use crate::distribution_core::health::inspect_fragment_health;
use crate::distribution_core::maintenance_gate::acquire_fragment_repair_permit;
use crate::distribution_core::model::{
    CrossParams, DeleteN2ObjectQuery, GetObjectQuery, ListN2ObjectsQuery, ListN2VersionsQuery,
    N2ClusterShardRef, N2DeleteObjectResponse, N2ListObjectVersionsResponse, N2ListObjectsResponse,
    N2ObjectRecord, N2ObjectSummary, N2ObjectVersionSummary, N2PutObjectResponse,
    N2ReconcileObjectResult, N2ReconcileResponse, N2RenameObjectResponse, NodeDescriptor,
    ObjectIdentity, PutObjectQuery, ReconcileFragmentQuery, RenameN2ObjectRequest,
};
use crate::distribution_core::object_index::{
    delete_n2_object_record_local, delete_n2_object_record_remote, list_fragment_n2_namespace_page,
    list_fragment_n2_records, list_n2_fragments, list_n2_records_for_object, load_n2_object_record,
    persist_n2_object_record, rename_n2_namespace_entry, resolve_or_create_n2_object_id_local,
};
use crate::distribution_core::object_request::{
    content_type_from_query_or_headers, parse_single_byte_range, required_cluster_commit_shards,
};
use crate::distribution_core::peer::normalize_endpoint;
use crate::distribution_core::shard_session::{
    CoordinatorShardSession, RemoteCoordinatorShardSession,
};
use crate::distribution_core::state::ensure_cluster_write_ready;
use crate::distribution_core::state::resolve_placement;
use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;
use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{
    HeaderMap, HeaderValue, StatusCode,
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG},
};
use axum::response::{IntoResponse, Response};
use bytes::{Bytes, BytesMut};
use chrono::Utc;
use futures_util::{Stream, StreamExt};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Duration;
#[cfg(test)]
use tokio::fs;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn};
use ulid::Ulid;

const X_AMZ_VERSION_ID_HEADER: &str = "x-amz-version-id";
const N2_REMOTE_CHUNK_FLUSH_MIN_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Default)]
struct AutoMaintenanceSchedulerState {
    running: bool,
    pending_epoch: Option<u64>,
}

type AutoMaintenanceScheduler = Mutex<AutoMaintenanceSchedulerState>;
static AUTO_MAINTENANCE_SCHEDULER: OnceLock<AutoMaintenanceScheduler> = OnceLock::new();

fn auto_maintenance_scheduler() -> &'static AutoMaintenanceScheduler {
    AUTO_MAINTENANCE_SCHEDULER.get_or_init(|| Mutex::new(AutoMaintenanceSchedulerState::default()))
}

#[derive(Clone, Debug, Default)]
struct AutoMaintenanceRunSummary {
    fragments_scanned: usize,
    fragments_with_planned_changes: usize,
    failed_fragments: usize,
    dry_run: bool,
}

async fn cleanup_committed_cluster_shards(
    http_client: &reqwest::Client,
    object: &ObjectIdentity,
    committed: &[N2ClusterShardRef],
) {
    for shard in committed {
        let target = NodeDescriptor {
            node_id: shard.node_id.clone(),
            endpoint: shard.endpoint.clone(),
            epoch: 0,
            active: true,
        };
        let deleted = if is_local_target(&target) {
            delete_local_cluster_shard(
                object.fragment.as_str(),
                object.version_id.as_str(),
                shard.cluster_shard_index,
            )
            .await
        } else {
            delete_remote_cluster_shard(
                http_client,
                shard.endpoint.as_str(),
                object.fragment.as_str(),
                object.version_id.as_str(),
                shard.cluster_shard_index,
            )
            .await
        };
        if let Err(err) = deleted {
            warn!(
                "n2 strict cleanup failed fragment={} object_key={} version={} shard={} node={} endpoint={} err={}",
                object.fragment,
                object.object_key,
                object.version_id,
                shard.cluster_shard_index,
                shard.node_id,
                shard.endpoint,
                err
            );
        }
    }
}

async fn cleanup_failed_n2_record(http_client: &reqwest::Client, record: &N2ObjectRecord) {
    if let Err(err) = delete_n2_object_record_local(
        record.object.fragment.as_str(),
        record.object.object_key.as_str(),
        record.object.version_id.as_str(),
    )
    .await
    {
        warn!(
            "n2 strict cleanup local object record delete failed fragment={} object_key={} version={} err={}",
            record.object.fragment, record.object.object_key, record.object.version_id, err
        );
    }

    let local_endpoint = normalize_endpoint(&n2_runtime_config().node_endpoint);
    let mut remote_endpoints = record
        .shards
        .iter()
        .map(|s| s.endpoint.clone())
        .filter(|endpoint| normalize_endpoint(endpoint) != local_endpoint)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    remote_endpoints.sort();

    for endpoint in remote_endpoints {
        if let Err(err) = delete_n2_object_record_remote(
            http_client,
            endpoint.as_str(),
            record.object.fragment.as_str(),
            record.object.object_key.as_str(),
            record.object.version_id.as_str(),
        )
        .await
        {
            warn!(
                "n2 strict cleanup remote object record delete failed fragment={} object_key={} version={} endpoint={} err={}",
                record.object.fragment,
                record.object.object_key,
                record.object.version_id,
                endpoint,
                err
            );
        }
    }

    cleanup_committed_cluster_shards(http_client, &record.object, &record.shards).await;
}

pub async fn put_n2_object(
    claims: Claims,
    headers: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<PutObjectQuery>,
    body: Body,
) -> Result<AxumJson<N2PutObjectResponse>, AppError> {
    let body_stream = body.into_data_stream().map(|chunk_result| {
        chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "read n2 upload stream".to_string(),
            )
        })
    });
    put_n2_object_stream(claims, headers, fragment, object_key, query, body_stream).await
}

pub async fn put_n2_object_stream<S>(
    claims: Claims,
    headers: HeaderMap,
    fragment: String,
    object_key: String,
    query: PutObjectQuery,
    mut body_stream: S,
) -> Result<AxumJson<N2PutObjectResponse>, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>> + Unpin,
{
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".to_string()));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }
    let cluster_view = ensure_cluster_write_ready(query.expected_epoch).await?;

    let cfg = n2_runtime_config();
    let cross_params = effective_cross_params();
    let content_type = content_type_from_query_or_headers(&query, &headers);
    let kc = query.kc.unwrap_or(cross_params.kc);
    let mc = query.mc.unwrap_or(cross_params.mc);
    let kd = query.kd.unwrap_or(cross_params.kd);
    let md = query.md.unwrap_or(cross_params.md);
    let commit_mode = cfg.cluster_commit_mode;
    let stripe_block_size = normalize_n2_stripe_block_size(
        query
            .stripe_block_size
            .unwrap_or(cfg.cluster_stripe_block_size),
    );

    if kc == 0 || kd == 0 {
        return Err(AppError::BadRequest(
            "kc and kd must be greater than 0".to_string(),
        ));
    }

    let uid = query.uid.unwrap_or_else(|| claims.fragment.clone());
    let version_id = query.version_id.unwrap_or_else(|| Ulid::new().to_string());
    let normalized_object_key = object_key.trim_start_matches('/').to_string();
    let object_id =
        resolve_or_create_n2_object_id_local(fragment.as_str(), normalized_object_key.as_str())
            .await?;
    let object = ObjectIdentity {
        uid,
        fragment: fragment.clone(),
        object_id,
        object_key: normalized_object_key,
        version_id: version_id.clone(),
    };
    let placement = resolve_placement(object.clone(), kc, mc).await?;
    let nc = usize::from(kc).saturating_add(usize::from(mc));
    let required_commit_shards = required_cluster_commit_shards(commit_mode, kc, nc);

    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;

    let mut sessions = Vec::with_capacity(nc);
    for (idx, target) in placement.targets.iter().enumerate() {
        let result = if is_local_target(target) {
            begin_local_coordinator_shard_session(target.clone(), idx).await
        } else {
            begin_remote_cluster_shard_session(
                &http_client,
                target,
                &object,
                idx,
                kd,
                md,
                stripe_block_size,
            )
            .await
            .map(|session_id| {
                CoordinatorShardSession::Remote(RemoteCoordinatorShardSession {
                    target: target.clone(),
                    cluster_shard_index: idx,
                    session_id,
                    pending: Vec::new(),
                    checksum: Sha256::new(),
                    size_bytes: 0,
                    failed: None,
                })
            })
        };

        match result {
            Ok(session) => {
                sessions.push(session);
            }
            Err(e) => {
                warn!(
                    "n2 shard session begin failed fragment={} object_key={} version={} shard={} target_node={} target_endpoint={} err={}",
                    object.fragment,
                    object.object_key,
                    object.version_id,
                    idx,
                    target.node_id,
                    target.endpoint,
                    e
                );
            }
        }
    }

    let ready_sessions = sessions.iter().filter(|s| !s.is_failed()).count();
    if ready_sessions < required_commit_shards {
        for session in &mut sessions {
            let _ = abort_coordinator_shard_session(&http_client, session).await;
        }
        return Err(AppError::Conflict(format!(
            "n2 cluster commit policy {} requires {} ready shard sessions before ingest, but only {} are available",
            commit_mode.as_str(),
            required_commit_shards,
            ready_sessions
        )));
    }

    let stripe_width = stripe_block_size.saturating_mul(usize::from(kc));
    let mut total_size_bytes = 0u64;
    let mut pending = BytesMut::with_capacity(stripe_width.max(stripe_block_size));
    let remote_flush_threshold =
        N2_REMOTE_CHUNK_FLUSH_MIN_BYTES.max(stripe_block_size.saturating_mul(4));

    while let Some(chunk_result) = body_stream.next().await {
        let chunk = chunk_result?;
        if chunk.is_empty() {
            continue;
        }
        total_size_bytes = total_size_bytes.saturating_add(chunk.len() as u64);
        pending.extend_from_slice(&chunk);

        while pending.len() >= stripe_width {
            let stripe = pending.split_to(stripe_width);
            let shard_blocks = encode_cluster_stripe_blocks(
                stripe.as_ref(),
                usize::from(kc),
                usize::from(mc),
                stripe_block_size,
            )?;
            for session in &mut sessions {
                if session.is_failed() {
                    continue;
                }
                let cluster_idx = session.cluster_shard_index();
                let Some(block) = shard_blocks.get(cluster_idx) else {
                    session.mark_failed("missing encoded shard block".to_string());
                    continue;
                };
                if let Err(e) = append_coordinator_shard_data(
                    &http_client,
                    session,
                    block,
                    remote_flush_threshold,
                )
                .await
                {
                    warn!(
                        "n2 shard append failed fragment={} object_key={} version={} shard={} target_node={} err={}",
                        object.fragment,
                        object.object_key,
                        object.version_id,
                        session.cluster_shard_index(),
                        session.target().node_id,
                        e
                    );
                    session.mark_failed(e.to_string());
                }
            }
            let active_sessions = sessions.iter().filter(|s| !s.is_failed()).count();
            if active_sessions < required_commit_shards {
                for session in &mut sessions {
                    let _ = abort_coordinator_shard_session(&http_client, session).await;
                }
                return Err(AppError::Conflict(format!(
                    "n2 cluster commit policy {} dropped below the required active shard count during ingest: required {}, active {}",
                    commit_mode.as_str(),
                    required_commit_shards,
                    active_sessions
                )));
            }
        }
    }

    if !pending.is_empty() {
        let mut stripe = vec![0u8; stripe_width];
        stripe[..pending.len()].copy_from_slice(&pending);
        let shard_blocks = encode_cluster_stripe_blocks(
            stripe.as_slice(),
            usize::from(kc),
            usize::from(mc),
            stripe_block_size,
        )?;
        for session in &mut sessions {
            if session.is_failed() {
                continue;
            }
            let cluster_idx = session.cluster_shard_index();
            let Some(block) = shard_blocks.get(cluster_idx) else {
                session.mark_failed("missing encoded shard block".to_string());
                continue;
            };
            if let Err(e) =
                append_coordinator_shard_data(&http_client, session, block, remote_flush_threshold)
                    .await
            {
                warn!(
                    "n2 shard append failed fragment={} object_key={} version={} shard={} target_node={} err={}",
                    object.fragment,
                    object.object_key,
                    object.version_id,
                    session.cluster_shard_index(),
                    session.target().node_id,
                    e
                );
                session.mark_failed(e.to_string());
            }
        }
    }

    let mut committed = Vec::new();
    for session in &mut sessions {
        if session.is_failed() {
            let _ = abort_coordinator_shard_session(&http_client, session).await;
            continue;
        }
        if let Err(e) = flush_coordinator_shard_data(&http_client, session).await {
            warn!(
                "n2 shard flush failed fragment={} object_key={} version={} shard={} target_node={} err={}",
                object.fragment,
                object.object_key,
                object.version_id,
                session.cluster_shard_index(),
                session.target().node_id,
                e
            );
            session.mark_failed(e.to_string());
            let _ = abort_coordinator_shard_session(&http_client, session).await;
            continue;
        }

        match commit_coordinator_shard_session(
            &http_client,
            session,
            &object,
            kd,
            md,
            stripe_block_size,
        )
        .await
        {
            Ok(mut shard_ref) => {
                // Keep canonical checksum/size from streamed coordinator view when missing.
                match session {
                    CoordinatorShardSession::Local(v) => {
                        if shard_ref.size_bytes == 0 {
                            shard_ref.size_bytes = v.size_bytes;
                        }
                        if shard_ref.checksum.is_empty() {
                            shard_ref.checksum = hex::encode(v.checksum.clone().finalize());
                        }
                    }
                    CoordinatorShardSession::Remote(v) => {
                        if shard_ref.size_bytes == 0 {
                            shard_ref.size_bytes = v.size_bytes;
                        }
                        if shard_ref.checksum.is_empty() {
                            shard_ref.checksum = hex::encode(v.checksum.clone().finalize());
                        }
                    }
                }
                committed.push(shard_ref);
            }
            Err(e) => {
                warn!(
                    "n2 shard commit failed fragment={} object_key={} version={} shard={} target_node={} err={}",
                    object.fragment,
                    object.object_key,
                    object.version_id,
                    session.cluster_shard_index(),
                    session.target().node_id,
                    e
                );
                session.mark_failed(e.to_string());
                let _ = abort_coordinator_shard_session(&http_client, session).await;
            }
        }
    }

    if committed.len() < required_commit_shards {
        cleanup_committed_cluster_shards(&http_client, &object, &committed).await;
        return Err(AppError::Conflict(format!(
            "n2 cluster commit policy {} was not satisfied: required {} committed shards, got {} out of {} planned targets",
            commit_mode.as_str(),
            required_commit_shards,
            committed.len(),
            nc
        )));
    }

    committed.sort_by_key(|s| s.cluster_shard_index);

    let now = Utc::now().timestamp();
    let object_record = N2ObjectRecord {
        epoch: placement.epoch,
        object: object.clone(),
        params: CrossParams { kc, mc, kd, md },
        stripe_block_size,
        total_size_bytes,
        content_type,
        write_quorum: kc,
        created_at: now,
        committed_at: now,
        shards: committed,
    };

    if let Err(err) = persist_n2_object_record(&object_record).await {
        warn!(
            "n2 record persistence failed after shard commit fragment={} object_key={} version={} mode={} err={}",
            object.fragment,
            object.object_key,
            object.version_id,
            commit_mode.as_str(),
            err
        );
        cleanup_failed_n2_record(&http_client, &object_record).await;
        return Err(err);
    }

    info!(
        "n2 put committed fragment={} object_key={} version={} size={} quorum={} required_commit={} planned={} committed={} commit_mode={} membership_epoch={}",
        object.fragment,
        object.object_key,
        object.version_id,
        object_record.total_size_bytes,
        object_record.write_quorum,
        required_commit_shards,
        nc,
        object_record.shards.len(),
        commit_mode.as_str(),
        cluster_view.membership_epoch
    );

    Ok(AxumJson(N2PutObjectResponse {
        epoch: object_record.epoch,
        version_id,
        total_size_bytes: object_record.total_size_bytes,
        commit_mode,
        planned_shards: nc,
        committed_shards: object_record.shards.len(),
        required_quorum: object_record.write_quorum,
        required_commit_shards,
        object: object_record,
    }))
}

pub async fn get_n2_object(
    claims: Claims,
    headers: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<GetObjectQuery>,
) -> Result<impl IntoResponse, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }

    let record = load_n2_object_record(
        fragment.as_str(),
        object_key.as_str(),
        query.version_id.as_deref(),
    )
    .await?;

    let cfg = n2_runtime_config();
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;

    let kc = usize::from(record.params.kc);
    let mc = usize::from(record.params.mc);
    let mut shard_sources = Vec::new();
    for shard in &record.shards {
        let result = if is_local_target(&NodeDescriptor {
            node_id: shard.node_id.clone(),
            endpoint: shard.endpoint.clone(),
            epoch: record.epoch,
            active: true,
        }) {
            open_local_cluster_shard_stream(
                record.object.fragment.as_str(),
                record.object.version_id.as_str(),
                shard.cluster_shard_index,
            )
            .await
        } else {
            open_remote_cluster_shard_stream(
                &http_client,
                shard.endpoint.as_str(),
                record.object.fragment.as_str(),
                record.object.version_id.as_str(),
                shard.cluster_shard_index,
                shard.size_bytes as usize,
            )
            .await
        };

        match result {
            Ok(source) => shard_sources.push(source),
            Err(e) => warn!(
                "n2 shard read failed fragment={} object_key={} version={} shard={} node={} endpoint={} err={}",
                record.object.fragment,
                record.object.object_key,
                record.object.version_id,
                shard.cluster_shard_index,
                shard.node_id,
                shard.endpoint,
                e
            ),
        }
    }

    if shard_sources.len() < kc {
        return Err(AppError::InternalServerError(format!(
            "insufficient cluster shards available for read: need {}, have {}",
            kc,
            shard_sources.len()
        )));
    }

    let range = match parse_single_byte_range(&headers, record.total_size_bytes) {
        Ok(v) => v,
        Err(e) => return Err(e),
    };
    let (status, content_len, content_range_value) = if let Some((start, end)) = range {
        (
            StatusCode::PARTIAL_CONTENT,
            end.saturating_sub(start).saturating_add(1),
            Some(format!(
                "bytes {}-{}/{}",
                start, end, record.total_size_bytes
            )),
        )
    } else {
        (StatusCode::OK, record.total_size_bytes, None)
    };

    if kc == 1 && range.is_none() {
        if let Some(source_idx) = shard_sources
            .iter()
            .position(|source| source.cluster_shard_index() == 0)
        {
            let source = shard_sources.swap_remove(source_idx);
            let mut response = Response::new(Body::from_stream(
                source.into_byte_stream(record.total_size_bytes),
            ));
            *response.status_mut() = StatusCode::OK;
            let content_type_header = HeaderValue::from_str(record.content_type.as_str())
                .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
            response
                .headers_mut()
                .insert(CONTENT_TYPE, content_type_header);
            if let Ok(v) =
                HeaderValue::from_str(format!("\"{}\"", record.object.version_id).as_str())
            {
                response.headers_mut().insert(ETAG, v);
            }
            if let Ok(name) = axum::http::HeaderName::from_bytes(X_AMZ_VERSION_ID_HEADER.as_bytes())
            {
                if let Ok(v) = HeaderValue::from_str(record.object.version_id.as_str()) {
                    response.headers_mut().insert(name, v);
                }
            }
            response
                .headers_mut()
                .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            if let Ok(v) = HeaderValue::from_str(record.total_size_bytes.to_string().as_str()) {
                response.headers_mut().insert(CONTENT_LENGTH, v);
            }
            return Ok(response);
        }
    }

    let stream = reconstruct_object_stream_from_cluster_sources(
        kc,
        mc,
        record.stripe_block_size,
        record.total_size_bytes,
        shard_sources,
        range,
    );

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    let content_type_header = HeaderValue::from_str(record.content_type.as_str())
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, content_type_header);
    if let Ok(v) = HeaderValue::from_str(format!("\"{}\"", record.object.version_id).as_str()) {
        response.headers_mut().insert(ETAG, v);
    }
    if let Ok(name) = axum::http::HeaderName::from_bytes(X_AMZ_VERSION_ID_HEADER.as_bytes()) {
        if let Ok(v) = HeaderValue::from_str(record.object.version_id.as_str()) {
            response.headers_mut().insert(name, v);
        }
    }
    response
        .headers_mut()
        .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(v) = HeaderValue::from_str(content_len.to_string().as_str()) {
        response.headers_mut().insert(CONTENT_LENGTH, v);
    }
    if let Some(content_range_value) = content_range_value {
        if let Ok(v) = HeaderValue::from_str(content_range_value.as_str()) {
            response.headers_mut().insert(CONTENT_RANGE, v);
        }
    }
    Ok(response)
}

pub async fn list_n2_objects(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(query): Query<ListN2ObjectsQuery>,
) -> Result<AxumJson<N2ListObjectsResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }

    let limit = query.limit.unwrap_or(256).clamp(1, 10_000);
    let continuation = query
        .continuation
        .as_ref()
        .map(|v| v.trim().trim_matches('/').to_string())
        .filter(|v| !v.is_empty());
    let prefix = query
        .prefix
        .as_ref()
        .map(|v| v.trim().trim_start_matches('/').to_string())
        .filter(|v| !v.is_empty());
    let delimiter = query
        .delimiter
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let page = list_fragment_n2_namespace_page(
        fragment.as_str(),
        prefix.as_deref(),
        delimiter.as_deref(),
        limit,
        continuation.as_deref(),
    )
    .await?;

    let objects = page
        .records
        .into_iter()
        .map(|record| N2ObjectSummary {
            object_key: record.object.object_key,
            latest_version_id: record.object.version_id,
            epoch: record.epoch,
            total_size_bytes: record.total_size_bytes,
            content_type: record.content_type,
            committed_at: record.committed_at,
            shard_count: record.shards.len(),
        })
        .collect::<Vec<_>>();

    Ok(AxumJson(N2ListObjectsResponse {
        fragment,
        prefix,
        delimiter,
        limit,
        continuation: page.next_continuation,
        has_more: page.has_more,
        common_prefixes: page.common_prefixes,
        objects,
    }))
}

pub async fn list_n2_object_versions(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<ListN2VersionsQuery>,
) -> Result<AxumJson<N2ListObjectVersionsResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".to_string()));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }

    let key = object_key.trim().trim_matches('/').to_string();
    if key.is_empty() {
        return Err(AppError::BadRequest(
            "object_key cannot be empty".to_string(),
        ));
    }
    let limit = query.limit.unwrap_or(256).clamp(1, 10_000);
    let mut records = list_n2_records_for_object(fragment.as_str(), key.as_str(), 100_000).await?;
    records.truncate(limit);

    let versions = records
        .into_iter()
        .map(|record| N2ObjectVersionSummary {
            version_id: record.object.version_id,
            epoch: record.epoch,
            total_size_bytes: record.total_size_bytes,
            content_type: record.content_type,
            committed_at: record.committed_at,
            shard_count: record.shards.len(),
        })
        .collect::<Vec<_>>();

    Ok(AxumJson(N2ListObjectVersionsResponse {
        fragment,
        object_key: key,
        versions,
    }))
}

pub async fn delete_n2_object(
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    Query(query): Query<DeleteN2ObjectQuery>,
) -> Result<AxumJson<N2DeleteObjectResponse>, AppError> {
    if !claims.delete {
        return Err(AppError::Forbidden(
            "Delete permission required".to_string(),
        ));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }
    ensure_cluster_write_ready(query.expected_epoch).await?;

    let key = object_key.trim().trim_matches('/').to_string();
    if key.is_empty() {
        return Err(AppError::BadRequest(
            "object_key cannot be empty".to_string(),
        ));
    }

    let mut records = list_n2_records_for_object(fragment.as_str(), key.as_str(), 100_000).await?;
    if records.is_empty() {
        return Err(AppError::NotFound);
    }
    if let Some(version_id) = query.version_id.as_ref().map(|v| v.trim().to_string()) {
        records.retain(|m| m.object.version_id == version_id);
        if records.is_empty() {
            return Err(AppError::NotFound);
        }
    }

    let purge_shards = query.purge_shards.unwrap_or(true);
    let cfg = n2_runtime_config();
    let local_endpoint = normalize_endpoint(&cfg.node_endpoint);
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;

    let mut deleted_metadata_files = 0usize;
    let mut attempted_shard_deletes = 0usize;
    let mut deleted_shard_paths = 0usize;
    let mut shard_delete_failures = 0usize;

    for record in &records {
        deleted_metadata_files = deleted_metadata_files.saturating_add(
            delete_n2_object_record_local(
                record.object.fragment.as_str(),
                record.object.object_key.as_str(),
                record.object.version_id.as_str(),
            )
            .await?,
        );

        let mut remote_endpoints = record
            .shards
            .iter()
            .map(|s| s.endpoint.clone())
            .filter(|endpoint| normalize_endpoint(endpoint) != local_endpoint)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        remote_endpoints.sort();
        for endpoint in remote_endpoints {
            match delete_n2_object_record_remote(
                &http_client,
                endpoint.as_str(),
                record.object.fragment.as_str(),
                record.object.object_key.as_str(),
                record.object.version_id.as_str(),
            )
            .await
            {
                Ok(removed) => {
                    deleted_metadata_files = deleted_metadata_files.saturating_add(removed);
                }
                Err(err) => {
                    warn!(
                        "n2 remote object record delete failed fragment={} object_key={} version={} endpoint={} err={}",
                        record.object.fragment,
                        record.object.object_key,
                        record.object.version_id,
                        endpoint,
                        err
                    );
                }
            }
        }
    }

    if purge_shards {
        for record in &records {
            for shard in &record.shards {
                attempted_shard_deletes = attempted_shard_deletes.saturating_add(1);
                let target = NodeDescriptor {
                    node_id: shard.node_id.clone(),
                    endpoint: shard.endpoint.clone(),
                    epoch: record.epoch,
                    active: true,
                };
                let deleted = if is_local_target(&target) {
                    delete_local_cluster_shard(
                        record.object.fragment.as_str(),
                        record.object.version_id.as_str(),
                        shard.cluster_shard_index,
                    )
                    .await
                } else {
                    delete_remote_cluster_shard(
                        &http_client,
                        shard.endpoint.as_str(),
                        record.object.fragment.as_str(),
                        record.object.version_id.as_str(),
                        shard.cluster_shard_index,
                    )
                    .await
                };
                match deleted {
                    Ok(paths) => {
                        deleted_shard_paths = deleted_shard_paths.saturating_add(paths);
                    }
                    Err(err) => {
                        shard_delete_failures = shard_delete_failures.saturating_add(1);
                        warn!(
                            "n2 shard delete failed fragment={} object_key={} version={} shard={} node={} endpoint={} err={}",
                            record.object.fragment,
                            record.object.object_key,
                            record.object.version_id,
                            shard.cluster_shard_index,
                            shard.node_id,
                            shard.endpoint,
                            err
                        );
                    }
                }
            }
        }
    }

    Ok(AxumJson(N2DeleteObjectResponse {
        fragment,
        object_key: key,
        deleted_versions: records.len(),
        deleted_metadata_files,
        attempted_shard_deletes,
        deleted_shard_paths,
        shard_delete_failures,
    }))
}

pub async fn rename_n2_object(
    claims: Claims,
    AxumJson(mut request): AxumJson<RenameN2ObjectRequest>,
) -> Result<AxumJson<N2RenameObjectResponse>, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required".to_string(),
        ));
    }

    request.fragment = request.fragment.trim().to_string();
    request.source_object_key = request
        .source_object_key
        .trim()
        .trim_matches('/')
        .to_string();
    request.destination_object_key = request
        .destination_object_key
        .trim()
        .trim_matches('/')
        .to_string();

    if request.fragment.is_empty() {
        return Err(AppError::BadRequest("fragment cannot be empty".to_string()));
    }
    if request.source_object_key.is_empty() || request.destination_object_key.is_empty() {
        return Err(AppError::BadRequest(
            "source and destination object keys are required".to_string(),
        ));
    }
    if request.source_object_key == request.destination_object_key {
        return Err(AppError::BadRequest(
            "source and destination object keys must differ".to_string(),
        ));
    }

    if !is_admin_claim(&claims) && claims.fragment != request.fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }
    ensure_cluster_write_ready(request.expected_epoch).await?;

    let destination_exists = list_n2_records_for_object(
        request.fragment.as_str(),
        request.destination_object_key.as_str(),
        1,
    )
    .await?;
    if !destination_exists.is_empty() {
        return Err(AppError::Conflict(format!(
            "destination object already exists: {}",
            request.destination_object_key
        )));
    }

    let source_records = list_n2_records_for_object(
        request.fragment.as_str(),
        request.source_object_key.as_str(),
        100_000,
    )
    .await?;
    if source_records.is_empty() {
        return Err(AppError::NotFound);
    }

    let cfg = n2_runtime_config();
    let local_endpoint = normalize_endpoint(&cfg.node_endpoint);
    let moved_versions = source_records.len();
    let mut remote_endpoints = source_records
        .iter()
        .flat_map(|record| record.shards.iter().map(|s| s.endpoint.clone()))
        .filter(|endpoint| normalize_endpoint(endpoint) != local_endpoint)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    remote_endpoints.sort();

    rename_n2_namespace_entry(
        request.fragment.as_str(),
        request.source_object_key.as_str(),
        request.destination_object_key.as_str(),
        &remote_endpoints,
    )
    .await?;

    Ok(AxumJson(N2RenameObjectResponse {
        fragment: request.fragment,
        source_object_key: request.source_object_key,
        destination_object_key: request.destination_object_key,
        moved_versions,
        metadata_records_rewritten: 0,
    }))
}

async fn reconcile_n2_fragment_internal(
    fragment: &str,
    expected_epoch: Option<u64>,
    dry_run: bool,
    cleanup_orphans: bool,
    max_objects: usize,
) -> Result<N2ReconcileResponse, AppError> {
    let _repair_permit = if dry_run {
        None
    } else {
        Some(acquire_fragment_repair_permit(fragment).await?)
    };
    let cluster_view = ensure_cluster_write_ready(expected_epoch).await?;
    let cfg = n2_runtime_config();
    let records = list_fragment_n2_records(fragment, max_objects).await?;

    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;

    let mut results = Vec::with_capacity(records.len());
    let mut updated_objects = 0usize;
    let mut failed_objects = 0usize;

    for mut record in records {
        let old_epoch = record.epoch;
        let reconcile = async {
            let placement =
                resolve_placement(record.object.clone(), record.params.kc, record.params.mc).await?;
            let kc = usize::from(record.params.kc);
            let nc = usize::from(record.params.kc) + usize::from(record.params.mc);
            let previous_shards = record.shards.clone();
            let mut readable_shards = collect_readable_cluster_shard_refs(
                &http_client,
                record.object.fragment.as_str(),
                record.object.version_id.as_str(),
                &record.shards,
            )
            .await;

            if readable_shards.len() < kc {
                return Err(AppError::InternalServerError(format!(
                    "insufficient readable shards for reconcile: need {}, have {}",
                    kc,
                    readable_shards.len()
                )));
            }

            let mut next_shards = Vec::with_capacity(nc);
            let mut planned_shards = 0usize;
            let mut repaired_shards = 0usize;
            let mut cleaned_orphans = 0usize;
            let mut cleanup_failures = 0usize;

            for idx in 0..nc {
                let target = &placement.targets[idx];
                if let Some(existing) = record
                    .shards
                    .iter()
                    .find(|s| {
                        s.cluster_shard_index == idx
                            && shard_matches_target(s, target)
                            && readable_shards
                                .iter()
                                .any(|shard| shard.cluster_shard_index == idx)
                    })
                {
                    next_shards.push(existing.clone());
                    continue;
                }

                planned_shards = planned_shards.saturating_add(1);
                if dry_run {
                    continue;
                }

                let stored = store_cluster_shard_from_available_sources(
                    &http_client,
                    target,
                    &record.object,
                    idx,
                    record.params.kd,
                    record.params.md,
                    record.stripe_block_size,
                    usize::from(record.params.kc),
                    usize::from(record.params.mc),
                    &readable_shards,
                )
                .await?;

                next_shards.push(stored);
                readable_shards.push(next_shards.last().cloned().ok_or_else(|| {
                    AppError::InternalServerError("stored shard ref missing".to_string())
                })?);
                repaired_shards = repaired_shards.saturating_add(1);
            }

            if !dry_run {
                next_shards.sort_by_key(|s| s.cluster_shard_index);
                if next_shards.len() < kc {
                    return Err(AppError::InternalServerError(format!(
                        "reconcile produced insufficient shard refs: need {}, have {}",
                        kc,
                        next_shards.len()
                    )));
                }
                let stale_shards = previous_shards
                    .iter()
                    .filter(|old| {
                        !next_shards.iter().any(|next| {
                            old.cluster_shard_index == next.cluster_shard_index
                                && (old.node_id == next.node_id
                                    || normalize_endpoint(old.endpoint.as_str())
                                        == normalize_endpoint(next.endpoint.as_str()))
                        })
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                record.shards = next_shards;
                record.epoch = placement.epoch;
                record.committed_at = Utc::now().timestamp();
                persist_n2_object_record(&record).await?;

                if cleanup_orphans && !stale_shards.is_empty() {
                    for stale in stale_shards {
                        let target = NodeDescriptor {
                            node_id: stale.node_id.clone(),
                            endpoint: stale.endpoint.clone(),
                            epoch: record.epoch,
                            active: true,
                        };
                        let cleanup_result = if is_local_target(&target) {
                            delete_local_cluster_shard(
                                record.object.fragment.as_str(),
                                record.object.version_id.as_str(),
                                stale.cluster_shard_index,
                            )
                            .await
                        } else {
                            delete_remote_cluster_shard(
                                &http_client,
                                stale.endpoint.as_str(),
                                record.object.fragment.as_str(),
                                record.object.version_id.as_str(),
                                stale.cluster_shard_index,
                            )
                            .await
                        };

                        match cleanup_result {
                            Ok(paths) => {
                                cleaned_orphans = cleaned_orphans.saturating_add(paths);
                            }
                            Err(err) => {
                                cleanup_failures = cleanup_failures.saturating_add(1);
                                warn!(
                                    "n2 reconcile orphan cleanup failed fragment={} object_key={} version={} shard={} node={} endpoint={} err={}",
                                    record.object.fragment,
                                    record.object.object_key,
                                    record.object.version_id,
                                    stale.cluster_shard_index,
                                    stale.node_id,
                                    stale.endpoint,
                                    err
                                );
                            }
                        }
                    }
                }
            }

            Ok::<(usize, usize, usize, usize, u64), AppError>((
                planned_shards,
                repaired_shards,
                cleaned_orphans,
                cleanup_failures,
                placement.epoch,
            ))
        }
        .await;

        match reconcile {
            Ok((
                planned_shards,
                repaired_shards,
                cleaned_orphans,
                cleanup_failures,
                target_epoch,
            )) => {
                if planned_shards > 0 {
                    updated_objects = updated_objects.saturating_add(1);
                }
                results.push(N2ReconcileObjectResult {
                    object_key: record.object.object_key.clone(),
                    version_id: record.object.version_id.clone(),
                    old_epoch,
                    target_epoch,
                    planned_shards,
                    repaired_shards,
                    cleaned_orphans,
                    cleanup_failures,
                    status: if dry_run {
                        "planned".to_string()
                    } else {
                        "reconciled".to_string()
                    },
                    error: None,
                });
            }
            Err(err) => {
                failed_objects = failed_objects.saturating_add(1);
                warn!(
                    "n2 reconcile failed fragment={} object_key={} version={} err={}",
                    record.object.fragment, record.object.object_key, record.object.version_id, err
                );
                results.push(N2ReconcileObjectResult {
                    object_key: record.object.object_key.clone(),
                    version_id: record.object.version_id.clone(),
                    old_epoch,
                    target_epoch: old_epoch,
                    planned_shards: 0,
                    repaired_shards: 0,
                    cleaned_orphans: 0,
                    cleanup_failures: 0,
                    status: "failed".to_string(),
                    error: Some(err.to_string()),
                });
            }
        }
    }

    info!(
        "n2 reconcile completed fragment={} scanned={} updated={} failed={} dry_run={} membership_epoch={}",
        fragment,
        results.len(),
        updated_objects,
        failed_objects,
        dry_run,
        cluster_view.membership_epoch
    );

    Ok(N2ReconcileResponse {
        fragment: fragment.to_string(),
        dry_run,
        scanned_objects: results.len(),
        updated_objects,
        failed_objects,
        results,
    })
}

async fn run_n2_auto_maintenance_once(
    reason: &str,
    expected_epoch: u64,
    mode: N2AutoMaintenanceMode,
    max_fragments: usize,
    max_objects_per_fragment: usize,
    cleanup_orphans: bool,
) -> Result<AutoMaintenanceRunSummary, AppError> {
    if matches!(mode, N2AutoMaintenanceMode::Off) {
        return Ok(AutoMaintenanceRunSummary::default());
    }

    let dry_run = matches!(mode, N2AutoMaintenanceMode::Observe);
    let fragments = list_n2_fragments(max_fragments).await?;
    let mut summary = AutoMaintenanceRunSummary {
        dry_run,
        ..Default::default()
    };

    for fragment in fragments {
        if !dry_run {
            match inspect_fragment_health(fragment.as_str(), max_objects_per_fragment).await {
                Ok(health)
                    if health.needs_repair_objects == 0
                        && !health
                            .objects
                            .iter()
                            .any(|object| object.needs_redistribution) =>
                {
                    summary.fragments_scanned = summary.fragments_scanned.saturating_add(1);
                    continue;
                }
                Ok(_) => {}
                Err(err) => {
                    summary.fragments_scanned = summary.fragments_scanned.saturating_add(1);
                    summary.failed_fragments = summary.failed_fragments.saturating_add(1);
                    warn!(
                        "n2 auto maintenance health preflight failed reason={} fragment={} epoch={} mode={} err={}",
                        reason,
                        fragment,
                        expected_epoch,
                        mode.as_str(),
                        err
                    );
                    continue;
                }
            }
        }
        match reconcile_n2_fragment_internal(
            fragment.as_str(),
            Some(expected_epoch),
            dry_run,
            cleanup_orphans,
            max_objects_per_fragment,
        )
        .await
        {
            Ok(response) => {
                summary.fragments_scanned = summary.fragments_scanned.saturating_add(1);
                if response.updated_objects > 0 {
                    summary.fragments_with_planned_changes =
                        summary.fragments_with_planned_changes.saturating_add(1);
                }
                if response.failed_objects > 0 {
                    summary.failed_fragments = summary.failed_fragments.saturating_add(1);
                }
            }
            Err(AppError::Conflict(message))
                if message.contains("membership epoch mismatch")
                    || message.contains("stale n2 cluster view") =>
            {
                return Err(AppError::Conflict(message));
            }
            Err(err) => {
                summary.fragments_scanned = summary.fragments_scanned.saturating_add(1);
                summary.failed_fragments = summary.failed_fragments.saturating_add(1);
                warn!(
                    "n2 auto maintenance fragment failed reason={} fragment={} epoch={} mode={} err={}",
                    reason,
                    fragment,
                    expected_epoch,
                    mode.as_str(),
                    err
                );
            }
        }
    }

    Ok(summary)
}

pub fn schedule_n2_auto_maintenance(reason: &'static str, expected_epoch: u64) {
    if matches!(
        n2_runtime_config().auto_maintenance_mode,
        N2AutoMaintenanceMode::Off
    ) {
        return;
    }

    tokio::spawn(async move {
        let scheduler = auto_maintenance_scheduler();
        let should_start = {
            let mut state = scheduler.lock().await;
            state.pending_epoch = Some(
                state
                    .pending_epoch
                    .map_or(expected_epoch, |current| current.max(expected_epoch)),
            );
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };

        if !should_start {
            return;
        }

        loop {
            let epoch = {
                let mut state = scheduler.lock().await;
                match state.pending_epoch.take() {
                    Some(epoch) => epoch,
                    None => {
                        state.running = false;
                        return;
                    }
                }
            };

            let cfg = n2_runtime_config();
            match run_n2_auto_maintenance_once(
                reason,
                epoch,
                cfg.auto_maintenance_mode,
                cfg.auto_maintenance_max_fragments,
                cfg.auto_maintenance_max_objects_per_fragment,
                cfg.auto_maintenance_cleanup_orphans,
            )
            .await
            {
                Ok(summary) => {
                    info!(
                        "n2 auto maintenance completed reason={} epoch={} mode={} dry_run={} fragments_scanned={} fragments_with_changes={} failed_fragments={}",
                        reason,
                        epoch,
                        cfg.auto_maintenance_mode.as_str(),
                        summary.dry_run,
                        summary.fragments_scanned,
                        summary.fragments_with_planned_changes,
                        summary.failed_fragments
                    );
                }
                Err(err) => {
                    warn!(
                        "n2 auto maintenance failed reason={} epoch={} mode={} err={}",
                        reason,
                        epoch,
                        cfg.auto_maintenance_mode.as_str(),
                        err
                    );
                }
            }
        }
    });
}

pub fn start_n2_auto_maintenance_loop() {
    if matches!(
        n2_runtime_config().auto_maintenance_mode,
        N2AutoMaintenanceMode::Off
    ) {
        return;
    }

    tokio::spawn(async move {
        loop {
            let interval =
                Duration::from_secs(n2_runtime_config().auto_maintenance_interval_seconds.max(5));
            sleep(interval).await;
            match crate::distribution_core::state::get_membership().await {
                Ok(membership) => {
                    schedule_n2_auto_maintenance("periodic", membership.epoch);
                }
                Err(err) => {
                    warn!("n2 periodic auto maintenance skipped: {}", err);
                }
            }
        }
    });
}

pub async fn reconcile_n2_fragment(
    claims: Claims,
    Path(fragment): Path<String>,
    Query(query): Query<ReconcileFragmentQuery>,
) -> Result<AxumJson<N2ReconcileResponse>, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required".to_string(),
        ));
    }
    if !is_admin_claim(&claims) && claims.fragment != fragment {
        return Err(AppError::Forbidden(
            "Access denied to this fragment".to_string(),
        ));
    }

    Ok(AxumJson(
        reconcile_n2_fragment_internal(
            fragment.as_str(),
            query.expected_epoch,
            query.dry_run.unwrap_or(false),
            query.cleanup_orphans.unwrap_or(true),
            query.max_objects.unwrap_or(128).clamp(1, 10_000),
        )
        .await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::disk_manager::{BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS};
    use crate::distribution_core::codec::reconstruct_object_from_cluster_shards;
    use crate::distribution_core::health::{
        get_n2_cluster_health, get_n2_fragment_health, get_n2_internal_node_health,
        get_n2_internal_shard_health, get_n2_node_health, get_n2_object_health, repair_n2_object,
    };
    use crate::distribution_core::model::{
        ClusterCommitMode, ClusterHealthQuery, DeleteN2ObjectQuery, FragmentHealthQuery,
        GetObjectQuery, InternalStoreShardQuery, ListN2ObjectsQuery, ListN2VersionsQuery,
        N2HealthLevel, ObjectHealthQuery, PutObjectQuery, ReconcileFragmentQuery,
        RenameN2ObjectRequest, RepairN2ObjectQuery, UpsertMembershipRequest,
    };
    use crate::distribution_core::nmtd::{LOCAL_CLUSTER_SHARD_METADATA_FILE, record_path_for_root};
    use crate::distribution_core::object_index::load_n2_object_record_local;
    use crate::distribution_core::peer::{
        INTERNAL_TOKEN_HEADER, InternalObjectRecordReadQuery, InternalShardAbortResponse,
        InternalShardChunkAck, InternalShardSessionResponse, normalize_endpoint,
    };
    use crate::distribution_core::peer_api::{
        n2_internal_abort_shard, n2_internal_begin_shard, n2_internal_commit_shard,
        n2_internal_delete_object_record, n2_internal_delete_shard, n2_internal_read_object_record,
        n2_internal_read_shard, n2_internal_store_shard, n2_internal_upsert_object_record,
        n2_internal_write_shard_chunk,
    };
    use crate::distribution_core::state::{
        N2_CLUSTER_FINGERPRINT_HEADER, N2_EXPECTED_EPOCH_HEADER,
        N2_TEST_CLUSTER_LOCK as STORAGE_TEST_LOCK, current_cluster_epoch_and_fingerprint,
        get_membership, upsert_membership,
    };
    use crate::structs::auth_structs::Claims;
    use axum::Router;
    use axum::body::to_bytes;
    use axum::extract::{Path as AxumPath, Query as AxumQuery};
    use axum::http::{HeaderMap, HeaderValue, StatusCode, header, header::RANGE};
    use axum::response::IntoResponse;
    use axum::routing::{get, post, put};
    use simd_erasure_core::ReedSolomonEncoder;
    use std::path::Path as StdPath;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    fn full_claims(fragment: &str) -> Claims {
        Claims {
            fragment: fragment.to_string(),
            read: true,
            write: true,
            modify: true,
            delete: true,
            exp: usize::MAX,
        }
    }

    async fn internal_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            INTERNAL_TOKEN_HEADER,
            HeaderValue::from_str(n2_runtime_config().internal_token.as_str())
                .expect("internal token header"),
        );
        let (epoch, cluster_fingerprint) = current_cluster_epoch_and_fingerprint()
            .await
            .expect("cluster proof");
        headers.insert(
            N2_EXPECTED_EPOCH_HEADER,
            HeaderValue::from_str(epoch.to_string().as_str()).expect("expected epoch header"),
        );
        headers.insert(
            N2_CLUSTER_FINGERPRINT_HEADER,
            HeaderValue::from_str(cluster_fingerprint.as_str())
                .expect("cluster fingerprint header"),
        );
        headers
    }

    fn local_node_descriptor() -> NodeDescriptor {
        let cfg = n2_runtime_config();
        NodeDescriptor {
            node_id: cfg.node_id.clone(),
            endpoint: cfg.node_endpoint.clone(),
            epoch: 0,
            active: true,
        }
    }

    async fn ensure_n2_test_storage_roots() {
        // Keep the same global test root convention used by workflow matrix tests so
        // OnceLock-backed disk roots remain coherent across the full test process.
        let base = std::env::temp_dir().join(format!("n1-matrix-{}", std::process::id()));
        let index_1 = base.join("index_1");
        let index_2 = base.join("index_2");
        let blob_1 = base.join("blob_1");
        let blob_2 = base.join("blob_2");
        let blob_3 = base.join("blob_3");
        for p in [&index_1, &index_2, &blob_1, &blob_2, &blob_3] {
            let _ = fs::create_dir_all(p).await;
        }

        if INDEX_STORE_DISKS.get().is_none() {
            INDEX_STORE_DISKS
                .set(vec![
                    index_1.to_string_lossy().to_string(),
                    index_2.to_string_lossy().to_string(),
                ])
                .expect("set INDEX_STORE_DISKS");
        }
        if BLOB_STORE_DISKS.get().is_none() {
            BLOB_STORE_DISKS
                .set(vec![
                    blob_1.to_string_lossy().to_string(),
                    blob_2.to_string_lossy().to_string(),
                    blob_3.to_string_lossy().to_string(),
                ])
                .expect("set BLOB_STORE_DISKS");
        }
        if FS_STORAGE_ROOT.get().is_none() {
            FS_STORAGE_ROOT
                .set(base.to_string_lossy().to_string())
                .expect("set FS_STORAGE_ROOT");
        }
    }

    async fn reset_n2_test_storage_contents() {
        if let Some(roots) = INDEX_STORE_DISKS.get() {
            for root in roots {
                let path = std::path::Path::new(root);
                let _ = fs::remove_dir_all(path).await;
                let _ = fs::create_dir_all(path).await;
            }
        }
        if let Some(roots) = BLOB_STORE_DISKS.get() {
            for root in roots {
                let path = std::path::Path::new(root);
                let _ = fs::remove_dir_all(path).await;
                let _ = fs::create_dir_all(path).await;
            }
        }
    }

    async fn spawn_internal_session_server() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route(
                "/n2/internal/session/begin/{fragment}/{version_id}/{cluster_shard_index}",
                post(n2_internal_begin_shard),
            )
            .route(
                "/n2/internal/session/chunk/{session_id}",
                put(n2_internal_write_shard_chunk),
            )
            .route(
                "/n2/internal/session/commit/{session_id}",
                post(n2_internal_commit_shard),
            )
            .route(
                "/n2/internal/session/abort/{session_id}",
                post(n2_internal_abort_shard),
            )
            .route(
                "/n2/internal/read/{fragment}/{version_id}/{cluster_shard_index}",
                get(n2_internal_read_shard),
            )
            .route(
                "/n2/internal/store/{fragment}/{version_id}/{cluster_shard_index}",
                post(n2_internal_store_shard),
            )
            .route(
                "/n2/internal/delete/{fragment}/{version_id}/{cluster_shard_index}",
                axum::routing::delete(n2_internal_delete_shard),
            )
            .route(
                "/n2/internal/object-record/upsert",
                post(n2_internal_upsert_object_record),
            )
            .route(
                "/n2/internal/object-record/{fragment}",
                get(n2_internal_read_object_record),
            )
            .route(
                "/n2/internal/object-record/{fragment}",
                axum::routing::delete(n2_internal_delete_object_record),
            )
            .route("/n2/internal/health/node", get(get_n2_internal_node_health))
            .route(
                "/n2/internal/health/shard/{fragment}/{version_id}/{cluster_shard_index}",
                get(get_n2_internal_shard_health),
            );
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind internal session server");
        let addr = listener.local_addr().expect("local addr");
        let join = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve internal routes");
        });
        (format!("http://{}", addr), join)
    }

    async fn spawn_commit_failure_session_server() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route(
                "/n2/internal/session/begin/{fragment}/{version_id}/{cluster_shard_index}",
                post(
                    |AxumPath((_fragment, _version_id, _cluster_shard_index)): AxumPath<(
                        String,
                        String,
                        usize,
                    )>| async move {
                        Ok::<AxumJson<InternalShardSessionResponse>, StatusCode>(AxumJson(
                            InternalShardSessionResponse {
                                session_id: Ulid::new().to_string(),
                            },
                        ))
                    },
                ),
            )
            .route(
                "/n2/internal/session/chunk/{session_id}",
                put(|AxumPath(session_id): AxumPath<String>| async move {
                    Ok::<AxumJson<InternalShardChunkAck>, StatusCode>(AxumJson(
                        InternalShardChunkAck {
                            session_id,
                            written_bytes: 0,
                        },
                    ))
                }),
            )
            .route(
                "/n2/internal/session/commit/{session_id}",
                post(|| async move {
                    Err::<AxumJson<N2ClusterShardRef>, StatusCode>(
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )
                }),
            )
            .route(
                "/n2/internal/session/abort/{session_id}",
                post(|AxumPath(session_id): AxumPath<String>| async move {
                    Ok::<AxumJson<InternalShardAbortResponse>, StatusCode>(AxumJson(
                        InternalShardAbortResponse {
                            session_id,
                            aborted: true,
                        },
                    ))
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind commit failure server");
        let addr = listener.local_addr().expect("local addr");
        let join = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve commit failure routes");
        });
        (format!("http://{}", addr), join)
    }

    fn ensure_auth_env_for_tests() {
        // SAFETY: test-only setup before handlers touch these env-backed LazyLocks.
        unsafe {
            if std::env::var("ADMIN_KEY").is_err() {
                std::env::set_var("ADMIN_KEY", "admin");
            }
            if std::env::var("MASTER_KEY").is_err() {
                std::env::set_var("MASTER_KEY", "master");
            }
            if std::env::var("AUTH_KEY").is_err() {
                std::env::set_var("AUTH_KEY", "auth");
            }
            if std::env::var("JWT_SECRET").is_err() {
                std::env::set_var("JWT_SECRET", "jwt-secret-for-tests");
            }
        }
    }

    fn sample_bytes(len: usize, seed: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        for (i, b) in out.iter_mut().enumerate() {
            *b = ((i + seed) % 251) as u8;
        }
        out
    }

    #[test]
    fn reconstruct_roundtrip_with_kc2_mc1_missing_one_data_shard() {
        let kc = 2usize;
        let mc = 1usize;
        let block_size = 4096usize;
        let data = sample_bytes(15_777, 17);

        let stripe_data_width = kc * block_size;
        let stripe_count = data.len().div_ceil(stripe_data_width);
        let shard_size = stripe_count * block_size;
        let mut shards = vec![
            vec![0u8; shard_size],
            vec![0u8; shard_size],
            vec![0u8; shard_size],
        ];

        for stripe_idx in 0..stripe_count {
            let start = stripe_idx * stripe_data_width;
            let end = (start + stripe_data_width).min(data.len());
            let mut stripe = vec![0u8; stripe_data_width];
            stripe[..end - start].copy_from_slice(&data[start..end]);

            let mut enc = ReedSolomonEncoder::new(kc, mc, block_size).expect("encoder");
            enc.add_original_shard(&stripe[0..block_size])
                .expect("add shard0");
            enc.add_original_shard(&stripe[block_size..2 * block_size])
                .expect("add shard1");
            let encoded = enc.encode().expect("encode");

            let shard_off = stripe_idx * block_size;
            shards[0][shard_off..shard_off + block_size].copy_from_slice(&stripe[0..block_size]);
            shards[1][shard_off..shard_off + block_size]
                .copy_from_slice(&stripe[block_size..2 * block_size]);
            shards[2][shard_off..shard_off + block_size]
                .copy_from_slice(encoded.recovery(0).expect("parity"));
        }

        let reconstructed = reconstruct_object_from_cluster_shards(
            kc,
            mc,
            block_size,
            data.len() as u64,
            vec![(1, shards[1].clone()), (2, shards[2].clone())],
        )
        .expect("reconstruct");
        assert_eq!(reconstructed, data);
    }

    #[test]
    fn reconstruct_fails_when_available_shards_below_quorum() {
        let err =
            reconstruct_object_from_cluster_shards(2, 1, 4096, 10, vec![(0, vec![0u8; 4096])])
                .expect_err("should fail");
        match err {
            AppError::InternalServerError(_) => {}
            _ => panic!("expected InternalServerError"),
        }
    }

    #[test]
    fn reconstruct_cluster_shard_recovers_missing_data_shard() {
        let kc = 2usize;
        let mc = 1usize;
        let block_size = 4096usize;
        let data = sample_bytes(23_777, 7);

        let stripe_data_width = kc * block_size;
        let stripe_count = data.len().div_ceil(stripe_data_width);
        let shard_size = stripe_count * block_size;
        let mut shards = vec![
            vec![0u8; shard_size],
            vec![0u8; shard_size],
            vec![0u8; shard_size],
        ];

        for stripe_idx in 0..stripe_count {
            let start = stripe_idx * stripe_data_width;
            let end = (start + stripe_data_width).min(data.len());
            let mut stripe = vec![0u8; stripe_data_width];
            stripe[..end - start].copy_from_slice(&data[start..end]);

            let mut enc = ReedSolomonEncoder::new(kc, mc, block_size).expect("encoder");
            enc.add_original_shard(&stripe[0..block_size])
                .expect("add shard0");
            enc.add_original_shard(&stripe[block_size..2 * block_size])
                .expect("add shard1");
            let encoded = enc.encode().expect("encode");

            let shard_off = stripe_idx * block_size;
            shards[0][shard_off..shard_off + block_size].copy_from_slice(&stripe[0..block_size]);
            shards[1][shard_off..shard_off + block_size]
                .copy_from_slice(&stripe[block_size..2 * block_size]);
            shards[2][shard_off..shard_off + block_size]
                .copy_from_slice(encoded.recovery(0).expect("parity"));
        }

        let mut available = HashMap::new();
        available.insert(1usize, shards[1].clone());
        available.insert(2usize, shards[2].clone());

        let rebuilt = reconstruct_cluster_shard_from_available(kc, mc, block_size, 0, &available)
            .expect("rebuild data shard");
        assert_eq!(rebuilt, shards[0]);
    }

    #[test]
    fn reconstruct_cluster_shard_recovers_parity_shard() {
        let kc = 2usize;
        let mc = 1usize;
        let block_size = 4096usize;
        let data = sample_bytes(11_900, 29);

        let stripe_data_width = kc * block_size;
        let stripe_count = data.len().div_ceil(stripe_data_width);
        let shard_size = stripe_count * block_size;
        let mut shards = vec![
            vec![0u8; shard_size],
            vec![0u8; shard_size],
            vec![0u8; shard_size],
        ];

        for stripe_idx in 0..stripe_count {
            let start = stripe_idx * stripe_data_width;
            let end = (start + stripe_data_width).min(data.len());
            let mut stripe = vec![0u8; stripe_data_width];
            stripe[..end - start].copy_from_slice(&data[start..end]);

            let mut enc = ReedSolomonEncoder::new(kc, mc, block_size).expect("encoder");
            enc.add_original_shard(&stripe[0..block_size])
                .expect("add shard0");
            enc.add_original_shard(&stripe[block_size..2 * block_size])
                .expect("add shard1");
            let encoded = enc.encode().expect("encode");

            let shard_off = stripe_idx * block_size;
            shards[0][shard_off..shard_off + block_size].copy_from_slice(&stripe[0..block_size]);
            shards[1][shard_off..shard_off + block_size]
                .copy_from_slice(&stripe[block_size..2 * block_size]);
            shards[2][shard_off..shard_off + block_size]
                .copy_from_slice(encoded.recovery(0).expect("parity"));
        }

        let mut available = HashMap::new();
        available.insert(0usize, shards[0].clone());
        available.insert(1usize, shards[1].clone());

        let rebuilt = reconstruct_cluster_shard_from_available(kc, mc, block_size, 2, &available)
            .expect("rebuild parity shard");
        assert_eq!(rebuilt, shards[2]);
    }

    #[test]
    fn encode_cluster_stripe_blocks_produces_expected_count_and_size() {
        let kc = 2usize;
        let mc = 1usize;
        let block_size = 4096usize;
        let mut stripe = vec![0u8; kc * block_size];
        for (idx, b) in stripe.iter_mut().enumerate() {
            *b = (idx % 251) as u8;
        }

        let blocks =
            encode_cluster_stripe_blocks(stripe.as_slice(), kc, mc, block_size).expect("encode");
        assert_eq!(blocks.len(), kc + mc);
        assert!(blocks.iter().all(|b| b.len() == block_size));
        assert_eq!(blocks[0].as_slice(), &stripe[0..block_size]);
        assert_eq!(blocks[1].as_slice(), &stripe[block_size..2 * block_size]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn internal_session_begin_chunk_commit_read_roundtrip() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let fragment = format!("frag-{}", Ulid::new());
        let version_id = Ulid::new().to_string();
        let chunk_a = sample_bytes(5_000, 3);
        let chunk_b = sample_bytes(7_000, 9);
        let mut expected = chunk_a.clone();
        expected.extend_from_slice(&chunk_b);

        let begin = n2_internal_begin_shard(
            internal_headers().await,
            AxumPath((fragment.clone(), version_id.clone(), 0usize)),
            AxumQuery(InternalStoreShardQuery {
                uid: Some(fragment.clone()),
                object_id: Some("session-roundtrip-object".to_string()),
                object_key: Some("videos/session-roundtrip.bin".to_string()),
                kd: 1,
                md: 0,
                stripe_block_size: 4096,
            }),
        )
        .await
        .expect("begin session");
        let session_id = begin.0.session_id.clone();

        let _ = n2_internal_write_shard_chunk(
            internal_headers().await,
            AxumPath(session_id.clone()),
            Body::from(chunk_a),
        )
        .await
        .expect("write chunk a");
        let _ = n2_internal_write_shard_chunk(
            internal_headers().await,
            AxumPath(session_id.clone()),
            Body::from(chunk_b),
        )
        .await
        .expect("write chunk b");

        let committed =
            n2_internal_commit_shard(internal_headers().await, AxumPath(session_id.clone()))
                .await
                .expect("commit");
        assert_eq!(committed.0.cluster_shard_index, 0);
        assert_eq!(committed.0.size_bytes as usize, expected.len());

        let read_resp = n2_internal_read_shard(
            internal_headers().await,
            AxumPath((fragment.clone(), version_id.clone(), 0usize)),
        )
        .await
        .expect("read shard")
        .into_response();
        let read_bytes = to_bytes(read_resp.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert_eq!(read_bytes.as_ref(), expected.as_slice());

        let after_commit = n2_internal_write_shard_chunk(
            internal_headers().await,
            AxumPath(session_id),
            Body::from(vec![1u8; 32]),
        )
        .await;
        assert!(
            matches!(after_commit, Err(AppError::NotFound)),
            "session should be closed after commit"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn internal_session_abort_rejects_further_writes_and_commit() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let fragment = format!("frag-{}", Ulid::new());
        let version_id = Ulid::new().to_string();
        let begin = n2_internal_begin_shard(
            internal_headers().await,
            AxumPath((fragment, version_id, 0usize)),
            AxumQuery(InternalStoreShardQuery {
                uid: None,
                object_id: Some("session-abort-object".to_string()),
                object_key: Some("videos/session-abort.bin".to_string()),
                kd: 1,
                md: 0,
                stripe_block_size: 4096,
            }),
        )
        .await
        .expect("begin session");
        let session_id = begin.0.session_id.clone();

        let _ = n2_internal_write_shard_chunk(
            internal_headers().await,
            AxumPath(session_id.clone()),
            Body::from(vec![9u8; 2048]),
        )
        .await
        .expect("write chunk before abort");

        let _ = n2_internal_abort_shard(internal_headers().await, AxumPath(session_id.clone()))
            .await
            .expect("abort");

        let chunk_after_abort = n2_internal_write_shard_chunk(
            internal_headers().await,
            AxumPath(session_id.clone()),
            Body::from(vec![1u8; 128]),
        )
        .await;
        assert!(matches!(chunk_after_abort, Err(AppError::NotFound)));

        let commit_after_abort =
            n2_internal_commit_shard(internal_headers().await, AxumPath(session_id)).await;
        assert!(matches!(commit_after_abort, Err(AppError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn internal_session_requires_token() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let result = n2_internal_begin_shard(
            HeaderMap::new(),
            AxumPath((
                format!("frag-{}", Ulid::new()),
                Ulid::new().to_string(),
                0usize,
            )),
            AxumQuery(InternalStoreShardQuery {
                uid: None,
                object_id: None,
                object_key: None,
                kd: 1,
                md: 0,
                stripe_block_size: 4096,
            }),
        )
        .await;
        assert!(matches!(result, Err(AppError::Forbidden(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn internal_session_requires_cluster_view_proof() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let mut headers = HeaderMap::new();
        headers.insert(
            INTERNAL_TOKEN_HEADER,
            HeaderValue::from_str(n2_runtime_config().internal_token.as_str())
                .expect("internal token header"),
        );

        let result = n2_internal_begin_shard(
            headers,
            AxumPath((
                format!("frag-{}", Ulid::new()),
                Ulid::new().to_string(),
                0usize,
            )),
            AxumQuery(InternalStoreShardQuery {
                uid: None,
                object_id: None,
                object_key: None,
                kd: 1,
                md: 0,
                stripe_block_size: 4096,
            }),
        )
        .await;

        match result {
            Err(AppError::Conflict(message)) => {
                assert!(message.contains("expected epoch") || message.contains("fingerprint"));
            }
            other => panic!("expected cluster proof conflict, got {:?}", other),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn put_get_n2_object_remote_session_pipeline_roundtrip() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let (remote_endpoint, remote_server) = spawn_internal_session_server().await;
        let previous_membership = get_membership().await.expect("current membership");
        let cfg = n2_runtime_config();

        let remote_node = NodeDescriptor {
            node_id: format!("remote-{}", Ulid::new()),
            endpoint: remote_endpoint.clone(),
            epoch: 0,
            active: true,
        };
        let local_node = NodeDescriptor {
            node_id: cfg.node_id.clone(),
            endpoint: cfg.node_endpoint.clone(),
            epoch: 0,
            active: true,
        };
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node, remote_node],
        })
        .await
        .expect("set membership with local+remote nodes");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/remote-session-path.bin".to_string();
        let payload = sample_bytes(2_600_000, 41);
        let mut put_headers = HeaderMap::new();
        put_headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        let put_result = put_n2_object(
            full_claims(fragment.as_str()),
            put_headers,
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(2),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(256 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put n2 object");
        assert_eq!(
            put_result.0.commit_mode,
            ClusterCommitMode::StrictFullPlacement
        );
        assert_eq!(put_result.0.planned_shards, 2);
        assert_eq!(put_result.0.required_quorum, 2);
        assert_eq!(put_result.0.required_commit_shards, 2);
        assert_eq!(put_result.0.committed_shards, 2);

        let index_roots = INDEX_STORE_DISKS.get().expect("index roots initialized");
        let mut nmtd_mirrors = 0usize;
        for root in index_roots {
            let root = StdPath::new(root);
            let nmtd_path = record_path_for_root(
                root,
                fragment.as_str(),
                put_result.0.object.object.object_id.as_str(),
                put_result.0.version_id.as_str(),
            );
            if fs::try_exists(&nmtd_path).await.expect("check .nmtd") {
                nmtd_mirrors = nmtd_mirrors.saturating_add(1);
            }
        }
        assert!(
            nmtd_mirrors > 0,
            "N2 object metadata must be written as .nmtd"
        );

        let blob_roots = BLOB_STORE_DISKS.get().expect("blob roots initialized");
        let mut local_shard_nmtd = 0usize;
        for root in blob_roots {
            let version_dir = StdPath::new(root)
                .join(fragment.as_str())
                .join("n2")
                .join("shards")
                .join(put_result.0.version_id.as_str());
            if let Ok(mut entries) = fs::read_dir(&version_dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let marker = entry.path().join(LOCAL_CLUSTER_SHARD_METADATA_FILE);
                    if fs::try_exists(&marker).await.unwrap_or(false) {
                        local_shard_nmtd = local_shard_nmtd.saturating_add(1);
                    }
                }
            }
        }
        assert!(
            local_shard_nmtd > 0,
            "N2 local cluster shard metadata must be written as .nmtd"
        );

        let get_resp = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put_result.0.version_id.clone()),
            }),
        )
        .await
        .expect("get n2 object")
        .into_response();
        let got = to_bytes(get_resp.into_body(), usize::MAX)
            .await
            .expect("read get body");
        assert_eq!(got.as_ref(), payload.as_slice());

        // Restore membership and stop remote server.
        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
        remote_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_n2_object_rejects_stale_expected_epoch() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        let current = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node_descriptor()],
        })
        .await
        .expect("set single-node membership");
        let stale_epoch = current.epoch.saturating_sub(1);

        let result = put_n2_object(
            full_claims("frag-stale"),
            HeaderMap::new(),
            AxumPath((
                "frag-stale".to_string(),
                "videos/stale-epoch.bin".to_string(),
            )),
            AxumQuery(PutObjectQuery {
                uid: Some("frag-stale".to_string()),
                version_id: None,
                expected_epoch: Some(stale_epoch),
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(64 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(sample_bytes(32_000, 1)),
        )
        .await;
        assert!(matches!(result, Err(AppError::Conflict(_))));

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_strict_full_placement_rejects_partial_commit_and_cleans_local_shards() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let (remote_endpoint, remote_server) = spawn_commit_failure_session_server().await;
        let previous_membership = get_membership().await.expect("current membership");
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node_descriptor(),
                NodeDescriptor {
                    node_id: format!("remote-fail-{}", Ulid::new()),
                    endpoint: remote_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("set two-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/strict-fail.bin".to_string();
        let version_id = Ulid::new().to_string();
        let result = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: Some(version_id.clone()),
                expected_epoch: None,
                kc: Some(1),
                mc: Some(1),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(128 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(sample_bytes(600_000, 211)),
        )
        .await;

        match result {
            Err(AppError::Conflict(message)) => {
                assert!(message.contains("strict_full_placement"));
                assert!(message.contains("required 2"));
            }
            other => panic!("expected strict placement conflict, got {:?}", other),
        }

        let record_result = load_n2_object_record_local(
            fragment.as_str(),
            object_key.as_str(),
            Some(version_id.as_str()),
        )
        .await;
        assert!(
            matches!(record_result, Err(AppError::NotFound)),
            "strict failure must not leave an authoritative record behind"
        );

        for cluster_shard_index in 0..2 {
            let shard_result = read_local_cluster_shard(
                fragment.as_str(),
                version_id.as_str(),
                cluster_shard_index,
            )
            .await;
            assert!(
                matches!(shard_result, Err(AppError::NotFound)),
                "strict failure must clean local shard residue for cluster_shard_index={cluster_shard_index}"
            );
        }

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
        remote_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_n2_object_honors_single_range_requests() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node_descriptor()],
        })
        .await
        .expect("set single-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/range-check.bin".to_string();
        let payload = sample_bytes(900_000, 23);
        let version_id = Ulid::new().to_string();
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: Some(version_id.clone()),
                expected_epoch: None,
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(128 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put object");
        assert_eq!(put.0.version_id, version_id);

        let mut headers = HeaderMap::new();
        headers.insert(RANGE, HeaderValue::from_static("bytes=100-999"));
        let response = get_n2_object(
            full_claims(fragment.as_str()),
            headers,
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(version_id.clone()),
            }),
        )
        .await
        .expect("get range")
        .into_response();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|v| v.to_str().ok()),
            Some(format!("bytes 100-999/{}", payload.len()).as_str())
        );
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("range bytes");
        assert_eq!(bytes.as_ref(), &payload[100..1000]);

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_record_read_falls_back_to_remote_replica_when_local_records_are_missing() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node_descriptor()],
        })
        .await
        .expect("set single-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/record-fallback.bin".to_string();
        let payload = sample_bytes(1_200_000, 99);
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(256 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put object");

        let remote_record = Arc::new(put.0.object.clone());
        let internal_token = n2_runtime_config().internal_token.clone();
        let app = Router::new().route(
            "/n2/internal/object-record/{fragment}",
            get({
                let remote_record = Arc::clone(&remote_record);
                let internal_token = internal_token.clone();
                move |headers: HeaderMap,
                      AxumPath(route_fragment): AxumPath<String>,
                      AxumQuery(query): AxumQuery<InternalObjectRecordReadQuery>| {
                    let remote_record = Arc::clone(&remote_record);
                    let internal_token = internal_token.clone();
                    async move {
                        let token_ok = headers
                            .get(INTERNAL_TOKEN_HEADER)
                            .and_then(|v| v.to_str().ok())
                            .map(|v| v == internal_token.as_str())
                            .unwrap_or(false);
                        if !token_ok {
                            return Err(StatusCode::FORBIDDEN);
                        }
                        if route_fragment != remote_record.object.fragment
                            || query.object_key.trim() != remote_record.object.object_key
                        {
                            return Err(StatusCode::NOT_FOUND);
                        }
                        if let Some(version_id) = query.version_id.as_ref() {
                            if version_id.trim() != remote_record.object.version_id {
                                return Err(StatusCode::NOT_FOUND);
                            }
                        }
                        Ok::<AxumJson<N2ObjectRecord>, StatusCode>(AxumJson(
                            (*remote_record).clone(),
                        ))
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind record fallback server");
        let record_endpoint = format!(
            "http://{}",
            listener.local_addr().expect("record server addr")
        );
        let record_server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve record fallback server");
        });

        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node_descriptor(),
                NodeDescriptor {
                    node_id: format!("record-replica-{}", Ulid::new()),
                    endpoint: record_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("set membership with record replica endpoint");

        let removed = delete_n2_object_record_local(
            fragment.as_str(),
            object_key.as_str(),
            put.0.version_id.as_str(),
        )
        .await
        .expect("delete local record copies");
        assert!(removed > 0);

        let response = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("get object via remote record fallback")
        .into_response();
        let got = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read get body");
        assert_eq!(got.as_ref(), payload.as_slice());

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
        record_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_object_api_parity_list_versions_rename_and_delete() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node_descriptor()],
        })
        .await
        .expect("set single-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let source_key = "docs/report.bin".to_string();
        let destination_key = "archive/report.bin".to_string();
        let v1 = Ulid::new().to_string();
        let v2 = Ulid::new().to_string();

        let _ = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), source_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: Some(v1.clone()),
                expected_epoch: None,
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(128 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(sample_bytes(250_000, 31)),
        )
        .await
        .expect("put version 1");

        let _ = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), source_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: Some(v2.clone()),
                expected_epoch: None,
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(128 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(sample_bytes(260_000, 32)),
        )
        .await
        .expect("put version 2");

        let source_v1_record =
            load_n2_object_record_local(fragment.as_str(), source_key.as_str(), Some(v1.as_str()))
                .await
                .expect("load source v1 record");
        let source_object_id = source_v1_record.object.object_id.clone();
        let index_roots = INDEX_STORE_DISKS.get().expect("index roots initialized");
        for root in index_roots {
            let object_dir = record_path_for_root(
                StdPath::new(root),
                fragment.as_str(),
                source_object_id.as_str(),
                v1.as_str(),
            )
            .parent()
            .expect("object dir")
            .to_path_buf();
            assert!(
                fs::try_exists(object_dir.join(format!("{}.nmtd", v1)))
                    .await
                    .expect("check v1 nmtd")
            );
            assert!(
                fs::try_exists(object_dir.join(format!("{}.nmtd", v2)))
                    .await
                    .expect("check v2 nmtd")
            );
            assert!(
                fs::read_dir(&object_dir)
                    .await
                    .expect("read object dir")
                    .next_entry()
                    .await
                    .expect("first object record entry")
                    .is_some(),
                "N2 object dir must contain immutable .nmtd records"
            );
        }

        let before = list_n2_object_versions(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), source_key.clone())),
            AxumQuery(ListN2VersionsQuery { limit: Some(10) }),
        )
        .await
        .expect("list versions before rename");
        assert_eq!(before.0.versions.len(), 2);

        let rename = rename_n2_object(
            full_claims(fragment.as_str()),
            AxumJson(RenameN2ObjectRequest {
                fragment: fragment.clone(),
                source_object_key: source_key.clone(),
                destination_object_key: destination_key.clone(),
                expected_epoch: None,
            }),
        )
        .await
        .expect("rename n2 object");
        assert_eq!(rename.0.moved_versions, 2);
        assert_eq!(rename.0.metadata_records_rewritten, 0);

        let old_versions = list_n2_object_versions(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), source_key.clone())),
            AxumQuery(ListN2VersionsQuery { limit: Some(10) }),
        )
        .await
        .expect("list old key versions");
        assert!(old_versions.0.versions.is_empty());

        let new_versions = list_n2_object_versions(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), destination_key.clone())),
            AxumQuery(ListN2VersionsQuery { limit: Some(10) }),
        )
        .await
        .expect("list new key versions");
        assert_eq!(new_versions.0.versions.len(), 2);

        let renamed_v1_record = load_n2_object_record_local(
            fragment.as_str(),
            destination_key.as_str(),
            Some(v1.as_str()),
        )
        .await
        .expect("load renamed v1 record");
        assert_eq!(renamed_v1_record.object.object_id, source_object_id);
        assert_eq!(renamed_v1_record.object.object_key, destination_key);

        let listed = list_n2_objects(
            full_claims(fragment.as_str()),
            AxumPath(fragment.clone()),
            AxumQuery(ListN2ObjectsQuery {
                limit: Some(100),
                continuation: None,
                prefix: None,
                delimiter: None,
            }),
        )
        .await
        .expect("list n2 objects")
        .0;
        assert!(
            listed
                .objects
                .iter()
                .any(|row| row.object_key == destination_key)
        );

        let root_delimited = list_n2_objects(
            full_claims(fragment.as_str()),
            AxumPath(fragment.clone()),
            AxumQuery(ListN2ObjectsQuery {
                limit: Some(100),
                continuation: None,
                prefix: None,
                delimiter: Some("/".to_string()),
            }),
        )
        .await
        .expect("list n2 root common prefixes")
        .0;
        assert!(
            root_delimited
                .common_prefixes
                .iter()
                .any(|row| row == "archive/")
        );

        let archive_delimited = list_n2_objects(
            full_claims(fragment.as_str()),
            AxumPath(fragment.clone()),
            AxumQuery(ListN2ObjectsQuery {
                limit: Some(100),
                continuation: None,
                prefix: Some("archive/".to_string()),
                delimiter: Some("/".to_string()),
            }),
        )
        .await
        .expect("list n2 archive prefix")
        .0;
        assert_eq!(archive_delimited.objects.len(), 1);
        assert_eq!(archive_delimited.objects[0].object_key, destination_key);

        let delete_one = delete_n2_object(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), destination_key.clone())),
            AxumQuery(DeleteN2ObjectQuery {
                version_id: Some(v1.clone()),
                expected_epoch: None,
                purge_shards: Some(false),
            }),
        )
        .await
        .expect("delete one version");
        assert_eq!(delete_one.0.deleted_versions, 1);

        let after_one = list_n2_object_versions(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), destination_key.clone())),
            AxumQuery(ListN2VersionsQuery { limit: Some(10) }),
        )
        .await
        .expect("list versions after one delete");
        assert_eq!(after_one.0.versions.len(), 1);

        let delete_all = delete_n2_object(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), destination_key.clone())),
            AxumQuery(DeleteN2ObjectQuery {
                version_id: None,
                expected_epoch: None,
                purge_shards: Some(false),
            }),
        )
        .await
        .expect("delete all versions");
        assert_eq!(delete_all.0.deleted_versions, 1);

        let after_all = list_n2_objects(
            full_claims(fragment.as_str()),
            AxumPath(fragment.clone()),
            AxumQuery(ListN2ObjectsQuery {
                limit: Some(100),
                continuation: None,
                prefix: None,
                delimiter: None,
            }),
        )
        .await
        .expect("list n2 objects after deletes")
        .0;
        assert!(
            after_all
                .objects
                .iter()
                .all(|row| row.object_key != destination_key)
        );

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_reconcile_cleans_orphan_shards_when_record_has_stale_entries() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        let fragment = format!("frag-{}", Ulid::new());
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node_descriptor()],
        })
        .await
        .expect("set single-node membership");

        let object_key = "videos/reconcile-orphan.bin".to_string();
        let payload = sample_bytes(1_000_000, 55);
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(128 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put local-only object");

        let record = load_n2_object_record_local(
            fragment.as_str(),
            object_key.as_str(),
            Some(put.0.version_id.as_str()),
        )
        .await
        .expect("load record");
        let original_cshard =
            read_local_cluster_shard(fragment.as_str(), put.0.version_id.as_str(), 0)
                .await
                .expect("read original cshard");
        let stale_ref = store_local_cluster_shard_from_bytes(
            original_cshard.as_slice(),
            &record.object,
            1,
            record.params.kd,
            record.params.md,
            record.stripe_block_size,
        )
        .await
        .expect("store stale cshard");
        let mut stale_record = record.clone();
        stale_record.shards.push(stale_ref);
        stale_record.shards.sort_by_key(|s| s.cluster_shard_index);
        persist_n2_object_record(&stale_record)
            .await
            .expect("persist stale record");

        let stale_before =
            read_local_cluster_shard(fragment.as_str(), put.0.version_id.as_str(), 1)
                .await
                .expect("stale shard should exist before reconcile");
        assert!(!stale_before.is_empty());

        let reconcile = reconcile_n2_fragment(
            full_claims(fragment.as_str()),
            AxumPath(fragment.clone()),
            AxumQuery(ReconcileFragmentQuery {
                max_objects: Some(10),
                dry_run: Some(false),
                expected_epoch: None,
                cleanup_orphans: Some(true),
            }),
        )
        .await
        .expect("reconcile fragment")
        .0;
        assert_eq!(reconcile.failed_objects, 0);
        assert_eq!(reconcile.updated_objects, 0);
        assert_eq!(reconcile.results.len(), 1);
        assert_eq!(reconcile.results[0].cleanup_failures, 0);
        assert!(reconcile.results[0].cleaned_orphans > 0);

        let stale_after =
            read_local_cluster_shard(fragment.as_str(), put.0.version_id.as_str(), 1).await;
        assert!(
            matches!(stale_after, Err(AppError::NotFound)),
            "stale shard should be removed after reconcile cleanup"
        );

        let response = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("read reconciled object")
        .into_response();
        let got = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read reconciled body");
        assert_eq!(got.as_ref(), payload.as_slice());

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_read_survives_single_node_loss_in_k2_m1_layout() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let (remote_a_endpoint, remote_a_server) = spawn_internal_session_server().await;
        let (remote_b_endpoint, remote_b_server) = spawn_internal_session_server().await;
        let previous_membership = get_membership().await.expect("current membership");

        let nodes = vec![
            local_node_descriptor(),
            NodeDescriptor {
                node_id: format!("remote-a-{}", Ulid::new()),
                endpoint: remote_a_endpoint.clone(),
                epoch: 0,
                active: true,
            },
            NodeDescriptor {
                node_id: format!("remote-b-{}", Ulid::new()),
                endpoint: remote_b_endpoint.clone(),
                epoch: 0,
                active: true,
            },
        ];
        upsert_membership(UpsertMembershipRequest { epoch: None, nodes })
            .await
            .expect("set three-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/node-loss.bin".to_string();
        let payload = sample_bytes(2_000_000, 77);
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(2),
                mc: Some(1),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(256 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put k2+m1 object");
        assert_eq!(put.0.commit_mode, ClusterCommitMode::StrictFullPlacement);
        assert_eq!(put.0.planned_shards, 3);
        assert_eq!(put.0.required_commit_shards, 3);
        assert_eq!(put.0.committed_shards, 3);

        remote_a_server.abort();

        let response = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("read after one remote node loss")
        .into_response();
        let got = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        assert_eq!(got.as_ref(), payload.as_slice());

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
        remote_b_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn n2_five_node_two_loss_rejoin_reconcile_roundtrip() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        ensure_auth_env_for_tests();

        let (remote_a_endpoint, remote_a_server) = spawn_internal_session_server().await;
        let (remote_b_endpoint, remote_b_server) = spawn_internal_session_server().await;
        let (remote_c_endpoint, remote_c_server) = spawn_internal_session_server().await;
        let (remote_d_endpoint, remote_d_server) = spawn_internal_session_server().await;

        let remote_a_id = format!("remote-a-{}", Ulid::new());
        let remote_b_id = format!("remote-b-{}", Ulid::new());
        let remote_c_id = format!("remote-c-{}", Ulid::new());
        let remote_d_id = format!("remote-d-{}", Ulid::new());
        let previous_membership = get_membership().await.expect("current membership");

        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node_descriptor(),
                NodeDescriptor {
                    node_id: remote_a_id.clone(),
                    endpoint: remote_a_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_b_id.clone(),
                    endpoint: remote_b_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_c_id.clone(),
                    endpoint: remote_c_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_d_id.clone(),
                    endpoint: remote_d_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("set five-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/five-node-loss-rejoin.bin".to_string();
        let payload = sample_bytes(3_300_000, 117);
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(3),
                mc: Some(2),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(256 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put k3+m2 object");
        assert_eq!(put.0.commit_mode, ClusterCommitMode::StrictFullPlacement);
        assert_eq!(put.0.planned_shards, 5);
        assert_eq!(put.0.required_quorum, 3);
        assert_eq!(put.0.required_commit_shards, 5);
        assert_eq!(put.0.committed_shards, 5);

        // Simulate two-node outage in a k=3,m=2 layout.
        remote_a_server.abort();
        remote_b_server.abort();

        let before_rejoin = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("read after two-node loss")
        .into_response();
        let before_rejoin_bytes = to_bytes(before_rejoin.into_body(), usize::MAX)
            .await
            .expect("read body before rejoin");
        assert_eq!(before_rejoin_bytes.as_ref(), payload.as_slice());

        // Rejoin with replacement nodes and reconcile to new placement epoch.
        let (remote_e_endpoint, remote_e_server) = spawn_internal_session_server().await;
        let (remote_f_endpoint, remote_f_server) = spawn_internal_session_server().await;
        let remote_e_id = format!("remote-e-{}", Ulid::new());
        let remote_f_id = format!("remote-f-{}", Ulid::new());

        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node_descriptor(),
                NodeDescriptor {
                    node_id: remote_c_id.clone(),
                    endpoint: remote_c_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_d_id.clone(),
                    endpoint: remote_d_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_e_id.clone(),
                    endpoint: remote_e_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
                NodeDescriptor {
                    node_id: remote_f_id.clone(),
                    endpoint: remote_f_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("set five-node membership after rejoin");

        let reconcile = reconcile_n2_fragment(
            full_claims(fragment.as_str()),
            AxumPath(fragment.clone()),
            AxumQuery(ReconcileFragmentQuery {
                max_objects: Some(10),
                dry_run: Some(false),
                expected_epoch: None,
                cleanup_orphans: Some(true),
            }),
        )
        .await
        .expect("reconcile after membership change")
        .0;
        assert_eq!(reconcile.failed_objects, 0);
        assert_eq!(reconcile.scanned_objects, 1);
        assert_eq!(reconcile.updated_objects, 1);
        assert_eq!(reconcile.results.len(), 1);
        assert_eq!(reconcile.results[0].status, "reconciled");
        assert!(reconcile.results[0].error.is_none());
        assert!(reconcile.results[0].planned_shards > 0);
        assert!(reconcile.results[0].repaired_shards > 0);
        assert!(reconcile.results[0].target_epoch >= reconcile.results[0].old_epoch);

        let record = load_n2_object_record_local(
            fragment.as_str(),
            object_key.as_str(),
            Some(put.0.version_id.as_str()),
        )
        .await
        .expect("load record after reconcile");
        assert_eq!(record.epoch, reconcile.results[0].target_epoch);
        assert_eq!(record.shards.len(), 5);
        let mut shard_indexes = record
            .shards
            .iter()
            .map(|shard| shard.cluster_shard_index)
            .collect::<Vec<_>>();
        shard_indexes.sort_unstable();
        assert_eq!(shard_indexes, vec![0, 1, 2, 3, 4]);

        let after_reconcile = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("read after rejoin+reconcile")
        .into_response();
        let after_reconcile_bytes = to_bytes(after_reconcile.into_body(), usize::MAX)
            .await
            .expect("read body after reconcile");
        assert_eq!(after_reconcile_bytes.as_ref(), payload.as_slice());

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;

        remote_c_server.abort();
        remote_d_server.abort();
        remote_e_server.abort();
        remote_f_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_auto_maintenance_observe_then_repair_on_topology_change() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        reset_n2_test_storage_contents().await;
        ensure_auth_env_for_tests();

        let (remote_a_endpoint, remote_a_server) = spawn_internal_session_server().await;
        let (remote_b_endpoint, remote_b_server) = spawn_internal_session_server().await;
        let previous_membership = get_membership().await.expect("current membership");

        let remote_a_id = format!("remote-a-{}", Ulid::new());
        let remote_b_id = format!("remote-b-{}", Ulid::new());

        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node_descriptor(),
                NodeDescriptor {
                    node_id: remote_a_id.clone(),
                    endpoint: remote_a_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("set initial two-node membership");

        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "videos/auto-maintenance.bin".to_string();
        let payload = sample_bytes(1_400_000, 151);
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(1),
                mc: Some(1),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(256 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put replicated object");
        assert_eq!(put.0.committed_shards, 2);

        let after_topology_change = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node_descriptor(),
                NodeDescriptor {
                    node_id: remote_b_id.clone(),
                    endpoint: remote_b_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("switch placement target to remote-b");

        let observe = run_n2_auto_maintenance_once(
            "test-observe",
            after_topology_change.epoch,
            N2AutoMaintenanceMode::Observe,
            16,
            64,
            false,
        )
        .await
        .expect("observe auto maintenance");
        assert!(observe.dry_run);
        assert_eq!(observe.fragments_scanned, 1);
        assert_eq!(observe.fragments_with_planned_changes, 1);

        let observed_record = load_n2_object_record_local(
            fragment.as_str(),
            object_key.as_str(),
            Some(put.0.version_id.as_str()),
        )
        .await
        .expect("load observed record");
        assert_eq!(observed_record.epoch, put.0.object.epoch);
        assert_eq!(observed_record.shards.len(), put.0.object.shards.len());

        let repair = run_n2_auto_maintenance_once(
            "test-repair",
            after_topology_change.epoch,
            N2AutoMaintenanceMode::RepairAndRedistribute,
            16,
            64,
            false,
        )
        .await
        .expect("repair auto maintenance");
        assert!(!repair.dry_run);
        assert_eq!(repair.fragments_scanned, 1);
        assert_eq!(repair.fragments_with_planned_changes, 1);
        assert_eq!(repair.failed_fragments, 0);

        let repaired_record = load_n2_object_record_local(
            fragment.as_str(),
            object_key.as_str(),
            Some(put.0.version_id.as_str()),
        )
        .await
        .expect("load repaired record");
        assert_eq!(repaired_record.epoch, after_topology_change.epoch);
        assert_eq!(repaired_record.shards.len(), put.0.object.shards.len());

        let response = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("read repaired object")
        .into_response();
        let got = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read repaired body");
        assert_eq!(got.as_ref(), payload.as_slice());

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
        remote_a_server.abort();
        remote_b_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_health_reports_node_cluster_fragment_and_object_state() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        reset_n2_test_storage_contents().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        let fragment = format!("frag-{}", Ulid::new());
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![local_node_descriptor()],
        })
        .await
        .expect("single-node membership");

        let object_key = "videos/health-local.bin".to_string();
        let payload = sample_bytes(512 * 1024, 101);
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(1),
                mc: Some(0),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(128 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload),
        )
        .await
        .expect("put object");

        let node = get_n2_node_health(full_claims(fragment.as_str()))
            .await
            .expect("node health")
            .0;
        assert!(node.reachable);
        assert!(!node.index_roots.is_empty());
        assert!(!node.blob_roots.is_empty());

        let object = get_n2_object_health(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(ObjectHealthQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("object health")
        .0;
        assert_eq!(object.level, N2HealthLevel::Healthy);
        assert!(object.readable);
        assert_eq!(object.available_shards, 1);

        let fragment_health = get_n2_fragment_health(
            full_claims(fragment.as_str()),
            AxumPath(fragment.clone()),
            AxumQuery(FragmentHealthQuery {
                max_objects: Some(10),
            }),
        )
        .await
        .expect("fragment health")
        .0;
        assert_eq!(fragment_health.level, N2HealthLevel::Healthy);
        assert_eq!(fragment_health.scanned_objects, 1);

        let cluster = get_n2_cluster_health(
            full_claims(fragment.as_str()),
            AxumQuery(ClusterHealthQuery {
                expected_epoch: None,
                deep: Some(true),
                max_fragments: Some(10),
                max_objects_per_fragment: Some(10),
            }),
        )
        .await
        .expect("cluster health")
        .0;
        assert!(cluster.reachable_active_nodes >= 1);
        assert!(!cluster.nodes.is_empty());
        assert!(cluster.fragments.iter().any(|f| f.fragment == fragment));

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn n2_object_health_and_repair_restore_missing_local_shard_from_remote() {
        let _guard = STORAGE_TEST_LOCK.lock().await;
        ensure_n2_test_storage_roots().await;
        reset_n2_test_storage_contents().await;
        ensure_auth_env_for_tests();

        let previous_membership = get_membership().await.expect("current membership");
        let (remote_endpoint, remote_server) = spawn_internal_session_server().await;
        let fragment = format!("frag-{}", Ulid::new());
        upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: vec![
                local_node_descriptor(),
                NodeDescriptor {
                    node_id: "remote-health-repair".to_string(),
                    endpoint: remote_endpoint.clone(),
                    epoch: 0,
                    active: true,
                },
            ],
        })
        .await
        .expect("two-node membership");

        let object_key = "videos/repair-object.bin".to_string();
        let payload = sample_bytes(2 * 1024 * 1024, 77);
        let put = put_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(PutObjectQuery {
                uid: Some(fragment.clone()),
                version_id: None,
                expected_epoch: None,
                kc: Some(1),
                mc: Some(1),
                kd: Some(1),
                md: Some(0),
                stripe_block_size: Some(128 * 1024),
                content_type: Some("application/octet-stream".to_string()),
            }),
            Body::from(payload.clone()),
        )
        .await
        .expect("put redundant object");

        let local_endpoint = normalize_endpoint(n2_runtime_config().node_endpoint.as_str());
        let local_shard = put
            .0
            .object
            .shards
            .iter()
            .find(|shard| normalize_endpoint(shard.endpoint.as_str()) == local_endpoint)
            .expect("one shard is local")
            .cluster_shard_index;
        let deleted =
            delete_local_cluster_shard(fragment.as_str(), put.0.version_id.as_str(), local_shard)
                .await
                .expect("delete local shard");
        assert!(deleted > 0);

        let degraded = get_n2_object_health(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(ObjectHealthQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("degraded object health")
        .0;
        assert_eq!(degraded.level, N2HealthLevel::Degraded);
        assert!(degraded.readable);
        assert!(degraded.repairable);
        assert_eq!(degraded.missing_shards, 1);

        let repair = repair_n2_object(
            full_claims(fragment.as_str()),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(RepairN2ObjectQuery {
                version_id: Some(put.0.version_id.clone()),
                expected_epoch: None,
                dry_run: Some(false),
                cleanup_orphans: Some(true),
            }),
        )
        .await
        .expect("object repair")
        .0;
        assert_eq!(repair.reconcile.status, "reconciled");
        assert!(repair.reconcile.repaired_shards >= 1);
        let after = repair.after.expect("post-repair health");
        assert_eq!(after.level, N2HealthLevel::Healthy, "{after:#?}");
        assert_eq!(after.available_shards, 2);

        let response = get_n2_object(
            full_claims(fragment.as_str()),
            HeaderMap::new(),
            AxumPath((fragment.clone(), object_key.clone())),
            AxumQuery(GetObjectQuery {
                version_id: Some(put.0.version_id.clone()),
            }),
        )
        .await
        .expect("read repaired object")
        .into_response();
        let got = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read repaired body");
        assert_eq!(got.as_ref(), payload.as_slice());

        let _ = upsert_membership(UpsertMembershipRequest {
            epoch: None,
            nodes: previous_membership.nodes,
        })
        .await;
        remote_server.abort();
    }
}
