use crate::distribution_core::cluster_shards::{
    delete_local_cluster_shard, open_local_cluster_shard_file, store_local_cluster_shard_from_file,
};
use crate::distribution_core::codec::normalize_n2_stripe_block_size;
use crate::distribution_core::model::{
    InternalStoreShardQuery, N2ClusterShardRef, N2ObjectRecord, ObjectIdentity,
};
use crate::distribution_core::object_index::{
    delete_n2_object_record_local, load_n2_object_record_local, persist_n2_object_record_local,
    rename_n2_namespace_entry_local,
};
use crate::distribution_core::peer::{
    InternalNamespaceRenameRequest, InternalNamespaceRenameResponse,
    InternalObjectRecordDeleteQuery, InternalObjectRecordDeleteResponse,
    InternalObjectRecordReadQuery, InternalObjectRecordUpsertResponse, InternalShardAbortResponse,
    InternalShardChunkAck, InternalShardDeleteResponse, InternalShardSessionResponse,
    validate_internal_peer_request,
};
use crate::distribution_core::shard_session::{
    InternalShardUploadSession, internal_shard_upload_sessions,
};
use crate::enums::app_error_enum::AppError;
use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Path, Query};
use axum::http::{
    HeaderMap, HeaderValue, StatusCode, header::CONTENT_LENGTH, header::CONTENT_TYPE,
};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;
use ulid::Ulid;

pub async fn n2_internal_begin_shard(
    headers: HeaderMap,
    Path((fragment, version_id, cluster_shard_index)): Path<(String, String, usize)>,
    Query(query): Query<InternalStoreShardQuery>,
) -> Result<AxumJson<InternalShardSessionResponse>, AppError> {
    validate_internal_peer_request(&headers).await?;
    if query.kd == 0 {
        return Err(AppError::BadRequest("kd must be >= 1".to_string()));
    }
    let stripe_block_size = normalize_n2_stripe_block_size(query.stripe_block_size);
    let session_id = Ulid::new().to_string();
    let temp_root =
        std::env::temp_dir().join(format!("n2-internal-session-{}", session_id.as_str()));
    fs::create_dir_all(&temp_root)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {:?}", temp_root)))?;
    let temp_file_path = temp_root.join("cluster_shard.bin");
    fs::File::create(&temp_file_path)
        .await
        .map_err(|e| AppError::Io(e, format!("create {:?}", temp_file_path)))?;

    let object = ObjectIdentity {
        uid: query.uid.unwrap_or_else(|| fragment.clone()),
        fragment,
        object_id: query
            .object_id
            .unwrap_or_else(|| format!("internal-{}", Ulid::new())),
        object_key: query
            .object_key
            .unwrap_or_else(|| "<internal-cshard-session>".to_string()),
        version_id,
    };

    let session = InternalShardUploadSession {
        object,
        cluster_shard_index,
        kd: query.kd,
        md: query.md,
        stripe_block_size,
        temp_root,
        temp_file_path,
    };
    internal_shard_upload_sessions()
        .lock()
        .await
        .insert(session_id.clone(), session);
    Ok(AxumJson(InternalShardSessionResponse { session_id }))
}

pub async fn n2_internal_write_shard_chunk(
    headers: HeaderMap,
    Path(session_id): Path<String>,
    body: Body,
) -> Result<AxumJson<InternalShardChunkAck>, AppError> {
    validate_internal_peer_request(&headers).await?;
    let temp_file_path = {
        let guard = internal_shard_upload_sessions().lock().await;
        let session = guard.get(session_id.as_str()).ok_or(AppError::NotFound)?;
        session.temp_file_path.clone()
    };

    let mut file = OpenOptions::new()
        .append(true)
        .open(&temp_file_path)
        .await
        .map_err(|e| AppError::Io(e, format!("open {:?}", temp_file_path)))?;
    let mut stream = body.into_data_stream();
    let mut written = 0u64;
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "read internal shard session chunk body".to_string(),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        file.write_all(&chunk)
            .await
            .map_err(|e| AppError::Io(e, "append internal shard session chunk".to_string()))?;
        written = written.saturating_add(chunk.len() as u64);
    }
    file.flush()
        .await
        .map_err(|e| AppError::Io(e, "flush internal shard session chunk".to_string()))?;

    Ok(AxumJson(InternalShardChunkAck {
        session_id,
        written_bytes: written,
    }))
}

pub async fn n2_internal_commit_shard(
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<AxumJson<N2ClusterShardRef>, AppError> {
    validate_internal_peer_request(&headers).await?;
    let session = internal_shard_upload_sessions()
        .lock()
        .await
        .remove(session_id.as_str())
        .ok_or(AppError::NotFound)?;
    let stored = store_local_cluster_shard_from_file(
        session.temp_file_path.as_path(),
        &session.object,
        session.cluster_shard_index,
        session.kd,
        session.md,
        session.stripe_block_size,
    )
    .await;
    let _ = fs::remove_dir_all(&session.temp_root).await;
    stored.map(AxumJson)
}

pub async fn n2_internal_abort_shard(
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<AxumJson<InternalShardAbortResponse>, AppError> {
    validate_internal_peer_request(&headers).await?;
    if let Some(session) = internal_shard_upload_sessions()
        .lock()
        .await
        .remove(session_id.as_str())
    {
        let _ = fs::remove_dir_all(&session.temp_root).await;
    }
    Ok(AxumJson(InternalShardAbortResponse {
        session_id,
        aborted: true,
    }))
}

pub async fn n2_internal_store_shard(
    headers: HeaderMap,
    Path((fragment, version_id, cluster_shard_index)): Path<(String, String, usize)>,
    Query(query): Query<InternalStoreShardQuery>,
    body: Body,
) -> Result<AxumJson<N2ClusterShardRef>, AppError> {
    validate_internal_peer_request(&headers).await?;

    if query.kd == 0 {
        return Err(AppError::BadRequest("kd must be >= 1".to_string()));
    }
    let stripe_block_size = normalize_n2_stripe_block_size(query.stripe_block_size);

    let tmp_root = std::env::temp_dir().join(format!("n2-internal-store-{}", Ulid::new()));
    fs::create_dir_all(&tmp_root)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {:?}", tmp_root)))?;
    let tmp_file_path = tmp_root.join("cluster_shard.bin");
    let mut tmp_file = fs::File::create(&tmp_file_path)
        .await
        .map_err(|e| AppError::Io(e, format!("create {:?}", tmp_file_path)))?;

    let mut stream = body.into_data_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "read internal shard request body".to_string(),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        tmp_file
            .write_all(&chunk)
            .await
            .map_err(|e| AppError::Io(e, "write internal temp shard".to_string()))?;
    }
    tmp_file
        .flush()
        .await
        .map_err(|e| AppError::Io(e, "flush internal temp shard".to_string()))?;
    drop(tmp_file);

    let object = ObjectIdentity {
        uid: query.uid.unwrap_or_else(|| fragment.clone()),
        fragment: fragment.clone(),
        object_id: query
            .object_id
            .unwrap_or_else(|| format!("internal-{}", Ulid::new())),
        object_key: query
            .object_key
            .unwrap_or_else(|| "<internal-cshard>".to_string()),
        version_id,
    };

    let stored = store_local_cluster_shard_from_file(
        tmp_file_path.as_path(),
        &object,
        cluster_shard_index,
        query.kd,
        query.md,
        stripe_block_size,
    )
    .await?;

    let _ = fs::remove_dir_all(&tmp_root).await;
    Ok(AxumJson(stored))
}

pub async fn n2_internal_read_shard(
    headers: HeaderMap,
    Path((fragment, version_id, cluster_shard_index)): Path<(String, String, usize)>,
) -> Result<impl IntoResponse, AppError> {
    validate_internal_peer_request(&headers).await?;
    let (file, size_bytes) =
        open_local_cluster_shard_file(fragment.as_str(), version_id.as_str(), cluster_shard_index)
            .await?;
    let stream = ReaderStream::new(file.take(size_bytes));
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    if let Ok(value) = HeaderValue::from_str(size_bytes.to_string().as_str()) {
        response.headers_mut().insert(CONTENT_LENGTH, value);
    }
    Ok(response)
}

pub async fn n2_internal_delete_shard(
    headers: HeaderMap,
    Path((fragment, version_id, cluster_shard_index)): Path<(String, String, usize)>,
) -> Result<AxumJson<InternalShardDeleteResponse>, AppError> {
    validate_internal_peer_request(&headers).await?;
    let deleted_paths =
        delete_local_cluster_shard(fragment.as_str(), version_id.as_str(), cluster_shard_index)
            .await?;
    Ok(AxumJson(InternalShardDeleteResponse { deleted_paths }))
}

pub async fn n2_internal_upsert_object_record(
    headers: HeaderMap,
    AxumJson(record): AxumJson<N2ObjectRecord>,
) -> Result<AxumJson<InternalObjectRecordUpsertResponse>, AppError> {
    validate_internal_peer_request(&headers).await?;
    let writes = persist_n2_object_record_local(&record).await?;
    Ok(AxumJson(InternalObjectRecordUpsertResponse {
        local_mirrors_written: writes,
    }))
}

pub async fn n2_internal_read_object_record(
    headers: HeaderMap,
    Path(fragment): Path<String>,
    Query(query): Query<InternalObjectRecordReadQuery>,
) -> Result<AxumJson<N2ObjectRecord>, AppError> {
    validate_internal_peer_request(&headers).await?;
    let object_key = query.object_key.trim();
    if object_key.is_empty() {
        return Err(AppError::BadRequest(
            "object_key is required for internal object record read".to_string(),
        ));
    }
    let record =
        load_n2_object_record_local(fragment.as_str(), object_key, query.version_id.as_deref())
            .await?;
    Ok(AxumJson(record))
}

pub async fn n2_internal_delete_object_record(
    headers: HeaderMap,
    Path(fragment): Path<String>,
    Query(query): Query<InternalObjectRecordDeleteQuery>,
) -> Result<AxumJson<InternalObjectRecordDeleteResponse>, AppError> {
    validate_internal_peer_request(&headers).await?;
    let object_key = query.object_key.trim();
    let version_id = query.version_id.trim();
    if object_key.is_empty() || version_id.is_empty() {
        return Err(AppError::BadRequest(
            "object_key and version_id are required for internal object record delete".to_string(),
        ));
    }
    let deleted = delete_n2_object_record_local(fragment.as_str(), object_key, version_id).await?;
    Ok(AxumJson(InternalObjectRecordDeleteResponse {
        local_metadata_files_deleted: deleted,
    }))
}

pub async fn n2_internal_rename_namespace(
    headers: HeaderMap,
    AxumJson(request): AxumJson<InternalNamespaceRenameRequest>,
) -> Result<AxumJson<InternalNamespaceRenameResponse>, AppError> {
    validate_internal_peer_request(&headers).await?;
    let moved = rename_n2_namespace_entry_local(
        request.fragment.trim(),
        request.source_object_key.trim().trim_matches('/'),
        request.destination_object_key.trim().trim_matches('/'),
    )
    .await?;
    Ok(AxumJson(InternalNamespaceRenameResponse {
        local_namespace_entries_moved: moved,
    }))
}
