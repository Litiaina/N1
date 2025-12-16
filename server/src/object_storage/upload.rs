use crate::database::database_constants::DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::fs::fragmented_upload::with_io_timeout;
use crate::object_storage::caching::invalidate_cache;
use crate::object_storage::constants::{OBJECT_META_FILE, PART_META_SUFFIX, PART_SUFFIX};
use crate::object_storage::storage_capacity_parser::{
    calculate_storage_usage, increment_storage_usage,
};
use crate::routes::storage_routes::AppState;
use crate::statics::merge_semaphore_static::CHUNK_UPLOAD_SEMAPHORE;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{
    CapacityUsageResponse, InitiateUploadParams, InitiateUploadResponse, ObjectKeyQuery,
    ObjectMetadata, ObjectPathManager, PartMetadata, UploadStatusResponse, UserAuthDetails,
};
use axum::body::Body;
use axum::{
    Json as AxumJson,
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::Utc;
use futures_util::StreamExt;
use mongodb::bson::doc;
#[cfg(not(unix))]
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::info;
use ulid::Ulid;

#[cfg(unix)]
pub async fn sync_dir(path: &Path) -> Result<(), AppError> {
    use tokio::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    let dir = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, "open dir for sync".into()))?;

    dir.sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync dir".into()))?;

    Ok(())
}

#[cfg(not(unix))]
pub async fn sync_dir(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

pub async fn initiate_object_upload(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, object_key)): AxumPath<(String, String)>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let auth_collection = state
        .db_client
        .database(DATABASE)
        .collection::<UserAuthDetails>("auth");
    let user_details = auth_collection
        .find_one(doc! { "uid": &uid })
        .await
        .map_err(|_| AppError::InternalServerError("Auth DB check failed.".to_string()))?
        .ok_or(AppError::Unauthorized)?;

    if user_details
        .storage_used
        .saturating_add(params.total_size_bytes)
        > user_details.allocated_storage
    {
        return Err(AppError::InsufficientStorage);
    }

    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();

    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    with_io_timeout(
        "creating upload directory",
        fs::create_dir_all(&version_dir),
    )
    .await?;

    info!(
        "Initiated upload: {}/{} (v: {})",
        fragment, object_key, version_id
    );

    Ok((
        StatusCode::CREATED,
        AxumJson(InitiateUploadResponse {
            session_id,
            version_id,
        }),
    ))
}

pub async fn get_upload_status(
    claims: Claims,
    AxumPath((fragment, version_id)): AxumPath<(String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let object_key = key_query.object_key;
    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    if !fs::try_exists(&version_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let mut uploaded_parts = Vec::new();
    let mut entries = fs::read_dir(&version_dir)
        .await
        .map_err(|e| AppError::Io(e, "read dir".into()))?;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.ends_with(PART_META_SUFFIX) {
            if let Some(rest) = name_str.strip_prefix("part_") {
                if let Some(index_str) = rest.strip_suffix(PART_META_SUFFIX) {
                    if let Ok(index) = index_str.parse::<u64>() {
                        uploaded_parts.push(index);
                    }
                }
            }
        }
    }

    uploaded_parts.sort();

    Ok(AxumJson(UploadStatusResponse { uploaded_parts }))
}

pub async fn object_upload_part(
    claims: Claims,
    AxumPath((fragment, version_id, part_index)): AxumPath<(String, String, u64)>,
    Query(key_query): Query<ObjectKeyQuery>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    let object_key = key_query.object_key;

    let _permit = CHUNK_UPLOAD_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| AppError::InternalServerError("Server busy.".to_string()))?;

    let uid = claims.uid;
    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    if !fs::try_exists(&version_dir).await.unwrap_or(false) {
        return Err(AppError::NotFound);
    }

    let part_filename = format!("part_{}{}", part_index, PART_SUFFIX);
    let meta_filename = format!("part_{}{}", part_index, PART_META_SUFFIX);

    let part_path = version_dir.join(&part_filename);
    let meta_path = version_dir.join(&meta_filename);

    let file = File::create(&part_path)
        .await
        .map_err(|e| AppError::Io(e, "create part file".into()))?;
    let mut writer = BufWriter::new(file);
    let mut size_bytes = 0;

    let mut stream = body.into_data_stream();

    while let Some(chunk_res) = stream.next().await {
        let data =
            chunk_res.map_err(|e| AppError::BadRequest(format!("Stream read failed: {}", e)))?;
        size_bytes += data.len() as u64;
        writer
            .write_all(&data)
            .await
            .map_err(|e| AppError::Io(e, "write part".into()))?;
    }

    writer
        .flush()
        .await
        .map_err(|e| AppError::Io(e, "flush part".into()))?;

    let part_file = writer.into_inner();
    part_file
        .sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync part data".into()))?;

    let part_meta = PartMetadata {
        part_index,
        size_bytes,
        file_name: part_filename,
        created_at: Utc::now().timestamp(),
    };

    let bson_data = mongodb::bson::to_vec(&part_meta).map_err(|e| {
        AppError::InternalServerError(format!("Failed to serialize part metadata: {}", e))
    })?;

    let mut meta_file = File::create(&meta_path)
        .await
        .map_err(|e| AppError::Io(e, "create meta file".into()))?;

    meta_file
        .write_all(&bson_data)
        .await
        .map_err(|e| AppError::Io(e, "write meta data".into()))?;

    meta_file
        .sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync meta file".into()))?;

    Ok((StatusCode::OK, "Part uploaded."))
}

pub async fn finalize_object_upload(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumPath((fragment, version_id, session_id)): AxumPath<(String, String, String)>,
    Query(key_query): Query<ObjectKeyQuery>,
    AxumJson(params): AxumJson<InitiateUploadParams>,
) -> Result<impl IntoResponse, AppError> {
    let object_key = key_query.object_key;
    let uid = claims.uid;

    let path_manager = ObjectPathManager::new();
    let version_dir = path_manager.get_version_dir(&uid, &fragment, &object_key, &version_id);

    let mut entries = fs::read_dir(&version_dir)
        .await
        .map_err(|e| AppError::Io(e, "read dir".into()))?;

    let mut parts: Vec<PartMetadata> = Vec::new();
    let mut total_size_calculated = 0;

    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.ends_with(PART_META_SUFFIX) {
            let data = fs::read(entry.path())
                .await
                .map_err(|e| AppError::Io(e, "read meta".into()))?;

            let part: PartMetadata = mongodb::bson::from_slice(&data)
                .map_err(|_| AppError::InternalServerError("Corrupt part meta".into()))?;

            total_size_calculated += part.size_bytes;
            parts.push(part);
        }
    }

    if parts.is_empty() {
        return Err(AppError::BadRequest(
            "No parts found to finalize.".to_string(),
        ));
    }

    parts.sort_by_key(|p| p.part_index);

    for (i, part) in parts.iter().enumerate() {
        if part.part_index != i as u64 {
            return Err(AppError::BadRequest(format!("Missing part index {}", i)));
        }
    }

    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: uid.clone(),
        content_type: params
            .content_type
            .unwrap_or("application/octet-stream".to_string()),
        total_size_bytes: total_size_calculated,
        created_at: Utc::now().timestamp(),
        parts,
    };

    let bson_data = mongodb::bson::to_vec(&object_meta)
        .map_err(|e| AppError::InternalServerError(format!("Failed to serialize object metadata: {}", e)))?;

    let meta_path = version_dir.join(OBJECT_META_FILE);

    let mut final_meta_file = File::create(&meta_path)
        .await
        .map_err(|e| AppError::Io(e, "create object meta".into()))?;

    final_meta_file
        .write_all(&bson_data)
        .await
        .map_err(|e| AppError::Io(e, "write object meta".into()))?;

    final_meta_file
        .sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync object meta".into()))?;

    sync_dir(&version_dir).await?;

    increment_storage_usage(&state.db_client, &uid, total_size_calculated as i64).await?;

    invalidate_cache(&fragment, &object_key, &uid).await;

    info!(
        "Finalized object: {}/{} (Size: {})",
        fragment, object_key, total_size_calculated
    );

    Ok((StatusCode::OK, "Upload finalized."))
}

pub async fn get_capacity_usage(
    State(state): State<Arc<AppState>>,
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let usage = calculate_storage_usage(&state.db_client, &claims.uid).await?;
    Ok(AxumJson(CapacityUsageResponse {
        capacity_used: usage,
    }))
}
