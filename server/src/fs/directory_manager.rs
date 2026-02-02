use crate::structs::delete_file_params_struct::DeleteFileParams;
use crate::structs::file_metadata_struct::FileMetadata;
use crate::structs::metadata_params_struct::MetadataParams;
use crate::structs::rename_params_struct::RenameParams;
use crate::validation::directory_validation::sanitize_path;
use axum::Json;
use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use percent_encoding::percent_decode_str;
use serde_json::json;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use tokio::fs;
use tracing::error;
use crate::fs::file_path_parser::file_path;
use crate::structs::auth_structs::FileSystemClaims;

pub async fn rename_file_query(
    claims: FileSystemClaims,
    path: Option<Path<String>>,
    Query(params): Query<RenameParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = &claims.uid;

    let relative_path = path
        .as_ref()
        .map(|Path(p)| sanitize_path(p))
        .unwrap_or_default();

    let full_dir_path = if relative_path.is_empty() {
        PathBuf::from(format!("storage-files/{}", validated_uid))
    } else {
        PathBuf::from(format!("storage-files/{}/{}", validated_uid, relative_path))
    };

    if !full_dir_path.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let old_file_path = full_dir_path.join(&params.file_to_modify);

    if !old_file_path.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let file_extension = old_file_path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("");

    let new_file_name_with_ext = if file_extension.is_empty() {
        params.new_file_name
    } else {
        format!("{}.{}", params.new_file_name, file_extension)
    };

    let new_file_path = old_file_path.with_file_name(new_file_name_with_ext);

    fs::rename(&old_file_path, &new_file_path)
        .await
        .map_err(|e| {
            error!("Error renaming file: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({ "message": "File renamed successfully" })))
}

pub async fn delete_file(
    claims: FileSystemClaims,
    path: Option<Path<String>>,
    Query(params): Query<DeleteFileParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = &claims.uid;

    let relative_path = path
        .as_ref()
        .map(|Path(p)| sanitize_path(p))
        .unwrap_or_default();

    let base_dir = if relative_path.is_empty() {
        format!("storage-files/{}", validated_uid)
    } else {
        format!("storage-files/{}/{}", validated_uid, relative_path)
    };

    let file_path = PathBuf::from(base_dir).join(&params.file_to_delete);

    if !file_path.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    match fs::remove_file(&file_path).await {
        Ok(_) => Ok(Json(json!({ "message": "File deleted successfully" })).into_response()),
        Err(err) => {
            let error_message = format!("Failed to delete file: {}", err);
            Ok(Json(json!({ "error": error_message })).into_response())
        }
    }
}

pub async fn create_directory(
    claims: FileSystemClaims,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = &claims.uid;
    let decoded_path = percent_decode_str(&path)
        .decode_utf8()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let directory_path = PathBuf::from(format!("storage-files/{}/{}", validated_uid, decoded_path));

    if directory_path.exists() {
        return Ok(Json(json!({"message": "Folder already exists"})).into_response());
    }

    if let Err(err) = fs::create_dir_all(&directory_path).await {
        let error_message = format!("Failed to create folder: {}", err);
        return Ok(Json(json!({ "error": error_message })).into_response());
    }

    Ok(Json(json!({"message": "Folder created successfully"})).into_response())
}

pub async fn delete_directory(
    claims: FileSystemClaims,
    Path(path): Path<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = &claims.uid;
    let decoded_path = percent_decode_str(&path)
        .decode_utf8()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let directory_path = PathBuf::from(format!("storage-files/{}/{}", validated_uid, decoded_path));

    if !directory_path.exists() {
        return Ok(Json(json!({"message": "Folder does not exist"})).into_response());
    }

    if let Err(err) = fs::remove_dir_all(&directory_path).await {
        let error_message = format!("Failed to delete folder: {}", err);
        return Ok(Json(json!({ "error": error_message })).into_response());
    }

    Ok(Json(json!({"message": "Folder deleted successfully"})).into_response())
}

pub async fn get_file_metadata(
    claims: FileSystemClaims,
    Query(params): Query<MetadataParams>,
    path: Option<Path<String>>,
) -> impl IntoResponse {
    let validated_uid = &claims.uid;

    let relative_path = path
        .as_ref()
        .map(|Path(p)| p.trim_matches('/').to_string())
        .unwrap_or_default();

    let file_path = file_path(validated_uid, &relative_path, &params.file_name).await;

    let metadata = match fs::metadata(&file_path).await {
        Ok(meta) => meta,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };

    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let info = FileMetadata {
        file_name: params.file_name,
        file_size: metadata.len(),
        last_modified: modified,
    };

    Json(info).into_response()
}
