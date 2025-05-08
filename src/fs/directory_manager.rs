use percent_encoding::percent_decode_str;
use std::path::PathBuf;
use axum::extract::{Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use axum::response::IntoResponse;
use serde_json::json;
use tokio::fs;
use tracing::error;
use crate::structs::delete_file_params::DeleteFileParams;
use crate::structs::rename_params_struct::RenameParams;
use crate::utils::directory_validation::sanitize_path;
use crate::utils::header_extractor::extract_uid_from_header;

pub async fn rename_file_query(
    header: HeaderMap,
    path: Option<Path<String>>,
    Query(params): Query<RenameParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = match extract_uid_from_header(&header).await {
        Ok(email) => email,
        Err(code) => return Err(code),
    };

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
    header: HeaderMap,
    path: Option<Path<String>>,
    Query(params): Query<DeleteFileParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = match extract_uid_from_header(&header).await {
        Ok(email) => email,
        Err(code) => return Err(code),
    };

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

pub async fn create_directory(header: HeaderMap, Path(path): Path<String>) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = match extract_uid_from_header(&header).await {
        Ok(email) => email,
        Err(code) => return Err(code),
    };
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

pub async fn delete_directory(header: HeaderMap, Path(path): Path<String>) -> Result<impl IntoResponse, StatusCode> {
    let validated_uid = match extract_uid_from_header(&header).await {
        Ok(email) => email,
        Err(code) => return Err(code),
    };
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
