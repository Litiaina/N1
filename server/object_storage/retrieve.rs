use crate::database::database_constants::OBJECT_STORAGE_DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::object_storage::metadata_parser::find_latest_object_metadata;
use crate::object_storage::object_parser::{get_object_logic, stream_object_logic};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::ObjectMetadata;
use axum::Json as AxumJson;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use futures_util::stream::TryStreamExt;
use mongodb::bson::doc;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn get_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
    req: axum::http::Request<Body>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let metadata =
        find_latest_object_metadata(&state.db_client, &fragment, &object_key, &uid).await?;
    get_object_logic(metadata, req).await
}

pub async fn stream_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    headers: HeaderMap,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let metadata =
        find_latest_object_metadata(&state.db_client, &fragment, &object_key, &uid).await?;
    stream_object_logic(headers, metadata).await
}

pub async fn get_object_metadata(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path((fragment, object_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let metadata =
        find_latest_object_metadata(&state.db_client, &fragment, &object_key, &uid).await?;
    Ok(AxumJson(metadata))
}

pub async fn list_objects(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Path(fragment): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let collection = state
        .db_client
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<ObjectMetadata>("objects");

    let limit: i64 = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let marker = params.get("marker").cloned();

    let show_all = params.get("show_all").map_or(false, |s| s == "true");

    let mut query = doc! {
        "fragment": fragment,
        "owner_uid": uid,
    };

    if !show_all {
        query.insert("is_latest", true);
    }

    if let Some(marker_key) = marker {
        query.insert("key", doc! { "$gt": marker_key });
    }

    let find_options = mongodb::options::FindOptions::builder()
        .limit(limit)
        .sort(doc! { "key": 1 })
        .build();

    let mut cursor = collection
        .find(query)
        .with_options(find_options)
        .await
        .map_err(|e| {
            eprintln!("Database query failed: {:?}", e);
            AppError::Conflict("Database query failed.".to_string())
        })?;

    let mut objects = Vec::new();
    while let Some(result) = cursor.try_next().await.map_err(|e| {
        eprintln!("Failed to read from db cursor: {:?}", e);
        AppError::Conflict("Failed to read from database cursor.".to_string())
    })? {
        objects.push(result);
    }

    Ok(AxumJson(objects))
}

pub async fn list_fragments(
    State(state): State<Arc<AppState>>,
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let collection = state
        .db_client
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<ObjectMetadata>("objects");

    let filter = doc! { "owner_uid": uid };

    let fragments = collection
        .distinct("fragment", filter)
        .await
        .map_err(|_| AppError::Conflict("Database query for fragments failed.".to_string()))?;

    Ok(AxumJson(fragments))
}
