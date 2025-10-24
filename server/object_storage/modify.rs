use std::sync::Arc;
use axum::extract::State;
use axum::Json as AxumJson;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use mongodb::bson::doc;
use tracing::info;
use crate::database::database_constants::OBJECT_STORAGE_DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::object_storage::metadata_parser::find_latest_object_metadata;
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;
use crate::structs::object_storage_structs::{ObjectMetadata, RenameObjectRequest};

pub async fn rename_object(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    AxumJson(payload): AxumJson<RenameObjectRequest>,
) -> Result<impl IntoResponse, AppError> {
    let uid = claims.uid;
    let db_client = &state.db_client;
    let collection = db_client
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<ObjectMetadata>("objects");

    find_latest_object_metadata(
        db_client,
        &payload.source_fragment,
        &payload.source_key,
        &uid,
    )
        .await?;

    let destination_exists = collection
        .find_one(doc! {
            "fragment": &payload.destination_fragment,
            "key": &payload.destination_key,
            "owner_uid": &uid
        })
        .await
        .map_err(|_| AppError::Conflict("Database query failed.".to_string()))?
        .is_some();

    if destination_exists {
        return Err(AppError::Conflict(
            "An object with the destination name already exists.".to_string(),
        ));
    }

    let filter = doc! {
        "fragment": &payload.source_fragment,
        "key": &payload.source_key,
        "owner_uid": &uid
    };
    let update = doc! {
        "$set": {
            "fragment": &payload.destination_fragment,
            "key": &payload.destination_key
        }
    };

    collection
        .update_many(filter, update)
        .await
        .map_err(|_| AppError::Conflict("Failed to rename object.".to_string()))?;

    info!(
        "Renamed object from {}/{} to {}/{}",
        payload.source_fragment,
        payload.source_key,
        payload.destination_fragment,
        payload.destination_key
    );

    Ok(StatusCode::OK)
}