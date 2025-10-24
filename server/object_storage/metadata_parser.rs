use mongodb::{Client as MongoClient, bson::doc};
use crate::database::database_constants::OBJECT_STORAGE_DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::structs::object_storage_structs::ObjectMetadata;

pub async fn find_latest_object_metadata(
    db: &MongoClient,
    fragment: &str,
    object_key: &str,
    uid: &str,
) -> Result<ObjectMetadata, AppError> {
    let collection = db
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<ObjectMetadata>("objects");

    collection
        .find_one(
            doc! { "fragment": fragment, "key": object_key, "owner_uid": uid, "is_latest": true },
        )
        .await
        .map_err(|_| AppError::Conflict("Database query failed.".to_string()))?
        .ok_or(AppError::BadRequest("Object not found.".to_string()))
}
