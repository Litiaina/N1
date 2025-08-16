use crate::enums::app_error_enum::AppError;
use crate::structs::object_storage_structs::{ObjectMetadata, TotalStorageUsage};
use futures_util::TryStreamExt;
use mongodb::Client as MongoClient;
use mongodb::bson;
use mongodb::bson::doc;
use crate::database::database_constants::OBJECT_STORAGE_DATABASE;

pub async fn calculate_storage_usage(db: &MongoClient, uid: &str) -> Result<u64, AppError> {
    let objects_collection = db
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<ObjectMetadata>("objects");

    let pipeline = vec![
        doc! { "$match": { "owner_uid": uid, "is_latest": true } },
        doc! { "$group": { "_id": null, "total_usage": { "$sum": "$size_bytes" } } },
    ];

    let mut cursor = objects_collection
        .aggregate(pipeline)
        .await
        .map_err(|_| AppError::Conflict("Failed to calculate storage usage.".to_string()))?;

    if let Some(result) = cursor
        .try_next()
        .await
        .map_err(|_| AppError::Conflict("Failed to read usage result.".to_string()))?
    {
        if let Ok(usage_doc) = bson::from_document::<TotalStorageUsage>(result) {
            return Ok(usage_doc.total_usage);
        }
    }

    Ok(0)
}
