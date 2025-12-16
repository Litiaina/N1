use crate::database::database_constants::DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::structs::object_storage_structs::UserAuthDetails;
use mongodb::Client as MongoClient;
use mongodb::bson::doc;

pub async fn calculate_storage_usage(db: &MongoClient, uid: &str) -> Result<u64, AppError> {
    let auth_collection = db.database(DATABASE).collection::<UserAuthDetails>("auth");

    let user_details = auth_collection
        .find_one(doc! { "uid": uid })
        .await
        .map_err(|_| AppError::InternalServerError("Database query failed.".to_string()))?
        .ok_or(AppError::Unauthorized)?;

    Ok(user_details.storage_used)
}

pub async fn increment_storage_usage(
    db: &MongoClient,
    uid: &str,
    bytes: i64,
) -> Result<(), AppError> {
    let auth_collection = db.database(DATABASE).collection::<UserAuthDetails>("auth");

    auth_collection
        .update_one(
            doc! { "uid": uid },
            doc! { "$inc": { "storage_used": bytes } },
        )
        .await
        .map_err(|_| {
            AppError::InternalServerError("Failed to update storage quota.".to_string())
        })?;

    Ok(())
}
