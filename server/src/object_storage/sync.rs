use axum::{http::StatusCode, response::IntoResponse};
use futures_util::StreamExt;
use mongodb::{Client, bson::doc, options::ClientOptions};
use std::error::Error;
use tracing::{error, info, warn};
use walkdir::WalkDir;

use crate::{
    database::database_constants::DATABASE, enums::app_error_enum::AppError, object_storage::quota_manager::{init_quota_file, set_storage_used}, statics::database_statics::DATABASE_URL, structs::object_storage_structs::{ObjectMetadata, ObjectPathManager, UserRecord}
};

pub async fn sync_quota() -> Result<(), Box<dyn Error>> {
    info!("Background Task: Connecting to MongoDB for Quota Sync...");

    let client_options = ClientOptions::parse(&*DATABASE_URL).await?;
    let client = Client::with_options(client_options)?;
    let db = client.database(DATABASE);
    let auth_collection = db.collection::<UserRecord>("auth");

    info!("Background Task: Verifying consistency (MongoDB <-> Disk)...");

    let mut cursor = auth_collection.find(doc! {}).await?;
    let mut count = 0;
    let path_manager = ObjectPathManager::new();

    while let Some(result) = cursor.next().await {
        match result {
            Ok(user) => {
                let uid = user.uid.clone();

                if let Err(e) = init_quota_file(&uid, user.allocated_storage).await {
                    error!("Failed to sync quota file for {}: {}", uid, e);
                    continue;
                }

                let user_dir = path_manager.get_user_dir(&uid);

                tokio::spawn(async move {
                    if user_dir.exists() {
                        let calculated_usage = tokio::task::spawn_blocking(move || {
                            let mut total = 0u64;
                            for entry in WalkDir::new(&user_dir).into_iter().filter_map(|e| e.ok()) {
                                if entry.file_type().is_file() && entry.file_name().to_string_lossy().ends_with(".bson") {
                                    if entry.file_name() == "quota.bson" { continue; }
                                    
                                    if let Ok(data) = std::fs::read(entry.path()) {
                                        if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                                            if !meta.is_delete_marker {
                                                total += meta.total_size_bytes;
                                            }
                                        }
                                    }
                                }
                            }
                            total
                        })
                        .await;

                        match calculated_usage {
                            Ok(usage) => {
                                if let Err(e) = set_storage_used(&uid, usage).await {
                                    warn!("Failed to update usage stats for {}: {}", uid, e);
                                }
                            }
                            Err(e) => error!("Failed to calc usage for {}: {}", uid, e),
                        }
                    }
                });

                count += 1;
            }
            Err(e) => {
                error!("Failed to retrieve user document during sync: {}", e);
            }
        }
    }

    info!(
        "Background Task: Consistency check initiated for {} users (running in background).",
        count
    );

    Ok(())
}

pub async fn sync_quota_api() -> Result<impl IntoResponse, AppError> {
    match sync_quota().await {
        Ok(_) => Ok(
            StatusCode::PROCESSING
        ),
        Err(err) => Err(AppError::InternalServerError(format!("Sync error: {}", err))),
    }
}
