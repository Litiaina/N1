//! # Garbage Collector for Stale Uploads
//!
//! This module provides a background task that periodically cleans up abandoned
//! multipart uploads. It operates by querying the MongoDB database for metadata
//! records with a "Pending" status that are older than a specified Time-To-Live (TTL).
//!
//! This approach is highly efficient because it uses a targeted database query
//! instead of scanning the entire filesystem for old temporary files.

use crate::database::database_constants::OBJECT_STORAGE_DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::fs::fs_constants::{CLEANUP_INTERVAL, FILE_TTL, TEMP_UPLOAD_ROOT};
use crate::object_storage::constants::OBJECT_STORAGE_ROOT;
use crate::structs::object_storage_structs::ObjectMetadata;
use chrono::{Duration, Utc};
use futures_util::TryStreamExt;
use mongodb::{
    Client as MongoClient,
    bson::{Bson, doc},
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::{fs, time::sleep};
use tracing::{info, warn};

pub async fn run_garbage_collector(db_client: Arc<MongoClient>) {
    info!(
        "🗑️ Garbage Collector started. Cycle interval: {:?}",
        CLEANUP_INTERVAL
    );
    loop {
        sleep(CLEANUP_INTERVAL).await;
        info!("🗑️ Running garbage collection cycle for stale pending uploads...");

        if let Err(e) = cleanup_stale_pending_uploads(&db_client, FILE_TTL).await {
            warn!("🗑️ Garbage collection cycle failed: {}", e);
        }
    }
}

async fn cleanup_stale_pending_uploads(
    db: &MongoClient,
    ttl: std::time::Duration,
) -> Result<(), AppError> {
    let collection = db
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<ObjectMetadata>("objects");

    let stale_threshold =
        Utc::now() - Duration::from_std(ttl).expect("Failed to convert TTL to chrono::Duration");

    let filter = doc! {
        "status": "Pending",
        "created_at": { "$lt": Bson::DateTime(std::time::SystemTime::from(stale_threshold).into()) }
    };

    let cursor = collection.find(filter.clone()).await?;
    let stale_records: Vec<ObjectMetadata> = cursor.try_collect().await?;

    if stale_records.is_empty() {
        info!("🗑️ No stale pending uploads found.");
        return Ok(());
    }

    info!(
        "🗑️ Found {} stale pending uploads to clean up.",
        stale_records.len()
    );

    for record in &stale_records {
        if let Some(session_id) = &record.session_id {
            let temp_upload_dir = PathBuf::from(OBJECT_STORAGE_ROOT)
                .join(TEMP_UPLOAD_ROOT)
                .join(session_id);

            if temp_upload_dir.exists() {
                if let Err(e) = fs::remove_dir_all(&temp_upload_dir).await {
                    warn!(
                        "🗑️ Failed to delete stale temp directory {:?}: {}",
                        temp_upload_dir, e
                    );
                } else {
                    info!("🗑️ Deleted stale temp directory: {:?}", temp_upload_dir);
                }
            }
        }
    }

    let delete_result = collection.delete_many(filter).await?;
    info!(
        "🗑️ Deleted {} stale metadata records from the database.",
        delete_result.deleted_count
    );

    Ok(())
}
