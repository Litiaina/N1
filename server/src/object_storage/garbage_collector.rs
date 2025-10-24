use crate::database::database_constants::OBJECT_STORAGE_DATABASE;
use crate::enums::app_error_enum::AppError;
use crate::settings::config::CONFIG;
use crate::statics::merge_semaphore_static::CHUNK_WRITE_LOCKS;
use crate::structs::object_storage_structs::{
    ObjectMetadata, ObjectPathManager, TempUploadMetadata,
};
use chrono::{Duration, Utc};
use mongodb::{
    Client as MongoClient,
    bson::{Bson, doc},
};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::{fs, time::sleep};
use tracing::{info, warn};

pub async fn run_garbage_collector(db_client: Arc<MongoClient>) {
    let cleanup_interval = StdDuration::from_secs(3);
    let file_ttl = StdDuration::from_secs(3);

    info!(
        "Object storage cleaning in: {:?}, File TTL: {:?}",
        cleanup_interval, file_ttl
    );

    sleep(cleanup_interval).await;
    info!("Cleaning object stale pending uploads...");

    loop {
        match cleanup_one_stale_upload(&db_client, file_ttl).await {
            Ok(Some(session_id)) => {
                info!("🗑️ Cleaned up stale upload session: {}", session_id);
            }
            Ok(None) => {
                info!("🗑️ No more stale pending uploads found in this cycle.");
                break;
            }
            Err(e) => {
                warn!(
                    "🗑️ Garbage collection failed on one record, ending cycle early: {}",
                    e
                );
                break;
            }
        }
    }
}

pub async fn interval_garbage_collector(db_client: Arc<MongoClient>) {
    let cleanup_interval =
        StdDuration::from_secs(CONFIG.garbage_collector.cleanup_interval_seconds);
    let file_ttl = StdDuration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);

    info!(
        "🗑️ Garbage Collector started. Cycle interval: {:?}, File TTL: {:?}",
        cleanup_interval, file_ttl
    );

    loop {
        sleep(cleanup_interval).await;
        info!("🗑️ Running garbage collection cycle for stale pending uploads...");

        loop {
            match cleanup_one_stale_upload(&db_client, file_ttl).await {
                Ok(Some(session_id)) => {
                    info!("🗑️ Cleaned up stale upload session: {}", session_id);
                }
                Ok(None) => {
                    info!("🗑️ No more stale pending uploads found in this cycle.");
                    break;
                }
                Err(e) => {
                    warn!(
                        "🗑️ Garbage collection failed on one record, ending cycle early: {}",
                        e
                    );
                    break;
                }
            }
        }
    }
}

async fn cleanup_one_stale_upload(
    db: &MongoClient,
    ttl: StdDuration,
) -> Result<Option<String>, AppError> {
    let collection = db
        .database(OBJECT_STORAGE_DATABASE)
        .collection::<ObjectMetadata>("objects");

    let stale_threshold =
        Utc::now() - Duration::from_std(ttl).expect("Failed to convert TTL to chrono::Duration");

    let filter = doc! {
        "status": "Pending",
        "created_at": { "$lt": Bson::DateTime(std::time::SystemTime::from(stale_threshold).into()) }
    };

    let stale_record = collection.find_one_and_delete(filter).await?;

    if let Some(record) = stale_record {
        if let Some(session_id) = &record.session_id {
            let paths = ObjectPathManager::new(session_id);
            let temp_upload_dir = paths.temp_upload_dir();

            if let Ok(content) = fs::read_to_string(paths.metadata_path()).await {
                if let Ok(temp_metadata) = serde_json::from_str::<TempUploadMetadata>(&content) {
                    for i in 0..temp_metadata.total_chunks {
                        let chunk_key = format!("{}-{}", temp_metadata.file_id, i);
                        CHUNK_WRITE_LOCKS.remove(&chunk_key);
                    }
                    info!(
                        "🗑️ Cleaned up {} in-memory chunk locks for session {}",
                        temp_metadata.total_chunks, session_id
                    );
                }
            } else {
                warn!(
                    "🗑️ Could not read temp metadata for session {}, unable to clean up chunk locks.",
                    session_id
                );
            }

            if temp_upload_dir.exists() {
                if let Err(e) = fs::remove_dir_all(&temp_upload_dir).await {
                    warn!(
                        "🗑️ Failed to delete stale temp directory {:?} for session {}: {}. The metadata record has already been deleted.",
                        temp_upload_dir, session_id, e
                    );
                }
            }
            Ok(Some(session_id.clone()))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}
