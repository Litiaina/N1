use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{OBJECT_DATA_ROOT, OBJECT_META_FILE};
use crate::settings::config::CONFIG;
use crate::structs::object_storage_structs::OBJECT_STORAGE_ROOT;
use mongodb::Client as MongoClient;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::time::sleep;
use tracing::{error, info, warn};

pub async fn run_garbage_collector(_db_client: Arc<MongoClient>) {
    let file_ttl = Duration::from_secs(60);

    info!("🗑️ GC (One-Time) executing. TTL: {:?}", file_ttl);

    if let Err(e) = perform_filesystem_cleanup(file_ttl).await {
        error!("🗑️ GC One-Time run failed: {}", e);
    } else {
        info!("🗑️ GC (One-Time) completed successfully.");
    }
}

pub async fn interval_garbage_collector(_db_client: Arc<MongoClient>) {
    let cleanup_interval = Duration::from_secs(CONFIG.garbage_collector.cleanup_interval_seconds);
    let file_ttl = Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);

    info!(
        "🗑️ GC (Interval) started. Interval: {:?}, TTL: {:?}",
        cleanup_interval, file_ttl
    );

    loop {
        sleep(cleanup_interval).await;
        if let Err(e) = perform_filesystem_cleanup(file_ttl).await {
            error!("🗑️ GC Cycle failed: {}", e);
        }
    }
}

async fn perform_filesystem_cleanup(ttl: Duration) -> Result<(), AppError> {
    let root_str = OBJECT_STORAGE_ROOT
        .get()
        .ok_or_else(|| AppError::InternalServerError("Storage root not initialized".to_string()))?;

    let storage_root = PathBuf::from(root_str).join(OBJECT_DATA_ROOT);

    if !fs::try_exists(&storage_root).await.unwrap_or(false) {
        return Ok(());
    }

    let mut uid_entries = match fs::read_dir(&storage_root).await {
        Ok(entries) => entries,
        Err(e) => {
            error!("Failed to read storage root: {}", e);
            return Err(AppError::Io(e, "read storage root".into()));
        }
    };

    while let Ok(Some(entry)) = uid_entries.next_entry().await {
        let path = entry.path();
        if entry
            .file_type()
            .await
            .map(|ft| ft.is_dir())
            .unwrap_or(false)
        {
            let _ = process_uid_directory(&path, ttl).await;
            tokio::task::yield_now().await;
        }
    }

    Ok(())
}

async fn process_uid_directory(uid_path: &Path, ttl: Duration) -> Result<(), AppError> {
    let mut entries = match fs::read_dir(uid_path).await {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let is_dir = entry
            .file_type()
            .await
            .map(|ft| ft.is_dir())
            .unwrap_or(false);

        if is_dir {
            let _ = process_fragment_directory(&path, ttl).await;
        }
    }

    Ok(())
}

async fn process_fragment_directory(frag_path: &Path, ttl: Duration) -> Result<bool, AppError> {
    let is_empty = {
        let mut entries = match fs::read_dir(frag_path).await {
            Ok(e) => e,
            Err(_) => return Ok(true),
        };

        let mut empty = true;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let is_dir = entry
                .file_type()
                .await
                .map(|ft| ft.is_dir())
                .unwrap_or(false);

            if is_dir {
                if !process_key_directory(&path, ttl).await? {
                    empty = false;
                }
            } else {
                empty = false;
            }
        }
        empty
    };

    if is_empty {
        info!("🗑️ Removing empty fragment directory: {:?}", frag_path);
        if let Err(e) = fs::remove_dir(frag_path).await {
            warn!("Failed to remove empty fragment dir {:?}: {}", frag_path, e);
            return Ok(false);
        }
        return Ok(true);
    }

    Ok(false)
}

async fn process_key_directory(key_path: &Path, ttl: Duration) -> Result<bool, AppError> {
    let is_empty = {
        let mut entries = match fs::read_dir(key_path).await {
            Ok(e) => e,
            Err(_) => return Ok(true),
        };

        let mut empty = true;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let is_dir = entry
                .file_type()
                .await
                .map(|ft| ft.is_dir())
                .unwrap_or(false);

            if is_dir {
                if !check_and_clean_version(&path, ttl).await {
                    empty = false;
                }
            } else {
                empty = false;
            }
        }
        empty
    };

    if is_empty {
        info!("🗑️ Removing empty object key directory: {:?}", key_path);
        if let Err(e) = fs::remove_dir(key_path).await {
            warn!("Failed to remove empty key dir {:?}: {}", key_path, e);
            return Ok(false);
        }
        return Ok(true);
    }

    Ok(false)
}

async fn check_and_clean_version(version_path: &Path, ttl: Duration) -> bool {
    let meta_path = version_path.join(OBJECT_META_FILE);

    if fs::try_exists(&meta_path).await.unwrap_or(false) {
        return false;
    }

    let metadata = match fs::metadata(version_path).await {
        Ok(m) => m,
        Err(_) => return false,
    };

    let modified = match metadata.modified() {
        Ok(t) => t,
        Err(_) => return false,
    };

    let now = SystemTime::now();
    let age = match now.duration_since(modified) {
        Ok(d) => d,
        Err(_) => Duration::from_secs(0),
    };

    if age > ttl {
        info!(
            "🗑️ Found stale incomplete version at {:?} (Age: {:?}). Deleting...",
            version_path, age
        );
        if let Err(e) = fs::remove_dir_all(version_path).await {
            warn!("Failed to delete stale version {:?}: {}", version_path, e);
            return false;
        }
        return true;
    }

    false
}
