use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tokio::{fs, io};
use tracing::{info, warn};
use walkdir::WalkDir;

use crate::settings::config::CONFIG;

pub async fn start_cleanup(source_path: &str) {
    let cleanup_interval = Duration::from_secs(3);
    let file_ttl = Duration::from_secs(3);
    info!(
        "File system cleaning in: {:?}, File TTL: {:?}",
        cleanup_interval, file_ttl
    );
    sleep(cleanup_interval).await;
    cleanup(source_path, file_ttl).await;
}

pub async fn interval_cleanup(source_path: &str) {
    loop {
        let cleanup_interval =
            Duration::from_secs(CONFIG.garbage_collector.cleanup_interval_seconds);
        let file_ttl = Duration::from_secs(CONFIG.garbage_collector.file_ttl_seconds);
        sleep(cleanup_interval).await;
        cleanup(source_path, file_ttl).await;
    }
}

async fn cleanup(source_path: &str, file_ttl: Duration) {
    let root = PathBuf::from(source_path);

    for entry in WalkDir::new(&root)
        .min_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_dir())
    {
        let dir_path = entry.path().to_path_buf();

        match is_unfinished_upload(&dir_path, file_ttl).await {
            Ok(true) => {
                if let Err(e) = fs::remove_dir_all(&dir_path).await {
                    warn!("⚠️ Failed to delete {:?}: {:?}", dir_path, e);
                } else {
                    info!("✅ Deleted stale unfinished upload: {:?}", dir_path);
                }
            }
            Ok(false) => {
                info!(
                    "ℹ️ Skipping directory (no chunks or metadata.json, or not stale): {:?}",
                    dir_path
                );
            }
            Err(e) => {
                warn!("⚠️ Could not check directory {:?}: {:?}", dir_path, e);
            }
        }
    }
}

async fn is_unfinished_upload(dir: &std::path::Path, ttl: Duration) -> Result<bool, io::Error> {
    let mut oldest_time: Option<SystemTime> = None;
    let mut found_relevant_file = false;

    let mut entries = fs::read_dir(dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let file_name_os = entry.file_name();
        let file_name = file_name_os.to_string_lossy();

        if file_name.starts_with("chunk_") || file_name == "metadata.json" {
            found_relevant_file = true;
            let metadata = entry.metadata().await?;
            if let Ok(modified) = metadata.modified() {
                oldest_time = Some(match oldest_time {
                    Some(oldest) => oldest.min(modified),
                    None => modified,
                });
            }
        }
    }

    if !found_relevant_file {
        return Ok(false);
    }

    if let Some(time) = oldest_time {
        let age = SystemTime::now()
            .duration_since(time)
            .unwrap_or(Duration::ZERO);
        return Ok(age > ttl);
    }

    Ok(false)
}
