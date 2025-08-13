use crate::fs::fs_constants::{CLEANUP_INTERVAL, FILE_TTL};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tokio::{fs, io};
use tracing::{info, warn};
use walkdir::WalkDir;

pub async fn cleanup_stale_uploads() {
    loop {
        sleep(CLEANUP_INTERVAL).await;

        let root = PathBuf::from("storage-files");

        for entry in WalkDir::new(&root)
            .min_depth(1)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_dir())
        {
            let dir_path = entry.path().to_path_buf();

            if let Ok(true) = is_unfinished_upload(&dir_path, FILE_TTL).await {
                if let Err(e) = fs::remove_dir_all(&dir_path).await {
                    warn!("⚠️ Failed to delete {:?}: {:?}", dir_path, e);
                } else {
                    info!("✅ Deleted stale unfinished upload: {:?}", dir_path);
                }
            } else if let Err(e) = is_unfinished_upload(&dir_path, FILE_TTL).await {
                warn!("⚠️ Could not check {:?}: {:?}", dir_path, e);
            }
        }
    }
}

async fn is_unfinished_upload(dir: &std::path::Path, ttl: Duration) -> Result<bool, io::Error> {
    let mut oldest_chunk_time: Option<SystemTime> = None;
    let mut found_chunk = false;

    let mut entries = fs::read_dir(dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();

        if file_name.starts_with("chunk_") {
            found_chunk = true;
            let metadata = entry.metadata().await?;
            if let Ok(modified) = metadata.modified() {
                oldest_chunk_time = Some(match oldest_chunk_time {
                    Some(oldest) => oldest.min(modified),
                    None => modified,
                });
            }
        }
    }

    if !found_chunk {
        return Ok(false);
    }

    if let Some(time) = oldest_chunk_time {
        let age = SystemTime::now()
            .duration_since(time)
            .unwrap_or(Duration::ZERO);
        return Ok(age > ttl);
    }

    Ok(false)
}
