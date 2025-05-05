use std::path::PathBuf;
use tokio::fs;
use tokio::time::sleep;
use tracing::{info, warn};
use walkdir::WalkDir;
use crate::fs::fragmented_upload::is_unfinished_upload;
use crate::fs::fs_constants::{CLEANUP_INTERVAL, FILE_TTL};

pub async fn cleanup_stale_uploads() {
    loop {
        sleep(CLEANUP_INTERVAL).await;

        let root = PathBuf::from("storage-files");

        for entry in WalkDir::new(&root)
            .min_depth(2)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_dir())
        {
            let dir_path = entry.path().to_path_buf();

            match is_unfinished_upload(&dir_path, FILE_TTL).await {
                Ok(true) => {
                    match fs::remove_dir_all(&dir_path).await {
                        Ok(_) => info!("✅ Deleted stale unfinished upload: {:?}", dir_path),
                        Err(e) => warn!("⚠️ Failed to delete {:?}: {:?}", dir_path, e),
                    }
                }
                Ok(false) => {
                    info!("⏩ Skipped (not stale or finalized): {:?}", dir_path);
                }
                Err(e) => {
                    warn!("⚠️ Could not check {:?}: {:?}", dir_path, e);
                }
            }
        }
    }
}
