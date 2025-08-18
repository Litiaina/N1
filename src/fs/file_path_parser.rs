use std::path::{Path, PathBuf};
use tokio::fs;
use crate::enums::app_error_enum::AppError;

pub async fn file_path(uid: &String, relative_path: &String, file_name: &String) -> PathBuf {
    if relative_path.is_empty() {
        PathBuf::from(format!(
            "storage-files/{}/{}",
            uid, file_name
        ))
    } else {
        PathBuf::from(format!(
            "storage-files/{}/{}/{}",
            uid, relative_path, file_name
        ))
    }
}

pub async fn find_unique_path(
    base_dir: &Path,
    stem: &str,
    ext: &str,
    max_attempts: usize,
) -> Result<Option<PathBuf>, AppError> {
    for i in 1..=max_attempts {
        let new_name = format!("{} ({}){}", stem, i, ext);
        let candidate = base_dir.join(&new_name);

        if !fs::try_exists(&candidate)
            .await
            .map_err(|e| AppError::from((e, "checking candidate path")))?
        {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}