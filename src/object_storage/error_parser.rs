use std::path::PathBuf;
use tokio::fs;
use tracing::error;
use crate::enums::app_error_enum::AppError;

pub async fn physical_path_error(physical_path: &PathBuf) -> Result<(), AppError> {
    if !fs::try_exists(&physical_path).await.unwrap_or(false) {
        error!(
            "CRITICAL: Metadata found but physical file is missing at {}",
            physical_path.display()
        );
        return Err(AppError::Conflict("Object data is missing.".to_string()));
    }

    Ok(())
}