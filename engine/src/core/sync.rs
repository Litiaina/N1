use axum::{http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::timeout;
use tracing::{error, info, warn};
use walkdir::WalkDir;

use crate::{
    core::{
        constants::N2_BLOB_DIR, disk_manager::ObjectPathManager,
        fragment_identity::physical_fragment_root_on,
    },
    enums::app_error_enum::AppError,
    structs::auth_structs::Claims,
};

const DISK_OP_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
pub struct CreateFragmentRequest {
    pub name: String,
    pub secret: String,
    pub key: String,
}

#[derive(Serialize)]
pub struct CreateFragmentResponse {
    pub fragment_hash: String,
    pub stable_fragment_id: String,
    pub name: String,
}

pub async fn ensure_fragment_blob_directories(fragment_hash: &str) {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_storage_roots();

    for root in blob_roots {
        let blob_dir = physical_fragment_root_on(&root, fragment_hash).join(N2_BLOB_DIR);

        let exists_check = timeout(DISK_OP_TIMEOUT, tokio::fs::try_exists(&blob_dir)).await;

        match exists_check {
            Ok(Ok(exists)) => {
                if !exists {
                    if let Err(_) =
                        timeout(DISK_OP_TIMEOUT, tokio::fs::create_dir_all(&blob_dir)).await
                    {
                        warn!(
                            "Timeout creating blob dir {:?}. Disk might be slow or dead.",
                            blob_dir
                        );
                        continue;
                    }
                }
            }
            Ok(Err(e)) => {
                warn!(
                    "Failed to check existence of {:?}: {}. Skipping disk.",
                    blob_dir, e
                );
            }
            Err(_) => {
                warn!("Timeout checking {:?}. Disk might be dead.", blob_dir);
            }
        }
    }
}

pub async fn ensure_fragment_blob_directories_strict(fragment_hash: &str) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_storage_roots();

    if blob_roots.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "no storage devices configured for fragment provisioning".into(),
        ));
    }

    for root in blob_roots {
        let blob_dir = physical_fragment_root_on(&root, fragment_hash).join(N2_BLOB_DIR);

        match timeout(DISK_OP_TIMEOUT, tokio::fs::try_exists(&blob_dir)).await {
            Ok(Ok(false)) => {
                timeout(DISK_OP_TIMEOUT, tokio::fs::create_dir_all(&blob_dir))
                    .await
                    .map_err(|_| {
                        AppError::ServiceUnavailable(format!(
                            "timed out creating storage device root {}",
                            blob_dir.display()
                        ))
                    })?
                    .map_err(|e| {
                        AppError::ServiceUnavailable(format!(
                            "failed to create storage device root {}: {}",
                            blob_dir.display(),
                            e
                        ))
                    })?;
            }
            Ok(Ok(true)) => {}
            Ok(Err(e)) => {
                return Err(AppError::ServiceUnavailable(format!(
                    "failed to inspect storage device root {}: {}",
                    blob_dir.display(),
                    e
                )));
            }
            Err(_) => {
                return Err(AppError::ServiceUnavailable(format!(
                    "timed out inspecting storage device root {}",
                    blob_dir.display()
                )));
            }
        }

        let metadata = timeout(DISK_OP_TIMEOUT, tokio::fs::metadata(&blob_dir))
            .await
            .map_err(|_| {
                AppError::ServiceUnavailable(format!(
                    "timed out verifying storage device root {}",
                    blob_dir.display()
                ))
            })?
            .map_err(|e| {
                AppError::ServiceUnavailable(format!(
                    "failed to verify storage device root {}: {}",
                    blob_dir.display(),
                    e
                ))
            })?;

        if !metadata.is_dir() {
            return Err(AppError::ServiceUnavailable(format!(
                "storage device root {} is not a directory",
                blob_dir.display()
            )));
        }
    }

    Ok(())
}

async fn sweep_fragment_gc_files(fragment_hash: String) {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_storage_roots();

    tokio::task::spawn_blocking(move || {
        for root in blob_roots {
            let fragment_root = physical_fragment_root_on(&root, &fragment_hash);
            if !fragment_root.exists() {
                continue;
            }

            for entry in WalkDir::new(&fragment_root)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if !entry.file_type().is_file() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy();
                if name.ends_with(".gc") {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    })
    .await
    .ok();
}

pub async fn initialize_storage(fragment_hash: String) -> Result<(), AppError> {
    ensure_fragment_blob_directories(&fragment_hash).await;
    sweep_fragment_gc_files(fragment_hash.clone()).await;

    info!(
        "Initialized storage structure for fragment {}.",
        &fragment_hash
    );
    Ok(())
}

pub async fn initialize_fragment_storage_api(
    claims: Claims,
) -> Result<impl IntoResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden(
            "Write permission required to initialize storage".into(),
        ));
    }

    let fragment = claims.fragment.clone();

    tokio::spawn(async move {
        if let Err(e) = initialize_storage(fragment.clone()).await {
            error!("Storage initialization failed for {}: {}", fragment, e);
        }
    });

    Ok(StatusCode::ACCEPTED)
}
