use axum::{http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::error::Error;
use tracing::{error, info, warn};
use walkdir::WalkDir;

use crate::{
    enums::app_error_enum::AppError,
    n1_core::{disk_manager::ObjectPathManager, index, quota_manager::set_storage_used},
    structs::auth_structs::Claims,
};

#[derive(Deserialize)]
pub struct CreateFragmentRequest {
    pub name: String,
    pub quota: u64,
    pub secret: String,
    pub key: String,
}

#[derive(Serialize)]
pub struct CreateFragmentResponse {
    pub fragment_hash: String,
    pub name: String,
    pub quota: u64,
}

pub async fn ensure_fragment_blob_directories(fragment_hash: &str) {
    let _ = fragment_hash;
}

async fn sweep_fragment_gc_files(fragment_hash: String) {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();

    tokio::task::spawn_blocking(move || {
        for root in blob_roots {
            let fragment_root = root.join(&fragment_hash);
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

async fn calculate_usage_across_mirrors(fragment_hash: String) -> u64 {
    let mut total_usage = 0u64;
    let mut seen_versions = HashSet::new();
    for trashed in [false, true] {
        let mut stream = index::stream_fragment_object_versions(fragment_hash.clone(), trashed);
        while let Some(next_meta) = stream.recv().await {
            let Ok(meta) = next_meta else {
                continue;
            };
            if meta.is_delete_marker || !seen_versions.insert(meta.version_id.clone()) {
                continue;
            }
            total_usage += meta.total_size_bytes;
        }
    }

    total_usage
}

async fn recover_fragment_name_from_disk(fragment_hash: String) -> Option<String> {
    Some(fragment_hash)
}

pub async fn sync_quota() -> Result<(), Box<dyn Error + Send + Sync>> {
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    let mut processed_hashes = HashSet::new();
    let mut count = 0;

    for root in index_roots {
        let mut entries = match tokio::fs::read_dir(&root).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(file_type) = entry.file_type().await {
                if file_type.is_dir() {
                    let fragment_hash = entry.file_name().to_string_lossy().to_string();

                    if processed_hashes.contains(&fragment_hash) {
                        continue;
                    }
                    processed_hashes.insert(fragment_hash.clone());

                    let readable_name = recover_fragment_name_from_disk(fragment_hash.clone())
                        .await
                        .unwrap_or_else(|| "Unknown/Empty".to_string());

                    ensure_fragment_blob_directories(&fragment_hash).await;
                    sweep_fragment_gc_files(fragment_hash.clone()).await;

                    let usage = calculate_usage_across_mirrors(fragment_hash.clone()).await;

                    if let Err(e) = set_storage_used(&fragment_hash, usage).await {
                        warn!(
                            "Failed to update usage for Fragment '{}' (Hash: {}): {}",
                            readable_name, fragment_hash, e
                        );
                    } else {
                        info!(
                            "Synced quota for Fragment '{}' (Hash: {}). Usage: {} bytes",
                            readable_name, fragment_hash, usage
                        );
                    }
                    count += 1;
                }
            }
        }
    }

    info!(
        "Consistency check completed. Synced {} active fragments.",
        count
    );
    Ok(())
}

pub async fn sync_single_fragment_quota(
    fragment: String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    ensure_fragment_blob_directories(&fragment).await;

    sweep_fragment_gc_files(fragment.clone()).await;

    let calculated_usage = calculate_usage_across_mirrors(fragment.clone()).await;

    if let Err(e) = set_storage_used(&fragment, calculated_usage).await {
        warn!("Failed to update usage stats for {}: {}", fragment, e);
    }

    info!("Fragment-specific quota sync completed for {}", fragment);
    Ok(())
}

pub async fn initialize_storage(fragment_hash: String) -> Result<(), Box<dyn Error + Send + Sync>> {
    ensure_fragment_blob_directories(&fragment_hash).await;

    let calculated_usage = calculate_usage_across_mirrors(fragment_hash.clone()).await;
    let _ = set_storage_used(&fragment_hash, calculated_usage).await;

    info!(
        "Initialized storage structure for fragment {}",
        &fragment_hash
    );
    Ok(())
}

pub async fn sync_quota_api() -> impl IntoResponse {
    tokio::spawn(async move {
        match sync_quota().await {
            Ok(_) => info!("Manual global quota sync completed."),
            Err(e) => error!("Manual global quota sync failed: {}", e),
        }
    });

    (
        StatusCode::ACCEPTED,
        "Global quota sync started in background.",
    )
}

pub async fn sync_fragment_quota_api(claims: Claims) -> Result<impl IntoResponse, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden(
            "Modify permission required to sync quota".into(),
        ));
    }

    let fragment = claims.fragment.clone();

    tokio::spawn(async move {
        match sync_single_fragment_quota(fragment.clone()).await {
            Ok(_) => info!(
                "Fragment quota sync completed successfully for {}.",
                fragment
            ),
            Err(e) => error!("Fragment quota sync failed for {}: {}", fragment, e),
        }
    });

    Ok(StatusCode::ACCEPTED)
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
