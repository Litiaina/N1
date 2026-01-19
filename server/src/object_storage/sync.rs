use axum::{extract::State, http::StatusCode, response::IntoResponse};
use futures_util::StreamExt;
use mongodb::{Client, bson::doc};
use std::{error::Error, sync::Arc, time::Duration};
use tokio::time::timeout;
use tracing::{error, info, warn};
use walkdir::WalkDir;

use crate::{
    database::database_constants::DATABASE,
    object_storage::constants::OBJECT_DATA_ROOT,
    object_storage::quota_manager::{init_quota_file, set_storage_used},
    routes::storage_routes::AppState,
    structs::{
        auth_structs::Claims,
        object_storage_structs::{ObjectMetadata, ObjectPathManager, UserRecord},
    },
};

const DISK_OP_TIMEOUT: Duration = Duration::from_secs(2);

async fn ensure_user_blob_directories(uid: &str) {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();

    for root in blob_roots {
        let blob_dir = root.join(OBJECT_DATA_ROOT).join(uid);

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

async fn sweep_user_gc_files(uid: String) {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();

    tokio::task::spawn_blocking(move || {
        for root in blob_roots {
            let user_root = root.join(OBJECT_DATA_ROOT).join(&uid);
            if !user_root.exists() {
                continue;
            }

            for entry in WalkDir::new(&user_root).into_iter().filter_map(|e| e.ok()) {
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

async fn calculate_usage_across_mirrors(uid: String) -> u64 {
    let path_manager = ObjectPathManager::new();
    let user_dirs = path_manager.get_all_user_index_dirs(&uid);

    tokio::task::spawn_blocking(move || {
        let mut max_total_usage = 0u64;

        for user_dir in user_dirs {
            if !user_dir.exists() {
                continue;
            }

            let mut current_disk_usage = 0u64;

            for entry in WalkDir::new(&user_dir).into_iter().filter_map(|e| e.ok()) {
                if entry.file_type().is_file()
                    && entry.file_name().to_string_lossy().ends_with(".bson")
                {
                    if entry.file_name() == "quota.bson" {
                        continue;
                    }

                    if let Ok(data) = std::fs::read(entry.path()) {
                        if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                            if !meta.is_delete_marker {
                                current_disk_usage += meta.total_size_bytes;
                            }
                        }
                    }
                }
            }

            if current_disk_usage > max_total_usage {
                max_total_usage = current_disk_usage;
            }
        }

        max_total_usage
    })
    .await
    .unwrap_or(0)
}

pub async fn sync_quota(db_client: Arc<Client>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let db = db_client.database(DATABASE);
    let auth_collection = db.collection::<UserRecord>("auth");
    let mut cursor = auth_collection.find(doc! {}).await?;
    let mut count = 0;

    while let Some(result) = cursor.next().await {
        match result {
            Ok(user) => {
                let uid = user.uid;

                if let Err(e) = init_quota_file(&uid, user.allocated_storage).await {
                    warn!("Partial failure syncing quota file for {}: {}", &uid, e);
                }

                ensure_user_blob_directories(&uid).await;

                sweep_user_gc_files(uid.clone()).await;

                let calculated_usage = calculate_usage_across_mirrors(uid.clone()).await;

                if let Err(e) = set_storage_used(&uid, calculated_usage).await {
                    warn!("Failed to update usage stats for {}: {}", &uid, e);
                }

                count += 1;
            }
            Err(e) => {
                error!("Failed to retrieve user document during sync: {}", e);
            }
        }
    }

    info!("Consistency check completed for {} users.", count);

    Ok(())
}

pub async fn sync_single_user_quota(
    db_client: Arc<Client>,
    uid: Arc<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let db = db_client.database(DATABASE);
    let auth_collection = db.collection::<UserRecord>("auth");

    match auth_collection
        .find_one(doc! { "uid": &uid.as_str() })
        .await?
    {
        Some(user) => {
            if let Err(e) = init_quota_file(&uid, user.allocated_storage).await {
                warn!("Partial failure syncing quota file for {}: {}", &uid, e);
            }

            ensure_user_blob_directories(&uid).await;

            sweep_user_gc_files(uid.to_string()).await;

            let calculated_usage = calculate_usage_across_mirrors(uid.to_string()).await;

            if let Err(e) = set_storage_used(&uid, calculated_usage).await {
                warn!("Failed to update usage stats for {}: {}", &uid, e);
            }
        }
        None => {
            warn!(
                "User {} not found in database during specific quota sync.",
                &uid
            );
        }
    }

    info!("User-specific quota sync completed for {}", &uid);
    Ok(())
}

pub async fn initialize_storage(
    db_client: Arc<Client>,
    uid: Arc<String>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let db = db_client.database(DATABASE);
    let auth_collection = db.collection::<UserRecord>("auth");
    let path_manager = ObjectPathManager::new();

    match auth_collection
        .find_one(doc! { "uid": &uid.as_str() })
        .await?
    {
        Some(user) => {
            let user_index_dirs = path_manager.get_all_user_index_dirs(&uid);
            let mut quota_exists = false;

            for dir in &user_index_dirs {
                if tokio::fs::try_exists(dir.join("quota.bson"))
                    .await
                    .unwrap_or(false)
                {
                    quota_exists = true;
                    break;
                }
            }

            if !quota_exists {
                if let Err(e) = init_quota_file(&uid, user.allocated_storage).await {
                    warn!("Partial failure init quota file for {}: {}", &uid, e);
                }

                let calculated_usage = calculate_usage_across_mirrors(uid.to_string()).await;
                if let Err(e) = set_storage_used(&uid, calculated_usage).await {
                    warn!("Failed to update usage stats for {}: {}", &uid, e);
                }
            }

            ensure_user_blob_directories(&uid).await;
        }
        None => {
            warn!(
                "User {} not found in database during specific quota sync.",
                &uid
            );
        }
    }

    info!("Initialized storage for {}", &uid);
    Ok(())
}

pub async fn sync_quota_api(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let db_client = state.db_client.clone();

    tokio::spawn(async move {
        match sync_quota(db_client).await {
            Ok(_) => info!("Manual global quota sync completed."),
            Err(e) => error!("Manual global quota sync failed: {}", e),
        }
    });

    (
        StatusCode::ACCEPTED,
        "Global quota sync started in background.",
    )
}

pub async fn sync_user_quota_api(
    State(state): State<Arc<AppState>>,
    claims: Claims,
) -> impl IntoResponse {
    let uid = Arc::new(claims.uid);
    let db_client = Arc::clone(&state.db_client);

    tokio::spawn(async move {
        match sync_single_user_quota(db_client, uid.clone()).await {
            Ok(_) => info!("User quota sync completed successfully for {}.", &uid),
            Err(e) => error!("User quota sync failed for {}: {}", &uid, e),
        }
    });

    StatusCode::ACCEPTED
}

pub async fn initialize_user_storage_api(
    State(state): State<Arc<AppState>>,
    claims: Claims,
) -> impl IntoResponse {
    let uid = Arc::new(claims.uid);
    let db_client = Arc::clone(&state.db_client);

    tokio::spawn(async move {
        if let Err(e) = initialize_storage(db_client, uid.clone()).await {
            error!("Storage initialization failed for {}: {}", uid, e);
        }
    });

    StatusCode::ACCEPTED
}
