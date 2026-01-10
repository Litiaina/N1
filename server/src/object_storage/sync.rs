use axum::{extract::State, http::StatusCode, response::IntoResponse};
use futures_util::StreamExt;
use mongodb::{Client, bson::doc};
use std::{error::Error, sync::Arc};
use tracing::{error, info, warn};
use walkdir::WalkDir;

use crate::{
    database::database_constants::DATABASE,
    object_storage::quota_manager::{init_quota_file, set_storage_used},
    routes::storage_routes::AppState,
    structs::{
        auth_structs::Claims,
        object_storage_structs::{ObjectMetadata, ObjectPathManager, UserRecord},
    },
};

pub async fn sync_quota(db_client: Arc<Client>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let db = db_client.database(DATABASE);
    let auth_collection = db.collection::<UserRecord>("auth");
    let mut cursor = auth_collection.find(doc! {}).await?;
    let mut count = 0;
    let path_manager = ObjectPathManager::new();

    while let Some(result) = cursor.next().await {
        match result {
            Ok(user) => {
                let uid = user.uid;

                if let Err(e) = init_quota_file(&uid, user.allocated_storage).await {
                    error!("Failed to sync quota file for {}: {}", &uid, e);
                    continue;
                }

                let user_dirs = path_manager.get_all_user_index_dirs(&uid);

                tokio::spawn(async move {
                    let mut active_dir = None;
                    for dir in user_dirs {
                        if dir.exists() {
                            active_dir = Some(dir);
                            break;
                        }
                    }

                    if let Some(user_dir) = active_dir {
                        let calculated_usage = tokio::task::spawn_blocking(move || {
                            let mut total = 0u64;
                            for entry in WalkDir::new(&user_dir).into_iter().filter_map(|e| e.ok())
                            {
                                if entry.file_type().is_file()
                                    && entry.file_name().to_string_lossy().ends_with(".bson")
                                {
                                    if entry.file_name() == "quota.bson" {
                                        continue;
                                    }

                                    if let Ok(data) = std::fs::read(entry.path()) {
                                        if let Ok(meta) =
                                            mongodb::bson::from_slice::<ObjectMetadata>(&data)
                                        {
                                            if !meta.is_delete_marker {
                                                total += meta.total_size_bytes;
                                            }
                                        }
                                    }
                                }
                            }
                            total
                        })
                        .await;

                        match calculated_usage {
                            Ok(usage) => {
                                if let Err(e) = set_storage_used(&uid, usage).await {
                                    warn!("Failed to update usage stats for {}: {}", &uid, e);
                                }
                            }
                            Err(e) => error!("Failed to calc usage for {}: {}", &uid, e),
                        }
                    }
                });

                count += 1;
            }
            Err(e) => {
                error!("Failed to retrieve user document during sync: {}", e);
            }
        }
    }

    info!(
        "Background Task: Consistency check initiated for {} users (running in background).",
        count
    );

    Ok(())
}

pub async fn sync_single_user_quota(
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
            if let Err(e) = init_quota_file(&uid, user.allocated_storage).await {
                error!("Failed to sync quota file for {}: {}", &uid, e);
                return Ok(());
            }

            let user_dirs = path_manager.get_all_user_index_dirs(&uid);

            let mut active_dir = None;
            for dir in user_dirs {
                if dir.exists() {
                    active_dir = Some(dir);
                    break;
                }
            }

            if let Some(user_dir) = active_dir {
                let calculated_usage = tokio::task::spawn_blocking(move || {
                    let mut total = 0u64;
                    for entry in WalkDir::new(&user_dir).into_iter().filter_map(|e| e.ok()) {
                        if entry.file_type().is_file()
                            && entry.file_name().to_string_lossy().ends_with(".bson")
                        {
                            if entry.file_name() == "quota.bson" {
                                continue;
                            }

                            if let Ok(data) = std::fs::read(entry.path()) {
                                if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data)
                                {
                                    if !meta.is_delete_marker {
                                        total += meta.total_size_bytes;
                                    }
                                }
                            }
                        }
                    }
                    total
                })
                .await?;

                if let Err(e) = set_storage_used(&uid, calculated_usage).await {
                    warn!("Failed to update usage stats for {}: {}", &uid, e);
                }
            }
        }
        None => {
            warn!(
                "User {} not found in database during specific quota sync.",
                &uid
            );
        }
    }

    info!("Background Task: Completed quota sync for {}", &uid);
    Ok(())
}

pub async fn sync_quota_api(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let db_client = state.db_client.clone();

    tokio::spawn(async move {
        match sync_quota(db_client).await {
            Ok(_) => info!("Full quota sync cycle completed successfully."),
            Err(e) => error!("Full quota sync cycle failed: {}", e),
        }
    });

    StatusCode::ACCEPTED
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
