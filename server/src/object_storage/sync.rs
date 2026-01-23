use axum::{extract::State, http::StatusCode, response::IntoResponse};
use futures_util::StreamExt;
use mongodb::{Client, bson::doc};
use std::{
    collections::HashMap,
    error::Error,
    path::{Path as StdPath, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};
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

fn parse_version_from_meta_filename(name: &str) -> Option<&str> {
    if !name.ends_with(".bson") {
        return None;
    }
    let (_, ver_ext) = name.split_once('#')?;
    let ver = ver_ext.strip_suffix(".bson")?;
    if ver.is_empty() {
        return None;
    }
    Some(ver)
}

#[cfg(unix)]
fn sync_dir_std(path: &StdPath) {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;
    if let Ok(f) = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path)
    {
        let _ = f.sync_all();
    }
}

#[cfg(windows)]
fn sync_dir_std(path: &StdPath) {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;
    if let Ok(f) = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
    {
        let _ = f.sync_all();
    }
}

#[cfg(not(any(unix, windows)))]
fn sync_dir_std(_path: &StdPath) {}

async fn filter_reachable(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for p in paths {
        let ok = timeout(DISK_OP_TIMEOUT, tokio::fs::try_exists(&p)).await;
        match ok {
            Ok(Ok(true)) => out.push(p),
            Ok(Ok(false)) => {}
            Ok(Err(e)) => warn!("Failed try_exists {:?}: {}", p, e),
            Err(_) => warn!("Timeout try_exists {:?}. Skipping.", p),
        }
    }
    out
}

fn remove_dir_strict(dir: &StdPath) -> bool {
    if !dir.exists() {
        return true;
    }
    let parent = match dir.parent() {
        Some(p) => p,
        None => return false,
    };

    let gc_dir = PathBuf::from(format!("{}.gcdir", dir.to_string_lossy()));
    if !gc_dir.exists() {
        if std::fs::rename(dir, &gc_dir).is_err() {
            return false;
        }
        sync_dir_std(parent);
    }

    let _ = std::fs::remove_dir_all(&gc_dir);
    sync_dir_std(parent);
    !dir.exists()
}

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
                        warn!("Timeout creating blob dir {:?}. Skipping disk.", blob_dir);
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
                warn!("Timeout checking {:?}. Skipping disk.", blob_dir);
            }
        }
    }
}

async fn calculate_usage_across_mirrors(uid: String) -> u64 {
    let path_manager = ObjectPathManager::new();
    let user_dirs = path_manager.get_all_user_index_dirs(&uid);
    let reachable_user_dirs = filter_reachable(user_dirs).await;

    tokio::task::spawn_blocking(move || {
        let mut version_candidates: HashMap<String, Vec<(SystemTime, PathBuf)>> = HashMap::new();

        for user_dir in reachable_user_dirs {
            if !user_dir.exists() {
                continue;
            }

            let fragments = match std::fs::read_dir(&user_dir) {
                Ok(r) => r,
                Err(_) => continue,
            };

            for frag_entry in fragments.filter_map(|e| e.ok()) {
                if !frag_entry.path().is_dir() {
                    continue;
                }

                let index_dir = frag_entry.path().join("index");
                if !index_dir.exists() {
                    continue;
                }

                for entry in WalkDir::new(&index_dir).into_iter().filter_map(|e| e.ok()) {
                    if !entry.file_type().is_file() {
                        continue;
                    }

                    let name = entry.file_name().to_string_lossy().to_string();
                    if name == "quota.bson"
                        || name == "reservation.bson"
                        || name.ends_with(".partmeta")
                    {
                        continue;
                    }

                    let Some(ver) = parse_version_from_meta_filename(&name) else {
                        continue;
                    };

                    let modified = entry
                        .metadata()
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .unwrap_or(SystemTime::UNIX_EPOCH);

                    let v = version_candidates.entry(ver.to_string()).or_default();
                    v.push((modified, entry.path().to_path_buf()));
                    v.sort_by(|a, b| b.0.cmp(&a.0));
                    if v.len() > 3 {
                        v.truncate(3);
                    }
                }
            }
        }

        let mut total = 0u64;

        for (_ver, paths) in version_candidates {
            let mut added = false;
            for (_mtime, p) in paths {
                if let Ok(data) = std::fs::read(&p) {
                    if let Ok(meta) = mongodb::bson::from_slice::<ObjectMetadata>(&data) {
                        total = total.saturating_add(meta.total_size_bytes);
                        added = true;
                        break;
                    }
                }
            }
            if !added {
                continue;
            }
        }

        total
    })
    .await
    .unwrap_or(0)
}

async fn cleanup_orphan_upload_sessions(uid: String) {
    let path_manager = ObjectPathManager::new();
    let user_index_dirs = path_manager.get_all_user_index_dirs(&uid);
    let reachable_index_dirs = filter_reachable(user_index_dirs).await;

    let blob_roots = path_manager.get_blob_roots();
    let mut blob_user_roots = Vec::new();
    for root in blob_roots {
        blob_user_roots.push(root.join(OBJECT_DATA_ROOT).join(&uid));
    }
    let reachable_blob_roots = filter_reachable(blob_user_roots).await;

    let uid_clone = uid.clone();

    let valid_versions = tokio::task::spawn_blocking(move || -> HashMap<String, ()> {
        let mut seen: HashMap<String, ()> = HashMap::new();

        for user_dir in reachable_index_dirs {
            if !user_dir.exists() {
                continue;
            }
            let fragments = match std::fs::read_dir(&user_dir) {
                Ok(r) => r,
                Err(_) => continue,
            };
            for frag_entry in fragments.filter_map(|e| e.ok()) {
                if !frag_entry.path().is_dir() {
                    continue;
                }
                let index_dir = frag_entry.path().join("index");
                if !index_dir.exists() {
                    continue;
                }
                for entry in WalkDir::new(&index_dir).into_iter().filter_map(|e| e.ok()) {
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    let name = entry.file_name().to_string_lossy().to_string();
                    let Some(ver) = parse_version_from_meta_filename(&name) else {
                        continue;
                    };
                    seen.insert(ver.to_string(), ());
                }
            }
        }

        seen
    })
    .await
    .unwrap_or_default();

    tokio::task::spawn_blocking(move || {
        let mut roots = Vec::new();
        roots.extend(reachable_blob_roots);

        for user_root in roots {
            if !user_root.exists() {
                continue;
            }

            let fragments = match std::fs::read_dir(&user_root) {
                Ok(r) => r,
                Err(_) => continue,
            };

            for frag_entry in fragments.filter_map(|e| e.ok()) {
                if !frag_entry.path().is_dir() {
                    continue;
                }

                let blobs_dir = frag_entry.path().join("blobs");
                if !blobs_dir.exists() {
                    continue;
                }

                let blob_entries = match std::fs::read_dir(&blobs_dir) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                for blob_entry in blob_entries.filter_map(|e| e.ok()) {
                    let path = blob_entry.path();
                    if !path.is_dir() {
                        continue;
                    }

                    let version_id = blob_entry.file_name().to_string_lossy().to_string();
                    if valid_versions.contains_key(&version_id) {
                        continue;
                    }

                    let receipt = path.join("reservation.bson");
                    if !receipt.exists() {
                        continue;
                    }

                    let _ = remove_dir_strict(&path);
                }
            }
        }

        let _ = uid_clone;
    })
    .await
    .ok();
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

                cleanup_orphan_upload_sessions(uid.clone()).await;

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

            cleanup_orphan_upload_sessions(uid.to_string()).await;

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
                if timeout(
                    DISK_OP_TIMEOUT,
                    tokio::fs::try_exists(dir.join("quota.bson")),
                )
                .await
                .ok()
                .and_then(|r| r.ok())
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
