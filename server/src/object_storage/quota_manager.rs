use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{LOCK_SHARD_COUNT, QUOTA_FILE};
use crate::structs::object_storage_structs::{ObjectPathManager, Quota};
use futures_util::future::join_all;
use moka::future::Cache;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::info;

static QUOTA_LOCKS: LazyLock<Vec<Mutex<()>>> = LazyLock::new(|| {
    let mut locks = Vec::with_capacity(LOCK_SHARD_COUNT);
    for _ in 0..LOCK_SHARD_COUNT {
        locks.push(Mutex::new(()));
    }
    locks
});

fn get_quota_lock(uid: &str) -> &Mutex<()> {
    let mut hasher = DefaultHasher::new();
    uid.hash(&mut hasher);
    let idx = (hasher.finish() as usize) % LOCK_SHARD_COUNT;
    &QUOTA_LOCKS[idx]
}

static QUOTA_CACHE: LazyLock<Cache<String, Quota>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(10_000)
        .time_to_idle(Duration::from_secs(600))
        .build()
});

fn get_all_quota_paths(uid: &str) -> Vec<PathBuf> {
    let path_manager = ObjectPathManager::new();
    path_manager
        .get_all_user_index_dirs(uid)
        .into_iter()
        .map(|dir| dir.join(QUOTA_FILE))
        .collect()
}

async fn write_quota_to_all_disks(uid: &str, quota: &Quota) -> Result<(), AppError> {
    let paths = get_all_quota_paths(uid);
    let bytes = Arc::new(
        mongodb::bson::to_vec(quota)
            .map_err(|_| AppError::InternalServerError("Quota serialize error".into()))?,
    );

    let write_futures = paths.into_iter().map(|path| {
        let bytes = bytes.clone();
        async move {
            if let Some(parent) = path.parent() {
                if !fs::try_exists(parent).await.unwrap_or(false) {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| AppError::Io(e, "create user dir".into()))?;
                }
            }

            let tmp_path = path.with_extension("tmp");
            let mut file = File::create(&tmp_path)
                .await
                .map_err(|e| AppError::Io(e, "create quota tmp".into()))?;
            file.write_all(&bytes)
                .await
                .map_err(|e| AppError::Io(e, "write quota bytes".into()))?;
            file.flush()
                .await
                .map_err(|e| AppError::Io(e, "flush quota".into()))?;
            fs::rename(tmp_path, path)
                .await
                .map_err(|e| AppError::Io(e, "rename quota".into()))?;
            Ok::<(), AppError>(())
        }
    });

    let results = join_all(write_futures).await;

    for result in results {
        if let Err(e) = result {
            return Err(e);
        }
    }

    QUOTA_CACHE.insert(uid.to_string(), quota.clone()).await;
    Ok(())
}

async fn read_quota_from_any_disk(uid: &str) -> Result<Quota, AppError> {
    let paths = get_all_quota_paths(uid);

    for path in paths {
        if path.exists() {
            let bytes = fs::read(&path)
                .await
                .map_err(|e| AppError::Io(e, "read quota".into()))?;
            let quota = mongodb::bson::from_slice(&bytes)
                .map_err(|_| AppError::InternalServerError("Corrupt quota file".into()))?;
            return Ok(quota);
        }
    }

    Err(AppError::Unauthorized)
}

pub async fn init_quota_file(uid: &str, allocated: u64) -> Result<(), AppError> {
    let _guard = get_quota_lock(uid).lock().await;

    match read_quota_from_any_disk(uid).await {
        Ok(current_quota) => {
            if current_quota.allocated_storage != allocated {
                let new_quota = Quota {
                    allocated_storage: allocated,
                    storage_used: current_quota.storage_used,
                };
                write_quota_to_all_disks(uid, &new_quota).await?;
                info!(
                    "Synced quota allocation for user {}: {} bytes",
                    uid, allocated
                );
            }
        }
        Err(_) => {
            let quota = Quota {
                allocated_storage: allocated,
                storage_used: 0,
            };
            write_quota_to_all_disks(uid, &quota).await?;
            info!("Initialized quota file for user {}", uid);
        }
    }

    Ok(())
}

pub async fn get_quota(uid: &str) -> Result<Quota, AppError> {
    if let Some(quota) = QUOTA_CACHE.get(uid).await {
        return Ok(quota);
    }

    let _guard = get_quota_lock(uid).lock().await;

    let quota = read_quota_from_any_disk(uid).await?;

    QUOTA_CACHE.insert(uid.to_string(), quota.clone()).await;
    Ok(quota)
}

pub async fn update_quota(uid: &str, delta_bytes: i64) -> Result<(), AppError> {
    let _guard = get_quota_lock(uid).lock().await;

    let mut quota = match read_quota_from_any_disk(uid).await {
        Ok(q) => q,
        Err(_) => {
            return Err(AppError::InternalServerError(
                "Quota file missing during update".into(),
            ));
        }
    };

    if delta_bytes < 0 {
        quota.storage_used = quota.storage_used.saturating_sub(delta_bytes.abs() as u64);
    } else {
        quota.storage_used = quota.storage_used.saturating_add(delta_bytes as u64);
    }

    write_quota_to_all_disks(uid, &quota).await?;
    Ok(())
}

pub async fn try_reserve_quota(uid: &str, reserve_amount: u64) -> Result<(), AppError> {
    let _guard = get_quota_lock(uid).lock().await;

    let mut quota = read_quota_from_any_disk(uid).await?;

    if quota.storage_used.saturating_add(reserve_amount) > quota.allocated_storage {
        return Err(AppError::InsufficientStorage);
    }

    quota.storage_used += reserve_amount;

    write_quota_to_all_disks(uid, &quota).await?;

    Ok(())
}

pub async fn set_storage_used(uid: &str, usage: u64) -> Result<(), AppError> {
    let _guard = get_quota_lock(uid).lock().await;

    let paths = get_all_quota_paths(uid);
    let mut quota_found = false;
    let mut current_quota = Quota {
        allocated_storage: 0,
        storage_used: 0,
    };

    for path in &paths {
        if path.exists() {
             if let Ok(bytes) = fs::read(path).await {
                 if let Ok(q) = mongodb::bson::from_slice::<Quota>(&bytes) {
                     current_quota = q;
                     quota_found = true;
                     break;
                 }
             }
        }
    }

    if !quota_found {
         return Ok(());
    }

    if current_quota.storage_used != usage {
        info!(
            "Consistency Fix: Correcting usage for {} (File: {} -> Disk: {})",
            uid, current_quota.storage_used, usage
        );
        current_quota.storage_used = usage;
        
        write_quota_to_all_disks(uid, &current_quota).await?;
    }

    Ok(())
}
