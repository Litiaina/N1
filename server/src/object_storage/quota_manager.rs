use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::{LOCK_SHARD_COUNT, QUOTA_FILE};
use crate::structs::object_storage_structs::{ObjectPathManager, Quota};
use moka::future::Cache;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::LazyLock;
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

fn get_quota_path(uid: &str) -> PathBuf {
    let path_manager = ObjectPathManager::new();
    path_manager.get_user_index_dir(uid).join(QUOTA_FILE)
}

pub async fn init_quota_file(uid: &str, allocated: u64) -> Result<(), AppError> {
    let path = get_quota_path(uid);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| AppError::Io(e, "create user dir".into()))?;
    }

    let _guard = get_quota_lock(uid).lock().await;

    if path.exists() {
        let bytes = fs::read(&path)
            .await
            .map_err(|e| AppError::Io(e, "read quota for sync".into()))?;
        let current_quota: Quota = mongodb::bson::from_slice(&bytes)
            .map_err(|_| AppError::InternalServerError("Corrupt quota file".into()))?;

        if current_quota.allocated_storage != allocated {
            let new_quota = Quota {
                allocated_storage: allocated,
                storage_used: current_quota.storage_used,
            };
            write_quota_bytes(uid, &new_quota, &path).await?;
            info!(
                "Synced quota allocation for user {}: {} bytes",
                uid, allocated
            );
        }
    } else {
        let quota = Quota {
            allocated_storage: allocated,
            storage_used: 0,
        };
        write_quota_bytes(uid, &quota, &path).await?;
        info!("Initialized quota file for user {}", uid);
    }

    Ok(())
}

async fn write_quota_bytes(uid: &str, quota: &Quota, path: &PathBuf) -> Result<(), AppError> {
    let bytes = mongodb::bson::to_vec(quota)
        .map_err(|_| AppError::InternalServerError("Quota serialize error".into()))?;

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

    QUOTA_CACHE.insert(uid.to_string(), quota.clone()).await;
    Ok(())
}

pub async fn get_quota(uid: &str) -> Result<Quota, AppError> {
    if let Some(quota) = QUOTA_CACHE.get(uid).await {
        return Ok(quota);
    }

    let _guard = get_quota_lock(uid).lock().await;
    let path = get_quota_path(uid);

    if !path.exists() {
        return Err(AppError::Unauthorized);
    }

    let bytes = fs::read(&path)
        .await
        .map_err(|e| AppError::Io(e, "read quota".into()))?;
    let quota: Quota = mongodb::bson::from_slice(&bytes)
        .map_err(|_| AppError::InternalServerError("Corrupt quota file".into()))?;

    QUOTA_CACHE.insert(uid.to_string(), quota.clone()).await;
    Ok(quota)
}

pub async fn update_quota(uid: &str, delta_bytes: i64) -> Result<(), AppError> {
    let _guard = get_quota_lock(uid).lock().await;
    let path = get_quota_path(uid);

    if !path.exists() {
        return Err(AppError::InternalServerError(
            "Quota file missing during update".into(),
        ));
    };

    let bytes = fs::read(&path)
        .await
        .map_err(|e| AppError::Io(e, "read quota update".into()))?;
    let mut quota: Quota = mongodb::bson::from_slice(&bytes)
        .map_err(|_| AppError::InternalServerError("Corrupt quota file".into()))?;

    if delta_bytes < 0 {
        quota.storage_used = quota.storage_used.saturating_sub(delta_bytes.abs() as u64);
    } else {
        quota.storage_used = quota.storage_used.saturating_add(delta_bytes as u64);
    }

    write_quota_bytes(uid, &quota, &path).await?;
    Ok(())
}

pub async fn try_reserve_quota(uid: &str, reserve_amount: u64) -> Result<(), AppError> {
    let _guard = get_quota_lock(uid).lock().await;
    let path = get_quota_path(uid);

    if !path.exists() {
        return Err(AppError::Unauthorized);
    }

    let bytes = fs::read(&path)
        .await
        .map_err(|e| AppError::Io(e, "read quota reserve".into()))?;
    let mut quota: Quota = mongodb::bson::from_slice(&bytes)
        .map_err(|_| AppError::InternalServerError("Corrupt quota file".into()))?;

    if quota.storage_used.saturating_add(reserve_amount) > quota.allocated_storage {
        return Err(AppError::InsufficientStorage);
    }

    quota.storage_used += reserve_amount;

    write_quota_bytes(uid, &quota, &path).await?;

    Ok(())
}

pub async fn set_storage_used(uid: &str, usage: u64) -> Result<(), AppError> {
    let _guard = get_quota_lock(uid).lock().await;
    let path = get_quota_path(uid);

    if !path.exists() {
        return Ok(());
    }

    let bytes = fs::read(&path)
        .await
        .map_err(|e| AppError::Io(e, "read quota for set".into()))?;
    let mut quota: Quota = mongodb::bson::from_slice(&bytes)
        .map_err(|_| AppError::InternalServerError("Corrupt quota file".into()))?;

    if quota.storage_used != usage {
        info!(
            "Consistency Fix: Correcting usage for {} (File: {} -> Disk: {})",
            uid, quota.storage_used, usage
        );
        quota.storage_used = usage;
        write_quota_bytes(uid, &quota, &path).await?;
    }

    Ok(())
}
