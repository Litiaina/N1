use crate::enums::app_error_enum::AppError;
use crate::core::constants::LOCK_SHARD_COUNT;
use crate::core::disk_manager::Quota;
use crate::core::raw_index;
use moka::future::Cache;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::LazyLock;
use tokio::sync::Mutex;

static QUOTA_LOCKS: LazyLock<Vec<Mutex<()>>> = LazyLock::new(|| {
    let mut locks = Vec::with_capacity(LOCK_SHARD_COUNT);
    for _ in 0..LOCK_SHARD_COUNT {
        locks.push(Mutex::new(()));
    }
    locks
});

static QUOTA_CACHE: LazyLock<Cache<String, Quota>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(50_000)
        .time_to_idle(std::time::Duration::from_secs(600))
        .build()
});

fn get_quota_lock(fragment: &str) -> &Mutex<()> {
    let mut hasher = DefaultHasher::new();
    fragment.hash(&mut hasher);
    let idx = (hasher.finish() as usize) % LOCK_SHARD_COUNT;
    &QUOTA_LOCKS[idx]
}

async fn load_quota_locked(fragment: &str) -> Result<Quota, AppError> {
    if let Some(quota) = QUOTA_CACHE.get(fragment).await {
        return Ok(quota);
    }

    let quota = raw_index::read_quota(fragment)
        .await?
        .ok_or(AppError::Unauthorized)?;
    QUOTA_CACHE
        .insert(fragment.to_string(), quota.clone())
        .await;
    Ok(quota)
}

pub async fn init_quota_file(fragment: &str, allocated: u64) -> Result<(), AppError> {
    let _guard = get_quota_lock(fragment).lock().await;
    let mut quota = raw_index::read_quota(fragment).await?.unwrap_or(Quota {
        allocated_storage: allocated,
        storage_used: 0,
    });
    quota.allocated_storage = allocated;
    raw_index::upsert_quota(fragment, &quota).await?;
    QUOTA_CACHE.insert(fragment.to_string(), quota).await;
    Ok(())
}

pub async fn get_quota(fragment: &str) -> Result<Quota, AppError> {
    if let Some(quota) = QUOTA_CACHE.get(fragment).await {
        return Ok(quota);
    }

    let _guard = get_quota_lock(fragment).lock().await;
    load_quota_locked(fragment).await
}

pub async fn update_quota(fragment: &str, delta_bytes: i64) -> Result<(), AppError> {
    let _guard = get_quota_lock(fragment).lock().await;
    let mut quota = load_quota_locked(fragment).await.map_err(|_| {
        AppError::InternalServerError("Quota file missing. Fragment not initialized.".into())
    })?;

    if delta_bytes < 0 {
        quota.storage_used = quota
            .storage_used
            .saturating_sub(delta_bytes.unsigned_abs());
    } else {
        quota.storage_used = quota.storage_used.saturating_add(delta_bytes as u64);
    }

    raw_index::upsert_quota(fragment, &quota).await?;
    QUOTA_CACHE.insert(fragment.to_string(), quota).await;
    Ok(())
}

pub async fn try_reserve_quota(fragment: &str, reserve_amount: u64) -> Result<(), AppError> {
    let _guard = get_quota_lock(fragment).lock().await;
    let mut quota = load_quota_locked(fragment).await?;
    if quota.storage_used.saturating_add(reserve_amount) > quota.allocated_storage {
        return Err(AppError::InsufficientStorage);
    }
    quota.storage_used = quota.storage_used.saturating_add(reserve_amount);
    raw_index::upsert_quota(fragment, &quota).await?;
    QUOTA_CACHE.insert(fragment.to_string(), quota).await;
    Ok(())
}

pub async fn set_storage_used(fragment: &str, usage: u64) -> Result<(), AppError> {
    let _guard = get_quota_lock(fragment).lock().await;
    let mut quota = match load_quota_locked(fragment).await {
        Ok(quota) => quota,
        Err(AppError::Unauthorized) => return Ok(()),
        Err(err) => return Err(err),
    };

    if quota.storage_used != usage {
        quota.storage_used = usage;
        raw_index::upsert_quota(fragment, &quota).await?;
        QUOTA_CACHE.insert(fragment.to_string(), quota).await;
    }

    Ok(())
}
