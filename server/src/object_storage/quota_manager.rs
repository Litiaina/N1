use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::LOCK_SHARD_COUNT;
use crate::object_storage::disk_manager::{ObjectPathManager, Quota};
use futures_util::future::join_all;
use moka::future::Cache;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{info, warn};

const DISK_IO_TIMEOUT: Duration = Duration::from_secs(5);

static QUOTA_LOCKS: LazyLock<Vec<Mutex<()>>> = LazyLock::new(|| {
    let mut locks = Vec::with_capacity(LOCK_SHARD_COUNT);
    for _ in 0..LOCK_SHARD_COUNT {
        locks.push(Mutex::new(()));
    }
    locks
});

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn get_quota_lock(fragment: &str) -> &Mutex<()> {
    let mut hasher = DefaultHasher::new();
    fragment.hash(&mut hasher);
    let idx = (hasher.finish() as usize) % LOCK_SHARD_COUNT;
    &QUOTA_LOCKS[idx]
}

static QUOTA_CACHE: LazyLock<Cache<String, Quota>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(10_000)
        .time_to_idle(Duration::from_secs(600))
        .build()
});

fn get_all_quota_paths(fragment: &str) -> Vec<PathBuf> {
    let path_manager = ObjectPathManager::new();
    path_manager.get_all_quota_paths(fragment)
}

async fn write_quota_to_all_disks(fragment: &str, quota: &Quota) -> Result<(), AppError> {
    let paths = get_all_quota_paths(fragment);
    let bytes = Arc::new(
        rmp_serde::to_vec(quota)
            .map_err(|_| AppError::InternalServerError("Quota serialize error".into()))?,
    );

    let req_id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();

    let write_futures = paths.into_iter().map(|path| {
        let bytes = bytes.clone();
        async move {
            if let Some(parent) = path.parent() {
                let exists_result = timeout(DISK_IO_TIMEOUT, fs::try_exists(parent)).await;
                match exists_result {
                    Ok(Ok(true)) => {}
                    Ok(Ok(false)) => {
                        match timeout(DISK_IO_TIMEOUT, fs::create_dir_all(parent)).await {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => return Err(AppError::Io(e, "create quota dir".into())),
                            Err(_) => {
                                return Err(AppError::Timeout(format!(
                                    "Create dir {:?} timed out",
                                    parent
                                )));
                            }
                        }
                    }
                    Ok(Err(e)) => return Err(AppError::Io(e, "check parent existence".into())),
                    Err(_) => return Err(AppError::Timeout("check parent existence".into())),
                }
            }

            let tmp_path = path.with_extension(format!("tmp.{}.{}", pid, req_id));

            let write_result = timeout(DISK_IO_TIMEOUT, async {
                let mut file = File::create(&tmp_path)
                    .await
                    .map_err(|e| AppError::Io(e, "create quota tmp".into()))?;
                file.write_all(&bytes)
                    .await
                    .map_err(|e| AppError::Io(e, "write quota bytes".into()))?;
                file.sync_data()
                    .await
                    .map_err(|e| AppError::Io(e, "sync quota".into()))?;
                Ok::<(), AppError>(())
            })
            .await;

            match write_result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let _ = fs::remove_file(&tmp_path).await;
                    return Err(e);
                }
                Err(_) => {
                    let _ = fs::remove_file(&tmp_path).await;
                    return Err(AppError::Timeout("write quota".into()));
                }
            }

            let mut rename_result_final = Ok(());
            for attempt in 0..3 {
                let rename_result = timeout(DISK_IO_TIMEOUT, fs::rename(&tmp_path, &path)).await;
                match rename_result {
                    Ok(Ok(_)) => {
                        if let Some(parent) = path.parent() {
                            if let Ok(dir) = std::fs::File::open(parent) {
                                let _ = dir.sync_all();
                            }
                        }
                        rename_result_final = Ok(());
                        break;
                    }
                    Ok(Err(e)) => {
                        if attempt == 2 {
                            rename_result_final = Err(AppError::Io(e, "rename quota".into()));
                        } else {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                    Err(_) => {
                        rename_result_final = Err(AppError::Timeout("rename quota".into()));
                        break;
                    }
                }
            }

            if rename_result_final.is_err() {
                let _ = fs::remove_file(&tmp_path).await;
                return rename_result_final;
            }

            Ok(())
        }
    });

    let results = join_all(write_futures).await;
    let mut success_count = 0;

    for result in results {
        if result.is_ok() {
            success_count += 1;
        } else if let Err(e) = result {
            warn!("Failed to write quota to one mirror: {}", e);
        }
    }

    if success_count == 0 {
        return Err(AppError::InternalServerError(
            "Failed to write quota to ANY disk".into(),
        ));
    }

    QUOTA_CACHE
        .insert(fragment.to_string(), quota.clone())
        .await;
    Ok(())
}

async fn read_quota_from_any_disk(fragment: &str) -> Result<Quota, AppError> {
    let paths = get_all_quota_paths(fragment);
    for path in paths {
        let exists_check = timeout(DISK_IO_TIMEOUT, fs::try_exists(&path)).await;
        if let Ok(Ok(true)) = exists_check {
            let read_result = timeout(DISK_IO_TIMEOUT, fs::read(&path)).await;
            match read_result {
                Ok(Ok(bytes)) => {
                    if let Ok(quota) = rmp_serde::from_slice(&bytes) {
                        return Ok(quota);
                    }
                }
                Ok(Err(e)) => warn!("Failed to read quota from {:?}: {}", path, e),
                Err(_) => warn!("Timeout reading quota from {:?}", path),
            }
        }
    }
    Err(AppError::Unauthorized)
}

pub async fn init_quota_file(fragment: &str, allocated: u64) -> Result<(), AppError> {
    let _guard = get_quota_lock(fragment).lock().await;
    match read_quota_from_any_disk(fragment).await {
        Ok(current_quota) => {
            if current_quota.allocated_storage != allocated {
                let new_quota = Quota {
                    allocated_storage: allocated,
                    storage_used: current_quota.storage_used,
                };
                write_quota_to_all_disks(fragment, &new_quota).await?;
                info!(
                    "Synced quota allocation for fragment {}: {} bytes",
                    fragment, allocated
                );
            }
        }
        Err(_) => {
            let quota = Quota {
                allocated_storage: allocated,
                storage_used: 0,
            };
            write_quota_to_all_disks(fragment, &quota).await?;
            info!("Initialized quota file for new fragment {}", fragment);
        }
    }
    Ok(())
}

pub async fn get_quota(fragment: &str) -> Result<Quota, AppError> {
    if let Some(quota) = QUOTA_CACHE.get(fragment).await {
        return Ok(quota);
    }
    let _guard = get_quota_lock(fragment).lock().await;
    let quota = read_quota_from_any_disk(fragment).await?;
    QUOTA_CACHE
        .insert(fragment.to_string(), quota.clone())
        .await;
    Ok(quota)
}

pub async fn update_quota(fragment: &str, delta_bytes: i64) -> Result<(), AppError> {
    let _guard = get_quota_lock(fragment).lock().await;
    let mut quota = match read_quota_from_any_disk(fragment).await {
        Ok(q) => q,
        Err(_) => {
            return Err(AppError::InternalServerError(
                "Quota file missing. Fragment not initialized.".into(),
            ));
        }
    };
    if delta_bytes < 0 {
        quota.storage_used = quota
            .storage_used
            .saturating_sub(delta_bytes.unsigned_abs());
    } else {
        quota.storage_used = quota.storage_used.saturating_add(delta_bytes as u64);
    }
    write_quota_to_all_disks(fragment, &quota).await?;
    Ok(())
}

pub async fn try_reserve_quota(fragment: &str, reserve_amount: u64) -> Result<(), AppError> {
    let _guard = get_quota_lock(fragment).lock().await;
    let mut quota = read_quota_from_any_disk(fragment).await?;
    if quota.storage_used.saturating_add(reserve_amount) > quota.allocated_storage {
        return Err(AppError::InsufficientStorage);
    }
    quota.storage_used += reserve_amount;
    write_quota_to_all_disks(fragment, &quota).await?;
    Ok(())
}

pub async fn set_storage_used(fragment: &str, usage: u64) -> Result<(), AppError> {
    let _guard = get_quota_lock(fragment).lock().await;
    let paths = get_all_quota_paths(fragment);
    let mut quota_found = false;
    let mut current_quota = Quota {
        allocated_storage: 0,
        storage_used: 0,
    };
    for path in &paths {
        if timeout(DISK_IO_TIMEOUT, fs::try_exists(path))
            .await
            .unwrap_or(Ok(false))
            .unwrap_or(false)
        {
            if let Ok(Ok(bytes)) = timeout(DISK_IO_TIMEOUT, fs::read(path)).await {
                if let Ok(q) = rmp_serde::from_slice::<Quota>(&bytes) {
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
            fragment, current_quota.storage_used, usage
        );
        current_quota.storage_used = usage;
        write_quota_to_all_disks(fragment, &current_quota).await?;
    }
    Ok(())
}
