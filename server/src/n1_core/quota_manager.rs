use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::LOCK_SHARD_COUNT;
use crate::n1_core::disk_manager::{ObjectPathManager, Quota};
use futures_util::future::join_all;
use moka::future::Cache;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

const WRITE_TIMEOUT: Duration = Duration::from_secs(15);
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const RENAME_TIMEOUT: Duration = Duration::from_secs(120);
const CREATE_DIR_TIMEOUT: Duration = Duration::from_secs(5);

const FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const DIRTY_QUEUE_CAPACITY: usize = 2048;
const MAX_FLUSH_PER_TICK: usize = 512;

const TMP_TTL: Duration = Duration::from_secs(600);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

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
        .max_capacity(50_000)
        .time_to_idle(Duration::from_secs(600))
        .build()
});

fn get_all_quota_paths(fragment: &str) -> Vec<PathBuf> {
    let path_manager = ObjectPathManager::new();
    path_manager.get_all_quota_paths(fragment)
}

static FLUSHER_TX: LazyLock<Mutex<Option<mpsc::Sender<String>>>> =
    LazyLock::new(|| Mutex::new(None));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QuotaWriteSummary {
    success_count: usize,
    failure_count: usize,
}

async fn ensure_flusher_tx() -> mpsc::Sender<String> {
    let mut guard = FLUSHER_TX.lock().await;
    let need_new = match guard.as_ref() {
        None => true,
        Some(tx) => tx.is_closed(),
    };

    if need_new {
        let (tx, mut rx) = mpsc::channel::<String>(DIRTY_QUEUE_CAPACITY);
        *guard = Some(tx.clone());

        tokio::spawn(async move {
            let mut dirty: HashSet<String> = HashSet::new();
            let mut rr: VecDeque<String> = VecDeque::new();
            let mut last_tick = Instant::now();
            let mut last_cleanup = Instant::now();

            loop {
                tokio::select! {
                    v = rx.recv() => {
                        if let Some(frag) = v {
                            if dirty.insert(frag.clone()) {
                                rr.push_back(frag);
                            }
                        } else {
                            break;
                        }
                    }
                    _ = sleep(Duration::from_millis(25)) => {}
                }

                if last_tick.elapsed() < FLUSH_INTERVAL {
                    continue;
                }
                last_tick = Instant::now();

                let do_cleanup = last_cleanup.elapsed() >= CLEANUP_INTERVAL;
                if do_cleanup {
                    last_cleanup = Instant::now();
                }

                let mut count = 0usize;
                while count < MAX_FLUSH_PER_TICK {
                    let frag = match rr.pop_front() {
                        Some(f) => f,
                        None => break,
                    };

                    if !dirty.remove(&frag) {
                        continue;
                    }

                    if let Some(q) = QUOTA_CACHE.get(&frag).await {
                        if do_cleanup {
                            let _ = cleanup_stale_quota_tmps(&frag).await;
                        }

                        match write_quota_to_all_disks(&frag, &q).await {
                            Ok(summary) if summary.failure_count == 0 => {}
                            Ok(summary) => {
                                debug!(
                                    "Quota mirror flush incomplete for fragment {}: {} succeeded, {} failed; retrying",
                                    frag, summary.success_count, summary.failure_count
                                );
                                if dirty.insert(frag.clone()) {
                                    rr.push_back(frag);
                                }
                            }
                            Err(e) => {
                                warn!("Quota mirror flush failed for fragment {}: {}", frag, e);
                                if dirty.insert(frag.clone()) {
                                    rr.push_back(frag);
                                }
                            }
                        }
                    }

                    count += 1;
                }
            }
        });

        return tx;
    }

    guard.as_ref().unwrap().clone()
}

async fn enqueue_dirty(fragment: &str) -> Result<(), AppError> {
    let tx = ensure_flusher_tx().await;

    match tx.try_send(fragment.to_string()) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => Err(AppError::Timeout("quota backlog".into())),
        Err(mpsc::error::TrySendError::Closed(_)) => {
            let tx2 = ensure_flusher_tx().await;
            match tx2.try_send(fragment.to_string()) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => {
                    Err(AppError::Timeout("quota backlog".into()))
                }
                Err(mpsc::error::TrySendError::Closed(_)) => Err(AppError::InternalServerError(
                    "Quota flusher channel closed".into(),
                )),
            }
        }
    }
}

fn is_tmp_quota_name(name: &str) -> bool {
    name.contains(".tmp.")
}

async fn cleanup_stale_quota_tmps(fragment: &str) -> Result<(), AppError> {
    let paths = get_all_quota_paths(fragment);
    let now = SystemTime::now();

    for quota_path in paths {
        let parent = match quota_path.parent() {
            Some(p) => p.to_path_buf(),
            None => continue,
        };

        let mut rd = match timeout(READ_TIMEOUT, fs::read_dir(&parent)).await {
            Ok(Ok(rd)) => rd,
            _ => continue,
        };

        loop {
            let entry = match timeout(READ_TIMEOUT, rd.next_entry()).await {
                Ok(Ok(Some(e))) => e,
                Ok(Ok(None)) => break,
                _ => break,
            };

            let name = entry.file_name().to_string_lossy().to_string();
            if !is_tmp_quota_name(&name) {
                continue;
            }

            let meta = match timeout(READ_TIMEOUT, entry.metadata()).await {
                Ok(Ok(m)) => m,
                _ => continue,
            };

            let mtime = match meta.modified() {
                Ok(t) => t,
                Err(_) => continue,
            };

            let age = match now.duration_since(mtime) {
                Ok(d) => d,
                Err(_) => Duration::from_secs(0),
            };

            if age >= TMP_TTL {
                let _ = timeout(READ_TIMEOUT, fs::remove_file(entry.path())).await;
            }
        }
    }

    Ok(())
}

async fn ensure_parent_dir(path: &Path) -> Result<(), AppError> {
    let parent = match path.parent() {
        Some(p) => p,
        None => return Ok(()),
    };

    match timeout(CREATE_DIR_TIMEOUT, fs::try_exists(parent)).await {
        Ok(Ok(true)) => Ok(()),
        Ok(Ok(false)) => match timeout(CREATE_DIR_TIMEOUT, fs::create_dir_all(parent)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(AppError::Io(e, "create quota dir".into())),
            Err(_) => Err(AppError::Timeout("create quota dir".into())),
        },
        Ok(Err(e)) => Err(AppError::Io(e, "check parent existence".into())),
        Err(_) => Err(AppError::Timeout("check parent existence".into())),
    }
}

async fn write_quota_to_all_disks(
    fragment: &str,
    quota: &Quota,
) -> Result<QuotaWriteSummary, AppError> {
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
            ensure_parent_dir(&path).await?;

            let tmp_path = path.with_extension(format!("tmp.{}.{}", pid, req_id));

            let write_result = timeout(WRITE_TIMEOUT, async {
                let mut file = File::create(&tmp_path)
                    .await
                    .map_err(|e| AppError::Io(e, "create quota tmp".into()))?;
                file.write_all(&bytes)
                    .await
                    .map_err(|e| AppError::Io(e, "write quota bytes".into()))?;
                file.flush()
                    .await
                    .map_err(|e| AppError::Io(e, "flush quota".into()))?;
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
                    return Err(AppError::Timeout("write quota".into()));
                }
            }

            let mut rename_result_final = Ok(());
            let mut remove_tmp_on_failure = true;
            for attempt in 0..3 {
                let rename_result = timeout(RENAME_TIMEOUT, fs::rename(&tmp_path, &path)).await;
                match rename_result {
                    Ok(Ok(_)) => {
                        rename_result_final = Ok(());
                        break;
                    }
                    Ok(Err(e)) => {
                        if attempt == 2 {
                            rename_result_final = Err(AppError::Io(e, "rename quota".into()));
                        } else {
                            sleep(Duration::from_millis(50)).await;
                        }
                    }
                    Err(_) => {
                        remove_tmp_on_failure = false;
                        rename_result_final = Err(AppError::Timeout("rename quota".into()));
                        break;
                    }
                }
            }

            if rename_result_final.is_err() {
                if remove_tmp_on_failure {
                    let _ = fs::remove_file(&tmp_path).await;
                }
                return rename_result_final;
            }

            Ok(())
        }
    });

    let results = join_all(write_futures).await;
    let mut success_count = 0usize;
    let mut failure_count = 0usize;

    for result in results {
        if result.is_ok() {
            success_count += 1;
        } else if let Err(e) = result {
            failure_count += 1;
            debug!(
                "Failed to write quota to one mirror for {}: {}",
                fragment, e
            );
        }
    }

    if success_count == 0 {
        return Err(AppError::InternalServerError(
            "Failed to write quota to ANY disk".into(),
        ));
    }

    Ok(QuotaWriteSummary {
        success_count,
        failure_count,
    })
}

async fn read_quota_from_any_disk(fragment: &str) -> Result<Quota, AppError> {
    let paths = get_all_quota_paths(fragment);
    for path in paths {
        let exists_check = timeout(READ_TIMEOUT, fs::try_exists(&path)).await;
        if let Ok(Ok(true)) = exists_check {
            let read_result = timeout(READ_TIMEOUT, fs::read(&path)).await;
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
    let _ = ensure_flusher_tx().await;

    let _guard = get_quota_lock(fragment).lock().await;

    match read_quota_from_any_disk(fragment).await {
        Ok(mut current_quota) => {
            if current_quota.allocated_storage != allocated {
                current_quota.allocated_storage = allocated;
                let summary = write_quota_to_all_disks(fragment, &current_quota).await?;
                info!(
                    "Synced quota allocation for fragment {}: {} bytes",
                    fragment, allocated
                );
                if summary.failure_count > 0 {
                    let _ = enqueue_dirty(fragment).await;
                }
            }
            QUOTA_CACHE
                .insert(fragment.to_string(), current_quota)
                .await;
        }
        Err(_) => {
            let quota = Quota {
                allocated_storage: allocated,
                storage_used: 0,
            };
            let summary = write_quota_to_all_disks(fragment, &quota).await?;
            info!("Initialized quota file for new fragment {}", fragment);
            QUOTA_CACHE.insert(fragment.to_string(), quota).await;
            if summary.failure_count > 0 {
                let _ = enqueue_dirty(fragment).await;
            }
        }
    }

    Ok(())
}

pub async fn get_quota(fragment: &str) -> Result<Quota, AppError> {
    let _ = ensure_flusher_tx().await;

    if let Some(quota) = QUOTA_CACHE.get(fragment).await {
        return Ok(quota);
    }

    let _guard = get_quota_lock(fragment).lock().await;

    if let Some(quota) = QUOTA_CACHE.get(fragment).await {
        return Ok(quota);
    }

    let quota = read_quota_from_any_disk(fragment).await?;
    QUOTA_CACHE
        .insert(fragment.to_string(), quota.clone())
        .await;
    Ok(quota)
}

pub async fn update_quota(fragment: &str, delta_bytes: i64) -> Result<(), AppError> {
    let _ = ensure_flusher_tx().await;

    let _guard = get_quota_lock(fragment).lock().await;

    let mut quota = match QUOTA_CACHE.get(fragment).await {
        Some(q) => q,
        None => read_quota_from_any_disk(fragment).await.map_err(|_| {
            AppError::InternalServerError("Quota file missing. Fragment not initialized.".into())
        })?,
    };

    if delta_bytes < 0 {
        quota.storage_used = quota
            .storage_used
            .saturating_sub(delta_bytes.unsigned_abs());
    } else {
        quota.storage_used = quota.storage_used.saturating_add(delta_bytes as u64);
    }

    QUOTA_CACHE.insert(fragment.to_string(), quota).await;
    enqueue_dirty(fragment).await?;
    Ok(())
}

pub async fn try_reserve_quota(fragment: &str, reserve_amount: u64) -> Result<(), AppError> {
    let _ = ensure_flusher_tx().await;

    let _guard = get_quota_lock(fragment).lock().await;

    let mut quota = match QUOTA_CACHE.get(fragment).await {
        Some(q) => q,
        None => read_quota_from_any_disk(fragment).await?,
    };

    if quota.storage_used.saturating_add(reserve_amount) > quota.allocated_storage {
        return Err(AppError::InsufficientStorage);
    }

    quota.storage_used = quota.storage_used.saturating_add(reserve_amount);

    QUOTA_CACHE.insert(fragment.to_string(), quota).await;
    enqueue_dirty(fragment).await?;
    Ok(())
}

pub async fn set_storage_used(fragment: &str, usage: u64) -> Result<(), AppError> {
    let _ = ensure_flusher_tx().await;

    let _guard = get_quota_lock(fragment).lock().await;

    let mut quota = match QUOTA_CACHE.get(fragment).await {
        Some(q) => q,
        None => match read_quota_from_any_disk(fragment).await {
            Ok(q) => q,
            Err(_) => return Ok(()),
        },
    };

    if quota.storage_used != usage {
        quota.storage_used = usage;
        QUOTA_CACHE.insert(fragment.to_string(), quota).await;
        enqueue_dirty(fragment).await?;
    }

    Ok(())
}
