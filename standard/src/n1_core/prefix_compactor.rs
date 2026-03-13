use crate::enums::app_error_enum::AppError;
use crate::n1_core::constants::{
    PREFIX_DELIMITER_COMPACTED_FILE, PREFIX_DELIMITER_SEGMENT_PREFIX, PREFIX_SEGMENT_EXT,
};
use crate::n1_core::disk_manager::{ObjectPathManager, PrefixIndexEntry, PrefixIndexSegment};
use crate::n1_core::prefix_delimiter::build_delimiter_compacted_from_entries;
use once_cell::sync::OnceCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::sync::{Notify, RwLock};

pub static PREFIX_COMPACT_TRIGGER_SEGMENTS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static PREFIX_COMPACT_MIN_INTERVAL_SECS_ATOMIC: OnceCell<AtomicU64> = OnceCell::new();

static PREFIX_LAST_COMPACTED_AT: LazyLock<Arc<RwLock<HashMap<String, Instant>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));
static PREFIX_COMPACTION_PENDING: LazyLock<Arc<Mutex<HashMap<String, BTreeSet<String>>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));
static PREFIX_COMPACTION_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);
static PREFIX_COMPACTION_WORKER_STARTED: AtomicBool = AtomicBool::new(false);
const PREFIX_LAST_COMPACTED_MAX_ENTRIES: usize = 16_384;
const PREFIX_LAST_COMPACTED_STALE_MULTIPLIER: u32 = 8;
const PREFIX_LAST_COMPACTED_MIN_STALE_SECS: u64 = 300;
const PREFIX_COMPACTION_BATCH_DEBOUNCE: Duration = Duration::from_millis(25);

fn compact_trigger_segments() -> usize {
    PREFIX_COMPACT_TRIGGER_SEGMENTS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(128)
        .clamp(8, 131_072)
}

fn compact_min_interval() -> Duration {
    let secs = PREFIX_COMPACT_MIN_INTERVAL_SECS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(30)
        .clamp(1, 3600);
    Duration::from_secs(secs)
}

fn prune_last_compacted_entries(last: &mut HashMap<String, Instant>, min_interval: Duration) {
    let stale_after = std::cmp::max(
        min_interval
            .checked_mul(PREFIX_LAST_COMPACTED_STALE_MULTIPLIER)
            .unwrap_or(Duration::from_secs(PREFIX_LAST_COMPACTED_MIN_STALE_SECS)),
        Duration::from_secs(PREFIX_LAST_COMPACTED_MIN_STALE_SECS),
    );
    last.retain(|_, ts| ts.elapsed() <= stale_after);

    if last.len() <= PREFIX_LAST_COMPACTED_MAX_ENTRIES {
        return;
    }

    let mut by_age: Vec<(String, Instant)> = last.iter().map(|(k, ts)| (k.clone(), *ts)).collect();
    by_age.sort_by_key(|(_, ts)| *ts);

    let to_remove = last.len().saturating_sub(PREFIX_LAST_COMPACTED_MAX_ENTRIES);
    for (k, _) in by_age.into_iter().take(to_remove) {
        last.remove(&k);
    }
}

#[cfg(unix)]
async fn sync_dir(path: &Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;

        let f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY)
            .open(&path)
            .map_err(|e| AppError::Io(e, format!("open dir for sync: {:?}", path)))?;

        f.sync_all()
            .map_err(|e| AppError::Io(e, format!("sync dir: {:?}", path)))?;
        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(windows)]
async fn sync_dir(path: &Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::fs::OpenOptions;
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;
        if let Ok(file) = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&path)
        {
            let _ = file.sync_all();
        }
        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(not(any(unix, windows)))]
async fn sync_dir(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

async fn write_atomic_overwrite(path: &Path, data: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("No parent dir for {}", path.display()))
    })?;

    fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, "create parent dir".into()))?;

    let tmp = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        ulid::Ulid::new()
    ));

    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("open tmp {}", tmp.display())))?;

    tokio::io::AsyncWriteExt::write_all(&mut f, data)
        .await
        .map_err(|e| AppError::Io(e, "write tmp".into()))?;
    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync tmp".into()))?;
    drop(f);

    fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))?;
    Ok(())
}

fn merge_entry(latest: &mut HashMap<String, PrefixIndexEntry>, e: PrefixIndexEntry) {
    match latest.get(&e.object_key) {
        None => {
            latest.insert(e.object_key.clone(), e);
        }
        Some(current) => {
            if e.created_at > current.created_at
                || (e.created_at == current.created_at && e.version_id > current.version_id)
            {
                latest.insert(e.object_key.clone(), e);
            }
        }
    }
}

#[allow(dead_code)]
async fn compact_prefix_if_needed(
    path_manager: &ObjectPathManager,
    fragment: &str,
    prefix: &str,
) -> Result<(), AppError> {
    compact_prefix_if_needed_scoped(path_manager, fragment, prefix, false).await
}

async fn compact_prefix_if_needed_scoped(
    path_manager: &ObjectPathManager,
    fragment: &str,
    prefix: &str,
    trashed: bool,
) -> Result<(), AppError> {
    let bucket_dirs = if trashed {
        path_manager.get_all_trash_prefix_bucket_dirs(fragment, prefix)
    } else {
        path_manager.get_all_prefix_bucket_dirs(fragment, prefix)
    };
    let mut all_seg_paths: Vec<(String, PathBuf)> = Vec::new();
    let mut all_delim_seg_paths: Vec<PathBuf> = Vec::new();
    let mut latest: HashMap<String, PrefixIndexEntry> = HashMap::new();
    let mut had_any_data = false;
    let mut needs_delimiter_bootstrap = false;

    for d in &bucket_dirs {
        if !fs::try_exists(d).await.unwrap_or(false) {
            continue;
        }

        let compacted_path = d.join(format!("compacted{}", PREFIX_SEGMENT_EXT));
        if fs::try_exists(&compacted_path).await.unwrap_or(false) {
            if let Ok(data) = fs::read(&compacted_path).await {
                if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                    had_any_data = true;
                    if !fs::try_exists(&d.join(PREFIX_DELIMITER_COMPACTED_FILE))
                        .await
                        .unwrap_or(false)
                    {
                        needs_delimiter_bootstrap = true;
                    }
                    for e in seg.entries {
                        merge_entry(&mut latest, e);
                    }
                }
            }
        }

        if let Ok(mut entries) = fs::read_dir(d).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("seg_") && name.ends_with(PREFIX_SEGMENT_EXT) {
                    all_seg_paths.push((name, entry.path()));
                } else if name.starts_with(PREFIX_DELIMITER_SEGMENT_PREFIX)
                    && name.ends_with(PREFIX_SEGMENT_EXT)
                {
                    all_delim_seg_paths.push(entry.path());
                    needs_delimiter_bootstrap = true;
                }
            }
        }
    }

    if all_seg_paths.len() < compact_trigger_segments()
        && !(had_any_data && needs_delimiter_bootstrap)
    {
        return Ok(());
    }

    all_seg_paths.sort_by(|a, b| a.0.cmp(&b.0));
    for (_name, p) in &all_seg_paths {
        if let Ok(data) = fs::read(p).await {
            if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                had_any_data = true;
                for e in seg.entries {
                    merge_entry(&mut latest, e);
                }
            }
        }
    }

    if !had_any_data {
        return Ok(());
    }

    let mut entries: Vec<PrefixIndexEntry> = latest
        .into_values()
        .filter(|entry| !entry.is_delete_marker)
        .collect();
    entries.sort_by(|a, b| match a.object_key.cmp(&b.object_key) {
        std::cmp::Ordering::Equal => a.version_id.cmp(&b.version_id),
        other => other,
    });

    let compacted = PrefixIndexSegment {
        prefix: prefix.to_string(),
        entries,
    };
    let compacted_bytes = rmp_serde::to_vec(&compacted).map_err(|e| {
        AppError::InternalServerError(format!("serialize compacted prefix segment: {}", e))
    })?;
    let delimiter_compacted = build_delimiter_compacted_from_entries(prefix, &compacted.entries);
    let delimiter_compacted_bytes = rmp_serde::to_vec(&delimiter_compacted).map_err(|e| {
        AppError::InternalServerError(format!("serialize compacted delimiter segment: {}", e))
    })?;

    let mut parent_dirs_to_sync: HashSet<PathBuf> = HashSet::new();
    let mut wrote_any = false;
    let mut wrote_delimiter_any = false;

    let compacted_paths = if trashed {
        path_manager.get_all_trash_prefix_compacted_paths(fragment, prefix)
    } else {
        path_manager.get_all_prefix_compacted_paths(fragment, prefix)
    };
    for p in compacted_paths {
        if let Some(parent) = p.parent() {
            let _ = fs::create_dir_all(parent).await;
            parent_dirs_to_sync.insert(parent.to_path_buf());
        }
        if write_atomic_overwrite(&p, &compacted_bytes).await.is_ok() {
            wrote_any = true;
        }
    }
    let delimiter_paths = if trashed {
        path_manager.get_all_trash_prefix_delimiter_compacted_paths(fragment, prefix)
    } else {
        path_manager.get_all_prefix_delimiter_compacted_paths(fragment, prefix)
    };
    for p in delimiter_paths {
        if let Some(parent) = p.parent() {
            let _ = fs::create_dir_all(parent).await;
            parent_dirs_to_sync.insert(parent.to_path_buf());
        }
        if write_atomic_overwrite(&p, &delimiter_compacted_bytes)
            .await
            .is_ok()
        {
            wrote_delimiter_any = true;
        }
    }

    if !wrote_any || !wrote_delimiter_any {
        return Err(AppError::InternalServerError(
            "failed to write compacted prefix index to any mirror".into(),
        ));
    }

    for (_name, p) in all_seg_paths {
        let _ = fs::remove_file(&p).await;
        if let Some(parent) = p.parent() {
            parent_dirs_to_sync.insert(parent.to_path_buf());
        }
    }
    for p in all_delim_seg_paths {
        let _ = fs::remove_file(&p).await;
        if let Some(parent) = p.parent() {
            parent_dirs_to_sync.insert(parent.to_path_buf());
        }
    }

    for d in parent_dirs_to_sync {
        let _ = sync_dir(&d).await;
    }

    Ok(())
}

pub fn schedule_prefix_compaction(fragment: String, prefixes: Vec<String>) {
    schedule_prefix_compaction_scoped(fragment, prefixes, false);
}

pub fn schedule_trash_prefix_compaction(fragment: String, prefixes: Vec<String>) {
    schedule_prefix_compaction_scoped(fragment, prefixes, true);
}

fn schedule_prefix_compaction_scoped(fragment: String, prefixes: Vec<String>, trashed: bool) {
    ensure_prefix_compaction_worker();

    let mut dedup: BTreeSet<String> = BTreeSet::new();
    for prefix in prefixes {
        dedup.insert(ObjectPathManager::canonical_prefix(&prefix));
    }

    if dedup.is_empty() {
        return;
    }

    if let Ok(mut pending) = PREFIX_COMPACTION_PENDING.lock() {
        let scope_key = format!("{}|{}", if trashed { "trash" } else { "live" }, fragment);
        let queued = pending.entry(scope_key).or_default();
        queued.extend(dedup);
    }

    PREFIX_COMPACTION_NOTIFY.notify_one();
}

fn ensure_prefix_compaction_worker() {
    if PREFIX_COMPACTION_WORKER_STARTED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }

    tokio::spawn(async move {
        let path_manager = ObjectPathManager::new();

        loop {
            PREFIX_COMPACTION_NOTIFY.notified().await;
            tokio::time::sleep(PREFIX_COMPACTION_BATCH_DEBOUNCE).await;

            loop {
                let pending = {
                    let mut guard = PREFIX_COMPACTION_PENDING
                        .lock()
                        .expect("prefix compaction pending mutex poisoned");
                    if guard.is_empty() {
                        break;
                    }
                    std::mem::take(&mut *guard)
                };

                let min_interval = compact_min_interval();
                for (scope_key, prefixes) in pending {
                    let (trashed, fragment) =
                        if let Some(fragment) = scope_key.strip_prefix("trash|") {
                            (true, fragment.to_string())
                        } else if let Some(fragment) = scope_key.strip_prefix("live|") {
                            (false, fragment.to_string())
                        } else {
                            (false, scope_key)
                        };
                    for prefix in prefixes {
                        let key = format!(
                            "{}|{}|{}",
                            if trashed { "trash" } else { "live" },
                            fragment,
                            prefix
                        );
                        {
                            let last = PREFIX_LAST_COMPACTED_AT.read().await;
                            if let Some(ts) = last.get(&key) {
                                if ts.elapsed() < min_interval {
                                    continue;
                                }
                            }
                        }

                        if compact_prefix_if_needed_scoped(
                            &path_manager,
                            &fragment,
                            &prefix,
                            trashed,
                        )
                        .await
                        .is_ok()
                        {
                            let mut last = PREFIX_LAST_COMPACTED_AT.write().await;
                            last.insert(key, Instant::now());
                            prune_last_compacted_entries(&mut last, min_interval);
                        }
                    }
                }
            }
        }
    });
}
