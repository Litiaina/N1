use crate::enums::app_error_enum::AppError;
use crate::object_storage::constants::PREFIX_SEGMENT_EXT;
use crate::object_storage::disk_manager::{ObjectPathManager, PrefixIndexEntry, PrefixIndexSegment};
use once_cell::sync::OnceCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::sync::{RwLock, Semaphore};

pub static PREFIX_COMPACT_TRIGGER_SEGMENTS_ATOMIC: OnceCell<AtomicUsize> = OnceCell::new();
pub static PREFIX_COMPACT_MIN_INTERVAL_SECS_ATOMIC: OnceCell<AtomicU64> = OnceCell::new();

static PREFIX_COMPACTION_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(1));
static PREFIX_LAST_COMPACTED_AT: LazyLock<Arc<RwLock<HashMap<String, Instant>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

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

fn merge_entry(latest: &mut HashMap<String, (String, i64)>, e: PrefixIndexEntry) {
    match latest.get(&e.object_key) {
        None => {
            latest.insert(e.object_key, (e.version_id, e.created_at));
        }
        Some((cur_vid, cur_ts)) => {
            if e.created_at > *cur_ts || (e.created_at == *cur_ts && e.version_id > *cur_vid) {
                latest.insert(e.object_key, (e.version_id, e.created_at));
            }
        }
    }
}

async fn compact_prefix_if_needed(
    path_manager: &ObjectPathManager,
    fragment: &str,
    prefix: &str,
) -> Result<(), AppError> {
    let bucket_dirs = path_manager.get_all_prefix_bucket_dirs(fragment, prefix);
    let mut all_seg_paths: Vec<(String, PathBuf)> = Vec::new();
    let mut latest: HashMap<String, (String, i64)> = HashMap::new();
    let mut had_any_data = false;

    for d in &bucket_dirs {
        if !fs::try_exists(d).await.unwrap_or(false) {
            continue;
        }

        let compacted_path = d.join(format!("compacted{}", PREFIX_SEGMENT_EXT));
        if fs::try_exists(&compacted_path).await.unwrap_or(false) {
            if let Ok(data) = fs::read(&compacted_path).await {
                if let Ok(seg) = rmp_serde::from_slice::<PrefixIndexSegment>(&data) {
                    had_any_data = true;
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
                }
            }
        }
    }

    if all_seg_paths.len() < compact_trigger_segments() {
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
        .into_iter()
        .map(|(object_key, (version_id, created_at))| PrefixIndexEntry {
            object_key,
            version_id,
            created_at,
        })
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

    let mut parent_dirs_to_sync: HashSet<PathBuf> = HashSet::new();
    let mut wrote_any = false;

    for p in path_manager.get_all_prefix_compacted_paths(fragment, prefix) {
        if let Some(parent) = p.parent() {
            let _ = fs::create_dir_all(parent).await;
            parent_dirs_to_sync.insert(parent.to_path_buf());
        }
        if write_atomic_overwrite(&p, &compacted_bytes).await.is_ok() {
            wrote_any = true;
        }
    }

    if !wrote_any {
        return Err(AppError::InternalServerError(
            "failed to write compacted prefix to any mirror".into(),
        ));
    }

    for (_name, p) in all_seg_paths {
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
    tokio::spawn(async move {
        let _permit = match PREFIX_COMPACTION_SEMAPHORE.try_acquire() {
            Ok(p) => p,
            Err(_) => return,
        };

        let path_manager = ObjectPathManager::new();
        let mut dedup: BTreeSet<String> = BTreeSet::new();
        for p in prefixes {
            dedup.insert(ObjectPathManager::canonical_prefix(&p));
        }

        for prefix in dedup {
            let key = format!("{}|{}", fragment, prefix);
            let min_interval = compact_min_interval();
            {
                let last = PREFIX_LAST_COMPACTED_AT.read().await;
                if let Some(ts) = last.get(&key) {
                    if ts.elapsed() < min_interval {
                        continue;
                    }
                }
            }

            if compact_prefix_if_needed(&path_manager, &fragment, &prefix)
                .await
                .is_ok()
            {
                let mut last = PREFIX_LAST_COMPACTED_AT.write().await;
                last.insert(key, Instant::now());
            }
        }
    });
}
