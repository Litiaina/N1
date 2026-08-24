//! Restart-proof redistribution intent journal and replay mechanics.

use super::execution::{
    check_needs_redistribution, redistribute_object_streaming, restore_colocated_metadata,
};
use super::*;

pub(super) fn get_shard_dir_custom(
    disk_root: &Path,
    fragment_hash: &str,
    object_id: &str,
    version_id: &str,
    is_trash: bool,
) -> PathBuf {
    let _ = is_trash;
    ObjectPathManager::new().get_shard_dir_on_disk(
        &disk_root.to_path_buf(),
        fragment_hash,
        object_id,
        version_id,
    )
}

fn get_partition_intent_paths_custom(
    index_roots: &[PathBuf],
    fragment_hash: &str,
    object_key: &str,
) -> Vec<PathBuf> {
    use sha2::{Digest, Sha256};

    if index_roots.is_empty() {
        return Vec::new();
    }

    let partition_count = fragment_index_partition_count(fragment_hash).unwrap_or(1);
    let canonical_key = ObjectPathManager::canonical_object_key(object_key);
    let mut object_hasher = Sha256::new();
    for part in ["object", fragment_hash, canonical_key.as_str()] {
        object_hasher.update(part.as_bytes());
        object_hasher.update([0u8]);
    }
    let object_hash: [u8; 32] = object_hasher.finalize().into();
    let partition_id =
        (u16::from(object_hash[0]) << 8 | u16::from(object_hash[1])) % partition_count;

    let mut route_hasher = Sha256::new();
    for part in ["fragment", fragment_hash] {
        route_hasher.update(part.as_bytes());
        route_hasher.update([0u8]);
    }
    let route_hash: [u8; 32] = route_hasher.finalize().into();
    let fragment_owner =
        usize::from(u16::from(route_hash[0]) << 8 | u16::from(route_hash[1])) % index_roots.len();
    let owner_idx = (fragment_owner + usize::from(partition_id)) % index_roots.len();
    let mut selected: Vec<&PathBuf> = vec![&index_roots[owner_idx]];
    for (idx, root) in index_roots.iter().enumerate() {
        if idx != owner_idx {
            selected.push(root);
        }
    }

    selected
        .into_iter()
        .map(|root| {
            fragment_root_on(root, fragment_hash)
                .join(".intents")
                .join(format!("partition_{partition_id:04x}.n1"))
        })
        .collect()
}

pub(super) fn get_all_intent_paths_custom(
    index_roots: &[PathBuf],
    fragment_hash: &str,
    object_key: &str,
    version_id: &str,
    is_trash: bool,
) -> Vec<PathBuf> {
    get_partition_intent_paths_custom(index_roots, fragment_hash, object_key)
        .into_iter()
        .map(|db_path| {
            let partition_stem = db_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("partition");
            let object_hash = ObjectPathManager::object_key_hash(object_key);
            db_path.with_file_name(format!(
                "{partition_stem}.{object_hash}.{version_id}.{is_trash}{}",
                REDIST_INTENT_SUFFIX
            ))
        })
        .collect()
}

fn intent_dedupe_key(fragment: &str, object_key: &str, version_id: &str, is_trash: bool) -> String {
    format!("{fragment}\u{1f}{object_key}\u{1f}{version_id}\u{1f}{is_trash}")
}

pub(super) async fn write_intent_state(
    intent_paths: &[PathBuf],
    intent: &RedistributionIntent,
    strict_durability: bool,
) -> Result<(), AppError> {
    if intent_paths.is_empty() {
        return Err(AppError::InternalServerError(
            "no intent paths resolved for redistribution object".into(),
        ));
    }
    let payload = rmp_serde::to_vec(intent).map_err(|e| {
        AppError::InternalServerError(format!("serialize redistribution intent: {}", e))
    })?;
    let mut wrote_any = false;
    for path in intent_paths {
        if let Some(parent) = path.parent() {
            let _ = tokio_fs::create_dir_all(parent).await;
        }
        match durable_atomic_write_bytes_with_mode(path, &payload, strict_durability).await {
            Ok(_) => wrote_any = true,
            Err(e) => warn!(
                "Failed to write redistribution intent {}: {}",
                path.display(),
                e
            ),
        }
    }

    if !wrote_any {
        return Err(AppError::InternalServerError(
            "failed to persist redistribution intent on any reachable index root".into(),
        ));
    }
    Ok(())
}

pub(super) async fn remove_intent_paths_best_effort(intent_paths: &[PathBuf]) {
    for path in intent_paths {
        let _ = tokio_fs::remove_file(path).await;
    }
}

pub async fn collect_redistribution_intent_debt(
    index_roots: &[PathBuf],
    target_fragment: Option<String>,
) -> RedistributionIntentDebtSnapshot {
    if index_roots.is_empty() {
        return RedistributionIntentDebtSnapshot::default();
    }

    let roots = index_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let now_ms = Utc::now().timestamp_millis();
        let mut grouped: HashMap<String, RedistributionIntent> = HashMap::new();
        let mut fragments = HashSet::new();

        for root in roots {
            let direct_intent_dir = root.join(".intents");
            if target_fragment.is_none() && direct_intent_dir.exists() {
                for entry in WalkDir::new(direct_intent_dir)
                    .into_iter()
                    .filter_map(|e| e.ok())
                {
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    let name = entry.file_name().to_string_lossy();
                    if !name.ends_with(REDIST_INTENT_SUFFIX) {
                        continue;
                    }
                    let Ok(bytes) = std::fs::read(entry.path()) else {
                        continue;
                    };
                    let Ok(intent) = rmp_serde::from_slice::<RedistributionIntent>(&bytes) else {
                        continue;
                    };
                    let key = intent_dedupe_key(
                        &intent.fragment,
                        &intent.object_key,
                        &intent.version_id,
                        intent.is_trash,
                    );
                    grouped
                        .entry(key)
                        .and_modify(|current| {
                            if intent.updated_at_unix_ms > current.updated_at_unix_ms {
                                *current = intent.clone();
                            }
                        })
                        .or_insert(intent);
                }
                continue;
            }

            if !root.exists() {
                continue;
            }
            let scan_root = if let Some(fragment) = target_fragment.as_ref() {
                let scoped = fragment_root_on(&root, fragment).join(".intents");
                if !scoped.exists() {
                    continue;
                }
                scoped
            } else {
                root
            };
            for entry in WalkDir::new(scan_root).into_iter().filter_map(|e| e.ok()) {
                if !entry.file_type().is_file() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy();
                if !name.ends_with(REDIST_INTENT_SUFFIX) {
                    continue;
                }
                let Ok(bytes) = std::fs::read(entry.path()) else {
                    continue;
                };
                let Ok(intent) = rmp_serde::from_slice::<RedistributionIntent>(&bytes) else {
                    continue;
                };
                if target_fragment
                    .as_ref()
                    .is_some_and(|target| intent.fragment != *target)
                {
                    continue;
                }
                let key = intent_dedupe_key(
                    &intent.fragment,
                    &intent.object_key,
                    &intent.version_id,
                    intent.is_trash,
                );
                grouped
                    .entry(key)
                    .and_modify(|current| {
                        if intent.updated_at_unix_ms > current.updated_at_unix_ms {
                            *current = intent.clone();
                        }
                    })
                    .or_insert(intent);
            }
        }

        let mut snapshot = RedistributionIntentDebtSnapshot::default();
        let mut oldest_created_ms: Option<i64> = None;
        for intent in grouped.values() {
            snapshot.pending_intents = snapshot.pending_intents.saturating_add(1);
            if intent.phase.eq_ignore_ascii_case("failed") {
                snapshot.failed_intents = snapshot.failed_intents.saturating_add(1);
            }
            fragments.insert(intent.fragment.clone());
            oldest_created_ms = Some(match oldest_created_ms {
                Some(current) => current.min(intent.created_at_unix_ms),
                None => intent.created_at_unix_ms,
            });
        }
        snapshot.fragments_with_pending_intents = fragments.len() as u64;
        snapshot.oldest_pending_intent_age_seconds = oldest_created_ms.and_then(|created_at| {
            if created_at <= 0 || now_ms <= created_at {
                None
            } else {
                Some(((now_ms - created_at) / 1000) as u64)
            }
        });
        snapshot
    })
    .await
    .unwrap_or_default()
}

pub(super) async fn replay_pending_intents_for_fragment(
    fragment_hash: &str,
    live_index: &[PathBuf],
    live_blob: &[PathBuf],
    target_data_shards: usize,
    target_parity_shards: usize,
    target_stripe_size: usize,
    strict_durability: bool,
) -> Result<(), AppError> {
    let mut intent_files = Vec::<PathBuf>::new();
    for root in live_index {
        let intent_dir = fragment_root_on(root, fragment_hash).join(".intents");
        if !intent_dir.exists() {
            continue;
        }
        for entry in WalkDir::new(intent_dir).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy();
            if name.ends_with(REDIST_INTENT_SUFFIX) {
                intent_files.push(entry.path().to_path_buf());
            }
        }
    }
    if intent_files.is_empty() {
        return Ok(());
    }

    let mut grouped: HashMap<String, (RedistributionIntent, Vec<PathBuf>)> = HashMap::new();
    for path in intent_files {
        let bytes = match tokio_fs::read(&path).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let intent = match rmp_serde::from_slice::<RedistributionIntent>(&bytes) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let key = intent_dedupe_key(
            &intent.fragment,
            &intent.object_key,
            &intent.version_id,
            intent.is_trash,
        );
        grouped
            .entry(key)
            .and_modify(|(_, paths)| paths.push(path.clone()))
            .or_insert_with(|| (intent, vec![path]));
    }

    let mut first_err: Option<AppError> = None;
    for (_, (intent, mut found_paths)) in grouped {
        if should_stop() {
            break;
        }
        let mut all_intent_paths = get_all_intent_paths_custom(
            live_index,
            &intent.fragment,
            &intent.object_key,
            &intent.version_id,
            intent.is_trash,
        );
        all_intent_paths.append(&mut found_paths);
        all_intent_paths.sort();
        all_intent_paths.dedup();

        let mut meta = if intent.is_trash {
            match index::read_trashed_object_version(
                &intent.fragment,
                &intent.object_key,
                &intent.version_id,
            )
            .await
            {
                Ok(v) => v,
                Err(_) => {
                    remove_intent_paths_best_effort(&all_intent_paths).await;
                    continue;
                }
            }
        } else {
            match index::read_object_version(
                &intent.fragment,
                &intent.object_key,
                &intent.version_id,
            )
            .await
            {
                Ok(v) => v,
                Err(_) => {
                    remove_intent_paths_best_effort(&all_intent_paths).await;
                    continue;
                }
            }
        };

        let scope = intent.scope_mode();
        if scope == RedistributionScope::Namespace {
            remove_intent_paths_best_effort(&all_intent_paths).await;
            continue;
        }
        if live_blob.is_empty() {
            continue;
        }
        let needs = check_needs_redistribution(
            &meta,
            scope,
            target_data_shards,
            target_parity_shards,
            target_stripe_size,
            live_blob.len(),
            live_blob,
            live_index,
            intent.is_trash,
        )
        .await;

        if !needs.object() {
            remove_intent_paths_best_effort(&all_intent_paths).await;
            continue;
        }

        let resume_result = if needs.payload {
            redistribute_object_streaming(
                &mut meta,
                target_data_shards,
                target_parity_shards,
                target_stripe_size,
                live_blob,
                intent.is_trash,
                strict_durability,
            )
            .await
        } else {
            restore_colocated_metadata(&meta, intent.is_trash).await
        };

        match resume_result {
            Ok(_) => remove_intent_paths_best_effort(&all_intent_paths).await,
            Err(e) => {
                if should_stop() || is_stopped_error(&e.to_string()) {
                    break;
                }
                warn!(
                    "Failed replaying redistribution intent for {}/{} (v:{}) [Trash:{}]: {}",
                    intent.fragment, intent.object_key, intent.version_id, intent.is_trash, e
                );
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }

    if let Some(err) = first_err {
        return Err(err);
    }

    Ok(())
}

pub async fn replay_pending_intents_for_fragment_now(
    fragment_hash: &str,
    strict_durability: bool,
) -> Result<(), AppError> {
    let index_cfg = INDEX_STORE_DISKS
        .get()
        .ok_or(AppError::InternalServerError(
            "index roots not configured".to_string(),
        ))?
        .iter()
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    let live_index = reachable_roots(&index_cfg, true).await;
    if live_index.is_empty() {
        return Ok(());
    }

    let live_blob = reachable_roots(&ObjectPathManager::new().get_blob_roots(), true).await;
    let target_data_shards = DATA_SHARDS_ATOMIC
        .get()
        .map(|value| value.load(Ordering::Relaxed))
        .unwrap_or(1)
        .max(1);
    let target_parity_shards = PARITY_SHARDS_ATOMIC
        .get()
        .map(|value| value.load(Ordering::Relaxed))
        .unwrap_or(0);
    let target_stripe_size = STRIPE_SIZE_ATOMIC
        .get()
        .map(|value| value.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size);

    replay_pending_intents_for_fragment(
        fragment_hash,
        &live_index,
        &live_blob,
        target_data_shards,
        target_parity_shards,
        target_stripe_size,
        strict_durability,
    )
    .await
}

pub(super) async fn reachable_roots(roots: &[PathBuf], require_identity: bool) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for r in roots {
        if let Ok(meta) = tokio_fs::metadata(r).await {
            if meta.is_dir() && (!require_identity || configured_root_identity_is_current(r)) {
                out.push(r.clone());
            }
        }
    }
    out
}

async fn durable_atomic_write_bytes_with_mode(
    path: &Path,
    bytes: &[u8],
    strict_durability: bool,
) -> Result<(), AppError> {
    linux_io::atomic_write_with_durability(path, bytes.to_vec(), strict_durability).await
}

pub(super) async fn stage_old_shard_gc_with_symlink(old_abs: &str) -> Result<(), AppError> {
    let old_path = PathBuf::from(old_abs);
    if !tokio_fs::try_exists(&old_path).await.unwrap_or(false) {
        return Ok(());
    }

    let gc_path = PathBuf::from(format!("{}{}", old_abs, GC_SUFFIX));
    if tokio_fs::try_exists(&gc_path).await.unwrap_or(false) {
        return Ok(());
    }

    let parent = old_path
        .parent()
        .ok_or_else(|| AppError::InternalServerError("missing shard parent".into()))?
        .to_path_buf();

    linux_io::rename(&old_path, &gc_path).await?;

    let target_name = gc_path
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("")
        .to_string();

    let old_path_clone = old_path.clone();
    let parent_clone = parent.clone();

    let symlink_res = tokio::task::spawn_blocking(move || {
        let target = PathBuf::from(target_name);
        std::os::unix::fs::symlink(target, &old_path_clone)
    })
    .await
    .map_err(|_| AppError::InternalServerError("symlink join failed".into()))?;

    if let Err(e) = symlink_res {
        let _ = linux_io::rename(&gc_path, &old_path).await;
        return Err(AppError::Io(e, "create gc symlink".into()));
    }

    let _ = sync_dir(&parent_clone).await;
    Ok(())
}

pub(super) fn dedupe_id_from_metadata(meta: &ObjectMetadata, is_trash: bool) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    meta.fragment.hash(&mut h);
    meta.object_key.hash(&mut h);
    meta.version_id.hash(&mut h);
    is_trash.hash(&mut h);
    h.finish()
}

pub(super) async fn list_all_fragments(index_roots: &[PathBuf]) -> Vec<String> {
    let roots = index_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut out = HashSet::<String>::new();
        for root in roots {
            if !root.exists() {
                continue;
            }
            for name in list_fragment_aliases(&root) {
                let metadata_path = fragment_root_on(&root, &name).join(FRAGMENT_METADATA_FILE);
                if metadata_path.is_file() {
                    out.insert(stable_fragment_id_for_root(&root, &name).unwrap_or(name));
                }
            }
        }
        let mut v: Vec<String> = out.into_iter().collect();
        v.sort();
        v
    })
    .await
    .unwrap_or_default()
}

pub(super) async fn fragment_has_blob_payload(fragment: &str, blob_roots: &[PathBuf]) -> bool {
    let fragment = fragment.to_string();
    let roots = blob_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        for root in roots {
            let Ok(object_dirs) = index_store::local_object_dirs_under_root(&root, &fragment)
            else {
                continue;
            };
            for (object_dir, _) in object_dirs {
                if std::fs::read_dir(&object_dir).ok().is_some_and(|entries| {
                    entries.filter_map(Result::ok).any(|entry| {
                        entry.path().is_dir()
                            && std::fs::read_dir(entry.path())
                                .ok()
                                .and_then(|mut files| files.next())
                                .is_some()
                    })
                }) {
                    return true;
                }
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

pub(super) async fn fragment_has_indexed_payload(
    fragment: &str,
    active_index_roots: &[PathBuf],
) -> Result<bool, AppError> {
    let fragment_owned = fragment.to_string();
    let roots = active_index_roots.to_vec();
    let has_object_index = tokio::task::spawn_blocking(move || {
        index_store::fragment_has_object_index_records(&fragment_owned, &roots)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("N1 object index scan join: {e}")))?;
    if has_object_index {
        return Ok(true);
    }

    let live = index::scan_fragment_live_metadata_page(fragment, None, None, 1, None, true).await?;
    if !live.objects.is_empty() {
        return Ok(true);
    }

    let trash =
        index::scan_fragment_trash_metadata_page(fragment, None, None, 1, None, true).await?;
    Ok(!trash.objects.is_empty())
}
