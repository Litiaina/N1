use super::*;

pub(super) async fn list_all_fragments(
    index_roots: &[PathBuf],
    blob_roots: &[PathBuf],
) -> Vec<String> {
    let roots = index_roots.to_vec();
    let blob_roots = blob_roots.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut out = HashSet::new();
        for root in roots {
            if !root.exists() {
                continue;
            }
            for name in list_fragment_aliases(&root) {
                let Some(stable) = stable_fragment_id_for_root(&root, &name) else {
                    continue;
                };
                let has_metadata = blob_roots.iter().any(|blob_root| {
                    physical_fragment_root_on(blob_root, &stable)
                        .join(FRAGMENT_METADATA_FILE)
                        .is_file()
                });
                if has_metadata {
                    out.insert(stable);
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

pub(super) async fn reachable_dir_roots(roots: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for root in roots {
        if let Ok(meta) = fs::metadata(&root).await {
            if meta.is_dir() {
                out.push(root);
            }
        }
    }
    out
}

pub(super) fn sha256_bytes(b: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b);
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out[..]);
    arr
}

pub(super) async fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    let _ = fs::create_dir_all(parent).await;
    let tmp = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        Ulid::new().to_string()
    ));
    fs::write(&tmp, data).await?;
    fs::rename(&tmp, path).await?;
    Ok(())
}

#[derive(Clone)]
pub(super) struct RawReplica {
    bytes: Option<Vec<u8>>,
    mtime: SystemTime,
    len: u64,
    sha256: Option<[u8; 32]>,
    valid: bool,
}

pub(super) async fn read_raw_replica<F>(full: &Path, valid_fn: &F) -> RawReplica
where
    F: Fn(&[u8]) -> bool + Send + Sync,
{
    match fs::read(full).await {
        Ok(bytes) => {
            let meta = fs::metadata(full).await.ok();
            let mtime = meta
                .as_ref()
                .and_then(|m| m.modified().ok())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let len = meta.as_ref().map(|m| m.len()).unwrap_or(bytes.len() as u64);
            let sha = Some(sha256_bytes(&bytes));
            let valid = valid_fn(&bytes);

            RawReplica {
                bytes: Some(bytes),
                mtime,
                len,
                sha256: sha,
                valid,
            }
        }
        Err(_) => RawReplica {
            bytes: None,
            mtime: SystemTime::UNIX_EPOCH,
            len: 0,
            sha256: None,
            valid: false,
        },
    }
}

pub(super) fn choose_best_raw_replica(replicas: &[RawReplica]) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (i, r) in replicas.iter().enumerate() {
        if !r.valid {
            continue;
        }
        let take = match best {
            None => true,
            Some(cur) => {
                let c = &replicas[cur];
                (r.mtime > c.mtime)
                    || (r.mtime == c.mtime && r.len > c.len)
                    || (r.mtime == c.mtime
                        && r.len == c.len
                        && r.sha256.unwrap_or([0u8; 32]) > c.sha256.unwrap_or([0u8; 32]))
            }
        };
        if take {
            best = Some(i);
        }
    }
    best
}

pub(super) async fn replicate_bytes_strict(
    paths: &[PathBuf],
    canonical: &[u8],
    dry_run: bool,
) -> Result<(u64, u64), AppError> {
    let canon_sha = sha256_bytes(canonical);
    let mut missing = 0u64;
    let mut divergent = 0u64;

    for p in paths {
        match fs::read(p).await {
            Ok(b) => {
                if sha256_bytes(&b) != canon_sha {
                    divergent += 1;
                    if !dry_run {
                        atomic_write(p, canonical)
                            .await
                            .map_err(|e| AppError::Io(e, "repair replica file".into()))?;
                    }
                }
            }
            Err(_) => {
                missing += 1;
                if !dry_run {
                    atomic_write(p, canonical)
                        .await
                        .map_err(|e| AppError::Io(e, "repair replica file".into()))?;
                }
            }
        }
    }

    Ok((missing, divergent))
}

pub(super) async fn repair_fragment_metadata_replicas(
    blob_roots: &[PathBuf],
    fragment: &str,
    dry_run: bool,
) -> Result<(u64, u64, u64), AppError> {
    let stable_fragment = ObjectPathManager::new().resolve_stable_fragment_id(fragment);
    let paths: Vec<PathBuf> = blob_roots
        .iter()
        .map(|r| physical_fragment_root_on(r, &stable_fragment).join(FRAGMENT_METADATA_FILE))
        .collect();

    let valid = |b: &[u8]| -> bool { rmp_serde::from_slice::<serde_json::Value>(b).is_ok() };

    let mut reps = Vec::with_capacity(paths.len());
    for p in &paths {
        reps.push(read_raw_replica(p, &valid).await);
    }

    let Some(best) = choose_best_raw_replica(&reps) else {
        let any_exists = reps.iter().any(|r| r.bytes.is_some());
        let corrupt = if any_exists { 1 } else { 0 };
        return Ok((0, 0, corrupt));
    };

    let canon = reps[best].bytes.clone().unwrap();
    let (m, d) = replicate_bytes_strict(&paths, &canon, dry_run).await?;
    Ok((m, d, 0))
}
