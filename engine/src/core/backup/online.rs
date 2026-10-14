//! Committed-state online snapshots. Pin existing immutable files, copy small
//! mutable identity records, then release the boundary before hashing payload.
//! No mutable version head, namespace journal, object cache or upload checkpoint.
use super::{Result, format::Root, fs::private_dir, invalid, repository::Repository};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use walkdir::WalkDir;

pub(crate) const VIEW_DIR: &str = ".n1-online-backups";
const OWNER: &[u8] = b"N1 online backup scratch v1\n";

pub(crate) fn validate_result(value: &Value) -> Result<()> {
    if value["capture_mode"] != "online_committed" {
        return Err(invalid("source did not confirm committed online capture"));
    }
    super::fs::safe_id(
        value["snapshot"]
            .as_str()
            .ok_or_else(|| invalid("source returned no completed snapshot"))?,
    )
}

pub(crate) struct FrozenView {
    roots: Vec<Root>,
    jobs: Vec<PathBuf>,
    created_at: i64,
    boundary_ms: u128,
}
impl Drop for FrozenView {
    fn drop(&mut self) {
        for job in &self.jobs {
            if let Err(e) = fs::remove_dir_all(job) {
                tracing::warn!("Cannot remove online backup scratch {}: {e}", job.display());
            }
        }
    }
}

fn reference_path(refs: &Path, fragment: &str, object: &str) -> PathBuf {
    refs.join(fragment)
        .join(hex::encode(Sha256::digest(object.as_bytes())))
}
fn copy_small(source: &Path, target: &Path) -> Result<()> {
    let kind = fs::symlink_metadata(source)?;
    if !kind.is_file() {
        return Err(invalid("online identity metadata must be a regular file"));
    }
    if kind.len() > super::format::MAX_METADATA_BYTES as u64 {
        return Err(invalid("online metadata exceeds backup limit"));
    }
    let metadata = fs::metadata(source)?;
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(source, target)?;
    File::open(target)?.set_times(fs::FileTimes::new().set_modified(metadata.modified()?))?;
    Ok(())
}
fn pin(source: &Path, target: &Path) -> Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::hard_link(source, target)?;
    Ok(())
}
fn deadline(end: Instant) -> Result<()> {
    if Instant::now() > end {
        return Err(invalid(
            "online capture boundary exceeded its limit; view abandoned, no snapshot published",
        ));
    }
    Ok(())
}
fn namespace(
    source: &Path,
    target: &Path,
    fragment: &str,
    refs: &Path,
    end: Instant,
) -> Result<()> {
    if !source.exists() {
        return Ok(());
    }
    for entry in WalkDir::new(source).max_open(32).follow_links(false) {
        deadline(end)?;
        let entry = entry?;
        let relative = entry.path().strip_prefix(source)?;
        let dest = target.join(relative);
        if entry.file_type().is_dir() {
            fs::create_dir_all(dest)?;
        } else if entry.file_type().is_file()
            && entry.file_name().to_string_lossy().ends_with(".ncx")
        {
            if entry.metadata()?.len() > 65536 {
                return Err(invalid("NCX exceeds the online capture metadata limit"));
            }
            let pointer = crate::namespace::flat::read(entry.path())?
                .ok_or_else(|| invalid("captured NCX disappeared"))?;
            crate::core::index_store::normalize_object_id(&pointer.object_id)?;
            fs::create_dir_all(reference_path(refs, fragment, &pointer.object_id))?;
            pin(entry.path(), &dest)?;
        } else if entry.file_type().is_file()
            && entry.file_name().to_string_lossy().starts_with(".ncx-")
            && entry.file_name().to_string_lossy().ends_with(".tmp")
        {
            // An abandoned copy/publication temporary is not a namespace entry.
            continue;
        } else {
            return Err(invalid("unsupported file in committed namespace"));
        }
    }
    Ok(())
}

pub(crate) fn freeze_roots(original: &[Root], boundary_seconds: u64) -> Result<FrozenView> {
    super::capture::validate_roots(original)?;
    let id = ulid::Ulid::new().to_string();
    let mut view = FrozenView {
        roots: Vec::new(),
        jobs: Vec::new(),
        created_at: 0,
        boundary_ms: 0,
    };
    for root in original {
        let base = Path::new(&root.path).join(VIEW_DIR);
        private_dir(&base)?;
        let owner = base.join("owner");
        if owner.exists() {
            if super::fs::read_regular(&owner, 128)? != OWNER {
                return Err(invalid("unknown online backup scratch owner"));
            }
        } else {
            super::fs::atomic_new(&owner, OWNER)?;
        }
        let job = base.join(&id);
        fs::create_dir(&job)?;
        view.jobs.push(job.clone());
        let path = job.join("root");
        fs::create_dir(&path)?;
        let mut descriptor = root.clone();
        descriptor.path = path
            .to_str()
            .ok_or_else(|| invalid("non-UTF8 view path"))?
            .into();
        view.roots.push(descriptor);
    }
    let refs = view.jobs[0].join("references");
    fs::create_dir(&refs)?;
    // Waits for in-flight publication/NCX/reclamation, not upload body streams.
    // All sources stay fixed while creating the namespace and version selection.
    let started = Instant::now();
    let _boundary = super::barrier::freeze(boundary_seconds.clamp(1, 300))?;
    view.created_at = chrono::Utc::now().timestamp();
    let end = started + Duration::from_secs(boundary_seconds.clamp(1, 300));
    for (root, frozen) in original.iter().zip(&view.roots) {
        deadline(end)?;
        let source = Path::new(&root.path);
        let target = Path::new(&frozen.path);
        copy_small(&source.join(".n1-pool.json"), &target.join(".n1-pool.json"))?;
        let aliases = source.join(".fragment");
        if aliases.is_dir() {
            fs::create_dir(target.join(".fragment"))?;
            for alias in fs::read_dir(aliases)? {
                deadline(end)?;
                let alias = alias?;
                if !alias.file_type()?.is_symlink() {
                    return Err(invalid("invalid fragment alias in online view"));
                }
                std::os::unix::fs::symlink(
                    fs::read_link(alias.path())?,
                    target.join(".fragment").join(alias.file_name()),
                )?;
            }
        }
        for fragment in fs::read_dir(source)? {
            deadline(end)?;
            let fragment = fragment?;
            let name = fragment.file_name().to_string_lossy().into_owned();
            if !name.starts_with("@f_") {
                continue;
            }
            if !fragment.file_type()?.is_dir() {
                return Err(invalid("invalid physical fragment in online view"));
            }
            let dest = target.join(&name);
            fs::create_dir(&dest)?;
            fs::create_dir(dest.join(".objects"))?;
            copy_small(
                &fragment.path().join(".fragment.ndx"),
                &dest.join(".fragment.ndx"),
            )?;
            for part in [".namespace", ".trash"] {
                namespace(
                    &fragment.path().join(part),
                    &dest.join(part),
                    &name,
                    &refs,
                    end,
                )?;
            }
        }
    }
    // Reference inventory is on disk, not an O(object-count) memory cache.
    for (root, frozen) in original.iter().zip(&view.roots) {
        for fragment in fs::read_dir(&root.path)? {
            deadline(end)?;
            let fragment = fragment?;
            let name = fragment.file_name().to_string_lossy().into_owned();
            if !name.starts_with("@f_") || !fragment.file_type()?.is_dir() {
                continue;
            }
            let objects = fragment.path().join(".objects");
            if !objects.is_dir() {
                continue;
            }
            for entry in WalkDir::new(&objects)
                .max_open(32)
                .follow_links(false)
                .min_depth(1)
                .max_depth(3)
            {
                deadline(end)?;
                let entry = entry?;
                if entry.depth() != 3 {
                    continue;
                }
                let object = crate::core::index_store::object_id_from_dir_name(
                    &entry.file_name().to_string_lossy(),
                )
                .ok_or_else(|| invalid("invalid object directory"))?;
                if !reference_path(&refs, &name, &object).is_dir() {
                    continue;
                }
                if !entry.file_type().is_dir() {
                    return Err(invalid("invalid committed object directory"));
                }
                let relative = entry.path().strip_prefix(&root.path)?;
                let dest = Path::new(&frozen.path).join(relative);
                for metadata in fs::read_dir(entry.path())? {
                    deadline(end)?;
                    let metadata = metadata?;
                    let Some(version) = crate::core::index_store::backup_version_from_ndx_name(
                        &metadata.file_name().to_string_lossy(),
                    ) else {
                        continue;
                    };
                    if !metadata.file_type()?.is_file() {
                        return Err(invalid("invalid committed version metadata"));
                    }
                    pin(&metadata.path(), &dest.join(metadata.file_name()))?;
                    let payload = entry.path().join(&version);
                    for piece in fs::read_dir(&payload)? {
                        deadline(end)?;
                        let piece = piece?;
                        if !piece.file_type()?.is_file() {
                            return Err(invalid("invalid committed version piece"));
                        }
                        if !matches!(
                            piece.path().extension().and_then(|e| e.to_str()),
                            Some("shard" | "parity")
                        ) {
                            continue;
                        }
                        pin(&piece.path(), &dest.join(&version).join(piece.file_name()))?;
                    }
                }
            }
        }
    }
    view.boundary_ms = started.elapsed().as_millis();
    drop(_boundary);
    // Scratch progress only (not a commit record or part of the snapshot).
    fs::write(view.jobs[0].join("ready"), b"committed view pinned\n")?;
    Ok(view)
}

pub(crate) fn capture(repo: &Repository, config: &Path, boundary_seconds: u64) -> Result<Value> {
    let roots = super::capture::roots_from_config(config)?;
    let live = crate::core::disk_manager::ObjectPathManager::new()
        .get_blob_roots()
        .into_iter()
        .map(fs::canonicalize)
        .collect::<std::io::Result<BTreeSet<_>>>()?;
    if roots
        .iter()
        .map(|r| PathBuf::from(&r.path))
        .collect::<BTreeSet<_>>()
        != live
    {
        return Err(invalid(
            "online capture configuration does not match the running engine",
        ));
    }
    for root in &roots {
        if repo.path.starts_with(&root.path) || Path::new(&root.path).starts_with(&repo.path) {
            return Err(invalid("backup repository must be outside source roots"));
        }
    }
    let view = freeze_roots(&roots, boundary_seconds)?;
    let s = super::capture::create_from_view(
        repo,
        view.roots.clone(),
        roots,
        4,
        view.created_at,
        Some("online_committed".into()),
    )?;
    Ok(
        json!({"snapshot":s.id,"source_bytes":s.source_bytes,"records":s.records,"capture_mode":"online_committed","boundary_ms":view.boundary_ms}),
    )
}

/// Called after startup owns every live member lease, before serving traffic.
pub fn cleanup_interrupted_views(roots: &[PathBuf]) -> Result<()> {
    for root in roots {
        let base = root.join(VIEW_DIR);
        if !base.try_exists()? {
            continue;
        }
        if !fs::symlink_metadata(&base)?.is_dir() {
            return Err(invalid("unrecognized online backup scratch directory"));
        }
        let owned = base.join("owner").try_exists()?;
        if owned && super::fs::read_regular(&base.join("owner"), 128)? != OWNER {
            return Err(invalid("unrecognized online backup scratch owner"));
        }
        for job in fs::read_dir(&base)? {
            let job = job?;
            if job.file_name() == "owner" {
                continue;
            }
            // Owner publication itself can be interrupted before/after link.
            // Only its exact bounded temporary format may be cleaned without
            // an owner; never adopt/delete unknown contents in this directory.
            if job
                .file_name()
                .to_str()
                .and_then(|n| n.strip_prefix(".pending-"))
                .is_some_and(|id| ulid::Ulid::from_string(id).is_ok())
                && job.file_type()?.is_file()
            {
                let bytes = super::fs::read_regular(&job.path(), 128)?;
                if OWNER.starts_with(&bytes) {
                    fs::remove_file(job.path())?;
                    continue;
                }
            }
            if !owned {
                return Err(invalid("unrecognized unowned online scratch contents"));
            }
            if ulid::Ulid::from_string(&job.file_name().to_string_lossy()).is_err()
                || !job.file_type()?.is_dir()
            {
                return Err(invalid("unrecognized online backup scratch entry"));
            }
            fs::remove_dir_all(job.path())?;
        }
        if !owned {
            fs::remove_dir(&base)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn startup_recovers_owner_publication_interruption_without_adopting_unknown_data() {
        let root = std::env::temp_dir().join(format!("n1-view-startup-{}", ulid::Ulid::new()));
        fs::create_dir(&root).unwrap();
        let base = root.join(VIEW_DIR);
        private_dir(&base).unwrap();
        fs::write(
            base.join(format!(".pending-{}", ulid::Ulid::new())),
            &OWNER[..8],
        )
        .unwrap();
        cleanup_interrupted_views(std::slice::from_ref(&root)).unwrap();
        assert!(!base.exists());
        private_dir(&base).unwrap();
        fs::write(base.join("unknown"), b"preserve").unwrap();
        assert!(cleanup_interrupted_views(std::slice::from_ref(&root)).is_err());
        assert!(base.join("unknown").exists());
        fs::remove_file(base.join("unknown")).unwrap();
        fs::write(base.join("owner"), OWNER).unwrap();
        fs::write(base.join(format!(".pending-{}", ulid::Ulid::new())), OWNER).unwrap();
        let job = base.join(ulid::Ulid::new().to_string());
        fs::create_dir(&job).unwrap();
        cleanup_interrupted_views(std::slice::from_ref(&root)).unwrap();
        assert!(!job.exists());
        assert!(base.join("owner").exists());
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn empty_or_offline_responses_are_not_online_capture_acknowledgements() {
        assert!(validate_result(&json!({})).is_err());
        assert!(
            validate_result(
                &json!({"snapshot":ulid::Ulid::new().to_string(),"capture_mode":"offline_full"})
            )
            .is_err()
        );
        assert!(
            validate_result(
                &json!({"snapshot":ulid::Ulid::new().to_string(),"capture_mode":"online_committed"})
            )
            .is_ok()
        );
    }
    #[test]
    fn pinned_view_excludes_unfinished_versions_and_survives_source_deletion() {
        let base = std::env::temp_dir().join(format!("n1-online-{}", ulid::Ulid::new()));
        fs::create_dir(&base).unwrap();
        fs::write(base.join(".n1-pool.json"), b"fixture identity").unwrap();
        let fragment = base.join("@f_fixture");
        fs::create_dir(&fragment).unwrap();
        fs::write(fragment.join(".fragment.ndx"), b"fixture metadata").unwrap();
        let object = ulid::Ulid::new().to_string();
        let version = ulid::Ulid::new().to_string();
        let unfinished = ulid::Ulid::new().to_string();
        let pointer = crate::namespace::flat::Pointer {
            format: 4,
            pool_id: "A".into(),
            object_id: object.clone(),
        };
        let body = rmp_serde::to_vec_named(&pointer).unwrap();
        let mut ncx = b"N1NCX4\0".to_vec();
        ncx.extend(Sha256::digest(&body));
        ncx.extend(body);
        fs::create_dir(fragment.join(".namespace")).unwrap();
        fs::write(fragment.join(".namespace/file.ncx"), &ncx).unwrap();
        fs::write(
            fragment.join(".namespace/.ncx-interrupted.tmp"),
            b"not published",
        )
        .unwrap();
        let object_dir = fragment.join(".objects/aa/bb").join(format!("@o_{object}"));
        fs::create_dir_all(object_dir.join(&version)).unwrap();
        fs::create_dir(object_dir.join(&unfinished)).unwrap();
        let metadata = format!("{version}.4.1.0.ndx");
        fs::write(object_dir.join(&metadata), b"committed metadata").unwrap();
        fs::write(object_dir.join(&version).join("0.shard"), b"DATA").unwrap();
        fs::write(object_dir.join(&unfinished).join("0.shard"), b"PART").unwrap();
        fs::create_dir(fragment.join(".uploads")).unwrap();
        fs::write(fragment.join(".uploads/pending"), b"durable receipt").unwrap();
        let root = Root {
            path: base.to_str().unwrap().into(),
            legacy_path: None,
            index: true,
            blob: true,
            member: None,
        };
        let view = freeze_roots(&[root], 5).unwrap();
        let frozen = Path::new(&view.roots[0].path).join("@f_fixture");
        let pinned = frozen.join(".objects/aa/bb").join(format!("@o_{object}"));
        assert!(!frozen.join(".uploads").exists());
        assert!(!pinned.join(&unfinished).exists());
        assert!(!frozen.join(".namespace/.ncx-interrupted.tmp").exists());
        fs::remove_dir_all(&fragment).unwrap();
        assert_eq!(
            fs::read(pinned.join(&version).join("0.shard")).unwrap(),
            b"DATA"
        );
        assert_eq!(fs::read(frozen.join(".namespace/file.ncx")).unwrap(), ncx);
        let job = view.jobs[0].clone();
        drop(view);
        assert!(!job.exists());
        cleanup_interrupted_views(std::slice::from_ref(&base)).unwrap();
        fs::remove_dir_all(base).unwrap();
    }
}
