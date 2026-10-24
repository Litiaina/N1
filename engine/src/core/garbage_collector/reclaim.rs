//! Shared conservative orphan reclamation for GC and fragment maintenance.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::core::index_store;
use crate::enums::app_error_enum::AppError;

fn reclaim_error(error: io::Error) -> AppError {
    AppError::Io(
        error,
        "inspect or reclaim unpublished N1 version".to_string(),
    )
}

/// A summary filename is enough to PRESERVE a version, even if its NDX body is
/// damaged. GC must not mistake a corrupt committed version for an abandoned
/// upload. Unknown metadata names/types make absence of authority uncertain.
fn ndx_versions(object_dir: &Path) -> Result<HashSet<String>, AppError> {
    Ok(inspect_object_versions(object_dir)?.0)
}

fn inspect_object_versions(
    object_dir: &Path,
) -> Result<(HashSet<String>, Vec<fs::DirEntry>), AppError> {
    let mut committed = HashSet::new();
    let mut versions = Vec::new();
    for entry in fs::read_dir(object_dir).map_err(reclaim_error)? {
        let entry = entry.map_err(reclaim_error)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.ends_with(".ndx") {
            if entry.file_type().map_err(reclaim_error)?.is_dir() {
                versions.push(entry);
            }
            continue;
        }
        let Some(version) = index_store::backup_version_from_ndx_name(&name) else {
            return Err(AppError::Conflict(
                "unrecognized NDX authority; preserving unpublished candidates".into(),
            ));
        };
        if !entry.file_type().map_err(reclaim_error)?.is_file() {
            return Err(AppError::Conflict(
                "unsafe NDX authority type; preserving unpublished candidates".into(),
            ));
        }
        committed.insert(version);
    }
    Ok((committed, versions))
}

/// Inspect every component below an enrolled root without following a
/// symlink. A missing directory is absence; an unreadable or redirected path
/// is not authority to delete a version on another member.
pub(crate) fn object_directory_present(root: &Path, directory: &Path) -> Result<bool, AppError> {
    let relative = directory.strip_prefix(root).map_err(|_| {
        AppError::Conflict("unpublished candidate escaped its enrolled root".into())
    })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(AppError::Conflict(
                "unsafe unpublished candidate path".into(),
            ));
        }
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_dir() => {}
            Ok(_) => {
                return Err(AppError::Conflict(
                    "unsafe object authority path; preserving unpublished candidates".into(),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(reclaim_error(error)),
        }
    }
    Ok(true)
}

fn committed_versions_on_members(
    fragment: &str,
    object_id: &str,
) -> Result<HashSet<String>, AppError> {
    // Foreground placement intentionally filters unavailable/stale identities.
    // Absence authority must instead include EVERY configured member: otherwise
    // the sole surviving NDX on an omitted member could be mistaken for absence.
    let roots = crate::core::disk_manager::BLOB_STORE_DISKS
        .get()
        .into_iter()
        .flatten()
        .map(std::path::PathBuf::from)
        .collect::<Vec<_>>();
    if roots.is_empty() {
        return Err(AppError::ServiceUnavailable(
            "no enrolled members establish unpublished version authority".into(),
        ));
    }
    let mut committed = HashSet::new();
    for root in roots {
        if !crate::core::storage_pool::root_available(&root) {
            return Err(AppError::ServiceUnavailable(
                "an enrolled member is unavailable; preserving unpublished candidates".into(),
            ));
        }
        let directory = index_store::object_index_dir(&root, fragment, object_id)?;
        if object_directory_present(&root, &directory)? {
            committed.extend(ndx_versions(&directory)?);
        }
    }
    Ok(committed)
}

fn committed_with_authority(
    authority: &mut Option<HashSet<String>>,
    version: &str,
    load: impl FnOnce() -> Result<HashSet<String>, AppError>,
) -> Result<bool, AppError> {
    if authority.is_none() {
        *authority = Some(load()?);
    }
    Ok(authority.as_ref().unwrap().contains(version))
}

/// Reclaim only unpublished, expired version directories below a referenced
/// object. The caller must hold UploadGcScan's object reclaim guard. Committed
/// history and NDX files are never removed. The common healthy path checks
/// local NDX names only; missing local authority requires checking the other
/// enrolled members before an abandoned version can be removed.
pub(crate) fn reclaim_unpublished_versions(
    fragment: &str,
    object_id: &str,
    object_dir: &Path,
    active_versions: &HashSet<String>,
    now: SystemTime,
    ttl: Duration,
    keep_running: impl FnMut() -> bool,
) -> Result<(u64, u64), AppError> {
    // The caller fences this object against publication. Cross-member NDX
    // authority is needed only for an expired candidate without local NDX,
    // and can be established once for this object, not once per candidate.
    let mut remote_authority = None;
    reclaim_unpublished_versions_with(
        object_dir,
        active_versions,
        now,
        ttl,
        keep_running,
        |version| {
            let committed = committed_with_authority(&mut remote_authority, version, || {
                committed_versions_on_members(fragment, object_id)
            })?;
            // An object-scoped authority set is valid only while its members
            // remain enrolled/available. Do not turn a mount replacement in a
            // long history walk into authority to delete the next candidate.
            for root in crate::core::disk_manager::BLOB_STORE_DISKS
                .get()
                .into_iter()
                .flatten()
            {
                if !crate::core::storage_pool::root_available(Path::new(root)) {
                    return Err(AppError::ServiceUnavailable(
                        "member changed during unpublished-version cleanup; preserving candidates"
                            .into(),
                    ));
                }
            }
            Ok(committed)
        },
    )
}

fn reclaim_unpublished_versions_with(
    object_dir: &Path,
    active_versions: &HashSet<String>,
    now: SystemTime,
    ttl: Duration,
    mut keep_running: impl FnMut() -> bool,
    mut committed_elsewhere: impl FnMut(&str) -> Result<bool, AppError>,
) -> Result<(u64, u64), AppError> {
    let _snapshot_boundary = crate::core::backup::barrier::mutation();
    match fs::symlink_metadata(object_dir) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => return Ok((0, 0)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(error) => return Err(reclaim_error(error)),
    }
    let (committed, versions) = inspect_object_versions(object_dir)?;
    let mut removed = 0;
    let mut live_versions_seen = 0;
    for entry in versions {
        if !keep_running() {
            break;
        }
        let version = entry.file_name().to_string_lossy().into_owned();
        // Redistribution artifacts still need cleanup, including beneath a
        // live version. Reuse this directory pass instead of enumerating the
        // object once more in the outer collector. No payload bytes are read.
        super::gc_cleanup_version_dir_unix(
            &entry.path(),
            now,
            ttl,
            false,
            !active_versions.contains(&version),
        );
        // Avoid even a version-directory stat for committed or active data.
        if committed.contains(&version) {
            live_versions_seen += 1;
            continue;
        }
        if active_versions.contains(&version) {
            continue;
        }
        let modified = entry
            .metadata()
            .map_err(reclaim_error)?
            .modified()
            .map_err(reclaim_error)?;
        let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
        if !super::orphan_version_reclaimable(&version, false, false, age, ttl)
            || committed_elsewhere(&version)?
        {
            continue;
        }
        fs::remove_dir_all(entry.path()).map_err(reclaim_error)?;
        removed += 1;
    }
    Ok((removed, live_versions_seen))
}

/// Visit only real version directories. Unknown entries and recent/active
/// versions survive even when their parent is old. `keep_running` also lets
/// background GC report progress and stop between entries.
pub(crate) fn reclaim_object_versions(
    object_dir: &Path,
    active_versions: &HashSet<String>,
    now: SystemTime,
    ttl: Duration,
    mut keep_running: impl FnMut() -> bool,
) -> io::Result<u64> {
    let _snapshot_boundary = crate::core::backup::barrier::mutation();
    let metadata = match fs::symlink_metadata(object_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_dir() {
        return Ok(0);
    }
    let mut removed = 0;
    for entry in fs::read_dir(object_dir)? {
        if !keep_running() {
            return Ok(removed);
        }
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let version_id = entry.file_name().to_string_lossy().into_owned();
        super::gc_cleanup_version_dir_unix(
            &entry.path(),
            now,
            ttl,
            false,
            !active_versions.contains(&version_id),
        );
        let modified = entry.metadata()?.modified()?;
        let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
        if super::orphan_version_reclaimable(
            &version_id,
            false,
            active_versions.contains(&version_id),
            age,
            ttl,
        ) {
            fs::remove_dir_all(entry.path())?;
            removed += 1;
        }
    }
    // This helper is only for objects whose live/trash references have already
    // been ruled out under an upload reclaim guard. Remove recognized, expired
    // NDX leftovers only AFTER their matching version directory is absent. A
    // prior interrupted sweep can have removed the payload but left its NDX;
    // unknown files and active/young version authorities still survive.
    for entry in fs::read_dir(object_dir)? {
        if !keep_running() {
            return Ok(removed);
        }
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let Some(version) = entry
            .file_name()
            .to_str()
            .and_then(index_store::backup_version_from_ndx_name)
        else {
            continue;
        };
        if active_versions.contains(&version) {
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
        if !super::orphan_blob_age_expired(age, ttl) {
            continue;
        }
        match fs::symlink_metadata(object_dir.join(&version)) {
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::remove_file(entry.path())?;
    }
    if keep_running() && super::prune_orphan_object_dir(object_dir, now, ttl) {
        removed += 1;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_candidates_load_cross_member_authority_once_per_object() {
        let calls = std::cell::Cell::new(0);
        let mut authority = None;
        for version in ["retained", "abandoned-a", "abandoned-b", "retained"] {
            let committed = committed_with_authority(&mut authority, version, || {
                calls.set(calls.get() + 1);
                Ok(HashSet::from(["retained".to_string()]))
            })
            .unwrap();
            assert_eq!(committed, version == "retained");
        }
        assert_eq!(
            calls.get(),
            1,
            "history candidates must not rescan the object per version"
        );
        let mut failed = None;
        assert!(
            committed_with_authority(&mut failed, "candidate", || {
                Err(AppError::ServiceUnavailable("member unavailable".into()))
            })
            .is_err()
        );
        assert!(
            failed.is_none(),
            "uncertain authority must never become an empty absence set"
        );
    }

    fn version_directory(base: &Path, modified: SystemTime) -> String {
        let version = ulid::Ulid::new().to_string();
        let directory = base.join(&version);
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("payload"), b"immutable test bytes").unwrap();
        fs::File::open(directory)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(modified))
            .unwrap();
        version
    }

    fn ndx_file(base: &Path, version: &str, modified: SystemTime) -> std::path::PathBuf {
        let path = base.join(format!("{version}.1.0.0.ndx"));
        fs::write(&path, b"NDX authority").unwrap();
        fs::File::open(&path)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(modified))
            .unwrap();
        path
    }

    #[test]
    fn unreferenced_objects_remove_expired_ndx_only_after_matching_payload_is_gone() {
        let base = std::env::temp_dir().join(format!("n1-orphan-ndx-{}", ulid::Ulid::new()));
        let now = SystemTime::now();
        let old = now - Duration::from_secs(120);
        let expired = version_directory(&base, old);
        let young = version_directory(&base, now);
        let active = version_directory(&base, old);
        let expired_ndx = ndx_file(&base, &expired, old);
        let young_ndx = ndx_file(&base, &young, old);
        let active_ndx = ndx_file(&base, &active, old);
        let already_removed = ulid::Ulid::new().to_string();
        let leftover_ndx = ndx_file(&base, &already_removed, old);
        let fresh_ndx = ndx_file(&base, &ulid::Ulid::new().to_string(), now);
        let malformed = base.join("unknown.ndx");
        fs::write(&malformed, b"unknown data").unwrap();
        assert_eq!(
            reclaim_object_versions(
                &base,
                &HashSet::from([active.clone()]),
                now,
                Duration::from_secs(60),
                || true,
            )
            .unwrap(),
            1
        );
        assert!(!base.join(expired).exists());
        assert!(!expired_ndx.exists());
        assert!(!leftover_ndx.exists());
        for path in [young_ndx, active_ndx, fresh_ndx, malformed] {
            assert!(path.is_file());
        }
        assert!(base.join(young).join("payload").is_file());
        assert!(base.join(active).join("payload").is_file());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn referenced_object_reclaims_only_expired_unpublished_inactive_versions() {
        let base = std::env::temp_dir().join(format!("n1-live-reclaim-{}", ulid::Ulid::new()));
        let now = SystemTime::now();
        let old = now - Duration::from_secs(120);
        let local = version_directory(&base, old);
        let remote = version_directory(&base, old);
        let abandoned = version_directory(&base, old);
        let active = version_directory(&base, old);
        let young = version_directory(&base, now);
        fs::create_dir(base.join("unknown-version")).unwrap();
        // A damaged NDX still establishes reason to preserve, not to reclaim.
        fs::write(base.join(format!("{local}.1.0.0.ndx")), b"damaged NDX").unwrap();
        let mut checked = HashSet::new();
        assert_eq!(
            reclaim_unpublished_versions_with(
                &base,
                &HashSet::from([active.clone()]),
                now,
                Duration::from_secs(60),
                || true,
                |version| {
                    checked.insert(version.to_string());
                    Ok(version == remote)
                },
            )
            .unwrap(),
            (1, 1)
        );
        assert_eq!(checked, HashSet::from([remote.clone(), abandoned.clone()]));
        assert!(!base.join(abandoned).exists());
        for version in [local.clone(), remote, active, young] {
            assert_eq!(
                fs::read(base.join(version).join("payload")).unwrap(),
                b"immutable test bytes"
            );
        }
        assert_eq!(
            fs::read(base.join(format!("{local}.1.0.0.ndx"))).unwrap(),
            b"damaged NDX"
        );
        assert!(base.join("unknown-version").is_dir());
        fs::remove_dir_all(base).unwrap();
    }

    fn batch_temporary(directory: &Path, modified: SystemTime) -> std::path::PathBuf {
        let file = directory.join(format!(".n1-batch-{}.tmp", ulid::Ulid::new()));
        fs::write(&file, b"unpublished private batch metadata").unwrap();
        fs::File::open(&file)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(modified))
            .unwrap();
        file
    }

    #[cfg(unix)]
    #[test]
    fn batch_temporaries_expire_in_existing_version_pass_without_touching_committed_data() {
        use crate::core::upload::{UploadActivityGuard, UploadGcScan};
        use std::os::unix::fs::symlink;

        let fragment = ulid::Ulid::new().to_string();
        let object = ulid::Ulid::new().to_string();
        let base = std::env::temp_dir().join(format!("n1-batch-temp-gc-{fragment}"));
        let now = SystemTime::now();
        let ttl = Duration::from_secs(60);
        let old = now - Duration::from_secs(120);
        let version = version_directory(&base, old);
        let directory = base.join(&version);
        let authority = ndx_file(&base, &version, old);
        let authority_before = fs::read(&authority).unwrap();
        let authority_modified = fs::metadata(&authority).unwrap().modified().unwrap();
        let payload = directory.join("payload");
        let payload_before = fs::read(&payload).unwrap();
        let payload_modified = fs::metadata(&payload).unwrap().modified().unwrap();
        let expired = batch_temporary(&directory, old);
        let fresh = batch_temporary(&directory, now);
        let unfamiliar = directory.join(".n1-batch-not-a-ulid.tmp");
        fs::write(&unfamiliar, b"unknown file must remain").unwrap();
        let nested = directory.join(format!(".n1-batch-{}.tmp", ulid::Ulid::new()));
        fs::create_dir(&nested).unwrap();
        let outside = base.join("outside-committed-payload.gc");
        fs::write(&outside, b"symlink target must remain").unwrap();
        let link = directory.join(format!(".n1-batch-{}.tmp", ulid::Ulid::new()));
        symlink(&outside, &link).unwrap();

        let upload = UploadActivityGuard::begin(&fragment, &object).unwrap();
        let scan = UploadGcScan::begin(&fragment).unwrap();
        if let Some(_reclaim) = scan.try_reclaim(&object).unwrap() {
            reclaim_unpublished_versions_with(
                &base,
                &HashSet::new(),
                now,
                ttl,
                || true,
                |_| panic!("a live upload must prevent entering reclamation"),
            )
            .unwrap();
        }
        assert!(expired.is_file(), "active upload must protect its staging");
        drop(upload);
        assert!(
            scan.try_reclaim(&object).unwrap().is_none(),
            "the overlapping scan stays conservative after upload completion"
        );
        drop(scan);

        let next = UploadGcScan::begin(&fragment).unwrap();
        let _reclaim = next.try_reclaim(&object).unwrap().unwrap();
        assert_eq!(
            reclaim_unpublished_versions_with(
                &base,
                &HashSet::new(),
                now,
                ttl,
                || true,
                |_| panic!("local committed NDX must avoid remote authority checks"),
            )
            .unwrap(),
            (0, 1)
        );
        assert!(!expired.exists());
        assert!(fresh.is_file());
        assert!(unfamiliar.is_file());
        assert!(nested.is_dir());
        assert!(
            fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(&outside).unwrap(), b"symlink target must remain");
        assert_eq!(fs::read(&authority).unwrap(), authority_before);
        assert_eq!(fs::read(&payload).unwrap(), payload_before);
        assert_eq!(
            fs::metadata(&authority).unwrap().modified().unwrap(),
            authority_modified
        );
        assert_eq!(
            fs::metadata(&payload).unwrap().modified().unwrap(),
            payload_modified
        );
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn active_session_versions_keep_private_batch_temporaries_even_under_committed_ndx() {
        let base = std::env::temp_dir().join(format!("n1-active-batch-gc-{}", ulid::Ulid::new()));
        let now = SystemTime::now();
        let old = now - Duration::from_secs(120);
        let version = version_directory(&base, old);
        let temporary = batch_temporary(&base.join(&version), old);
        let authority = ndx_file(&base, &version, old);
        assert_eq!(
            reclaim_unpublished_versions_with(
                &base,
                &HashSet::from([version]),
                now,
                Duration::ZERO,
                || true,
                |_| panic!("local committed NDX must avoid remote authority checks"),
            )
            .unwrap(),
            (0, 1)
        );
        assert!(temporary.is_file());
        assert!(authority.is_file());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn batch_temporary_recognition_requires_exact_canonical_ulid_filename() {
        let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        assert!(super::super::is_batch_temporary_name(&format!(
            ".n1-batch-{id}.tmp"
        )));
        for name in [
            format!("n1-batch-{id}.tmp"),
            format!(".n1-batch-{id}.tmp.extra"),
            format!(".n1-batch-{}.tmp", id.to_lowercase()),
            format!(".n1-batch-{id}0.tmp"),
            ".n1-batch-not-a-ulid.tmp".to_string(),
            "part_0_shard_0.shard".to_string(),
            format!("{id}.1.0.0.ndx"),
        ] {
            assert!(!super::super::is_batch_temporary_name(&name), "{name}");
        }
    }

    #[test]
    fn missing_or_unreadable_remote_authority_preserves_candidate() {
        let base = std::env::temp_dir().join(format!("n1-uncertain-reclaim-{}", ulid::Ulid::new()));
        let now = SystemTime::now();
        let version = version_directory(&base, now - Duration::from_secs(120));
        let result = reclaim_unpublished_versions_with(
            &base,
            &HashSet::new(),
            now,
            Duration::ZERO,
            || true,
            |_| Err(AppError::ServiceUnavailable("unavailable member".into())),
        );
        assert!(result.is_err());
        assert!(base.join(version).join("payload").is_file());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn malformed_ndx_authority_preserves_all_candidate_versions() {
        let base = std::env::temp_dir().join(format!("n1-unknown-ndx-{}", ulid::Ulid::new()));
        let now = SystemTime::now();
        let version = version_directory(&base, now - Duration::from_secs(120));
        fs::write(base.join("unrecognized.ndx"), b"uncertain authority").unwrap();
        assert!(
            reclaim_unpublished_versions_with(
                &base,
                &HashSet::new(),
                now,
                Duration::ZERO,
                || true,
                |_| panic!("unknown local metadata must stop before remote checks"),
            )
            .is_err()
        );
        assert!(base.join(version).join("payload").is_file());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn referenced_reclaim_honors_stop_without_remote_checks() {
        let base = std::env::temp_dir().join(format!("n1-stopped-reclaim-{}", ulid::Ulid::new()));
        let now = SystemTime::now();
        let version = version_directory(&base, now - Duration::from_secs(120));
        assert_eq!(
            reclaim_unpublished_versions_with(
                &base,
                &HashSet::new(),
                now,
                Duration::ZERO,
                || false,
                |_| panic!("stopped scan must not inspect another member"),
            )
            .unwrap(),
            (0, 0)
        );
        assert!(base.join(version).join("payload").is_file());
        fs::remove_dir_all(base).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn referenced_reclaim_and_authority_checks_do_not_follow_symlinks() {
        use std::os::unix::fs::symlink;
        let base = std::env::temp_dir().join(format!("n1-symlink-reclaim-{}", ulid::Ulid::new()));
        let now = SystemTime::now();
        let target = base.join("target");
        let version = version_directory(&target, now - Duration::from_secs(120));
        let link = base.join("object-link");
        symlink(&target, &link).unwrap();
        assert_eq!(
            reclaim_unpublished_versions_with(
                &link,
                &HashSet::new(),
                now,
                Duration::ZERO,
                || true,
                |_| panic!("symlink object must not be inspected"),
            )
            .unwrap(),
            (0, 0)
        );
        assert!(object_directory_present(&base, &link.join(&version)).is_err());
        let object = base.join("real-object");
        fs::create_dir(&object).unwrap();
        symlink(target.join(&version), object.join(&version)).unwrap();
        assert_eq!(
            reclaim_unpublished_versions_with(
                &object,
                &HashSet::new(),
                now,
                Duration::ZERO,
                || true,
                |_| panic!("symlink version must not be inspected"),
            )
            .unwrap(),
            (0, 0)
        );
        symlink(
            target.join(&version).join("payload"),
            object.join(format!("{version}.1.0.0.ndx")),
        )
        .unwrap();
        assert!(ndx_versions(&object).is_err());
        assert!(target.join(version).join("payload").is_file());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn reclaim_respects_each_version_age_activity_unknown_entries_and_stop() {
        let base = std::env::temp_dir().join(format!("n1-reclaim-{}", ulid::Ulid::new()));
        let now = SystemTime::now();
        let old = now - Duration::from_secs(120);
        let expired = ulid::Ulid::new().to_string();
        let young = ulid::Ulid::new().to_string();
        let active = ulid::Ulid::new().to_string();
        for (name, modified) in [
            (&expired, old),
            (&young, now),
            (&active, old),
            (&"unknown".to_string(), old),
        ] {
            let dir = base.join(name);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("payload"), b"test").unwrap();
            fs::File::open(dir)
                .unwrap()
                .set_times(fs::FileTimes::new().set_modified(modified))
                .unwrap();
        }
        fs::File::open(&base)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(old))
            .unwrap();
        let active_versions = HashSet::from([active.clone()]);
        assert_eq!(
            reclaim_object_versions(
                &base,
                &active_versions,
                now,
                Duration::from_secs(60),
                || false
            )
            .unwrap(),
            0
        );
        assert!(base.join(&expired).exists());
        assert_eq!(
            reclaim_object_versions(
                &base,
                &active_versions,
                now,
                Duration::from_secs(60),
                || true
            )
            .unwrap(),
            1
        );
        assert!(!base.join(expired).exists());
        for name in [young, active, "unknown".to_string()] {
            assert_eq!(fs::read(base.join(name).join("payload")).unwrap(), b"test");
        }
        fs::remove_dir_all(base).unwrap();
    }
}
