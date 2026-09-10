//! Shared conservative orphan reclamation for GC and fragment maintenance.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;
use std::time::{Duration, SystemTime};

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
    if keep_running() && super::prune_orphan_object_dir(object_dir, now, ttl) {
        removed += 1;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

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
