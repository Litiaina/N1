use crate::core::disk_manager::{BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use ulid::Ulid;

const ROOT_MARKER_FILE: &str = ".n1-root-id";
const REGISTRY_DIR: &str = ".n1";
const REGISTRY_FILE: &str = "topology-roots.json";
const ROOT_IDENTITY_FORMAT: &str = "n1-4-root-identity-v1";

static EXPECTED_ROOT_IDENTITIES: OnceLock<BTreeMap<String, String>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RootMarker {
    engine_format: String,
    root_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RootRegistry {
    engine_format: String,
    roots: BTreeMap<String, String>,
}

impl Default for RootRegistry {
    fn default() -> Self {
        Self {
            engine_format: ROOT_IDENTITY_FORMAT.to_string(),
            roots: BTreeMap::new(),
        }
    }
}

fn io_context(error: io::Error, action: &str, path: &Path) -> String {
    format!("{action} '{}': {error}", path.display())
}

fn canonical_root_key(path: &Path) -> String {
    fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .into_owned()
}

fn registry_path() -> Result<PathBuf, String> {
    let root = FS_STORAGE_ROOT
        .get()
        .ok_or_else(|| "N1 fs_storage_path is not initialized".to_string())?;
    Ok(PathBuf::from(root).join(REGISTRY_DIR).join(REGISTRY_FILE))
}

fn sync_parent(path: &Path) -> Result<(), String> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_context(error, "sync parent directory for", path))
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("root identity path '{}' has no parent", path.display()))?;
    fs::create_dir_all(parent)
        .map_err(|error| io_context(error, "create root identity directory", parent))?;
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| format!("encode root identity '{}': {error}", path.display()))?;
    let temp = parent.join(format!(".{}.tmp-{}", REGISTRY_FILE, Ulid::new()));
    let result = (|| -> Result<(), String> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)
            .map_err(|error| io_context(error, "create temporary root identity", &temp))?;
        file.write_all(&bytes)
            .map_err(|error| io_context(error, "write temporary root identity", &temp))?;
        file.sync_all()
            .map_err(|error| io_context(error, "sync temporary root identity", &temp))?;
        fs::rename(&temp, path)
            .map_err(|error| io_context(error, "publish root identity", path))?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn load_registry_from(path: &Path) -> Result<RootRegistry, String> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(RootRegistry::default()),
        Err(error) => return Err(io_context(error, "read root identity registry", path)),
    };
    let registry = serde_json::from_slice::<RootRegistry>(&bytes).map_err(|error| {
        format!(
            "decode root identity registry '{}': {error}",
            path.display()
        )
    })?;
    if registry.engine_format != ROOT_IDENTITY_FORMAT {
        return Err(format!(
            "unsupported N1 root identity registry format '{}' in '{}'",
            registry.engine_format,
            path.display()
        ));
    }
    Ok(registry)
}

fn read_marker(root: &Path) -> Result<Option<RootMarker>, String> {
    let path = root.join(ROOT_MARKER_FILE);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_context(error, "read root identity marker", &path)),
    };
    let marker = serde_json::from_slice::<RootMarker>(&bytes)
        .map_err(|error| format!("decode root identity marker '{}': {error}", path.display()))?;
    if marker.engine_format != ROOT_IDENTITY_FORMAT || marker.root_id.is_empty() {
        return Err(format!(
            "invalid N1 root identity marker '{}'",
            path.display()
        ));
    }
    Ok(Some(marker))
}

fn create_marker(root: &Path) -> Result<RootMarker, String> {
    let marker = RootMarker {
        engine_format: ROOT_IDENTITY_FORMAT.to_string(),
        root_id: Ulid::new().to_string(),
    };
    let path = root.join(ROOT_MARKER_FILE);
    let bytes = serde_json::to_vec_pretty(&marker)
        .map_err(|error| format!("encode root identity marker '{}': {error}", path.display()))?;
    match OpenOptions::new().create_new(true).write(true).open(&path) {
        Ok(mut file) => {
            file.write_all(&bytes)
                .map_err(|error| io_context(error, "write root identity marker", &path))?;
            file.sync_all()
                .map_err(|error| io_context(error, "sync root identity marker", &path))?;
            sync_parent(&path)?;
            Ok(marker)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => read_marker(root)?
            .ok_or_else(|| format!("root identity marker '{}' disappeared", path.display())),
        Err(error) => Err(io_context(error, "create root identity marker", &path)),
    }
}

fn reconcile_roots(registry_path: &Path, roots: &[PathBuf]) -> Result<RootRegistry, String> {
    let mut registry = load_registry_from(registry_path)?;
    let mut changed = false;
    let configured_keys = roots
        .iter()
        .map(|root| canonical_root_key(root))
        .collect::<BTreeSet<_>>();
    let before = registry.roots.len();
    registry
        .roots
        .retain(|path, _| configured_keys.contains(path));
    changed |= registry.roots.len() != before;
    for root in roots {
        let key = canonical_root_key(root);
        if registry.roots.contains_key(&key) {
            continue;
        }
        let marker = match read_marker(root)? {
            Some(marker) => marker,
            None => create_marker(root)?,
        };
        registry.roots.insert(key, marker.root_id);
        changed = true;
    }
    if changed || !registry_path.exists() {
        write_json_atomic(registry_path, &registry)?;
    }
    Ok(registry)
}

fn configured_roots() -> Vec<PathBuf> {
    let mut roots = BTreeSet::new();
    for root in INDEX_STORE_DISKS
        .get()
        .into_iter()
        .flatten()
        .chain(BLOB_STORE_DISKS.get().into_iter().flatten())
    {
        roots.insert(PathBuf::from(root));
    }
    roots.into_iter().collect()
}

/// Enrolls only roots that are new to the configured topology. Existing entries
/// are never rebound automatically, so a missing mount cannot become a new disk.
pub fn initialize_configured_root_identities() -> Result<(), String> {
    let path = registry_path()?;
    let registry = reconcile_roots(&path, &configured_roots())?;
    EXPECTED_ROOT_IDENTITIES
        .set(registry.roots)
        .map_err(|_| "N1 root identity authority was initialized twice".to_string())
}

pub fn expected_root_identities() -> Result<BTreeMap<String, String>, String> {
    if let Some(expected) = EXPECTED_ROOT_IDENTITIES.get() {
        return Ok(expected.clone());
    }
    Ok(load_registry_from(&registry_path()?)?.roots)
}

pub fn root_identity_enforcement_initialized() -> bool {
    EXPECTED_ROOT_IDENTITIES.get().is_some()
}

pub fn root_matches_expected_identity(root: &Path, expected: &BTreeMap<String, String>) -> bool {
    let key = canonical_root_key(root);
    let Some(expected_id) = expected.get(&key) else {
        return false;
    };
    matches!(read_marker(root), Ok(Some(marker)) if marker.root_id == *expected_id)
}

pub fn configured_root_identity_is_current(root: &Path) -> bool {
    expected_root_identities()
        .map(|expected| root_matches_expected_identity(root, &expected))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("n1-root-identity-{label}-{}", Ulid::new()))
    }

    #[test]
    fn missing_or_replaced_root_never_rebinds_automatically() {
        let control = test_root("control");
        let root = test_root("disk");
        fs::create_dir_all(&control).expect("create control root");
        fs::create_dir_all(&root).expect("create disk root");
        let registry_path = control.join("registry.json");

        let registry = reconcile_roots(&registry_path, std::slice::from_ref(&root))
            .expect("enroll original root");
        assert!(root_matches_expected_identity(&root, &registry.roots));

        fs::remove_file(root.join(ROOT_MARKER_FILE)).expect("simulate absent or replaced disk");
        let unchanged = reconcile_roots(&registry_path, std::slice::from_ref(&root))
            .expect("load existing topology");
        assert!(!root_matches_expected_identity(&root, &unchanged.roots));
        assert!(
            !root.join(ROOT_MARKER_FILE).exists(),
            "known roots must never receive a replacement identity automatically"
        );

        let _ = fs::remove_dir_all(control);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn new_root_enrollment_preserves_existing_root_identity() {
        let control = test_root("control-expand");
        let first = test_root("disk-first");
        let added = test_root("disk-added");
        fs::create_dir_all(&control).expect("create control root");
        fs::create_dir_all(&first).expect("create first disk root");
        fs::create_dir_all(&added).expect("create added disk root");
        let registry_path = control.join("registry.json");

        let initial = reconcile_roots(&registry_path, std::slice::from_ref(&first))
            .expect("enroll initial root");
        let first_id = initial.roots[&canonical_root_key(&first)].clone();
        let expanded = reconcile_roots(&registry_path, &[first.clone(), added.clone()])
            .expect("enroll added root");

        assert_eq!(expanded.roots[&canonical_root_key(&first)], first_id);
        assert!(root_matches_expected_identity(&added, &expanded.roots));

        let _ = fs::remove_dir_all(control);
        let _ = fs::remove_dir_all(first);
        let _ = fs::remove_dir_all(added);
    }

    #[test]
    fn explicit_config_removal_allows_controlled_disk_replacement() {
        let control = test_root("control-replace");
        let root = test_root("disk-replace");
        fs::create_dir_all(&control).expect("create control root");
        fs::create_dir_all(&root).expect("create disk root");
        let registry_path = control.join("registry.json");

        let initial = reconcile_roots(&registry_path, std::slice::from_ref(&root))
            .expect("enroll initial root");
        let old_id = initial.roots[&canonical_root_key(&root)].clone();

        let decommissioned = reconcile_roots(&registry_path, &[]).expect("decommission root");
        assert!(decommissioned.roots.is_empty());
        fs::remove_file(root.join(ROOT_MARKER_FILE)).expect("replace physical disk");

        let replacement = reconcile_roots(&registry_path, std::slice::from_ref(&root))
            .expect("enroll replacement root");
        assert_ne!(replacement.roots[&canonical_root_key(&root)], old_id);
        assert!(root_matches_expected_identity(&root, &replacement.roots));

        let _ = fs::remove_dir_all(control);
        let _ = fs::remove_dir_all(root);
    }
}
