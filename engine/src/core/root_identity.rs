use crate::core::disk_manager::FS_STORAGE_ROOT;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
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

pub(crate) fn member_id_on(root: &Path) -> Result<Option<String>, String> {
    Ok(read_marker(root)?.map(|marker| marker.root_id))
}

pub(crate) fn registered_roots(control: &Path) -> Result<BTreeMap<String, String>, String> {
    Ok(load_registry_from(&control.join(REGISTRY_DIR).join(REGISTRY_FILE))?.roots)
}

#[cfg(test)]
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

#[cfg(test)]
pub(crate) fn reconcile_roots_for_test(
    registry_path: &Path,
    roots: &[PathBuf],
) -> Result<(), String> {
    reconcile_roots(registry_path, roots).map(|_| ())
}

#[cfg(test)]
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

/// Enrolls only roots that are new to the configured topology. Existing entries
/// are never rebound automatically, so a missing mount cannot become a new disk.
pub fn initialize_configured_root_identities() -> Result<(), String> {
    let path = registry_path()?;
    let mut registry = RootRegistry::default();
    let catalog: Vec<crate::core::storage_pool::StoragePool> =
        match fs::read(Path::new(FS_STORAGE_ROOT.get().unwrap()).join(".n1/storage-pools.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| e.to_string())?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.to_string()),
        };
    for pool in crate::core::storage_pool::configured() {
        for (slot, root) in pool.roots.iter().enumerate() {
            let expected = pool.members.get(slot).ok_or("unbound member identity")?;
            registry
                .roots
                .insert(canonical_root_key(root), expected.clone());
            if !root.is_dir()
                || root
                    .join(crate::core::instance_lease::RESTORE_PENDING)
                    .exists()
            {
                continue;
            }
            match read_marker(root)? {
                Some(marker) if marker.root_id != *expected => {
                    return Err("mounted member carries a different root identity".into());
                }
                Some(_) => {}
                None => {
                    let identified = crate::core::storage_pool::read_member(root)?
                        .is_some_and(|d| d.member_id == *expected);
                    if identified || !catalog.iter().any(|old| old.id == pool.id) {
                        write_json_atomic(
                            &root.join(ROOT_MARKER_FILE),
                            &RootMarker {
                                engine_format: ROOT_IDENTITY_FORMAT.into(),
                                root_id: expected.clone(),
                            },
                        )?;
                    }
                }
            }
        }
    }
    if load_registry_from(&path)?.roots != registry.roots || !path.exists() {
        write_json_atomic(&path, &registry)?;
    }
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
    if let Some(expected) = EXPECTED_ROOT_IDENTITIES.get() {
        return root_matches_expected_identity(root, expected);
    }
    expected_root_identities()
        .map(|expected| root_matches_expected_identity(root, &expected))
        .unwrap_or(false)
}

/// Offline maintenance retains the enrolled logical identity. Call only while
/// holding the instance lease and a durable startup fence, after verification.
pub(crate) fn registered_identity(control: &Path, root: &Path) -> Result<String, String> {
    if let Some(descriptor) = crate::core::storage_pool::read_member(root)? {
        let pools: Vec<crate::core::storage_pool::StoragePool> = serde_json::from_slice(
            &fs::read(control.join(".n1/storage-pools.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        if pools.iter().any(|pool| {
            pool.same_geometry(&descriptor.pool)
                && pool.members.get(descriptor.member_slot) == Some(&descriptor.member_id)
        }) {
            return Ok(descriptor.member_id);
        }
    }
    let registry = load_registry_from(&control.join(REGISTRY_DIR).join(REGISTRY_FILE))?;
    registry
        .roots
        .get(&canonical_root_key(root))
        .cloned()
        .ok_or_else(|| format!("root '{}' is not enrolled", root.display()))
}

pub(crate) fn verify_registered_identity(control: &Path, root: &Path) -> Result<(), String> {
    let expected = registered_identity(control, root)?;
    match read_marker(root)? {
        Some(marker) if marker.root_id == expected => Ok(()),
        _ => Err(format!(
            "root '{}' does not match its enrolled identity",
            root.display()
        )),
    }
}

pub(crate) fn restore_verified_marker(control: &Path, root: &Path) -> Result<(), String> {
    let expected = registered_identity(control, root)?;
    if let Some(marker) = read_marker(root)? {
        if marker.root_id != expected {
            return Err("replacement has an unrelated root identity".into());
        }
        return Ok(());
    }
    // Keep staging inside the explicitly fenced maintenance workspace so a
    // process kill cannot leave an unexplained identity temporary at the root.
    let staged = root.join(".n1-pool-work").join("verified-root.json");
    write_json_atomic(
        &staged,
        &RootMarker {
            engine_format: ROOT_IDENTITY_FORMAT.into(),
            root_id: expected,
        },
    )?;
    fs::rename(&staged, root.join(ROOT_MARKER_FILE))
        .map_err(|error| io_context(error, "activate verified root identity", root))?;
    sync_parent(&root.join(ROOT_MARKER_FILE))
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
