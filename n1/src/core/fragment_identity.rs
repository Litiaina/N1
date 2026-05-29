use crate::enums::app_error_enum::AppError;
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs as unix_fs;
use std::path::{Path, PathBuf};
use tokio::fs as tokio_fs;
use ulid::Ulid;

pub const FRAGMENT_ALIAS_DIR: &str = ".fragment";
pub const PHYSICAL_FRAGMENT_PREFIX: &str = "@f_";

pub fn new_stable_fragment_id() -> String {
    Ulid::new().to_string()
}

pub fn physical_fragment_dir_name(stable_fragment_id: &str) -> String {
    if stable_fragment_id.starts_with(PHYSICAL_FRAGMENT_PREFIX) {
        stable_fragment_id.to_string()
    } else {
        format!("{PHYSICAL_FRAGMENT_PREFIX}{stable_fragment_id}")
    }
}

pub fn physical_fragment_id_from_dir_name(name: &str) -> Option<String> {
    name.strip_prefix(PHYSICAL_FRAGMENT_PREFIX)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub fn fragment_alias_dir(root: &Path) -> PathBuf {
    root.join(FRAGMENT_ALIAS_DIR)
}

pub fn fragment_alias_path(root: &Path, fragment_hash: &str) -> PathBuf {
    fragment_alias_dir(root).join(fragment_hash)
}

pub fn physical_fragment_root_on(root: &Path, stable_fragment_id: &str) -> PathBuf {
    root.join(physical_fragment_dir_name(stable_fragment_id))
}

fn symlink_target_dir_name(link_path: &Path) -> Option<String> {
    let target = fs::read_link(link_path).ok()?;
    target
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| name.starts_with(PHYSICAL_FRAGMENT_PREFIX))
        .map(ToOwned::to_owned)
}

pub fn resolve_physical_fragment_dir_name(root: &Path, fragment_hash: &str) -> String {
    if fragment_hash.starts_with(PHYSICAL_FRAGMENT_PREFIX) {
        return fragment_hash.to_string();
    }

    if let Some(target) = symlink_target_dir_name(&fragment_alias_path(root, fragment_hash)) {
        return target;
    }

    let physical_from_hash = physical_fragment_dir_name(fragment_hash);
    if root.join(&physical_from_hash).exists() {
        return physical_from_hash;
    }

    physical_from_hash
}

pub fn fragment_root_on(root: &Path, fragment_hash: &str) -> PathBuf {
    root.join(resolve_physical_fragment_dir_name(root, fragment_hash))
}

pub fn stable_fragment_id_for_root(root: &Path, fragment_hash: &str) -> Option<String> {
    let dir = resolve_physical_fragment_dir_name(root, fragment_hash);
    physical_fragment_id_from_dir_name(&dir)
}

pub fn list_fragment_aliases(root: &Path) -> Vec<String> {
    let alias_dir = fragment_alias_dir(root);
    let Ok(entries) = fs::read_dir(alias_dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.filter_map(|entry| entry.ok()) {
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if symlink_target_dir_name(&entry.path()).is_some() {
            out.push(name);
        }
    }
    out
}

pub async fn ensure_fragment_identity_on_root(
    root: &Path,
    fragment_hash: &str,
    stable_fragment_id: &str,
) -> Result<PathBuf, AppError> {
    let alias_dir = fragment_alias_dir(root);
    let physical_root = physical_fragment_root_on(root, stable_fragment_id);
    tokio_fs::create_dir_all(&physical_root)
        .await
        .map_err(|e| AppError::Io(e, format!("create {}", physical_root.display())))?;
    tokio_fs::create_dir_all(&alias_dir)
        .await
        .map_err(|e| AppError::Io(e, format!("create {}", alias_dir.display())))?;

    let alias_path = alias_dir.join(fragment_hash);
    match tokio_fs::symlink_metadata(&alias_path).await {
        Ok(_) => {
            if let Some(target) = symlink_target_dir_name(&alias_path) {
                if target == physical_fragment_dir_name(stable_fragment_id) {
                    return Ok(physical_root);
                }
            }
            return Err(AppError::Conflict(format!(
                "fragment alias {} already points to another fragment root",
                alias_path.display()
            )));
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => {
            return Err(AppError::Io(
                e,
                format!("inspect fragment alias {}", alias_path.display()),
            ));
        }
    }

    let target = PathBuf::from("..").join(physical_fragment_dir_name(stable_fragment_id));
    unix_fs::symlink(&target, &alias_path).map_err(|e| {
        AppError::Io(
            e,
            format!(
                "create fragment alias {} -> {}",
                alias_path.display(),
                target.display()
            ),
        )
    })?;

    Ok(physical_root)
}

pub async fn ensure_fragment_identity_on_roots(
    roots: &[PathBuf],
    fragment_hash: &str,
    stable_fragment_id: &str,
) -> Result<(), AppError> {
    for root in roots {
        ensure_fragment_identity_on_root(root, fragment_hash, stable_fragment_id).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fragment_alias_changes_without_moving_physical_root() {
        let root = std::env::temp_dir().join(format!("n1-fragment-identity-{}", Ulid::new()));
        let stable = new_stable_fragment_id();
        let old_alias = "old-fragment-hash";
        let new_alias = "new-fragment-hash";

        ensure_fragment_identity_on_root(&root, old_alias, &stable)
            .await
            .expect("create old alias");
        let physical_root = physical_fragment_root_on(&root, &stable);
        tokio_fs::write(physical_root.join("marker"), b"stable")
            .await
            .expect("write marker");

        ensure_fragment_identity_on_root(&root, new_alias, &stable)
            .await
            .expect("create new alias");
        tokio_fs::remove_file(fragment_alias_path(&root, old_alias))
            .await
            .expect("remove old alias");

        assert_eq!(fragment_root_on(&root, new_alias), physical_root);
        assert_eq!(
            tokio_fs::read(physical_root.join("marker"))
                .await
                .expect("read marker"),
            b"stable"
        );
        assert!(!fragment_alias_path(&root, old_alias).exists());

        let _ = tokio_fs::remove_dir_all(root).await;
    }
}
