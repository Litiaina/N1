use crate::core::disk_manager::ObjectPathManager;
use crate::enums::app_error_enum::AppError;
use crate::namespace::dentry::{
    DentryKey, DentryTarget, DirectoryLayout, NamespaceDentry, NamespaceDentryStore, TrashContext,
    new_directory_id, root_directory_id, trash_directory_id,
};
use crate::namespace::paths::{base_name, is_same_or_descendant, normalize_path, parent_path};
use std::sync::OnceLock;

static GLOBAL_NAMESPACE_FACADE: OnceLock<NamespaceFacade> = OnceLock::new();

pub fn global_namespace_facade() -> Result<&'static NamespaceFacade, AppError> {
    if let Some(facade) = GLOBAL_NAMESPACE_FACADE.get() {
        return Ok(facade);
    }
    let facade = NamespaceFacade::new(NamespaceDentryStore::new(
        ObjectPathManager::new().get_storage_roots(),
    )?);
    let _ = GLOBAL_NAMESPACE_FACADE.set(facade);
    GLOBAL_NAMESPACE_FACADE
        .get()
        .ok_or_else(|| AppError::InternalServerError("initialize N2 namespace facade".into()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedNamespaceEntry {
    pub path: String,
    pub parent_directory_id: String,
    pub parent_layout: DirectoryLayout,
    pub record: NamespaceDentry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDirectory {
    pub path: String,
    pub directory_id: String,
    pub layout: DirectoryLayout,
}

#[derive(Clone)]
pub struct NamespaceFacade {
    store: NamespaceDentryStore,
}

impl NamespaceFacade {
    pub fn new(store: NamespaceDentryStore) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &NamespaceDentryStore {
        &self.store
    }

    pub fn resolve_directory(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<ResolvedDirectory, AppError> {
        let path = normalize_user_path(path)?;
        let mut directory = ResolvedDirectory {
            path: String::new(),
            directory_id: root_directory_id(fragment),
            layout: DirectoryLayout::default(),
        };
        if path.is_empty() {
            return Ok(directory);
        }
        for segment in path.split('/') {
            let key = DentryKey {
                fragment: fragment.to_string(),
                parent_directory_id: directory.directory_id.clone(),
                name: segment.to_string(),
            };
            let entry = self
                .store
                .get(&key, directory.layout)?
                .ok_or(AppError::NotFound)?;
            match entry.target {
                DentryTarget::Directory {
                    directory_id,
                    layout,
                } => {
                    directory = ResolvedDirectory {
                        path: join_path(&directory.path, segment),
                        directory_id,
                        layout,
                    };
                }
                DentryTarget::Object { .. } => {
                    return Err(AppError::BadRequest(format!(
                        "namespace path '{}' is a file, not a directory",
                        join_path(&directory.path, segment)
                    )));
                }
            }
        }
        Ok(directory)
    }

    pub fn resolve_entry(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<ResolvedNamespaceEntry>, AppError> {
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Ok(None);
        }
        let parent_path = parent_path(&path);
        let parent = self.resolve_directory(fragment, &parent_path)?;
        let key = DentryKey {
            fragment: fragment.to_string(),
            parent_directory_id: parent.directory_id.clone(),
            name: base_name(&path),
        };
        Ok(self
            .store
            .get(&key, parent.layout)?
            .map(|record| ResolvedNamespaceEntry {
                path,
                parent_directory_id: parent.directory_id,
                parent_layout: parent.layout,
                record,
            }))
    }

    pub fn list_directory(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Vec<NamespaceDentry>, AppError> {
        let directory = self.resolve_directory(fragment, path)?;
        self.store
            .list_parent(fragment, &directory.directory_id, directory.layout)
    }

    pub fn list_trash(&self, fragment: &str) -> Result<Vec<NamespaceDentry>, AppError> {
        self.store.list_parent(
            fragment,
            &trash_directory_id(fragment),
            DirectoryLayout::default(),
        )
    }

    pub fn mkdir_all(
        &self,
        fragment: &str,
        path: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<ResolvedDirectory, AppError> {
        let path = normalize_user_path(path)?;
        let mut directory = ResolvedDirectory {
            path: String::new(),
            directory_id: root_directory_id(fragment),
            layout: DirectoryLayout::default(),
        };
        if path.is_empty() {
            return Ok(directory);
        }
        for (index, segment) in path.split('/').enumerate() {
            let key = DentryKey {
                fragment: fragment.to_string(),
                parent_directory_id: directory.directory_id.clone(),
                name: segment.to_string(),
            };
            let entry = match self.store.get(&key, directory.layout)? {
                Some(entry) => entry,
                None => self.store.create(
                    key,
                    directory.layout,
                    DentryTarget::Directory {
                        directory_id: new_directory_id(),
                        layout: DirectoryLayout::default(),
                    },
                    &format!("{mutation_id}-{index}"),
                    now_unix,
                )?,
            };
            match entry.target {
                DentryTarget::Directory {
                    directory_id,
                    layout,
                } => {
                    directory = ResolvedDirectory {
                        path: join_path(&directory.path, segment),
                        directory_id,
                        layout,
                    };
                }
                DentryTarget::Object { .. } => {
                    return Err(AppError::Conflict(format!(
                        "cannot create directory through file '{}'",
                        join_path(&directory.path, segment)
                    )));
                }
            }
        }
        Ok(directory)
    }

    pub fn publish_object(
        &self,
        fragment: &str,
        path: &str,
        object_id: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<NamespaceDentry, AppError> {
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Err(AppError::BadRequest(
                "root cannot reference an object".into(),
            ));
        }
        let parent_path = parent_path(&path);
        let parent = self.mkdir_all(
            fragment,
            &parent_path,
            &format!("{mutation_id}-parents"),
            now_unix,
        )?;
        self.store.create(
            DentryKey {
                fragment: fragment.to_string(),
                parent_directory_id: parent.directory_id,
                name: base_name(&path),
            },
            parent.layout,
            DentryTarget::Object {
                object_id: object_id.to_string(),
            },
            mutation_id,
            now_unix,
        )
    }

    pub fn rename(
        &self,
        fragment: &str,
        source_path: &str,
        destination_path: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<NamespaceDentry, AppError> {
        let source_path = normalize_user_path(source_path)?;
        let destination_path = normalize_user_path(destination_path)?;
        if source_path.is_empty() || destination_path.is_empty() {
            return Err(AppError::BadRequest("root cannot be renamed".into()));
        }
        if is_same_or_descendant(&destination_path, &source_path) {
            return Err(AppError::Conflict(
                "destination cannot be inside the source subtree".into(),
            ));
        }
        let source = self
            .resolve_entry(fragment, &source_path)?
            .ok_or(AppError::NotFound)?;
        let destination_parent =
            self.resolve_directory(fragment, &parent_path(&destination_path))?;
        self.store.rename(
            source.record.key.clone(),
            source.parent_layout,
            DentryKey {
                fragment: fragment.to_string(),
                parent_directory_id: destination_parent.directory_id,
                name: base_name(&destination_path),
            },
            destination_parent.layout,
            source.record.generation,
            mutation_id,
            now_unix,
            None,
        )
    }

    pub fn trash(
        &self,
        fragment: &str,
        path: &str,
        trash_entry_id: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<NamespaceDentry, AppError> {
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Err(AppError::BadRequest("root cannot be trashed".into()));
        }
        let source = self
            .resolve_entry(fragment, &path)?
            .ok_or(AppError::NotFound)?;
        self.store.rename(
            source.record.key.clone(),
            source.parent_layout,
            DentryKey {
                fragment: fragment.to_string(),
                parent_directory_id: trash_directory_id(fragment),
                name: trash_entry_id.to_string(),
            },
            DirectoryLayout::default(),
            source.record.generation,
            mutation_id,
            now_unix,
            Some(TrashContext {
                original_parent_directory_id: source.parent_directory_id,
                original_parent_layout: source.parent_layout,
                original_name: source.record.key.name,
                original_path: path,
                deleted_at_unix: now_unix,
            }),
        )
    }

    pub fn restore(
        &self,
        fragment: &str,
        trash_entry_id: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<NamespaceDentry, AppError> {
        let source_key = DentryKey {
            fragment: fragment.to_string(),
            parent_directory_id: trash_directory_id(fragment),
            name: trash_entry_id.to_string(),
        };
        let source = self
            .store
            .get(&source_key, DirectoryLayout::default())?
            .ok_or(AppError::NotFound)?;
        let trash = source.trash.clone().ok_or_else(|| {
            AppError::InternalServerError("trash entry has no restore context".into())
        })?;
        self.store.rename(
            source_key,
            DirectoryLayout::default(),
            DentryKey {
                fragment: fragment.to_string(),
                parent_directory_id: trash.original_parent_directory_id,
                name: trash.original_name,
            },
            trash.original_parent_layout,
            source.generation,
            mutation_id,
            now_unix,
            None,
        )
    }

    pub fn purge_expired(
        &self,
        fragment: &str,
        trash_entry_id: &str,
        ttl_seconds: u64,
        now_unix: i64,
        mutation_id: &str,
    ) -> Result<DentryTarget, AppError> {
        let key = DentryKey {
            fragment: fragment.to_string(),
            parent_directory_id: trash_directory_id(fragment),
            name: trash_entry_id.to_string(),
        };
        let entry = self
            .store
            .get(&key, DirectoryLayout::default())?
            .ok_or(AppError::NotFound)?;
        let trash = entry.trash.as_ref().ok_or_else(|| {
            AppError::InternalServerError("trash entry has no lifecycle context".into())
        })?;
        let expires_at = trash
            .deleted_at_unix
            .saturating_add(i64::try_from(ttl_seconds).unwrap_or(i64::MAX));
        if now_unix < expires_at {
            return Err(AppError::Conflict(format!(
                "trash entry is retained until unix timestamp {expires_at}"
            )));
        }
        let removed = self.store.remove(
            &key,
            DirectoryLayout::default(),
            entry.generation,
            mutation_id,
        )?;
        Ok(removed.target)
    }
}

fn normalize_user_path(path: &str) -> Result<String, AppError> {
    let path = normalize_path(path).map_err(|err| AppError::BadRequest(err.to_string()))?;
    if path.split('/').any(|segment| segment == ".trash") {
        return Err(AppError::BadRequest(
            "'.trash' is reserved for N2 lifecycle management".into(),
        ));
    }
    Ok(path)
}

fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn facade(label: &str) -> (NamespaceFacade, Vec<PathBuf>) {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let roots = (0..2)
            .map(|index| {
                std::env::temp_dir().join(format!(
                    "n2-facade-{label}-{}-{stamp}-{index}",
                    std::process::id()
                ))
            })
            .collect::<Vec<_>>();
        let store = NamespaceDentryStore::new(roots.clone()).expect("store");
        (NamespaceFacade::new(store), roots)
    }

    fn cleanup(roots: Vec<PathBuf>) {
        for root in roots {
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn nested_resolution_uses_stable_directory_ids() {
        let (facade, roots) = facade("resolve");
        facade
            .publish_object("fragment", "a/b/file", "object-1", "upload", 1)
            .expect("publish");
        let before = facade
            .resolve_directory("fragment", "a/b")
            .expect("resolve");
        facade
            .rename("fragment", "a", "renamed", "rename", 2)
            .expect("rename");
        let after = facade
            .resolve_directory("fragment", "renamed/b")
            .expect("resolve renamed");
        assert_eq!(before.directory_id, after.directory_id);
        assert_eq!(
            facade
                .resolve_entry("fragment", "renamed/b/file")
                .expect("entry")
                .expect("present")
                .record
                .target,
            DentryTarget::Object {
                object_id: "object-1".into()
            }
        );
        assert!(facade.resolve_entry("fragment", "a/b/file").is_err());
        cleanup(roots);
    }

    #[test]
    fn trash_has_no_manual_early_purge() {
        let (facade, roots) = facade("ttl");
        facade
            .publish_object("fragment", "movie", "object-1", "upload", 1)
            .expect("publish");
        facade
            .trash("fragment", "movie", "trash-1", "trash", 10)
            .expect("trash");
        assert!(
            facade
                .resolve_entry("fragment", "movie")
                .expect("resolve")
                .is_none()
        );
        assert!(matches!(
            facade.purge_expired("fragment", "trash-1", 100, 109, "gc-early"),
            Err(AppError::Conflict(_))
        ));
        let target = facade
            .purge_expired("fragment", "trash-1", 100, 110, "gc")
            .expect("expired purge");
        assert_eq!(
            target,
            DentryTarget::Object {
                object_id: "object-1".into()
            }
        );
        assert!(
            facade
                .list_trash("fragment")
                .expect("trash list")
                .is_empty()
        );
        cleanup(roots);
    }

    #[test]
    fn restore_reverses_only_the_namespace_move() {
        let (facade, roots) = facade("restore");
        facade
            .publish_object("fragment", "docs/file", "object-1", "upload", 1)
            .expect("publish");
        facade
            .trash("fragment", "docs/file", "trash-1", "trash", 2)
            .expect("trash");
        let restored = facade
            .restore("fragment", "trash-1", "restore", 3)
            .expect("restore");
        assert_eq!(
            restored.target,
            DentryTarget::Object {
                object_id: "object-1".into()
            }
        );
        assert!(
            facade
                .list_trash("fragment")
                .expect("trash list")
                .is_empty()
        );
        cleanup(roots);
    }
}
