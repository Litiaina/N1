//! Namespace publication for one bounded small-object commit group.
//!
//! The caller holds every group key through all three filesystem barriers.
//! NCX is still an object identity, never a version head. New references are
//! prepared privately beside the group's payload, so they are invisible to
//! LIST and remain inside the ordinary uncommitted-version reclamation scope.

use super::*;
use crate::core::linux_io::group::StagedFile;

pub(crate) struct NamespacePlan {
    fragment: String,
    key: String,
    pointer: Pointer,
    roots: Vec<PathBuf>,
    existing: bool,
    repair: bool,
}

pub(crate) struct StagedNamespace {
    roots: Vec<PathBuf>,
    files: Vec<StagedFile>,
}

/// Resolve exact holders under the caller's existing sorted key guards. No
/// namespace writes occur until `stage`, after the group opens its filesystem
/// barrier handles.
pub(crate) fn plan(
    fragment: &str,
    key: &str,
    object_id: &str,
    pool: Option<&StoragePool>,
    existing: Option<Pointer>,
) -> Result<NamespacePlan, AppError> {
    if index_store::normalize_object_id(object_id)? != object_id {
        return Err(AppError::BadRequest("invalid batch object identity".into()));
    }
    let fragment = index_store::stable_fragment_id_for_fragment(fragment);
    let was_existing = existing.is_some();
    let pointer = match existing {
        Some(pointer) if pointer.object_id != object_id => {
            return Err(AppError::Conflict(format!(
                "key '{key}' has another object identity"
            )));
        }
        Some(pointer) => pointer,
        None => Pointer {
            format: 4,
            pool_id: pool.map(|pool| pool.id.clone()).unwrap_or_default(),
            object_id: object_id.into(),
        },
    };
    let roots = holders(&pointer, &fragment, key)?;
    check_holders(&roots)?;
    let mut repair = false;
    if was_existing {
        for root in &roots {
            match read(&path(root, &fragment, key)) {
                Ok(Some(current)) if current == pointer => (),
                Ok(Some(_)) => {
                    return Err(AppError::Conflict(format!(
                        "key '{key}' has another object identity"
                    )));
                }
                Ok(None) | Err(_) => repair = true,
            }
        }
    }
    Ok(NamespacePlan {
        fragment,
        key: key.into(),
        pointer,
        roots,
        existing: was_existing,
        repair,
    })
}

/// Check every organizer component without following a redirected directory.
/// Group syncfs barriers, not per-object directory fsync, persist creation.
fn parents(parent: &Path, root: &Path, create: bool) -> Result<(), AppError> {
    if !storage_pool::root_available(root) {
        return Err(AppError::ServiceUnavailable(format!(
            "namespace holder '{}' is unavailable",
            root.display()
        )));
    }
    let relative = parent
        .strip_prefix(root)
        .map_err(|_| error("namespace path escaped storage root"))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(error("invalid namespace parent"));
        }
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() => (),
            Ok(_) => {
                return Err(AppError::Conflict(
                    "namespace parent is not a directory".into(),
                ));
            }
            Err(e) if create && e.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        if !fs::symlink_metadata(&current)
                            .map_err(|e| io(e, "inspect batch NCX parent"))?
                            .is_dir()
                        {
                            return Err(error("unsafe namespace parent"));
                        }
                    }
                    Err(e) => return Err(io(e, "create batch NCX parent")),
                }
            }
            Err(e) => return Err(io(e, "inspect batch NCX parent")),
        }
    }
    Ok(())
}

impl NamespacePlan {
    pub(crate) fn roots(&self) -> &[PathBuf] {
        &self.roots
    }

    pub(crate) fn stage(self, version: &str) -> Result<StagedNamespace, AppError> {
        check_holders(&self.roots)?;
        if self.repair {
            // The uncommon repair path retains the established strict holder
            // checks and publication semantics. It repairs only NCX copies;
            // committed NDX and payload bytes are never rewritten here.
            publish_with_existing(
                &self.fragment,
                &self.key,
                &self.pointer.object_id,
                None,
                Some(self.pointer.clone()),
            )?;
        }
        let mut staged = StagedNamespace {
            roots: self.roots,
            files: Vec::new(),
        };
        if self.existing {
            // Every proper holder already names the same immutable object.
            // A new version never requires replacing its namespace inode.
            return Ok(staged);
        }
        let bytes = encode(&self.pointer)?;
        let manager = ObjectPathManager::new();
        for root in &staged.roots {
            let target = path(root, &self.fragment, &self.key);
            let parent = target.parent().ok_or_else(|| error("NCX has no parent"))?;
            parents(parent, root, true)?;
            let upload = crate::core::upload::staging::private_dir(
                root,
                &self.fragment,
                &self.pointer.object_id,
                version,
            );
            let temporary_parent = if upload.is_dir() {
                upload
            } else {
                manager.get_shard_dir_on_disk(
                    root,
                    &self.fragment,
                    &self.pointer.object_id,
                    version,
                )
            };
            // Payload staging has already created this directory. Never make
            // an alternate payload tree merely to publish a namespace key.
            parents(&temporary_parent, root, false)?;
            staged.files.push(
                StagedFile::create(&target, &temporary_parent, &bytes)
                    .map_err(|e| io(e, "stage batch NCX"))?,
            );
        }
        Ok(staged)
    }
}

impl StagedNamespace {
    /// Called only after payload and metadata durability barriers complete.
    /// A partial publication is safe but is never a group acknowledgement.
    pub(crate) fn publish(&mut self) -> Result<(), AppError> {
        check_holders(&self.roots)?;
        for file in &mut self.files {
            file.publish().map_err(|e| io(e, "publish batch NCX"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    struct Fixture {
        fragment: String,
        object: String,
        version: String,
        roots: Vec<PathBuf>,
    }

    impl Fixture {
        fn new() -> Self {
            let configured = crate::core::disk_manager::INDEX_STORE_DISKS.get_or_init(|| {
                let base = std::env::temp_dir().join(format!("n1-index-{}", std::process::id()));
                [base.join("index-0"), base.join("index-1")]
                    .into_iter()
                    .map(|root| root.to_string_lossy().into_owned())
                    .collect()
            });
            for root in configured {
                fs::create_dir_all(root).unwrap();
            }
            let fixture = Self {
                fragment: Ulid::new().to_string(),
                object: Ulid::new().to_string(),
                version: Ulid::new().to_string(),
                roots: roots(),
            };
            for root in &fixture.roots {
                fs::create_dir_all(fixture.version_path(root)).unwrap();
            }
            fixture
        }

        fn version_path(&self, root: &PathBuf) -> PathBuf {
            ObjectPathManager::new().get_shard_dir_on_disk(
                root,
                &self.fragment,
                &self.object,
                &self.version,
            )
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            for root in &self.roots {
                let _ = fs::remove_dir_all(index_store::fragment_root_on(root, &self.fragment));
            }
        }
    }

    #[test]
    fn staged_namespace_is_private_until_publication_and_drops_unpublished_files() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let fixture = Fixture::new();
        let key = "projects/subdir/document.txt";
        with_keys(&fixture.fragment, &[key], || {
            let planned = plan(&fixture.fragment, key, &fixture.object, None, None)?;
            assert_eq!(planned.roots(), fixture.roots);
            let staged = planned.stage(&fixture.version)?;
            for root in &fixture.roots {
                assert!(!path(root, &fixture.fragment, key).exists());
                assert_eq!(fs::read_dir(fixture.version_path(root)).unwrap().count(), 1);
            }
            assert!(lookup(&fixture.fragment, key)?.is_none());
            drop(staged);
            for root in &fixture.roots {
                assert_eq!(fs::read_dir(fixture.version_path(root)).unwrap().count(), 0);
            }
            let mut staged = plan(&fixture.fragment, key, &fixture.object, None, None)?
                .stage(&fixture.version)?;
            assert!(lookup(&fixture.fragment, key)?.is_none());
            staged.publish()?;
            assert_eq!(
                lookup(&fixture.fragment, key)?.unwrap().object_id,
                fixture.object
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn existing_object_versions_preserve_every_unchanged_namespace_inode() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let fixture = Fixture::new();
        let key = "document.txt";
        with_keys(&fixture.fragment, &[key], || {
            let pointer = publish(&fixture.fragment, key, &fixture.object, None)?;
            let immutable = fixture
                .roots
                .iter()
                .flat_map(|root| {
                    let payload = fixture.version_path(root);
                    [payload.join("payload-piece"), payload.join("version.ndx")]
                })
                .map(|path| {
                    fs::write(&path, b"immutable committed bytes").unwrap();
                    let metadata = fs::metadata(&path).unwrap();
                    (path, metadata.ino(), metadata.modified().unwrap())
                })
                .collect::<Vec<_>>();
            let before = fixture
                .roots
                .iter()
                .map(|root| {
                    fs::metadata(path(root, &fixture.fragment, key))
                        .unwrap()
                        .ino()
                })
                .collect::<Vec<_>>();
            let mut staged = plan(&fixture.fragment, key, &fixture.object, None, Some(pointer))?
                .stage("another-version-does-not-need-an-NCX-directory")?;
            assert!(staged.files.is_empty());
            staged.publish()?;
            for (root, inode) in fixture.roots.iter().zip(before) {
                assert_eq!(
                    fs::metadata(path(root, &fixture.fragment, key))
                        .unwrap()
                        .ino(),
                    inode
                );
            }
            for (path, inode, modified) in immutable {
                let metadata = fs::metadata(&path).unwrap();
                assert_eq!(metadata.ino(), inode);
                assert_eq!(metadata.modified().unwrap(), modified);
                assert_eq!(fs::read(path).unwrap(), b"immutable committed bytes");
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn missing_or_corrupt_existing_references_are_repaired_without_rebinding() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let fixture = Fixture::new();
        let key = "document.txt";
        with_keys(&fixture.fragment, &[key], || {
            let pointer = publish(&fixture.fragment, key, &fixture.object, None)?;
            fs::remove_file(path(&fixture.roots[0], &fixture.fragment, key)).unwrap();
            if fixture.roots.len() > 1 {
                fs::write(path(&fixture.roots[1], &fixture.fragment, key), b"corrupt").unwrap();
            }
            let mut staged = plan(
                &fixture.fragment,
                key,
                &fixture.object,
                None,
                Some(pointer.clone()),
            )?
            .stage(&fixture.version)?;
            assert!(staged.files.is_empty());
            staged.publish()?;
            for root in &fixture.roots {
                assert_eq!(
                    read(&path(root, &fixture.fragment, key))?,
                    Some(pointer.clone())
                );
            }
            assert!(
                plan(
                    &fixture.fragment,
                    key,
                    &Ulid::new().to_string(),
                    None,
                    Some(pointer)
                )
                .is_err()
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn batch_namespace_rejects_redirected_parents_and_never_replaces_a_destination() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let fixture = Fixture::new();
        let key = "projects/document.txt";
        let outside = std::env::temp_dir().join(format!("n1-batch-outside-{}", Ulid::new()));
        fs::create_dir(&outside).unwrap();
        let namespace = namespace_root(&fixture.roots[0], &fixture.fragment);
        std::os::unix::fs::symlink(&outside, &namespace).unwrap();
        assert!(
            plan(&fixture.fragment, key, &fixture.object, None, None)
                .unwrap()
                .stage(&fixture.version)
                .is_err()
        );
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
        fs::remove_file(&namespace).unwrap();
        fs::remove_dir(&outside).unwrap();
        let mut staged = plan(&fixture.fragment, key, &fixture.object, None, None)
            .unwrap()
            .stage(&fixture.version)
            .unwrap();
        let destination = path(&fixture.roots[0], &fixture.fragment, key);
        fs::write(&destination, b"independent-existing-destination").unwrap();
        assert!(staged.publish().is_err());
        assert_eq!(
            fs::read(&destination).unwrap(),
            b"independent-existing-destination"
        );
    }

    #[test]
    fn absent_payload_directory_fails_without_creating_payload_or_namespace_files() {
        let _guard = crate::test_support::global_test_lock().blocking_lock();
        let fixture = Fixture::new();
        let key = "new-object.txt";
        let missing_version = Ulid::new().to_string();
        let missing_path = ObjectPathManager::new().get_shard_dir_on_disk(
            &fixture.roots[0],
            &fixture.fragment,
            &fixture.object,
            &missing_version,
        );
        assert!(
            plan(&fixture.fragment, key, &fixture.object, None, None)
                .unwrap()
                .stage(&missing_version)
                .is_err()
        );
        assert!(!missing_path.exists());
        for root in &fixture.roots {
            assert!(!path(root, &fixture.fragment, key).exists());
            assert_eq!(fs::read_dir(fixture.version_path(root)).unwrap().count(), 0);
        }
    }
}
