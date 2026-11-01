//! Unfinished payload belongs to an upload, never to the committed object tree.
//! Metadata always records the final address. Promotion moves directories on
//! each member before publishing any NDX; it never copies or edits shard bytes.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::core::disk_manager::ObjectMetadata;
use crate::core::{fragment_identity, index_store, linux_io, storage_pool};
use crate::enums::app_error_enum::AppError;

fn io(error: std::io::Error, action: &str) -> AppError {
    AppError::Io(error, action.into())
}

pub(crate) fn owner_dir(root: &Path, fragment: &str, version: &str) -> PathBuf {
    fragment_identity::physical_fragment_root_on(root, fragment)
        .join(index_store::UPLOADS_DIR)
        .join(version)
}

pub(crate) fn payload_dir(root: &Path, fragment: &str, object: &str, version: &str) -> PathBuf {
    private_dir(root, fragment, object, version).join("payload")
}

pub(crate) fn private_dir(root: &Path, fragment: &str, object: &str, version: &str) -> PathBuf {
    owner_dir(root, fragment, version).join(format!("@o_{object}"))
}

/// Check organizer components without following a redirected internal tree.
/// Durability belongs to the caller's existing file/group publication boundary.
pub(crate) fn parents(root: &Path, path: &Path, create: bool) -> Result<(), AppError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| AppError::Conflict("upload path escaped its storage member".into()))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(AppError::Conflict("invalid upload path component".into()));
        }
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() => (),
            Err(error) if create && error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => (),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        if !fs::symlink_metadata(&current)
                            .map_err(|e| io(e, "inspect upload parent"))?
                            .is_dir()
                        {
                            return Err(AppError::Conflict("redirected upload parent".into()));
                        }
                    }
                    Err(error) => return Err(io(error, "create upload parent")),
                }
            }
            Ok(_) => return Err(AppError::Conflict("redirected upload parent".into())),
            Err(error) => return Err(io(error, "inspect upload parent")),
        }
    }
    Ok(())
}

/// Derive only the actual K+P placements, using immutable final shard addresses.
pub(crate) fn placements(
    metadata: &ObjectMetadata,
) -> Result<BTreeMap<PathBuf, PathBuf>, AppError> {
    let mut placements = BTreeMap::new();
    for shard in metadata.parts.iter().flat_map(|part| &part.shards) {
        let path = storage_pool::resolve_address(&shard.absolute_path)?;
        let destination = path
            .parent()
            .ok_or_else(|| AppError::Conflict("invalid shard address".into()))?;
        let root = destination
            .ancestors()
            .nth(6)
            .ok_or_else(|| AppError::Conflict("invalid shard member address".into()))?;
        let object = index_store::object_index_dir(root, &metadata.fragment, &metadata.object_key)?;
        if destination != object.join(&metadata.version_id)
            || path.file_name().and_then(|n| n.to_str()) != Some(shard.file_name.as_str())
        {
            return Err(AppError::Conflict(
                "shard address does not match immutable version".into(),
            ));
        }
        placements.insert(root.to_path_buf(), destination.to_path_buf());
    }
    Ok(placements)
}

pub(crate) fn promote(metadata: &ObjectMetadata, strict: bool) -> Result<(), AppError> {
    let placements = placements(metadata)?;
    // Existing bounded holder executor joins every member, including failures.
    let roots = placements.keys().cloned().collect::<Vec<_>>();
    crate::namespace::flat::on_holders(&roots, |root| {
        let source = payload_dir(
            root,
            &metadata.fragment,
            &metadata.object_key,
            &metadata.version_id,
        );
        let source_present = match fs::symlink_metadata(&source) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !private_dir(
                    root,
                    &metadata.fragment,
                    &metadata.object_key,
                    &metadata.version_id,
                )
                .is_dir()
                {
                    return Ok(());
                }
                false
            }
            Err(error) => return Err(io(error, "inspect staged upload")),
            Ok(metadata) if metadata.is_dir() => true,
            Ok(_) => return Err(AppError::Conflict("redirected staged payload".into())),
        };
        if !storage_pool::root_available(root) {
            return Err(AppError::ServiceUnavailable(
                "upload member is unavailable".into(),
            ));
        }
        parents(root, source.parent().unwrap(), false)?;
        let destination = &placements[root];
        let parent = destination.parent().expect("validated object directory");
        parents(root, parent, true)?;
        // NOREPLACE prevents a retry from replacing an immutable version. A
        // partial promotion can be retried: already moved members have no source.
        if source_present {
            parents(root, &source, false)?;
            rustix::fs::renameat_with(
                rustix::fs::CWD,
                &source,
                rustix::fs::CWD,
                destination,
                rustix::fs::RenameFlags::NOREPLACE,
            )
            .map_err(std::io::Error::from)
            .map_err(|e| io(e, "promote upload payload"))?;
        } else {
            parents(root, destination, false)?;
        }
        if strict {
            // File contents were synced by strict part writes. Persist the
            // cross-directory move and new destination ancestors before NDX.
            // Groups pass false and retain their three shared syncfs barriers.
            linux_io::sync_dir_blocking(source.parent().unwrap())
                .map_err(|e| io(e, "sync upload promotion source"))?;
            let fragment = fragment_identity::physical_fragment_root_on(root, &metadata.fragment);
            for directory in parent.ancestors().take_while(|p| p.starts_with(&fragment)) {
                linux_io::sync_dir_blocking(directory)
                    .map_err(|e| io(e, "sync upload promotion destination"))?;
            }
        }
        Ok(())
    })
    .map(|_| ())
}

/// Only remove an empty owner. Durable manifests/receipts remain until logical
/// NCX publication, and uncertain leftovers stay available for candidate GC.
pub(crate) fn retire_empty(metadata: &ObjectMetadata) {
    if let Ok(placements) = placements(metadata) {
        for root in placements.keys() {
            let _ = fs::remove_dir(private_dir(
                root,
                &metadata.fragment,
                &metadata.object_key,
                &metadata.version_id,
            ));
            let _ = fs::remove_dir(owner_dir(root, &metadata.fragment, &metadata.version_id));
        }
    }
}
