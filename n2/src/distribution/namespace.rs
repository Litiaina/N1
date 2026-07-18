use std::collections::BTreeSet;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::core::disk_manager::ObjectMetadata;
use crate::distribution::dentry_service::DistributedNamespace;
use crate::enums::app_error_enum::AppError;
use crate::namespace::inode_store::{InodeDentry, InodeDentryTarget, InodeLayout};
use crate::namespace::models::{
    LifecycleState, NamespaceEntry, NamespaceEntryKind, NamespaceMutationSummary,
};
use crate::namespace::runtime::{NamespaceObjectRef, NamespaceObjectRefPage};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerNamespaceReadKind {
    ListDir,
    Entry,
    TrashedEntry,
    ResolveLiveObject,
    ObjectRefs,
    TrashObjectRefs,
    Reachability,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerNamespaceReadRequest {
    pub fragment: String,
    pub kind: PeerNamespaceReadKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub include_trashed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerNamespaceReadResponse {
    ListDir {
        entries: Vec<NamespaceEntry>,
        continuation_token: Option<String>,
    },
    Entry(Option<NamespaceEntry>),
    ObjectId(Option<String>),
    ObjectRefs(NamespaceObjectRefPage),
    Reachability(BTreeSet<String>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceMutationKind {
    Mkdir,
    Rename,
    Trash,
    Recover,
    UploadCommit,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NamespaceApplyOutcome {
    Created(bool),
    Summary(NamespaceMutationSummary),
    CommittedObject(String),
}

impl NamespaceApplyOutcome {
    fn created(&self) -> Option<bool> {
        match self {
            Self::Created(created) => Some(*created),
            _ => None,
        }
    }

    fn summary(&self) -> Option<NamespaceMutationSummary> {
        match self {
            Self::Summary(summary) => Some(summary.clone()),
            _ => None,
        }
    }

    fn committed_object(&self) -> Option<String> {
        match self {
            Self::CommittedObject(object_id) => Some(object_id.clone()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerNamespaceApplyRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_id: Option<String>,
    pub kind: NamespaceMutationKind,
    pub fragment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMetadata>,
}

/// All namespace reads use the exact replica layout recorded by each stable
/// directory inode. Returning `Some` is deliberate: N2 2.0 has no legacy
/// local-filesystem namespace fallback.
pub async fn namespace_read_if_remote(
    request: PeerNamespaceReadRequest,
) -> Result<Option<PeerNamespaceReadResponse>, AppError> {
    let path = request.path.clone().unwrap_or_default();
    let response = match request.kind {
        PeerNamespaceReadKind::ListDir => {
            let mut entries = if request.include_trashed {
                DistributedNamespace::list_trash(&request.fragment)
                    .await?
                    .into_iter()
                    .map(|entry| trash_dentry_to_namespace_entry(&request.fragment, entry))
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                DistributedNamespace::list_directory(&request.fragment, &path)
                    .await?
                    .into_iter()
                    .map(|entry| {
                        let child_path = join_path(path.trim_matches('/'), &entry.name);
                        live_dentry_to_namespace_entry(&request.fragment, entry, child_path)
                    })
                    .collect()
            };
            entries.sort_by(|left, right| left.path.cmp(&right.path));
            if let Some(after) = request.continuation_token.as_deref() {
                entries.retain(|entry| entry.path.as_str() > after);
            }
            let limit = request.limit.unwrap_or(200).clamp(1, 1000);
            let continuation_token =
                (entries.len() > limit).then(|| entries[limit - 1].path.clone());
            entries.truncate(limit);
            PeerNamespaceReadResponse::ListDir {
                entries,
                continuation_token,
            }
        }
        PeerNamespaceReadKind::Entry => {
            let entry = DistributedNamespace::resolve_entry(&request.fragment, &path)
                .await?
                .map(|resolved| {
                    live_dentry_to_namespace_entry(
                        &request.fragment,
                        resolved.record,
                        resolved.path,
                    )
                });
            PeerNamespaceReadResponse::Entry(entry)
        }
        PeerNamespaceReadKind::TrashedEntry => {
            let entry = find_trash_entry(&request.fragment, &path)
                .await?
                .map(|entry| trash_dentry_to_namespace_entry(&request.fragment, entry))
                .transpose()?;
            PeerNamespaceReadResponse::Entry(entry)
        }
        PeerNamespaceReadKind::ResolveLiveObject => {
            let object_id = DistributedNamespace::resolve_entry(&request.fragment, &path)
                .await?
                .and_then(|resolved| object_id(&resolved.record.target));
            PeerNamespaceReadResponse::ObjectId(object_id)
        }
        PeerNamespaceReadKind::ObjectRefs
        | PeerNamespaceReadKind::TrashObjectRefs
        | PeerNamespaceReadKind::Reachability => {
            let mut refs = match request.kind {
                PeerNamespaceReadKind::ObjectRefs => walk_live_namespace(&request.fragment).await?,
                PeerNamespaceReadKind::TrashObjectRefs => {
                    walk_trash_namespace(&request.fragment).await?
                }
                PeerNamespaceReadKind::Reachability => {
                    let mut refs = walk_live_namespace(&request.fragment).await?;
                    if request.include_trashed {
                        refs.extend(walk_trash_namespace(&request.fragment).await?);
                    }
                    refs
                }
                _ => unreachable!(),
            };
            if matches!(request.kind, PeerNamespaceReadKind::Reachability) {
                PeerNamespaceReadResponse::Reachability(
                    refs.into_iter().map(|entry| entry.object_id).collect(),
                )
            } else {
                if let Some(prefix) = request.prefix.as_deref() {
                    refs.retain(|entry| entry.path.starts_with(prefix));
                }
                refs.sort_by(|left, right| left.path.cmp(&right.path));
                if let Some(after) = request.continuation_token.as_deref() {
                    refs.retain(|entry| entry.path.as_str() > after);
                }
                let limit = request.limit.unwrap_or(200).clamp(1, 1000);
                let continuation_token = (refs.len() > limit).then(|| refs[limit - 1].path.clone());
                refs.truncate(limit);
                PeerNamespaceReadResponse::ObjectRefs(NamespaceObjectRefPage {
                    entries: refs,
                    continuation_token,
                })
            }
        }
    };
    Ok(Some(response))
}

pub async fn commit_upload_quorum(
    fragment: &str,
    logical_path: &str,
    physical_object_id: &str,
    metadata: &ObjectMetadata,
) -> Result<String, AppError> {
    let outcome = commit_namespace_quorum(PeerNamespaceApplyRequest {
        mutation_id: None,
        kind: NamespaceMutationKind::UploadCommit,
        fragment: fragment.to_string(),
        source_path: Some(logical_path.to_string()),
        destination_path: None,
        object_id: Some(physical_object_id.to_string()),
        version_id: Some(metadata.version_id.clone()),
        metadata: Some(metadata.clone()),
    })
    .await?;
    Ok(outcome
        .committed_object()
        .unwrap_or_else(|| physical_object_id.to_string()))
}

pub async fn commit_namespace_quorum(
    request: PeerNamespaceApplyRequest,
) -> Result<NamespaceApplyOutcome, AppError> {
    let mutation_id = request
        .mutation_id
        .unwrap_or_else(|| Ulid::new().to_string());
    let now = Utc::now().timestamp();
    match request.kind {
        NamespaceMutationKind::Mkdir => {
            let path = required(&request.source_path, "mkdir requires source_path")?;
            let existed = match DistributedNamespace::resolve_entry(&request.fragment, path).await {
                Ok(entry) => entry.is_some(),
                Err(AppError::NotFound) => false,
                Err(err) => return Err(err),
            };
            DistributedNamespace::mkdir_all(&request.fragment, path, &mutation_id, now).await?;
            Ok(NamespaceApplyOutcome::Created(!existed))
        }
        NamespaceMutationKind::Rename => {
            let source = required(&request.source_path, "rename requires source_path")?;
            let destination = required(
                &request.destination_path,
                "rename requires destination_path",
            )?;
            let record = DistributedNamespace::rename(
                &request.fragment,
                source,
                destination,
                &mutation_id,
                now,
            )
            .await?;
            Ok(NamespaceApplyOutcome::Summary(summary_from_dentry(
                "rename",
                &record,
                Some(source.to_string()),
                Some(destination.to_string()),
            )))
        }
        NamespaceMutationKind::Trash => {
            let source = required(&request.source_path, "trash requires source_path")?;
            let trash_entry_id = Ulid::new().to_string();
            let record = DistributedNamespace::trash(
                &request.fragment,
                source,
                &trash_entry_id,
                &mutation_id,
                now,
            )
            .await?;
            let mut summary = summary_from_dentry("trash", &record, Some(source.to_string()), None);
            summary.trash_entry_id = Some(trash_entry_id);
            summary.deleted_at_unix = Some(now);
            Ok(NamespaceApplyOutcome::Summary(summary))
        }
        NamespaceMutationKind::Recover => {
            let requested = required(&request.source_path, "recover requires a path or trash id")?;
            let trash_entry = find_trash_entry(&request.fragment, requested)
                .await?
                .ok_or(AppError::NotFound)?;
            let trash_entry_id = trash_entry.name.clone();
            let original_path = trash_entry
                .trash
                .as_ref()
                .map(|context| context.path.clone())
                .ok_or_else(|| {
                    AppError::InternalServerError("trash dentry has no lifecycle context".into())
                })?;
            let record = DistributedNamespace::restore(
                &request.fragment,
                &trash_entry_id,
                &mutation_id,
                now,
            )
            .await?;
            Ok(NamespaceApplyOutcome::Summary(summary_from_dentry(
                "recover",
                &record,
                Some(original_path),
                None,
            )))
        }
        NamespaceMutationKind::UploadCommit => {
            let path = required(&request.source_path, "upload commit requires source_path")?;
            let object_id = required(&request.object_id, "upload commit requires object_id")?;
            DistributedNamespace::publish_object(
                &request.fragment,
                path,
                object_id,
                &mutation_id,
                now,
            )
            .await?;
            Ok(NamespaceApplyOutcome::CommittedObject(
                object_id.to_string(),
            ))
        }
    }
}

pub async fn commit_namespace_created_quorum(
    request: PeerNamespaceApplyRequest,
) -> Result<bool, AppError> {
    Ok(commit_namespace_quorum(request)
        .await?
        .created()
        .unwrap_or(false))
}

pub async fn commit_namespace_summary_quorum(
    request: PeerNamespaceApplyRequest,
) -> Result<NamespaceMutationSummary, AppError> {
    commit_namespace_quorum(request)
        .await?
        .summary()
        .ok_or_else(|| {
            AppError::InternalServerError("namespace mutation did not return a summary".into())
        })
}

async fn find_trash_entry(
    fragment: &str,
    requested: &str,
) -> Result<Option<InodeDentry>, AppError> {
    Ok(DistributedNamespace::list_trash(fragment)
        .await?
        .into_iter()
        .find(|entry| {
            entry.name == requested
                || entry
                    .trash
                    .as_ref()
                    .is_some_and(|trash| trash.path == requested)
        }))
}

fn live_dentry_to_namespace_entry(
    fragment: &str,
    entry: InodeDentry,
    path: String,
) -> NamespaceEntry {
    NamespaceEntry {
        fragment: fragment.to_string(),
        parent_path: crate::namespace::paths::parent_path(&path),
        name: entry.name,
        kind: dentry_kind(&entry.target),
        object_id: object_id(&entry.target),
        size_bytes: None,
        version_id: None,
        packed_ext_u64: None,
        lifecycle: LifecycleState::Live,
        trash_entry_id: None,
        deleted_at_unix: None,
        created_at_unix: nanos_to_unix_seconds(entry.created_at_unix_nanos),
        updated_at_unix: nanos_to_unix_seconds(entry.updated_at_unix_nanos),
        path,
    }
}

fn trash_dentry_to_namespace_entry(
    fragment: &str,
    entry: InodeDentry,
) -> Result<NamespaceEntry, AppError> {
    let trash = entry.trash.clone().ok_or_else(|| {
        AppError::InternalServerError("trash dentry has no lifecycle context".into())
    })?;
    Ok(NamespaceEntry {
        fragment: fragment.to_string(),
        path: trash.path.clone(),
        parent_path: crate::namespace::paths::parent_path(&trash.path),
        name: crate::namespace::paths::base_name(&trash.path),
        kind: dentry_kind(&entry.target),
        object_id: object_id(&entry.target),
        size_bytes: None,
        version_id: None,
        packed_ext_u64: None,
        lifecycle: LifecycleState::trashed(nanos_to_unix_seconds(trash.deleted_at_unix_nanos)),
        trash_entry_id: Some(entry.name),
        deleted_at_unix: Some(nanos_to_unix_seconds(trash.deleted_at_unix_nanos)),
        created_at_unix: nanos_to_unix_seconds(entry.created_at_unix_nanos),
        updated_at_unix: nanos_to_unix_seconds(entry.updated_at_unix_nanos),
    })
}

async fn walk_live_namespace(fragment: &str) -> Result<Vec<NamespaceObjectRef>, AppError> {
    let mut refs = Vec::new();
    let root = DistributedNamespace::resolve_directory(fragment, "").await?;
    let stable_fragment_id = root.inode.fragment_id.clone();
    walk_directory(
        &stable_fragment_id,
        String::new(),
        root.inode.inode_id,
        root.inode.layout,
        &mut refs,
    )
    .await?;
    Ok(refs)
}

async fn walk_trash_namespace(fragment: &str) -> Result<Vec<NamespaceObjectRef>, AppError> {
    let mut refs = Vec::new();
    let namespace = crate::namespace::catalog::resolve_fragment_namespace(
        crate::distribution::membership::active_cluster_runtime()?,
        fragment,
    )
    .await?;
    for entry in DistributedNamespace::list_trash(fragment).await? {
        let base = entry
            .trash
            .as_ref()
            .map(|trash| trash.path.clone())
            .ok_or_else(|| {
                AppError::InternalServerError("trash dentry has no lifecycle context".into())
            })?;
        walk_target(&namespace.stable_fragment_id, base, entry.target, &mut refs).await?;
    }
    Ok(refs)
}

async fn walk_directory(
    fragment: &str,
    base: String,
    directory_id: String,
    layout: InodeLayout,
    refs: &mut Vec<NamespaceObjectRef>,
) -> Result<(), AppError> {
    let mut stack = vec![(base, directory_id, layout)];
    while let Some((base, directory_id, layout)) = stack.pop() {
        for entry in
            DistributedNamespace::list_inode_dentries(fragment, &directory_id, &layout).await?
        {
            let path = join_path(&base, &entry.name);
            match entry.target {
                InodeDentryTarget::Directory { inode_id, layout } => {
                    stack.push((path, inode_id, layout))
                }
                InodeDentryTarget::Object { object_id } => refs.push(NamespaceObjectRef {
                    path,
                    object_id,
                    version_id: String::new(),
                }),
                InodeDentryTarget::Symlink { .. } => {}
                InodeDentryTarget::Fragment { .. } => {
                    return Err(AppError::Conflict(
                        "fragment catalog target appeared inside a fragment namespace".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

async fn walk_target(
    fragment: &str,
    path: String,
    target: InodeDentryTarget,
    refs: &mut Vec<NamespaceObjectRef>,
) -> Result<(), AppError> {
    match target {
        InodeDentryTarget::Object { object_id } => refs.push(NamespaceObjectRef {
            path,
            object_id,
            version_id: String::new(),
        }),
        InodeDentryTarget::Directory { inode_id, layout } => {
            walk_directory(fragment, path, inode_id, layout, refs).await?
        }
        InodeDentryTarget::Symlink { .. } => {}
        InodeDentryTarget::Fragment { .. } => {
            return Err(AppError::Conflict(
                "fragment catalog target appeared inside a fragment namespace".into(),
            ));
        }
    }
    Ok(())
}

fn summary_from_dentry(
    operation: &str,
    record: &InodeDentry,
    source_path: Option<String>,
    destination_path: Option<String>,
) -> NamespaceMutationSummary {
    NamespaceMutationSummary {
        operation: operation.to_string(),
        kind: dentry_kind(&record.target),
        affected_paths: 1,
        affected_objects: usize::from(matches!(record.target, InodeDentryTarget::Object { .. })),
        source_path,
        destination_path,
        trash_entry_id: record.trash.as_ref().map(|_| record.name.clone()),
        deleted_at_unix: record
            .trash
            .as_ref()
            .map(|trash| nanos_to_unix_seconds(trash.deleted_at_unix_nanos)),
    }
}

fn dentry_kind(target: &InodeDentryTarget) -> NamespaceEntryKind {
    match target {
        InodeDentryTarget::Directory { .. } => NamespaceEntryKind::Directory,
        InodeDentryTarget::Object { .. } => NamespaceEntryKind::File,
        InodeDentryTarget::Symlink { .. } => NamespaceEntryKind::File,
        InodeDentryTarget::Fragment { .. } => NamespaceEntryKind::Directory,
    }
}

fn object_id(target: &InodeDentryTarget) -> Option<String> {
    match target {
        InodeDentryTarget::Object { object_id } => Some(object_id.clone()),
        InodeDentryTarget::Directory { .. }
        | InodeDentryTarget::Symlink { .. }
        | InodeDentryTarget::Fragment { .. } => None,
    }
}

fn nanos_to_unix_seconds(nanos: i64) -> i64 {
    nanos / 1_000_000_000
}

fn required<'a>(value: &'a Option<String>, message: &str) -> Result<&'a str, AppError> {
    value
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AppError::BadRequest(message.into()))
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

    #[test]
    fn public_mutation_set_has_no_manual_purge_operations() {
        let encoded = serde_json::to_string(&[
            NamespaceMutationKind::Mkdir,
            NamespaceMutationKind::Rename,
            NamespaceMutationKind::Trash,
            NamespaceMutationKind::Recover,
            NamespaceMutationKind::UploadCommit,
        ])
        .expect("encode mutation kinds");
        assert!(!encoded.contains("purge"));
        assert!(!encoded.contains("empty"));
        assert!(!encoded.contains("recover_all"));
    }
}
