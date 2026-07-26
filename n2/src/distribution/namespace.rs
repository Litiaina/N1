use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::core::disk_manager::ObjectMetadata;
use crate::distribution::ncnx::{
    LocatedNcnx, PeerNcnxName, copy_live_to_key, copy_live_to_key_with_record, copy_live_to_trash,
    find_live, list_canonical_names, ncnx_cursor, publish_new, read_canonical_record, remove_live,
    remove_trash,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::{
    LifecycleState, NamespaceEntry, NamespaceEntryKind, NamespaceMutationSummary,
};
use crate::namespace::ncnx_store::{NcnxRecord, TrashLocator, validate_logical_key};
use crate::namespace::paths::{base_name, normalize_path, parent_path, rename_prefix};
use crate::namespace::runtime::{NamespaceObjectRef, NamespaceObjectRefPage};

const LIST_BATCH_MAX: usize = 1_000;
const PREFIX_MUTATION_CONCURRENCY: usize = 32;

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

/// N2 3.2 namespace operations are already distributed by deterministic NCNX
/// placement. This facade preserves the NOA/native protocol boundary without
/// reintroducing a coordinator database or inode quorum.
pub async fn namespace_read(
    request: PeerNamespaceReadRequest,
) -> Result<Option<PeerNamespaceReadResponse>, AppError> {
    let response = match request.kind {
        PeerNamespaceReadKind::ListDir => {
            let (entries, continuation_token) = list_directory_page(
                &request.fragment,
                request.path.as_deref().unwrap_or_default(),
                request.include_trashed,
                request.limit.unwrap_or(200).clamp(1, 1_000),
                request.continuation_token.as_deref(),
            )
            .await?;
            PeerNamespaceReadResponse::ListDir {
                entries,
                continuation_token,
            }
        }
        PeerNamespaceReadKind::Entry => {
            let path = request.path.as_deref().unwrap_or_default();
            PeerNamespaceReadResponse::Entry(namespace_entry(&request.fragment, path).await?)
        }
        PeerNamespaceReadKind::TrashedEntry => {
            let requested = request.path.as_deref().unwrap_or_default();
            PeerNamespaceReadResponse::Entry(
                find_trash_record(&request.fragment, requested)
                    .await?
                    .map(|(name, located)| trash_entry(&request.fragment, &name, &located)),
            )
        }
        PeerNamespaceReadKind::ResolveLiveObject => {
            let path = request.path.as_deref().unwrap_or_default();
            PeerNamespaceReadResponse::ObjectId(
                find_live(&request.fragment, path)
                    .await?
                    .map(|located| located.record.object_id),
            )
        }
        PeerNamespaceReadKind::ObjectRefs | PeerNamespaceReadKind::TrashObjectRefs => {
            let trash = matches!(request.kind, PeerNamespaceReadKind::TrashObjectRefs);
            let page = object_refs_page(
                &request.fragment,
                request.prefix.as_deref().unwrap_or_default(),
                request.continuation_token.as_deref(),
                request.limit.unwrap_or(200),
                trash,
            )
            .await?;
            PeerNamespaceReadResponse::ObjectRefs(page)
        }
        PeerNamespaceReadKind::Reachability => {
            let mut ids = collect_object_ids(&request.fragment, false).await?;
            if request.include_trashed {
                ids.extend(collect_object_ids(&request.fragment, true).await?);
            }
            PeerNamespaceReadResponse::Reachability(ids)
        }
    };
    Ok(Some(response))
}

pub async fn commit_upload_pointer(
    fragment: &str,
    logical_path: &str,
    physical_object_id: &str,
    metadata: &ObjectMetadata,
) -> Result<String, AppError> {
    let outcome = commit_namespace_mutation(PeerNamespaceApplyRequest {
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

pub async fn commit_namespace_mutation(
    request: PeerNamespaceApplyRequest,
) -> Result<NamespaceApplyOutcome, AppError> {
    let mutation_id = request
        .mutation_id
        .unwrap_or_else(|| Ulid::new().to_string());
    if Ulid::from_string(&mutation_id).is_err() {
        return Err(AppError::BadRequest(
            "namespace mutation_id must be a ULID".into(),
        ));
    }
    let now_nanos = Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| AppError::InternalServerError("system time exceeds nanoseconds".into()))?;
    match request.kind {
        NamespaceMutationKind::Mkdir => {
            let path = required(&request.source_path, "mkdir requires source_path")?;
            validate_logical_key(path)?;
            Err(AppError::BadRequest(
                "N2 3.2 is a flat key namespace and does not persist empty directories".into(),
            ))
        }
        NamespaceMutationKind::Rename => {
            let source = required(&request.source_path, "rename requires source_path")?;
            let destination = required(
                &request.destination_path,
                "rename requires destination_path",
            )?;
            let summary = rename_key_or_prefix(
                &request.fragment,
                source,
                destination,
                &mutation_id,
                now_nanos,
            )
            .await?;
            Ok(NamespaceApplyOutcome::Summary(summary))
        }
        NamespaceMutationKind::Trash => {
            let source = required(&request.source_path, "trash requires source_path")?;
            let summary =
                trash_key_or_prefix(&request.fragment, source, &mutation_id, now_nanos).await?;
            Ok(NamespaceApplyOutcome::Summary(summary))
        }
        NamespaceMutationKind::Recover => {
            let requested = required(&request.source_path, "recover requires path or trash id")?;
            let recovery = find_trash_recovery_set(&request.fragment, requested).await?;
            if recovery.is_empty() {
                return Err(AppError::NotFound);
            }
            // Prove every destination before publishing any live pointer.
            // Recovery is strict: any live destination rejects the whole
            // selected generation, even when it points to the same object.
            for (name, _) in &recovery {
                if find_live(&request.fragment, &name.logical_key)
                    .await?
                    .is_some()
                {
                    return Err(AppError::Conflict(format!(
                        "cannot recover '{}': live key already exists",
                        name.logical_key
                    )));
                }
            }
            let affected = recovery.len();
            let first_name = recovery.first().expect("non-empty recovery set").0.clone();
            let first_locator = first_name
                .trash
                .clone()
                .expect("trash recovery name has a locator");
            let source_path = if affected == 1 {
                first_name.logical_key.clone()
            } else {
                common_parent_for_keys(recovery.iter().map(|(name, _)| name.logical_key.as_str()))
            };
            let mut pending = recovery.into_iter();
            let mut running = FuturesUnordered::new();
            loop {
                while running.len() < PREFIX_MUTATION_CONCURRENCY {
                    let Some((name, located)) = pending.next() else {
                        break;
                    };
                    running.push(async move { recover_one(&name, &located).await });
                }
                match running.next().await {
                    Some(result) => result?,
                    None => break,
                }
            }
            Ok(NamespaceApplyOutcome::Summary(NamespaceMutationSummary {
                operation: "recover".into(),
                kind: if affected == 1 {
                    NamespaceEntryKind::File
                } else {
                    NamespaceEntryKind::Directory
                },
                affected_paths: affected,
                affected_objects: affected,
                source_path: Some(source_path.clone()),
                destination_path: Some(source_path),
                trash_entry_id: Some(first_locator.delete_mutation_id),
                deleted_at_unix: Some(first_locator.deleted_at_unix_nanos / 1_000_000_000),
            }))
        }
        NamespaceMutationKind::UploadCommit => {
            let path = required(&request.source_path, "upload commit requires source_path")?;
            let object_id = required(&request.object_id, "upload commit requires object_id")?;
            let record = NcnxRecord::new(&request.fragment, object_id, mutation_id, now_nanos)?;
            publish_new(&request.fragment, path, &record).await?;
            Ok(NamespaceApplyOutcome::CommittedObject(
                object_id.to_string(),
            ))
        }
    }
}

pub async fn commit_namespace_created(
    request: PeerNamespaceApplyRequest,
) -> Result<bool, AppError> {
    Ok(commit_namespace_mutation(request)
        .await?
        .created()
        .unwrap_or(false))
}

pub async fn commit_namespace_summary(
    request: PeerNamespaceApplyRequest,
) -> Result<NamespaceMutationSummary, AppError> {
    commit_namespace_mutation(request)
        .await?
        .summary()
        .ok_or_else(|| {
            AppError::InternalServerError("namespace mutation did not return a summary".into())
        })
}

async fn namespace_entry(
    fragment: &str,
    requested_path: &str,
) -> Result<Option<NamespaceEntry>, AppError> {
    let normalized =
        normalize_path(requested_path).map_err(|err| AppError::BadRequest(err.to_string()))?;
    if normalized.is_empty() {
        return Ok(Some(directory_entry(fragment, "", "")));
    }
    if let Some(located) = find_live(fragment, &normalized).await? {
        return Ok(Some(live_file_entry(fragment, &normalized, &located)));
    }
    let prefix = format!("{normalized}/");
    if !list_canonical_names(fragment, &prefix, None, 1, false)
        .await?
        .is_empty()
    {
        return Ok(Some(directory_entry(
            fragment,
            &normalized,
            &base_name(&normalized),
        )));
    }
    Ok(None)
}

async fn list_directory_page(
    fragment: &str,
    requested_path: &str,
    trash: bool,
    limit: usize,
    continuation: Option<&str>,
) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
    let directory =
        normalize_path(requested_path).map_err(|err| AppError::BadRequest(err.to_string()))?;
    let prefix = if directory.is_empty() {
        String::new()
    } else {
        format!("{directory}/")
    };
    let mut entries = BTreeMap::<String, NamespaceEntry>::new();
    let mut cursor = continuation.map(ToOwned::to_owned);
    let mut has_more = false;
    loop {
        let names =
            list_canonical_names(fragment, &prefix, cursor.as_deref(), LIST_BATCH_MAX, trash)
                .await?;
        if names.is_empty() {
            break;
        }
        let batch_full = names.len() == LIST_BATCH_MAX;
        for name in names {
            let record_cursor = ncnx_cursor(&name.logical_key, name.trash.as_ref());
            let relative = name.logical_key.strip_prefix(&prefix).unwrap_or_default();
            let first = relative.split('/').next().unwrap_or_default();
            if first.is_empty() {
                cursor = Some(record_cursor);
                continue;
            }
            let child_path = if directory.is_empty() {
                first.to_string()
            } else {
                format!("{directory}/{first}")
            };
            let is_directory = relative.contains('/');
            let entry = if is_directory {
                directory_entry(fragment, &child_path, first)
            } else if trash {
                let located = read_canonical_record(fragment, &name).await?;
                trash_entry(fragment, &name, &located)
            } else {
                // Zero-decode LIST: object identity and object metadata remain
                // absent until HEAD/GET.
                zero_decode_file_entry(fragment, &child_path, first)
            };
            if !entries.contains_key(&child_path) && entries.len() == limit {
                has_more = true;
                break;
            }
            entries.entry(child_path).or_insert(entry);
            cursor = Some(record_cursor);
        }
        if has_more || !batch_full {
            break;
        }
    }
    Ok((
        entries.into_values().collect(),
        has_more.then_some(cursor).flatten(),
    ))
}

async fn object_refs_page(
    fragment: &str,
    prefix: &str,
    continuation: Option<&str>,
    limit: usize,
    trash: bool,
) -> Result<NamespaceObjectRefPage, AppError> {
    let limit = limit.clamp(1, 1_000);
    let names = list_canonical_names(fragment, prefix, continuation, limit + 1, trash).await?;
    let has_more = names.len() > limit;
    let selected = names.into_iter().take(limit).collect::<Vec<_>>();
    let continuation_token = has_more
        .then(|| {
            selected
                .last()
                .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()))
        })
        .flatten();
    let mut entries = Vec::with_capacity(selected.len());
    for name in selected {
        let located = read_canonical_record(fragment, &name).await?;
        entries.push(NamespaceObjectRef {
            path: name.logical_key,
            object_id: located.record.object_id,
            version_id: String::new(),
        });
    }
    Ok(NamespaceObjectRefPage {
        entries,
        continuation_token,
    })
}

async fn all_names(
    fragment: &str,
    prefix: &str,
    trash: bool,
) -> Result<Vec<PeerNcnxName>, AppError> {
    let mut names = Vec::new();
    let mut cursor = None;
    loop {
        let page = list_canonical_names(fragment, prefix, cursor.as_deref(), LIST_BATCH_MAX, trash)
            .await?;
        if page.is_empty() {
            break;
        }
        cursor = page
            .last()
            .map(|name| ncnx_cursor(&name.logical_key, name.trash.as_ref()));
        let full = page.len() == LIST_BATCH_MAX;
        names.extend(page);
        if !full {
            break;
        }
    }
    Ok(names)
}

async fn collect_object_ids(fragment: &str, trash: bool) -> Result<BTreeSet<String>, AppError> {
    let mut ids = BTreeSet::new();
    for name in all_names(fragment, "", trash).await? {
        ids.insert(
            read_canonical_record(fragment, &name)
                .await?
                .record
                .object_id,
        );
    }
    Ok(ids)
}

async fn find_trash_record(
    fragment: &str,
    requested: &str,
) -> Result<Option<(PeerNcnxName, LocatedNcnx)>, AppError> {
    let by_id = Ulid::from_string(requested).is_ok();
    let prefix = if by_id {
        "".to_string()
    } else {
        normalize_path(requested).map_err(|err| AppError::BadRequest(err.to_string()))?
    };
    let mut matching = all_names(fragment, &prefix, true)
        .await?
        .into_iter()
        .filter(|name| {
            let locator = name.trash.as_ref().expect("trash listing includes locator");
            if by_id {
                locator.delete_mutation_id == requested
            } else {
                locator.logical_key == prefix
            }
        })
        .collect::<Vec<_>>();
    matching
        .sort_by_key(|name| std::cmp::Reverse(name.trash.as_ref().unwrap().deleted_at_unix_nanos));
    let Some(name) = matching.into_iter().next() else {
        return Ok(None);
    };
    let located = read_canonical_record(fragment, &name).await?;
    Ok(Some((name, located)))
}

/// Selects one complete trash/recover operation. A mutation ULID identifies
/// the exact delete group. A logical file selects its newest exact record; a
/// virtual directory selects the newest mutation group below that prefix.
async fn find_trash_recovery_set(
    fragment: &str,
    requested: &str,
) -> Result<Vec<(PeerNcnxName, LocatedNcnx)>, AppError> {
    let by_id = Ulid::from_string(requested).is_ok();
    let requested_key = if by_id {
        String::new()
    } else {
        validate_logical_key(requested)?
    };
    let scan_prefix = if by_id { "" } else { requested_key.as_str() };
    let mut candidates = all_names(fragment, scan_prefix, true)
        .await?
        .into_iter()
        .filter(|name| {
            let Some(locator) = name.trash.as_ref() else {
                return false;
            };
            if by_id {
                locator.delete_mutation_id == requested
            } else {
                name.logical_key == requested_key
                    || name.logical_key.starts_with(&format!("{requested_key}/"))
            }
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    if !by_id {
        if let Some(exact) = candidates
            .iter()
            .filter(|name| name.logical_key == requested_key)
            .max_by_key(|name| name.trash.as_ref().unwrap().deleted_at_unix_nanos)
            .cloned()
        {
            candidates = vec![exact];
        } else {
            let newest = candidates
                .iter()
                .max_by_key(|name| name.trash.as_ref().unwrap().deleted_at_unix_nanos)
                .and_then(|name| name.trash.as_ref())
                .expect("trash candidate has locator")
                .delete_mutation_id
                .clone();
            candidates.retain(|name| {
                name.trash
                    .as_ref()
                    .is_some_and(|locator| locator.delete_mutation_id == newest)
            });
        }
    }
    candidates.sort_by(|left, right| left.logical_key.cmp(&right.logical_key));
    let mut recovery = Vec::with_capacity(candidates.len());
    for name in candidates {
        let located = read_canonical_record(fragment, &name).await?;
        recovery.push((name, located));
    }
    Ok(recovery)
}

fn common_parent_for_keys<'a>(keys: impl Iterator<Item = &'a str>) -> String {
    let mut keys = keys;
    let Some(first) = keys.next() else {
        return String::new();
    };
    let mut common = first.split('/').collect::<Vec<_>>();
    for key in keys {
        let components = key.split('/').collect::<Vec<_>>();
        let shared = common
            .iter()
            .zip(components.iter())
            .take_while(|(left, right)| left == right)
            .count();
        common.truncate(shared);
    }
    if common.len() == first.split('/').count() {
        common.pop();
    }
    common.join("/")
}

async fn rename_key_or_prefix(
    fragment: &str,
    source: &str,
    destination: &str,
    mutation_id: &str,
    now_nanos: i64,
) -> Result<NamespaceMutationSummary, AppError> {
    let source = validate_logical_key(source)?;
    let destination = validate_logical_key(destination)?;
    if source == destination {
        return Err(AppError::BadRequest(
            "rename source and destination are identical".into(),
        ));
    }
    if destination.starts_with(&format!("{source}/")) {
        return Err(AppError::BadRequest(
            "cannot move a key prefix inside itself".into(),
        ));
    }
    if let Some(located) = find_live(fragment, &source).await? {
        rename_one(&located, &destination, mutation_id, now_nanos).await?;
        return Ok(mutation_summary(
            "rename",
            NamespaceEntryKind::File,
            1,
            Some(source),
            Some(destination),
            None,
            None,
        ));
    }

    let source_prefix = format!("{source}/");
    let names = all_names(fragment, &source_prefix, false).await?;
    if names.is_empty() {
        return Err(AppError::NotFound);
    }
    let affected = names.len();
    let mut work = Vec::with_capacity(affected);
    for name in names {
        let destination_key = rename_prefix(&name.logical_key, &source, &destination)
            .map_err(|err| AppError::BadRequest(err.to_string()))?;
        let source_record = read_canonical_record(fragment, &name).await?;
        if let Some(existing) = find_live(fragment, &destination_key).await?
            && existing.record.object_id != source_record.record.object_id
        {
            return Err(AppError::Conflict(format!(
                "rename destination '{destination_key}' already exists"
            )));
        }
        work.push((source_record, destination_key));
    }
    run_prefix_rename(work, mutation_id, now_nanos).await?;
    Ok(mutation_summary(
        "rename",
        NamespaceEntryKind::Directory,
        affected,
        Some(source),
        Some(destination),
        None,
        None,
    ))
}

async fn run_prefix_rename(
    work: Vec<(LocatedNcnx, String)>,
    mutation_id: &str,
    now_nanos: i64,
) -> Result<(), AppError> {
    let mut pending = work.into_iter();
    let mut running = FuturesUnordered::new();
    loop {
        while running.len() < PREFIX_MUTATION_CONCURRENCY {
            let Some((located, destination)) = pending.next() else {
                break;
            };
            let mutation_id = mutation_id.to_string();
            running.push(async move {
                rename_one(&located, &destination, &mutation_id, now_nanos).await
            });
        }
        match running.next().await {
            Some(result) => result?,
            None => break,
        }
    }
    Ok(())
}

async fn rename_one(
    located: &LocatedNcnx,
    destination: &str,
    mutation_id: &str,
    now_nanos: i64,
) -> Result<(), AppError> {
    // The destination is a new immutable namespace generation. Historical
    // trash from an older name then cannot masquerade as an active tombstone
    // for this current live pointer.
    let destination_record = NcnxRecord::new(
        &located.record.fragment_id,
        &located.record.object_id,
        mutation_id,
        now_nanos,
    )?;
    copy_live_to_key_with_record(located, destination, &destination_record).await?;
    remove_live(located).await
}

async fn trash_key_or_prefix(
    fragment: &str,
    source: &str,
    mutation_id: &str,
    now_nanos: i64,
) -> Result<NamespaceMutationSummary, AppError> {
    let source = validate_logical_key(source)?;
    if let Some(located) = find_live(fragment, &source).await? {
        trash_one(&located, mutation_id, now_nanos).await?;
        return Ok(mutation_summary(
            "trash",
            NamespaceEntryKind::File,
            1,
            Some(source),
            None,
            Some(mutation_id.to_string()),
            Some(now_nanos),
        ));
    }
    let names = all_names(fragment, &format!("{source}/"), false).await?;
    if names.is_empty() {
        // Repeated user deletion is safe and does not become an internal
        // server error. Report the already-trashed key when present.
        if let Some((name, _)) = find_trash_record(fragment, &source).await? {
            let locator = name.trash.unwrap();
            return Ok(mutation_summary(
                "trash",
                NamespaceEntryKind::File,
                0,
                Some(source),
                None,
                Some(locator.delete_mutation_id),
                Some(locator.deleted_at_unix_nanos),
            ));
        }
        return Err(AppError::NotFound);
    }
    let affected = names.len();
    let mut work = Vec::with_capacity(affected);
    for name in names {
        work.push(read_canonical_record(fragment, &name).await?);
    }
    let mut pending = work.into_iter();
    let mut running = FuturesUnordered::new();
    loop {
        while running.len() < PREFIX_MUTATION_CONCURRENCY {
            let Some(located) = pending.next() else {
                break;
            };
            let mutation_id = mutation_id.to_string();
            running.push(async move { trash_one(&located, &mutation_id, now_nanos).await });
        }
        match running.next().await {
            Some(result) => result?,
            None => break,
        }
    }
    Ok(mutation_summary(
        "trash",
        NamespaceEntryKind::Directory,
        affected,
        Some(source),
        None,
        Some(mutation_id.to_string()),
        Some(now_nanos),
    ))
}

async fn trash_one(
    located: &LocatedNcnx,
    mutation_id: &str,
    now_nanos: i64,
) -> Result<(), AppError> {
    let locator = TrashLocator {
        logical_key: located.logical_key.clone(),
        deleted_at_unix_nanos: now_nanos,
        delete_mutation_id: mutation_id.to_string(),
    };
    copy_live_to_trash(located, &locator).await?;
    remove_live(located).await
}

async fn recover_one(name: &PeerNcnxName, located: &LocatedNcnx) -> Result<(), AppError> {
    let locator = name
        .trash
        .as_ref()
        .ok_or_else(|| AppError::InternalServerError("trash recovery has no locator".into()))?;
    if find_live(&located.record.fragment_id, &name.logical_key)
        .await?
        .is_some()
    {
        return Err(AppError::Conflict(format!(
            "cannot recover '{}': live key already exists",
            name.logical_key
        )));
    }
    copy_live_to_key(located, &name.logical_key).await?;
    remove_trash(located, locator).await
}

fn live_file_entry(fragment: &str, path: &str, located: &LocatedNcnx) -> NamespaceEntry {
    NamespaceEntry {
        fragment: fragment.to_string(),
        path: path.to_string(),
        parent_path: parent_path(path),
        name: base_name(path),
        kind: NamespaceEntryKind::File,
        object_id: Some(located.record.object_id.clone()),
        size_bytes: None,
        version_id: None,
        packed_ext_u64: None,
        lifecycle: LifecycleState::Live,
        trash_entry_id: None,
        deleted_at_unix: None,
        created_at_unix: located.record.created_at_unix_nanos / 1_000_000_000,
        updated_at_unix: located.record.created_at_unix_nanos / 1_000_000_000,
    }
}

fn zero_decode_file_entry(fragment: &str, path: &str, name: &str) -> NamespaceEntry {
    NamespaceEntry {
        fragment: fragment.to_string(),
        path: path.to_string(),
        parent_path: parent_path(path),
        name: name.to_string(),
        kind: NamespaceEntryKind::File,
        object_id: None,
        size_bytes: None,
        version_id: None,
        packed_ext_u64: None,
        lifecycle: LifecycleState::Live,
        trash_entry_id: None,
        deleted_at_unix: None,
        created_at_unix: 0,
        updated_at_unix: 0,
    }
}

fn directory_entry(fragment: &str, path: &str, name: &str) -> NamespaceEntry {
    NamespaceEntry {
        fragment: fragment.to_string(),
        path: path.to_string(),
        parent_path: parent_path(path),
        name: name.to_string(),
        kind: NamespaceEntryKind::Directory,
        object_id: None,
        size_bytes: None,
        version_id: None,
        packed_ext_u64: None,
        lifecycle: LifecycleState::Live,
        trash_entry_id: None,
        deleted_at_unix: None,
        created_at_unix: 0,
        updated_at_unix: 0,
    }
}

fn trash_entry(fragment: &str, name: &PeerNcnxName, located: &LocatedNcnx) -> NamespaceEntry {
    let locator = name.trash.as_ref().expect("trash name includes locator");
    NamespaceEntry {
        fragment: fragment.to_string(),
        path: name.logical_key.clone(),
        parent_path: parent_path(&name.logical_key),
        name: base_name(&name.logical_key),
        kind: NamespaceEntryKind::File,
        object_id: Some(located.record.object_id.clone()),
        size_bytes: None,
        version_id: None,
        packed_ext_u64: None,
        lifecycle: LifecycleState::trashed(locator.deleted_at_unix_nanos / 1_000_000_000),
        trash_entry_id: Some(locator.delete_mutation_id.clone()),
        deleted_at_unix: Some(locator.deleted_at_unix_nanos / 1_000_000_000),
        created_at_unix: located.record.created_at_unix_nanos / 1_000_000_000,
        updated_at_unix: locator.deleted_at_unix_nanos / 1_000_000_000,
    }
}

fn mutation_summary(
    operation: &str,
    kind: NamespaceEntryKind,
    affected: usize,
    source_path: Option<String>,
    destination_path: Option<String>,
    trash_entry_id: Option<String>,
    deleted_at_nanos: Option<i64>,
) -> NamespaceMutationSummary {
    NamespaceMutationSummary {
        operation: operation.to_string(),
        kind,
        affected_paths: affected,
        affected_objects: affected,
        source_path,
        destination_path,
        trash_entry_id,
        deleted_at_unix: deleted_at_nanos.map(|value| value / 1_000_000_000),
    }
}

fn required<'a>(value: &'a Option<String>, message: &str) -> Result<&'a str, AppError> {
    value
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AppError::BadRequest(message.into()))
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

    #[test]
    fn zero_decode_listing_entry_has_no_object_metadata() {
        let entry = zero_decode_file_entry("fragment", "videos/a.mp4", "a.mp4");
        assert!(entry.object_id.is_none());
        assert!(entry.version_id.is_none());
        assert!(entry.size_bytes.is_none());
    }

    #[test]
    fn recovery_group_reports_the_component_aligned_common_parent() {
        assert_eq!(
            common_parent_for_keys(["videos/a.mp4", "videos/nested/b.mp4"].into_iter()),
            "videos"
        );
        assert_eq!(common_parent_for_keys(["a/file", "b/file"].into_iter()), "");
    }
}
