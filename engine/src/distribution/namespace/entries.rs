use super::*;

pub(super) fn live_file_entry(fragment: &str, path: &str, located: &LocatedNcnx) -> NamespaceEntry {
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

pub(super) fn zero_decode_file_entry(fragment: &str, path: &str, name: &str) -> NamespaceEntry {
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

pub(super) fn directory_entry(fragment: &str, path: &str, name: &str) -> NamespaceEntry {
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

pub(super) fn trash_entry(
    fragment: &str,
    name: &PeerNcnxName,
    located: &LocatedNcnx,
) -> NamespaceEntry {
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

pub(super) fn mutation_summary(
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

pub(super) fn required<'a>(value: &'a Option<String>, message: &str) -> Result<&'a str, AppError> {
    value
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AppError::BadRequest(message.into()))
}
