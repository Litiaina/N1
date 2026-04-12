use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::models::{
    LifecycleState, NamespaceEntry, NamespaceEntryKind, NamespaceError, NamespaceFileListing,
    NamespaceListEntry, NamespaceMutationSummary, NamespaceNode, NamespacePage,
    NamespaceReachabilitySnapshot, ObjectHead, ObjectRecord, ObjectVersionRecord,
};
use super::paths::{base_name, normalize_path, parent_path};

const LIVE_ROOT_NODE_ID: &str = "__live_root__";
const TRASH_ROOT_NODE_ID: &str = "__trash_root__";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceCatalog {
    fragment: String,
    live_root_node_id: String,
    trash_root_node_id: String,
    nodes: BTreeMap<String, NamespaceNode>,
    child_index: BTreeMap<String, String>,
    objects: BTreeMap<String, ObjectRecord>,
    object_versions: BTreeMap<(String, String), ObjectVersionRecord>,
    object_nodes: BTreeMap<String, String>,
    #[serde(skip, default)]
    trash_anchor_index: BTreeMap<String, String>,
    #[serde(skip, default)]
    subtree_path_counts: BTreeMap<String, usize>,
    #[serde(skip, default)]
    subtree_object_counts: BTreeMap<String, usize>,
}

impl Default for NamespaceCatalog {
    fn default() -> Self {
        Self::new(String::new())
    }
}

impl NamespaceCatalog {
    pub fn new(fragment: impl Into<String>) -> Self {
        let fragment = fragment.into();
        let mut nodes = BTreeMap::new();
        nodes.insert(
            LIVE_ROOT_NODE_ID.to_string(),
            NamespaceNode {
                fragment: fragment.clone(),
                node_id: LIVE_ROOT_NODE_ID.to_string(),
                parent_node_id: None,
                name: String::new(),
                kind: NamespaceEntryKind::Directory,
                object_id: None,
                head_version_id: None,
                trashed_at_unix: None,
                restore_parent_path: None,
                restore_name: None,
                created_at_unix: 0,
                updated_at_unix: 0,
            },
        );
        nodes.insert(
            TRASH_ROOT_NODE_ID.to_string(),
            NamespaceNode {
                fragment: fragment.clone(),
                node_id: TRASH_ROOT_NODE_ID.to_string(),
                parent_node_id: None,
                name: String::new(),
                kind: NamespaceEntryKind::Directory,
                object_id: None,
                head_version_id: None,
                trashed_at_unix: None,
                restore_parent_path: None,
                restore_name: None,
                created_at_unix: 0,
                updated_at_unix: 0,
            },
        );
        Self {
            fragment,
            live_root_node_id: LIVE_ROOT_NODE_ID.to_string(),
            trash_root_node_id: TRASH_ROOT_NODE_ID.to_string(),
            nodes,
            child_index: BTreeMap::new(),
            objects: BTreeMap::new(),
            object_versions: BTreeMap::new(),
            object_nodes: BTreeMap::new(),
            trash_anchor_index: BTreeMap::new(),
            subtree_path_counts: BTreeMap::new(),
            subtree_object_counts: BTreeMap::new(),
        }
    }

    pub fn fragment(&self) -> &str {
        &self.fragment
    }

    pub fn rebuild_secondary_indices(&mut self) -> Result<(), NamespaceError> {
        self.trash_anchor_index.clear();
        self.subtree_path_counts.clear();
        self.subtree_object_counts.clear();

        for anchor_id in self.sorted_child_ids(&self.trash_root_node_id) {
            let anchor_path = self.anchor_visible_path(&anchor_id)?;
            self.trash_anchor_index.insert(anchor_path, anchor_id);
        }

        self.rebuild_subtree_stats()?;

        Ok(())
    }

    pub fn mkdir(&mut self, path: &str, changed_at_unix: i64) -> Result<bool, NamespaceError> {
        let normalized = normalize_path(path)?;
        if normalized.is_empty() {
            return Ok(false);
        }

        if let Some(existing_id) = self.resolve_live_node_id(&normalized)? {
            let existing = self
                .nodes
                .get(&existing_id)
                .ok_or_else(|| self.missing_node_error(&existing_id))?;
            if existing.kind == NamespaceEntryKind::Directory {
                return Ok(false);
            }
            return Err(NamespaceError::Conflict(format!(
                "path '{normalized}' already exists as a file"
            )));
        }

        let (parent_node_id, name) = self.ensure_live_parent(&normalized, changed_at_unix)?;
        self.insert_node(NamespaceNode {
            fragment: self.fragment.clone(),
            node_id: Ulid::new().to_string(),
            parent_node_id: Some(parent_node_id),
            name,
            kind: NamespaceEntryKind::Directory,
            object_id: None,
            head_version_id: None,
            trashed_at_unix: None,
            restore_parent_path: None,
            restore_name: None,
            created_at_unix: changed_at_unix,
            updated_at_unix: changed_at_unix,
        })?;
        Ok(true)
    }

    pub fn put_file(
        &mut self,
        path: &str,
        object_id: &str,
        version_id: &str,
        storage_object_id: &str,
        content_type: &str,
        total_size_bytes: u64,
        stripe_size_bytes: u64,
        changed_at_unix: i64,
    ) -> Result<(), NamespaceError> {
        let normalized = normalize_path(path)?;
        if normalized.is_empty() {
            return Err(NamespaceError::InvalidPath(
                "root cannot be written as a file".to_string(),
            ));
        }

        let existing_live = self.resolve_live_node_id(&normalized)?;
        let (parent_node_id, name) = self.ensure_live_parent(&normalized, changed_at_unix)?;

        let previous_version_id = if let Some(existing_id) = existing_live.clone() {
            let existing = self
                .nodes
                .get(&existing_id)
                .ok_or_else(|| self.missing_node_error(&existing_id))?;
            if existing.kind == NamespaceEntryKind::Directory {
                return Err(NamespaceError::Conflict(format!(
                    "path '{normalized}' already exists as a directory"
                )));
            }
            if existing.object_id.as_deref() != Some(object_id) {
                return Err(NamespaceError::Conflict(format!(
                    "path '{normalized}' is already bound to a different object id"
                )));
            }
            existing.head_version_id.clone()
        } else {
            None
        };

        self.objects
            .entry(object_id.to_string())
            .or_insert_with(|| ObjectRecord {
                fragment: self.fragment.clone(),
                object_id: object_id.to_string(),
                created_at_unix: changed_at_unix,
            });

        self.object_versions.insert(
            (object_id.to_string(), version_id.to_string()),
            ObjectVersionRecord {
                fragment: self.fragment.clone(),
                object_id: object_id.to_string(),
                version_id: version_id.to_string(),
                previous_version_id,
                storage_object_id: storage_object_id.to_string(),
                content_type: content_type.to_string(),
                total_size_bytes,
                stripe_size_bytes,
                created_at_unix: changed_at_unix,
            },
        );

        if let Some(existing_id) = existing_live {
            let Some(node) = self.nodes.get_mut(&existing_id) else {
                return Err(self.missing_node_error(&existing_id));
            };
            node.parent_node_id = Some(parent_node_id);
            node.name = name;
            node.object_id = Some(object_id.to_string());
            node.head_version_id = Some(version_id.to_string());
            node.updated_at_unix = changed_at_unix;
            self.object_nodes
                .insert(object_id.to_string(), existing_id.to_string());
        } else {
            let node = NamespaceNode {
                fragment: self.fragment.clone(),
                node_id: Ulid::new().to_string(),
                parent_node_id: Some(parent_node_id),
                name,
                kind: NamespaceEntryKind::File,
                object_id: Some(object_id.to_string()),
                head_version_id: Some(version_id.to_string()),
                trashed_at_unix: None,
                restore_parent_path: None,
                restore_name: None,
                created_at_unix: changed_at_unix,
                updated_at_unix: changed_at_unix,
            };
            self.object_nodes
                .insert(object_id.to_string(), node.node_id.clone());
            self.insert_node(node)?;
        }

        Ok(())
    }

    pub fn list_dir(
        &self,
        path: &str,
        include_trashed: bool,
    ) -> Result<Vec<NamespaceListEntry>, NamespaceError> {
        let normalized = normalize_path(path)?;
        if include_trashed {
            return self.list_trash_dir(&normalized);
        }
        self.list_live_dir(&normalized)
    }

    pub fn list_dir_page(
        &self,
        path: &str,
        include_trashed: bool,
        limit: usize,
        continuation_token: Option<&str>,
    ) -> Result<(Vec<NamespaceListEntry>, Option<String>), NamespaceError> {
        let limit = limit.max(1);
        let entries = self.list_dir(path, include_trashed)?;
        let continuation = continuation_token.unwrap_or("");
        let mut page = Vec::with_capacity(limit.min(entries.len()));
        let mut has_more = false;
        let mut last_path = None::<String>;

        for entry in entries
            .into_iter()
            .filter(|entry| entry.path.as_str() > continuation)
        {
            if page.len() >= limit {
                has_more = true;
                break;
            }
            last_path = Some(entry.path.clone());
            page.push(entry);
        }

        Ok((page, if has_more { last_path } else { None }))
    }

    pub fn list_page(
        &self,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        limit: usize,
        continuation_token: Option<&str>,
        scan_trash: bool,
    ) -> Result<NamespacePage, NamespaceError> {
        let prefix = prefix.unwrap_or("").trim_start_matches('/').to_string();
        let delimiter = delimiter.filter(|value| !value.is_empty());
        if !scan_trash && delimiter.is_none() {
            return self.live_file_page(&prefix, limit, continuation_token);
        }

        #[derive(Clone)]
        enum PageItem {
            File(NamespaceFileListing),
            Prefix(String),
        }

        impl PageItem {
            fn sort_key(&self) -> &str {
                match self {
                    Self::File(file) => &file.path,
                    Self::Prefix(prefix) => prefix,
                }
            }
        }

        let entries = self.visible_entries_under_prefix(scan_trash, &prefix)?;
        let start_key = continuation_token
            .filter(|token| *token > prefix.as_str())
            .unwrap_or(prefix.as_str())
            .to_string();
        let continuation_token = continuation_token.unwrap_or("");
        let mut files = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut seen_prefixes = BTreeSet::new();
        let mut last_sort_key = None::<String>;
        let mut has_more = false;

        for entry in entries {
            if entry.path < start_key {
                continue;
            }
            let path = &entry.path;

            let suffix = if prefix.is_empty() {
                path.as_str()
            } else {
                path.strip_prefix(&prefix).unwrap_or_default()
            };
            let suffix = suffix.trim_start_matches('/');

            let item = if let Some(delimiter) = delimiter {
                if suffix.is_empty() && !prefix.is_empty() && path == &prefix {
                    None
                } else if let Some(index) = suffix.find(delimiter) {
                    let prefix_head = if prefix.is_empty() {
                        String::new()
                    } else if prefix.ends_with(delimiter)
                        || path[prefix.len()..].starts_with(delimiter)
                    {
                        prefix.clone()
                    } else {
                        format!("{prefix}{delimiter}")
                    };
                    let segment = &suffix[..index];
                    let common_prefix = format!("{prefix_head}{segment}{delimiter}");
                    if common_prefix.as_str() <= continuation_token
                        || !seen_prefixes.insert(common_prefix.clone())
                    {
                        None
                    } else {
                        Some(PageItem::Prefix(common_prefix))
                    }
                } else if entry.kind == NamespaceEntryKind::Directory {
                    let common_prefix = format!("{}/", path.trim_end_matches('/'));
                    if common_prefix.as_str() <= continuation_token
                        || !seen_prefixes.insert(common_prefix.clone())
                    {
                        None
                    } else {
                        Some(PageItem::Prefix(common_prefix))
                    }
                } else if entry.kind == NamespaceEntryKind::File
                    && path.as_str() > continuation_token
                {
                    let object_id = entry.object_id.clone().ok_or_else(|| {
                        NamespaceError::Invariant(format!(
                            "file '{}' is missing a stable object id",
                            path
                        ))
                    })?;
                    Some(PageItem::File(NamespaceFileListing {
                        path: path.clone(),
                        object_id,
                    }))
                } else {
                    None
                }
            } else if entry.kind == NamespaceEntryKind::File && path.as_str() > continuation_token {
                let object_id = entry.object_id.clone().ok_or_else(|| {
                    NamespaceError::Invariant(format!(
                        "file '{}' is missing a stable object id",
                        path
                    ))
                })?;
                Some(PageItem::File(NamespaceFileListing {
                    path: path.clone(),
                    object_id,
                }))
            } else {
                None
            };

            let Some(item) = item else {
                continue;
            };

            if files.len() + common_prefixes.len() >= limit {
                has_more = true;
                break;
            }

            last_sort_key = Some(item.sort_key().to_string());
            match item {
                PageItem::File(file) => files.push(file),
                PageItem::Prefix(prefix) => common_prefixes.push(prefix),
            }
        }

        let continuation_token = if has_more { last_sort_key } else { None };

        Ok(NamespacePage {
            files,
            common_prefixes,
            continuation_token,
        })
    }

    fn live_file_page(
        &self,
        prefix: &str,
        limit: usize,
        continuation_token: Option<&str>,
    ) -> Result<NamespacePage, NamespaceError> {
        let (start_node_id, start_path) = self.find_live_traversal_start(prefix)?;
        let continuation_token = continuation_token.unwrap_or("");
        let mut cursor = if continuation_token > prefix {
            match self.resolve_live_node_id(continuation_token)? {
                Some(node_id)
                    if self
                        .nodes
                        .get(&node_id)
                        .is_some_and(|node| node.kind == NamespaceEntryKind::File) =>
                {
                    self.next_live_file_after(&start_node_id, &node_id, continuation_token)?
                }
                _ => self.first_live_file_after(
                    &start_node_id,
                    &start_path,
                    prefix,
                    continuation_token,
                )?,
            }
        } else {
            self.first_live_file_after(&start_node_id, &start_path, prefix, continuation_token)?
        };
        let mut files = Vec::new();
        let mut last_key = None::<String>;
        let mut has_more = false;

        while let Some((node_id, path)) = cursor.clone() {
            let node = self
                .nodes
                .get(&node_id)
                .ok_or_else(|| self.missing_node_error(&node_id))?;

            let object_id = node.object_id.clone().ok_or_else(|| {
                NamespaceError::Invariant(format!("file '{}' is missing a stable object id", path))
            })?;
            last_key = Some(path.clone());
            files.push(NamespaceFileListing {
                path: path.clone(),
                object_id,
            });

            let mut next_cursor = self.next_live_file_after(&start_node_id, &node_id, &path)?;
            while let Some((next_id, next_path)) = next_cursor.clone() {
                if prefix.is_empty() || next_path.starts_with(prefix) {
                    break;
                }
                next_cursor = self.next_live_file_after(&start_node_id, &next_id, &next_path)?;
            }

            if files.len() >= limit {
                has_more = next_cursor.is_some();
                break;
            }

            cursor = next_cursor;
        }

        Ok(NamespacePage {
            files,
            common_prefixes: Vec::new(),
            continuation_token: if has_more { last_key } else { None },
        })
    }

    pub fn rename_path(
        &mut self,
        source_path: &str,
        destination_path: &str,
        changed_at_unix: i64,
    ) -> Result<NamespaceMutationSummary, NamespaceError> {
        let source = normalize_path(source_path)?;
        let destination = normalize_path(destination_path)?;
        if source.is_empty() || destination.is_empty() {
            return Err(NamespaceError::InvalidPath(
                "root path cannot be renamed".to_string(),
            ));
        }

        let source_id = self
            .resolve_live_node_id(&source)?
            .ok_or_else(|| NamespaceError::NotFound(format!("path '{source}' does not exist")))?;
        if let Some(existing_id) = self.resolve_live_node_id(&destination)? {
            if existing_id != source_id {
                return Err(NamespaceError::Conflict(format!(
                    "destination path '{destination}' already exists"
                )));
            }
        }

        let (destination_parent_id, destination_name) =
            self.ensure_live_parent(&destination, changed_at_unix)?;
        if self.is_ancestor(&source_id, &destination_parent_id)? {
            return Err(NamespaceError::Conflict(format!(
                "cannot move '{source}' into its own descendant '{destination}'"
            )));
        }

        if self
            .lookup_child_id(&destination_parent_id, &destination_name)?
            .is_some_and(|existing_id| existing_id != source_id)
        {
            return Err(NamespaceError::Conflict(format!(
                "destination path '{destination}' already exists"
            )));
        }

        let source_entry = self
            .entry(&source)?
            .ok_or_else(|| NamespaceError::NotFound(format!("path '{source}' does not exist")))?;
        let (affected_paths, affected_objects) = self.subtree_counts(&source_id)?;
        self.rebind_node(
            &source_id,
            &destination_parent_id,
            &destination_name,
            changed_at_unix,
        )?;

        Ok(NamespaceMutationSummary {
            operation: "rename".to_string(),
            kind: source_entry.kind,
            affected_paths,
            affected_objects,
            source_path: Some(source),
            destination_path: Some(destination),
        })
    }

    pub fn trash_path(
        &mut self,
        path: &str,
        changed_at_unix: i64,
    ) -> Result<NamespaceMutationSummary, NamespaceError> {
        let normalized = normalize_path(path)?;
        if normalized.is_empty() {
            return Err(NamespaceError::InvalidPath(
                "root path cannot be trashed".to_string(),
            ));
        }

        let source_id = self.resolve_live_node_id(&normalized)?.ok_or_else(|| {
            NamespaceError::NotFound(format!("path '{normalized}' does not exist"))
        })?;
        let source_entry = self.entry(&normalized)?.ok_or_else(|| {
            NamespaceError::NotFound(format!("path '{normalized}' does not exist"))
        })?;
        let (affected_paths, affected_objects) = self.subtree_counts(&source_id)?;
        self.trash_anchor_index
            .insert(normalized.clone(), source_id.clone());

        let Some(source_node) = self.nodes.get_mut(&source_id) else {
            return Err(self.missing_node_error(&source_id));
        };
        source_node.trashed_at_unix = Some(changed_at_unix);
        source_node.restore_parent_path = Some(parent_path(&normalized));
        source_node.restore_name = Some(source_node.name.clone());

        let trash_slot = Ulid::new().to_string();
        self.rebind_node(
            &source_id,
            &self.trash_root_node_id.clone(),
            &trash_slot,
            changed_at_unix,
        )?;

        Ok(NamespaceMutationSummary {
            operation: "trash".to_string(),
            kind: source_entry.kind,
            affected_paths,
            affected_objects,
            source_path: Some(normalized),
            destination_path: None,
        })
    }

    pub fn recover_path(
        &mut self,
        path: &str,
        changed_at_unix: i64,
    ) -> Result<NamespaceMutationSummary, NamespaceError> {
        let normalized = normalize_path(path)?;
        let entry = self.trashed_entry(&normalized)?.ok_or_else(|| {
            NamespaceError::NotFound(format!("path '{normalized}' is not in trash"))
        })?;
        if let Some(node_id) = self.resolve_trashed_node_id(&normalized)? {
            return self.recover_resolved_trashed_path(
                &normalized,
                entry,
                &node_id,
                changed_at_unix,
            );
        }

        if entry.kind == NamespaceEntryKind::Directory {
            return self.recover_virtual_trash_directory(&normalized, changed_at_unix);
        }

        Err(NamespaceError::NotFound(format!(
            "path '{normalized}' is not in trash"
        )))
    }

    fn recover_resolved_trashed_path(
        &mut self,
        normalized: &str,
        entry: NamespaceEntry,
        node_id: &str,
        changed_at_unix: i64,
    ) -> Result<NamespaceMutationSummary, NamespaceError> {
        let node_id = node_id.to_string();

        let destination_parent_path = parent_path(normalized);
        let destination_name = base_name(normalized);
        let destination_parent_id =
            self.ensure_live_directory(&destination_parent_path, changed_at_unix)?;
        if self
            .lookup_child_id(&destination_parent_id, &destination_name)?
            .is_some()
        {
            return Err(NamespaceError::Conflict(format!(
                "destination path '{normalized}' already exists"
            )));
        }

        let (affected_paths, affected_objects) = self.subtree_counts(&node_id)?;
        let was_anchor = self
            .nodes
            .get(&node_id)
            .ok_or_else(|| self.missing_node_error(&node_id))?
            .trashed_at_unix
            .is_some();

        {
            let Some(node) = self.nodes.get_mut(&node_id) else {
                return Err(self.missing_node_error(&node_id));
            };
            node.trashed_at_unix = None;
            node.restore_parent_path = None;
            node.restore_name = None;
        }
        if was_anchor {
            self.trash_anchor_index.remove(normalized);
        }
        self.rebind_node(
            &node_id,
            &destination_parent_id,
            &destination_name,
            changed_at_unix,
        )?;

        if was_anchor {
            self.cleanup_empty_trash_ancestors(normalized)?;
        }

        Ok(NamespaceMutationSummary {
            operation: "recover".to_string(),
            kind: entry.kind,
            affected_paths,
            affected_objects,
            source_path: Some(normalized.to_string()),
            destination_path: None,
        })
    }

    fn recover_virtual_trash_directory(
        &mut self,
        path: &str,
        changed_at_unix: i64,
    ) -> Result<NamespaceMutationSummary, NamespaceError> {
        let normalized = normalize_path(path)?;
        let anchor_paths = self.collect_trash_anchor_paths_under(&normalized);
        if anchor_paths.is_empty() {
            return Err(NamespaceError::NotFound(format!(
                "path '{normalized}' is not in trash"
            )));
        }

        for anchor_path in &anchor_paths {
            self.preflight_recover_destination(anchor_path)?;
        }

        let mut affected_paths = 0usize;
        let mut affected_objects = 0usize;
        for anchor_path in anchor_paths {
            let entry = self.trashed_entry(&anchor_path)?.ok_or_else(|| {
                NamespaceError::NotFound(format!("path '{anchor_path}' is not in trash"))
            })?;
            let node_id = self.resolve_trashed_node_id(&anchor_path)?.ok_or_else(|| {
                NamespaceError::NotFound(format!("path '{anchor_path}' is not in trash"))
            })?;
            let summary =
                self.recover_resolved_trashed_path(&anchor_path, entry, &node_id, changed_at_unix)?;
            affected_paths = affected_paths.saturating_add(summary.affected_paths);
            affected_objects = affected_objects.saturating_add(summary.affected_objects);
        }

        Ok(NamespaceMutationSummary {
            operation: "recover".to_string(),
            kind: NamespaceEntryKind::Directory,
            affected_paths,
            affected_objects,
            source_path: Some(normalized),
            destination_path: None,
        })
    }

    pub fn empty_trash(&mut self, changed_at_unix: i64) -> NamespaceMutationSummary {
        let anchors = self.trash_anchor_ids();
        let mut affected_paths = 0usize;
        let mut affected_objects = 0usize;
        for anchor_id in anchors {
            if !self.nodes.contains_key(&anchor_id) {
                continue;
            }
            let (paths, objects) = self.subtree_counts(&anchor_id).unwrap_or((0, 0));
            affected_paths = affected_paths.saturating_add(paths);
            affected_objects = affected_objects.saturating_add(objects);
            let _ = self.remove_subtree(&anchor_id);
        }
        let _ = changed_at_unix;
        NamespaceMutationSummary {
            operation: "empty_trash".to_string(),
            kind: NamespaceEntryKind::File,
            affected_paths,
            affected_objects,
            source_path: None,
            destination_path: None,
        }
    }

    pub fn entry(&self, path: &str) -> Result<Option<NamespaceEntry>, NamespaceError> {
        self.live_entry(path)
    }

    pub fn live_entry(&self, path: &str) -> Result<Option<NamespaceEntry>, NamespaceError> {
        let normalized = normalize_path(path)?;
        let Some(node_id) = self.resolve_live_node_id(&normalized)? else {
            return Ok(None);
        };
        self.node_view(&node_id, &normalized, LifecycleState::live())
            .map(Some)
    }

    pub fn trashed_entry(&self, path: &str) -> Result<Option<NamespaceEntry>, NamespaceError> {
        let normalized = normalize_path(path)?;
        if let Some(node_id) = self.resolve_trashed_node_id(&normalized)? {
            let lifecycle = self.trashed_lifecycle_for_node(&node_id)?;
            return self.node_view(&node_id, &normalized, lifecycle).map(Some);
        }
        self.synthetic_trashed_directory_entry(&normalized)
    }

    pub fn object_head(&self, object_id: &str) -> Option<ObjectHead> {
        let node_id = self.object_nodes.get(object_id)?;
        let node = self.nodes.get(node_id)?;
        let version_id = node.head_version_id.as_ref()?;
        let version = self
            .object_versions
            .get(&(object_id.to_string(), version_id.clone()))?;
        let lifecycle = self.lifecycle_for_node(node_id).ok()?;
        Some(ObjectHead {
            fragment: self.fragment.clone(),
            object_id: object_id.to_string(),
            current_version_id: version.version_id.clone(),
            storage_object_id: version.storage_object_id.clone(),
            content_type: version.content_type.clone(),
            total_size_bytes: version.total_size_bytes,
            lifecycle,
            created_at_unix: self.objects.get(object_id)?.created_at_unix,
            updated_at_unix: node.updated_at_unix,
        })
    }

    pub fn current_version(&self, object_id: &str) -> Option<&ObjectVersionRecord> {
        let node_id = self.object_nodes.get(object_id)?;
        let node = self.nodes.get(node_id)?;
        let version_id = node.head_version_id.as_ref()?;
        self.object_versions
            .get(&(object_id.to_string(), version_id.clone()))
    }

    pub fn recover_all(&mut self, changed_at_unix: i64) -> NamespaceMutationSummary {
        let mut affected_paths = 0usize;
        let mut affected_objects = 0usize;
        let anchors = self.trash_anchor_index.keys().cloned().collect::<Vec<_>>();
        for anchor_path in anchors {
            if let Ok(summary) = self.recover_path(&anchor_path, changed_at_unix) {
                affected_paths = affected_paths.saturating_add(summary.affected_paths);
                affected_objects = affected_objects.saturating_add(summary.affected_objects);
            }
        }
        NamespaceMutationSummary {
            operation: "recover_all".to_string(),
            kind: NamespaceEntryKind::File,
            affected_paths,
            affected_objects,
            source_path: None,
            destination_path: None,
        }
    }

    pub fn reachability_snapshot(&self) -> NamespaceReachabilitySnapshot {
        let referenced_object_ids = self.object_nodes.keys().cloned().collect::<BTreeSet<_>>();
        let referenced_version_ids = self
            .object_versions
            .iter()
            .filter_map(|((object_id, version_id), _)| {
                referenced_object_ids
                    .contains(object_id)
                    .then(|| version_id.clone())
            })
            .collect::<BTreeSet<_>>();

        NamespaceReachabilitySnapshot {
            referenced_object_ids,
            referenced_version_ids,
        }
    }

    pub fn purge_unreferenced_object_ids(&mut self, object_ids: &BTreeSet<String>) -> usize {
        if object_ids.is_empty() {
            return 0;
        }

        let removed_objects = object_ids
            .iter()
            .filter(|object_id| self.object_nodes.get(*object_id).is_none())
            .count();

        self.objects
            .retain(|object_id, _| !object_ids.contains(object_id));
        self.object_versions
            .retain(|(object_id, _), _| !object_ids.contains(object_id));
        self.object_nodes
            .retain(|object_id, _| !object_ids.contains(object_id));

        removed_objects
    }

    fn list_live_dir(&self, path: &str) -> Result<Vec<NamespaceListEntry>, NamespaceError> {
        let node_id = if path.is_empty() {
            self.live_root_node_id.clone()
        } else {
            self.resolve_live_node_id(path)?.ok_or_else(|| {
                NamespaceError::NotFound(format!("directory '{path}' does not exist"))
            })?
        };
        let node = self
            .nodes
            .get(&node_id)
            .ok_or_else(|| self.missing_node_error(&node_id))?;
        if node.kind != NamespaceEntryKind::Directory {
            return Err(NamespaceError::Conflict(format!(
                "path '{path}' exists as a file"
            )));
        }

        let mut out = Vec::new();
        for child_id in self.sorted_child_ids(&node_id) {
            let child = self
                .nodes
                .get(&child_id)
                .ok_or_else(|| self.missing_node_error(&child_id))?;
            let child_path = join_visible_path(path, &child.name);
            out.push(NamespaceListEntry {
                name: child.name.clone(),
                path: child_path,
                kind: child.kind,
                lifecycle: LifecycleState::live(),
                object_id: child.object_id.clone(),
                created_at_unix: child.created_at_unix,
                updated_at_unix: child.updated_at_unix,
            });
        }
        Ok(out)
    }

    fn list_trash_dir(&self, path: &str) -> Result<Vec<NamespaceListEntry>, NamespaceError> {
        let normalized = normalize_path(path)?;
        if !normalized.is_empty() {
            match self.trashed_entry(&normalized)? {
                Some(entry) if entry.kind == NamespaceEntryKind::Directory => {}
                Some(_) => {
                    return Err(NamespaceError::Conflict(format!(
                        "path '{normalized}' exists as a file"
                    )));
                }
                None => {
                    return Err(NamespaceError::NotFound(format!(
                        "directory '{normalized}' does not exist"
                    )));
                }
            }
        }

        let mut out = BTreeMap::<String, NamespaceListEntry>::new();
        for entry in self.visible_entries_under_prefix(true, &normalized)? {
            let relative = if normalized.is_empty() {
                entry.path.clone()
            } else if entry.path == normalized {
                String::new()
            } else if let Some(suffix) = entry.path.strip_prefix(&(normalized.clone() + "/")) {
                suffix.to_string()
            } else {
                continue;
            };
            if relative.is_empty() {
                continue;
            }

            if let Some((segment, _)) = relative.split_once('/') {
                let virtual_path = join_visible_path(&normalized, segment);
                Self::upsert_virtual_trash_directory(
                    &mut out,
                    &virtual_path,
                    entry.lifecycle.clone(),
                    entry.created_at_unix,
                    entry.updated_at_unix,
                );
                continue;
            }

            let node_id = self
                .resolve_trashed_node_id(&entry.path)?
                .ok_or_else(|| self.missing_node_error(&entry.path))?;
            let node = self
                .nodes
                .get(&node_id)
                .ok_or_else(|| self.missing_node_error(&node_id))?;
            let lifecycle = self.trashed_lifecycle_for_node(&node_id)?;
            out.insert(
                entry.path.clone(),
                NamespaceListEntry {
                    name: entry.name,
                    path: entry.path,
                    kind: node.kind,
                    lifecycle,
                    object_id: node.object_id.clone(),
                    created_at_unix: node.created_at_unix,
                    updated_at_unix: node.updated_at_unix,
                },
            );
        }
        Ok(out.into_values().collect())
    }

    fn collect_trash_anchor_paths_under(&self, path: &str) -> Vec<String> {
        let normalized = normalize_path(path).unwrap_or_default();
        let prefix = if normalized.is_empty() {
            None
        } else {
            Some(format!("{normalized}/"))
        };
        let mut anchor_paths = self
            .trash_anchor_index
            .keys()
            .filter(|anchor_path| {
                if normalized.is_empty() {
                    true
                } else {
                    *anchor_path == &normalized
                        || prefix
                            .as_ref()
                            .is_some_and(|prefix| anchor_path.starts_with(prefix))
                }
            })
            .cloned()
            .collect::<Vec<_>>();
        anchor_paths.sort_by_key(|path| (path.matches('/').count(), path.clone()));
        anchor_paths
    }

    fn synthetic_trashed_directory_entry(
        &self,
        path: &str,
    ) -> Result<Option<NamespaceEntry>, NamespaceError> {
        let normalized = normalize_path(path)?;
        if normalized.is_empty() {
            return Ok(None);
        }

        let mut lifecycle = None::<LifecycleState>;
        let mut created_at_unix = i64::MAX;
        let mut updated_at_unix = 0i64;
        let prefix = format!("{normalized}/");

        for entry in self.visible_entries_under_prefix(true, &normalized)? {
            if entry.path == normalized || entry.path.starts_with(&prefix) {
                lifecycle.get_or_insert_with(|| entry.lifecycle.clone());
                created_at_unix = created_at_unix.min(entry.created_at_unix);
                updated_at_unix = updated_at_unix.max(entry.updated_at_unix);
            }
        }

        let Some(lifecycle) = lifecycle else {
            return Ok(None);
        };

        Ok(Some(NamespaceEntry {
            fragment: self.fragment.clone(),
            path: normalized.clone(),
            parent_path: parent_path(&normalized),
            name: base_name(&normalized),
            kind: NamespaceEntryKind::Directory,
            object_id: None,
            lifecycle,
            created_at_unix: if created_at_unix == i64::MAX {
                0
            } else {
                created_at_unix
            },
            updated_at_unix,
        }))
    }

    fn first_live_file_after(
        &self,
        root_node_id: &str,
        root_path: &str,
        prefix: &str,
        after_path: &str,
    ) -> Result<Option<(String, String)>, NamespaceError> {
        let mut stack = vec![(root_node_id.to_string(), root_path.to_string())];
        while let Some((node_id, path)) = stack.pop() {
            let node = self
                .nodes
                .get(&node_id)
                .ok_or_else(|| self.missing_node_error(&node_id))?;
            if node.kind == NamespaceEntryKind::File
                && !path.is_empty()
                && path.starts_with(prefix)
                && path.as_str() > after_path
            {
                return Ok(Some((node_id, path)));
            }

            let mut children = self.sorted_child_ids(&node_id);
            children.reverse();
            for child_id in children {
                let child = self
                    .nodes
                    .get(&child_id)
                    .ok_or_else(|| self.missing_node_error(&child_id))?;
                let child_path = join_visible_path(&path, &child.name);
                if prefix.is_empty() || subtrees_intersect(&child_path, prefix) {
                    stack.push((child_id, child_path));
                }
            }
        }
        Ok(None)
    }

    fn first_file_in_subtree(
        &self,
        root_node_id: &str,
        root_path: &str,
    ) -> Result<Option<(String, String)>, NamespaceError> {
        let mut stack = vec![(root_node_id.to_string(), root_path.to_string())];
        while let Some((node_id, path)) = stack.pop() {
            let node = self
                .nodes
                .get(&node_id)
                .ok_or_else(|| self.missing_node_error(&node_id))?;
            if node.kind == NamespaceEntryKind::File {
                return Ok(Some((node_id, path)));
            }

            let mut children = self.sorted_child_ids(&node_id);
            children.reverse();
            for child_id in children {
                let child = self
                    .nodes
                    .get(&child_id)
                    .ok_or_else(|| self.missing_node_error(&child_id))?;
                stack.push((child_id, join_visible_path(&path, &child.name)));
            }
        }
        Ok(None)
    }

    fn next_live_file_after(
        &self,
        root_node_id: &str,
        current_node_id: &str,
        current_path: &str,
    ) -> Result<Option<(String, String)>, NamespaceError> {
        if current_node_id == root_node_id {
            return Ok(None);
        }

        let mut cursor_id = current_node_id.to_string();
        let mut cursor_path = current_path.to_string();
        loop {
            let cursor = self
                .nodes
                .get(&cursor_id)
                .ok_or_else(|| self.missing_node_error(&cursor_id))?;
            let Some(parent_id) = cursor.parent_node_id.clone() else {
                return Ok(None);
            };
            let parent_path = parent_path(&cursor_path);
            if let Some((sibling_id, sibling_name)) =
                self.next_child_after(&parent_id, &cursor.name)?
            {
                let sibling_path = join_visible_path(&parent_path, &sibling_name);
                if let Some(next_file) = self.first_file_in_subtree(&sibling_id, &sibling_path)? {
                    return Ok(Some(next_file));
                }
            }
            if parent_id == root_node_id {
                return Ok(None);
            }
            cursor_id = parent_id;
            cursor_path = parent_path;
        }
    }

    fn visible_entries_under_prefix(
        &self,
        scan_trash: bool,
        prefix: &str,
    ) -> Result<Vec<NamespaceEntry>, NamespaceError> {
        if scan_trash {
            return self.collect_trashed_entries_under_prefix(prefix);
        }
        self.collect_live_entries_under_prefix(prefix)
    }

    fn collect_live_entries_under_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<NamespaceEntry>, NamespaceError> {
        let normalized = normalize_path(prefix)?;
        let mut entries = Vec::new();

        let (start_node_id, start_path) = self.find_live_traversal_start(&normalized)?;
        self.collect_visible_subtree_entries(
            &start_node_id,
            &start_path,
            LifecycleState::live(),
            &normalized,
            &mut entries,
        )?;
        Ok(entries)
    }

    fn collect_trashed_entries_under_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<NamespaceEntry>, NamespaceError> {
        let normalized = normalize_path(prefix)?;
        let mut entries = Vec::new();
        for (anchor_path, anchor_id) in &self.trash_anchor_index {
            if !subtrees_intersect(anchor_path, &normalized) {
                continue;
            }
            let lifecycle = self.trashed_lifecycle_for_node(anchor_id)?;
            self.collect_visible_subtree_entries(
                anchor_id,
                anchor_path,
                lifecycle,
                &normalized,
                &mut entries,
            )?;
        }
        Ok(entries)
    }

    fn collect_visible_subtree_entries(
        &self,
        root_node_id: &str,
        root_path: &str,
        lifecycle: LifecycleState,
        prefix_filter: &str,
        out: &mut Vec<NamespaceEntry>,
    ) -> Result<(), NamespaceError> {
        let mut stack = vec![(root_node_id.to_string(), root_path.to_string())];
        while let Some((node_id, path)) = stack.pop() {
            let node = self
                .nodes
                .get(&node_id)
                .ok_or_else(|| self.missing_node_error(&node_id))?;
            if !path.is_empty() && (prefix_filter.is_empty() || path.starts_with(prefix_filter)) {
                out.push(NamespaceEntry {
                    fragment: self.fragment.clone(),
                    parent_path: parent_path(&path),
                    name: base_name(&path),
                    path: path.clone(),
                    kind: node.kind,
                    object_id: node.object_id.clone(),
                    lifecycle: lifecycle.clone(),
                    created_at_unix: node.created_at_unix,
                    updated_at_unix: node.updated_at_unix,
                });
            }

            let mut children = self.sorted_child_ids(&node_id);
            children.reverse();
            for child_id in children {
                let child = self
                    .nodes
                    .get(&child_id)
                    .ok_or_else(|| self.missing_node_error(&child_id))?;
                let child_path = join_visible_path(&path, &child.name);
                if prefix_filter.is_empty() || subtrees_intersect(&child_path, prefix_filter) {
                    stack.push((child_id, child_path));
                }
            }
        }
        Ok(())
    }

    fn find_live_traversal_start(&self, prefix: &str) -> Result<(String, String), NamespaceError> {
        if prefix.is_empty() {
            return Ok((self.live_root_node_id.clone(), String::new()));
        }
        if let Some(exact) = self.resolve_live_node_id(prefix)? {
            return Ok((exact, prefix.to_string()));
        }

        let mut current = self.live_root_node_id.clone();
        let mut current_path = String::new();
        for segment in prefix.split('/') {
            let Some(child_id) = self.lookup_child_id(&current, segment)? else {
                break;
            };
            current = child_id;
            current_path = join_visible_path(&current_path, segment);
        }
        Ok((current, current_path))
    }

    fn node_view(
        &self,
        node_id: &str,
        path: &str,
        lifecycle: LifecycleState,
    ) -> Result<NamespaceEntry, NamespaceError> {
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| self.missing_node_error(node_id))?;
        Ok(NamespaceEntry {
            fragment: self.fragment.clone(),
            path: path.to_string(),
            parent_path: parent_path(path),
            name: base_name(path),
            kind: node.kind,
            object_id: node.object_id.clone(),
            lifecycle,
            created_at_unix: node.created_at_unix,
            updated_at_unix: node.updated_at_unix,
        })
    }

    fn resolve_live_node_id(&self, path: &str) -> Result<Option<String>, NamespaceError> {
        let normalized = normalize_path(path)?;
        if normalized.is_empty() {
            return Ok(Some(self.live_root_node_id.clone()));
        }
        self.resolve_node_id_from_root(&self.live_root_node_id, &normalized)
    }

    fn resolve_trashed_node_id(&self, path: &str) -> Result<Option<String>, NamespaceError> {
        let normalized = normalize_path(path)?;
        if let Some(anchor_id) = self.trash_anchor_index.get(&normalized) {
            return Ok(Some(anchor_id.clone()));
        }

        let mut current = normalized.clone();
        loop {
            if let Some(anchor_id) = self.trash_anchor_index.get(&current) {
                let suffix = normalized
                    .strip_prefix(&current)
                    .unwrap_or_default()
                    .trim_start_matches('/');
                if suffix.is_empty() {
                    return Ok(Some(anchor_id.clone()));
                }
                return self.resolve_node_id_from_root(anchor_id, suffix);
            }
            if current.is_empty() {
                break;
            }
            current = parent_path(&current);
        }
        Ok(None)
    }

    fn resolve_trashed_node_id_by_visible_path(
        &self,
        path: &str,
    ) -> Result<Option<String>, NamespaceError> {
        let normalized = normalize_path(path)?;
        self.resolve_trashed_node_id(&normalized)
    }

    fn resolve_node_id_from_root(
        &self,
        root_id: &str,
        path: &str,
    ) -> Result<Option<String>, NamespaceError> {
        let normalized = normalize_path(path)?;
        if normalized.is_empty() {
            return Ok(Some(root_id.to_string()));
        }

        let mut current = root_id.to_string();
        for segment in normalized.split('/') {
            let Some(child_id) = self.lookup_child_id(&current, segment)? else {
                return Ok(None);
            };
            current = child_id;
        }
        Ok(Some(current))
    }

    fn preflight_recover_destination(&self, path: &str) -> Result<(), NamespaceError> {
        let normalized = normalize_path(path)?;
        let parent = parent_path(&normalized);
        self.preflight_live_directory_chain(&parent)?;
        if self.resolve_live_node_id(&normalized)?.is_some() {
            return Err(NamespaceError::Conflict(format!(
                "destination path '{normalized}' already exists"
            )));
        }
        Ok(())
    }

    fn preflight_live_directory_chain(&self, path: &str) -> Result<(), NamespaceError> {
        let normalized = normalize_path(path)?;
        if normalized.is_empty() {
            return Ok(());
        }

        let mut current = self.live_root_node_id.clone();
        for segment in normalized.split('/') {
            let Some(child_id) = self.lookup_child_id(&current, segment)? else {
                return Ok(());
            };
            let child = self
                .nodes
                .get(&child_id)
                .ok_or_else(|| self.missing_node_error(&child_id))?;
            if child.kind != NamespaceEntryKind::Directory {
                return Err(NamespaceError::Conflict(format!(
                    "parent path '{}' exists as a file",
                    normalized
                )));
            }
            current = child_id;
        }
        Ok(())
    }

    fn ensure_live_parent(
        &mut self,
        path: &str,
        changed_at_unix: i64,
    ) -> Result<(String, String), NamespaceError> {
        let parent = parent_path(path);
        let parent_node_id = self.ensure_live_directory(&parent, changed_at_unix)?;
        Ok((parent_node_id, base_name(path)))
    }

    fn ensure_live_directory(
        &mut self,
        path: &str,
        changed_at_unix: i64,
    ) -> Result<String, NamespaceError> {
        let normalized = normalize_path(path)?;
        if normalized.is_empty() {
            return Ok(self.live_root_node_id.clone());
        }
        let mut current = self.live_root_node_id.clone();
        for segment in normalized.split('/') {
            if let Some(child_id) = self.lookup_child_id(&current, segment)? {
                let child = self
                    .nodes
                    .get(&child_id)
                    .ok_or_else(|| self.missing_node_error(&child_id))?;
                if child.kind != NamespaceEntryKind::Directory {
                    return Err(NamespaceError::Conflict(format!(
                        "parent path '{}' exists as a file",
                        normalized
                    )));
                }
                current = child_id;
                continue;
            }

            let node = NamespaceNode {
                fragment: self.fragment.clone(),
                node_id: Ulid::new().to_string(),
                parent_node_id: Some(current.clone()),
                name: segment.to_string(),
                kind: NamespaceEntryKind::Directory,
                object_id: None,
                head_version_id: None,
                trashed_at_unix: None,
                restore_parent_path: None,
                restore_name: None,
                created_at_unix: changed_at_unix,
                updated_at_unix: changed_at_unix,
            };
            let node_id = node.node_id.clone();
            self.insert_node(node)?;
            current = node_id;
        }
        Ok(current)
    }

    fn insert_node(&mut self, node: NamespaceNode) -> Result<(), NamespaceError> {
        let parent_id = node.parent_node_id.clone();
        let node_id = node.node_id.clone();
        let path_delta = 1usize;
        let object_delta = if node.kind == NamespaceEntryKind::File {
            1usize
        } else {
            0usize
        };
        if let Some(parent_id) = node.parent_node_id.as_ref() {
            let child_key = child_index_key(parent_id, &node.name);
            if self.child_index.contains_key(&child_key) {
                return Err(NamespaceError::Conflict(format!(
                    "sibling '{}' already exists under parent '{}'",
                    node.name, parent_id
                )));
            }
            self.child_index.insert(child_key, node.node_id.clone());
        }
        if let Some(object_id) = node.object_id.clone() {
            self.object_nodes.insert(object_id, node.node_id.clone());
        }
        self.nodes.insert(node_id.clone(), node);
        self.subtree_path_counts.insert(node_id.clone(), path_delta);
        self.subtree_object_counts
            .insert(node_id.clone(), object_delta);
        if let Some(parent_id) = parent_id {
            self.bump_ancestor_counts(&parent_id, path_delta as isize, object_delta as isize)?;
        }
        Ok(())
    }

    fn rebind_node(
        &mut self,
        node_id: &str,
        new_parent_id: &str,
        new_name: &str,
        changed_at_unix: i64,
    ) -> Result<(), NamespaceError> {
        let (old_parent_id, old_name) = {
            let node = self
                .nodes
                .get(node_id)
                .ok_or_else(|| self.missing_node_error(node_id))?;
            (
                node.parent_node_id.clone().ok_or_else(|| {
                    NamespaceError::Invariant("root node cannot be rebound".to_string())
                })?,
                node.name.clone(),
            )
        };
        let (path_delta, object_delta) = self.subtree_counts(node_id)?;

        self.child_index
            .remove(&child_index_key(&old_parent_id, &old_name));
        let new_key = child_index_key(new_parent_id, new_name);
        if self
            .child_index
            .get(&new_key)
            .is_some_and(|existing| existing != node_id)
        {
            self.child_index.insert(
                child_index_key(&old_parent_id, &old_name),
                node_id.to_string(),
            );
            return Err(NamespaceError::Conflict(format!(
                "destination name '{}' already exists under target parent",
                new_name
            )));
        }
        self.child_index.insert(new_key, node_id.to_string());

        let Some(node) = self.nodes.get_mut(node_id) else {
            return Err(self.missing_node_error(node_id));
        };
        node.parent_node_id = Some(new_parent_id.to_string());
        node.name = new_name.to_string();
        node.updated_at_unix = changed_at_unix;
        if old_parent_id != new_parent_id {
            self.bump_ancestor_counts(
                &old_parent_id,
                -(path_delta as isize),
                -(object_delta as isize),
            )?;
            self.bump_ancestor_counts(new_parent_id, path_delta as isize, object_delta as isize)?;
        }
        Ok(())
    }

    fn remove_subtree(&mut self, root_node_id: &str) -> Result<(), NamespaceError> {
        let root_parent_id = self
            .nodes
            .get(root_node_id)
            .ok_or_else(|| self.missing_node_error(root_node_id))?
            .parent_node_id
            .clone();
        let (path_delta, object_delta) = self.subtree_counts(root_node_id)?;
        if let Some(parent_id) = root_parent_id {
            self.bump_ancestor_counts(
                &parent_id,
                -(path_delta as isize),
                -(object_delta as isize),
            )?;
        }
        let subtree = self.collect_subtree_node_ids(root_node_id)?;
        for node_id in &subtree {
            self.clear_anchor_for_node(node_id);
            self.subtree_path_counts.remove(node_id);
            self.subtree_object_counts.remove(node_id);
        }
        for node_id in subtree.iter().rev() {
            let node = self
                .nodes
                .remove(node_id)
                .ok_or_else(|| self.missing_node_error(node_id))?;
            if let Some(parent_id) = node.parent_node_id.as_ref() {
                self.child_index
                    .remove(&child_index_key(parent_id, &node.name));
            }
            if let Some(object_id) = node.object_id.as_ref() {
                self.object_nodes.remove(object_id);
            }
        }
        Ok(())
    }

    fn collect_subtree_node_ids(&self, root_node_id: &str) -> Result<Vec<String>, NamespaceError> {
        if !self.nodes.contains_key(root_node_id) {
            return Err(self.missing_node_error(root_node_id));
        }
        let mut out = Vec::new();
        let mut stack = vec![root_node_id.to_string()];
        while let Some(node_id) = stack.pop() {
            out.push(node_id.clone());
            let mut children = self.sorted_child_ids(&node_id);
            children.reverse();
            for child in children {
                stack.push(child);
            }
        }
        Ok(out)
    }

    fn sorted_child_ids(&self, parent_id: &str) -> Vec<String> {
        let prefix = format!("{parent_id}\u{0000}");
        self.child_index
            .range(prefix.clone()..)
            .take_while(|(key, _)| key.starts_with(&prefix))
            .map(|(_, child_id)| child_id.clone())
            .collect()
    }

    fn lookup_child_id(
        &self,
        parent_id: &str,
        name: &str,
    ) -> Result<Option<String>, NamespaceError> {
        Ok(self
            .child_index
            .get(&child_index_key(parent_id, name))
            .cloned())
    }

    fn next_child_after(
        &self,
        parent_id: &str,
        current_name: &str,
    ) -> Result<Option<(String, String)>, NamespaceError> {
        let prefix = format!("{parent_id}\u{0000}");
        let start = child_index_key(parent_id, current_name);
        for (key, child_id) in self.child_index.range((Excluded(start), Unbounded)) {
            if !key.starts_with(&prefix) {
                break;
            }
            let name = key[prefix.len()..].to_string();
            return Ok(Some((child_id.clone(), name)));
        }
        Ok(None)
    }

    fn is_under_root(&self, node_id: &str, root_id: &str) -> Result<bool, NamespaceError> {
        if node_id == root_id {
            return Ok(true);
        }
        let mut current = node_id.to_string();
        loop {
            let node = self
                .nodes
                .get(&current)
                .ok_or_else(|| self.missing_node_error(&current))?;
            let Some(parent_id) = node.parent_node_id.as_ref() else {
                return Ok(false);
            };
            if parent_id == root_id {
                return Ok(true);
            }
            current = parent_id.clone();
        }
    }

    fn trash_anchor_for_node(&self, node_id: &str) -> Result<Option<String>, NamespaceError> {
        if !self.is_under_root(node_id, &self.trash_root_node_id)? {
            return Ok(None);
        }
        let mut current = node_id.to_string();
        loop {
            let node = self
                .nodes
                .get(&current)
                .ok_or_else(|| self.missing_node_error(&current))?;
            if node.trashed_at_unix.is_some() {
                return Ok(Some(current));
            }
            let Some(parent_id) = node.parent_node_id.clone() else {
                return Ok(None);
            };
            current = parent_id;
        }
    }

    fn lifecycle_for_node(&self, node_id: &str) -> Result<LifecycleState, NamespaceError> {
        if self.is_under_root(node_id, &self.trash_root_node_id)? {
            return self.trashed_lifecycle_for_node(node_id);
        }
        Ok(LifecycleState::live())
    }

    fn trashed_lifecycle_for_node(&self, node_id: &str) -> Result<LifecycleState, NamespaceError> {
        let anchor_id = self
            .trash_anchor_for_node(node_id)?
            .ok_or_else(|| self.missing_node_error(node_id))?;
        let anchor = self
            .nodes
            .get(&anchor_id)
            .ok_or_else(|| self.missing_node_error(&anchor_id))?;
        Ok(LifecycleState::trashed(
            anchor.trashed_at_unix.unwrap_or(anchor.updated_at_unix),
        ))
    }

    fn is_ancestor(&self, ancestor_id: &str, node_id: &str) -> Result<bool, NamespaceError> {
        if ancestor_id == node_id {
            return Ok(true);
        }
        let mut current = node_id.to_string();
        loop {
            let node = self
                .nodes
                .get(&current)
                .ok_or_else(|| self.missing_node_error(&current))?;
            let Some(parent_id) = node.parent_node_id.as_ref() else {
                return Ok(false);
            };
            if parent_id == ancestor_id {
                return Ok(true);
            }
            current = parent_id.clone();
        }
    }

    fn cleanup_empty_trash_ancestors(&mut self, path: &str) -> Result<(), NamespaceError> {
        let normalized = normalize_path(path)?;
        let mut maybe_node_id = self.resolve_trashed_node_id_by_visible_path(&normalized)?;
        while let Some(node_id) = maybe_node_id {
            let Some(parent_id) = self
                .nodes
                .get(&node_id)
                .ok_or_else(|| self.missing_node_error(&node_id))?
                .parent_node_id
                .clone()
            else {
                break;
            };
            if parent_id == self.trash_root_node_id {
                break;
            }
            let has_children = !self.sorted_child_ids(&parent_id).is_empty();
            let keep = self
                .nodes
                .get(&parent_id)
                .ok_or_else(|| self.missing_node_error(&parent_id))?
                .trashed_at_unix
                .is_some();
            if has_children || keep {
                break;
            }
            self.bump_ancestor_counts(&parent_id, -1, 0)?;
            self.clear_anchor_for_node(&parent_id);
            self.subtree_path_counts.remove(&parent_id);
            self.subtree_object_counts.remove(&parent_id);
            let parent_node = self
                .nodes
                .remove(&parent_id)
                .ok_or_else(|| self.missing_node_error(&parent_id))?;
            if let Some(grand_parent_id) = parent_node.parent_node_id.as_ref() {
                self.child_index
                    .remove(&child_index_key(grand_parent_id, &parent_node.name));
            }
            maybe_node_id = parent_node.parent_node_id;
        }
        Ok(())
    }

    fn trash_anchor_ids(&self) -> Vec<String> {
        self.sorted_child_ids(&self.trash_root_node_id)
    }

    fn missing_node_error(&self, node_id: &str) -> NamespaceError {
        NamespaceError::Invariant(format!("namespace node '{}' is missing", node_id))
    }

    fn rebuild_subtree_stats(&mut self) -> Result<(), NamespaceError> {
        for root_id in [
            self.live_root_node_id.clone(),
            self.trash_root_node_id.clone(),
        ] {
            let mut stack = vec![(root_id, false)];
            while let Some((node_id, visited)) = stack.pop() {
                if visited {
                    let node = self
                        .nodes
                        .get(&node_id)
                        .ok_or_else(|| self.missing_node_error(&node_id))?;
                    let mut path_count: usize = if node_id == self.live_root_node_id
                        || node_id == self.trash_root_node_id
                    {
                        0
                    } else {
                        1
                    };
                    let mut object_count: usize = if node.kind == NamespaceEntryKind::File {
                        1
                    } else {
                        0
                    };
                    for child_id in self.sorted_child_ids(&node_id) {
                        path_count = path_count
                            .saturating_add(*self.subtree_path_counts.get(&child_id).unwrap_or(&0));
                        object_count = object_count.saturating_add(
                            *self.subtree_object_counts.get(&child_id).unwrap_or(&0),
                        );
                    }
                    self.subtree_path_counts.insert(node_id.clone(), path_count);
                    self.subtree_object_counts
                        .insert(node_id.clone(), object_count);
                    continue;
                }

                stack.push((node_id.clone(), true));
                let mut children = self.sorted_child_ids(&node_id);
                children.reverse();
                for child_id in children {
                    stack.push((child_id, false));
                }
            }
        }
        Ok(())
    }

    fn subtree_counts(&self, node_id: &str) -> Result<(usize, usize), NamespaceError> {
        if !self.nodes.contains_key(node_id) {
            return Err(self.missing_node_error(node_id));
        }
        Ok((
            *self.subtree_path_counts.get(node_id).unwrap_or(&0),
            *self.subtree_object_counts.get(node_id).unwrap_or(&0),
        ))
    }

    fn bump_ancestor_counts(
        &mut self,
        start_node_id: &str,
        path_delta: isize,
        object_delta: isize,
    ) -> Result<(), NamespaceError> {
        let mut current = Some(start_node_id.to_string());
        while let Some(node_id) = current {
            let path_entry = self.subtree_path_counts.entry(node_id.clone()).or_insert(0);
            *path_entry = apply_delta(*path_entry, path_delta)?;
            let object_entry = self
                .subtree_object_counts
                .entry(node_id.clone())
                .or_insert(0);
            *object_entry = apply_delta(*object_entry, object_delta)?;
            current = self
                .nodes
                .get(&node_id)
                .ok_or_else(|| self.missing_node_error(&node_id))?
                .parent_node_id
                .clone();
        }
        Ok(())
    }

    fn clear_anchor_for_node(&mut self, node_id: &str) {
        let anchor_paths = self
            .trash_anchor_index
            .iter()
            .filter(|(_, anchor_id)| *anchor_id == node_id)
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        for path in anchor_paths {
            self.trash_anchor_index.remove(&path);
        }
    }

    fn upsert_virtual_trash_directory(
        out: &mut BTreeMap<String, NamespaceListEntry>,
        path: &str,
        lifecycle: LifecycleState,
        created_at_unix: i64,
        updated_at_unix: i64,
    ) {
        use std::collections::btree_map::Entry;

        match out.entry(path.to_string()) {
            Entry::Occupied(mut existing) => {
                let value = existing.get_mut();
                value.created_at_unix = value.created_at_unix.min(created_at_unix);
                value.updated_at_unix = value.updated_at_unix.max(updated_at_unix);
            }
            Entry::Vacant(slot) => {
                slot.insert(NamespaceListEntry {
                    name: base_name(path),
                    path: path.to_string(),
                    kind: NamespaceEntryKind::Directory,
                    lifecycle,
                    object_id: None,
                    created_at_unix,
                    updated_at_unix,
                });
            }
        }
    }

    fn anchor_visible_path(&self, anchor_id: &str) -> Result<String, NamespaceError> {
        let anchor = self
            .nodes
            .get(anchor_id)
            .ok_or_else(|| self.missing_node_error(anchor_id))?;
        Ok(join_visible_path(
            anchor.restore_parent_path.as_deref().unwrap_or_default(),
            anchor.restore_name.as_deref().unwrap_or_default(),
        ))
    }
}

fn child_index_key(parent_id: &str, name: &str) -> String {
    format!("{parent_id}\u{0000}{name}")
}

fn path_in_subtree(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn subtrees_intersect(path: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || path.is_empty()
        || path_in_subtree(path, prefix)
        || path_in_subtree(prefix, path)
}

fn apply_delta(value: usize, delta: isize) -> Result<usize, NamespaceError> {
    if delta >= 0 {
        value.checked_add(delta as usize).ok_or_else(|| {
            NamespaceError::Invariant("namespace subtree counter overflow".to_string())
        })
    } else {
        value.checked_sub(delta.unsigned_abs()).ok_or_else(|| {
            NamespaceError::Invariant("namespace subtree counter underflow".to_string())
        })
    }
}

fn join_visible_path(parent: &str, name: &str) -> String {
    match (
        parent.trim_matches('/').is_empty(),
        name.trim_matches('/').is_empty(),
    ) {
        (true, true) => String::new(),
        (true, false) => name.trim_matches('/').to_string(),
        (false, true) => parent.trim_matches('/').to_string(),
        (false, false) => format!("{}/{}", parent.trim_matches('/'), name.trim_matches('/')),
    }
}
