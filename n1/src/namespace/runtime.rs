use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use axum::http::StatusCode;
use chrono::Utc;
use dashmap::DashMap;
use indexdb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableError};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;
use ulid::Ulid;

use crate::core::disk_manager::{ListObjectsResult, ObjectMetadata, ObjectPathManager};
use crate::core::retrieve::list_objects_direct;
use crate::enums::app_error_enum::AppError;
use crate::namespace::catalog::NamespaceCatalog;
use crate::namespace::models::{
    NamespaceEntry, NamespaceEntryKind, NamespaceError, NamespaceReachabilitySnapshot, ObjectHead,
};
use crate::namespace::paths::normalize_path;

const NAMESPACE_V2_DIR: &str = "namespace_v2";
const NAMESPACE_V2_FILE: &str = "catalog.ndxdb";
const CATALOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("catalog");
const PENDING_UPLOADS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("pending_uploads");
const CATALOG_ROW_KEY: &str = "catalog";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingUploadBinding {
    pub fragment: String,
    pub logical_path: String,
    pub physical_object_id: String,
    pub version_id: String,
    pub session_id: String,
    pub created_at_unix: i64,
}

pub struct NamespaceRuntime {
    catalogs: DashMap<String, Arc<AsyncMutex<NamespaceCatalog>>>,
    dbs: DashMap<PathBuf, Arc<Database>>,
}

static GLOBAL_NAMESPACE_RUNTIME: LazyLock<NamespaceRuntime> = LazyLock::new(NamespaceRuntime::new);

pub fn global_namespace_runtime() -> &'static NamespaceRuntime {
    &GLOBAL_NAMESPACE_RUNTIME
}

impl NamespaceRuntime {
    pub fn new() -> Self {
        Self {
            catalogs: DashMap::new(),
            dbs: DashMap::new(),
        }
    }

    pub fn evict_fragment(&self, fragment: &str) {
        self.catalogs.remove(fragment);
        if let Ok(path) = namespace_db_path(fragment) {
            self.dbs.remove(&path);
        }
    }

    pub async fn list_dir(
        &self,
        fragment: &str,
        path: &str,
        include_trashed: bool,
    ) -> Result<Vec<NamespaceEntry>, AppError> {
        let catalog = self.load_catalog(fragment).await?;
        let guard = catalog.lock().await;
        let entries = guard
            .list_dir(path, include_trashed)
            .map_err(namespace_error_to_app)?;
        Ok(entries
            .into_iter()
            .map(|entry| NamespaceEntry {
                fragment: fragment.to_string(),
                path: entry.path.clone(),
                parent_path: crate::namespace::paths::parent_path(&entry.path),
                name: entry.name,
                kind: entry.kind,
                object_id: entry.object_id,
                lifecycle: entry.lifecycle,
                created_at_unix: entry.created_at_unix,
                updated_at_unix: entry.updated_at_unix,
            })
            .collect())
    }

    pub async fn list_dir_page(
        &self,
        fragment: &str,
        path: &str,
        include_trashed: bool,
        limit: usize,
        continuation_token: Option<&str>,
    ) -> Result<(Vec<NamespaceEntry>, Option<String>), AppError> {
        let catalog = self.load_catalog(fragment).await?;
        let guard = catalog.lock().await;
        let (entries, next_token) = guard
            .list_dir_page(path, include_trashed, limit, continuation_token)
            .map_err(namespace_error_to_app)?;
        Ok((
            entries
                .into_iter()
                .map(|entry| NamespaceEntry {
                    fragment: fragment.to_string(),
                    path: entry.path.clone(),
                    parent_path: crate::namespace::paths::parent_path(&entry.path),
                    name: entry.name,
                    kind: entry.kind,
                    object_id: entry.object_id,
                    lifecycle: entry.lifecycle,
                    created_at_unix: entry.created_at_unix,
                    updated_at_unix: entry.updated_at_unix,
                })
                .collect(),
            next_token,
        ))
    }

    pub async fn entry(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<NamespaceEntry>, AppError> {
        let catalog = self.load_catalog(fragment).await?;
        let guard = catalog.lock().await;
        Ok(guard.entry(path).map_err(namespace_error_to_app)?)
    }

    pub async fn trashed_entry(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<NamespaceEntry>, AppError> {
        let catalog = self.load_catalog(fragment).await?;
        let guard = catalog.lock().await;
        Ok(guard.trashed_entry(path).map_err(namespace_error_to_app)?)
    }

    pub async fn head_for_path(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<Option<ObjectHead>, AppError> {
        let normalized = normalize_path(path).map_err(namespace_error_to_app)?;
        let catalog = self.load_catalog(fragment).await?;
        let guard = catalog.lock().await;
        let Some(entry) = guard.entry(&normalized).map_err(namespace_error_to_app)? else {
            return Ok(None);
        };
        let Some(object_id) = entry.object_id.as_deref() else {
            return Ok(None);
        };
        Ok(guard.object_head(object_id))
    }

    pub async fn mkdir(&self, fragment: &str, path: &str) -> Result<bool, AppError> {
        let changed_at = Utc::now().timestamp();
        let catalog = self.load_catalog(fragment).await?;
        let mut guard = catalog.lock().await;
        let created = guard
            .mkdir(path, changed_at)
            .map_err(namespace_error_to_app)?;
        self.persist_catalog(fragment, &guard).await?;
        Ok(created)
    }

    pub async fn rename(
        &self,
        fragment: &str,
        source_path: &str,
        destination_path: &str,
    ) -> Result<crate::namespace::models::NamespaceMutationSummary, AppError> {
        let changed_at = Utc::now().timestamp();
        let catalog = self.load_catalog(fragment).await?;
        let mut guard = catalog.lock().await;
        let summary = guard
            .rename_path(source_path, destination_path, changed_at)
            .map_err(namespace_error_to_app)?;
        self.persist_catalog(fragment, &guard).await?;
        Ok(summary)
    }

    pub async fn trash(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<crate::namespace::models::NamespaceMutationSummary, AppError> {
        let changed_at = Utc::now().timestamp();
        let catalog = self.load_catalog(fragment).await?;
        let mut guard = catalog.lock().await;
        let summary = guard
            .trash_path(path, changed_at)
            .map_err(namespace_error_to_app)?;
        self.persist_catalog(fragment, &guard).await?;
        Ok(summary)
    }

    pub async fn recover(
        &self,
        fragment: &str,
        path: &str,
    ) -> Result<crate::namespace::models::NamespaceMutationSummary, AppError> {
        let changed_at = Utc::now().timestamp();
        let catalog = self.load_catalog(fragment).await?;
        let mut guard = catalog.lock().await;
        let summary = guard
            .recover_path(path, changed_at)
            .map_err(namespace_error_to_app)?;
        self.persist_catalog(fragment, &guard).await?;
        Ok(summary)
    }

    pub async fn recover_all(
        &self,
        fragment: &str,
    ) -> Result<crate::namespace::models::NamespaceMutationSummary, AppError> {
        let changed_at = Utc::now().timestamp();
        let catalog = self.load_catalog(fragment).await?;
        let mut guard = catalog.lock().await;
        let summary = guard.recover_all(changed_at);
        self.persist_catalog(fragment, &guard).await?;
        Ok(summary)
    }

    pub async fn empty_trash(
        &self,
        fragment: &str,
    ) -> Result<crate::namespace::models::NamespaceMutationSummary, AppError> {
        let changed_at = Utc::now().timestamp();
        let catalog = self.load_catalog(fragment).await?;
        let mut guard = catalog.lock().await;
        let summary = guard.empty_trash(changed_at);
        self.persist_catalog(fragment, &guard).await?;
        Ok(summary)
    }

    pub async fn reachability_snapshot_if_present(
        &self,
        fragment: &str,
    ) -> Result<Option<NamespaceReachabilitySnapshot>, AppError> {
        if let Some(existing) = self.catalogs.get(fragment) {
            let guard = existing.lock().await;
            return Ok(Some(guard.reachability_snapshot()));
        }

        let db = match self.open_existing_db(fragment)? {
            Some(db) => db,
            None => return Ok(None),
        };
        let read_txn = db
            .begin_read()
            .map_err(|e| map_indexdb_error("namespace catalog snapshot begin_read", e))?;
        let table = match read_txn.open_table(CATALOG_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => {
                return Err(map_indexdb_error(
                    "namespace catalog snapshot open_table",
                    err,
                ));
            }
        };
        let Some(value) = table
            .get(CATALOG_ROW_KEY)
            .map_err(|e| map_indexdb_error("namespace catalog snapshot get", e))?
        else {
            return Ok(None);
        };
        let catalog: NamespaceCatalog = deserialize(value.value())?;
        Ok(Some(catalog.reachability_snapshot()))
    }

    pub async fn purge_unreferenced_object_ids(
        &self,
        fragment: &str,
        object_ids: &std::collections::BTreeSet<String>,
    ) -> Result<usize, AppError> {
        if object_ids.is_empty() {
            return Ok(0);
        }
        let catalog = match self.load_catalog_if_present(fragment).await? {
            Some(catalog) => catalog,
            None => return Ok(0),
        };
        let mut guard = catalog.lock().await;
        let removed = guard.purge_unreferenced_object_ids(object_ids);
        if removed > 0 {
            self.persist_catalog(fragment, &guard).await?;
        }
        Ok(removed)
    }

    pub async fn resolve_live_object_id(
        &self,
        fragment: &str,
        logical_path: &str,
    ) -> Result<Option<String>, AppError> {
        let normalized = normalize_path(logical_path).map_err(namespace_error_to_app)?;
        let catalog = self.load_catalog(fragment).await?;
        let guard = catalog.lock().await;
        let Some(entry) = guard.entry(&normalized).map_err(namespace_error_to_app)? else {
            return Ok(None);
        };
        if entry.kind != NamespaceEntryKind::File || !entry.lifecycle.is_live() {
            return Ok(None);
        }
        Ok(entry.object_id.clone())
    }

    pub async fn resolve_any_object_id(
        &self,
        fragment: &str,
        logical_path: &str,
    ) -> Result<Option<String>, AppError> {
        let normalized = normalize_path(logical_path).map_err(namespace_error_to_app)?;
        let catalog = self.load_catalog(fragment).await?;
        let guard = catalog.lock().await;
        Ok(guard
            .entry(&normalized)
            .map_err(namespace_error_to_app)?
            .and_then(|entry| entry.object_id))
    }

    pub async fn select_object_id_for_write(
        &self,
        fragment: &str,
        logical_path: &str,
    ) -> Result<String, AppError> {
        let normalized = normalize_path(logical_path).map_err(namespace_error_to_app)?;
        let catalog = self.load_catalog(fragment).await?;
        let guard = catalog.lock().await;
        if let Some(existing) = guard.entry(&normalized).map_err(namespace_error_to_app)? {
            if existing.kind == NamespaceEntryKind::Directory {
                return Err(AppError::Conflict(format!(
                    "logical path '{}' already exists as a directory",
                    normalized
                )));
            }
            if let Some(object_id) = existing.object_id {
                return Ok(object_id);
            }
        }
        Ok(Ulid::new().to_string())
    }

    pub async fn finalize_logical_upload(
        &self,
        fragment: &str,
        logical_path: &str,
        physical_object_id: &str,
        metadata: &ObjectMetadata,
    ) -> Result<(), AppError> {
        let catalog = self.load_catalog(fragment).await?;
        let mut guard = catalog.lock().await;
        guard
            .put_file(
                logical_path,
                physical_object_id,
                &metadata.version_id,
                physical_object_id,
                &metadata.content_type,
                metadata.total_size_bytes,
                metadata.stripe_size_bytes as u64,
                metadata.created_at,
            )
            .map_err(namespace_error_to_app)?;
        self.persist_catalog(fragment, &guard).await?;
        Ok(())
    }

    pub async fn bind_pending_upload(
        &self,
        fragment: &str,
        logical_path: &str,
        physical_object_id: &str,
        version_id: &str,
        session_id: &str,
    ) -> Result<(), AppError> {
        let binding = PendingUploadBinding {
            fragment: fragment.to_string(),
            logical_path: normalize_path(logical_path).map_err(namespace_error_to_app)?,
            physical_object_id: physical_object_id.to_string(),
            version_id: version_id.to_string(),
            session_id: session_id.to_string(),
            created_at_unix: Utc::now().timestamp(),
        };
        let db = self.open_db(fragment)?;
        let bytes = serialize(&binding)?;
        let write_txn = db
            .begin_write()
            .map_err(|e| map_indexdb_error("namespace pending begin_write", e))?;
        {
            let mut table = write_txn
                .open_table(PENDING_UPLOADS_TABLE)
                .map_err(|e| map_indexdb_error("namespace pending open_table", e))?;
            table
                .insert(
                    pending_row_key(fragment, version_id).as_str(),
                    bytes.as_slice(),
                )
                .map_err(|e| map_indexdb_error("namespace pending insert", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| map_indexdb_error("namespace pending commit", e))?;
        Ok(())
    }

    pub async fn pending_upload(
        &self,
        fragment: &str,
        version_id: &str,
    ) -> Result<Option<PendingUploadBinding>, AppError> {
        let db = self.open_db(fragment)?;
        let read_txn = db
            .begin_read()
            .map_err(|e| map_indexdb_error("namespace pending begin_read", e))?;
        let table = match read_txn.open_table(PENDING_UPLOADS_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(map_indexdb_error("namespace pending open_table", err)),
        };
        table
            .get(pending_row_key(fragment, version_id).as_str())
            .map_err(|e| map_indexdb_error("namespace pending get", e))?
            .map(|value| deserialize(value.value()))
            .transpose()
    }

    pub async fn list_pending_uploads(
        &self,
        fragment: &str,
    ) -> Result<Vec<PendingUploadBinding>, AppError> {
        let db = self.open_db(fragment)?;
        let read_txn = db
            .begin_read()
            .map_err(|e| map_indexdb_error("namespace pending list begin_read", e))?;
        let table = match read_txn.open_table(PENDING_UPLOADS_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(err) => return Err(map_indexdb_error("namespace pending list open_table", err)),
        };

        let mut bindings = Vec::new();
        let iter = table
            .iter()
            .map_err(|e| map_indexdb_error("namespace pending list iter", e))?;
        for entry in iter {
            let (_, value) =
                entry.map_err(|e| map_indexdb_error("namespace pending list next", e))?;
            let binding: PendingUploadBinding = deserialize(value.value())?;
            if binding.fragment == fragment {
                bindings.push(binding);
            }
        }
        Ok(bindings)
    }

    pub async fn find_pending_upload_by_logical_path(
        &self,
        fragment: &str,
        logical_path: &str,
    ) -> Result<Option<PendingUploadBinding>, AppError> {
        let normalized = normalize_path(logical_path).map_err(namespace_error_to_app)?;
        let mut matches = self
            .list_pending_uploads(fragment)
            .await?
            .into_iter()
            .filter(|binding| binding.logical_path == normalized)
            .collect::<Vec<_>>();
        matches.sort_by(|a, b| b.created_at_unix.cmp(&a.created_at_unix));
        Ok(matches.into_iter().next())
    }

    pub async fn clear_pending_upload(
        &self,
        fragment: &str,
        version_id: &str,
    ) -> Result<(), AppError> {
        let db = self.open_db(fragment)?;
        let write_txn = db
            .begin_write()
            .map_err(|e| map_indexdb_error("namespace pending clear begin_write", e))?;
        {
            let mut table = match write_txn.open_table(PENDING_UPLOADS_TABLE) {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => return Ok(()),
                Err(err) => {
                    return Err(map_indexdb_error("namespace pending clear open_table", err));
                }
            };
            let _ = table
                .remove(pending_row_key(fragment, version_id).as_str())
                .map_err(|e| map_indexdb_error("namespace pending clear remove", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| map_indexdb_error("namespace pending clear commit", e))?;
        Ok(())
    }

    pub async fn list_objects_page(
        &self,
        fragment: &str,
        params: &HashMap<String, String>,
        scan_trash: bool,
    ) -> Result<ListObjectsResult, AppError> {
        let limit = params
            .get("limit")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100)
            .min(1000);
        let prefix = params.get("prefix").map(String::as_str);
        let delimiter = params.get("delimiter").map(String::as_str);
        let continuation_token = params.get("continuation_token").map(String::as_str);

        let catalog = self.load_catalog(fragment).await?;
        let guard = catalog.lock().await;
        let page = guard
            .list_page(prefix, delimiter, limit, continuation_token, scan_trash)
            .map_err(namespace_error_to_app)?;

        let mut objects = Vec::with_capacity(page.files.len());
        for file in page.files {
            let head = guard.object_head(&file.object_id).ok_or_else(|| {
                AppError::InternalServerError(format!(
                    "namespace head for '{}' is missing object id '{}'",
                    file.path, file.object_id
                ))
            })?;
            let version = guard.current_version(&file.object_id).ok_or_else(|| {
                AppError::InternalServerError(format!(
                    "namespace version for '{}' is missing current version '{}'",
                    file.path, head.current_version_id
                ))
            })?;
            objects.push(ObjectMetadata {
                object_key: file.path,
                fragment: fragment.to_string(),
                version_id: head.current_version_id.clone(),
                upload_session_id: String::new(),
                owner_uid: fragment.to_string(),
                content_type: head.content_type.clone(),
                total_size_bytes: head.total_size_bytes,
                created_at: version.created_at_unix,
                parts: Vec::new(),
                is_delete_marker: false,
                stripe_size_bytes: version.stripe_size_bytes as usize,
            });
        }

        Ok(ListObjectsResult {
            objects,
            common_prefixes: page.common_prefixes,
            continuation_token: page.continuation_token,
        })
    }

    fn open_db(&self, fragment: &str) -> Result<Arc<Database>, AppError> {
        let path = namespace_db_path(fragment)?;
        if let Some(existing) = self.dbs.get(&path) {
            return Ok(existing.clone());
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;
        }
        let db = Arc::new(
            Database::create(&path)
                .map_err(|e| map_indexdb_error(&format!("open {}", path.display()), e))?,
        );
        self.dbs.insert(path, db.clone());
        Ok(db)
    }

    fn open_existing_db(&self, fragment: &str) -> Result<Option<Arc<Database>>, AppError> {
        let path = namespace_db_path(fragment)?;
        if let Some(existing) = self.dbs.get(&path) {
            return Ok(Some(existing.clone()));
        }
        if !path.exists() {
            return Ok(None);
        }
        let db = Arc::new(
            Database::create(&path)
                .map_err(|e| map_indexdb_error(&format!("open {}", path.display()), e))?,
        );
        self.dbs.insert(path, db.clone());
        Ok(Some(db))
    }

    async fn load_catalog(
        &self,
        fragment: &str,
    ) -> Result<Arc<AsyncMutex<NamespaceCatalog>>, AppError> {
        if let Some(existing) = self.catalogs.get(fragment) {
            if namespace_db_path(fragment)?.exists() {
                return Ok(existing.clone());
            }
            drop(existing);
            self.evict_fragment(fragment);
        }
        let catalog = self.read_or_import_catalog(fragment).await?;
        let wrapped = Arc::new(AsyncMutex::new(catalog));
        let entry = self
            .catalogs
            .entry(fragment.to_string())
            .or_insert_with(|| wrapped.clone());
        Ok(entry.clone())
    }

    async fn load_catalog_if_present(
        &self,
        fragment: &str,
    ) -> Result<Option<Arc<AsyncMutex<NamespaceCatalog>>>, AppError> {
        if let Some(existing) = self.catalogs.get(fragment) {
            return Ok(Some(existing.clone()));
        }
        let db = match self.open_existing_db(fragment)? {
            Some(db) => db,
            None => return Ok(None),
        };
        let read_txn = db
            .begin_read()
            .map_err(|e| map_indexdb_error("namespace catalog present begin_read", e))?;
        let table = match read_txn.open_table(CATALOG_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => {
                return Err(map_indexdb_error(
                    "namespace catalog present open_table",
                    err,
                ));
            }
        };
        let Some(value) = table
            .get(CATALOG_ROW_KEY)
            .map_err(|e| map_indexdb_error("namespace catalog present get", e))?
        else {
            return Ok(None);
        };
        let mut catalog: NamespaceCatalog = deserialize(value.value())?;
        catalog
            .rebuild_secondary_indices()
            .map_err(namespace_error_to_app)?;
        let wrapped = Arc::new(AsyncMutex::new(catalog));
        let entry = self
            .catalogs
            .entry(fragment.to_string())
            .or_insert_with(|| wrapped.clone());
        Ok(Some(entry.clone()))
    }

    async fn read_or_import_catalog(&self, fragment: &str) -> Result<NamespaceCatalog, AppError> {
        let db = self.open_db(fragment)?;
        let read_txn = db
            .begin_read()
            .map_err(|e| map_indexdb_error("namespace catalog begin_read", e))?;
        let table = match read_txn.open_table(CATALOG_TABLE) {
            Ok(table) => Some(table),
            Err(TableError::TableDoesNotExist(_)) => None,
            Err(err) => return Err(map_indexdb_error("namespace catalog open_table", err)),
        };
        if let Some(table) = table {
            if let Some(value) = table
                .get(CATALOG_ROW_KEY)
                .map_err(|e| map_indexdb_error("namespace catalog get", e))?
            {
                let mut catalog: NamespaceCatalog = deserialize(value.value())?;
                catalog
                    .rebuild_secondary_indices()
                    .map_err(namespace_error_to_app)?;
                return Ok(catalog);
            }
        }

        let catalog = import_from_raw_object_index(fragment).await?;
        self.persist_catalog(fragment, &catalog).await?;
        Ok(catalog)
    }

    async fn persist_catalog(
        &self,
        fragment: &str,
        catalog: &NamespaceCatalog,
    ) -> Result<(), AppError> {
        let db = self.open_db(fragment)?;
        let bytes = serialize(catalog)?;
        let write_txn = db
            .begin_write()
            .map_err(|e| map_indexdb_error("namespace catalog begin_write", e))?;
        {
            let mut table = write_txn
                .open_table(CATALOG_TABLE)
                .map_err(|e| map_indexdb_error("namespace catalog open_table", e))?;
            table
                .insert(CATALOG_ROW_KEY, bytes.as_slice())
                .map_err(|e| map_indexdb_error("namespace catalog insert", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| map_indexdb_error("namespace catalog commit", e))?;
        Ok(())
    }
}

async fn import_from_raw_object_index(fragment: &str) -> Result<NamespaceCatalog, AppError> {
    let mut catalog = NamespaceCatalog::new(fragment.to_string());
    let mut continuation = None;
    loop {
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "1000".to_string());
        params.insert("include_internal".to_string(), "true".to_string());
        if let Some(token) = continuation.clone() {
            params.insert("continuation_token".to_string(), token);
        }
        let page = list_objects_direct(fragment, params, false).await?;
        continuation = page.continuation_token.clone();
        import_page_into_catalog(&mut catalog, page)?;
        if continuation.is_none() {
            break;
        }
    }
    Ok(catalog)
}

fn import_page_into_catalog(
    catalog: &mut NamespaceCatalog,
    page: ListObjectsResult,
) -> Result<(), AppError> {
    for object in page.objects {
        catalog
            .put_file(
                &object.object_key,
                &object.object_key,
                &object.version_id,
                &object.object_key,
                &object.content_type,
                object.total_size_bytes,
                object.stripe_size_bytes as u64,
                object.created_at,
            )
            .map_err(namespace_error_to_app)?;
    }
    Ok(())
}

fn namespace_db_path(fragment: &str) -> Result<PathBuf, AppError> {
    let manager = ObjectPathManager;
    let roots = manager.get_all_index_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "no index roots configured for namespace v2".to_string(),
        ));
    }
    let owner_idx = namespace_owner_index(fragment, roots.len());
    Ok(roots[owner_idx]
        .join(fragment)
        .join(NAMESPACE_V2_DIR)
        .join(NAMESPACE_V2_FILE))
}

fn namespace_owner_index(fragment: &str, root_count: usize) -> usize {
    if root_count <= 1 {
        return 0;
    }
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"namespace-v2");
    hasher.update([0u8]);
    hasher.update(fragment.as_bytes());
    let hash = hasher.finalize();
    usize::from(u16::from(hash[0]) << 8 | u16::from(hash[1])) % root_count
}

fn pending_row_key(fragment: &str, version_id: &str) -> String {
    format!("{fragment}\u{0000}{version_id}")
}

fn namespace_error_to_app(error: NamespaceError) -> AppError {
    match error {
        NamespaceError::InvalidPath(message) => AppError::BadRequest(message),
        NamespaceError::Conflict(message) => AppError::Conflict(message),
        NamespaceError::NotFound(_) => AppError::NotFound,
        NamespaceError::Invariant(message) => AppError::InternalServerError(message),
    }
}

fn map_indexdb_error(context: &str, error: impl std::fmt::Display) -> AppError {
    AppError::InternalServerError(format!("{context}: {error}"))
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, AppError> {
    rmp_serde::to_vec(value)
        .map_err(|e| AppError::InternalServerError(format!("namespace serialize: {e}")))
}

fn deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, AppError> {
    rmp_serde::from_slice(bytes)
        .map_err(|e| AppError::InternalServerError(format!("namespace deserialize: {e}")))
}

pub fn logical_not_found() -> AppError {
    AppError::NotFound
}

pub fn unexpected_response(status: StatusCode, context: &str) -> AppError {
    AppError::InternalServerError(format!("{context} returned unexpected status {status}"))
}
