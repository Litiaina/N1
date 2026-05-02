use crate::error::{
    CommitError, CompactionError, DatabaseError, StorageError, TableError, TransactionError,
};
use crate::ffi::{Connection, OpenMode, StepResult};
use crate::types::{Key, MutInPlaceValue, Value};
use std::borrow::Borrow;
use std::collections::VecDeque;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

pub trait TableHandle {
    fn name(&self) -> &str;
}

#[derive(Clone)]
pub struct UntypedTableHandle {
    name: String,
}

impl UntypedTableHandle {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }
}

impl TableHandle for UntypedTableHandle {
    fn name(&self) -> &str {
        &self.name
    }
}

pub struct TableDefinition<'a, K: Key + 'static, V: Value + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: Key + 'static, V: Value + 'static> TableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for TableDefinition<'_, K, V> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: Key + 'static, V: Value + 'static> Clone for TableDefinition<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: Key + 'static, V: Value + 'static> Copy for TableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Value + 'static> Display for TableDefinition<'_, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}<{}, {}>",
            self.name,
            K::type_name().name(),
            V::type_name().name()
        )
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    evictions: u64,
    read_hits: u64,
    read_misses: u64,
    write_hits: u64,
    write_misses: u64,
    used_bytes: usize,
}

impl CacheStats {
    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    pub fn read_hits(&self) -> u64 {
        self.read_hits
    }

    pub fn read_misses(&self) -> u64 {
        self.read_misses
    }

    pub fn write_hits(&self) -> u64 {
        self.write_hits
    }

    pub fn write_misses(&self) -> u64 {
        self.write_misses
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DatabaseStats {
    pub stored_bytes: u64,
    pub metadata_bytes: u64,
    pub fragmented_bytes: u64,
}

#[derive(Debug, Default)]
pub struct RepairSession {
    abort: bool,
    progress: f64,
}

impl RepairSession {
    pub fn abort(&mut self) {
        self.abort = true;
    }

    pub fn progress(&self) -> f64 {
        self.progress
    }
}

pub trait StorageBackend: Debug + Send + Sync + 'static {}

struct DatabaseInner {
    path: PathBuf,
    conn: Mutex<Connection>,
    write_gate: Arc<WriteGate>,
}

#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

#[derive(Clone)]
pub struct ReadOnlyDatabase {
    inner: Arc<DatabaseInner>,
}

pub trait ReadableDatabase {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError>;
    fn cache_stats(&self) -> CacheStats;
}

impl ReadableDatabase for Database {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        Ok(ReadTransaction {
            inner: self.inner.clone(),
        })
    }

    fn cache_stats(&self) -> CacheStats {
        CacheStats::default()
    }
}

impl ReadableDatabase for ReadOnlyDatabase {
    fn begin_read(&self) -> Result<ReadTransaction, TransactionError> {
        Ok(ReadTransaction {
            inner: self.inner.clone(),
        })
    }

    fn cache_stats(&self) -> CacheStats {
        CacheStats::default()
    }
}

impl Database {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        Self::builder().create(path)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        Self::builder().open(path)
    }

    pub fn builder() -> Builder {
        Builder::new()
    }

    pub fn begin_write(&self) -> Result<WriteTransaction, TransactionError> {
        let lease = self.inner.write_gate.acquire()?;
        {
            let mut conn = self.inner.conn.lock()?;
            conn.exec("BEGIN IMMEDIATE")?;
        }
        Ok(WriteTransaction {
            inner: self.inner.clone(),
            lease: Some(lease),
            completed: false,
        })
    }

    pub fn checkpoint(&self) -> Result<(), DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        conn.checkpoint().map_err(Into::into)
    }

    pub fn check_integrity(&mut self) -> Result<bool, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        pragma_check(&mut conn, "PRAGMA quick_check")?;
        pragma_check(&mut conn, "PRAGMA integrity_check")?;
        Ok(true)
    }

    pub fn compact(&mut self) -> Result<bool, CompactionError> {
        let mut conn = self.inner.conn.lock()?;
        conn.exec("VACUUM")?;
        Ok(true)
    }

    pub fn backup_to_path(&self, destination: impl AsRef<Path>) -> Result<(), DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        conn.backup_to_path(destination.as_ref())?;
        Ok(())
    }

    pub fn n1_get_object_version_by_key(
        &self,
        object_key: &str,
        version_id: &str,
    ) -> Result<Option<crate::n1::ObjectVersion>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_object_version_by_key_on_conn(&mut conn, object_key, version_id).map_err(Into::into)
    }

    pub fn n1_get_object_version(
        &self,
        object_id: &str,
        version_id: &str,
    ) -> Result<Option<crate::n1::ObjectVersion>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_object_version_on_conn(&mut conn, object_id, version_id).map_err(Into::into)
    }

    pub fn n1_get_object_version_summary_by_key(
        &self,
        object_key: &str,
        version_id: &str,
    ) -> Result<Option<crate::n1::ObjectVersion>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_object_version_summary_by_key_on_conn(&mut conn, object_key, version_id)
            .map_err(Into::into)
    }

    pub fn n1_get_object_version_summary(
        &self,
        object_id: &str,
        version_id: &str,
    ) -> Result<Option<crate::n1::ObjectVersion>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_object_version_summary_on_conn(&mut conn, object_id, version_id).map_err(Into::into)
    }

    pub fn n1_get_object_version_state(
        &self,
        object_id: &str,
        version_id: &str,
    ) -> Result<Option<crate::n1::ObjectVersionState>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_object_version_state_on_conn(&mut conn, object_id, version_id).map_err(Into::into)
    }

    pub fn n1_get_object_head(
        &self,
        object_id: &str,
    ) -> Result<Option<crate::n1::ObjectHead>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_object_head_on_conn(&mut conn, object_id).map_err(Into::into)
    }

    pub fn n1_get_object_head_by_key(
        &self,
        object_key: &str,
    ) -> Result<Option<crate::n1::ObjectHead>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_object_head_by_key_on_conn(&mut conn, object_key).map_err(Into::into)
    }

    pub fn n1_list_object_heads(
        &self,
        state: crate::n1::ObjectState,
        prefix: Option<&str>,
        after_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::n1::ObjectHead>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_object_heads_on_conn(&mut conn, state, prefix, after_key, limit).map_err(Into::into)
    }

    pub fn n1_get_namespace_entry(
        &self,
        path: &str,
        lifecycle: Option<crate::n1::NamespaceLifecycle>,
    ) -> Result<Option<crate::n1::NamespaceEntry>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_namespace_entry_on_conn(&mut conn, path, lifecycle).map_err(Into::into)
    }

    pub fn n1_list_namespace_children(
        &self,
        parent_path: &str,
        include_trashed: bool,
        after_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::n1::NamespaceEntry>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_children_on_conn(
            &mut conn,
            parent_path,
            include_trashed,
            after_name,
            limit,
        )
        .map_err(Into::into)
    }

    pub fn n1_list_namespace_entries_by_prefix(
        &self,
        prefix: Option<&str>,
        include_trashed: bool,
        after_path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::n1::NamespaceEntry>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_entries_by_prefix_on_conn(
            &mut conn,
            prefix,
            include_trashed,
            after_path,
            limit,
        )
        .map_err(Into::into)
    }

    pub fn n1_list_namespace_object_summaries(
        &self,
        prefix: Option<&str>,
        lifecycle: crate::n1::NamespaceLifecycle,
        after_path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::n1::NamespaceObjectSummary>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_object_summaries_on_conn(&mut conn, prefix, lifecycle, after_path, limit)
            .map_err(Into::into)
    }

    pub fn n1_get_namespace_node(
        &self,
        node_id: &str,
    ) -> Result<Option<crate::n1::NamespaceNode>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_namespace_node_on_conn(&mut conn, node_id).map_err(Into::into)
    }

    pub fn n1_get_namespace_child(
        &self,
        parent_node_id: &str,
        name: &str,
    ) -> Result<Option<crate::n1::NamespaceNode>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_namespace_child_on_conn(&mut conn, parent_node_id, name).map_err(Into::into)
    }

    pub fn n1_get_namespace_file_head(
        &self,
        node_id: &str,
    ) -> Result<Option<crate::n1::NamespaceFileHead>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_namespace_file_head_on_conn(&mut conn, node_id).map_err(Into::into)
    }

    pub fn n1_list_namespace_file_heads(
        &self,
        after_node_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::n1::NamespaceFileHead>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_file_heads_on_conn(&mut conn, after_node_id, limit).map_err(Into::into)
    }

    pub fn n1_list_namespace_node_children(
        &self,
        parent_node_id: &str,
        after_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::n1::NamespaceNode>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_node_children_on_conn(&mut conn, parent_node_id, after_name, limit)
            .map_err(Into::into)
    }

    pub fn n1_list_namespace_nodes(
        &self,
        after_node_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::n1::NamespaceNode>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_nodes_on_conn(&mut conn, after_node_id, limit).map_err(Into::into)
    }

    pub fn n1_list_namespace_node_object_summaries(
        &self,
        root_node_id: &str,
        base_path: &str,
        prefix: Option<&str>,
        after_path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::n1::NamespaceObjectSummary>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_node_object_summaries_on_conn(
            &mut conn,
            root_node_id,
            base_path,
            prefix,
            after_path,
            limit,
        )
        .map_err(Into::into)
    }

    pub fn n1_list_namespace_subtree_nodes(
        &self,
        root_node_id: &str,
        base_path: &str,
        after_path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, crate::n1::NamespaceNode)>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_subtree_nodes_on_conn(
            &mut conn,
            root_node_id,
            base_path,
            after_path,
            limit,
        )
        .map_err(Into::into)
    }

    pub fn n1_list_namespace_trash_roots(
        &self,
        state: crate::n1::TrashEntryState,
        after_path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::n1::NamespaceTrashRoot>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_trash_roots_on_conn(&mut conn, state, after_path, limit)
            .map_err(Into::into)
    }

    pub fn n1_get_namespace_trash_root(
        &self,
        trash_entry_id: &str,
    ) -> Result<Option<crate::n1::NamespaceTrashRoot>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_namespace_trash_root_on_conn(&mut conn, trash_entry_id).map_err(Into::into)
    }

    pub fn n1_list_namespace_trash_roots_by_path(
        &self,
        original_path: &str,
        state: crate::n1::TrashEntryState,
    ) -> Result<Vec<crate::n1::NamespaceTrashRoot>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_trash_roots_by_path_on_conn(&mut conn, original_path, state)
            .map_err(Into::into)
    }

    pub fn n1_list_namespace_trash_roots_all(
        &self,
        after_trash_entry_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::n1::NamespaceTrashRoot>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_trash_roots_all_on_conn(&mut conn, after_trash_entry_id, limit)
            .map_err(Into::into)
    }

    pub fn n1_list_namespace_subtree(
        &self,
        root_path: &str,
        include_trashed: bool,
    ) -> Result<Vec<crate::n1::NamespaceEntry>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_namespace_subtree_on_conn(&mut conn, root_path, include_trashed).map_err(Into::into)
    }

    pub fn n1_list_trash_entries(
        &self,
        state: crate::n1::TrashEntryState,
        after_path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<crate::n1::TrashEntry>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_trash_entries_on_conn(&mut conn, state, after_path, limit).map_err(Into::into)
    }

    pub fn n1_list_trash_entries_by_path(
        &self,
        original_path: &str,
        state: crate::n1::TrashEntryState,
    ) -> Result<Vec<crate::n1::TrashEntry>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_trash_entries_by_path_on_conn(&mut conn, original_path, state).map_err(Into::into)
    }

    pub fn n1_list_pending_uploads(
        &self,
    ) -> Result<Vec<crate::n1::PendingUploadBinding>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_list_pending_uploads_on_conn(&mut conn).map_err(Into::into)
    }

    pub fn n1_get_pending_upload(
        &self,
        version_id: &str,
    ) -> Result<Option<crate::n1::PendingUploadBinding>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_pending_upload_on_conn(&mut conn, version_id).map_err(Into::into)
    }

    pub fn n1_find_pending_upload_by_logical_path(
        &self,
        logical_path: &str,
    ) -> Result<Option<crate::n1::PendingUploadBinding>, DatabaseError> {
        let mut conn = self.inner.conn.lock()?;
        n1_find_pending_upload_by_logical_path_on_conn(&mut conn, logical_path).map_err(Into::into)
    }

    pub fn n1_get_fragment_meta(&self) -> Result<Option<crate::n1::FragmentMeta>, DatabaseError> {
        let Some(fragment_id) = infer_fragment_id(self.path()) else {
            return Ok(None);
        };
        let mut conn = self.inner.conn.lock()?;
        n1_get_fragment_meta_on_conn(&mut conn, &fragment_id).map_err(Into::into)
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }
}

impl ReadOnlyDatabase {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let path = path.as_ref();
        let conn = Connection::open(path, OpenMode::ReadOnly)?;
        Ok(Self {
            inner: Arc::new(DatabaseInner {
                path: path.to_path_buf(),
                conn: Mutex::new(conn),
                write_gate: Arc::new(WriteGate::default()),
            }),
        })
    }
}

pub struct Builder {
    cache_size: usize,
    repair_callback: Box<dyn Fn(&mut RepairSession) + Send + Sync>,
}

const DEFAULT_BUILDER_CACHE_SIZE_BYTES: usize = 2 * 1024 * 1024;

impl Builder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            cache_size: DEFAULT_BUILDER_CACHE_SIZE_BYTES,
            repair_callback: Box::new(|_| {}),
        }
    }

    pub fn set_cache_size(&mut self, bytes: usize) -> &mut Self {
        self.cache_size = bytes;
        self
    }

    pub fn set_repair_callback(
        &mut self,
        callback: impl Fn(&mut RepairSession) + Send + Sync + 'static,
    ) -> &mut Self {
        self.repair_callback = Box::new(callback);
        self
    }

    pub fn create(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open_with_cache(path, OpenMode::ReadWriteCreate, self.cache_size)?;
        let db = Database {
            inner: Arc::new(DatabaseInner {
                path: path.to_path_buf(),
                conn: Mutex::new(conn),
                write_gate: Arc::new(WriteGate::default()),
            }),
        };
        initialize_path_metadata(&db)?;
        Ok(db)
    }

    pub fn open(&self, path: impl AsRef<Path>) -> Result<Database, DatabaseError> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(DatabaseError::from(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("database file {} does not exist", path.display()),
            )));
        }
        let conn = Connection::open_with_cache(path, OpenMode::ReadWriteCreate, self.cache_size)?;
        let db = Database {
            inner: Arc::new(DatabaseInner {
                path: path.to_path_buf(),
                conn: Mutex::new(conn),
                write_gate: Arc::new(WriteGate::default()),
            }),
        };
        initialize_path_metadata(&db)?;
        Ok(db)
    }

    pub fn open_read_only(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<ReadOnlyDatabase, DatabaseError> {
        let path = path.as_ref();
        let conn = Connection::open_with_cache(path, OpenMode::ReadOnly, self.cache_size)?;
        Ok(ReadOnlyDatabase {
            inner: Arc::new(DatabaseInner {
                path: path.to_path_buf(),
                conn: Mutex::new(conn),
                write_gate: Arc::new(WriteGate::default()),
            }),
        })
    }
}

#[derive(Default)]
struct WriteGate {
    state: Mutex<bool>,
    condvar: Condvar,
}

impl WriteGate {
    fn acquire(self: &Arc<Self>) -> Result<WriteLease, StorageError> {
        let mut busy = self.state.lock()?;
        while *busy {
            busy = self
                .condvar
                .wait(busy)
                .map_err(|_| StorageError::LockPoisoned(std::panic::Location::caller()))?;
        }
        *busy = true;
        Ok(WriteLease {
            gate: self.clone(),
            active: true,
        })
    }

    fn release(&self) {
        if let Ok(mut busy) = self.state.lock() {
            *busy = false;
            self.condvar.notify_one();
        }
    }
}

struct WriteLease {
    gate: Arc<WriteGate>,
    active: bool,
}

impl Drop for WriteLease {
    fn drop(&mut self) {
        if self.active {
            self.gate.release();
            self.active = false;
        }
    }
}

pub struct WriteTransaction {
    inner: Arc<DatabaseInner>,
    lease: Option<WriteLease>,
    completed: bool,
}

impl WriteTransaction {
    pub fn open_table<'txn, K: Key + 'static, V: Value + 'static>(
        &'txn self,
        definition: TableDefinition<K, V>,
    ) -> Result<Table<'txn, K, V>, TableError> {
        ensure_table::<K, V>(&self.inner, definition.name)?;
        Ok(Table {
            name: definition.name.to_string(),
            inner: self.inner.clone(),
            _txn: PhantomData,
            _key_type: PhantomData,
            _value_type: PhantomData,
        })
    }

    pub fn delete_table(&self, definition: impl TableHandle) -> Result<bool, TableError> {
        let name = definition.name().to_string();
        let mut conn = self.inner.conn.lock()?;
        let mut delete_rows = conn.prepare("DELETE FROM indexdb_kv WHERE table_name = ?1")?;
        delete_rows.bind_text(1, &name)?;
        delete_rows.step_done()?;
        let mut delete_table = conn.prepare("DELETE FROM indexdb_tables WHERE name = ?1")?;
        delete_table.bind_text(1, &name)?;
        delete_table.step_done()?;
        Ok(conn.changes() > 0)
    }

    pub fn rename_table(
        &self,
        definition: impl TableHandle,
        new_name: impl TableHandle,
    ) -> Result<(), TableError> {
        let old_name = definition.name().to_string();
        let new_name = new_name.name().to_string();
        let mut conn = self.inner.conn.lock()?;
        let mut update_rows =
            conn.prepare("UPDATE indexdb_kv SET table_name = ?1 WHERE table_name = ?2")?;
        update_rows.bind_text(1, &new_name)?;
        update_rows.bind_text(2, &old_name)?;
        update_rows.step_done()?;
        let mut update_table =
            conn.prepare("UPDATE indexdb_tables SET name = ?1 WHERE name = ?2")?;
        update_table.bind_text(1, &new_name)?;
        update_table.bind_text(2, &old_name)?;
        update_table.step_done()?;
        Ok(())
    }

    pub fn list_tables(&self) -> Result<Vec<UntypedTableHandle>, TableError> {
        list_tables(&self.inner).map_err(TableError::from)
    }

    pub fn n1_upsert_object_version(
        &self,
        version: &crate::n1::ObjectVersion,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_upsert_object_version_on_conn(&mut conn, version).map_err(Into::into)
    }

    pub fn n1_delete_object_version(
        &self,
        object_id: &str,
        version_id: &str,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_delete_object_version_on_conn(&mut conn, object_id, version_id).map_err(Into::into)
    }

    pub fn n1_set_object_version_state(
        &self,
        state: &crate::n1::ObjectVersionState,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_set_object_version_state_on_conn(&mut conn, state).map_err(Into::into)
    }

    pub fn n1_delete_object_version_state(
        &self,
        object_id: &str,
        version_id: &str,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_delete_object_version_state_on_conn(&mut conn, object_id, version_id).map_err(Into::into)
    }

    pub fn n1_upsert_object_head(&self, head: &crate::n1::ObjectHead) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_upsert_object_head_on_conn(&mut conn, head).map_err(Into::into)
    }

    pub fn n1_delete_object_head(&self, object_id: &str) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_delete_object_head_on_conn(&mut conn, object_id).map_err(Into::into)
    }

    pub fn n1_get_namespace_entry(
        &self,
        path: &str,
        lifecycle: Option<crate::n1::NamespaceLifecycle>,
    ) -> Result<Option<crate::n1::NamespaceEntry>, TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_namespace_entry_on_conn(&mut conn, path, lifecycle).map_err(Into::into)
    }

    pub fn n1_upsert_namespace_entry(
        &self,
        entry: &crate::n1::NamespaceEntry,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_upsert_namespace_entry_on_conn(&mut conn, entry).map_err(Into::into)
    }

    pub fn n1_delete_namespace_entry(&self, path: &str) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_delete_namespace_entry_on_conn(&mut conn, path).map_err(Into::into)
    }

    pub fn n1_delete_namespace_node(&self, node_id: &str) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_delete_namespace_node_on_conn(&mut conn, node_id).map_err(Into::into)
    }

    pub fn n1_upsert_namespace_node(
        &self,
        node: &crate::n1::NamespaceNode,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_upsert_namespace_node_on_conn(&mut conn, node).map_err(Into::into)
    }

    pub fn n1_get_namespace_node(
        &self,
        node_id: &str,
    ) -> Result<Option<crate::n1::NamespaceNode>, TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_namespace_node_on_conn(&mut conn, node_id).map_err(Into::into)
    }

    pub fn n1_get_namespace_child(
        &self,
        parent_node_id: &str,
        name: &str,
    ) -> Result<Option<crate::n1::NamespaceNode>, TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_namespace_child_on_conn(&mut conn, parent_node_id, name).map_err(Into::into)
    }

    pub fn n1_get_namespace_file_head(
        &self,
        node_id: &str,
    ) -> Result<Option<crate::n1::NamespaceFileHead>, TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_namespace_file_head_on_conn(&mut conn, node_id).map_err(Into::into)
    }

    pub fn n1_upsert_namespace_file_head(
        &self,
        head: &crate::n1::NamespaceFileHead,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_upsert_namespace_file_head_on_conn(&mut conn, head).map_err(Into::into)
    }

    pub fn n1_delete_namespace_file_head(&self, node_id: &str) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_delete_namespace_file_head_on_conn(&mut conn, node_id).map_err(Into::into)
    }

    pub fn n1_move_namespace_node(
        &self,
        node_id: &str,
        parent_node_id: &str,
        name: &str,
        changed_at_unix: i64,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_move_namespace_node_on_conn(&mut conn, node_id, parent_node_id, name, changed_at_unix)
            .map_err(Into::into)
    }

    pub fn n1_upsert_namespace_trash_root(
        &self,
        root: &crate::n1::NamespaceTrashRoot,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_upsert_namespace_trash_root_on_conn(&mut conn, root).map_err(Into::into)
    }

    pub fn n1_get_namespace_trash_root(
        &self,
        trash_entry_id: &str,
    ) -> Result<Option<crate::n1::NamespaceTrashRoot>, TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_get_namespace_trash_root_on_conn(&mut conn, trash_entry_id).map_err(Into::into)
    }

    pub fn n1_set_namespace_trash_root_state(
        &self,
        trash_entry_id: &str,
        state: crate::n1::TrashEntryState,
        changed_at_unix: i64,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_set_namespace_trash_root_state_on_conn(&mut conn, trash_entry_id, state, changed_at_unix)
            .map_err(Into::into)
    }

    pub fn n1_replace_namespace_entries(
        &self,
        entries: &[crate::n1::NamespaceEntry],
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_replace_namespace_entries_on_conn(&mut conn, entries).map_err(Into::into)
    }

    pub fn n1_rename_namespace_subtree(
        &self,
        source_path: &str,
        destination_path: &str,
        changed_at_unix: i64,
    ) -> Result<crate::n1::NamespaceMutationStats, TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_rename_namespace_subtree_on_conn(
            &mut conn,
            source_path,
            destination_path,
            changed_at_unix,
        )
        .map_err(Into::into)
    }

    pub fn n1_set_namespace_lifecycle_subtree(
        &self,
        root_path: &str,
        from: crate::n1::NamespaceLifecycle,
        target: crate::n1::NamespaceLifecycle,
        changed_at_unix: i64,
        trash_entry_id: Option<&str>,
    ) -> Result<crate::n1::NamespaceMutationStats, TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_set_namespace_lifecycle_subtree_on_conn(
            &mut conn,
            root_path,
            from,
            target,
            changed_at_unix,
            trash_entry_id,
        )
        .map_err(Into::into)
    }

    pub fn n1_purge_namespace_subtree(
        &self,
        root_path: Option<&str>,
        trash_entry_id: Option<&str>,
    ) -> Result<crate::n1::NamespaceMutationStats, TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_purge_namespace_subtree_on_conn(&mut conn, root_path, trash_entry_id).map_err(Into::into)
    }

    pub fn n1_upsert_trash_entry(&self, entry: &crate::n1::TrashEntry) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_upsert_trash_entry_on_conn(&mut conn, entry).map_err(Into::into)
    }

    pub fn n1_set_trash_entry_state(
        &self,
        trash_entry_id: &str,
        state: crate::n1::TrashEntryState,
        changed_at_unix: i64,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_set_trash_entry_state_on_conn(&mut conn, trash_entry_id, state, changed_at_unix)
            .map_err(Into::into)
    }

    pub fn n1_upsert_pending_upload(
        &self,
        binding: &crate::n1::PendingUploadBinding,
    ) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_upsert_pending_upload_on_conn(&mut conn, binding).map_err(Into::into)
    }

    pub fn n1_delete_pending_upload(&self, version_id: &str) -> Result<(), TableError> {
        let mut conn = self.inner.conn.lock()?;
        n1_delete_pending_upload_on_conn(&mut conn, version_id).map_err(Into::into)
    }

    pub fn n1_set_fragment_quota(&self, quota_bytes: u64) -> Result<(), TableError> {
        let Some(fragment_id) = infer_fragment_id(&self.inner.path) else {
            return Ok(());
        };
        let mut conn = self.inner.conn.lock()?;
        n1_set_fragment_quota_on_conn(&mut conn, &fragment_id, quota_bytes).map_err(Into::into)
    }

    pub fn n1_set_fragment_quota_project_id(&self, project_id: u32) -> Result<(), TableError> {
        let Some(fragment_id) = infer_fragment_id(&self.inner.path) else {
            return Ok(());
        };
        let mut conn = self.inner.conn.lock()?;
        n1_set_fragment_quota_project_id_on_conn(&mut conn, &fragment_id, project_id)
            .map_err(Into::into)
    }

    pub fn commit(mut self) -> Result<(), CommitError> {
        {
            let mut conn = self.inner.conn.lock()?;
            conn.exec("COMMIT")?;
        }
        self.completed = true;
        self.lease.take();
        Ok(())
    }

    pub fn abort(mut self) -> Result<(), CommitError> {
        {
            let mut conn = self.inner.conn.lock()?;
            conn.exec("ROLLBACK")?;
        }
        self.completed = true;
        self.lease.take();
        Ok(())
    }
}

impl Drop for WriteTransaction {
    fn drop(&mut self) {
        if !self.completed {
            if let Ok(mut conn) = self.inner.conn.lock() {
                let _ = conn.exec("ROLLBACK");
            }
        }
    }
}

pub struct ReadTransaction {
    inner: Arc<DatabaseInner>,
}

impl ReadTransaction {
    pub fn open_table<K: Key + 'static, V: Value + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<ReadOnlyTable<K, V>, TableError> {
        if !table_exists(&self.inner, definition.name)? {
            return Err(TableError::TableDoesNotExist(definition.name.to_string()));
        }
        Ok(ReadOnlyTable {
            name: definition.name.to_string(),
            inner: self.inner.clone(),
            _key_type: PhantomData,
            _value_type: PhantomData,
        })
    }

    pub fn list_tables(&self) -> Result<Vec<UntypedTableHandle>, TableError> {
        list_tables(&self.inner).map_err(TableError::from)
    }
}

#[derive(Debug)]
pub struct TableStats {
    stored_leaf_bytes: u64,
    metadata_bytes: u64,
    fragmented_bytes: u64,
}

impl TableStats {
    pub fn tree_height(&self) -> u32 {
        1
    }

    pub fn leaf_pages(&self) -> u64 {
        0
    }

    pub fn branch_pages(&self) -> u64 {
        0
    }

    pub fn stored_bytes(&self) -> u64 {
        self.stored_leaf_bytes
    }

    pub fn metadata_bytes(&self) -> u64 {
        self.metadata_bytes
    }

    pub fn fragmented_bytes(&self) -> u64 {
        self.fragmented_bytes
    }
}

pub trait ReadableTableMetadata {
    fn stats(&self) -> crate::Result<TableStats>;
    fn len(&self) -> crate::Result<u64>;

    fn is_empty(&self) -> crate::Result<bool> {
        Ok(self.len()? == 0)
    }
}

pub trait ReadableTable<K: Key + 'static, V: Value + 'static>: ReadableTableMetadata {
    fn get<'a>(
        &self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> crate::Result<Option<AccessGuard<'_, V>>>;

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> crate::Result<Range<'_, K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a;

    fn first(&self) -> crate::Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>>;

    fn last(&self) -> crate::Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>>;

    fn iter(&self) -> crate::Result<Range<'_, K, V>> {
        self.range::<K::SelfType<'_>>(..)
    }
}

pub struct Table<'txn, K: Key + 'static, V: Value + 'static> {
    name: String,
    inner: Arc<DatabaseInner>,
    _txn: PhantomData<&'txn WriteTransaction>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for Table<'_, K, V> {
    fn name(&self) -> &str {
        &self.name
    }
}

impl<K: Key + 'static, V: Value + 'static> Table<'_, K, V> {
    pub fn insert<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> crate::Result<Option<AccessGuard<'_, V>>> {
        let key_bytes = encode_key::<K>(key.borrow());
        let value_bytes = encode_value::<V>(value.borrow());
        let previous = select_value::<V>(&self.inner, &self.name, &key_bytes)?;
        let mut conn = self.inner.conn.lock()?;
        let mut stmt = conn.prepare(
            "INSERT INTO indexdb_kv(table_name, key, value)
             VALUES(?1, ?2, ?3)
             ON CONFLICT(table_name, key) DO UPDATE SET value = excluded.value",
        )?;
        stmt.bind_text(1, &self.name)?;
        stmt.bind_blob(2, &key_bytes)?;
        stmt.bind_blob(3, &value_bytes)?;
        stmt.step_done()?;
        Ok(previous)
    }

    pub fn remove<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> crate::Result<Option<AccessGuard<'_, V>>> {
        let key_bytes = encode_key::<K>(key.borrow());
        let previous = select_value::<V>(&self.inner, &self.name, &key_bytes)?;
        let mut conn = self.inner.conn.lock()?;
        let mut stmt = conn.prepare("DELETE FROM indexdb_kv WHERE table_name = ?1 AND key = ?2")?;
        stmt.bind_text(1, &self.name)?;
        stmt.bind_blob(2, &key_bytes)?;
        stmt.step_done()?;
        Ok(previous)
    }

    pub fn pop_first(&mut self) -> crate::Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        let Some((key, value)) = select_edge::<K, V>(&self.inner, &self.name, true)? else {
            return Ok(None);
        };
        let key_bytes = key.data.clone();
        let mut conn = self.inner.conn.lock()?;
        let mut stmt = conn.prepare("DELETE FROM indexdb_kv WHERE table_name = ?1 AND key = ?2")?;
        stmt.bind_text(1, &self.name)?;
        stmt.bind_blob(2, &key_bytes)?;
        stmt.step_done()?;
        Ok(Some((key, value)))
    }

    pub fn pop_last(&mut self) -> crate::Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        let Some((key, value)) = select_edge::<K, V>(&self.inner, &self.name, false)? else {
            return Ok(None);
        };
        let key_bytes = key.data.clone();
        let mut conn = self.inner.conn.lock()?;
        let mut stmt = conn.prepare("DELETE FROM indexdb_kv WHERE table_name = ?1 AND key = ?2")?;
        stmt.bind_text(1, &self.name)?;
        stmt.bind_blob(2, &key_bytes)?;
        stmt.step_done()?;
        Ok(Some((key, value)))
    }
}

impl<K: Key + 'static, V: MutInPlaceValue + 'static> Table<'_, K, V> {
    pub fn insert_reserve<'a>(
        &mut self,
        _key: impl Borrow<K::SelfType<'a>>,
        _value_length: usize,
    ) -> crate::Result<AccessGuardMutInPlace<'_, V>> {
        Err(StorageError::InvalidInput(
            "insert_reserve is not supported by the N1 indexdb backend".to_string(),
        ))
    }
}

impl<K: Key + 'static, V: Value + 'static> ReadableTableMetadata for Table<'_, K, V> {
    fn stats(&self) -> crate::Result<TableStats> {
        table_stats(&self.inner, &self.name)
    }

    fn len(&self) -> crate::Result<u64> {
        table_len(&self.inner, &self.name)
    }
}

impl<K: Key + 'static, V: Value + 'static> ReadableTable<K, V> for Table<'_, K, V> {
    fn get<'a>(
        &self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> crate::Result<Option<AccessGuard<'_, V>>> {
        let key_bytes = encode_key::<K>(key.borrow());
        select_value::<V>(&self.inner, &self.name, &key_bytes)
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> crate::Result<Range<'_, K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        select_range::<K, V, KR>(&self.inner, &self.name, range)
    }

    fn first(&self) -> crate::Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        select_edge::<K, V>(&self.inner, &self.name, true)
    }

    fn last(&self) -> crate::Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        select_edge::<K, V>(&self.inner, &self.name, false)
    }
}

pub struct ReadOnlyTable<K: Key + 'static, V: Value + 'static> {
    name: String,
    inner: Arc<DatabaseInner>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for ReadOnlyTable<K, V> {
    fn name(&self) -> &str {
        &self.name
    }
}

impl<K: Key + 'static, V: Value + 'static> ReadOnlyTable<K, V> {
    pub fn get<'a>(
        &self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> crate::Result<Option<AccessGuard<'static, V>>> {
        let key_bytes = encode_key::<K>(key.borrow());
        select_value::<V>(&self.inner, &self.name, &key_bytes)
    }

    pub fn range<'a, KR>(
        &self,
        range: impl RangeBounds<KR> + 'a,
    ) -> crate::Result<Range<'static, K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        select_range::<K, V, KR>(&self.inner, &self.name, range)
    }
}

impl<K: Key + 'static, V: Value + 'static> ReadableTableMetadata for ReadOnlyTable<K, V> {
    fn stats(&self) -> crate::Result<TableStats> {
        table_stats(&self.inner, &self.name)
    }

    fn len(&self) -> crate::Result<u64> {
        table_len(&self.inner, &self.name)
    }
}

impl<K: Key + 'static, V: Value + 'static> ReadableTable<K, V> for ReadOnlyTable<K, V> {
    fn get<'a>(
        &self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> crate::Result<Option<AccessGuard<'_, V>>> {
        ReadOnlyTable::get(self, key)
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> crate::Result<Range<'_, K, V>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        ReadOnlyTable::range(self, range)
    }

    fn first(&self) -> crate::Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        select_edge::<K, V>(&self.inner, &self.name, true)
    }

    fn last(&self) -> crate::Result<Option<(AccessGuard<'_, K>, AccessGuard<'_, V>)>> {
        select_edge::<K, V>(&self.inner, &self.name, false)
    }
}

pub struct ReadOnlyUntypedTable;

pub struct AccessGuard<'a, V: Value + 'static> {
    data: Vec<u8>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl<V: Value + 'static> AccessGuard<'_, V> {
    pub(crate) fn with_owned_value(data: Vec<u8>) -> Self {
        Self {
            data,
            _value_type: PhantomData,
            _lifetime: PhantomData,
        }
    }

    pub fn value(&self) -> V::SelfType<'_> {
        V::from_bytes(&self.data)
    }
}

pub struct AccessGuardMut<'a, V: Value + 'static>(PhantomData<(&'a (), V)>);

pub struct AccessGuardMutInPlace<'a, V: Value + 'static>(PhantomData<(&'a (), V)>);

pub struct Range<'a, K: Key + 'static, V: Value + 'static> {
    rows: VecDeque<(Vec<u8>, Vec<u8>)>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
    _lifetime: PhantomData<&'a ()>,
}

impl<'a, K: Key + 'static, V: Value + 'static> Iterator for Range<'a, K, V> {
    type Item = crate::Result<(AccessGuard<'a, K>, AccessGuard<'a, V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.pop_front().map(|(key, value)| {
            Ok((
                AccessGuard::with_owned_value(key),
                AccessGuard::with_owned_value(value),
            ))
        })
    }
}

impl<K: Key + 'static, V: Value + 'static> DoubleEndedIterator for Range<'_, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.rows.pop_back().map(|(key, value)| {
            Ok((
                AccessGuard::with_owned_value(key),
                AccessGuard::with_owned_value(value),
            ))
        })
    }
}

fn ensure_table<K: Key + 'static, V: Value + 'static>(
    inner: &DatabaseInner,
    name: &str,
) -> Result<(), StorageError> {
    let mut conn = inner.conn.lock()?;
    let mut stmt = conn.prepare(
        "INSERT INTO indexdb_tables(name, key_type, value_type, kind)
         VALUES(?1, ?2, ?3, 'normal')
         ON CONFLICT(name) DO UPDATE SET
             key_type = excluded.key_type,
             value_type = excluded.value_type,
             kind = 'normal'",
    )?;
    stmt.bind_text(1, name)?;
    stmt.bind_text(2, K::type_name().name())?;
    stmt.bind_text(3, V::type_name().name())?;
    stmt.step_done()
}

fn table_exists(inner: &DatabaseInner, name: &str) -> Result<bool, StorageError> {
    let mut conn = inner.conn.lock()?;
    let mut stmt = conn.prepare("SELECT 1 FROM indexdb_tables WHERE name = ?1 LIMIT 1")?;
    stmt.bind_text(1, name)?;
    Ok(matches!(stmt.step()?, StepResult::Row))
}

fn list_tables(inner: &DatabaseInner) -> Result<Vec<UntypedTableHandle>, StorageError> {
    let mut conn = inner.conn.lock()?;
    let mut stmt =
        conn.prepare("SELECT name FROM indexdb_tables WHERE kind = 'normal' ORDER BY name")?;
    let mut out = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        if let Some(name) = stmt.column_text(0) {
            out.push(UntypedTableHandle::new(name));
        }
    }
    Ok(out)
}

fn select_value<V: Value + 'static>(
    inner: &DatabaseInner,
    table_name: &str,
    key: &[u8],
) -> crate::Result<Option<AccessGuard<'static, V>>> {
    let mut conn = inner.conn.lock()?;
    let mut stmt =
        conn.prepare("SELECT value FROM indexdb_kv WHERE table_name = ?1 AND key = ?2")?;
    stmt.bind_text(1, table_name)?;
    stmt.bind_blob(2, key)?;
    match stmt.step()? {
        StepResult::Row => Ok(stmt.column_blob(0).map(AccessGuard::with_owned_value)),
        StepResult::Done => Ok(None),
    }
}

fn select_edge<K: Key + 'static, V: Value + 'static>(
    inner: &DatabaseInner,
    table_name: &str,
    first: bool,
) -> crate::Result<Option<(AccessGuard<'static, K>, AccessGuard<'static, V>)>> {
    let order = if first { "ASC" } else { "DESC" };
    let sql = format!(
        "SELECT key, value FROM indexdb_kv WHERE table_name = ?1 ORDER BY key {order} LIMIT 1"
    );
    let mut conn = inner.conn.lock()?;
    let mut stmt = conn.prepare(&sql)?;
    stmt.bind_text(1, table_name)?;
    match stmt.step()? {
        StepResult::Row => {
            let key = stmt.column_blob(0).unwrap_or_default();
            let value = stmt.column_blob(1).unwrap_or_default();
            Ok(Some((
                AccessGuard::with_owned_value(key),
                AccessGuard::with_owned_value(value),
            )))
        }
        StepResult::Done => Ok(None),
    }
}

fn select_range<'a, K, V, KR>(
    inner: &DatabaseInner,
    table_name: &str,
    range: impl RangeBounds<KR> + 'a,
) -> crate::Result<Range<'static, K, V>>
where
    K: Key + 'static,
    V: Value + 'static,
    KR: Borrow<K::SelfType<'a>> + 'a,
{
    let start = bound_to_bytes::<K, KR>(range.start_bound());
    let end = bound_to_bytes::<K, KR>(range.end_bound());
    let mut sql = String::from("SELECT key, value FROM indexdb_kv WHERE table_name = ?1");
    if let Some((inclusive, _)) = &start {
        sql.push_str(if *inclusive {
            " AND key >= ?2"
        } else {
            " AND key > ?2"
        });
    }
    if let Some((inclusive, _)) = &end {
        let idx = if start.is_some() { "?3" } else { "?2" };
        if *inclusive {
            sql.push_str(" AND key <= ");
        } else {
            sql.push_str(" AND key < ");
        }
        sql.push_str(idx);
    }
    sql.push_str(" ORDER BY key ASC");

    let mut conn = inner.conn.lock()?;
    let mut stmt = conn.prepare(&sql)?;
    stmt.bind_text(1, table_name)?;
    let mut bind_idx = 2;
    if let Some((_, value)) = &start {
        stmt.bind_blob(bind_idx, value)?;
        bind_idx += 1;
    }
    if let Some((_, value)) = &end {
        stmt.bind_blob(bind_idx, value)?;
    }

    let mut rows = VecDeque::new();
    while matches!(stmt.step()?, StepResult::Row) {
        rows.push_back((
            stmt.column_blob(0).unwrap_or_default(),
            stmt.column_blob(1).unwrap_or_default(),
        ));
    }
    Ok(Range {
        rows,
        _key_type: PhantomData,
        _value_type: PhantomData,
        _lifetime: PhantomData,
    })
}

fn table_len(inner: &DatabaseInner, table_name: &str) -> crate::Result<u64> {
    let mut conn = inner.conn.lock()?;
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM indexdb_kv WHERE table_name = ?1")?;
    stmt.bind_text(1, table_name)?;
    match stmt.step()? {
        StepResult::Row => Ok(u64::try_from(stmt.column_i64(0)).unwrap_or(0)),
        StepResult::Done => Ok(0),
    }
}

fn table_stats(inner: &DatabaseInner, table_name: &str) -> crate::Result<TableStats> {
    let mut conn = inner.conn.lock()?;
    let mut stmt = conn.prepare(
        "SELECT COALESCE(SUM(length(key) + length(value)), 0)
         FROM indexdb_kv WHERE table_name = ?1",
    )?;
    stmt.bind_text(1, table_name)?;
    let stored_leaf_bytes = match stmt.step()? {
        StepResult::Row => u64::try_from(stmt.column_i64(0)).unwrap_or(0),
        StepResult::Done => 0,
    };
    Ok(TableStats {
        stored_leaf_bytes,
        metadata_bytes: 0,
        fragmented_bytes: 0,
    })
}

fn n1_upsert_object_version_on_conn(
    conn: &mut Connection,
    version: &crate::n1::ObjectVersion,
) -> Result<(), StorageError> {
    let mut version_stmt = conn.prepare(
        "INSERT INTO object_versions(
            object_id, object_key, version_id, upload_session_id, owner_uid,
            content_type, total_size_bytes, created_at_unix, committed_at_unix,
            is_delete_marker, stripe_size_bytes, checksum, metadata_json
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT(object_id, version_id) DO UPDATE SET
            committed_at_unix = excluded.committed_at_unix,
            stripe_size_bytes = excluded.stripe_size_bytes,
            checksum = excluded.checksum,
            metadata_json = excluded.metadata_json",
    )?;
    version_stmt.bind_text(1, &version.object_id)?;
    version_stmt.bind_text(2, &version.object_key)?;
    version_stmt.bind_text(3, &version.version_id)?;
    bind_optional_text(&mut version_stmt, 4, non_empty(&version.upload_session_id))?;
    bind_optional_text(&mut version_stmt, 5, non_empty(&version.owner_uid))?;
    bind_optional_text(&mut version_stmt, 6, non_empty(&version.content_type))?;
    version_stmt.bind_i64(7, u64_to_i64(version.total_size_bytes)?)?;
    version_stmt.bind_i64(8, version.created_at_unix)?;
    bind_optional_i64(&mut version_stmt, 9, version.committed_at_unix)?;
    version_stmt.bind_i32(10, i32::from(version.is_delete_marker))?;
    version_stmt.bind_i64(11, u64_to_i64(version.stripe_size_bytes)?)?;
    bind_optional_text(&mut version_stmt, 12, version.checksum.as_deref())?;
    bind_optional_text(&mut version_stmt, 13, version.metadata_json.as_deref())?;
    version_stmt.step_done()?;
    let mut delete_shards = conn.prepare("DELETE FROM part_shards WHERE version_id = ?1")?;
    delete_shards.bind_text(1, &version.version_id)?;
    delete_shards.step_done()?;

    let mut delete_parts = conn.prepare("DELETE FROM object_parts WHERE version_id = ?1")?;
    delete_parts.bind_text(1, &version.version_id)?;
    delete_parts.step_done()?;

    for part in &version.parts {
        let mut part_stmt = conn.prepare(
            "INSERT INTO object_parts(
                version_id, part_index, size_bytes, created_at_unix, checksum
             )
             VALUES(?1, ?2, ?3, ?4, ?5)",
        )?;
        part_stmt.bind_text(1, &version.version_id)?;
        part_stmt.bind_i64(2, u64_to_i64(part.part_index)?)?;
        part_stmt.bind_i64(3, u64_to_i64(part.size_bytes)?)?;
        part_stmt.bind_i64(4, part.created_at_unix)?;
        bind_optional_text(&mut part_stmt, 5, part.checksum.as_deref())?;
        part_stmt.step_done()?;

        for shard in &part.shards {
            let mut shard_stmt = conn.prepare(
                "INSERT INTO part_shards(
                    version_id, part_index, shard_index, file_name, absolute_path,
                    relative_path, blob_root_id, size_bytes, checksum, is_parity,
                    created_at_unix
                 )
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            )?;
            shard_stmt.bind_text(1, &version.version_id)?;
            shard_stmt.bind_i64(2, u64_to_i64(part.part_index)?)?;
            shard_stmt.bind_i64(3, u64_to_i64(shard.shard_index)?)?;
            shard_stmt.bind_text(4, &shard.file_name)?;
            shard_stmt.bind_text(5, &shard.absolute_path)?;
            bind_optional_text(&mut shard_stmt, 6, shard.relative_path.as_deref())?;
            bind_optional_text(&mut shard_stmt, 7, shard.blob_root_id.as_deref())?;
            shard_stmt.bind_i64(8, u64_to_i64(shard.size_bytes)?)?;
            shard_stmt.bind_text(9, &shard.checksum)?;
            shard_stmt.bind_i32(10, i32::from(shard.is_parity))?;
            shard_stmt.bind_i64(11, shard.created_at_unix)?;
            shard_stmt.step_done()?;
        }
    }

    Ok(())
}

fn n1_delete_object_version_on_conn(
    conn: &mut Connection,
    object_id: &str,
    version_id: &str,
) -> Result<(), StorageError> {
    let mut delete_shards = conn.prepare("DELETE FROM part_shards WHERE version_id = ?1")?;
    delete_shards.bind_text(1, version_id)?;
    delete_shards.step_done()?;

    let mut delete_parts = conn.prepare("DELETE FROM object_parts WHERE version_id = ?1")?;
    delete_parts.bind_text(1, version_id)?;
    delete_parts.step_done()?;

    let mut delete_version =
        conn.prepare("DELETE FROM object_versions WHERE object_id = ?1 AND version_id = ?2")?;
    delete_version.bind_text(1, object_id)?;
    delete_version.bind_text(2, version_id)?;
    delete_version.step_done()
}

fn n1_set_object_version_state_on_conn(
    conn: &mut Connection,
    state: &crate::n1::ObjectVersionState,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare(
        "INSERT INTO object_version_states(
            object_id, version_id, state, generation, updated_at_unix
         )
         VALUES(?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(object_id, version_id) DO UPDATE SET
            state = excluded.state,
            generation = excluded.generation,
            updated_at_unix = excluded.updated_at_unix",
    )?;
    stmt.bind_text(1, &state.object_id)?;
    stmt.bind_text(2, &state.version_id)?;
    stmt.bind_text(3, state.state.as_str())?;
    stmt.bind_i64(4, u64_to_i64(state.generation)?)?;
    stmt.bind_i64(5, state.updated_at_unix)?;
    stmt.step_done()
}

fn n1_delete_object_version_state_on_conn(
    conn: &mut Connection,
    object_id: &str,
    version_id: &str,
) -> Result<(), StorageError> {
    let mut stmt =
        conn.prepare("DELETE FROM object_version_states WHERE object_id = ?1 AND version_id = ?2")?;
    stmt.bind_text(1, object_id)?;
    stmt.bind_text(2, version_id)?;
    stmt.step_done()
}

fn n1_upsert_object_head_on_conn(
    conn: &mut Connection,
    head: &crate::n1::ObjectHead,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare(
        "INSERT INTO object_heads(
            object_id, object_key, head_version_id, owner_uid, content_type,
            total_size_bytes, state, generation, created_at_unix, updated_at_unix
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(object_id) DO UPDATE SET
            object_key = excluded.object_key,
            head_version_id = excluded.head_version_id,
            owner_uid = excluded.owner_uid,
            content_type = excluded.content_type,
            total_size_bytes = excluded.total_size_bytes,
            state = excluded.state,
            generation = excluded.generation,
            created_at_unix = excluded.created_at_unix,
            updated_at_unix = excluded.updated_at_unix",
    )?;
    stmt.bind_text(1, &head.object_id)?;
    stmt.bind_text(2, &head.object_key)?;
    stmt.bind_text(3, &head.head_version_id)?;
    bind_optional_text(&mut stmt, 4, non_empty(&head.owner_uid))?;
    bind_optional_text(&mut stmt, 5, non_empty(&head.content_type))?;
    stmt.bind_i64(6, u64_to_i64(head.total_size_bytes)?)?;
    stmt.bind_text(7, head.state.as_str())?;
    stmt.bind_i64(8, u64_to_i64(head.generation)?)?;
    stmt.bind_i64(9, head.created_at_unix)?;
    stmt.bind_i64(10, head.updated_at_unix)?;
    stmt.step_done()
}

fn n1_delete_object_head_on_conn(
    conn: &mut Connection,
    object_id: &str,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare("DELETE FROM object_heads WHERE object_id = ?1")?;
    stmt.bind_text(1, object_id)?;
    stmt.step_done()
}

fn n1_get_object_version_by_key_on_conn(
    conn: &mut Connection,
    object_key: &str,
    version_id: &str,
) -> Result<Option<crate::n1::ObjectVersion>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT object_id
         FROM object_versions
         WHERE object_key = ?1 AND version_id = ?2
         ORDER BY created_at_unix DESC
         LIMIT 1",
    )?;
    stmt.bind_text(1, object_key)?;
    stmt.bind_text(2, version_id)?;
    let object_id = match stmt.step()? {
        StepResult::Row => stmt.column_text(0).unwrap_or_default(),
        StepResult::Done => return Ok(None),
    };
    drop(stmt);
    n1_get_object_version_on_conn(conn, &object_id, version_id)
}

fn n1_get_object_version_summary_by_key_on_conn(
    conn: &mut Connection,
    object_key: &str,
    version_id: &str,
) -> Result<Option<crate::n1::ObjectVersion>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT object_id
         FROM object_versions
         WHERE object_key = ?1 AND version_id = ?2
         ORDER BY created_at_unix DESC
         LIMIT 1",
    )?;
    stmt.bind_text(1, object_key)?;
    stmt.bind_text(2, version_id)?;
    let object_id = match stmt.step()? {
        StepResult::Row => stmt.column_text(0).unwrap_or_default(),
        StepResult::Done => return Ok(None),
    };
    drop(stmt);
    n1_get_object_version_summary_on_conn(conn, &object_id, version_id)
}

fn n1_get_object_version_on_conn(
    conn: &mut Connection,
    object_id: &str,
    version_id: &str,
) -> Result<Option<crate::n1::ObjectVersion>, StorageError> {
    n1_get_object_version_row_on_conn(conn, object_id, version_id, true)
}

fn n1_get_object_version_summary_on_conn(
    conn: &mut Connection,
    object_id: &str,
    version_id: &str,
) -> Result<Option<crate::n1::ObjectVersion>, StorageError> {
    n1_get_object_version_row_on_conn(conn, object_id, version_id, false)
}

fn n1_get_object_version_row_on_conn(
    conn: &mut Connection,
    object_id: &str,
    version_id: &str,
    include_parts: bool,
) -> Result<Option<crate::n1::ObjectVersion>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT object_key, upload_session_id, owner_uid, content_type,
                total_size_bytes, created_at_unix, committed_at_unix,
                committed_at_unix IS NULL,
                is_delete_marker, stripe_size_bytes, checksum, metadata_json
         FROM object_versions
         WHERE object_id = ?1 AND version_id = ?2",
    )?;
    stmt.bind_text(1, object_id)?;
    stmt.bind_text(2, version_id)?;
    let row = match stmt.step()? {
        StepResult::Row => Some((
            stmt.column_text(0).unwrap_or_default(),
            stmt.column_text(1).unwrap_or_default(),
            stmt.column_text(2).unwrap_or_default(),
            stmt.column_text(3).unwrap_or_default(),
            i64_to_u64(stmt.column_i64(4))?,
            stmt.column_i64(5),
            if stmt.column_i32(7) != 0 {
                None
            } else {
                Some(stmt.column_i64(6))
            },
            stmt.column_i32(8) != 0,
            i64_to_u64(stmt.column_i64(9))?,
            non_empty_owned(stmt.column_text(10)),
            non_empty_owned(stmt.column_text(11)),
        )),
        StepResult::Done => None,
    };
    drop(stmt);
    let Some((
        object_key,
        upload_session_id,
        owner_uid,
        content_type,
        total_size_bytes,
        created_at_unix,
        committed_at_unix,
        is_delete_marker,
        stripe_size_bytes,
        checksum,
        metadata_json,
    )) = row
    else {
        return Ok(None);
    };
    let parts = if include_parts {
        n1_select_parts_on_conn(conn, version_id)?
    } else {
        Vec::new()
    };
    Ok(Some(crate::n1::ObjectVersion {
        object_id: object_id.to_string(),
        object_key,
        version_id: version_id.to_string(),
        upload_session_id,
        owner_uid,
        content_type,
        total_size_bytes,
        created_at_unix,
        committed_at_unix,
        is_delete_marker,
        stripe_size_bytes,
        checksum,
        metadata_json,
        parts,
    }))
}

fn n1_get_object_head_on_conn(
    conn: &mut Connection,
    object_id: &str,
) -> Result<Option<crate::n1::ObjectHead>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT object_id, object_key, head_version_id, owner_uid, content_type,
                total_size_bytes, state, generation, created_at_unix, updated_at_unix
         FROM object_heads
         WHERE object_id = ?1",
    )?;
    stmt.bind_text(1, object_id)?;
    match stmt.step()? {
        StepResult::Row => n1_object_head_from_stmt(&stmt).map(Some),
        StepResult::Done => Ok(None),
    }
}

fn n1_get_object_head_by_key_on_conn(
    conn: &mut Connection,
    object_key: &str,
) -> Result<Option<crate::n1::ObjectHead>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT object_id, object_key, head_version_id, owner_uid, content_type,
                total_size_bytes, state, generation, created_at_unix, updated_at_unix
         FROM object_heads
         WHERE object_key = ?1
         ORDER BY updated_at_unix DESC, generation DESC
         LIMIT 1",
    )?;
    stmt.bind_text(1, object_key)?;
    match stmt.step()? {
        StepResult::Row => n1_object_head_from_stmt(&stmt).map(Some),
        StepResult::Done => Ok(None),
    }
}

fn n1_list_object_heads_on_conn(
    conn: &mut Connection,
    state: crate::n1::ObjectState,
    prefix: Option<&str>,
    after_key: Option<&str>,
    limit: usize,
) -> Result<Vec<crate::n1::ObjectHead>, StorageError> {
    let mut sql = String::from(
        "SELECT object_id, object_key, head_version_id, owner_uid, content_type,
                total_size_bytes, state, generation, created_at_unix, updated_at_unix
         FROM object_heads
         WHERE state = ?1",
    );
    let upper = prefix.and_then(prefix_upper_bound);
    if prefix.is_some() {
        sql.push_str(" AND object_key >= ?2");
        if upper.is_some() {
            sql.push_str(" AND object_key < ?3");
        }
    }
    if after_key.is_some() {
        let idx = if prefix.is_some() {
            if upper.is_some() { 4 } else { 3 }
        } else {
            2
        };
        sql.push_str(&format!(" AND object_key > ?{idx}"));
    }
    sql.push_str(" ORDER BY object_key ASC LIMIT ?");
    let limit_idx = 2
        + i32::from(prefix.is_some())
        + i32::from(upper.is_some())
        + i32::from(after_key.is_some());
    sql.push_str(&limit_idx.to_string());

    let mut stmt = conn.prepare(&sql)?;
    stmt.bind_text(1, state.as_str())?;
    let mut idx = 2;
    if let Some(prefix) = prefix {
        stmt.bind_text(idx, prefix)?;
        idx += 1;
        if let Some(upper) = upper.as_deref() {
            stmt.bind_text(idx, upper)?;
            idx += 1;
        }
    }
    if let Some(after_key) = after_key {
        stmt.bind_text(idx, after_key)?;
        idx += 1;
    }
    stmt.bind_i64(idx, u64_to_i64(limit.max(1) as u64)?)?;
    let mut heads = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        heads.push(n1_object_head_from_stmt(&stmt)?);
    }
    Ok(heads)
}

fn n1_get_object_version_state_on_conn(
    conn: &mut Connection,
    object_id: &str,
    version_id: &str,
) -> Result<Option<crate::n1::ObjectVersionState>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT state, generation, updated_at_unix
         FROM object_version_states
         WHERE object_id = ?1 AND version_id = ?2",
    )?;
    stmt.bind_text(1, object_id)?;
    stmt.bind_text(2, version_id)?;
    match stmt.step()? {
        StepResult::Row => {
            let state = stmt
                .column_text(0)
                .and_then(|value| crate::n1::ObjectState::from_str(&value))
                .ok_or_else(|| {
                    StorageError::InvalidInput(
                        "object_version_states contains an invalid state".to_string(),
                    )
                })?;
            Ok(Some(crate::n1::ObjectVersionState {
                object_id: object_id.to_string(),
                version_id: version_id.to_string(),
                state,
                generation: i64_to_u64(stmt.column_i64(1))?,
                updated_at_unix: stmt.column_i64(2),
            }))
        }
        StepResult::Done => Ok(None),
    }
}

fn n1_object_head_from_stmt(
    stmt: &crate::ffi::Statement,
) -> Result<crate::n1::ObjectHead, StorageError> {
    let state = stmt
        .column_text(6)
        .and_then(|value| crate::n1::ObjectState::from_str(&value))
        .ok_or_else(|| {
            StorageError::InvalidInput("object_heads contains an invalid state".to_string())
        })?;
    Ok(crate::n1::ObjectHead {
        object_id: stmt.column_text(0).unwrap_or_default(),
        object_key: stmt.column_text(1).unwrap_or_default(),
        head_version_id: stmt.column_text(2).unwrap_or_default(),
        owner_uid: stmt.column_text(3).unwrap_or_default(),
        content_type: stmt.column_text(4).unwrap_or_default(),
        total_size_bytes: i64_to_u64(stmt.column_i64(5))?,
        state,
        generation: i64_to_u64(stmt.column_i64(7))?,
        created_at_unix: stmt.column_i64(8),
        updated_at_unix: stmt.column_i64(9),
    })
}

fn n1_select_parts_on_conn(
    conn: &mut Connection,
    version_id: &str,
) -> Result<Vec<crate::n1::ObjectPart>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT part_index, size_bytes, created_at_unix, checksum
         FROM object_parts
         WHERE version_id = ?1
         ORDER BY part_index ASC",
    )?;
    stmt.bind_text(1, version_id)?;
    let mut parts = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        parts.push(crate::n1::ObjectPart {
            part_index: i64_to_u64(stmt.column_i64(0))?,
            size_bytes: i64_to_u64(stmt.column_i64(1))?,
            created_at_unix: stmt.column_i64(2),
            checksum: non_empty_owned(stmt.column_text(3)),
            shards: Vec::new(),
        });
    }
    drop(stmt);

    for part in &mut parts {
        part.shards = n1_select_shards_on_conn(conn, version_id, part.part_index)?;
    }
    Ok(parts)
}

fn n1_select_shards_on_conn(
    conn: &mut Connection,
    version_id: &str,
    part_index: u64,
) -> Result<Vec<crate::n1::PartShard>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT shard_index, file_name, absolute_path, relative_path, blob_root_id,
                size_bytes, checksum, is_parity, created_at_unix
         FROM part_shards
         WHERE version_id = ?1 AND part_index = ?2
         ORDER BY shard_index ASC",
    )?;
    stmt.bind_text(1, version_id)?;
    stmt.bind_i64(2, u64_to_i64(part_index)?)?;
    let mut shards = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        shards.push(crate::n1::PartShard {
            shard_index: i64_to_u64(stmt.column_i64(0))?,
            file_name: stmt.column_text(1).unwrap_or_default(),
            absolute_path: stmt.column_text(2).unwrap_or_default(),
            relative_path: non_empty_owned(stmt.column_text(3)),
            blob_root_id: non_empty_owned(stmt.column_text(4)),
            size_bytes: i64_to_u64(stmt.column_i64(5))?,
            checksum: stmt.column_text(6).unwrap_or_default(),
            is_parity: stmt.column_i32(7) != 0,
            created_at_unix: stmt.column_i64(8),
        });
    }
    Ok(shards)
}

fn n1_replace_namespace_entries_on_conn(
    conn: &mut Connection,
    entries: &[crate::n1::NamespaceEntry],
) -> Result<(), StorageError> {
    conn.exec("DELETE FROM namespace_entries")?;
    for entry in entries {
        n1_upsert_namespace_entry_on_conn(conn, entry)?;
    }
    Ok(())
}

fn n1_upsert_namespace_entry_on_conn(
    conn: &mut Connection,
    entry: &crate::n1::NamespaceEntry,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare(
        "INSERT INTO namespace_entries(
            path, parent_path, name, kind, object_id, head_version_id,
            lifecycle, trash_entry_id, deleted_at_unix, created_at_unix,
            updated_at_unix
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(path) DO UPDATE SET
            parent_path = excluded.parent_path,
            name = excluded.name,
            kind = excluded.kind,
            object_id = excluded.object_id,
            head_version_id = excluded.head_version_id,
            lifecycle = excluded.lifecycle,
            trash_entry_id = excluded.trash_entry_id,
            deleted_at_unix = excluded.deleted_at_unix,
            created_at_unix = excluded.created_at_unix,
            updated_at_unix = excluded.updated_at_unix",
    )?;
    stmt.bind_text(1, &entry.path)?;
    stmt.bind_text(2, &entry.parent_path)?;
    stmt.bind_text(3, &entry.name)?;
    stmt.bind_text(4, entry.kind.as_str())?;
    bind_optional_text(&mut stmt, 5, entry.object_id.as_deref())?;
    bind_optional_text(&mut stmt, 6, entry.head_version_id.as_deref())?;
    stmt.bind_text(7, entry.lifecycle.as_str())?;
    bind_optional_text(&mut stmt, 8, entry.trash_entry_id.as_deref())?;
    bind_optional_i64(&mut stmt, 9, entry.deleted_at_unix)?;
    stmt.bind_i64(10, entry.created_at_unix)?;
    stmt.bind_i64(11, entry.updated_at_unix)?;
    stmt.step_done()
}

fn n1_delete_namespace_entry_on_conn(
    conn: &mut Connection,
    path: &str,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare("DELETE FROM namespace_entries WHERE path = ?1")?;
    stmt.bind_text(1, path)?;
    stmt.step_done()
}

fn n1_rename_namespace_subtree_on_conn(
    conn: &mut Connection,
    source_path: &str,
    destination_path: &str,
    changed_at_unix: i64,
) -> Result<crate::n1::NamespaceMutationStats, StorageError> {
    let stats = n1_namespace_scope_stats_on_conn(
        conn,
        Some(source_path),
        crate::n1::NamespaceLifecycle::Live,
        None,
    )?;
    if stats.affected_paths == 0 {
        return Ok(stats);
    }

    let child_prefix = namespace_child_prefix(source_path);
    let child_upper = prefix_upper_bound(&child_prefix).unwrap_or_default();
    let destination_parent = namespace_parent_path(destination_path);
    let destination_name = namespace_base_name(destination_path);
    let mut stmt = conn.prepare(
        "UPDATE namespace_entries
         SET path = CASE
                WHEN path = ?1 THEN ?2
                ELSE ?2 || substr(path, length(?1) + 1)
             END,
             parent_path = CASE
                WHEN path = ?1 THEN ?3
                WHEN parent_path = ?1 THEN ?2
                WHEN parent_path >= ?6 AND (?7 = '' OR parent_path < ?7)
                    THEN ?2 || substr(parent_path, length(?1) + 1)
                ELSE parent_path
             END,
             name = CASE WHEN path = ?1 THEN ?4 ELSE name END,
             updated_at_unix = ?5
         WHERE lifecycle = 'live'
           AND (path = ?1 OR (path >= ?6 AND (?7 = '' OR path < ?7)))",
    )?;
    stmt.bind_text(1, source_path)?;
    stmt.bind_text(2, destination_path)?;
    stmt.bind_text(3, &destination_parent)?;
    stmt.bind_text(4, &destination_name)?;
    stmt.bind_i64(5, changed_at_unix)?;
    stmt.bind_text(6, &child_prefix)?;
    stmt.bind_text(7, &child_upper)?;
    stmt.step_done()?;
    Ok(stats)
}

fn n1_set_namespace_lifecycle_subtree_on_conn(
    conn: &mut Connection,
    root_path: &str,
    from: crate::n1::NamespaceLifecycle,
    target: crate::n1::NamespaceLifecycle,
    changed_at_unix: i64,
    trash_entry_id: Option<&str>,
) -> Result<crate::n1::NamespaceMutationStats, StorageError> {
    let filter_trash_entry_id = if matches!(target, crate::n1::NamespaceLifecycle::Trashed) {
        None
    } else {
        trash_entry_id
    };
    let stats =
        n1_namespace_scope_stats_on_conn(conn, Some(root_path), from, filter_trash_entry_id)?;
    if stats.affected_paths == 0 {
        return Ok(stats);
    }

    let scope = namespace_scope_clause(Some(root_path), filter_trash_entry_id, 2);
    let target_idx = scope.next_idx;
    let changed_idx = target_idx + 1;
    let deleted_idx = target_idx + 2;
    let trash_set_idx = target_idx + 3;
    let mut stmt = conn.prepare(
        &format!(
            "UPDATE namespace_entries
         SET lifecycle = ?6,
             updated_at_unix = ?7,
             deleted_at_unix = ?8,
             trash_entry_id = ?9
         WHERE lifecycle = ?1{}",
            scope.sql
        )
        .replace("?6", &format!("?{target_idx}"))
        .replace("?7", &format!("?{changed_idx}"))
        .replace("?8", &format!("?{deleted_idx}"))
        .replace("?9", &format!("?{trash_set_idx}")),
    )?;
    stmt.bind_text(1, from.as_str())?;
    bind_namespace_scope_clause(&mut stmt, &scope)?;
    stmt.bind_text(target_idx, target.as_str())?;
    stmt.bind_i64(changed_idx, changed_at_unix)?;
    if matches!(target, crate::n1::NamespaceLifecycle::Trashed) {
        stmt.bind_i64(deleted_idx, changed_at_unix)?;
        bind_optional_text(&mut stmt, trash_set_idx, trash_entry_id)?;
    } else {
        stmt.bind_null(deleted_idx)?;
        stmt.bind_null(trash_set_idx)?;
    }
    stmt.step_done()?;
    Ok(stats)
}

fn n1_purge_namespace_subtree_on_conn(
    conn: &mut Connection,
    root_path: Option<&str>,
    trash_entry_id: Option<&str>,
) -> Result<crate::n1::NamespaceMutationStats, StorageError> {
    let stats = n1_namespace_scope_stats_on_conn(
        conn,
        root_path,
        crate::n1::NamespaceLifecycle::Trashed,
        trash_entry_id,
    )?;
    if stats.affected_paths == 0 {
        return Ok(stats);
    }

    let scope = namespace_scope_clause(root_path, trash_entry_id, 2);
    let mut stmt = conn.prepare(&format!(
        "DELETE FROM namespace_entries
         WHERE lifecycle = ?1{}",
        scope.sql
    ))?;
    stmt.bind_text(1, crate::n1::NamespaceLifecycle::Trashed.as_str())?;
    bind_namespace_scope_clause(&mut stmt, &scope)?;
    stmt.step_done()?;
    Ok(stats)
}

fn n1_upsert_namespace_node_on_conn(
    conn: &mut Connection,
    node: &crate::n1::NamespaceNode,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare(
        "INSERT INTO namespace_nodes(
            node_id, parent_node_id, name, kind, object_id, head_version_id,
            created_at_unix, updated_at_unix
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(node_id) DO UPDATE SET
            parent_node_id = excluded.parent_node_id,
            name = excluded.name,
            kind = excluded.kind,
            object_id = excluded.object_id,
            head_version_id = excluded.head_version_id,
            updated_at_unix = excluded.updated_at_unix",
    )?;
    stmt.bind_text(1, &node.node_id)?;
    bind_optional_text(&mut stmt, 2, node.parent_node_id.as_deref())?;
    stmt.bind_text(3, &node.name)?;
    stmt.bind_text(4, node.kind.as_str())?;
    bind_optional_text(&mut stmt, 5, node.object_id.as_deref())?;
    bind_optional_text(&mut stmt, 6, node.head_version_id.as_deref())?;
    stmt.bind_i64(7, node.created_at_unix)?;
    stmt.bind_i64(8, node.updated_at_unix)?;
    stmt.step_done()
}

fn n1_delete_namespace_node_on_conn(
    conn: &mut Connection,
    node_id: &str,
) -> Result<(), StorageError> {
    let mut delete_head = conn.prepare("DELETE FROM namespace_file_heads WHERE node_id = ?1")?;
    delete_head.bind_text(1, node_id)?;
    delete_head.step_done()?;

    let mut delete_node = conn.prepare("DELETE FROM namespace_nodes WHERE node_id = ?1")?;
    delete_node.bind_text(1, node_id)?;
    delete_node.step_done()
}

fn n1_upsert_namespace_file_head_on_conn(
    conn: &mut Connection,
    head: &crate::n1::NamespaceFileHead,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare(
        "INSERT INTO namespace_file_heads(
            node_id, object_id, version_id, upload_session_id, owner_uid,
            content_type, total_size_bytes, created_at_unix, is_delete_marker,
            stripe_size_bytes, updated_at_unix
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(node_id) DO UPDATE SET
            object_id = excluded.object_id,
            version_id = excluded.version_id,
            upload_session_id = excluded.upload_session_id,
            owner_uid = excluded.owner_uid,
            content_type = excluded.content_type,
            total_size_bytes = excluded.total_size_bytes,
            created_at_unix = excluded.created_at_unix,
            is_delete_marker = excluded.is_delete_marker,
            stripe_size_bytes = excluded.stripe_size_bytes,
            updated_at_unix = excluded.updated_at_unix",
    )?;
    stmt.bind_text(1, &head.node_id)?;
    stmt.bind_text(2, &head.object_id)?;
    stmt.bind_text(3, &head.version_id)?;
    bind_optional_text(&mut stmt, 4, non_empty(&head.upload_session_id))?;
    bind_optional_text(&mut stmt, 5, non_empty(&head.owner_uid))?;
    bind_optional_text(&mut stmt, 6, non_empty(&head.content_type))?;
    stmt.bind_i64(7, u64_to_i64(head.total_size_bytes)?)?;
    stmt.bind_i64(8, head.created_at_unix)?;
    stmt.bind_i32(9, i32::from(head.is_delete_marker))?;
    stmt.bind_i64(10, u64_to_i64(head.stripe_size_bytes)?)?;
    stmt.bind_i64(11, head.updated_at_unix)?;
    stmt.step_done()
}

fn n1_get_namespace_file_head_on_conn(
    conn: &mut Connection,
    node_id: &str,
) -> Result<Option<crate::n1::NamespaceFileHead>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT node_id, object_id, version_id, upload_session_id, owner_uid,
                content_type, total_size_bytes, created_at_unix, is_delete_marker,
                stripe_size_bytes, updated_at_unix
         FROM namespace_file_heads
         WHERE node_id = ?1",
    )?;
    stmt.bind_text(1, node_id)?;
    match stmt.step()? {
        StepResult::Row => n1_namespace_file_head_from_stmt(&stmt).map(Some),
        StepResult::Done => Ok(None),
    }
}

fn n1_list_namespace_file_heads_on_conn(
    conn: &mut Connection,
    after_node_id: Option<&str>,
    limit: usize,
) -> Result<Vec<crate::n1::NamespaceFileHead>, StorageError> {
    let sql = if after_node_id.is_some() {
        "SELECT node_id, object_id, version_id, upload_session_id, owner_uid,
                content_type, total_size_bytes, created_at_unix, is_delete_marker,
                stripe_size_bytes, updated_at_unix
         FROM namespace_file_heads
         WHERE node_id > ?1
         ORDER BY node_id ASC
         LIMIT ?2"
    } else {
        "SELECT node_id, object_id, version_id, upload_session_id, owner_uid,
                content_type, total_size_bytes, created_at_unix, is_delete_marker,
                stripe_size_bytes, updated_at_unix
         FROM namespace_file_heads
         ORDER BY node_id ASC
         LIMIT ?1"
    };
    let mut stmt = conn.prepare(sql)?;
    if let Some(after_node_id) = after_node_id {
        stmt.bind_text(1, after_node_id)?;
        stmt.bind_i64(2, u64_to_i64(limit.max(1) as u64)?)?;
    } else {
        stmt.bind_i64(1, u64_to_i64(limit.max(1) as u64)?)?;
    }
    let mut heads = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        heads.push(n1_namespace_file_head_from_stmt(&stmt)?);
    }
    Ok(heads)
}

fn n1_delete_namespace_file_head_on_conn(
    conn: &mut Connection,
    node_id: &str,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare("DELETE FROM namespace_file_heads WHERE node_id = ?1")?;
    stmt.bind_text(1, node_id)?;
    stmt.step_done()
}

fn n1_move_namespace_node_on_conn(
    conn: &mut Connection,
    node_id: &str,
    parent_node_id: &str,
    name: &str,
    changed_at_unix: i64,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare(
        "UPDATE namespace_nodes
         SET parent_node_id = ?2, name = ?3, updated_at_unix = ?4
         WHERE node_id = ?1",
    )?;
    stmt.bind_text(1, node_id)?;
    stmt.bind_text(2, parent_node_id)?;
    stmt.bind_text(3, name)?;
    stmt.bind_i64(4, changed_at_unix)?;
    stmt.step_done()
}

fn n1_get_namespace_node_on_conn(
    conn: &mut Connection,
    node_id: &str,
) -> Result<Option<crate::n1::NamespaceNode>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT node_id, parent_node_id, name, kind, object_id, head_version_id,
                created_at_unix, updated_at_unix
         FROM namespace_nodes
         WHERE node_id = ?1",
    )?;
    stmt.bind_text(1, node_id)?;
    match stmt.step()? {
        StepResult::Row => n1_namespace_node_from_stmt(&stmt, 0).map(Some),
        StepResult::Done => Ok(None),
    }
}

fn n1_get_namespace_child_on_conn(
    conn: &mut Connection,
    parent_node_id: &str,
    name: &str,
) -> Result<Option<crate::n1::NamespaceNode>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT node_id, parent_node_id, name, kind, object_id, head_version_id,
                created_at_unix, updated_at_unix
         FROM namespace_nodes
         WHERE parent_node_id = ?1 AND name = ?2",
    )?;
    stmt.bind_text(1, parent_node_id)?;
    stmt.bind_text(2, name)?;
    match stmt.step()? {
        StepResult::Row => n1_namespace_node_from_stmt(&stmt, 0).map(Some),
        StepResult::Done => Ok(None),
    }
}

fn n1_list_namespace_node_children_on_conn(
    conn: &mut Connection,
    parent_node_id: &str,
    after_name: Option<&str>,
    limit: usize,
) -> Result<Vec<crate::n1::NamespaceNode>, StorageError> {
    let sql = if after_name.is_some() {
        "SELECT node_id, parent_node_id, name, kind, object_id, head_version_id,
                created_at_unix, updated_at_unix
         FROM namespace_nodes
         WHERE parent_node_id = ?1 AND name > ?2
         ORDER BY name ASC, node_id ASC
         LIMIT ?3"
    } else {
        "SELECT node_id, parent_node_id, name, kind, object_id, head_version_id,
                created_at_unix, updated_at_unix
         FROM namespace_nodes
         WHERE parent_node_id = ?1
         ORDER BY name ASC, node_id ASC
         LIMIT ?2"
    };
    let mut stmt = conn.prepare(sql)?;
    stmt.bind_text(1, parent_node_id)?;
    if let Some(after_name) = after_name {
        stmt.bind_text(2, after_name)?;
        stmt.bind_i64(3, u64_to_i64(limit.max(1) as u64)?)?;
    } else {
        stmt.bind_i64(2, u64_to_i64(limit.max(1) as u64)?)?;
    }
    let mut nodes = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        nodes.push(n1_namespace_node_from_stmt(&stmt, 0)?);
    }
    Ok(nodes)
}

fn n1_list_namespace_nodes_on_conn(
    conn: &mut Connection,
    after_node_id: Option<&str>,
    limit: usize,
) -> Result<Vec<crate::n1::NamespaceNode>, StorageError> {
    let sql = if after_node_id.is_some() {
        "SELECT node_id, parent_node_id, name, kind, object_id, head_version_id,
                created_at_unix, updated_at_unix
         FROM namespace_nodes
         WHERE node_id > ?1
         ORDER BY node_id ASC
         LIMIT ?2"
    } else {
        "SELECT node_id, parent_node_id, name, kind, object_id, head_version_id,
                created_at_unix, updated_at_unix
         FROM namespace_nodes
         ORDER BY node_id ASC
         LIMIT ?1"
    };
    let mut stmt = conn.prepare(sql)?;
    if let Some(after_node_id) = after_node_id {
        stmt.bind_text(1, after_node_id)?;
        stmt.bind_i64(2, u64_to_i64(limit.max(1) as u64)?)?;
    } else {
        stmt.bind_i64(1, u64_to_i64(limit.max(1) as u64)?)?;
    }
    let mut nodes = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        nodes.push(n1_namespace_node_from_stmt(&stmt, 0)?);
    }
    Ok(nodes)
}

fn n1_list_namespace_subtree_nodes_on_conn(
    conn: &mut Connection,
    root_node_id: &str,
    base_path: &str,
    after_path: Option<&str>,
    limit: usize,
) -> Result<Vec<(String, crate::n1::NamespaceNode)>, StorageError> {
    let mut sql = String::from(
        "WITH RECURSIVE subtree(node_id, path) AS (
            SELECT node_id, ?2
            FROM namespace_nodes
            WHERE node_id = ?1
            UNION ALL
            SELECT child.node_id,
                   CASE
                       WHEN subtree.path = '' THEN child.name
                       ELSE subtree.path || '/' || child.name
                   END
            FROM namespace_nodes child
            JOIN subtree ON child.parent_node_id = subtree.node_id
         )
         SELECT subtree.path, n.node_id, n.parent_node_id, n.name, n.kind,
                n.object_id, n.head_version_id, n.created_at_unix, n.updated_at_unix
         FROM subtree
         JOIN namespace_nodes n ON n.node_id = subtree.node_id
         WHERE 1 = 1",
    );
    let after_path = after_path.filter(|value| !value.is_empty());
    if after_path.is_some() {
        sql.push_str(" AND subtree.path > ?3");
    }
    let limit_idx = if after_path.is_some() { 4 } else { 3 };
    sql.push_str(&format!(" ORDER BY subtree.path ASC LIMIT ?{limit_idx}"));

    let mut stmt = conn.prepare(&sql)?;
    stmt.bind_text(1, root_node_id)?;
    stmt.bind_text(2, base_path)?;
    if let Some(after_path) = after_path {
        stmt.bind_text(3, after_path)?;
    }
    stmt.bind_i64(limit_idx, u64_to_i64(limit.max(1) as u64)?)?;
    let mut nodes = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        nodes.push((
            stmt.column_text(0).unwrap_or_default(),
            n1_namespace_node_from_stmt(&stmt, 1)?,
        ));
    }
    Ok(nodes)
}

fn n1_list_namespace_node_object_summaries_on_conn(
    conn: &mut Connection,
    root_node_id: &str,
    base_path: &str,
    prefix: Option<&str>,
    after_path: Option<&str>,
    limit: usize,
) -> Result<Vec<crate::n1::NamespaceObjectSummary>, StorageError> {
    let mut sql = String::from(
        "WITH RECURSIVE subtree(node_id, path) AS (
            SELECT node_id, ?2
            FROM namespace_nodes
            WHERE node_id = ?1
            UNION ALL
            SELECT child.node_id,
                   CASE
                       WHEN subtree.path = '' THEN child.name
                       ELSE subtree.path || '/' || child.name
                   END
            FROM namespace_nodes child
            JOIN subtree ON child.parent_node_id = subtree.node_id
         )
         SELECT subtree.path,
                fh.version_id,
                COALESCE(fh.upload_session_id, ''),
                COALESCE(fh.owner_uid, ''),
                COALESCE(fh.content_type, ''),
                fh.total_size_bytes,
                fh.created_at_unix,
                fh.is_delete_marker,
                fh.stripe_size_bytes
         FROM subtree
         JOIN namespace_nodes n ON n.node_id = subtree.node_id
         JOIN namespace_file_heads fh
            ON fh.node_id = n.node_id
           AND fh.object_id = n.object_id
           AND fh.version_id = n.head_version_id
         WHERE n.kind = 'file'
           AND n.object_id IS NOT NULL
           AND n.head_version_id IS NOT NULL",
    );
    let mut next_idx = 3;
    let prefix = prefix.filter(|value| !value.is_empty());
    let upper = prefix.and_then(prefix_upper_bound);
    let prefix_idx = prefix.map(|_| {
        let idx = next_idx;
        next_idx += 1;
        sql.push_str(&format!(" AND subtree.path >= ?{idx}"));
        idx
    });
    let upper_idx = upper.as_ref().map(|_| {
        let idx = next_idx;
        next_idx += 1;
        sql.push_str(&format!(" AND subtree.path < ?{idx}"));
        idx
    });
    let after_path = after_path.filter(|value| !value.is_empty());
    let after_idx = after_path.map(|_| {
        let idx = next_idx;
        next_idx += 1;
        sql.push_str(&format!(" AND subtree.path > ?{idx}"));
        idx
    });
    let limit_idx = next_idx;
    sql.push_str(&format!(" ORDER BY subtree.path ASC LIMIT ?{limit_idx}"));

    let mut stmt = conn.prepare(&sql)?;
    stmt.bind_text(1, root_node_id)?;
    stmt.bind_text(2, base_path)?;
    if let (Some(idx), Some(value)) = (prefix_idx, prefix) {
        stmt.bind_text(idx, value)?;
    }
    if let (Some(idx), Some(value)) = (upper_idx, upper.as_deref()) {
        stmt.bind_text(idx, value)?;
    }
    if let (Some(idx), Some(value)) = (after_idx, after_path) {
        stmt.bind_text(idx, value)?;
    }
    stmt.bind_i64(limit_idx, u64_to_i64(limit.max(1) as u64)?)?;
    let mut summaries = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        summaries.push(n1_namespace_object_summary_from_stmt(&stmt)?);
    }
    Ok(summaries)
}

fn n1_upsert_namespace_trash_root_on_conn(
    conn: &mut Connection,
    root: &crate::n1::NamespaceTrashRoot,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare(
        "INSERT INTO namespace_trash_roots(
            trash_entry_id, root_node_id, original_parent_node_id, original_name,
            original_path, kind, object_id, head_version_id, deleted_at_unix,
            retired_at_unix, purged_at_unix, state
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(trash_entry_id) DO UPDATE SET
            root_node_id = excluded.root_node_id,
            original_parent_node_id = excluded.original_parent_node_id,
            original_name = excluded.original_name,
            original_path = excluded.original_path,
            kind = excluded.kind,
            object_id = excluded.object_id,
            head_version_id = excluded.head_version_id,
            deleted_at_unix = excluded.deleted_at_unix,
            retired_at_unix = excluded.retired_at_unix,
            purged_at_unix = excluded.purged_at_unix,
            state = excluded.state",
    )?;
    stmt.bind_text(1, &root.trash_entry_id)?;
    stmt.bind_text(2, &root.root_node_id)?;
    stmt.bind_text(3, &root.original_parent_node_id)?;
    stmt.bind_text(4, &root.original_name)?;
    stmt.bind_text(5, &root.original_path)?;
    stmt.bind_text(6, root.kind.as_str())?;
    bind_optional_text(&mut stmt, 7, root.object_id.as_deref())?;
    bind_optional_text(&mut stmt, 8, root.head_version_id.as_deref())?;
    stmt.bind_i64(9, root.deleted_at_unix)?;
    bind_optional_i64(&mut stmt, 10, root.retired_at_unix)?;
    bind_optional_i64(&mut stmt, 11, root.purged_at_unix)?;
    stmt.bind_text(12, root.state.as_str())?;
    stmt.step_done()
}

fn n1_set_namespace_trash_root_state_on_conn(
    conn: &mut Connection,
    trash_entry_id: &str,
    state: crate::n1::TrashEntryState,
    changed_at_unix: i64,
) -> Result<(), StorageError> {
    let mut stmt = match state {
        crate::n1::TrashEntryState::Trashed => conn.prepare(
            "UPDATE namespace_trash_roots
             SET state = ?2, retired_at_unix = NULL, purged_at_unix = NULL
             WHERE trash_entry_id = ?1",
        )?,
        crate::n1::TrashEntryState::Retired => conn.prepare(
            "UPDATE namespace_trash_roots
             SET state = ?2, retired_at_unix = ?3
             WHERE trash_entry_id = ?1",
        )?,
        crate::n1::TrashEntryState::Purged => conn.prepare(
            "UPDATE namespace_trash_roots
             SET state = ?2, purged_at_unix = ?3
             WHERE trash_entry_id = ?1",
        )?,
    };
    stmt.bind_text(1, trash_entry_id)?;
    stmt.bind_text(2, state.as_str())?;
    if !matches!(state, crate::n1::TrashEntryState::Trashed) {
        stmt.bind_i64(3, changed_at_unix)?;
    }
    stmt.step_done()
}

fn n1_get_namespace_trash_root_on_conn(
    conn: &mut Connection,
    trash_entry_id: &str,
) -> Result<Option<crate::n1::NamespaceTrashRoot>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT trash_entry_id, root_node_id, original_parent_node_id, original_name,
                original_path, kind, object_id, head_version_id, deleted_at_unix,
                retired_at_unix, purged_at_unix, state
         FROM namespace_trash_roots
         WHERE trash_entry_id = ?1",
    )?;
    stmt.bind_text(1, trash_entry_id)?;
    match stmt.step()? {
        StepResult::Row => n1_namespace_trash_root_from_stmt(&stmt).map(Some),
        StepResult::Done => Ok(None),
    }
}

fn n1_list_namespace_trash_roots_on_conn(
    conn: &mut Connection,
    state: crate::n1::TrashEntryState,
    after_path: Option<&str>,
    limit: usize,
) -> Result<Vec<crate::n1::NamespaceTrashRoot>, StorageError> {
    let sql = if after_path.is_some() {
        "SELECT trash_entry_id, root_node_id, original_parent_node_id, original_name,
                original_path, kind, object_id, head_version_id, deleted_at_unix,
                retired_at_unix, purged_at_unix, state
         FROM namespace_trash_roots
         WHERE state = ?1 AND original_path > ?2
         ORDER BY original_path ASC, trash_entry_id ASC
         LIMIT ?3"
    } else {
        "SELECT trash_entry_id, root_node_id, original_parent_node_id, original_name,
                original_path, kind, object_id, head_version_id, deleted_at_unix,
                retired_at_unix, purged_at_unix, state
         FROM namespace_trash_roots
         WHERE state = ?1
         ORDER BY original_path ASC, trash_entry_id ASC
         LIMIT ?2"
    };
    let mut stmt = conn.prepare(sql)?;
    stmt.bind_text(1, state.as_str())?;
    if let Some(after_path) = after_path {
        stmt.bind_text(2, after_path)?;
        stmt.bind_i64(3, u64_to_i64(limit.max(1) as u64)?)?;
    } else {
        stmt.bind_i64(2, u64_to_i64(limit.max(1) as u64)?)?;
    }
    let mut roots = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        roots.push(n1_namespace_trash_root_from_stmt(&stmt)?);
    }
    Ok(roots)
}

fn n1_list_namespace_trash_roots_all_on_conn(
    conn: &mut Connection,
    after_trash_entry_id: Option<&str>,
    limit: usize,
) -> Result<Vec<crate::n1::NamespaceTrashRoot>, StorageError> {
    let sql = if after_trash_entry_id.is_some() {
        "SELECT trash_entry_id, root_node_id, original_parent_node_id, original_name,
                original_path, kind, object_id, head_version_id, deleted_at_unix,
                retired_at_unix, purged_at_unix, state
         FROM namespace_trash_roots
         WHERE trash_entry_id > ?1
         ORDER BY trash_entry_id ASC
         LIMIT ?2"
    } else {
        "SELECT trash_entry_id, root_node_id, original_parent_node_id, original_name,
                original_path, kind, object_id, head_version_id, deleted_at_unix,
                retired_at_unix, purged_at_unix, state
         FROM namespace_trash_roots
         ORDER BY trash_entry_id ASC
         LIMIT ?1"
    };
    let mut stmt = conn.prepare(sql)?;
    if let Some(after_trash_entry_id) = after_trash_entry_id {
        stmt.bind_text(1, after_trash_entry_id)?;
        stmt.bind_i64(2, u64_to_i64(limit.max(1) as u64)?)?;
    } else {
        stmt.bind_i64(1, u64_to_i64(limit.max(1) as u64)?)?;
    }
    let mut roots = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        roots.push(n1_namespace_trash_root_from_stmt(&stmt)?);
    }
    Ok(roots)
}

fn n1_list_namespace_trash_roots_by_path_on_conn(
    conn: &mut Connection,
    original_path: &str,
    state: crate::n1::TrashEntryState,
) -> Result<Vec<crate::n1::NamespaceTrashRoot>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT trash_entry_id, root_node_id, original_parent_node_id, original_name,
                original_path, kind, object_id, head_version_id, deleted_at_unix,
                retired_at_unix, purged_at_unix, state
         FROM namespace_trash_roots
         WHERE original_path = ?1 AND state = ?2
         ORDER BY deleted_at_unix DESC, trash_entry_id ASC",
    )?;
    stmt.bind_text(1, original_path)?;
    stmt.bind_text(2, state.as_str())?;
    let mut roots = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        roots.push(n1_namespace_trash_root_from_stmt(&stmt)?);
    }
    Ok(roots)
}

fn n1_namespace_node_from_stmt(
    stmt: &crate::ffi::Statement,
    offset: i32,
) -> Result<crate::n1::NamespaceNode, StorageError> {
    let kind = stmt
        .column_text(offset + 3)
        .and_then(|value| crate::n1::NamespaceEntryKind::from_str(&value))
        .ok_or_else(|| {
            StorageError::InvalidInput("namespace_nodes contains an invalid kind".to_string())
        })?;
    Ok(crate::n1::NamespaceNode {
        node_id: stmt.column_text(offset).unwrap_or_default(),
        parent_node_id: non_empty_owned(stmt.column_text(offset + 1)),
        name: stmt.column_text(offset + 2).unwrap_or_default(),
        kind,
        object_id: non_empty_owned(stmt.column_text(offset + 4)),
        head_version_id: non_empty_owned(stmt.column_text(offset + 5)),
        created_at_unix: stmt.column_i64(offset + 6),
        updated_at_unix: stmt.column_i64(offset + 7),
    })
}

fn n1_namespace_file_head_from_stmt(
    stmt: &crate::ffi::Statement,
) -> Result<crate::n1::NamespaceFileHead, StorageError> {
    Ok(crate::n1::NamespaceFileHead {
        node_id: stmt.column_text(0).unwrap_or_default(),
        object_id: stmt.column_text(1).unwrap_or_default(),
        version_id: stmt.column_text(2).unwrap_or_default(),
        upload_session_id: stmt.column_text(3).unwrap_or_default(),
        owner_uid: stmt.column_text(4).unwrap_or_default(),
        content_type: stmt.column_text(5).unwrap_or_default(),
        total_size_bytes: i64_to_u64(stmt.column_i64(6))?,
        created_at_unix: stmt.column_i64(7),
        is_delete_marker: stmt.column_i32(8) != 0,
        stripe_size_bytes: i64_to_u64(stmt.column_i64(9))?,
        updated_at_unix: stmt.column_i64(10),
    })
}

fn n1_namespace_trash_root_from_stmt(
    stmt: &crate::ffi::Statement,
) -> Result<crate::n1::NamespaceTrashRoot, StorageError> {
    let kind = stmt
        .column_text(5)
        .and_then(|value| crate::n1::NamespaceEntryKind::from_str(&value))
        .ok_or_else(|| {
            StorageError::InvalidInput("namespace_trash_roots contains an invalid kind".to_string())
        })?;
    let state = stmt
        .column_text(11)
        .and_then(|value| crate::n1::TrashEntryState::from_str(&value))
        .ok_or_else(|| {
            StorageError::InvalidInput(
                "namespace_trash_roots contains an invalid state".to_string(),
            )
        })?;
    Ok(crate::n1::NamespaceTrashRoot {
        trash_entry_id: stmt.column_text(0).unwrap_or_default(),
        root_node_id: stmt.column_text(1).unwrap_or_default(),
        original_parent_node_id: stmt.column_text(2).unwrap_or_default(),
        original_name: stmt.column_text(3).unwrap_or_default(),
        original_path: stmt.column_text(4).unwrap_or_default(),
        kind,
        object_id: non_empty_owned(stmt.column_text(6)),
        head_version_id: non_empty_owned(stmt.column_text(7)),
        deleted_at_unix: stmt.column_i64(8),
        retired_at_unix: stmt.column_text(9).map(|_| stmt.column_i64(9)),
        purged_at_unix: stmt.column_text(10).map(|_| stmt.column_i64(10)),
        state,
    })
}

fn n1_get_namespace_entry_on_conn(
    conn: &mut Connection,
    path: &str,
    lifecycle: Option<crate::n1::NamespaceLifecycle>,
) -> Result<Option<crate::n1::NamespaceEntry>, StorageError> {
    let sql = if lifecycle.is_some() {
        "SELECT path, parent_path, name, kind, object_id, head_version_id,
                lifecycle, trash_entry_id, deleted_at_unix, created_at_unix,
                updated_at_unix
         FROM namespace_entries
         WHERE path = ?1 AND lifecycle = ?2"
    } else {
        "SELECT path, parent_path, name, kind, object_id, head_version_id,
                lifecycle, trash_entry_id, deleted_at_unix, created_at_unix,
                updated_at_unix
         FROM namespace_entries
         WHERE path = ?1"
    };
    let mut stmt = conn.prepare(sql)?;
    stmt.bind_text(1, path)?;
    if let Some(lifecycle) = lifecycle {
        stmt.bind_text(2, lifecycle.as_str())?;
    }
    match stmt.step()? {
        StepResult::Row => n1_namespace_entry_from_stmt(&stmt).map(Some),
        StepResult::Done => Ok(None),
    }
}

fn n1_list_namespace_children_on_conn(
    conn: &mut Connection,
    parent_path: &str,
    include_trashed: bool,
    after_name: Option<&str>,
    limit: usize,
) -> Result<Vec<crate::n1::NamespaceEntry>, StorageError> {
    let mut sql = String::from(
        "SELECT path, parent_path, name, kind, object_id, head_version_id,
                lifecycle, trash_entry_id, deleted_at_unix, created_at_unix,
                updated_at_unix
         FROM namespace_entries
         WHERE parent_path = ?1",
    );
    if include_trashed {
        sql.push_str(" AND lifecycle IN ('live', 'trashed')");
    } else {
        sql.push_str(" AND lifecycle = 'live'");
    }
    if after_name.is_some() {
        sql.push_str(" AND name > ?2 ORDER BY name ASC LIMIT ?3");
    } else {
        sql.push_str(" ORDER BY name ASC LIMIT ?2");
    }
    let mut stmt = conn.prepare(&sql)?;
    stmt.bind_text(1, parent_path)?;
    if let Some(after_name) = after_name {
        stmt.bind_text(2, after_name)?;
        stmt.bind_i64(3, u64_to_i64(limit.max(1) as u64)?)?;
    } else {
        stmt.bind_i64(2, u64_to_i64(limit.max(1) as u64)?)?;
    }
    let mut entries = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        entries.push(n1_namespace_entry_from_stmt(&stmt)?);
    }
    Ok(entries)
}

fn n1_list_namespace_entries_by_prefix_on_conn(
    conn: &mut Connection,
    prefix: Option<&str>,
    include_trashed: bool,
    after_path: Option<&str>,
    limit: usize,
) -> Result<Vec<crate::n1::NamespaceEntry>, StorageError> {
    let upper = prefix.and_then(prefix_upper_bound);
    let mut sql = String::from(
        "SELECT path, parent_path, name, kind, object_id, head_version_id,
                lifecycle, trash_entry_id, deleted_at_unix, created_at_unix,
                updated_at_unix
         FROM namespace_entries
         WHERE 1 = 1",
    );
    if include_trashed {
        sql.push_str(" AND lifecycle IN ('live', 'trashed')");
    } else {
        sql.push_str(" AND lifecycle = 'live'");
    }
    if prefix.is_some() {
        sql.push_str(" AND path >= ?");
        sql.push_str("2");
        if upper.is_some() {
            sql.push_str(" AND path < ?3");
        }
    }
    if after_path.is_some() {
        let idx = 2 + i32::from(prefix.is_some()) + i32::from(upper.is_some());
        sql.push_str(&format!(" AND path > ?{idx}"));
    }
    let limit_idx = 2
        + i32::from(prefix.is_some())
        + i32::from(upper.is_some())
        + i32::from(after_path.is_some());
    sql.push_str(&format!(" ORDER BY path ASC LIMIT ?{limit_idx}"));

    let mut stmt = conn.prepare(&sql)?;
    let mut idx = 2;
    if let Some(prefix) = prefix {
        stmt.bind_text(idx, prefix)?;
        idx += 1;
        if let Some(upper) = upper.as_deref() {
            stmt.bind_text(idx, upper)?;
            idx += 1;
        }
    }
    if let Some(after_path) = after_path {
        stmt.bind_text(idx, after_path)?;
        idx += 1;
    }
    stmt.bind_i64(idx, u64_to_i64(limit.max(1) as u64)?)?;
    let mut entries = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        entries.push(n1_namespace_entry_from_stmt(&stmt)?);
    }
    Ok(entries)
}

fn n1_list_namespace_object_summaries_on_conn(
    conn: &mut Connection,
    prefix: Option<&str>,
    lifecycle: crate::n1::NamespaceLifecycle,
    after_path: Option<&str>,
    limit: usize,
) -> Result<Vec<crate::n1::NamespaceObjectSummary>, StorageError> {
    let mut sql = String::from(
        "SELECT e.path,
                fh.version_id,
                COALESCE(fh.upload_session_id, ''),
                COALESCE(fh.owner_uid, ''),
                COALESCE(fh.content_type, ''),
                fh.total_size_bytes,
                fh.created_at_unix,
                fh.is_delete_marker,
                fh.stripe_size_bytes
         FROM namespace_entries e
         JOIN namespace_nodes n
            ON n.object_id = e.object_id
           AND n.head_version_id = e.head_version_id
         JOIN namespace_file_heads fh
            ON fh.node_id = n.node_id
           AND fh.object_id = e.object_id
           AND fh.version_id = e.head_version_id
         WHERE e.kind = 'file'
           AND e.lifecycle = ?1
           AND e.object_id IS NOT NULL
           AND e.head_version_id IS NOT NULL",
    );
    let mut next_idx = 2;
    let prefix = prefix.filter(|value| !value.is_empty());
    let upper = prefix.and_then(prefix_upper_bound);
    let prefix_idx = prefix.map(|_| {
        let idx = next_idx;
        next_idx += 1;
        sql.push_str(&format!(" AND e.path >= ?{idx}"));
        idx
    });
    let upper_idx = upper.as_ref().map(|_| {
        let idx = next_idx;
        next_idx += 1;
        sql.push_str(&format!(" AND e.path < ?{idx}"));
        idx
    });
    let after_path = after_path.filter(|value| !value.is_empty());
    let after_idx = after_path.map(|_| {
        let idx = next_idx;
        next_idx += 1;
        sql.push_str(&format!(" AND e.path > ?{idx}"));
        idx
    });
    let limit_idx = next_idx;
    sql.push_str(&format!(" ORDER BY e.path ASC LIMIT ?{limit_idx}"));

    let mut stmt = conn.prepare(&sql)?;
    stmt.bind_text(1, lifecycle.as_str())?;
    if let (Some(idx), Some(value)) = (prefix_idx, prefix) {
        stmt.bind_text(idx, value)?;
    }
    if let (Some(idx), Some(value)) = (upper_idx, upper.as_deref()) {
        stmt.bind_text(idx, value)?;
    }
    if let (Some(idx), Some(value)) = (after_idx, after_path) {
        stmt.bind_text(idx, value)?;
    }
    stmt.bind_i64(limit_idx, u64_to_i64(limit.max(1) as u64)?)?;
    let mut entries = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        entries.push(n1_namespace_object_summary_from_stmt(&stmt)?);
    }
    Ok(entries)
}

fn n1_list_namespace_subtree_on_conn(
    conn: &mut Connection,
    root_path: &str,
    include_trashed: bool,
) -> Result<Vec<crate::n1::NamespaceEntry>, StorageError> {
    let child_prefix = if root_path.is_empty() {
        String::new()
    } else {
        format!("{root_path}/")
    };
    let mut sql = String::from(
        "SELECT path, parent_path, name, kind, object_id, head_version_id,
                lifecycle, trash_entry_id, deleted_at_unix, created_at_unix,
                updated_at_unix
         FROM namespace_entries
         WHERE ",
    );
    if root_path.is_empty() {
        sql.push_str("path >= ?1");
    } else {
        sql.push_str("(path = ?1 OR path >= ?2");
        if let Some(upper) = prefix_upper_bound(&child_prefix) {
            let _ = upper;
            sql.push_str(" AND path < ?3");
        }
        sql.push(')');
    }
    if include_trashed {
        sql.push_str(" AND lifecycle IN ('live', 'trashed')");
    } else {
        sql.push_str(" AND lifecycle = 'live'");
    }
    sql.push_str(" ORDER BY path ASC");

    let upper = prefix_upper_bound(&child_prefix);
    let mut stmt = conn.prepare(&sql)?;
    stmt.bind_text(1, root_path)?;
    if !root_path.is_empty() {
        stmt.bind_text(2, &child_prefix)?;
        if let Some(upper) = upper.as_deref() {
            stmt.bind_text(3, upper)?;
        }
    }
    let mut entries = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        entries.push(n1_namespace_entry_from_stmt(&stmt)?);
    }
    Ok(entries)
}

fn n1_namespace_entry_from_stmt(
    stmt: &crate::ffi::Statement,
) -> Result<crate::n1::NamespaceEntry, StorageError> {
    let kind = stmt
        .column_text(3)
        .and_then(|value| crate::n1::NamespaceEntryKind::from_str(&value))
        .ok_or_else(|| {
            StorageError::InvalidInput("namespace_entries contains an invalid kind".to_string())
        })?;
    let lifecycle = stmt
        .column_text(6)
        .and_then(|value| crate::n1::NamespaceLifecycle::from_str(&value))
        .ok_or_else(|| {
            StorageError::InvalidInput(
                "namespace_entries contains an invalid lifecycle".to_string(),
            )
        })?;
    Ok(crate::n1::NamespaceEntry {
        path: stmt.column_text(0).unwrap_or_default(),
        parent_path: stmt.column_text(1).unwrap_or_default(),
        name: stmt.column_text(2).unwrap_or_default(),
        kind,
        object_id: non_empty_owned(stmt.column_text(4)),
        head_version_id: non_empty_owned(stmt.column_text(5)),
        lifecycle,
        trash_entry_id: non_empty_owned(stmt.column_text(7)),
        deleted_at_unix: if stmt.column_text(8).is_some() {
            Some(stmt.column_i64(8))
        } else {
            None
        },
        created_at_unix: stmt.column_i64(9),
        updated_at_unix: stmt.column_i64(10),
    })
}

fn n1_namespace_object_summary_from_stmt(
    stmt: &crate::ffi::Statement,
) -> Result<crate::n1::NamespaceObjectSummary, StorageError> {
    Ok(crate::n1::NamespaceObjectSummary {
        path: stmt.column_text(0).unwrap_or_default(),
        version_id: stmt.column_text(1).unwrap_or_default(),
        upload_session_id: stmt.column_text(2).unwrap_or_default(),
        owner_uid: stmt.column_text(3).unwrap_or_default(),
        content_type: stmt.column_text(4).unwrap_or_default(),
        total_size_bytes: i64_to_u64(stmt.column_i64(5))?,
        created_at_unix: stmt.column_i64(6),
        is_delete_marker: stmt.column_i32(7) != 0,
        stripe_size_bytes: i64_to_u64(stmt.column_i64(8))?,
    })
}

fn n1_upsert_trash_entry_on_conn(
    conn: &mut Connection,
    entry: &crate::n1::TrashEntry,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare(
        "INSERT INTO trash_entries(
            trash_entry_id, original_path, object_id, version_id, kind,
            deleted_at_unix, retired_at_unix, purged_at_unix, state, metadata_json
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(trash_entry_id) DO UPDATE SET
            original_path = excluded.original_path,
            object_id = excluded.object_id,
            version_id = excluded.version_id,
            kind = excluded.kind,
            deleted_at_unix = excluded.deleted_at_unix,
            retired_at_unix = excluded.retired_at_unix,
            purged_at_unix = excluded.purged_at_unix,
            state = excluded.state,
            metadata_json = excluded.metadata_json",
    )?;
    stmt.bind_text(1, &entry.trash_entry_id)?;
    stmt.bind_text(2, &entry.original_path)?;
    bind_optional_text(&mut stmt, 3, entry.object_id.as_deref())?;
    bind_optional_text(&mut stmt, 4, entry.version_id.as_deref())?;
    stmt.bind_text(5, entry.kind.as_str())?;
    stmt.bind_i64(6, entry.deleted_at_unix)?;
    bind_optional_i64(&mut stmt, 7, entry.retired_at_unix)?;
    bind_optional_i64(&mut stmt, 8, entry.purged_at_unix)?;
    stmt.bind_text(9, entry.state.as_str())?;
    bind_optional_text(&mut stmt, 10, entry.metadata_json.as_deref())?;
    stmt.step_done()
}

fn n1_set_trash_entry_state_on_conn(
    conn: &mut Connection,
    trash_entry_id: &str,
    state: crate::n1::TrashEntryState,
    changed_at_unix: i64,
) -> Result<(), StorageError> {
    let (retired_at, purged_at) = match state {
        crate::n1::TrashEntryState::Trashed => (None, None),
        crate::n1::TrashEntryState::Retired => (Some(changed_at_unix), None),
        crate::n1::TrashEntryState::Purged => (None, Some(changed_at_unix)),
    };
    let mut stmt = conn.prepare(
        "UPDATE trash_entries
         SET state = ?2,
             retired_at_unix = COALESCE(?3, retired_at_unix),
             purged_at_unix = COALESCE(?4, purged_at_unix)
         WHERE trash_entry_id = ?1",
    )?;
    stmt.bind_text(1, trash_entry_id)?;
    stmt.bind_text(2, state.as_str())?;
    bind_optional_i64(&mut stmt, 3, retired_at)?;
    bind_optional_i64(&mut stmt, 4, purged_at)?;
    stmt.step_done()
}

fn n1_list_trash_entries_on_conn(
    conn: &mut Connection,
    state: crate::n1::TrashEntryState,
    after_path: Option<&str>,
    limit: usize,
) -> Result<Vec<crate::n1::TrashEntry>, StorageError> {
    let sql = if after_path.is_some() {
        "SELECT trash_entry_id, original_path, object_id, version_id, kind,
                deleted_at_unix, retired_at_unix, purged_at_unix, state, metadata_json
         FROM trash_entries
         WHERE state = ?1 AND original_path > ?2
         ORDER BY original_path ASC, trash_entry_id ASC
         LIMIT ?3"
    } else {
        "SELECT trash_entry_id, original_path, object_id, version_id, kind,
                deleted_at_unix, retired_at_unix, purged_at_unix, state, metadata_json
         FROM trash_entries
         WHERE state = ?1
         ORDER BY original_path ASC, trash_entry_id ASC
         LIMIT ?2"
    };
    let mut stmt = conn.prepare(sql)?;
    stmt.bind_text(1, state.as_str())?;
    if let Some(after_path) = after_path {
        stmt.bind_text(2, after_path)?;
        stmt.bind_i64(3, u64_to_i64(limit.max(1) as u64)?)?;
    } else {
        stmt.bind_i64(2, u64_to_i64(limit.max(1) as u64)?)?;
    }
    let mut entries = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        entries.push(n1_trash_entry_from_stmt(&stmt)?);
    }
    Ok(entries)
}

fn n1_list_trash_entries_by_path_on_conn(
    conn: &mut Connection,
    original_path: &str,
    state: crate::n1::TrashEntryState,
) -> Result<Vec<crate::n1::TrashEntry>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT trash_entry_id, original_path, object_id, version_id, kind,
                deleted_at_unix, retired_at_unix, purged_at_unix, state, metadata_json
         FROM trash_entries
         WHERE original_path = ?1 AND state = ?2
         ORDER BY deleted_at_unix ASC, trash_entry_id ASC",
    )?;
    stmt.bind_text(1, original_path)?;
    stmt.bind_text(2, state.as_str())?;
    let mut entries = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        entries.push(n1_trash_entry_from_stmt(&stmt)?);
    }
    Ok(entries)
}

fn n1_trash_entry_from_stmt(
    stmt: &crate::ffi::Statement,
) -> Result<crate::n1::TrashEntry, StorageError> {
    let kind = stmt
        .column_text(4)
        .and_then(|value| crate::n1::NamespaceEntryKind::from_str(&value))
        .ok_or_else(|| {
            StorageError::InvalidInput("trash_entries contains an invalid kind".to_string())
        })?;
    let state = stmt
        .column_text(8)
        .and_then(|value| crate::n1::TrashEntryState::from_str(&value))
        .ok_or_else(|| {
            StorageError::InvalidInput("trash_entries contains an invalid state".to_string())
        })?;
    Ok(crate::n1::TrashEntry {
        trash_entry_id: stmt.column_text(0).unwrap_or_default(),
        original_path: stmt.column_text(1).unwrap_or_default(),
        object_id: non_empty_owned(stmt.column_text(2)),
        version_id: non_empty_owned(stmt.column_text(3)),
        kind,
        deleted_at_unix: stmt.column_i64(5),
        retired_at_unix: if stmt.column_text(6).is_some() {
            Some(stmt.column_i64(6))
        } else {
            None
        },
        purged_at_unix: if stmt.column_text(7).is_some() {
            Some(stmt.column_i64(7))
        } else {
            None
        },
        state,
        metadata_json: non_empty_owned(stmt.column_text(9)),
    })
}

fn n1_upsert_pending_upload_on_conn(
    conn: &mut Connection,
    binding: &crate::n1::PendingUploadBinding,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare(
        "INSERT INTO pending_upload_bindings(
            version_id, session_id, logical_path, physical_object_id, created_at_unix
         )
         VALUES(?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(version_id) DO UPDATE SET
            session_id = excluded.session_id,
            logical_path = excluded.logical_path,
            physical_object_id = excluded.physical_object_id,
            created_at_unix = excluded.created_at_unix",
    )?;
    stmt.bind_text(1, &binding.version_id)?;
    stmt.bind_text(2, &binding.session_id)?;
    stmt.bind_text(3, &binding.logical_path)?;
    stmt.bind_text(4, &binding.physical_object_id)?;
    stmt.bind_i64(5, binding.created_at_unix)?;
    stmt.step_done()
}

fn n1_delete_pending_upload_on_conn(
    conn: &mut Connection,
    version_id: &str,
) -> Result<(), StorageError> {
    let mut stmt = conn.prepare("DELETE FROM pending_upload_bindings WHERE version_id = ?1")?;
    stmt.bind_text(1, version_id)?;
    stmt.step_done()
}

fn n1_get_pending_upload_on_conn(
    conn: &mut Connection,
    version_id: &str,
) -> Result<Option<crate::n1::PendingUploadBinding>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT logical_path, physical_object_id, version_id, session_id, created_at_unix
         FROM pending_upload_bindings
         WHERE version_id = ?1",
    )?;
    stmt.bind_text(1, version_id)?;
    match stmt.step()? {
        StepResult::Row => n1_pending_upload_from_stmt(&stmt).map(Some),
        StepResult::Done => Ok(None),
    }
}

fn n1_find_pending_upload_by_logical_path_on_conn(
    conn: &mut Connection,
    logical_path: &str,
) -> Result<Option<crate::n1::PendingUploadBinding>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT logical_path, physical_object_id, version_id, session_id, created_at_unix
         FROM pending_upload_bindings
         WHERE logical_path = ?1
         ORDER BY created_at_unix DESC
         LIMIT 1",
    )?;
    stmt.bind_text(1, logical_path)?;
    match stmt.step()? {
        StepResult::Row => n1_pending_upload_from_stmt(&stmt).map(Some),
        StepResult::Done => Ok(None),
    }
}

fn n1_list_pending_uploads_on_conn(
    conn: &mut Connection,
) -> Result<Vec<crate::n1::PendingUploadBinding>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT logical_path, physical_object_id, version_id, session_id, created_at_unix
         FROM pending_upload_bindings
         ORDER BY created_at_unix DESC, version_id ASC",
    )?;
    let mut bindings = Vec::new();
    while matches!(stmt.step()?, StepResult::Row) {
        bindings.push(n1_pending_upload_from_stmt(&stmt)?);
    }
    Ok(bindings)
}

fn n1_pending_upload_from_stmt(
    stmt: &crate::ffi::Statement,
) -> Result<crate::n1::PendingUploadBinding, StorageError> {
    Ok(crate::n1::PendingUploadBinding {
        logical_path: stmt.column_text(0).unwrap_or_default(),
        physical_object_id: stmt.column_text(1).unwrap_or_default(),
        version_id: stmt.column_text(2).unwrap_or_default(),
        session_id: stmt.column_text(3).unwrap_or_default(),
        created_at_unix: stmt.column_i64(4),
    })
}

fn n1_get_fragment_meta_on_conn(
    conn: &mut Connection,
    fragment_id: &str,
) -> Result<Option<crate::n1::FragmentMeta>, StorageError> {
    let mut stmt = conn.prepare(
        "SELECT fragment_id, state, quota_bytes, quota_project_id, committed_bytes, reserved_bytes,
                trash_bytes, created_at_unix, updated_at_unix
         FROM fragment_meta
         WHERE fragment_id = ?1",
    )?;
    stmt.bind_text(1, fragment_id)?;
    match stmt.step()? {
        StepResult::Row => Ok(Some(crate::n1::FragmentMeta {
            fragment_id: stmt.column_text(0).unwrap_or_default(),
            state: stmt.column_text(1).unwrap_or_default(),
            quota_bytes: i64_to_u64(stmt.column_i64(2))?,
            quota_project_id: stmt.column_i64_optional(3).map(i64_to_u32).transpose()?,
            committed_bytes: i64_to_u64(stmt.column_i64(4))?,
            reserved_bytes: i64_to_u64(stmt.column_i64(5))?,
            trash_bytes: i64_to_u64(stmt.column_i64(6))?,
            created_at_unix: stmt.column_i64(7),
            updated_at_unix: stmt.column_i64(8),
        })),
        StepResult::Done => Ok(None),
    }
}

fn n1_set_fragment_quota_on_conn(
    conn: &mut Connection,
    fragment_id: &str,
    quota_bytes: u64,
) -> Result<(), StorageError> {
    let now = unix_now();
    let mut stmt = conn.prepare(
        "UPDATE fragment_meta
         SET quota_bytes = ?2, updated_at_unix = ?3
         WHERE fragment_id = ?1",
    )?;
    stmt.bind_text(1, fragment_id)?;
    stmt.bind_i64(2, u64_to_i64(quota_bytes)?)?;
    stmt.bind_i64(3, now)?;
    stmt.step_done()
}

fn n1_set_fragment_quota_project_id_on_conn(
    conn: &mut Connection,
    fragment_id: &str,
    project_id: u32,
) -> Result<(), StorageError> {
    let now = unix_now();
    let mut stmt = conn.prepare(
        "UPDATE fragment_meta
         SET quota_project_id = ?2, updated_at_unix = ?3
         WHERE fragment_id = ?1",
    )?;
    stmt.bind_text(1, fragment_id)?;
    stmt.bind_i64(2, project_id as i64)?;
    stmt.bind_i64(3, now)?;
    stmt.step_done()
}

fn bind_optional_text(
    stmt: &mut crate::ffi::Statement,
    idx: i32,
    value: Option<&str>,
) -> Result<(), StorageError> {
    if let Some(value) = value {
        stmt.bind_text(idx, value)
    } else {
        stmt.bind_null(idx)
    }
}

fn bind_optional_i64(
    stmt: &mut crate::ffi::Statement,
    idx: i32,
    value: Option<i64>,
) -> Result<(), StorageError> {
    if let Some(value) = value {
        stmt.bind_i64(idx, value)
    } else {
        stmt.bind_null(idx)
    }
}

fn non_empty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn non_empty_owned(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

fn u64_to_i64(value: u64) -> Result<i64, StorageError> {
    i64::try_from(value)
        .map_err(|_| StorageError::InvalidInput(format!("integer exceeds indexdb i64: {value}")))
}

fn i64_to_u64(value: i64) -> Result<u64, StorageError> {
    u64::try_from(value)
        .map_err(|_| StorageError::InvalidInput(format!("negative unsigned integer: {value}")))
}

fn i64_to_u32(value: i64) -> Result<u32, StorageError> {
    u32::try_from(value)
        .map_err(|_| StorageError::InvalidInput(format!("invalid project id: {value}")))
}

fn i64_to_usize(value: i64) -> Result<usize, StorageError> {
    usize::try_from(value)
        .map_err(|_| StorageError::InvalidInput(format!("invalid row count: {value}")))
}

fn prefix_upper_bound(prefix: &str) -> Option<String> {
    if prefix.is_empty() {
        return None;
    }
    let mut bytes = prefix.as_bytes().to_vec();
    for idx in (0..bytes.len()).rev() {
        if bytes[idx] != u8::MAX {
            bytes[idx] = bytes[idx].saturating_add(1);
            bytes.truncate(idx + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}

fn namespace_child_prefix(path: &str) -> String {
    if path.is_empty() {
        String::new()
    } else {
        format!("{path}/")
    }
}

fn namespace_parent_path(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

fn namespace_base_name(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(_, name)| name.to_string())
        .unwrap_or_else(|| path.to_string())
}

struct NamespaceScopeClause {
    sql: String,
    root_path: Option<String>,
    child_prefix: Option<String>,
    child_upper: Option<String>,
    trash_entry_id: Option<String>,
    root_idx: Option<i32>,
    child_idx: Option<i32>,
    child_upper_idx: Option<i32>,
    trash_idx: Option<i32>,
    next_idx: i32,
}

fn namespace_scope_clause(
    root_path: Option<&str>,
    trash_entry_id: Option<&str>,
    first_idx: i32,
) -> NamespaceScopeClause {
    let root = root_path.unwrap_or_default();
    let mut sql = String::new();
    let mut next_idx = first_idx;
    let mut root_idx = None;
    let mut child_idx = None;
    let mut child_upper_idx = None;
    let root_path = if root.is_empty() {
        None
    } else {
        let root_i = next_idx;
        next_idx += 1;
        let child_i = next_idx;
        next_idx += 1;
        let child_prefix = namespace_child_prefix(root);
        let child_upper = prefix_upper_bound(&child_prefix);
        root_idx = Some(root_i);
        child_idx = Some(child_i);
        sql.push_str(&format!(" AND (path = ?{root_i} OR (path >= ?{child_i}"));
        if child_upper.is_some() {
            let upper_i = next_idx;
            next_idx += 1;
            child_upper_idx = Some(upper_i);
            sql.push_str(&format!(" AND path < ?{upper_i}"));
        }
        sql.push_str("))");
        Some(root.to_string())
    };

    let child_prefix = root_path.as_deref().map(namespace_child_prefix);
    let child_upper = child_prefix.as_deref().and_then(prefix_upper_bound);
    let trash_entry_id = trash_entry_id.filter(|value| !value.is_empty());
    let trash_idx = trash_entry_id.map(|_| {
        let idx = next_idx;
        next_idx += 1;
        sql.push_str(&format!(" AND trash_entry_id = ?{idx}"));
        idx
    });

    NamespaceScopeClause {
        sql,
        root_path,
        child_prefix,
        child_upper,
        trash_entry_id: trash_entry_id.map(ToOwned::to_owned),
        root_idx,
        child_idx,
        child_upper_idx,
        trash_idx,
        next_idx,
    }
}

fn bind_namespace_scope_clause(
    stmt: &mut crate::ffi::Statement,
    scope: &NamespaceScopeClause,
) -> Result<(), StorageError> {
    if let (Some(idx), Some(value)) = (scope.root_idx, scope.root_path.as_deref()) {
        stmt.bind_text(idx, value)?;
    }
    if let (Some(idx), Some(value)) = (scope.child_idx, scope.child_prefix.as_deref()) {
        stmt.bind_text(idx, value)?;
    }
    if let (Some(idx), Some(value)) = (scope.child_upper_idx, scope.child_upper.as_deref()) {
        stmt.bind_text(idx, value)?;
    }
    if let (Some(idx), Some(value)) = (scope.trash_idx, scope.trash_entry_id.as_deref()) {
        stmt.bind_text(idx, value)?;
    }
    Ok(())
}

fn n1_namespace_scope_stats_on_conn(
    conn: &mut Connection,
    root_path: Option<&str>,
    lifecycle: crate::n1::NamespaceLifecycle,
    trash_entry_id: Option<&str>,
) -> Result<crate::n1::NamespaceMutationStats, StorageError> {
    let scope = namespace_scope_clause(root_path, trash_entry_id, 2);
    let mut count_stmt = conn.prepare(&format!(
        "SELECT COUNT(*),
                COALESCE(SUM(CASE WHEN object_id IS NOT NULL THEN 1 ELSE 0 END), 0)
         FROM namespace_entries
         WHERE lifecycle = ?1{}",
        scope.sql
    ))?;
    count_stmt.bind_text(1, lifecycle.as_str())?;
    bind_namespace_scope_clause(&mut count_stmt, &scope)?;
    let (affected_paths, affected_objects) = match count_stmt.step()? {
        StepResult::Row => (
            i64_to_usize(count_stmt.column_i64(0))?,
            i64_to_usize(count_stmt.column_i64(1))?,
        ),
        StepResult::Done => (0, 0),
    };
    drop(count_stmt);

    let order_sql = scope
        .root_idx
        .map(|idx| format!("CASE WHEN path = ?{idx} THEN 0 ELSE 1 END, path ASC"))
        .unwrap_or_else(|| "path ASC".to_string());
    let mut source_stmt = conn.prepare(&format!(
        "SELECT path, kind, object_id, head_version_id
         FROM namespace_entries
         WHERE lifecycle = ?1{}
         ORDER BY {order_sql}
         LIMIT 1",
        scope.sql
    ))?;
    source_stmt.bind_text(1, lifecycle.as_str())?;
    bind_namespace_scope_clause(&mut source_stmt, &scope)?;
    let (source_path, source_kind, source_object_id, source_head_version_id) =
        match source_stmt.step()? {
            StepResult::Row => {
                let kind = source_stmt
                    .column_text(1)
                    .as_deref()
                    .and_then(crate::n1::NamespaceEntryKind::from_str)
                    .ok_or_else(|| {
                        StorageError::InvalidInput(
                            "namespace_entries contains an invalid kind".to_string(),
                        )
                    })?;
                (
                    source_stmt.column_text(0),
                    kind,
                    non_empty_owned(source_stmt.column_text(2)),
                    non_empty_owned(source_stmt.column_text(3)),
                )
            }
            StepResult::Done => (None, crate::n1::NamespaceEntryKind::Directory, None, None),
        };
    drop(source_stmt);

    let mut trash_stmt = conn.prepare(&format!(
        "SELECT DISTINCT trash_entry_id
         FROM namespace_entries
         WHERE lifecycle = ?1{}
           AND trash_entry_id IS NOT NULL
         ORDER BY trash_entry_id ASC",
        scope.sql
    ))?;
    trash_stmt.bind_text(1, lifecycle.as_str())?;
    bind_namespace_scope_clause(&mut trash_stmt, &scope)?;
    let mut trash_entry_ids = Vec::new();
    while matches!(trash_stmt.step()?, StepResult::Row) {
        if let Some(value) = non_empty_owned(trash_stmt.column_text(0)) {
            trash_entry_ids.push(value);
        }
    }

    Ok(crate::n1::NamespaceMutationStats {
        affected_paths,
        affected_objects,
        source_path,
        source_kind,
        source_object_id,
        source_head_version_id,
        trash_entry_ids,
    })
}

fn pragma_check(conn: &mut Connection, sql: &str) -> Result<(), StorageError> {
    let mut stmt = conn.prepare(sql)?;
    while matches!(stmt.step()?, StepResult::Row) {
        let value = stmt.column_text(0).unwrap_or_default();
        if value != "ok" {
            return Err(StorageError::Sqlite {
                code: 0,
                message: format!("{sql} failed: {value}"),
            });
        }
    }
    Ok(())
}

fn initialize_path_metadata(db: &Database) -> Result<(), StorageError> {
    let Some(fragment_id) = infer_fragment_id(db.path()) else {
        return Ok(());
    };
    let now = unix_now();
    let mut conn = db.inner.conn.lock()?;
    let mut meta = conn.prepare(
        "INSERT INTO indexdb_meta(key, value)
         VALUES('fragment_id', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )?;
    meta.bind_text(1, &fragment_id)?;
    meta.step_done()?;

    let mut fragment = conn.prepare(
        "INSERT INTO fragment_meta(
            fragment_id, state, quota_bytes, quota_project_id, committed_bytes, reserved_bytes,
            trash_bytes, created_at_unix, updated_at_unix
         )
         VALUES(?1, 'active', 0, NULL, 0, 0, 0, ?2, ?2)
         ON CONFLICT(fragment_id) DO NOTHING",
    )?;
    fragment.bind_text(1, &fragment_id)?;
    fragment.bind_i64(2, now)?;
    fragment.step_done()?;
    Ok(())
}

fn infer_fragment_id(path: &Path) -> Option<String> {
    let index_dir = path.parent()?;
    if index_dir.file_name()?.to_str()? != "indexdb" {
        return None;
    }
    Some(index_dir.parent()?.file_name()?.to_str()?.to_string())
}

fn bound_to_bytes<'a, K, KR>(bound: Bound<&KR>) -> Option<(bool, Vec<u8>)>
where
    K: Key + 'static,
    KR: Borrow<K::SelfType<'a>> + 'a,
{
    match bound {
        Bound::Included(value) => Some((true, encode_key::<K>(value.borrow()))),
        Bound::Excluded(value) => Some((false, encode_key::<K>(value.borrow()))),
        Bound::Unbounded => None,
    }
}

fn encode_key<K: Key + 'static>(value: &K::SelfType<'_>) -> Vec<u8> {
    K::as_bytes(value).as_ref().to_vec()
}

fn encode_value<V: Value + 'static>(value: &V::SelfType<'_>) -> Vec<u8> {
    V::as_bytes(value).as_ref().to_vec()
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{Database, ReadableDatabase, TableDefinition};
    use crate::ffi::StepResult;
    use crate::n1::{ObjectPart, ObjectState, ObjectVersion, ObjectVersionState, PartShard};

    #[test]
    fn range_from_empty_string_bound_returns_rows() {
        let file = crate::create_temp_db_file();
        let db = Database::create(file.path()).expect("create db");
        let table_def: TableDefinition<&str, &[u8]> = TableDefinition::new("range_test");
        let txn = db.begin_write().expect("begin write");
        {
            let mut table = txn.open_table(table_def).expect("open table");
            table
                .insert("listing/summary-only.bin", b"summary".as_slice())
                .expect("insert row");
        }
        txn.commit().expect("commit");

        let read = db.begin_read().expect("begin read");
        let table = read.open_table(table_def).expect("open table");
        let rows = table
            .range(""..)
            .expect("range from empty bound")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.value(), "listing/summary-only.bin");
    }

    #[test]
    fn builder_cache_size_is_applied_to_connection() {
        let file = crate::create_temp_db_file();
        let mut builder = Database::builder();
        builder.set_cache_size(512 * 1024);
        let db = builder.create(file.path()).expect("create db");

        let mut conn = db.inner.conn.lock().expect("connection lock");
        let mut stmt = conn.prepare("PRAGMA cache_size").expect("cache pragma");
        assert!(matches!(
            stmt.step().expect("step cache pragma"),
            StepResult::Row
        ));
        assert_eq!(stmt.column_i64(0), -512);
    }

    #[test]
    fn n1_object_metadata_round_trips_split_parts_and_shards() {
        let file = crate::create_temp_db_file();
        let db = Database::create(file.path()).expect("create db");
        let version = ObjectVersion {
            object_id: "photos/cat.jpg".to_string(),
            object_key: "photos/cat.jpg".to_string(),
            version_id: "01HV0000000000000000000000".to_string(),
            upload_session_id: "session-1".to_string(),
            owner_uid: "owner".to_string(),
            content_type: "image/jpeg".to_string(),
            total_size_bytes: 42,
            created_at_unix: 100,
            committed_at_unix: Some(101),
            is_delete_marker: false,
            stripe_size_bytes: 262_144,
            checksum: None,
            metadata_json: None,
            parts: vec![ObjectPart {
                part_index: 0,
                size_bytes: 42,
                created_at_unix: 100,
                checksum: Some("part-sum".to_string()),
                shards: vec![PartShard {
                    shard_index: 0,
                    file_name: "shard-0".to_string(),
                    absolute_path: "/blob/shard-0".to_string(),
                    relative_path: None,
                    blob_root_id: None,
                    size_bytes: 42,
                    checksum: "shard-sum".to_string(),
                    is_parity: false,
                    created_at_unix: 100,
                }],
            }],
        };
        let state = ObjectVersionState {
            object_id: version.object_id.clone(),
            version_id: version.version_id.clone(),
            state: ObjectState::Live,
            generation: 7,
            updated_at_unix: 102,
        };

        let txn = db.begin_write().expect("begin write");
        txn.n1_upsert_object_version(&version)
            .expect("upsert version");
        txn.n1_set_object_version_state(&state)
            .expect("upsert state");
        txn.commit().expect("commit");

        let loaded = db
            .n1_get_object_version(&version.object_id, &version.version_id)
            .expect("read version")
            .expect("version row");
        assert_eq!(loaded.parts.len(), 1);
        assert_eq!(loaded.parts[0].shards.len(), 1);
        assert_eq!(loaded.parts[0].shards[0].absolute_path, "/blob/shard-0");
        let loaded_state = db
            .n1_get_object_version_state(&version.object_id, &version.version_id)
            .expect("read state")
            .expect("state row");
        assert_eq!(loaded_state.state, ObjectState::Live);
        assert_eq!(loaded_state.generation, 7);
    }
}
