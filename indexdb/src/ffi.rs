#![allow(dead_code)]

use crate::StorageError;
use libc::{c_char, c_int, c_uchar, c_void};
use std::ffi::{CStr, CString};
use std::path::Path;
use std::ptr;

#[allow(non_camel_case_types)]
enum sqlite3 {}

#[allow(non_camel_case_types)]
enum sqlite3_stmt {}

#[allow(non_camel_case_types)]
enum sqlite3_backup {}

type Destructor = Option<unsafe extern "C" fn(*mut c_void)>;

const SQLITE_OK: c_int = 0;
const SQLITE_ROW: c_int = 100;
const SQLITE_DONE: c_int = 101;
const SQLITE_NULL: c_int = 5;

const SQLITE_OPEN_READONLY: c_int = 0x0000_0001;
const SQLITE_OPEN_READWRITE: c_int = 0x0000_0002;
const SQLITE_OPEN_CREATE: c_int = 0x0000_0004;
const SQLITE_OPEN_URI: c_int = 0x0000_0040;
const SQLITE_OPEN_FULLMUTEX: c_int = 0x0001_0000;

const SQLITE_PREPARE_PERSISTENT: c_int = 0x01;
const SQLITE_CHECKPOINT_TRUNCATE: c_int = 3;

unsafe extern "C" {
    fn sqlite3_open_v2(
        filename: *const c_char,
        pp_db: *mut *mut sqlite3,
        flags: c_int,
        z_vfs: *const c_char,
    ) -> c_int;
    fn sqlite3_close_v2(db: *mut sqlite3) -> c_int;
    fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        callback: Option<
            unsafe extern "C" fn(*mut c_void, c_int, *mut *mut c_char, *mut *mut c_char) -> c_int,
        >,
        arg: *mut c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;
    fn sqlite3_prepare_v3(
        db: *mut sqlite3,
        sql: *const c_char,
        n_byte: c_int,
        prep_flags: u32,
        pp_stmt: *mut *mut sqlite3_stmt,
        pz_tail: *mut *const c_char,
    ) -> c_int;
    fn sqlite3_step(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_reset(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_clear_bindings(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char;
    fn sqlite3_errcode(db: *mut sqlite3) -> c_int;
    fn sqlite3_extended_errcode(db: *mut sqlite3) -> c_int;
    fn sqlite3_extended_result_codes(db: *mut sqlite3, onoff: c_int) -> c_int;
    fn sqlite3_bind_text(
        stmt: *mut sqlite3_stmt,
        idx: c_int,
        value: *const c_char,
        n: c_int,
        destructor: Destructor,
    ) -> c_int;
    fn sqlite3_bind_blob(
        stmt: *mut sqlite3_stmt,
        idx: c_int,
        value: *const c_void,
        n: c_int,
        destructor: Destructor,
    ) -> c_int;
    fn sqlite3_bind_int64(stmt: *mut sqlite3_stmt, idx: c_int, value: i64) -> c_int;
    fn sqlite3_bind_int(stmt: *mut sqlite3_stmt, idx: c_int, value: c_int) -> c_int;
    fn sqlite3_bind_null(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int;
    fn sqlite3_column_text(stmt: *mut sqlite3_stmt, idx: c_int) -> *const c_uchar;
    fn sqlite3_column_blob(stmt: *mut sqlite3_stmt, idx: c_int) -> *const c_void;
    fn sqlite3_column_bytes(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int;
    fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, idx: c_int) -> i64;
    fn sqlite3_column_int(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int;
    fn sqlite3_column_type(stmt: *mut sqlite3_stmt, idx: c_int) -> c_int;
    fn sqlite3_changes64(db: *mut sqlite3) -> i64;
    fn sqlite3_busy_timeout(db: *mut sqlite3, ms: c_int) -> c_int;
    fn sqlite3_wal_checkpoint_v2(
        db: *mut sqlite3,
        z_db: *const c_char,
        mode: c_int,
        log_size: *mut c_int,
        checkpointed: *mut c_int,
    ) -> c_int;
    fn sqlite3_db_status(
        db: *mut sqlite3,
        op: c_int,
        current: *mut c_int,
        highwater: *mut c_int,
        reset: c_int,
    ) -> c_int;
    fn sqlite3_backup_init(
        dest: *mut sqlite3,
        dest_name: *const c_char,
        source: *mut sqlite3,
        source_name: *const c_char,
    ) -> *mut sqlite3_backup;
    fn sqlite3_backup_step(backup: *mut sqlite3_backup, pages: c_int) -> c_int;
    fn sqlite3_backup_finish(backup: *mut sqlite3_backup) -> c_int;
    fn sqlite3_backup_remaining(backup: *mut sqlite3_backup) -> c_int;
    fn sqlite3_backup_pagecount(backup: *mut sqlite3_backup) -> c_int;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenMode {
    ReadOnly,
    ReadWriteCreate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StepResult {
    Row,
    Done,
}

pub(crate) struct Connection {
    db: *mut sqlite3,
}

unsafe impl Send for Connection {}

impl Connection {
    pub(crate) fn open(path: &Path, mode: OpenMode) -> Result<Self, StorageError> {
        let path = path.to_string_lossy();
        let c_path = CString::new(path.as_bytes()).map_err(|err| {
            StorageError::InvalidInput(format!("indexdb path contains NUL byte: {err}"))
        })?;
        let flags = match mode {
            OpenMode::ReadOnly => SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_URI,
            OpenMode::ReadWriteCreate => {
                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_URI
            }
        };
        let mut db = ptr::null_mut();
        // SAFETY: sqlite3_open_v2 initializes db for a valid C string path and does not retain z_vfs.
        let rc = unsafe { sqlite3_open_v2(c_path.as_ptr(), &mut db, flags, ptr::null()) };
        if rc != SQLITE_OK {
            let err = if db.is_null() {
                StorageError::Sqlite {
                    code: rc,
                    message: "indexdb engine failed before connection initialization".to_string(),
                }
            } else {
                Self::last_error_for(db)
            };
            if !db.is_null() {
                // SAFETY: db was returned by sqlite3_open_v2 and is being discarded after failure.
                let _ = unsafe { sqlite3_close_v2(db) };
            }
            return Err(err);
        }

        let mut conn = Self { db };
        conn.extended_result_codes(true)?;
        conn.busy_timeout(30_000)?;
        if matches!(mode, OpenMode::ReadWriteCreate) {
            conn.apply_pragmas()?;
            conn.initialize_schema()?;
        }
        Ok(conn)
    }

    pub(crate) fn exec(&mut self, sql: &str) -> Result<(), StorageError> {
        let c_sql = CString::new(sql)
            .map_err(|err| StorageError::InvalidInput(format!("sql contains NUL byte: {err}")))?;
        // SAFETY: db is a live sqlite connection and sql is a valid C string. No callback is used.
        let rc = unsafe {
            sqlite3_exec(
                self.db,
                c_sql.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        self.check(rc)
    }

    pub(crate) fn prepare(&mut self, sql: &str) -> Result<Statement, StorageError> {
        let c_sql = CString::new(sql)
            .map_err(|err| StorageError::InvalidInput(format!("sql contains NUL byte: {err}")))?;
        let mut stmt = ptr::null_mut();
        // SAFETY: db is live, sql is a valid C string, and stmt is initialized by SQLite.
        let rc = unsafe {
            sqlite3_prepare_v3(
                self.db,
                c_sql.as_ptr(),
                -1,
                SQLITE_PREPARE_PERSISTENT as u32,
                &mut stmt,
                ptr::null_mut(),
            )
        };
        self.check(rc)?;
        Ok(Statement { stmt })
    }

    pub(crate) fn changes(&self) -> i64 {
        // SAFETY: db is a live sqlite connection.
        unsafe { sqlite3_changes64(self.db) }
    }

    pub(crate) fn checkpoint(&mut self) -> Result<(), StorageError> {
        // SAFETY: db is live; null database name means all attached databases.
        let rc = unsafe {
            sqlite3_wal_checkpoint_v2(
                self.db,
                ptr::null(),
                SQLITE_CHECKPOINT_TRUNCATE,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        self.check(rc)
    }

    pub(crate) fn db_status(&mut self, op: c_int, reset: bool) -> Result<(i32, i32), StorageError> {
        let mut current = 0;
        let mut highwater = 0;
        // SAFETY: db is live and output pointers are valid.
        let rc = unsafe {
            sqlite3_db_status(self.db, op, &mut current, &mut highwater, i32::from(reset))
        };
        self.check(rc)?;
        Ok((current, highwater))
    }

    pub(crate) fn backup_to_path(&mut self, destination: &Path) -> Result<(), StorageError> {
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(StorageError::Io)?;
        }
        remove_sqlite_file_set(destination)?;
        let mut dest = Self::open(destination, OpenMode::ReadWriteCreate)?;
        let main = c"main";
        // SAFETY: both sqlite handles are live and the schema names are static C strings.
        let backup = unsafe { sqlite3_backup_init(dest.db, main.as_ptr(), self.db, main.as_ptr()) };
        if backup.is_null() {
            return Err(Self::last_error_for(dest.db));
        }
        let mut backup = Backup { backup };
        backup.step_all()?;
        backup.finish()?;
        dest.checkpoint()?;
        Ok(())
    }

    fn extended_result_codes(&mut self, enabled: bool) -> Result<(), StorageError> {
        // SAFETY: db is a live sqlite connection.
        let rc = unsafe { sqlite3_extended_result_codes(self.db, i32::from(enabled)) };
        self.check(rc)
    }

    fn busy_timeout(&mut self, ms: c_int) -> Result<(), StorageError> {
        // SAFETY: db is a live sqlite connection.
        let rc = unsafe { sqlite3_busy_timeout(self.db, ms) };
        self.check(rc)
    }

    fn apply_pragmas(&mut self) -> Result<(), StorageError> {
        self.exec(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;
            PRAGMA temp_store = FILE;
            PRAGMA cache_size = -2048;
            PRAGMA mmap_size = 0;
            PRAGMA busy_timeout = 30000;
            PRAGMA wal_autocheckpoint = 1000;
            ",
        )
    }

    fn initialize_schema(&mut self) -> Result<(), StorageError> {
        self.exec(
            "
            CREATE TABLE IF NOT EXISTS indexdb_meta (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS indexdb_tables (
                name TEXT PRIMARY KEY NOT NULL,
                key_type TEXT NOT NULL,
                value_type TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'normal'
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS indexdb_kv (
                table_name TEXT NOT NULL,
                key BLOB NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY (table_name, key)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS fragment_meta (
                fragment_id TEXT PRIMARY KEY NOT NULL,
                state TEXT NOT NULL,
                quota_bytes INTEGER NOT NULL,
                quota_project_id INTEGER,
                committed_bytes INTEGER NOT NULL DEFAULT 0,
                reserved_bytes INTEGER NOT NULL DEFAULT 0,
                trash_bytes INTEGER NOT NULL DEFAULT 0,
                created_at_unix INTEGER NOT NULL,
                updated_at_unix INTEGER NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS namespace_entries (
                path TEXT PRIMARY KEY NOT NULL,
                parent_path TEXT NOT NULL,
                name TEXT NOT NULL,
                kind TEXT NOT NULL CHECK (kind IN ('file', 'directory')),
                object_id TEXT,
                head_version_id TEXT,
                lifecycle TEXT NOT NULL CHECK (lifecycle IN ('live', 'trashed', 'retired', 'purged')),
                trash_entry_id TEXT,
                deleted_at_unix INTEGER,
                created_at_unix INTEGER NOT NULL,
                updated_at_unix INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_namespace_parent_lifecycle_name
                ON namespace_entries(parent_path, lifecycle, name);
            CREATE INDEX IF NOT EXISTS idx_namespace_lifecycle_path
                ON namespace_entries(lifecycle, path);
            CREATE INDEX IF NOT EXISTS idx_namespace_trash_entry
                ON namespace_entries(trash_entry_id);

            CREATE TABLE IF NOT EXISTS namespace_nodes (
                node_id TEXT PRIMARY KEY NOT NULL,
                parent_node_id TEXT,
                name TEXT NOT NULL,
                kind TEXT NOT NULL CHECK (kind IN ('file', 'directory')),
                object_id TEXT,
                head_version_id TEXT,
                created_at_unix INTEGER NOT NULL,
                updated_at_unix INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_namespace_nodes_parent_name
                ON namespace_nodes(parent_node_id, name);
            CREATE INDEX IF NOT EXISTS idx_namespace_nodes_parent_order
                ON namespace_nodes(parent_node_id, name, node_id);
            CREATE INDEX IF NOT EXISTS idx_namespace_nodes_object
                ON namespace_nodes(object_id);

            CREATE TABLE IF NOT EXISTS namespace_trash_roots (
                trash_entry_id TEXT PRIMARY KEY NOT NULL,
                root_node_id TEXT NOT NULL,
                original_parent_node_id TEXT NOT NULL,
                original_name TEXT NOT NULL,
                original_path TEXT NOT NULL,
                kind TEXT NOT NULL CHECK (kind IN ('file', 'directory')),
                object_id TEXT,
                head_version_id TEXT,
                deleted_at_unix INTEGER NOT NULL,
                retired_at_unix INTEGER,
                purged_at_unix INTEGER,
                state TEXT NOT NULL CHECK (state IN ('trashed', 'retired', 'purged'))
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_namespace_trash_roots_state_path
                ON namespace_trash_roots(state, original_path, trash_entry_id);
            CREATE INDEX IF NOT EXISTS idx_namespace_trash_roots_root
                ON namespace_trash_roots(root_node_id);

            CREATE TABLE IF NOT EXISTS object_heads (
                object_id TEXT PRIMARY KEY NOT NULL,
                object_key TEXT NOT NULL,
                head_version_id TEXT NOT NULL,
                owner_uid TEXT,
                content_type TEXT,
                total_size_bytes INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL CHECK (state IN ('live', 'trash', 'deleted')),
                generation INTEGER NOT NULL DEFAULT 0,
                created_at_unix INTEGER NOT NULL,
                updated_at_unix INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_object_heads_key ON object_heads(object_key);
            CREATE INDEX IF NOT EXISTS idx_object_heads_state_key ON object_heads(state, object_key);

            CREATE TABLE IF NOT EXISTS object_versions (
                object_id TEXT NOT NULL,
                object_key TEXT NOT NULL,
                version_id TEXT NOT NULL,
                upload_session_id TEXT,
                owner_uid TEXT,
                content_type TEXT,
                total_size_bytes INTEGER NOT NULL,
                created_at_unix INTEGER NOT NULL,
                committed_at_unix INTEGER,
                is_delete_marker INTEGER NOT NULL DEFAULT 0,
                stripe_size_bytes INTEGER NOT NULL,
                checksum TEXT,
                metadata_json TEXT,
                PRIMARY KEY (object_id, version_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_object_versions_key_created
                ON object_versions(object_key, created_at_unix DESC);
            CREATE INDEX IF NOT EXISTS idx_object_versions_version ON object_versions(version_id);

            CREATE TABLE IF NOT EXISTS object_parts (
                version_id TEXT NOT NULL,
                part_index INTEGER NOT NULL,
                size_bytes INTEGER NOT NULL,
                created_at_unix INTEGER NOT NULL,
                checksum TEXT,
                PRIMARY KEY (version_id, part_index)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS part_shards (
                version_id TEXT NOT NULL,
                part_index INTEGER NOT NULL,
                shard_index INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                absolute_path TEXT NOT NULL,
                relative_path TEXT,
                blob_root_id TEXT,
                size_bytes INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                is_parity INTEGER NOT NULL DEFAULT 0,
                created_at_unix INTEGER NOT NULL,
                PRIMARY KEY (version_id, part_index, shard_index)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_part_shards_version ON part_shards(version_id);
            CREATE INDEX IF NOT EXISTS idx_part_shards_part ON part_shards(version_id, part_index);

            CREATE TABLE IF NOT EXISTS object_version_states (
                object_id TEXT NOT NULL,
                version_id TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('live', 'trash', 'deleted')),
                generation INTEGER NOT NULL,
                updated_at_unix INTEGER NOT NULL,
                PRIMARY KEY (object_id, version_id)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS upload_sessions (
                version_id TEXT PRIMARY KEY NOT NULL,
                session_id TEXT NOT NULL,
                object_key TEXT NOT NULL,
                object_id TEXT,
                physical_object_key TEXT,
                total_size_bytes INTEGER NOT NULL,
                expected_parts INTEGER NOT NULL,
                part_size_bytes INTEGER NOT NULL,
                committed_parts INTEGER NOT NULL DEFAULT 0,
                committed_bytes INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL CHECK (state IN ('active', 'finalizing', 'committed', 'aborted', 'stale')),
                reservation_id TEXT,
                created_at_unix INTEGER NOT NULL,
                updated_at_unix INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_upload_sessions_state
                ON upload_sessions(state, updated_at_unix);
            CREATE INDEX IF NOT EXISTS idx_upload_sessions_object_key ON upload_sessions(object_key);

            CREATE TABLE IF NOT EXISTS upload_parts (
                version_id TEXT NOT NULL,
                part_index INTEGER NOT NULL,
                size_bytes INTEGER NOT NULL,
                checksum TEXT,
                committed_at_unix INTEGER NOT NULL,
                PRIMARY KEY (version_id, part_index)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS pending_upload_bindings (
                version_id TEXT PRIMARY KEY NOT NULL,
                session_id TEXT NOT NULL,
                logical_path TEXT NOT NULL,
                physical_object_id TEXT NOT NULL,
                created_at_unix INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_pending_upload_logical_path
                ON pending_upload_bindings(logical_path, created_at_unix DESC);

            CREATE TABLE IF NOT EXISTS trash_entries (
                trash_entry_id TEXT PRIMARY KEY NOT NULL,
                original_path TEXT NOT NULL,
                object_id TEXT,
                version_id TEXT,
                kind TEXT NOT NULL CHECK (kind IN ('file', 'directory')),
                deleted_at_unix INTEGER NOT NULL,
                retired_at_unix INTEGER,
                purged_at_unix INTEGER,
                state TEXT NOT NULL CHECK (state IN ('trashed', 'retired', 'purged')),
                metadata_json TEXT
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_trash_state_path ON trash_entries(state, original_path);
            CREATE INDEX IF NOT EXISTS idx_trash_deleted_at ON trash_entries(deleted_at_unix DESC);

            CREATE TABLE IF NOT EXISTS quota_reservations (
                reservation_id TEXT PRIMARY KEY NOT NULL,
                version_id TEXT NOT NULL,
                object_key TEXT NOT NULL,
                reserved_bytes INTEGER NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('active', 'committed', 'released', 'stale')),
                created_at_unix INTEGER NOT NULL,
                updated_at_unix INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_quota_reservations_state
                ON quota_reservations(state, updated_at_unix);

            CREATE TABLE IF NOT EXISTS maintenance_state (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL,
                updated_at_unix INTEGER NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS replica_meta (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            ) WITHOUT ROWID;
            ",
        )?;

        if !self.column_exists("fragment_meta", "quota_project_id")? {
            self.exec("ALTER TABLE fragment_meta ADD COLUMN quota_project_id INTEGER")?;
        }

        self.upsert_meta("engine", "indexdb")?;
        self.upsert_meta("format", "ndxdb")?;
        self.upsert_meta("layout", "per_fragment")?;
        self.upsert_meta("schema_version", "1")?;
        self.upsert_meta("indexdb_version", env!("CARGO_PKG_VERSION"))?;
        self.upsert_meta("n1_min_version", "3.0")?;
        self.upsert_meta("created_at_unix", &unix_now().to_string())?;
        self.upsert_meta("last_commit_id", "0")?;
        self.upsert_meta("replica_generation", "0")?;
        Ok(())
    }

    fn column_exists(&mut self, table: &str, column: &str) -> Result<bool, StorageError> {
        let sql = format!("PRAGMA table_info({table})");
        let mut stmt = self.prepare(&sql)?;
        loop {
            match stmt.step()? {
                StepResult::Row => {
                    if stmt.column_text(1).as_deref() == Some(column) {
                        return Ok(true);
                    }
                }
                StepResult::Done => return Ok(false),
            }
        }
    }

    fn upsert_meta(&mut self, key: &str, value: &str) -> Result<(), StorageError> {
        let mut stmt = self.prepare(
            "INSERT INTO indexdb_meta(key, value)
             VALUES(?1, ?2)
             ON CONFLICT(key) DO NOTHING",
        )?;
        stmt.bind_text(1, key)?;
        stmt.bind_text(2, value)?;
        stmt.step_done()
    }

    fn check(&self, rc: c_int) -> Result<(), StorageError> {
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.last_error())
        }
    }

    fn last_error(&self) -> StorageError {
        Self::last_error_for(self.db)
    }

    fn last_error_for(db: *mut sqlite3) -> StorageError {
        // SAFETY: db is either live or just returned from sqlite3_open_v2; errmsg is valid while db lives.
        let message = unsafe {
            let ptr = sqlite3_errmsg(db);
            if ptr.is_null() {
                "unknown indexdb engine error".to_string()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };
        // SAFETY: db is a SQLite connection pointer.
        let code = unsafe { sqlite3_extended_errcode(db) };
        StorageError::Sqlite { code, message }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.db.is_null() {
            // SAFETY: db is owned by this Connection and will not be used after this call.
            let _ = unsafe { sqlite3_close_v2(self.db) };
            self.db = ptr::null_mut();
        }
    }
}

pub(crate) struct Statement {
    stmt: *mut sqlite3_stmt,
}

impl Statement {
    pub(crate) fn bind_text(&mut self, idx: i32, value: &str) -> Result<(), StorageError> {
        let len =
            c_int::try_from(value.len()).map_err(|_| StorageError::ValueTooLarge(value.len()))?;
        // SAFETY: SQLite copies the bytes because SQLITE_TRANSIENT is supplied.
        let rc = unsafe {
            sqlite3_bind_text(
                self.stmt,
                idx,
                value.as_ptr().cast::<c_char>(),
                len,
                sqlite_transient(),
            )
        };
        check_stmt(rc)
    }

    pub(crate) fn bind_blob(&mut self, idx: i32, value: &[u8]) -> Result<(), StorageError> {
        let len =
            c_int::try_from(value.len()).map_err(|_| StorageError::ValueTooLarge(value.len()))?;
        let ptr = value.as_ptr().cast::<c_void>();
        // SAFETY: SQLite copies the bytes because SQLITE_TRANSIENT is supplied.
        let rc = unsafe { sqlite3_bind_blob(self.stmt, idx, ptr, len, sqlite_transient()) };
        check_stmt(rc)
    }

    pub(crate) fn bind_i64(&mut self, idx: i32, value: i64) -> Result<(), StorageError> {
        // SAFETY: stmt is live and idx is caller-provided SQLite bind index.
        let rc = unsafe { sqlite3_bind_int64(self.stmt, idx, value) };
        check_stmt(rc)
    }

    pub(crate) fn bind_i32(&mut self, idx: i32, value: i32) -> Result<(), StorageError> {
        // SAFETY: stmt is live and idx is caller-provided SQLite bind index.
        let rc = unsafe { sqlite3_bind_int(self.stmt, idx, value) };
        check_stmt(rc)
    }

    pub(crate) fn bind_null(&mut self, idx: i32) -> Result<(), StorageError> {
        // SAFETY: stmt is live and idx is caller-provided SQLite bind index.
        let rc = unsafe { sqlite3_bind_null(self.stmt, idx) };
        check_stmt(rc)
    }

    pub(crate) fn step(&mut self) -> Result<StepResult, StorageError> {
        // SAFETY: stmt is live and owned by this wrapper.
        match unsafe { sqlite3_step(self.stmt) } {
            SQLITE_ROW => Ok(StepResult::Row),
            SQLITE_DONE => Ok(StepResult::Done),
            code => Err(StorageError::Sqlite {
                code,
                message: format!("indexdb engine step returned {code}"),
            }),
        }
    }

    pub(crate) fn step_done(&mut self) -> Result<(), StorageError> {
        match self.step()? {
            StepResult::Done => Ok(()),
            StepResult::Row => Err(StorageError::Sqlite {
                code: SQLITE_ROW,
                message: "statement unexpectedly returned a row".to_string(),
            }),
        }
    }

    pub(crate) fn reset(&mut self) -> Result<(), StorageError> {
        // SAFETY: stmt is live and owned by this wrapper.
        let rc = unsafe { sqlite3_reset(self.stmt) };
        check_stmt(rc)
    }

    pub(crate) fn clear_bindings(&mut self) -> Result<(), StorageError> {
        // SAFETY: stmt is live and owned by this wrapper.
        let rc = unsafe { sqlite3_clear_bindings(self.stmt) };
        check_stmt(rc)
    }

    pub(crate) fn column_text(&self, idx: i32) -> Option<String> {
        // SAFETY: stmt is currently on a row; SQLite owns the returned memory until next step/reset.
        unsafe {
            if sqlite3_column_type(self.stmt, idx) == SQLITE_NULL {
                return None;
            }
            let ptr = sqlite3_column_text(self.stmt, idx);
            if ptr.is_null() {
                return Some(String::new());
            }
            let len = sqlite3_column_bytes(self.stmt, idx).max(0) as usize;
            let bytes = std::slice::from_raw_parts(ptr, len);
            Some(String::from_utf8_lossy(bytes).into_owned())
        }
    }

    pub(crate) fn column_blob(&self, idx: i32) -> Option<Vec<u8>> {
        // SAFETY: stmt is currently on a row; SQLite owns the returned memory until next step/reset.
        unsafe {
            if sqlite3_column_type(self.stmt, idx) == SQLITE_NULL {
                return None;
            }
            let len = sqlite3_column_bytes(self.stmt, idx).max(0) as usize;
            if len == 0 {
                return Some(Vec::new());
            }
            let ptr = sqlite3_column_blob(self.stmt, idx);
            if ptr.is_null() {
                return Some(Vec::new());
            }
            Some(std::slice::from_raw_parts(ptr.cast::<u8>(), len).to_vec())
        }
    }

    pub(crate) fn column_i64(&self, idx: i32) -> i64 {
        // SAFETY: stmt is currently on a row.
        unsafe { sqlite3_column_int64(self.stmt, idx) }
    }

    pub(crate) fn column_i64_optional(&self, idx: i32) -> Option<i64> {
        // SAFETY: stmt is currently on a row.
        unsafe {
            if sqlite3_column_type(self.stmt, idx) == SQLITE_NULL {
                None
            } else {
                Some(sqlite3_column_int64(self.stmt, idx))
            }
        }
    }

    pub(crate) fn column_i32(&self, idx: i32) -> i32 {
        // SAFETY: stmt is currently on a row.
        unsafe { sqlite3_column_int(self.stmt, idx) }
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        if !self.stmt.is_null() {
            // SAFETY: stmt is owned by this wrapper and will not be used after finalization.
            let _ = unsafe { sqlite3_finalize(self.stmt) };
            self.stmt = ptr::null_mut();
        }
    }
}

pub(crate) struct Transaction<'a> {
    conn: &'a mut Connection,
    complete: bool,
}

impl<'a> Transaction<'a> {
    pub(crate) fn begin_immediate(conn: &'a mut Connection) -> Result<Self, StorageError> {
        conn.exec("BEGIN IMMEDIATE")?;
        Ok(Self {
            conn,
            complete: false,
        })
    }

    pub(crate) fn connection(&mut self) -> &mut Connection {
        self.conn
    }

    pub(crate) fn commit(mut self) -> Result<(), StorageError> {
        self.conn.exec("COMMIT")?;
        self.complete = true;
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.complete {
            let _ = self.conn.exec("ROLLBACK");
        }
    }
}

pub(crate) struct Backup {
    backup: *mut sqlite3_backup,
}

impl Backup {
    fn step_all(&mut self) -> Result<(), StorageError> {
        // SAFETY: backup is a live SQLite backup handle.
        match unsafe { sqlite3_backup_step(self.backup, -1) } {
            SQLITE_DONE | SQLITE_OK => Ok(()),
            code => Err(StorageError::Sqlite {
                code,
                message: format!("indexdb engine backup step returned {code}"),
            }),
        }
    }

    fn finish(&mut self) -> Result<(), StorageError> {
        if self.backup.is_null() {
            return Ok(());
        }
        // SAFETY: backup is owned by this wrapper and will not be used after finish.
        let rc = unsafe { sqlite3_backup_finish(self.backup) };
        self.backup = ptr::null_mut();
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(StorageError::Sqlite {
                code: rc,
                message: format!("indexdb engine backup finish returned {rc}"),
            })
        }
    }

    pub(crate) fn remaining(&self) -> i32 {
        if self.backup.is_null() {
            return 0;
        }
        // SAFETY: backup is live.
        unsafe { sqlite3_backup_remaining(self.backup) }
    }

    pub(crate) fn pagecount(&self) -> i32 {
        if self.backup.is_null() {
            return 0;
        }
        // SAFETY: backup is live.
        unsafe { sqlite3_backup_pagecount(self.backup) }
    }
}

impl Drop for Backup {
    fn drop(&mut self) {
        if !self.backup.is_null() {
            // SAFETY: backup is owned by this wrapper.
            let _ = unsafe { sqlite3_backup_finish(self.backup) };
            self.backup = ptr::null_mut();
        }
    }
}

fn check_stmt(rc: c_int) -> Result<(), StorageError> {
    if rc == SQLITE_OK {
        Ok(())
    } else {
        Err(StorageError::Sqlite {
            code: rc,
            message: format!("indexdb engine bind/reset/finalize returned {rc}"),
        })
    }
}

fn remove_sqlite_file_set(path: &Path) -> Result<(), StorageError> {
    for candidate in [
        path.to_path_buf(),
        path.with_file_name(format!(
            "{}-wal",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("index.ndxdb")
        )),
        path.with_file_name(format!(
            "{}-shm",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("index.ndxdb")
        )),
    ] {
        match std::fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(StorageError::Io(err)),
        }
    }
    Ok(())
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn sqlite_transient() -> Destructor {
    // SAFETY: SQLite documents the all-ones destructor sentinel as SQLITE_TRANSIENT.
    Some(unsafe { std::mem::transmute::<isize, unsafe extern "C" fn(*mut c_void)>(-1) })
}
