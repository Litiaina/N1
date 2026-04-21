use std::path::PathBuf;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use serde_json::Value as JsonValue;
use tokio::task;
use uuid::Uuid;

use crate::auth::hash_refresh_token;
use crate::error::RegistryError;
use crate::models::{
    AdminActivityUserSummary, AdminLargestObjectSummary, AdminStatsTotals, AuditEventRecord,
    FragmentBindingRecord, HealthResponse, ObjectCommentRecord, ObjectExtensionStat,
    ObjectOverlayRecord, ObjectRecord, ShareDisposition, ShareRecord, StorageDeployment,
    UserAuthRecord, UserRecord,
};
use crate::storage_broker::EngineObjectMetadata;

#[derive(Debug, Clone)]
pub struct RegistryListPage {
    pub objects: Vec<ObjectRecord>,
    pub common_prefixes: Vec<String>,
    pub has_more: bool,
}

enum RegistryListEntry {
    Object(ObjectRecord),
    CommonPrefix(String),
}

#[derive(Debug, Clone)]
pub struct RegistryObjectStats {
    pub total_objects: usize,
    pub total_size_bytes: i64,
    pub extension_stats: Vec<ObjectExtensionStat>,
}

#[derive(Debug, Clone)]
pub struct RegistryAdminStats {
    pub totals: AdminStatsTotals,
    pub file_types: Vec<ObjectExtensionStat>,
    pub largest_objects: Vec<AdminLargestObjectSummary>,
    pub active_users: Vec<AdminActivityUserSummary>,
    pub top_storage_users: Vec<AdminActivityUserSummary>,
}

#[derive(Debug, Clone)]
pub struct PendingUploadReservation {
    pub owner_uid: String,
    pub fragment: String,
    pub object_key: String,
    pub version_id: Option<String>,
    pub session_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct ArchiveObjectSelection {
    pub objects: Vec<ObjectRecord>,
}

#[derive(Debug, Clone)]
pub struct ObjectPurgeQueueEntry {
    pub owner_uid: String,
    pub fragment: String,
    pub object_key: String,
    pub attempts: i64,
}

#[derive(Clone, Debug)]
pub struct RegistryDb {
    sqlite_path: PathBuf,
    default_quota_bytes: i64,
}

const SQLITE_BULK_KEY_CHUNK: usize = 400;

impl RegistryDb {
    pub fn new(sqlite_path: PathBuf, default_quota_bytes: i64) -> Self {
        Self {
            sqlite_path,
            default_quota_bytes,
        }
    }

    pub async fn init(&self) -> Result<(), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            conn.pragma_update(None, "journal_mode", "WAL")
                .map_err(sql_err)?;
            conn.pragma_update(None, "synchronous", "NORMAL")
                .map_err(sql_err)?;
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS users (
                    uid TEXT PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    quota_limit_bytes INTEGER NOT NULL,
                    quota_used_bytes INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS objects (
                    owner_uid TEXT NOT NULL,
                    fragment TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    storage_object_id TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    object_size_bytes INTEGER NOT NULL,
                    content_type TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (owner_uid, fragment, object_key)
                );

                CREATE TABLE IF NOT EXISTS object_versions (
                    owner_uid TEXT NOT NULL,
                    fragment TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    storage_object_id TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    object_size_bytes INTEGER NOT NULL,
                    content_type TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (owner_uid, fragment, object_key, version_id)
                );

                CREATE TABLE IF NOT EXISTS object_uploads (
                    owner_uid TEXT NOT NULL,
                    fragment TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    storage_object_id TEXT NOT NULL,
                    expected_size_bytes INTEGER,
                    reserved_bytes INTEGER NOT NULL DEFAULT 0,
                    content_type TEXT,
                    version_id TEXT,
                    session_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (owner_uid, fragment, object_key)
                );

                CREATE INDEX IF NOT EXISTS idx_objects_owner_prefix
                    ON objects (owner_uid, fragment, object_key);
                CREATE INDEX IF NOT EXISTS idx_object_uploads_owner_prefix
                    ON object_uploads (owner_uid, fragment, object_key);
                CREATE INDEX IF NOT EXISTS idx_object_uploads_owner_session
                    ON object_uploads (owner_uid, fragment, version_id, session_id);
                CREATE TABLE IF NOT EXISTS fragment_bindings (
                    owner_uid TEXT NOT NULL,
                    fragment TEXT NOT NULL,
                    deployment TEXT NOT NULL,
                    engine_url_override TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (owner_uid, fragment)
                );
                CREATE INDEX IF NOT EXISTS idx_fragment_bindings_owner
                    ON fragment_bindings (owner_uid, fragment);

                CREATE TABLE IF NOT EXISTS registry_refresh_sessions (
                    token_hash TEXT PRIMARY KEY,
                    user_uid TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    rotated_at TEXT,
                    replaced_by_hash TEXT,
                    revoked_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_registry_refresh_sessions_user
                    ON registry_refresh_sessions (user_uid, expires_at);

                CREATE TABLE IF NOT EXISTS object_shares (
                    share_id TEXT PRIMARY KEY,
                    token_hash TEXT NOT NULL UNIQUE,
                    owner_uid TEXT NOT NULL,
                    fragment TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    storage_object_id TEXT NOT NULL,
                    disposition TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    revoked_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_object_shares_owner
                    ON object_shares (owner_uid, fragment, object_key, created_at);
                CREATE INDEX IF NOT EXISTS idx_object_shares_token
                    ON object_shares (token_hash, expires_at);

                CREATE TABLE IF NOT EXISTS object_purge_queue (
                    owner_uid TEXT NOT NULL,
                    fragment TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    storage_object_id TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    available_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (owner_uid, fragment, object_key)
                );
                CREATE INDEX IF NOT EXISTS idx_object_purge_queue_due
                    ON object_purge_queue (available_at, created_at);

                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id TEXT PRIMARY KEY,
                    owner_uid TEXT NOT NULL,
                    actor_uid TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    fragment TEXT,
                    object_key TEXT,
                    details_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_events_owner_created
                    ON audit_events (owner_uid, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_audit_events_object
                    ON audit_events (owner_uid, fragment, object_key, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_audit_events_type
                    ON audit_events (event_type, created_at DESC);

                CREATE TABLE IF NOT EXISTS object_overlays (
                    owner_uid TEXT NOT NULL,
                    fragment TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    favorite INTEGER NOT NULL DEFAULT 0,
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    labels_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (owner_uid, fragment, object_key)
                );
                CREATE INDEX IF NOT EXISTS idx_object_overlays_owner
                    ON object_overlays (owner_uid, fragment, object_key);

                CREATE TABLE IF NOT EXISTS object_comments (
                    comment_id TEXT PRIMARY KEY,
                    owner_uid TEXT NOT NULL,
                    actor_uid TEXT NOT NULL,
                    fragment TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    body TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    deleted_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_object_comments_object
                    ON object_comments (owner_uid, fragment, object_key, created_at);
                "#,
            )
            .map_err(sql_err)?;
            for statement in [
                "ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE users ADD COLUMN totp_secret TEXT",
                "ALTER TABLE users ADD COLUMN totp_pending_secret TEXT",
                "ALTER TABLE users ADD COLUMN totp_enabled INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE objects ADD COLUMN storage_object_id TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE object_versions ADD COLUMN storage_object_id TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE object_uploads ADD COLUMN version_id TEXT",
                "ALTER TABLE object_uploads ADD COLUMN session_id TEXT",
                "ALTER TABLE object_uploads ADD COLUMN storage_object_id TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE object_uploads ADD COLUMN reserved_bytes INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE object_shares ADD COLUMN storage_object_id TEXT NOT NULL DEFAULT ''",
            ] {
                let _ = conn.execute(statement, params![]);
            }
            for statement in [
                "UPDATE objects SET storage_object_id = object_key WHERE storage_object_id = ''",
                "UPDATE object_versions SET storage_object_id = object_key WHERE storage_object_id = ''",
                "UPDATE object_uploads SET storage_object_id = object_key WHERE storage_object_id = ''",
                "UPDATE object_shares SET storage_object_id = object_key WHERE storage_object_id = ''",
            ] {
                let _ = conn.execute(statement, params![]);
            }
            Ok(())
        })
        .await
    }

    pub async fn health(&self) -> Result<HealthResponse, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let users = scalar_i64(&conn, "SELECT COUNT(*) FROM users", params![])?;
            let objects = scalar_i64(&conn, "SELECT COUNT(*) FROM objects", params![])?;
            let ready_objects = scalar_i64(
                &conn,
                "SELECT COUNT(*) FROM objects WHERE status = 'ready'",
                params![],
            )?;
            Ok(HealthResponse {
                status: "healthy",
                sqlite_path: db.sqlite_path.display().to_string(),
                users,
                objects,
                ready_objects,
            })
        })
        .await
    }

    pub async fn create_user(
        &self,
        email: String,
        password_hash: String,
        quota_limit_bytes: Option<i64>,
        is_admin: bool,
    ) -> Result<UserRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let now = now_rfc3339();
            let uid = Uuid::new_v4().to_string();
            let quota_limit = quota_limit_bytes.unwrap_or(db.default_quota_bytes);
            conn.execute(
                r#"
                INSERT INTO users (
                    uid, email, password_hash, quota_limit_bytes, quota_used_bytes, created_at, updated_at, is_admin, totp_secret, totp_pending_secret, totp_enabled
                ) VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, NULL, NULL, 0)
                "#,
                params![uid, email, password_hash, quota_limit, now, now, is_admin],
            )
            .map_err(map_insert_user_err)?;
            user_by_uid(&conn, &uid)
        })
        .await
    }

    pub async fn find_user_auth_by_email(
        &self,
        email: String,
    ) -> Result<Option<UserAuthRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT uid, email, password_hash, quota_limit_bytes, quota_used_bytes, created_at, updated_at, is_admin, totp_secret, totp_pending_secret, totp_enabled
                    FROM users
                    WHERE email = ?1
                    "#,
                )
                .map_err(sql_err)?;
            let row = stmt
                .query_row(params![email], |row| {
                    Ok(UserAuthRecord {
                        user: UserRecord {
                            uid: row.get(0)?,
                            email: row.get(1)?,
                            is_admin: row.get::<_, i64>(7)? != 0,
                            two_factor_enabled: row.get::<_, i64>(10)? != 0,
                            quota_limit_bytes: row.get(3)?,
                            quota_used_bytes: row.get(4)?,
                            created_at: row.get(5)?,
                            updated_at: row.get(6)?,
                        },
                        password_hash: row.get(2)?,
                        totp_secret: row.get(8)?,
                        totp_pending_secret: row.get(9)?,
                    })
                })
                .optional()
                .map_err(sql_err)?;
            let mut row = row;
            if let Some(auth) = row.as_mut() {
                apply_effective_quota_usage_to_auth(&conn, auth)?;
            }
            Ok(row)
        })
        .await
    }

    pub async fn get_user(&self, uid: String) -> Result<UserRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            user_by_uid(&conn, &uid)
        })
        .await
    }

    pub async fn count_admin_users(&self) -> Result<i64, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            scalar_i64(
                &conn,
                "SELECT COUNT(*) FROM users WHERE is_admin = 1",
                params![],
            )
        })
        .await
    }

    pub async fn get_user_auth_by_uid(
        &self,
        uid: String,
    ) -> Result<Option<UserAuthRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT uid, email, password_hash, quota_limit_bytes, quota_used_bytes, created_at, updated_at, is_admin, totp_secret, totp_pending_secret, totp_enabled
                    FROM users
                    WHERE uid = ?1
                    "#,
                )
                .map_err(sql_err)?;
            let row = stmt
                .query_row(params![uid], map_user_auth_row)
                .optional()
                .map_err(sql_err)?;
            let mut row = row;
            if let Some(auth) = row.as_mut() {
                apply_effective_quota_usage_to_auth(&conn, auth)?;
            }
            Ok(row)
        })
        .await
    }

    pub async fn list_admin_users(&self) -> Result<Vec<UserRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT uid, email, quota_limit_bytes, quota_used_bytes, created_at, updated_at, is_admin, totp_enabled
                    FROM users
                    ORDER BY email ASC
                    "#,
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![], |row| {
                    Ok(UserRecord {
                        uid: row.get(0)?,
                        email: row.get(1)?,
                        quota_limit_bytes: row.get(2)?,
                        quota_used_bytes: row.get(3)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                        is_admin: row.get::<_, i64>(6)? != 0,
                        two_factor_enabled: row.get::<_, i64>(7)? != 0,
                    })
                })
                .map_err(sql_err)?;
            let mut out = Vec::new();
            for row in rows {
                let mut user = row.map_err(sql_err)?;
                apply_effective_quota_usage(&conn, &mut user)?;
                out.push(user);
            }
            Ok(out)
        })
        .await
    }

    pub async fn record_audit_event(
        &self,
        owner_uid: String,
        actor_uid: String,
        event_type: String,
        fragment: Option<String>,
        object_key: Option<String>,
        details: JsonValue,
    ) -> Result<AuditEventRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let event_id = Uuid::new_v4().to_string();
            let created_at = now_rfc3339();
            let details_json = serde_json::to_string(&details)
                .map_err(|err| RegistryError::internal(format!("encode audit details: {err}")))?;
            conn.execute(
                r#"
                INSERT INTO audit_events (
                    event_id, owner_uid, actor_uid, event_type, fragment, object_key, details_json, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    event_id,
                    owner_uid,
                    actor_uid,
                    event_type,
                    fragment,
                    object_key,
                    details_json,
                    created_at
                ],
            )
            .map_err(sql_err)?;
            Ok(AuditEventRecord {
                event_id,
                owner_uid,
                actor_uid,
                event_type,
                fragment,
                object_key,
                details_json,
                created_at,
            })
        })
        .await
    }

    pub async fn list_audit_events(
        &self,
        owner_uid: Option<String>,
        fragment: Option<String>,
        object_key: Option<String>,
        event_type: Option<String>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<AuditEventRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let mut sql = String::from(
                r#"
                SELECT event_id, owner_uid, actor_uid, event_type, fragment, object_key, details_json, created_at
                FROM audit_events
                WHERE 1 = 1
                "#,
            );
            let mut args = Vec::<Value>::new();
            if let Some(owner_uid) = owner_uid.filter(|value| !value.trim().is_empty()) {
                sql.push_str(" AND owner_uid = ?");
                args.push(Value::Text(owner_uid));
            }
            if let Some(fragment) = fragment.filter(|value| !value.trim().is_empty()) {
                sql.push_str(" AND fragment = ?");
                args.push(Value::Text(fragment));
            }
            if let Some(object_key) = object_key.filter(|value| !value.trim().is_empty()) {
                sql.push_str(" AND object_key = ?");
                args.push(Value::Text(object_key));
            }
            if let Some(event_type) = event_type.filter(|value| !value.trim().is_empty()) {
                sql.push_str(" AND event_type = ?");
                args.push(Value::Text(event_type));
            }
            sql.push_str(" ORDER BY created_at DESC, event_id DESC LIMIT ? OFFSET ?");
            args.push(Value::Integer(limit as i64));
            args.push(Value::Integer(offset as i64));

            let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
            let rows = stmt
                .query_map(params_from_iter(args), map_audit_event_row)
                .map_err(sql_err)?;
            let mut events = Vec::new();
            for row in rows {
                events.push(row.map_err(sql_err)?);
            }
            Ok(events)
        })
        .await
    }

    pub async fn get_object_overlay(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
    ) -> Result<ObjectOverlayRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            resolve_overlay(&conn, &owner_uid, &fragment, &object_key)
        })
        .await
    }

    pub async fn update_object_overlay(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
        favorite: Option<bool>,
        tags: Option<Vec<String>>,
        labels: Option<Vec<String>>,
    ) -> Result<ObjectOverlayRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let current = resolve_overlay(&conn, &owner_uid, &fragment, &object_key)?;
            let next_favorite = favorite.unwrap_or(current.favorite);
            let next_tags = tags
                .map(sanitize_overlay_values)
                .unwrap_or_else(|| current.tags.clone());
            let next_labels = labels
                .map(sanitize_overlay_values)
                .unwrap_or_else(|| current.labels.clone());
            let tags_json = serde_json::to_string(&next_tags)
                .map_err(|err| RegistryError::internal(format!("encode tags: {err}")))?;
            let labels_json = serde_json::to_string(&next_labels)
                .map_err(|err| RegistryError::internal(format!("encode labels: {err}")))?;
            let now = now_rfc3339();
            conn.execute(
                r#"
                INSERT INTO object_overlays (
                    owner_uid, fragment, object_key, favorite, tags_json, labels_json, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ON CONFLICT(owner_uid, fragment, object_key)
                DO UPDATE SET
                    favorite = excluded.favorite,
                    tags_json = excluded.tags_json,
                    labels_json = excluded.labels_json,
                    updated_at = excluded.updated_at
                "#,
                params![
                    owner_uid,
                    fragment,
                    object_key,
                    if next_favorite { 1 } else { 0 },
                    tags_json,
                    labels_json,
                    now,
                    now
                ],
            )
            .map_err(sql_err)?;
            resolve_overlay(&conn, &owner_uid, &fragment, &object_key)
        })
        .await
    }

    pub async fn add_object_comment(
        &self,
        owner_uid: String,
        actor_uid: String,
        fragment: String,
        object_key: String,
        body: String,
    ) -> Result<ObjectCommentRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let body = body.trim().to_string();
            if body.is_empty() {
                return Err(RegistryError::bad_request("comment body is required"));
            }
            if body.len() > 8192 {
                return Err(RegistryError::bad_request(
                    "comment body cannot exceed 8192 bytes",
                ));
            }
            let conn = db.open_connection()?;
            let comment_id = Uuid::new_v4().to_string();
            let now = now_rfc3339();
            conn.execute(
                r#"
                INSERT INTO object_comments (
                    comment_id, owner_uid, actor_uid, fragment, object_key, body, created_at, updated_at, deleted_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL)
                "#,
                params![
                    comment_id,
                    owner_uid,
                    actor_uid,
                    fragment,
                    object_key,
                    body,
                    now,
                    now
                ],
            )
            .map_err(sql_err)?;
            conn.query_row(
                r#"
                SELECT comment_id, owner_uid, actor_uid, fragment, object_key, body, created_at, updated_at, deleted_at
                FROM object_comments
                WHERE comment_id = ?1
                "#,
                params![comment_id],
                map_object_comment_row,
            )
            .map_err(sql_err)
        })
        .await
    }

    pub async fn list_object_comments(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
        include_deleted: bool,
    ) -> Result<Vec<ObjectCommentRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let mut sql = String::from(
                r#"
                SELECT comment_id, owner_uid, actor_uid, fragment, object_key, body, created_at, updated_at, deleted_at
                FROM object_comments
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                "#,
            );
            if !include_deleted {
                sql.push_str(" AND deleted_at IS NULL");
            }
            sql.push_str(" ORDER BY created_at ASC, comment_id ASC");
            let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
            let rows = stmt
                .query_map(params![owner_uid, fragment, object_key], map_object_comment_row)
                .map_err(sql_err)?;
            let mut comments = Vec::new();
            for row in rows {
                comments.push(row.map_err(sql_err)?);
            }
            Ok(comments)
        })
        .await
    }

    pub async fn delete_object_comment(
        &self,
        owner_uid: String,
        comment_id: String,
    ) -> Result<bool, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let changed = conn
                .execute(
                    r#"
                    UPDATE object_comments
                    SET deleted_at = COALESCE(deleted_at, ?3), updated_at = ?3
                    WHERE owner_uid = ?1 AND comment_id = ?2
                    "#,
                    params![owner_uid, comment_id, now_rfc3339()],
                )
                .map_err(sql_err)?;
            Ok(changed > 0)
        })
        .await
    }

    pub async fn admin_stats(&self) -> Result<RegistryAdminStats, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let now_epoch = Utc::now().timestamp();

            let total_users = scalar_i64(&conn, "SELECT COUNT(*) FROM users", params![])? as usize;
            let admin_users =
                scalar_i64(&conn, "SELECT COUNT(*) FROM users WHERE is_admin = 1", params![])?
                    as usize;
            let two_factor_enabled_users = scalar_i64(
                &conn,
                "SELECT COUNT(*) FROM users WHERE totp_enabled = 1",
                params![],
            )? as usize;
            let total_quota_bytes = scalar_i64(
                &conn,
                "SELECT COALESCE(SUM(quota_limit_bytes), 0) FROM users",
                params![],
            )?;
            let total_fragments =
                scalar_i64(&conn, "SELECT COUNT(*) FROM fragment_bindings", params![])? as usize;
            let ready_objects = scalar_i64(
                &conn,
                "SELECT COUNT(*) FROM objects WHERE status = 'ready'",
                params![],
            )? as usize;
            let trashed_objects = scalar_i64(
                &conn,
                "SELECT COUNT(*) FROM objects WHERE status = 'trashed'",
                params![],
            )? as usize;
            let pending_uploads =
                scalar_i64(&conn, "SELECT COUNT(*) FROM object_uploads", params![])? as usize;
            let active_sessions = scalar_i64(
                &conn,
                r#"
                SELECT COUNT(*)
                FROM registry_refresh_sessions
                WHERE revoked_at IS NULL AND expires_at > ?1
                "#,
                params![now_epoch],
            )? as usize;
            let active_users = scalar_i64(
                &conn,
                r#"
                SELECT COUNT(DISTINCT user_uid)
                FROM registry_refresh_sessions
                WHERE revoked_at IS NULL AND expires_at > ?1
                "#,
                params![now_epoch],
            )? as usize;

            let mut extension_stats = std::collections::BTreeMap::<String, (usize, i64)>::new();
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT object_key, object_size_bytes
                    FROM objects
                    WHERE status = 'ready'
                    "#,
                )
                .map_err(sql_err)?;
            let mut rows = stmt
                .query_map(params![], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(sql_err)?;
            while let Some(row) = rows.next().transpose().map_err(sql_err)? {
                let extension = row
                    .0
                    .rsplit_once('.')
                    .map(|(_, value)| value.trim().to_lowercase())
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "_none".to_string());
                let entry = extension_stats.entry(extension).or_insert((0usize, 0i64));
                entry.0 += 1;
                entry.1 = entry.1.saturating_add(row.1.max(0));
            }
            let mut file_types = extension_stats
                .into_iter()
                .map(
                    |(extension, (object_count, total_size_bytes))| ObjectExtensionStat {
                        extension,
                        object_count,
                        total_size_bytes,
                    },
                )
                .collect::<Vec<_>>();
            file_types.sort_by(|a, b| {
                b.total_size_bytes
                    .cmp(&a.total_size_bytes)
                    .then_with(|| b.object_count.cmp(&a.object_count))
                    .then_with(|| a.extension.cmp(&b.extension))
            });

            let mut largest_objects = Vec::new();
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT o.owner_uid, u.email, o.fragment, o.object_key, o.object_size_bytes, o.content_type, o.updated_at
                    FROM objects o
                    JOIN users u ON u.uid = o.owner_uid
                    WHERE o.status = 'ready'
                    ORDER BY o.object_size_bytes DESC, o.updated_at DESC, o.object_key ASC
                    LIMIT 12
                    "#,
                )
                .map_err(sql_err)?;
            let mut rows = stmt
                .query_map(params![], |row| {
                    Ok(AdminLargestObjectSummary {
                        uid: row.get(0)?,
                        email: row.get(1)?,
                        fragment: row.get(2)?,
                        object_key: row.get(3)?,
                        object_size_bytes: row.get::<_, i64>(4)?.max(0),
                        content_type: row.get(5)?,
                        updated_at: row.get(6)?,
                    })
                })
                .map_err(sql_err)?;
            while let Some(row) = rows.next().transpose().map_err(sql_err)? {
                largest_objects.push(row);
            }

            let mut users = Vec::new();
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT uid, email, quota_limit_bytes, quota_used_bytes, updated_at, is_admin, totp_enabled
                    FROM users
                    ORDER BY quota_used_bytes DESC, updated_at DESC, email ASC
                    "#,
                )
                .map_err(sql_err)?;
            let mut rows = stmt
                .query_map(params![], |row| {
                    Ok(UserRecord {
                        uid: row.get(0)?,
                        email: row.get(1)?,
                        is_admin: row.get::<_, i64>(5)? != 0,
                        two_factor_enabled: row.get::<_, i64>(6)? != 0,
                        quota_limit_bytes: row.get(2)?,
                        quota_used_bytes: row.get(3)?,
                        created_at: String::new(),
                        updated_at: row.get(4)?,
                    })
                })
                .map_err(sql_err)?;
            while let Some(row) = rows.next().transpose().map_err(sql_err)? {
                let mut user = row;
                apply_effective_quota_usage(&conn, &mut user)?;
                users.push(user);
            }

            let mut user_activity = Vec::with_capacity(users.len());
            for user in users {
                let active_session_count = scalar_i64(
                    &conn,
                    r#"
                    SELECT COUNT(*)
                    FROM registry_refresh_sessions
                    WHERE user_uid = ?1 AND revoked_at IS NULL AND expires_at > ?2
                    "#,
                    params![user.uid.clone(), now_epoch],
                )? as usize;
                let fragment_count = scalar_i64(
                    &conn,
                    "SELECT COUNT(*) FROM fragment_bindings WHERE owner_uid = ?1",
                    params![user.uid.clone()],
                )? as usize;
                let ready_object_count = scalar_i64(
                    &conn,
                    "SELECT COUNT(*) FROM objects WHERE owner_uid = ?1 AND status = 'ready'",
                    params![user.uid.clone()],
                )? as usize;
                user_activity.push(AdminActivityUserSummary {
                    uid: user.uid,
                    email: user.email,
                    is_admin: user.is_admin,
                    two_factor_enabled: user.two_factor_enabled,
                    active_sessions: active_session_count,
                    fragment_count,
                    ready_object_count,
                    quota_used_bytes: user.quota_used_bytes.max(0),
                    quota_limit_bytes: user.quota_limit_bytes.max(0),
                    updated_at: user.updated_at,
                });
            }

            let mut active_user_rows = user_activity
                .iter()
                .filter(|user| user.active_sessions > 0)
                .cloned()
                .collect::<Vec<_>>();
            active_user_rows.sort_by(|a, b| {
                b.active_sessions
                    .cmp(&a.active_sessions)
                    .then_with(|| b.quota_used_bytes.cmp(&a.quota_used_bytes))
                    .then_with(|| a.email.cmp(&b.email))
            });
            active_user_rows.truncate(8);

            let mut top_storage_users = user_activity
                .iter()
                .filter(|user| user.quota_used_bytes > 0 || user.ready_object_count > 0)
                .cloned()
                .collect::<Vec<_>>();
            top_storage_users.sort_by(|a, b| {
                b.quota_used_bytes
                    .cmp(&a.quota_used_bytes)
                    .then_with(|| b.ready_object_count.cmp(&a.ready_object_count))
                    .then_with(|| a.email.cmp(&b.email))
            });
            top_storage_users.truncate(8);

            let users_using_storage = user_activity
                .iter()
                .filter(|user| user.quota_used_bytes > 0 || user.ready_object_count > 0)
                .count();
            let total_used_bytes = user_activity
                .iter()
                .map(|user| user.quota_used_bytes.max(0))
                .sum();

            Ok(RegistryAdminStats {
                totals: AdminStatsTotals {
                    total_users,
                    admin_users,
                    two_factor_enabled_users,
                    users_using_storage,
                    total_quota_bytes,
                    total_used_bytes,
                    total_fragments,
                    ready_objects,
                    trashed_objects,
                    pending_uploads,
                    active_sessions,
                    active_users,
                },
                file_types,
                largest_objects,
                active_users: active_user_rows,
                top_storage_users,
            })
        })
        .await
    }

    pub async fn update_user_quota(
        &self,
        uid: String,
        quota_limit_bytes: i64,
    ) -> Result<UserRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let quota_used = effective_quota_used_bytes_for_user(&tx, &uid)?;
            if quota_limit_bytes < quota_used {
                return Err(RegistryError::conflict(
                    "quota limit cannot be lower than current usage",
                ));
            }
            tx.execute(
                "UPDATE users SET quota_limit_bytes = ?2, updated_at = ?3 WHERE uid = ?1",
                params![uid.clone(), quota_limit_bytes, now_rfc3339()],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            user_by_uid(&conn, &uid)
        })
        .await
    }

    pub async fn update_user_credentials(
        &self,
        uid: String,
        next_email: Option<String>,
        next_password_hash: Option<String>,
        is_admin: Option<bool>,
    ) -> Result<UserRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let current = user_auth_by_uid(&tx, &uid)?
                .ok_or_else(|| RegistryError::not_found("user not found"))?;
            let email = next_email.unwrap_or(current.user.email);
            tx.execute(
                r#"
                UPDATE users
                SET email = ?2,
                    password_hash = COALESCE(?3, password_hash),
                    is_admin = COALESCE(?4, is_admin),
                    updated_at = ?5
                WHERE uid = ?1
                "#,
                params![
                    uid.clone(),
                    email,
                    next_password_hash,
                    is_admin.map(|value| if value { 1 } else { 0 }),
                    now_rfc3339()
                ],
            )
            .map_err(map_insert_user_err)?;
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            user_by_uid(&conn, &uid)
        })
        .await
    }

    pub async fn prepare_pending_totp(
        &self,
        uid: String,
        secret: String,
    ) -> Result<UserRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            tx.execute(
                r#"
                UPDATE users
                SET totp_pending_secret = ?2,
                    updated_at = ?3
                WHERE uid = ?1
                "#,
                params![uid.clone(), secret, now_rfc3339()],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            user_by_uid(&conn, &uid)
        })
        .await
    }

    pub async fn activate_totp_secret(&self, uid: String) -> Result<UserRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            tx.execute(
                r#"
                UPDATE users
                SET totp_secret = COALESCE(totp_pending_secret, totp_secret),
                    totp_pending_secret = NULL,
                    totp_enabled = CASE WHEN COALESCE(totp_pending_secret, totp_secret) IS NULL THEN 0 ELSE 1 END,
                    updated_at = ?2
                WHERE uid = ?1
                "#,
                params![uid.clone(), now_rfc3339()],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            user_by_uid(&conn, &uid)
        })
        .await
    }

    pub async fn force_set_totp_secret(
        &self,
        uid: String,
        secret: String,
    ) -> Result<UserRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            tx.execute(
                r#"
                UPDATE users
                SET totp_secret = ?2,
                    totp_pending_secret = NULL,
                    totp_enabled = 1,
                    updated_at = ?3
                WHERE uid = ?1
                "#,
                params![uid.clone(), secret, now_rfc3339()],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            user_by_uid(&conn, &uid)
        })
        .await
    }

    pub async fn clear_totp(&self, uid: String) -> Result<UserRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            tx.execute(
                r#"
                UPDATE users
                SET totp_secret = NULL,
                    totp_pending_secret = NULL,
                    totp_enabled = 0,
                    updated_at = ?2
                WHERE uid = ?1
                "#,
                params![uid.clone(), now_rfc3339()],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            user_by_uid(&conn, &uid)
        })
        .await
    }

    pub async fn rename_fragment_references(
        &self,
        owner_uid: String,
        old_fragment: String,
        new_fragment: String,
    ) -> Result<(), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            tx.execute(
                "UPDATE objects SET fragment = ?3 WHERE owner_uid = ?1 AND fragment = ?2",
                params![owner_uid, old_fragment, new_fragment],
            )
            .map_err(sql_err)?;
            tx.execute(
                "UPDATE object_versions SET fragment = ?3 WHERE owner_uid = ?1 AND fragment = ?2",
                params![owner_uid, old_fragment, new_fragment],
            )
            .map_err(sql_err)?;
            tx.execute(
                "UPDATE object_uploads SET fragment = ?3 WHERE owner_uid = ?1 AND fragment = ?2",
                params![owner_uid, old_fragment, new_fragment],
            )
            .map_err(sql_err)?;
            tx.execute(
                "UPDATE fragment_bindings SET fragment = ?3, updated_at = ?4 WHERE owner_uid = ?1 AND fragment = ?2",
                params![owner_uid, old_fragment, new_fragment, now_rfc3339()],
            )
            .map_err(sql_err)?;
            tx.execute(
                "UPDATE object_shares SET fragment = ?3 WHERE owner_uid = ?1 AND fragment = ?2",
                params![owner_uid, old_fragment, new_fragment],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    pub async fn delete_user_cascade(&self, uid: String) -> Result<(), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            tx.execute(
                "DELETE FROM registry_refresh_sessions WHERE user_uid = ?1",
                params![uid.clone()],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_uploads WHERE owner_uid = ?1",
                params![uid.clone()],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_versions WHERE owner_uid = ?1",
                params![uid.clone()],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM objects WHERE owner_uid = ?1",
                params![uid.clone()],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM fragment_bindings WHERE owner_uid = ?1",
                params![uid.clone()],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_shares WHERE owner_uid = ?1",
                params![uid.clone()],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_overlays WHERE owner_uid = ?1",
                params![uid.clone()],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_comments WHERE owner_uid = ?1",
                params![uid.clone()],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM audit_events WHERE owner_uid = ?1 OR actor_uid = ?1",
                params![uid.clone()],
            )
            .map_err(sql_err)?;
            let removed = tx
                .execute("DELETE FROM users WHERE uid = ?1", params![uid.clone()])
                .map_err(sql_err)?;
            if removed == 0 {
                return Err(RegistryError::not_found("user not found"));
            }
            tx.commit().map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    pub async fn create_refresh_session(
        &self,
        user_uid: String,
        ttl_secs: u64,
    ) -> Result<(String, u64), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let now = now_unix();
            let created_at = now_rfc3339();
            let expires_at = now.saturating_add(ttl_secs as i64);
            let token = generate_refresh_token();
            let token_hash = hash_refresh_token(&token);
            conn.execute(
                r#"
                INSERT INTO registry_refresh_sessions (
                    token_hash, user_uid, expires_at, created_at, rotated_at, replaced_by_hash, revoked_at
                ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, NULL)
                "#,
                params![token_hash, user_uid, expires_at, created_at],
            )
            .map_err(sql_err)?;
            Ok((token, ttl_secs))
        })
        .await
    }

    pub async fn rotate_refresh_session(
        &self,
        refresh_token: String,
        ttl_secs: u64,
    ) -> Result<(UserRecord, String, u64), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let now = now_unix();
            let now_text = now_rfc3339();
            let token_hash = hash_refresh_token(refresh_token.trim());

            let session = tx
                .query_row(
                    r#"
                    SELECT user_uid, expires_at, replaced_by_hash, revoked_at
                    FROM registry_refresh_sessions
                    WHERE token_hash = ?1
                    "#,
                    params![token_hash],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    },
                )
                .optional()
                .map_err(sql_err)?
                .ok_or_else(|| RegistryError::unauthorized("invalid or expired refresh token"))?;

            if session.3.is_some() || session.1 <= now {
                tx.execute(
                    "UPDATE registry_refresh_sessions SET revoked_at = COALESCE(revoked_at, ?2) WHERE token_hash = ?1",
                    params![token_hash, now_text],
                )
                .map_err(sql_err)?;
                tx.commit().map_err(sql_err)?;
                return Err(RegistryError::unauthorized(
                    "invalid or expired refresh token",
                ));
            }

            if let Some(next_hash) = session.2.clone() {
                revoke_refresh_chain(&tx, &next_hash, &now_text)?;
                tx.execute(
                    "UPDATE registry_refresh_sessions SET revoked_at = COALESCE(revoked_at, ?2) WHERE token_hash = ?1",
                    params![token_hash, now_text],
                )
                .map_err(sql_err)?;
                tx.commit().map_err(sql_err)?;
                return Err(RegistryError::unauthorized(
                    "invalid or expired refresh token",
                ));
            }

            let next_token = generate_refresh_token();
            let next_hash = hash_refresh_token(&next_token);
            let next_expires_at = now.saturating_add(ttl_secs as i64);

            tx.execute(
                r#"
                INSERT INTO registry_refresh_sessions (
                    token_hash, user_uid, expires_at, created_at, rotated_at, replaced_by_hash, revoked_at
                ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, NULL)
                "#,
                params![next_hash, session.0, next_expires_at, now_text],
            )
            .map_err(sql_err)?;

            tx.execute(
                r#"
                UPDATE registry_refresh_sessions
                SET rotated_at = ?2, replaced_by_hash = ?3
                WHERE token_hash = ?1
                "#,
                params![token_hash, now_text, next_hash],
            )
            .map_err(sql_err)?;

            let user = user_by_uid(&tx, &session.0)?;
            tx.commit().map_err(sql_err)?;
            Ok((user, next_token, ttl_secs))
        })
        .await
    }

    pub async fn revoke_refresh_session(&self, refresh_token: String) -> Result<(), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let token_hash = hash_refresh_token(refresh_token.trim());
            let now_text = now_rfc3339();
            let next_hash = tx
                .query_row(
                    "SELECT replaced_by_hash FROM registry_refresh_sessions WHERE token_hash = ?1",
                    params![token_hash],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()
                .map_err(sql_err)?
                .flatten();

            tx.execute(
                "UPDATE registry_refresh_sessions SET revoked_at = COALESCE(revoked_at, ?2) WHERE token_hash = ?1",
                params![token_hash, now_text],
            )
            .map_err(sql_err)?;
            if let Some(next_hash) = next_hash {
                revoke_refresh_chain(&tx, &next_hash, &now_text)?;
            }
            tx.commit().map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    pub async fn register_pending_object(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
        expected_size_bytes: Option<i64>,
        content_type: Option<String>,
        version_id: Option<String>,
        session_id: Option<String>,
    ) -> Result<ObjectRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;

            let (quota_limit, quota_used) = quota_for_user(&tx, &owner_uid)?;
            let current_reserved =
                pending_quota_reserved_bytes_for_user(&tx, &owner_uid, None)?;
            let existing_ready_size = tx
                .query_row(
                    r#"
                    SELECT object_size_bytes
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, object_key],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(sql_err)?
                .unwrap_or(0);
            let existing_storage_object_id = tx
                .query_row(
                    r#"
                    SELECT storage_object_id
                    FROM object_uploads
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, object_key],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(sql_err)?
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    tx.query_row(
                        r#"
                        SELECT storage_object_id
                        FROM objects
                        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                        "#,
                        params![owner_uid, fragment, object_key],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .ok()
                    .flatten()
                    .filter(|value| !value.trim().is_empty())
                })
                .unwrap_or_else(|| object_key.clone());
            let reserved_bytes = expected_size_bytes
                .unwrap_or(0)
                .max(0)
                .saturating_sub(existing_ready_size.max(0));

            if expected_size_bytes.is_some() {
                let other_reserved = pending_quota_reserved_bytes_for_user(
                    &tx,
                    &owner_uid,
                    Some((&fragment, &object_key)),
                )?;
                let projected_delta = expected_size_bytes
                    .unwrap_or(0)
                    .max(0)
                    .saturating_sub(existing_ready_size.max(0));
                let current_effective = quota_used.saturating_add(current_reserved);
                let projected_effective = quota_used
                    .saturating_add(other_reserved)
                    .saturating_add(projected_delta);
                if projected_effective > quota_limit && projected_effective > current_effective {
                    return Err(RegistryError::conflict("quota exceeded for pending object"));
                }
            }

            let now = now_rfc3339();
            tx.execute(
                r#"
                INSERT INTO object_uploads (
                    owner_uid, fragment, object_key, storage_object_id, expected_size_bytes, reserved_bytes, content_type, version_id, session_id, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                ON CONFLICT(owner_uid, fragment, object_key)
                DO UPDATE SET
                    storage_object_id = excluded.storage_object_id,
                    expected_size_bytes = excluded.expected_size_bytes,
                    reserved_bytes = excluded.reserved_bytes,
                    content_type = excluded.content_type,
                    version_id = excluded.version_id,
                    session_id = excluded.session_id,
                    updated_at = excluded.updated_at
                "#,
                params![
                    owner_uid,
                    fragment,
                    object_key,
                    existing_storage_object_id,
                    expected_size_bytes,
                    reserved_bytes,
                    content_type,
                    version_id,
                    session_id,
                    now,
                    now
                ],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;

            Ok(ObjectRecord {
                owner_uid,
                fragment,
                object_key,
                storage_object_id: existing_storage_object_id,
                version_id: String::new(),
                status: "pending".to_string(),
                kind: Some("file".to_string()),
                object_size_bytes: expected_size_bytes.unwrap_or(0),
                content_type,
                trash_entry_id: None,
                deleted_at_unix: None,
                created_at: now.clone(),
                updated_at: now,
            })
        })
        .await
    }

    pub async fn finalize_object(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
        version_id: String,
        object_size_bytes: i64,
        content_type: Option<String>,
    ) -> Result<(ObjectRecord, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;

            let (quota_limit, quota_used) = quota_for_user(&tx, &owner_uid)?;
            let current_reserved =
                pending_quota_reserved_bytes_for_user(&tx, &owner_uid, None)?;
            let existing = tx
                .query_row(
                    r#"
                    SELECT object_size_bytes, created_at, storage_object_id
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, object_key],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(sql_err)?;
            let previous_size = existing.as_ref().map(|v| v.0).unwrap_or(0);
            let created_at = existing
                .as_ref()
                .map(|v| v.1.clone())
                .unwrap_or_else(now_rfc3339);
            let storage_object_id = tx
                .query_row(
                    r#"
                    SELECT storage_object_id
                    FROM object_uploads
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, object_key],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(sql_err)?
                .filter(|value| !value.trim().is_empty())
                .or_else(|| existing.as_ref().map(|value| value.2.clone()).filter(|value| !value.trim().is_empty()))
                .unwrap_or_else(|| object_key.clone());
            let delta = object_size_bytes.saturating_sub(previous_size);
            let next_used = quota_used.saturating_add(delta).max(0);
            let other_reserved = pending_quota_reserved_bytes_for_user(
                &tx,
                &owner_uid,
                Some((&fragment, &object_key)),
            )?;
            let current_effective = quota_used.saturating_add(current_reserved);
            let projected_effective = next_used.saturating_add(other_reserved);
            if projected_effective > quota_limit && projected_effective > current_effective {
                return Err(RegistryError::conflict("quota exceeded"));
            }

            let now = now_rfc3339();
            tx.execute(
                r#"
                INSERT INTO objects (
                    owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, 'ready', ?6, ?7, ?8, ?9)
                ON CONFLICT(owner_uid, fragment, object_key)
                DO UPDATE SET
                    storage_object_id = excluded.storage_object_id,
                    version_id = excluded.version_id,
                    status = 'ready',
                    object_size_bytes = excluded.object_size_bytes,
                    content_type = excluded.content_type,
                    updated_at = excluded.updated_at
                "#,
                params![
                    owner_uid,
                    fragment,
                    object_key,
                    storage_object_id,
                    version_id,
                    object_size_bytes,
                    content_type,
                    created_at,
                    now
                ],
            )
            .map_err(sql_err)?;

            tx.execute(
                r#"
                INSERT OR REPLACE INTO object_versions (
                    owner_uid, fragment, object_key, storage_object_id, version_id, object_size_bytes, content_type, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                "#,
                params![
                    owner_uid,
                    fragment,
                    object_key,
                    storage_object_id,
                    version_id,
                    object_size_bytes,
                    content_type,
                    now
                ],
            )
            .map_err(sql_err)?;

            tx.execute(
                r#"
                DELETE FROM object_uploads
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                "#,
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;

            refresh_user_quota_used(&tx, &owner_uid, &now)?;

            tx.commit().map_err(sql_err)?;

            let conn = db.open_connection()?;
            let object = resolve_ready_object(&conn, &owner_uid, &fragment, &object_key)?
                .ok_or_else(|| RegistryError::internal("finalized object missing from registry"))?;
            let user = user_by_uid(&conn, &owner_uid)?;
            Ok((object, user))
        })
        .await
    }

    pub async fn list_objects(
        &self,
        owner_uid: String,
        fragment: Option<String>,
        prefix: Option<String>,
        delimiter: Option<String>,
        search: Option<String>,
        limit: usize,
        offset: usize,
        include_pending: bool,
        include_trashed: bool,
    ) -> Result<RegistryListPage, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let fragment_filter = fragment.clone();
            let delimiter = delimiter
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            let prefix_filter =
                normalize_listing_prefix(prefix.clone().unwrap_or_default(), delimiter.as_deref());
            let prefix_pattern = if prefix_filter.is_empty() {
                None
            } else {
                Some(format!("{}%", prefix_filter))
            };
            let search_pattern = search
                .clone()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(|value| format!("%{}%", value));
            let target_status = if include_trashed { "trashed" } else { "ready" };

            let mut sql = String::from(
                r#"
                SELECT owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
                FROM objects
                WHERE owner_uid = ?
                "#,
            );
            let mut args = vec![Value::Text(owner_uid.clone())];
            sql.push_str(" AND status = ?");
            args.push(Value::Text(target_status.to_string()));
            if let Some(fragment_value) = fragment_filter.clone() {
                sql.push_str(" AND fragment = ?");
                args.push(Value::Text(fragment_value));
            }
            if let Some(pattern) = prefix_pattern.clone() {
                sql.push_str(" AND object_key LIKE ?");
                args.push(Value::Text(pattern));
            }
            if let Some(pattern) = search_pattern.clone() {
                sql.push_str(" AND object_key LIKE ?");
                args.push(Value::Text(pattern));
            }
            sql.push_str(" ORDER BY object_key ASC");

            let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
            let mut rows = stmt
                .query_map(params_from_iter(args), map_object_row)
                .map_err(sql_err)?;

            let mut by_key = std::collections::BTreeMap::<(String, String), ObjectRecord>::new();
            while let Some(row) = rows.next().transpose().map_err(sql_err)? {
                by_key.insert((row.fragment.clone(), row.object_key.clone()), row);
            }

            if include_pending && !include_trashed {
                let mut pending_sql = String::from(
                    r#"
                    SELECT owner_uid, fragment, object_key, storage_object_id, expected_size_bytes, content_type, created_at, updated_at
                    FROM object_uploads
                    WHERE owner_uid = ?
                    "#,
                );
                let mut pending_args = vec![Value::Text(owner_uid)];
                if let Some(fragment_value) = fragment_filter {
                    pending_sql.push_str(" AND fragment = ?");
                    pending_args.push(Value::Text(fragment_value));
                }
                if let Some(pattern) = prefix_pattern {
                    pending_sql.push_str(" AND object_key LIKE ?");
                    pending_args.push(Value::Text(pattern));
                }
                if let Some(pattern) = search_pattern {
                    pending_sql.push_str(" AND object_key LIKE ?");
                    pending_args.push(Value::Text(pattern));
                }
                pending_sql.push_str(" ORDER BY object_key ASC");

                let mut stmt = conn.prepare(&pending_sql).map_err(sql_err)?;
                let mut rows = stmt
                    .query_map(params_from_iter(pending_args), map_pending_row)
                    .map_err(sql_err)?;

                while let Some(row) = rows.next().transpose().map_err(sql_err)? {
                    by_key.insert((row.fragment.clone(), row.object_key.clone()), row);
                }
            }

            let mut out = by_key.into_values().collect::<Vec<_>>();
            out.sort_by(|a, b| a.object_key.cmp(&b.object_key));

            let entries = if delimiter.as_deref() == Some("/") {
                let mut emitted_prefixes = std::collections::BTreeSet::new();
                let mut emitted = Vec::new();
                for object in out {
                    let suffix = if prefix_filter.is_empty() {
                        object.object_key.as_str()
                    } else if let Some(stripped) = object.object_key.strip_prefix(&prefix_filter) {
                        stripped
                    } else {
                        continue;
                    };
                    if suffix.is_empty() {
                        emitted.push(RegistryListEntry::Object(object));
                        continue;
                    }
                    if let Some(idx) = suffix.find('/') {
                        let common_prefix = format!("{}{}", prefix_filter, &suffix[..idx + 1]);
                        if emitted_prefixes.insert(common_prefix.clone()) {
                            emitted.push(RegistryListEntry::CommonPrefix(common_prefix));
                        }
                        continue;
                    }
                    emitted.push(RegistryListEntry::Object(object));
                }
                emitted
            } else {
                out.into_iter()
                    .map(RegistryListEntry::Object)
                    .collect::<Vec<_>>()
            };

            let mut paged_entries = entries
                .into_iter()
                .skip(offset)
                .take(limit.saturating_add(1))
                .collect::<Vec<_>>();
            let has_more = paged_entries.len() > limit;
            if has_more {
                paged_entries.truncate(limit);
            }

            let mut paged_objects = Vec::new();
            let mut common_prefixes = Vec::new();
            for entry in paged_entries {
                match entry {
                    RegistryListEntry::Object(object) => paged_objects.push(object),
                    RegistryListEntry::CommonPrefix(prefix) => common_prefixes.push(prefix),
                }
            }

            Ok(RegistryListPage {
                objects: paged_objects,
                common_prefixes,
                has_more,
            })
        })
        .await
    }

    pub async fn object_stats(
        &self,
        owner_uid: String,
        fragment: Option<String>,
        prefix: Option<String>,
        search: Option<String>,
        include_pending: bool,
        include_trashed: bool,
    ) -> Result<RegistryObjectStats, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let fragment_filter = fragment.clone();
            let prefix_filter = normalize_listing_prefix(prefix.unwrap_or_default(), Some("/"));
            let prefix_pattern = if prefix_filter.is_empty() {
                None
            } else {
                Some(format!("{}%", prefix_filter))
            };
            let search_pattern = search
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(|value| format!("%{}%", value));
            let target_status = if include_trashed { "trashed" } else { "ready" };

            let mut sql = String::from(
                r#"
                SELECT fragment, object_key, object_size_bytes
                FROM objects
                WHERE owner_uid = ?
                "#,
            );
            let mut args = vec![Value::Text(owner_uid.clone())];
            sql.push_str(" AND status = ?");
            args.push(Value::Text(target_status.to_string()));
            if let Some(fragment_value) = fragment_filter.clone() {
                sql.push_str(" AND fragment = ?");
                args.push(Value::Text(fragment_value));
            }
            if let Some(pattern) = prefix_pattern.clone() {
                sql.push_str(" AND object_key LIKE ?");
                args.push(Value::Text(pattern));
            }
            if let Some(pattern) = search_pattern.clone() {
                sql.push_str(" AND object_key LIKE ?");
                args.push(Value::Text(pattern));
            }

            let mut by_key = std::collections::BTreeMap::<(String, String), i64>::new();
            let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
            let mut rows = stmt
                .query_map(params_from_iter(args), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .map_err(sql_err)?;
            while let Some(row) = rows.next().transpose().map_err(sql_err)? {
                by_key.insert((row.0, row.1), row.2.max(0));
            }

            if include_pending && !include_trashed {
                let mut pending_sql = String::from(
                    r#"
                    SELECT fragment, object_key, expected_size_bytes
                    FROM object_uploads
                    WHERE owner_uid = ?
                    "#,
                );
                let mut pending_args = vec![Value::Text(owner_uid)];
                if let Some(fragment_value) = fragment_filter {
                    pending_sql.push_str(" AND fragment = ?");
                    pending_args.push(Value::Text(fragment_value));
                }
                if let Some(pattern) = prefix_pattern {
                    pending_sql.push_str(" AND object_key LIKE ?");
                    pending_args.push(Value::Text(pattern));
                }
                if let Some(pattern) = search_pattern {
                    pending_sql.push_str(" AND object_key LIKE ?");
                    pending_args.push(Value::Text(pattern));
                }

                let mut stmt = conn.prepare(&pending_sql).map_err(sql_err)?;
                let mut rows = stmt
                    .query_map(params_from_iter(pending_args), |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<i64>>(2)?.unwrap_or(0),
                        ))
                    })
                    .map_err(sql_err)?;
                while let Some(row) = rows.next().transpose().map_err(sql_err)? {
                    by_key.insert((row.0, row.1), row.2.max(0));
                }
            }

            let mut extension_stats = std::collections::BTreeMap::<String, (usize, i64)>::new();
            let mut total_size_bytes = 0i64;
            for ((_fragment, object_key), size_bytes) in &by_key {
                total_size_bytes = total_size_bytes.saturating_add(*size_bytes);
                let extension = object_key
                    .rsplit_once('.')
                    .map(|(_, value)| value.trim().to_lowercase())
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "_none".to_string());
                let entry = extension_stats.entry(extension).or_insert((0usize, 0i64));
                entry.0 += 1;
                entry.1 = entry.1.saturating_add(*size_bytes);
            }

            let mut extension_stats = extension_stats
                .into_iter()
                .map(
                    |(extension, (object_count, total_size_bytes))| ObjectExtensionStat {
                        extension,
                        object_count,
                        total_size_bytes,
                    },
                )
                .collect::<Vec<_>>();
            extension_stats.sort_by(|a, b| {
                b.total_size_bytes
                    .cmp(&a.total_size_bytes)
                    .then_with(|| a.extension.cmp(&b.extension))
            });

            Ok(RegistryObjectStats {
                total_objects: by_key.len(),
                total_size_bytes,
                extension_stats,
            })
        })
        .await
    }

    pub async fn list_fragments(&self, owner_uid: String) -> Result<Vec<String>, RegistryError> {
        let bindings = self.list_fragment_bindings(owner_uid.clone()).await?;
        let db = self.clone();
        let mut out = spawn_blocking(move || {
            let conn = db.open_connection()?;
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT DISTINCT fragment FROM (
                        SELECT fragment FROM objects WHERE owner_uid = ?1
                        UNION
                        SELECT fragment FROM object_uploads WHERE owner_uid = ?1
                        UNION
                        SELECT fragment FROM fragment_bindings WHERE owner_uid = ?1
                    )
                    ORDER BY fragment ASC
                    "#,
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![owner_uid], |row| row.get::<_, String>(0))
                .map_err(sql_err)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(sql_err)?);
            }
            Ok(out)
        })
        .await?;
        for binding in bindings {
            if !out.iter().any(|value| value == &binding.fragment) {
                out.push(binding.fragment);
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    pub async fn upsert_fragment_binding(
        &self,
        owner_uid: String,
        fragment: String,
        deployment: StorageDeployment,
        engine_url_override: Option<String>,
    ) -> Result<FragmentBindingRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let now = now_rfc3339();
            tx.execute(
                r#"
                INSERT INTO fragment_bindings (
                    owner_uid, fragment, deployment, engine_url_override, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(owner_uid, fragment)
                DO UPDATE SET
                    deployment = excluded.deployment,
                    engine_url_override = excluded.engine_url_override,
                    updated_at = excluded.updated_at
                "#,
                params![
                    owner_uid,
                    fragment,
                    deployment.as_str(),
                    engine_url_override,
                    now,
                    now
                ],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            resolve_fragment_binding(&conn, &owner_uid, &fragment)?
                .ok_or_else(|| RegistryError::internal("fragment binding missing after save"))
        })
        .await
    }

    pub async fn get_fragment_binding(
        &self,
        owner_uid: String,
        fragment: String,
    ) -> Result<Option<FragmentBindingRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            resolve_fragment_binding(&conn, &owner_uid, &fragment)
        })
        .await
    }

    pub async fn list_fragment_bindings(
        &self,
        owner_uid: String,
    ) -> Result<Vec<FragmentBindingRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT owner_uid, fragment, deployment, engine_url_override, created_at, updated_at
                    FROM fragment_bindings
                    WHERE owner_uid = ?1
                    ORDER BY fragment ASC
                    "#,
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![owner_uid], map_fragment_binding_row)
                .map_err(sql_err)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(sql_err)?);
            }
            Ok(out)
        })
        .await
    }

    pub async fn suggest_object_keys(
        &self,
        owner_uid: String,
        fragment: Option<String>,
        prefix: Option<String>,
        query: String,
        limit: usize,
        include_trashed: bool,
    ) -> Result<Vec<String>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let trimmed = query.trim().to_string();
            if trimmed.is_empty() {
                return Ok(Vec::new());
            }
            let prefix = prefix.map(|value| normalize_listing_prefix(value, Some("/")));

            let status = if include_trashed { "trashed" } else { "ready" };
            let mut sql = String::from(
                r#"
                SELECT object_key
                FROM objects
                WHERE owner_uid = ? AND status = ?
                "#,
            );
            let mut args = vec![Value::Text(owner_uid), Value::Text(status.to_string())];
            if let Some(fragment_value) = fragment {
                sql.push_str(" AND fragment = ?");
                args.push(Value::Text(fragment_value));
            }
            if let Some(prefix_value) = prefix.filter(|value| !value.is_empty()) {
                sql.push_str(" AND object_key LIKE ?");
                args.push(Value::Text(format!("{}%", prefix_value)));
            }
            sql.push_str(" AND object_key LIKE ?");
            args.push(Value::Text(format!("%{}%", trimmed)));
            sql.push_str(" ORDER BY object_key ASC");

            let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
            let rows = stmt
                .query_map(params_from_iter(args), |row| row.get::<_, String>(0))
                .map_err(sql_err)?;

            let needle = trimmed.to_ascii_lowercase();
            let mut results = Vec::new();
            for row in rows {
                results.push(row.map_err(sql_err)?);
            }
            results.sort_by(|left, right| {
                let left_lower = left.to_ascii_lowercase();
                let right_lower = right.to_ascii_lowercase();
                let left_starts = left_lower.starts_with(&needle);
                let right_starts = right_lower.starts_with(&needle);
                right_starts
                    .cmp(&left_starts)
                    .then_with(|| left.len().cmp(&right.len()))
                    .then_with(|| left.cmp(right))
            });
            results.dedup();
            results.truncate(limit);
            Ok(results)
        })
        .await
    }

    pub async fn select_archive_objects(
        &self,
        owner_uid: String,
        fragment: String,
        keys: Vec<String>,
        prefix: Option<String>,
        search: Option<String>,
        include_trashed: bool,
    ) -> Result<ArchiveObjectSelection, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let objects = select_archive_objects_from_conn(
                &conn,
                owner_uid,
                fragment,
                keys,
                prefix,
                search,
                include_trashed,
            )?;
            Ok(ArchiveObjectSelection { objects })
        })
        .await
    }

    pub async fn create_share(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
        disposition: ShareDisposition,
        expires_in_secs: u64,
    ) -> Result<(ShareRecord, String), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let live_object = resolve_live_object(&tx, &owner_uid, &fragment, &object_key)?
                .ok_or_else(|| RegistryError::not_found("object not found"))?;
            if resolve_fragment_binding(&tx, &owner_uid, &fragment)?.is_none() {
                return Err(RegistryError::conflict(
                    "fragment is not linked to a storage backend yet; link it first",
                ));
            }

            let created_at = now_rfc3339();
            let expires_at_unix = now_unix().saturating_add(expires_in_secs as i64);
            let share_id = Uuid::new_v4().to_string();
            let share_token = generate_share_token();
            let token_hash = hash_refresh_token(&share_token);

            tx.execute(
                r#"
                INSERT INTO object_shares (
                    share_id, token_hash, owner_uid, fragment, object_key, storage_object_id, disposition, expires_at, created_at, revoked_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL)
                "#,
                params![
                    share_id,
                    token_hash,
                    owner_uid,
                    fragment,
                    object_key,
                    live_object.storage_object_id,
                    disposition.as_str(),
                    expires_at_unix,
                    created_at
                ],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;

            Ok((
                ShareRecord {
                    share_id,
                    owner_uid: live_object.owner_uid,
                    fragment: live_object.fragment,
                    object_key: live_object.object_key,
                    storage_object_id: live_object.storage_object_id,
                    disposition,
                    expires_at: rfc3339_from_unix(expires_at_unix),
                    created_at,
                    revoked_at: None,
                },
                share_token,
            ))
        })
        .await
    }

    pub async fn list_shares(
        &self,
        owner_uid: String,
        fragment: Option<String>,
        include_expired: bool,
        include_revoked: bool,
    ) -> Result<Vec<ShareRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let mut sql = String::from(
                r#"
                SELECT share_id, owner_uid, fragment, object_key, storage_object_id, disposition, expires_at, created_at, revoked_at
                FROM object_shares
                WHERE owner_uid = ?
                "#,
            );
            let mut args = vec![Value::Text(owner_uid)];
            if let Some(fragment) = fragment.filter(|value| !value.trim().is_empty()) {
                sql.push_str(" AND fragment = ?");
                args.push(Value::Text(fragment));
            }
            if !include_expired {
                sql.push_str(" AND expires_at > ?");
                args.push(Value::Integer(now_unix()));
            }
            if !include_revoked {
                sql.push_str(" AND revoked_at IS NULL");
            }
            sql.push_str(" ORDER BY created_at DESC");

            let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
            let rows = stmt
                .query_map(params_from_iter(args), map_share_row)
                .map_err(sql_err)?;
            let mut shares = Vec::new();
            for row in rows {
                shares.push(row.map_err(sql_err)?);
            }
            Ok(shares)
        })
        .await
    }

    pub async fn revoke_share(
        &self,
        owner_uid: String,
        share_id: String,
    ) -> Result<bool, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let changed = conn
                .execute(
                    r#"
                    UPDATE object_shares
                    SET revoked_at = COALESCE(revoked_at, ?3)
                    WHERE owner_uid = ?1 AND share_id = ?2
                    "#,
                    params![owner_uid, share_id, now_rfc3339()],
                )
                .map_err(sql_err)?;
            Ok(changed > 0)
        })
        .await
    }

    pub async fn resolve_public_share(
        &self,
        token: String,
    ) -> Result<Option<ShareRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            conn.query_row(
                r#"
                SELECT share_id, owner_uid, fragment, object_key, storage_object_id, disposition, expires_at, created_at, revoked_at
                FROM object_shares
                WHERE token_hash = ?1
                  AND revoked_at IS NULL
                  AND expires_at > ?2
                "#,
                params![hash_refresh_token(token.trim()), now_unix()],
                map_share_row,
            )
            .optional()
            .map_err(sql_err)
        })
        .await
    }

    pub async fn resolve_object(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
    ) -> Result<Option<ObjectRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            resolve_ready_object(&conn, &owner_uid, &fragment, &object_key)
        })
        .await
    }

    pub async fn resolve_any_object_record(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
    ) -> Result<Option<ObjectRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            resolve_any_object(&conn, &owner_uid, &fragment, &object_key)
        })
        .await
    }

    pub async fn resolve_stored_object_record(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
    ) -> Result<Option<ObjectRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            resolve_stored_object(&conn, &owner_uid, &fragment, &object_key)
        })
        .await
    }

    pub async fn sync_object_from_engine(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
        storage_object_id: String,
        current: Option<EngineObjectMetadata>,
        versions: Vec<EngineObjectMetadata>,
    ) -> Result<(Option<ObjectRecord>, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;
            let existing = resolve_ready_object(&tx, &owner_uid, &fragment, &object_key)?;
            let storage_object_id = (!storage_object_id.trim().is_empty())
                .then_some(storage_object_id)
                .or_else(|| {
                    tx.query_row(
                        r#"
                        SELECT storage_object_id
                        FROM object_uploads
                        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                        "#,
                        params![owner_uid, fragment, object_key],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .ok()
                    .flatten()
                    .filter(|value| !value.trim().is_empty())
                })
                .or_else(|| {
                    existing
                        .as_ref()
                        .map(|record| record.storage_object_id.clone())
                        .filter(|value| !value.trim().is_empty())
                })
                .unwrap_or_else(|| object_key.clone());
            let now = now_rfc3339();

            if let Some(current) = current.clone() {
                let current_size = current.object_size_bytes.max(0);
                let created_at = existing
                    .as_ref()
                    .map(|record| record.created_at.clone())
                    .unwrap_or_else(|| rfc3339_from_unix(current.created_at_unix));

                tx.execute(
                    r#"
                    INSERT INTO objects (
                        owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
                    ) VALUES (?1, ?2, ?3, ?4, ?5, 'ready', ?6, ?7, ?8, ?9)
                    ON CONFLICT(owner_uid, fragment, object_key)
                    DO UPDATE SET
                        storage_object_id = excluded.storage_object_id,
                        version_id = excluded.version_id,
                        status = 'ready',
                        object_size_bytes = excluded.object_size_bytes,
                        content_type = excluded.content_type,
                        updated_at = excluded.updated_at
                    "#,
                    params![
                        owner_uid,
                        fragment,
                        object_key,
                        storage_object_id,
                        current.version_id,
                        current_size,
                        current.content_type,
                        created_at,
                        now
                    ],
                )
                .map_err(sql_err)?;

                tx.execute(
                    "DELETE FROM object_versions WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, object_key],
                )
                .map_err(sql_err)?;

                let versions_to_store = if versions.is_empty() {
                    vec![current]
                } else {
                    versions
                };
                for version in versions_to_store {
                    tx.execute(
                        r#"
                        INSERT OR REPLACE INTO object_versions (
                            owner_uid, fragment, object_key, storage_object_id, version_id, object_size_bytes, content_type, created_at
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                        "#,
                        params![
                            owner_uid,
                            fragment,
                            object_key,
                            storage_object_id,
                            version.version_id,
                            version.object_size_bytes.max(0),
                            version.content_type,
                            rfc3339_from_unix(version.created_at_unix)
                        ],
                    )
                    .map_err(sql_err)?;
                }

                tx.execute(
                    "DELETE FROM object_uploads WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, object_key],
                )
                .map_err(sql_err)?;
            } else {
                tx.execute(
                    "DELETE FROM objects WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, object_key],
                )
                .map_err(sql_err)?;
                tx.execute(
                    "DELETE FROM object_versions WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, object_key],
                )
                .map_err(sql_err)?;
                tx.execute(
                    "DELETE FROM object_uploads WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, object_key],
                )
                .map_err(sql_err)?;
                tx.execute(
                    "DELETE FROM object_shares WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, object_key],
                )
                .map_err(sql_err)?;
            }

            refresh_user_quota_used(&tx, &owner_uid, &now)?;

            tx.commit().map_err(sql_err)?;

            let conn = db.open_connection()?;
            let object = resolve_ready_object(&conn, &owner_uid, &fragment, &object_key)?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((object, user))
        })
        .await
    }

    pub async fn set_object_status(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
        next_status: &'static str,
    ) -> Result<(ObjectRecord, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let now = now_rfc3339();
            let changed = tx
                .execute(
                    r#"
                    UPDATE objects
                    SET status = ?4, updated_at = ?5
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, object_key, next_status, now],
                )
                .map_err(sql_err)?;
            if changed == 0 {
                return Err(RegistryError::not_found("object not found"));
            }
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let object = resolve_any_object(&conn, &owner_uid, &fragment, &object_key)?
                .ok_or_else(|| RegistryError::internal("updated object missing"))?;
            let user = user_by_uid(&conn, &owner_uid)?;
            Ok((object, user))
        })
        .await
    }

    pub async fn trash_object_if_ready(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
    ) -> Result<(ObjectRecord, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let now = now_rfc3339();
            let changed = tx
                .execute(
                    r#"
                    UPDATE objects
                    SET status = 'trashed', updated_at = ?4
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3 AND status = 'ready'
                    "#,
                    params![owner_uid, fragment, object_key, now],
                )
                .map_err(sql_err)?;
            if changed == 0 {
                return Err(RegistryError::not_found("object not found"));
            }
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let object = resolve_any_object(&conn, &owner_uid, &fragment, &object_key)?
                .ok_or_else(|| RegistryError::internal("updated object missing"))?;
            let user = user_by_uid(&conn, &owner_uid)?;
            Ok((object, user))
        })
        .await
    }

    #[cfg(test)]
    pub async fn set_objects_status_bulk(
        &self,
        owner_uid: String,
        fragment: String,
        object_keys: Vec<String>,
        next_status: &'static str,
    ) -> Result<(usize, usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut selected_keys = object_keys
                .into_iter()
                .map(|value| canonical_archive_key(&value))
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            selected_keys.sort();
            selected_keys.dedup();

            if selected_keys.is_empty() {
                return Err(RegistryError::bad_request(
                    "object_keys must include at least one object key",
                ));
            }

            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let now = now_rfc3339();
            let mut affected = 0usize;
            let mut matched_total = 0usize;

            for chunk in selected_keys.chunks(SQLITE_BULK_KEY_CHUNK) {
                let existing_keys = select_existing_object_keys(&tx, &owner_uid, &fragment, chunk)?;
                if existing_keys.is_empty() {
                    continue;
                }

                update_object_status_chunk(
                    &tx,
                    &owner_uid,
                    &fragment,
                    &existing_keys,
                    next_status,
                    &now,
                )?;
                matched_total = matched_total.saturating_add(existing_keys.len());
                affected = affected.saturating_add(existing_keys.len());
            }

            tx.commit().map_err(sql_err)?;

            let conn = db.open_connection()?;
            let user = user_by_uid(&conn, &owner_uid)?;
            let not_found = selected_keys.len().saturating_sub(matched_total);
            Ok((affected, not_found, user))
        })
        .await
    }

    pub async fn trash_objects_bulk_from_ready(
        &self,
        owner_uid: String,
        fragment: String,
        object_keys: Vec<String>,
    ) -> Result<(usize, usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut selected_keys = object_keys
                .into_iter()
                .map(|value| canonical_archive_key(&value))
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            selected_keys.sort();
            selected_keys.dedup();

            if selected_keys.is_empty() {
                return Err(RegistryError::bad_request(
                    "object_keys must include at least one object key",
                ));
            }

            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let now = now_rfc3339();
            let mut affected = 0usize;
            let mut matched_total = 0usize;

            for chunk in selected_keys.chunks(SQLITE_BULK_KEY_CHUNK) {
                let existing_keys = select_existing_object_keys_with_status(
                    &tx, &owner_uid, &fragment, chunk, "ready",
                )?;
                if existing_keys.is_empty() {
                    continue;
                }

                update_object_status_chunk(
                    &tx,
                    &owner_uid,
                    &fragment,
                    &existing_keys,
                    "trashed",
                    &now,
                )?;
                matched_total = matched_total.saturating_add(existing_keys.len());
                affected = affected.saturating_add(existing_keys.len());
            }

            tx.commit().map_err(sql_err)?;

            let conn = db.open_connection()?;
            let user = user_by_uid(&conn, &owner_uid)?;
            let not_found = selected_keys.len().saturating_sub(matched_total);
            Ok((affected, not_found, user))
        })
        .await
    }

    pub async fn recover_objects_bulk_from_trash(
        &self,
        owner_uid: String,
        fragment: String,
        object_keys: Vec<String>,
    ) -> Result<(usize, usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut selected_keys = object_keys
                .into_iter()
                .map(|value| canonical_archive_key(&value))
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            selected_keys.sort();
            selected_keys.dedup();

            if selected_keys.is_empty() {
                return Err(RegistryError::bad_request(
                    "object_keys must include at least one object key",
                ));
            }

            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let now = now_rfc3339();
            let mut affected = 0usize;
            let mut matched_total = 0usize;

            for chunk in selected_keys.chunks(SQLITE_BULK_KEY_CHUNK) {
                let existing_keys = select_existing_object_keys_with_status(
                    &tx, &owner_uid, &fragment, chunk, "trashed",
                )?;
                if existing_keys.is_empty() {
                    continue;
                }

                update_object_status_chunk(
                    &tx,
                    &owner_uid,
                    &fragment,
                    &existing_keys,
                    "ready",
                    &now,
                )?;
                matched_total = matched_total.saturating_add(existing_keys.len());
                affected = affected.saturating_add(existing_keys.len());
            }

            tx.commit().map_err(sql_err)?;

            let conn = db.open_connection()?;
            let user = user_by_uid(&conn, &owner_uid)?;
            let not_found = selected_keys.len().saturating_sub(matched_total);
            Ok((affected, not_found, user))
        })
        .await
    }

    pub async fn recover_all_trashed(
        &self,
        owner_uid: String,
        fragment: String,
    ) -> Result<(usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let now = now_rfc3339();
            let affected = tx
                .execute(
                    r#"
                    UPDATE objects
                    SET status = 'ready', updated_at = ?3
                    WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                    "#,
                    params![owner_uid, fragment, now],
                )
                .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let user = user_by_uid(&conn, &owner_uid)?;
            Ok((affected, user))
        })
        .await
    }

    pub async fn trash_objects_by_prefix_from_ready(
        &self,
        owner_uid: String,
        fragment: String,
        prefix: String,
    ) -> Result<(usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let prefix = normalize_namespace_prefix(prefix);
            if prefix.is_empty() {
                return Err(RegistryError::bad_request("prefix is required"));
            }

            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let now = now_rfc3339();
            let affected = tx
                .execute(
                    r#"
                    UPDATE objects
                    SET status = 'trashed', updated_at = ?5
                    WHERE owner_uid = ?1
                      AND fragment = ?2
                      AND object_key LIKE ?3
                      AND status = ?4
                    "#,
                    params![owner_uid, fragment, format!("{}%", prefix), "ready", now],
                )
                .map_err(sql_err)?;
            if affected == 0 {
                return Err(RegistryError::not_found("no matching objects found"));
            }
            tx.commit().map_err(sql_err)?;

            let conn = db.open_connection()?;
            let user = user_by_uid(&conn, &owner_uid)?;
            Ok((affected, user))
        })
        .await
    }

    pub async fn recover_objects_by_prefix_from_trash(
        &self,
        owner_uid: String,
        fragment: String,
        prefix: String,
    ) -> Result<(usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let prefix = normalize_namespace_prefix(prefix);
            if prefix.is_empty() {
                return Err(RegistryError::bad_request("prefix is required"));
            }

            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let now = now_rfc3339();
            let affected = tx
                .execute(
                    r#"
                    UPDATE objects
                    SET status = 'ready', updated_at = ?5
                    WHERE owner_uid = ?1
                      AND fragment = ?2
                      AND object_key LIKE ?3
                      AND status = ?4
                    "#,
                    params![owner_uid, fragment, format!("{}%", prefix), "trashed", now],
                )
                .map_err(sql_err)?;
            if affected == 0 {
                return Err(RegistryError::not_found("no matching objects found"));
            }
            tx.commit().map_err(sql_err)?;

            let conn = db.open_connection()?;
            let user = user_by_uid(&conn, &owner_uid)?;
            Ok((affected, user))
        })
        .await
    }

    pub async fn list_object_keys_by_prefix_with_status(
        &self,
        owner_uid: String,
        fragment: String,
        prefix: String,
        status: String,
    ) -> Result<Vec<String>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let prefix = normalize_namespace_prefix(prefix);
            if prefix.is_empty() {
                return Err(RegistryError::bad_request("prefix is required"));
            }

            let conn = db.open_connection()?;
            select_object_keys_by_prefix_with_status(&conn, &owner_uid, &fragment, &prefix, &status)
        })
        .await
    }

    #[cfg(test)]
    pub async fn list_objects_by_prefix_any_status(
        &self,
        owner_uid: String,
        fragment: String,
        prefix: String,
    ) -> Result<Vec<ObjectRecord>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let prefix = normalize_namespace_prefix(prefix);
            if prefix.is_empty() {
                return Err(RegistryError::bad_request("prefix is required"));
            }

            let conn = db.open_connection()?;
            select_objects_by_prefix(&conn, &owner_uid, &fragment, &prefix)
        })
        .await
    }

    #[cfg(test)]
    pub async fn delete_objects_by_prefix(
        &self,
        owner_uid: String,
        fragment: String,
        prefix: String,
    ) -> Result<(usize, Vec<String>, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let prefix = normalize_namespace_prefix(prefix);
            if prefix.is_empty() {
                return Err(RegistryError::bad_request("prefix is required"));
            }

            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;
            let objects = select_objects_by_prefix(&tx, &owner_uid, &fragment, &prefix)?;
            let pending_keys = select_upload_keys_by_prefix(&tx, &owner_uid, &fragment, &prefix)?;
            if objects.is_empty() && pending_keys.is_empty() {
                return Err(RegistryError::not_found("no matching objects found"));
            }

            let storage_ids = objects
                .iter()
                .map(|object| object.storage_object_id.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let affected = objects.len().saturating_add(pending_keys.len());
            let now = now_rfc3339();

            tx.execute(
                r#"
                DELETE FROM object_versions
                WHERE owner_uid = ?1
                  AND fragment = ?2
                  AND object_key IN (
                    SELECT object_key
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key LIKE ?3
                  )
                "#,
                params![owner_uid, fragment, format!("{}%", prefix)],
            )
            .map_err(sql_err)?;

            tx.execute(
                r#"
                DELETE FROM object_shares
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key LIKE ?3
                "#,
                params![owner_uid, fragment, format!("{}%", prefix)],
            )
            .map_err(sql_err)?;
            tx.execute(
                r#"
                DELETE FROM object_overlays
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key LIKE ?3
                "#,
                params![owner_uid, fragment, format!("{}%", prefix)],
            )
            .map_err(sql_err)?;
            tx.execute(
                r#"
                DELETE FROM object_comments
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key LIKE ?3
                "#,
                params![owner_uid, fragment, format!("{}%", prefix)],
            )
            .map_err(sql_err)?;

            tx.execute(
                r#"
                DELETE FROM objects
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key LIKE ?3
                "#,
                params![owner_uid, fragment, format!("{}%", prefix)],
            )
            .map_err(sql_err)?;

            tx.execute(
                r#"
                DELETE FROM object_uploads
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key LIKE ?3
                "#,
                params![owner_uid, fragment, format!("{}%", prefix)],
            )
            .map_err(sql_err)?;

            refresh_user_quota_used(&tx, &owner_uid, &now)?;

            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((affected, storage_ids, user))
        })
        .await
    }

    pub async fn rename_prefix(
        &self,
        owner_uid: String,
        fragment: String,
        source_prefix: String,
        destination_prefix: String,
    ) -> Result<(usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let source_prefix = normalize_namespace_prefix(source_prefix);
            let destination_prefix = normalize_namespace_prefix(destination_prefix);
            if source_prefix.is_empty() || destination_prefix.is_empty() {
                return Err(RegistryError::bad_request(
                    "source_prefix and destination_prefix are required",
                ));
            }
            if source_prefix == destination_prefix {
                let conn = db.open_connection()?;
                let user = user_by_uid(&conn, &owner_uid)?;
                return Ok((0, user));
            }
            if destination_prefix.starts_with(&source_prefix) {
                return Err(RegistryError::bad_request(
                    "destination_prefix cannot be nested inside source_prefix",
                ));
            }

            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let source_objects =
                select_objects_by_prefix(&tx, &owner_uid, &fragment, &source_prefix)?;
            let source_upload_keys =
                select_upload_keys_by_prefix(&tx, &owner_uid, &fragment, &source_prefix)?;
            if source_objects.is_empty() && source_upload_keys.is_empty() {
                return Err(RegistryError::not_found("no matching objects found"));
            }

            let source_keys = source_objects
                .iter()
                .map(|object| object.object_key.clone())
                .chain(source_upload_keys.iter().cloned())
                .collect::<std::collections::BTreeSet<_>>();
            let mappings = source_keys
                .iter()
                .map(|source_key| {
                    let suffix = source_key
                        .strip_prefix(&source_prefix)
                        .ok_or_else(|| {
                            RegistryError::internal(
                                "prefix rename source key did not match source prefix",
                            )
                        })?
                        .to_string();
                    Ok((
                        source_key.clone(),
                        format!("{}{}", destination_prefix, suffix),
                    ))
                })
                .collect::<Result<Vec<_>, RegistryError>>()?;

            let destination_keys = mappings
                .iter()
                .map(|(_, destination)| destination.clone())
                .collect::<Vec<_>>();
            let destination_objects =
                select_objects_by_keys(&tx, &owner_uid, &fragment, &destination_keys)?;
            let destination_upload_keys =
                select_upload_keys(&tx, &owner_uid, &fragment, &destination_keys)?;

            let mut destination_trashed = Vec::new();
            for object in destination_objects {
                if source_keys.contains(&object.object_key) {
                    continue;
                }
                if object.status == "trashed" {
                    destination_trashed.push(object);
                    continue;
                }
                return Err(RegistryError::conflict(format!(
                    "destination object already exists in registry: {}",
                    object.object_key
                )));
            }
            for upload_key in destination_upload_keys {
                if source_keys.contains(&upload_key) {
                    continue;
                }
                return Err(RegistryError::conflict(format!(
                    "destination object already exists in registry: {}",
                    upload_key
                )));
            }

            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;
            for trashed in &destination_trashed {
                delete_object_namespace_rows(&tx, &owner_uid, &fragment, &trashed.object_key)?;
            }

            let now = now_rfc3339();
            let temp_namespace = format!(".__registry_rename_tmp__/{}/", Uuid::new_v4());
            for (source_key, _) in &mappings {
                let temp_key = format!(
                    "{}{}",
                    temp_namespace,
                    source_key.strip_prefix(&source_prefix).ok_or_else(|| {
                        RegistryError::internal(
                            "prefix rename source key did not match source prefix",
                        )
                    })?
                );
                rewrite_namespace_key(&tx, &owner_uid, &fragment, source_key, &temp_key, &now)?;
            }
            for (source_key, destination_key) in &mappings {
                let temp_key = format!(
                    "{}{}",
                    temp_namespace,
                    source_key.strip_prefix(&source_prefix).ok_or_else(|| {
                        RegistryError::internal(
                            "prefix rename source key did not match source prefix",
                        )
                    })?
                );
                rewrite_namespace_key(
                    &tx,
                    &owner_uid,
                    &fragment,
                    &temp_key,
                    destination_key,
                    &now,
                )?;
            }

            if !destination_trashed.is_empty() {
                refresh_user_quota_used(&tx, &owner_uid, &now)?;
            }

            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((mappings.len(), user))
        })
        .await
    }

    pub async fn rename_object(
        &self,
        owner_uid: String,
        fragment: String,
        source_key: String,
        destination_key: String,
    ) -> Result<(ObjectRecord, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            if source_key == destination_key {
                let object = resolve_any_object(&tx, &owner_uid, &fragment, &source_key)?
                    .ok_or_else(|| RegistryError::not_found("object not found"))?;
                let user = user_by_uid(&tx, &owner_uid)?;
                return Ok((object, user));
            }

            let destination_pending = scalar_i64(
                &tx,
                r#"
                SELECT COUNT(*)
                FROM object_uploads
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                "#,
                params![owner_uid, fragment, destination_key],
            )?;
            if destination_pending > 0 {
                return Err(RegistryError::conflict(
                    "destination object already exists in registry",
                ));
            }

            let destination_current = tx
                .query_row(
                    r#"
                    SELECT owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, destination_key],
                    map_object_row,
                )
                .optional()
                .map_err(sql_err)?;
            if destination_current
                .as_ref()
                .is_some_and(|object| object.status != "trashed")
            {
                return Err(RegistryError::conflict(
                    "destination object already exists in registry",
                ));
            }

            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;
            if destination_current.as_ref().is_some() {
                tx.execute(
                    "DELETE FROM object_versions WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, destination_key],
                )
                .map_err(sql_err)?;
                tx.execute(
                    "DELETE FROM objects WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, destination_key],
                )
                .map_err(sql_err)?;
                tx.execute(
                    "DELETE FROM object_shares WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, destination_key],
                )
                .map_err(sql_err)?;
            }

            let now = now_rfc3339();
            let updated_ready = tx
                .execute(
                    r#"
                    UPDATE objects
                    SET object_key = ?4, storage_object_id = ?4, updated_at = ?5
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, source_key, destination_key, now],
                )
                .map_err(sql_err)?;
            tx.execute(
                r#"
                UPDATE object_versions
                SET object_key = ?4, storage_object_id = ?4
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                "#,
                params![owner_uid, fragment, source_key, destination_key],
            )
            .map_err(sql_err)?;
            let updated_pending = tx
                .execute(
                    r#"
                    UPDATE object_uploads
                    SET object_key = ?4, storage_object_id = ?4, updated_at = ?5
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, source_key, destination_key, now],
            )
            .map_err(sql_err)?;
            tx.execute(
                r#"
                UPDATE object_shares
                SET object_key = ?4, storage_object_id = ?4
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                "#,
                params![owner_uid, fragment, source_key, destination_key],
            )
            .map_err(sql_err)?;
            tx.execute(
                r#"
                UPDATE object_overlays
                SET object_key = ?4, updated_at = ?5
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                "#,
                params![owner_uid, fragment, source_key, destination_key, now],
            )
            .map_err(sql_err)?;
            tx.execute(
                r#"
                UPDATE object_comments
                SET object_key = ?4, updated_at = ?5
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                "#,
                params![owner_uid, fragment, source_key, destination_key, now],
            )
            .map_err(sql_err)?;
            if updated_ready == 0 && updated_pending == 0 {
                return Err(RegistryError::not_found("object not found"));
            }
            if destination_current.is_some() {
                refresh_user_quota_used(&tx, &owner_uid, &now)?;
            }
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let object = resolve_any_object(&conn, &owner_uid, &fragment, &destination_key)?
                .ok_or_else(|| RegistryError::internal("renamed object missing"))?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((object, user))
        })
        .await
    }

    pub async fn purge_object(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
    ) -> Result<(bool, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;
            let now = now_rfc3339();

            let stored = resolve_stored_object(&tx, &owner_uid, &fragment, &object_key)?;
            let pending_removed = tx
                .execute(
                    "DELETE FROM object_uploads WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, object_key],
                )
                .map_err(sql_err)?;

            let Some(stored) = stored else {
                if pending_removed == 0 {
                    return Err(RegistryError::not_found("object not found"));
                }
                tx.commit().map_err(sql_err)?;
                let conn = db.open_connection()?;
                let mut user = user_by_uid(&conn, &owner_uid)?;
                user.quota_limit_bytes = quota_limit;
                return Ok((true, user));
            };

            tx.execute(
                "DELETE FROM object_shares WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_overlays WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_comments WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;

            let transitioned = stored.status != "purged";
            if transitioned {
                tx.execute(
                    r#"
                    UPDATE objects
                    SET status = 'purged', updated_at = ?4
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, object_key, now],
                )
                .map_err(sql_err)?;
                enqueue_object_purge(&tx, &stored, &now)?;
            }

            refresh_user_quota_used(&tx, &owner_uid, &now)?;

            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((true, user))
        })
        .await
    }

    pub async fn mark_object_purged_without_queue(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
    ) -> Result<(bool, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;
            let now = now_rfc3339();

            let stored = resolve_stored_object(&tx, &owner_uid, &fragment, &object_key)?;
            let pending_removed = tx
                .execute(
                    "DELETE FROM object_uploads WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, object_key],
                )
                .map_err(sql_err)?;

            let Some(stored) = stored else {
                if pending_removed == 0 {
                    return Err(RegistryError::not_found("object not found"));
                }
                tx.commit().map_err(sql_err)?;
                let conn = db.open_connection()?;
                let mut user = user_by_uid(&conn, &owner_uid)?;
                user.quota_limit_bytes = quota_limit;
                return Ok((true, user));
            };

            tx.execute(
                "DELETE FROM object_shares WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_overlays WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_comments WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;

            if stored.status != "purged" {
                tx.execute(
                    r#"
                    UPDATE objects
                    SET status = 'purged', updated_at = ?4
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, object_key, now],
                )
                .map_err(sql_err)?;
            }

            refresh_user_quota_used(&tx, &owner_uid, &now)?;

            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((true, user))
        })
        .await
    }

    pub async fn purge_objects_by_prefix(
        &self,
        owner_uid: String,
        fragment: String,
        prefix: String,
    ) -> Result<(usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let prefix = normalize_namespace_prefix(prefix);
            if prefix.is_empty() {
                return Err(RegistryError::bad_request("prefix is required"));
            }

            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;
            let now = now_rfc3339();

            let objects = select_objects_by_prefix(&tx, &owner_uid, &fragment, &prefix)?;
            let pending_keys = select_upload_keys_by_prefix(&tx, &owner_uid, &fragment, &prefix)?;
            if objects.is_empty() && pending_keys.is_empty() {
                return Err(RegistryError::not_found("no matching objects found"));
            }

            tx.execute(
                r#"
                DELETE FROM object_uploads
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key LIKE ?3
                "#,
                params![owner_uid, fragment, format!("{}%", prefix)],
            )
            .map_err(sql_err)?;

            tx.execute(
                r#"
                DELETE FROM object_shares
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key LIKE ?3
                "#,
                params![owner_uid, fragment, format!("{}%", prefix)],
            )
            .map_err(sql_err)?;

            let mut transitioned = 0usize;
            for object in &objects {
                if object.status == "purged" {
                    continue;
                }
                tx.execute(
                    r#"
                    UPDATE objects
                    SET status = 'purged', updated_at = ?4
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, object.object_key, now],
                )
                .map_err(sql_err)?;
                enqueue_object_purge(&tx, object, &now)?;
                transitioned = transitioned.saturating_add(1);
            }
            refresh_user_quota_used(&tx, &owner_uid, &now)?;

            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((transitioned.saturating_add(pending_keys.len()), user))
        })
        .await
    }

    pub async fn mark_objects_purged_without_queue(
        &self,
        owner_uid: String,
        fragment: String,
        object_keys: Vec<String>,
    ) -> Result<(usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut selected_keys = object_keys
                .into_iter()
                .map(|value| canonical_archive_key(&value))
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            selected_keys.sort();
            selected_keys.dedup();

            if selected_keys.is_empty() {
                return Err(RegistryError::bad_request(
                    "object_keys must include at least one object key",
                ));
            }

            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;
            let now = now_rfc3339();
            let mut affected = 0usize;

            for chunk in selected_keys.chunks(SQLITE_BULK_KEY_CHUNK) {
                let existing_keys = select_existing_object_keys(&tx, &owner_uid, &fragment, chunk)?;
                if existing_keys.is_empty() {
                    continue;
                }

                delete_rows_for_object_keys_chunk(
                    &tx,
                    "object_uploads",
                    &owner_uid,
                    &fragment,
                    &existing_keys,
                )?;
                delete_rows_for_object_keys_chunk(
                    &tx,
                    "object_shares",
                    &owner_uid,
                    &fragment,
                    &existing_keys,
                )?;
                delete_rows_for_object_keys_chunk(
                    &tx,
                    "object_overlays",
                    &owner_uid,
                    &fragment,
                    &existing_keys,
                )?;
                delete_rows_for_object_keys_chunk(
                    &tx,
                    "object_comments",
                    &owner_uid,
                    &fragment,
                    &existing_keys,
                )?;
                update_object_status_chunk(
                    &tx,
                    &owner_uid,
                    &fragment,
                    &existing_keys,
                    "purged",
                    &now,
                )?;
                affected = affected.saturating_add(existing_keys.len());
            }

            refresh_user_quota_used(&tx, &owner_uid, &now)?;

            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((affected, user))
        })
        .await
    }

    pub async fn purge_trashed_objects(
        &self,
        owner_uid: String,
        fragment: String,
    ) -> Result<(usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;
            let now = now_rfc3339();

            let trashed_objects =
                list_objects_with_status_from_conn(&tx, &owner_uid, &fragment, "trashed")?;
            if trashed_objects.is_empty() {
                return Ok((0usize, user_by_uid(&tx, &owner_uid)?));
            }

            tx.execute(
                r#"
                DELETE FROM object_shares
                WHERE owner_uid = ?1
                  AND fragment = ?2
                  AND object_key IN (
                    SELECT object_key
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                  )
                "#,
                params![owner_uid, fragment],
            )
            .map_err(sql_err)?;
            tx.execute(
                r#"
                DELETE FROM object_overlays
                WHERE owner_uid = ?1
                  AND fragment = ?2
                  AND object_key IN (
                    SELECT object_key
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                  )
                "#,
                params![owner_uid, fragment],
            )
            .map_err(sql_err)?;
            tx.execute(
                r#"
                DELETE FROM object_comments
                WHERE owner_uid = ?1
                  AND fragment = ?2
                  AND object_key IN (
                    SELECT object_key
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                  )
                "#,
                params![owner_uid, fragment],
            )
            .map_err(sql_err)?;

            tx.execute(
                r#"
                UPDATE objects
                SET status = 'purged', updated_at = ?3
                WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                "#,
                params![owner_uid, fragment, now],
            )
            .map_err(sql_err)?;

            for object in &trashed_objects {
                enqueue_object_purge(&tx, object, &now)?;
            }

            refresh_user_quota_used(&tx, &owner_uid, &now)?;

            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((trashed_objects.len(), user))
        })
        .await
    }

    pub async fn purge_trashed_objects_without_queue(
        &self,
        owner_uid: String,
        fragment: String,
    ) -> Result<(usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;
            let now = now_rfc3339();

            let trashed_objects =
                list_objects_with_status_from_conn(&tx, &owner_uid, &fragment, "trashed")?;
            if trashed_objects.is_empty() {
                return Ok((0usize, user_by_uid(&tx, &owner_uid)?));
            }

            tx.execute(
                r#"
                DELETE FROM object_shares
                WHERE owner_uid = ?1
                  AND fragment = ?2
                  AND object_key IN (
                    SELECT object_key
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                  )
                "#,
                params![owner_uid, fragment],
            )
            .map_err(sql_err)?;
            tx.execute(
                r#"
                DELETE FROM object_overlays
                WHERE owner_uid = ?1
                  AND fragment = ?2
                  AND object_key IN (
                    SELECT object_key
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                  )
                "#,
                params![owner_uid, fragment],
            )
            .map_err(sql_err)?;
            tx.execute(
                r#"
                DELETE FROM object_comments
                WHERE owner_uid = ?1
                  AND fragment = ?2
                  AND object_key IN (
                    SELECT object_key
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                  )
                "#,
                params![owner_uid, fragment],
            )
            .map_err(sql_err)?;
            tx.execute(
                r#"
                UPDATE objects
                SET status = 'purged', updated_at = ?3
                WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                "#,
                params![owner_uid, fragment, now],
            )
            .map_err(sql_err)?;

            refresh_user_quota_used(&tx, &owner_uid, &now)?;

            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((trashed_objects.len(), user))
        })
        .await
    }

    #[cfg(test)]
    pub async fn delete_object(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
    ) -> Result<UserRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;
            let Some(_) = resolve_any_object(&tx, &owner_uid, &fragment, &object_key)? else {
                return Err(RegistryError::not_found("object not found"));
            };
            tx.execute(
                "DELETE FROM objects WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_versions WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_uploads WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM object_shares WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;
            refresh_user_quota_used(&tx, &owner_uid, &now_rfc3339())?;
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok(user)
        })
        .await
    }

    pub async fn cancel_pending_object(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
        version_id: Option<String>,
        session_id: Option<String>,
    ) -> Result<(bool, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let removed = if version_id.is_some() || session_id.is_some() {
                tx.execute(
                    r#"
                    DELETE FROM object_uploads
                    WHERE owner_uid = ?1
                      AND fragment = ?2
                      AND object_key = ?3
                      AND (?4 IS NULL OR version_id = ?4)
                      AND (?5 IS NULL OR session_id = ?5)
                    "#,
                    params![owner_uid, fragment, object_key, version_id, session_id],
                )
                .map_err(sql_err)?
                    > 0
            } else {
                tx.execute(
                    "DELETE FROM object_uploads WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                    params![owner_uid, fragment, object_key],
                )
                .map_err(sql_err)?
                    > 0
            };
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let user = user_by_uid(&conn, &owner_uid)?;
            Ok((removed, user))
        })
        .await
    }

    pub async fn list_pending_upload_reservations(
        &self,
        owner_uid: Option<String>,
    ) -> Result<Vec<PendingUploadReservation>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let mut sql = String::from(
                r#"
                SELECT owner_uid, fragment, object_key, storage_object_id, expected_size_bytes, content_type, version_id, session_id, created_at, updated_at
                FROM object_uploads
                "#,
            );
            let mut args = Vec::<Value>::new();
            if let Some(owner_uid) = owner_uid {
                sql.push_str(" WHERE owner_uid = ?");
                args.push(Value::Text(owner_uid));
            }
            sql.push_str(" ORDER BY owner_uid ASC, fragment ASC, object_key ASC");

            let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
            let rows = stmt
                .query_map(params_from_iter(args), |row| {
                    Ok(PendingUploadReservation {
                        owner_uid: row.get(0)?,
                        fragment: row.get(1)?,
                        object_key: row.get(2)?,
                        version_id: row.get(6)?,
                        session_id: row.get(7)?,
                        created_at: row.get(8)?,
                        updated_at: row.get(9)?,
                    })
                })
                .map_err(sql_err)?;

            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(sql_err)?);
            }
            Ok(out)
        })
        .await
    }

    pub async fn remove_pending_upload_reservations(
        &self,
        reservations: Vec<PendingUploadReservation>,
    ) -> Result<Vec<(String, String)>, RegistryError> {
        if reservations.is_empty() {
            return Ok(Vec::new());
        }

        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let mut removed_scopes = Vec::new();

            for reservation in reservations {
                let removed = tx
                    .execute(
                        r#"
                        DELETE FROM object_uploads
                        WHERE owner_uid = ?1
                          AND fragment = ?2
                          AND object_key = ?3
                          AND (
                                (?4 IS NULL AND version_id IS NULL)
                             OR (?4 IS NOT NULL AND version_id = ?4)
                          )
                          AND (
                                (?5 IS NULL AND session_id IS NULL)
                             OR (?5 IS NOT NULL AND session_id = ?5)
                          )
                        "#,
                        params![
                            reservation.owner_uid,
                            reservation.fragment,
                            reservation.object_key,
                            reservation.version_id,
                            reservation.session_id,
                        ],
                    )
                    .map_err(sql_err)?;
                if removed > 0 {
                    removed_scopes.push((reservation.owner_uid, reservation.fragment));
                }
            }

            tx.commit().map_err(sql_err)?;
            removed_scopes.sort();
            removed_scopes.dedup();
            Ok(removed_scopes)
        })
        .await
    }

    #[cfg(test)]
    pub async fn empty_trash(
        &self,
        owner_uid: String,
        fragment: String,
    ) -> Result<(usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let (quota_limit, _) = quota_for_user(&tx, &owner_uid)?;

            let removed_count = scalar_i64(
                &tx,
                r#"
                SELECT COUNT(*)
                FROM objects
                WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                "#,
                params![owner_uid, fragment],
            )? as usize;

            tx.execute(
                r#"
                DELETE FROM object_versions
                WHERE owner_uid = ?1
                  AND fragment = ?2
                  AND object_key IN (
                    SELECT object_key
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                  )
                "#,
                params![owner_uid, fragment],
            )
            .map_err(sql_err)?;

            tx.execute(
                r#"
                DELETE FROM object_uploads
                WHERE owner_uid = ?1
                  AND fragment = ?2
                  AND object_key IN (
                    SELECT object_key
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                  )
                "#,
                params![owner_uid, fragment],
            )
            .map_err(sql_err)?;

            tx.execute(
                r#"
                DELETE FROM object_shares
                WHERE owner_uid = ?1
                  AND fragment = ?2
                  AND object_key IN (
                    SELECT object_key
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                  )
                "#,
                params![owner_uid, fragment],
            )
            .map_err(sql_err)?;

            tx.execute(
                r#"
                DELETE FROM objects
                WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                "#,
                params![owner_uid, fragment],
            )
            .map_err(sql_err)?;

            refresh_user_quota_used(&tx, &owner_uid, &now_rfc3339())?;

            tx.commit().map_err(sql_err)?;

            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((removed_count, user))
        })
        .await
    }

    pub async fn peek_due_object_purge(
        &self,
    ) -> Result<Option<ObjectPurgeQueueEntry>, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            conn.query_row(
                r#"
                SELECT owner_uid, fragment, object_key, attempts
                FROM object_purge_queue
                WHERE available_at <= ?1
                ORDER BY created_at ASC
                LIMIT 1
                "#,
                params![now_rfc3339()],
                map_object_purge_queue_row,
            )
            .optional()
            .map_err(sql_err)
        })
        .await
    }

    pub async fn complete_object_purge(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
    ) -> Result<(), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            conn.execute(
                "DELETE FROM object_purge_queue WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    pub async fn fail_object_purge(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
        last_error: String,
        retry_after_secs: i64,
    ) -> Result<(), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let now = now_rfc3339();
            let next_attempt = rfc3339_after_seconds(retry_after_secs.max(1));
            conn.execute(
                r#"
                UPDATE object_purge_queue
                SET attempts = attempts + 1,
                    last_error = ?4,
                    available_at = ?5,
                    updated_at = ?6
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                "#,
                params![
                    owner_uid,
                    fragment,
                    object_key,
                    last_error,
                    next_attempt,
                    now
                ],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    pub async fn cancel_object_purge(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
    ) -> Result<(), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            conn.execute(
                "DELETE FROM object_purge_queue WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
                params![owner_uid, fragment, object_key],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    fn open_connection(&self) -> Result<Connection, RegistryError> {
        if let Some(parent) = self.sqlite_path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                RegistryError::internal(format!(
                    "failed to create registry directory {}: {}",
                    parent.display(),
                    err
                ))
            })?;
        }
        let conn = Connection::open(&self.sqlite_path).map_err(sql_err)?;
        conn.busy_timeout(Duration::from_secs(15))
            .map_err(sql_err)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(sql_err)?;
        Ok(conn)
    }
}

fn spawn_blocking<T, F>(f: F) -> impl std::future::Future<Output = Result<T, RegistryError>>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, RegistryError> + Send + 'static,
{
    async move {
        task::spawn_blocking(f)
            .await
            .map_err(|err| RegistryError::internal(format!("sqlite task failed: {}", err)))?
    }
}

fn user_by_uid(conn: &Connection, uid: &str) -> Result<UserRecord, RegistryError> {
    let mut user = user_by_uid_raw(conn, uid)?;
    apply_effective_quota_usage(conn, &mut user)?;
    Ok(user)
}

fn user_by_uid_raw(conn: &Connection, uid: &str) -> Result<UserRecord, RegistryError> {
    conn.query_row(
        r#"
        SELECT uid, email, quota_limit_bytes, quota_used_bytes, created_at, updated_at, is_admin, totp_enabled
        FROM users
        WHERE uid = ?1
        "#,
        params![uid],
        |row| {
            Ok(UserRecord {
                uid: row.get(0)?,
                email: row.get(1)?,
                quota_limit_bytes: row.get(2)?,
                quota_used_bytes: row.get(3)?,
                created_at: row.get(4)?,
                updated_at: row.get(5)?,
                is_admin: row.get::<_, i64>(6)? != 0,
                two_factor_enabled: row.get::<_, i64>(7)? != 0,
            })
        },
    )
    .map_err(|err| match err {
        rusqlite::Error::QueryReturnedNoRows => RegistryError::not_found("user not found"),
        other => sql_err(other),
    })
}

fn apply_effective_quota_usage(
    conn: &Connection,
    user: &mut UserRecord,
) -> Result<(), RegistryError> {
    user.quota_used_bytes = effective_quota_used_bytes_for_user(conn, &user.uid)?;
    Ok(())
}

fn apply_effective_quota_usage_to_auth(
    conn: &Connection,
    auth: &mut UserAuthRecord,
) -> Result<(), RegistryError> {
    apply_effective_quota_usage(conn, &mut auth.user)
}

fn user_auth_by_uid(conn: &Connection, uid: &str) -> Result<Option<UserAuthRecord>, RegistryError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT uid, email, password_hash, quota_limit_bytes, quota_used_bytes, created_at, updated_at, is_admin, totp_secret, totp_pending_secret, totp_enabled
            FROM users
            WHERE uid = ?1
            "#,
        )
        .map_err(sql_err)?;
    stmt.query_row(params![uid], map_user_auth_row)
        .optional()
        .map_err(sql_err)
}

fn map_user_auth_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<UserAuthRecord> {
    Ok(UserAuthRecord {
        user: UserRecord {
            uid: row.get(0)?,
            email: row.get(1)?,
            quota_limit_bytes: row.get(3)?,
            quota_used_bytes: row.get(4)?,
            created_at: row.get(5)?,
            updated_at: row.get(6)?,
            is_admin: row.get::<_, i64>(7)? != 0,
            two_factor_enabled: row.get::<_, i64>(10)? != 0,
        },
        password_hash: row.get(2)?,
        totp_secret: row.get(8)?,
        totp_pending_secret: row.get(9)?,
    })
}

fn resolve_ready_object(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectRecord>, RegistryError> {
    conn.query_row(
        r#"
        SELECT owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
        FROM objects
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3 AND status = 'ready'
        "#,
        params![owner_uid, fragment, object_key],
        map_object_row,
    )
    .optional()
    .map_err(sql_err)
}

fn resolve_live_object(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectRecord>, RegistryError> {
    conn.query_row(
        r#"
        SELECT owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
        FROM objects
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3 AND status = 'ready'
        "#,
        params![owner_uid, fragment, object_key],
        map_object_row,
    )
    .optional()
    .map_err(sql_err)
}

fn resolve_stored_object(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectRecord>, RegistryError> {
    conn.query_row(
        r#"
        SELECT owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
        FROM objects
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
        "#,
        params![owner_uid, fragment, object_key],
        map_object_row,
    )
    .optional()
    .map_err(sql_err)
}

fn resolve_any_object(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    object_key: &str,
) -> Result<Option<ObjectRecord>, RegistryError> {
    let ready = conn
        .query_row(
            r#"
            SELECT owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
            FROM objects
            WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
            "#,
            params![owner_uid, fragment, object_key],
            map_object_row,
        )
        .optional()
        .map_err(sql_err)?;
    if ready.is_some() {
        return Ok(ready);
    }

    conn.query_row(
        r#"
        SELECT owner_uid, fragment, object_key, storage_object_id, expected_size_bytes, content_type, created_at, updated_at
        FROM object_uploads
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
        "#,
        params![owner_uid, fragment, object_key],
        map_pending_row,
    )
    .optional()
    .map_err(sql_err)
}

fn resolve_fragment_binding(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
) -> Result<Option<FragmentBindingRecord>, RegistryError> {
    conn.query_row(
        r#"
        SELECT owner_uid, fragment, deployment, engine_url_override, created_at, updated_at
        FROM fragment_bindings
        WHERE owner_uid = ?1 AND fragment = ?2
        "#,
        params![owner_uid, fragment],
        map_fragment_binding_row,
    )
    .optional()
    .map_err(sql_err)
}

fn normalize_listing_prefix(prefix: String, delimiter: Option<&str>) -> String {
    let trimmed = prefix.trim().to_string();
    if trimmed.is_empty() {
        return String::new();
    }
    if delimiter == Some("/") && !trimmed.ends_with('/') {
        return format!("{}/", trimmed);
    }
    trimmed
}

fn normalize_namespace_prefix(prefix: String) -> String {
    normalize_listing_prefix(prefix, Some("/"))
}

fn select_objects_by_prefix(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    prefix: &str,
) -> Result<Vec<ObjectRecord>, RegistryError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
            FROM objects
            WHERE owner_uid = ?1 AND fragment = ?2 AND object_key LIKE ?3
            ORDER BY object_key ASC
            "#,
        )
        .map_err(sql_err)?;
    let rows = stmt
        .query_map(
            params![owner_uid, fragment, format!("{}%", prefix)],
            map_object_row,
        )
        .map_err(sql_err)?;
    let mut objects = Vec::new();
    for row in rows {
        objects.push(row.map_err(sql_err)?);
    }
    Ok(objects)
}

fn select_object_keys_by_prefix_with_status(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    prefix: &str,
    status: &str,
) -> Result<Vec<String>, RegistryError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT object_key
            FROM objects
            WHERE owner_uid = ?1
              AND fragment = ?2
              AND object_key LIKE ?3
              AND status = ?4
            ORDER BY object_key ASC
            "#,
        )
        .map_err(sql_err)?;
    let rows = stmt
        .query_map(
            params![owner_uid, fragment, format!("{}%", prefix), status],
            |row| row.get::<_, String>(0),
        )
        .map_err(sql_err)?;
    let mut object_keys = Vec::new();
    for row in rows {
        object_keys.push(row.map_err(sql_err)?);
    }
    Ok(object_keys)
}

fn list_objects_with_status_from_conn(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    status: &str,
) -> Result<Vec<ObjectRecord>, RegistryError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
            FROM objects
            WHERE owner_uid = ?1 AND fragment = ?2 AND status = ?3
            ORDER BY object_key ASC
            "#,
        )
        .map_err(sql_err)?;
    let rows = stmt
        .query_map(params![owner_uid, fragment, status], map_object_row)
        .map_err(sql_err)?;
    let mut objects = Vec::new();
    for row in rows {
        objects.push(row.map_err(sql_err)?);
    }
    Ok(objects)
}

fn select_upload_keys_by_prefix(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    prefix: &str,
) -> Result<Vec<String>, RegistryError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT object_key
            FROM object_uploads
            WHERE owner_uid = ?1 AND fragment = ?2 AND object_key LIKE ?3
            ORDER BY object_key ASC
            "#,
        )
        .map_err(sql_err)?;
    let rows = stmt
        .query_map(
            params![owner_uid, fragment, format!("{}%", prefix)],
            |row| row.get::<_, String>(0),
        )
        .map_err(sql_err)?;
    let mut keys = Vec::new();
    for row in rows {
        keys.push(row.map_err(sql_err)?);
    }
    Ok(keys)
}

fn select_objects_by_keys(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    object_keys: &[String],
) -> Result<Vec<ObjectRecord>, RegistryError> {
    if object_keys.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = std::iter::repeat_n("?", object_keys.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        SELECT owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
        FROM objects
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key IN ({})
        "#,
        placeholders
    );
    let mut args = vec![
        Value::Text(owner_uid.to_string()),
        Value::Text(fragment.to_string()),
    ];
    args.extend(object_keys.iter().cloned().map(Value::Text));

    let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
    let rows = stmt
        .query_map(params_from_iter(args), map_object_row)
        .map_err(sql_err)?;
    let mut objects = Vec::new();
    for row in rows {
        objects.push(row.map_err(sql_err)?);
    }
    Ok(objects)
}

fn select_upload_keys(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    object_keys: &[String],
) -> Result<Vec<String>, RegistryError> {
    if object_keys.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = std::iter::repeat_n("?", object_keys.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        SELECT object_key
        FROM object_uploads
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key IN ({})
        "#,
        placeholders
    );
    let mut args = vec![
        Value::Text(owner_uid.to_string()),
        Value::Text(fragment.to_string()),
    ];
    args.extend(object_keys.iter().cloned().map(Value::Text));

    let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
    let rows = stmt
        .query_map(params_from_iter(args), |row| row.get::<_, String>(0))
        .map_err(sql_err)?;
    let mut keys = Vec::new();
    for row in rows {
        keys.push(row.map_err(sql_err)?);
    }
    Ok(keys)
}

fn delete_object_namespace_rows(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    object_key: &str,
) -> Result<(), RegistryError> {
    conn.execute(
        "DELETE FROM object_versions WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
        params![owner_uid, fragment, object_key],
    )
    .map_err(sql_err)?;
    conn.execute(
        "DELETE FROM objects WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
        params![owner_uid, fragment, object_key],
    )
    .map_err(sql_err)?;
    conn.execute(
        "DELETE FROM object_shares WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
        params![owner_uid, fragment, object_key],
    )
    .map_err(sql_err)?;
    conn.execute(
        "DELETE FROM object_overlays WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
        params![owner_uid, fragment, object_key],
    )
    .map_err(sql_err)?;
    conn.execute(
        "DELETE FROM object_comments WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3",
        params![owner_uid, fragment, object_key],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn rewrite_namespace_key(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    source_key: &str,
    destination_key: &str,
    now: &str,
) -> Result<(), RegistryError> {
    conn.execute(
        r#"
        UPDATE objects
        SET object_key = ?4, storage_object_id = ?4, updated_at = ?5
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
        "#,
        params![owner_uid, fragment, source_key, destination_key, now],
    )
    .map_err(sql_err)?;
    conn.execute(
        r#"
        UPDATE object_versions
        SET object_key = ?4, storage_object_id = ?4
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
        "#,
        params![owner_uid, fragment, source_key, destination_key],
    )
    .map_err(sql_err)?;
    conn.execute(
        r#"
        UPDATE object_uploads
        SET object_key = ?4, storage_object_id = ?4, updated_at = ?5
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
        "#,
        params![owner_uid, fragment, source_key, destination_key, now],
    )
    .map_err(sql_err)?;
    conn.execute(
        r#"
        UPDATE object_shares
        SET object_key = ?4, storage_object_id = ?4
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
        "#,
        params![owner_uid, fragment, source_key, destination_key],
    )
    .map_err(sql_err)?;
    conn.execute(
        r#"
        UPDATE object_overlays
        SET object_key = ?4, updated_at = ?5
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
        "#,
        params![owner_uid, fragment, source_key, destination_key, now],
    )
    .map_err(sql_err)?;
    conn.execute(
        r#"
        UPDATE object_comments
        SET object_key = ?4, updated_at = ?5
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
        "#,
        params![owner_uid, fragment, source_key, destination_key, now],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn select_archive_objects_from_conn(
    conn: &Connection,
    owner_uid: String,
    fragment: String,
    keys: Vec<String>,
    prefix: Option<String>,
    search: Option<String>,
    include_trashed: bool,
) -> Result<Vec<ObjectRecord>, RegistryError> {
    let status = if include_trashed { "trashed" } else { "ready" };

    if !keys.is_empty() {
        let mut canonical_keys = keys
            .into_iter()
            .map(|value| canonical_archive_key(&value))
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        canonical_keys.sort();
        canonical_keys.dedup();

        if canonical_keys.is_empty() {
            return Err(RegistryError::bad_request(
                "archive request did not include any valid object keys",
            ));
        }

        let placeholders = std::iter::repeat_n("?", canonical_keys.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"
            SELECT owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
            FROM objects
            WHERE owner_uid = ? AND fragment = ? AND status = ? AND object_key IN ({})
            ORDER BY object_key ASC
            "#,
            placeholders
        );
        let mut args = vec![
            Value::Text(owner_uid),
            Value::Text(fragment),
            Value::Text(status.to_string()),
        ];
        args.extend(canonical_keys.into_iter().map(Value::Text));

        let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params_from_iter(args), map_object_row)
            .map_err(sql_err)?;
        let mut objects = Vec::new();
        for row in rows {
            objects.push(row.map_err(sql_err)?);
        }
        if objects.is_empty() {
            return Err(RegistryError::not_found(
                "no matching objects found for archive export",
            ));
        }
        return Ok(objects);
    }

    let prefix_filter = prefix
        .map(|value| normalize_listing_prefix(value, Some("/")))
        .filter(|value| !value.is_empty());
    let prefix_pattern = prefix_filter
        .as_ref()
        .map(|value| Value::Text(format!("{}%", value)));
    let search_pattern = search
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| Value::Text(format!("%{}%", value)));

    let mut sql = String::from(
        r#"
        SELECT owner_uid, fragment, object_key, storage_object_id, version_id, status, object_size_bytes, content_type, created_at, updated_at
        FROM objects
        WHERE owner_uid = ? AND fragment = ? AND status = ?
        "#,
    );
    let mut args = vec![
        Value::Text(owner_uid),
        Value::Text(fragment),
        Value::Text(status.to_string()),
    ];
    if let Some(pattern) = prefix_pattern {
        sql.push_str(" AND object_key LIKE ?");
        args.push(pattern);
    }
    if let Some(pattern) = search_pattern {
        sql.push_str(" AND object_key LIKE ?");
        args.push(pattern);
    }
    sql.push_str(" ORDER BY object_key ASC");

    let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
    let rows = stmt
        .query_map(params_from_iter(args), map_object_row)
        .map_err(sql_err)?;
    let mut objects = Vec::new();
    for row in rows {
        objects.push(row.map_err(sql_err)?);
    }
    if objects.is_empty() {
        return Err(RegistryError::not_found(
            "no matching objects found for archive export",
        ));
    }
    Ok(objects)
}

fn rfc3339_from_unix(value: i64) -> String {
    Utc.timestamp_opt(value, 0)
        .single()
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(now_rfc3339)
}

fn canonical_archive_key(value: &str) -> String {
    value.trim().trim_start_matches('/').to_string()
}

fn select_existing_object_keys(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    object_keys: &[String],
) -> Result<Vec<String>, RegistryError> {
    if object_keys.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = std::iter::repeat_n("?", object_keys.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        SELECT object_key
        FROM objects
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key IN ({})
        "#,
        placeholders
    );
    let mut args = vec![
        Value::Text(owner_uid.to_string()),
        Value::Text(fragment.to_string()),
    ];
    args.extend(object_keys.iter().cloned().map(Value::Text));

    let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
    let rows = stmt
        .query_map(params_from_iter(args), |row| row.get::<_, String>(0))
        .map_err(sql_err)?;
    let mut found = Vec::new();
    for row in rows {
        found.push(row.map_err(sql_err)?);
    }
    Ok(found)
}

fn select_existing_object_keys_with_status(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    object_keys: &[String],
    status: &str,
) -> Result<Vec<String>, RegistryError> {
    if object_keys.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders = std::iter::repeat_n("?", object_keys.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        SELECT object_key
        FROM objects
        WHERE owner_uid = ?1 AND fragment = ?2 AND status = ?3 AND object_key IN ({})
        "#,
        placeholders
    );
    let mut args = vec![
        Value::Text(owner_uid.to_string()),
        Value::Text(fragment.to_string()),
        Value::Text(status.to_string()),
    ];
    args.extend(object_keys.iter().cloned().map(Value::Text));

    let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
    let rows = stmt
        .query_map(params_from_iter(args), |row| row.get::<_, String>(0))
        .map_err(sql_err)?;
    let mut found = Vec::new();
    for row in rows {
        found.push(row.map_err(sql_err)?);
    }
    Ok(found)
}

fn update_object_status_chunk(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    object_keys: &[String],
    next_status: &str,
    now: &str,
) -> Result<(), RegistryError> {
    if object_keys.is_empty() {
        return Ok(());
    }

    let placeholders = std::iter::repeat_n("?", object_keys.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        UPDATE objects
        SET status = ?1, updated_at = ?2
        WHERE owner_uid = ?3 AND fragment = ?4 AND object_key IN ({})
        "#,
        placeholders
    );
    let mut args = vec![
        Value::Text(next_status.to_string()),
        Value::Text(now.to_string()),
        Value::Text(owner_uid.to_string()),
        Value::Text(fragment.to_string()),
    ];
    args.extend(object_keys.iter().cloned().map(Value::Text));
    conn.execute(&sql, params_from_iter(args))
        .map_err(sql_err)?;
    Ok(())
}

fn delete_rows_for_object_keys_chunk(
    conn: &Connection,
    table: &str,
    owner_uid: &str,
    fragment: &str,
    object_keys: &[String],
) -> Result<(), RegistryError> {
    if object_keys.is_empty() {
        return Ok(());
    }

    let placeholders = std::iter::repeat_n("?", object_keys.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        DELETE FROM {table}
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key IN ({placeholders})
        "#
    );
    let mut args = vec![
        Value::Text(owner_uid.to_string()),
        Value::Text(fragment.to_string()),
    ];
    args.extend(object_keys.iter().cloned().map(Value::Text));
    conn.execute(&sql, params_from_iter(args))
        .map_err(sql_err)?;
    Ok(())
}

fn quota_for_user(conn: &Connection, owner_uid: &str) -> Result<(i64, i64), RegistryError> {
    let quota_limit = conn
        .query_row(
            "SELECT quota_limit_bytes, quota_used_bytes FROM users WHERE uid = ?1",
            params![owner_uid],
            |row| row.get(0),
        )
        .map_err(|err| match err {
            rusqlite::Error::QueryReturnedNoRows => RegistryError::not_found("user not found"),
            other => sql_err(other),
        })?;
    Ok((
        quota_limit,
        committed_quota_used_bytes_for_user(conn, owner_uid)?,
    ))
}

fn committed_quota_used_bytes_for_user(
    conn: &Connection,
    owner_uid: &str,
) -> Result<i64, RegistryError> {
    conn.query_row(
        r#"
        SELECT COALESCE(SUM(object_size_bytes), 0)
        FROM objects
        WHERE owner_uid = ?1
          AND status != 'purged'
        "#,
        params![owner_uid],
        |row| row.get::<_, i64>(0),
    )
    .map(|value| value.max(0))
    .map_err(sql_err)
}

fn refresh_user_quota_used(
    conn: &Connection,
    owner_uid: &str,
    updated_at: &str,
) -> Result<i64, RegistryError> {
    let next_used = committed_quota_used_bytes_for_user(conn, owner_uid)?;
    conn.execute(
        r#"
        UPDATE users
        SET quota_used_bytes = ?2, updated_at = ?3
        WHERE uid = ?1
        "#,
        params![owner_uid, next_used, updated_at],
    )
    .map_err(sql_err)?;
    Ok(next_used)
}

fn pending_quota_reserved_bytes_for_user(
    conn: &Connection,
    owner_uid: &str,
    exclude: Option<(&str, &str)>,
) -> Result<i64, RegistryError> {
    let (exclude_fragment, exclude_object_key) = exclude
        .map(|(fragment, object_key)| (Some(fragment), Some(object_key)))
        .unwrap_or((None, None));
    conn.query_row(
        r#"
        SELECT COALESCE(SUM(COALESCE(ou.reserved_bytes, 0)), 0)
        FROM object_uploads ou
        WHERE ou.owner_uid = ?1
          AND (?2 IS NULL OR ou.fragment <> ?2 OR ou.object_key <> ?3)
        "#,
        params![owner_uid, exclude_fragment, exclude_object_key],
        |row| row.get(0),
    )
    .map_err(sql_err)
}

fn effective_quota_used_bytes_for_user(
    conn: &Connection,
    owner_uid: &str,
) -> Result<i64, RegistryError> {
    let (_, quota_used) = quota_for_user(conn, owner_uid)?;
    let pending_reserved = pending_quota_reserved_bytes_for_user(conn, owner_uid, None)?;
    Ok(quota_used.saturating_add(pending_reserved).max(0))
}

fn revoke_refresh_chain(
    conn: &Connection,
    start_hash: &str,
    now_text: &str,
) -> Result<(), RegistryError> {
    let mut current = Some(start_hash.to_string());
    while let Some(token_hash) = current.take() {
        current = conn
            .query_row(
                "SELECT replaced_by_hash FROM registry_refresh_sessions WHERE token_hash = ?1",
                params![token_hash],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(sql_err)?
            .flatten();

        conn.execute(
            "UPDATE registry_refresh_sessions SET revoked_at = COALESCE(revoked_at, ?2) WHERE token_hash = ?1",
            params![token_hash, now_text],
        )
        .map_err(sql_err)?;
    }
    Ok(())
}

fn scalar_i64<P>(conn: &Connection, sql: &str, params: P) -> Result<i64, RegistryError>
where
    P: rusqlite::Params,
{
    conn.query_row(sql, params, |row| row.get(0))
        .map_err(sql_err)
}

fn map_object_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObjectRecord> {
    Ok(ObjectRecord {
        owner_uid: row.get(0)?,
        fragment: row.get(1)?,
        object_key: row.get(2)?,
        storage_object_id: row.get(3)?,
        version_id: row.get(4)?,
        status: row.get(5)?,
        kind: Some("file".to_string()),
        object_size_bytes: row.get(6)?,
        content_type: row.get(7)?,
        trash_entry_id: None,
        deleted_at_unix: None,
        created_at: row.get(8)?,
        updated_at: row.get(9)?,
    })
}

fn map_pending_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObjectRecord> {
    Ok(ObjectRecord {
        owner_uid: row.get(0)?,
        fragment: row.get(1)?,
        object_key: row.get(2)?,
        storage_object_id: row.get(3)?,
        version_id: String::new(),
        status: "pending".to_string(),
        kind: Some("file".to_string()),
        object_size_bytes: row.get::<_, Option<i64>>(4)?.unwrap_or(0),
        content_type: row.get(5)?,
        trash_entry_id: None,
        deleted_at_unix: None,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

fn map_fragment_binding_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FragmentBindingRecord> {
    Ok(FragmentBindingRecord {
        owner_uid: row.get(0)?,
        fragment: row.get(1)?,
        deployment: StorageDeployment::parse_db(row.get::<_, String>(2)?).map_err(|message| {
            rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    message,
                )),
            )
        })?,
        engine_url_override: row.get(3)?,
        created_at: row.get(4)?,
        updated_at: row.get(5)?,
    })
}

fn map_share_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ShareRecord> {
    Ok(ShareRecord {
        share_id: row.get(0)?,
        owner_uid: row.get(1)?,
        fragment: row.get(2)?,
        object_key: row.get(3)?,
        storage_object_id: row.get(4)?,
        disposition: ShareDisposition::parse_db(row.get::<_, String>(5)?).map_err(|message| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    message,
                )),
            )
        })?,
        expires_at: rfc3339_from_unix(row.get(6)?),
        created_at: row.get(7)?,
        revoked_at: row.get(8)?,
    })
}

fn map_audit_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AuditEventRecord> {
    Ok(AuditEventRecord {
        event_id: row.get(0)?,
        owner_uid: row.get(1)?,
        actor_uid: row.get(2)?,
        event_type: row.get(3)?,
        fragment: row.get(4)?,
        object_key: row.get(5)?,
        details_json: row.get(6)?,
        created_at: row.get(7)?,
    })
}

fn map_object_comment_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObjectCommentRecord> {
    Ok(ObjectCommentRecord {
        comment_id: row.get(0)?,
        owner_uid: row.get(1)?,
        actor_uid: row.get(2)?,
        fragment: row.get(3)?,
        object_key: row.get(4)?,
        body: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
        deleted_at: row.get(8)?,
    })
}

fn map_object_purge_queue_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObjectPurgeQueueEntry> {
    Ok(ObjectPurgeQueueEntry {
        owner_uid: row.get(0)?,
        fragment: row.get(1)?,
        object_key: row.get(2)?,
        attempts: row.get(3)?,
    })
}

fn sanitize_overlay_values(values: Vec<String>) -> Vec<String> {
    let mut values = values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values.truncate(100);
    values
}

fn parse_overlay_json(value: String) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(&value)
        .map(sanitize_overlay_values)
        .unwrap_or_default()
}

fn resolve_overlay(
    conn: &Connection,
    owner_uid: &str,
    fragment: &str,
    object_key: &str,
) -> Result<ObjectOverlayRecord, RegistryError> {
    let row = conn
        .query_row(
            r#"
            SELECT favorite, tags_json, labels_json, updated_at
            FROM object_overlays
            WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
            "#,
            params![owner_uid, fragment, object_key],
            |row| {
                Ok((
                    row.get::<_, i64>(0)? != 0,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(sql_err)?;
    let comment_count = scalar_i64(
        conn,
        r#"
        SELECT COUNT(*)
        FROM object_comments
        WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3 AND deleted_at IS NULL
        "#,
        params![owner_uid, fragment, object_key],
    )? as usize;

    match row {
        Some((favorite, tags_json, labels_json, updated_at)) => Ok(ObjectOverlayRecord {
            owner_uid: owner_uid.to_string(),
            fragment: fragment.to_string(),
            object_key: object_key.to_string(),
            favorite,
            tags: parse_overlay_json(tags_json),
            labels: parse_overlay_json(labels_json),
            comment_count,
            updated_at,
        }),
        None => Ok(ObjectOverlayRecord {
            owner_uid: owner_uid.to_string(),
            fragment: fragment.to_string(),
            object_key: object_key.to_string(),
            favorite: false,
            tags: Vec::new(),
            labels: Vec::new(),
            comment_count,
            updated_at: now_rfc3339(),
        }),
    }
}

fn enqueue_object_purge(
    conn: &Connection,
    object: &ObjectRecord,
    now: &str,
) -> Result<(), RegistryError> {
    conn.execute(
        r#"
        INSERT INTO object_purge_queue (
            owner_uid, fragment, object_key, storage_object_id, attempts, last_error, created_at, available_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, 0, NULL, ?5, ?6, ?7)
        ON CONFLICT(owner_uid, fragment, object_key)
        DO UPDATE SET
            storage_object_id = excluded.storage_object_id,
            last_error = NULL,
            available_at = excluded.available_at,
            updated_at = excluded.updated_at
        "#,
        params![
            object.owner_uid,
            object.fragment,
            object.object_key,
            object.storage_object_id,
            now,
            now,
            now
        ],
    )
    .map_err(sql_err)?;
    Ok(())
}

fn map_insert_user_err(err: rusqlite::Error) -> RegistryError {
    match err {
        rusqlite::Error::SqliteFailure(_, Some(message))
            if message.contains("users.email") || message.contains("UNIQUE constraint failed") =>
        {
            RegistryError::conflict("email already exists")
        }
        other => sql_err(other),
    }
}

fn sql_err(err: rusqlite::Error) -> RegistryError {
    match &err {
        rusqlite::Error::SqliteFailure(code, Some(message))
            if code.code == rusqlite::ErrorCode::DatabaseBusy
                || code.code == rusqlite::ErrorCode::DatabaseLocked
                || message.to_ascii_lowercase().contains("database is locked") =>
        {
            RegistryError::service_unavailable("registry is busy, retry in a moment")
        }
        rusqlite::Error::SqliteFailure(code, None)
            if code.code == rusqlite::ErrorCode::DatabaseBusy
                || code.code == rusqlite::ErrorCode::DatabaseLocked =>
        {
            RegistryError::service_unavailable("registry is busy, retry in a moment")
        }
        _ => RegistryError::internal(format!("registry database error: {}", err)),
    }
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

fn rfc3339_after_seconds(seconds: i64) -> String {
    (Utc::now() + chrono::Duration::seconds(seconds.max(0))).to_rfc3339()
}

fn now_unix() -> i64 {
    Utc::now().timestamp()
}

fn generate_refresh_token() -> String {
    format!("{}.{}", Uuid::new_v4(), Uuid::new_v4())
}

fn generate_share_token() -> String {
    format!("share_{}.{}", Uuid::new_v4(), Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    use super::RegistryDb;
    use crate::auth::{hash_password, verify_password};
    use crate::models::{ShareDisposition, StorageDeployment};
    use crate::storage_broker::EngineObjectMetadata;
    use axum::http::StatusCode;

    #[tokio::test]
    async fn registry_round_trip_tracks_quota_and_listing() {
        let path = std::env::temp_dir().join(format!("registry-test-{}.db", uuid::Uuid::new_v4()));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "owner@example.com".to_string(),
                password_hash.clone(),
                Some(10_000),
                false,
            )
            .await
            .expect("create user");

        let auth = db
            .find_user_auth_by_email("owner@example.com".to_string())
            .await
            .expect("find user")
            .expect("user exists");
        verify_password("secret", &auth.password_hash).expect("password ok");

        let binding = db
            .upsert_fragment_binding(
                user.uid.clone(),
                "fragment-a".to_string(),
                StorageDeployment::SingleNode,
                Some("https://n1.example.test".to_string()),
            )
            .await
            .expect("save binding");
        assert_eq!(binding.deployment, StorageDeployment::SingleNode);

        let bindings = db
            .list_fragment_bindings(user.uid.clone())
            .await
            .expect("list bindings");
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].fragment, "fragment-a");

        db.register_pending_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "patients/12-1222/study/file-1.dcm".to_string(),
            Some(1024),
            Some("application/dicom".to_string()),
            None,
            None,
        )
        .await
        .expect("register pending");

        let pending = db
            .list_objects(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("patients/12-1222".to_string()),
                Some("/".to_string()),
                None,
                100,
                0,
                true,
                false,
            )
            .await
            .expect("list pending");
        assert!(pending.objects.is_empty());
        assert_eq!(
            pending.common_prefixes,
            vec!["patients/12-1222/study/".to_string()]
        );

        let (object, updated_user) = db
            .finalize_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "patients/12-1222/study/file-1.dcm".to_string(),
                "01HZZZZZZZZZZZZZZZZZZZZZZZ".to_string(),
                2048,
                Some("application/dicom".to_string()),
            )
            .await
            .expect("finalize");
        assert_eq!(object.status, "ready");
        assert_eq!(updated_user.quota_used_bytes, 2048);

        let resolved = db
            .resolve_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "patients/12-1222/study/file-1.dcm".to_string(),
            )
            .await
            .expect("resolve")
            .expect("ready object exists");
        assert_eq!(resolved.version_id, "01HZZZZZZZZZZZZZZZZZZZZZZZ");

        let ready_only = db
            .list_objects(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("patients/".to_string()),
                Some("/".to_string()),
                None,
                100,
                0,
                false,
                false,
            )
            .await
            .expect("list ready");
        assert_eq!(
            ready_only.common_prefixes,
            vec!["patients/12-1222/".to_string()]
        );

        let nested = db
            .list_objects(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("patients/12-1222/".to_string()),
                Some("/".to_string()),
                None,
                100,
                0,
                false,
                false,
            )
            .await
            .expect("list nested");
        assert_eq!(
            nested.common_prefixes,
            vec!["patients/12-1222/study/".to_string()]
        );

        let suggestions = db
            .suggest_object_keys(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("patients/".to_string()),
                "file-1".to_string(),
                5,
                false,
            )
            .await
            .expect("suggest object keys");
        assert_eq!(
            suggestions,
            vec!["patients/12-1222/study/file-1.dcm".to_string()]
        );

        let (trashed, _) = db
            .set_object_status(
                user.uid.clone(),
                "fragment-a".to_string(),
                "patients/12-1222/study/file-1.dcm".to_string(),
                "trashed",
            )
            .await
            .expect("trash object");
        assert_eq!(trashed.status, "trashed");

        let trash_listing = db
            .list_objects(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("patients/".to_string()),
                Some("/".to_string()),
                None,
                100,
                0,
                false,
                true,
            )
            .await
            .expect("list trash");
        assert_eq!(
            trash_listing.common_prefixes,
            vec!["patients/12-1222/".to_string()]
        );

        let (renamed, _) = db
            .rename_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "patients/12-1222/study/file-1.dcm".to_string(),
                "patients/12-1222/study/file-1-renamed.dcm".to_string(),
            )
            .await
            .expect("rename object");
        assert_eq!(
            renamed.object_key,
            "patients/12-1222/study/file-1-renamed.dcm"
        );

        let removed_user = db
            .delete_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "patients/12-1222/study/file-1-renamed.dcm".to_string(),
            )
            .await
            .expect("delete object");
        assert_eq!(removed_user.quota_used_bytes, 0);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn pending_quota_reservations_block_overcommit_and_release_on_cancel() {
        let path = std::env::temp_dir().join(format!(
            "registry-pending-quota-test-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "pending-quota@example.com".to_string(),
                password_hash,
                Some(10_000),
                false,
            )
            .await
            .expect("create user");

        db.register_pending_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "uploads/one.bin".to_string(),
            Some(4_000),
            Some("application/octet-stream".to_string()),
            None,
            None,
        )
        .await
        .expect("register first pending");
        let user_after_first = db
            .get_user(user.uid.clone())
            .await
            .expect("user after first");
        assert_eq!(user_after_first.quota_used_bytes, 4_000);

        db.register_pending_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "uploads/two.bin".to_string(),
            Some(5_000),
            Some("application/octet-stream".to_string()),
            None,
            None,
        )
        .await
        .expect("register second pending");
        let user_after_second = db
            .get_user(user.uid.clone())
            .await
            .expect("user after second");
        assert_eq!(user_after_second.quota_used_bytes, 9_000);

        let err = db
            .register_pending_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "uploads/three.bin".to_string(),
                Some(2_000),
                Some("application/octet-stream".to_string()),
                None,
                None,
            )
            .await
            .expect_err("third pending should exceed quota");
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert!(err.message.contains("quota exceeded"));

        let (removed, user_after_cancel) = db
            .cancel_pending_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "uploads/two.bin".to_string(),
                None,
                None,
            )
            .await
            .expect("cancel second pending");
        assert!(removed);
        assert_eq!(user_after_cancel.quota_used_bytes, 4_000);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn rename_object_replaces_destination_when_it_exists_only_as_trashed() {
        let path = std::env::temp_dir().join(format!(
            "registry-rename-trashed-destination-test-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "rename-trashed@example.com".to_string(),
                password_hash,
                Some(50_000),
                false,
            )
            .await
            .expect("create user");

        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "folder/source.bin".to_string(),
            "01HRENAMETRASHEDSOURCE0000001".to_string(),
            1024,
            Some("application/octet-stream".to_string()),
        )
        .await
        .expect("finalize source");
        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "folder/destination.bin".to_string(),
            "01HRENAMETRASHEDDEST00000001".to_string(),
            2048,
            Some("application/octet-stream".to_string()),
        )
        .await
        .expect("finalize destination");

        let (trashed, _) = db
            .set_object_status(
                user.uid.clone(),
                "fragment-a".to_string(),
                "folder/destination.bin".to_string(),
                "trashed",
            )
            .await
            .expect("trash destination");
        assert_eq!(trashed.status, "trashed");

        let (renamed, updated_user) = db
            .rename_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "folder/source.bin".to_string(),
                "folder/destination.bin".to_string(),
            )
            .await
            .expect("registry rename should replace trashed destination");
        assert_eq!(renamed.object_key, "folder/destination.bin");
        assert_eq!(renamed.version_id, "01HRENAMETRASHEDSOURCE0000001");
        assert_eq!(renamed.storage_object_id, "folder/destination.bin");
        assert_eq!(updated_user.quota_used_bytes, 1024);

        let source = db
            .resolve_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "folder/source.bin".to_string(),
            )
            .await
            .expect("resolve source after rename");
        assert!(source.is_none(), "source key should be gone after rename");

        let destination = db
            .resolve_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "folder/destination.bin".to_string(),
            )
            .await
            .expect("resolve renamed destination")
            .expect("destination should exist after rename");
        assert!(
            destination.status == "ready"
                && destination.version_id == "01HRENAMETRASHEDSOURCE0000001",
            "unexpected destination after rename: {:?}",
            destination
        );
        assert_eq!(destination.storage_object_id, renamed.storage_object_id);

        let trashed_listing = db
            .list_objects(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("folder/".to_string()),
                None,
                None,
                100,
                0,
                false,
                true,
            )
            .await
            .expect("list trashed destination");
        assert!(
            trashed_listing.objects.is_empty(),
            "trashed destination should be replaced"
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn rename_prefix_moves_objects_pending_rows_and_shares_without_changing_storage_ids() {
        let path = std::env::temp_dir().join(format!(
            "registry-rename-prefix-test-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "rename-prefix@example.com".to_string(),
                password_hash,
                Some(50_000),
                false,
            )
            .await
            .expect("create user");

        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "photos/2024/a.jpg".to_string(),
            "01HRENPFX000000000000000000001".to_string(),
            1024,
            Some("image/jpeg".to_string()),
        )
        .await
        .expect("finalize source object");
        db.register_pending_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "photos/2024/pending.jpg".to_string(),
            Some(512),
            Some("image/jpeg".to_string()),
            None,
            None,
        )
        .await
        .expect("register pending object");
        db.upsert_fragment_binding(
            user.uid.clone(),
            "fragment-a".to_string(),
            StorageDeployment::SingleNode,
            Some("https://n1.example.test".to_string()),
        )
        .await
        .expect("bind fragment");
        let share = db
            .create_share(
                user.uid.clone(),
                "fragment-a".to_string(),
                "photos/2024/a.jpg".to_string(),
                ShareDisposition::Download,
                3600,
            )
            .await
            .expect("create share");

        let (affected, updated_user) = db
            .rename_prefix(
                user.uid.clone(),
                "fragment-a".to_string(),
                "photos/2024".to_string(),
                "photos/archived".to_string(),
            )
            .await
            .expect("rename prefix");
        assert_eq!(affected, 2);
        assert_eq!(updated_user.quota_used_bytes, 1536);

        let source_after = db
            .resolve_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "photos/2024/a.jpg".to_string(),
            )
            .await
            .expect("resolve source after");
        assert!(
            source_after.is_none(),
            "source key should be gone after prefix rename"
        );

        let destination = db
            .resolve_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "photos/archived/a.jpg".to_string(),
            )
            .await
            .expect("resolve destination")
            .expect("destination exists");
        assert_eq!(destination.storage_object_id, "photos/archived/a.jpg");

        let pending = db
            .list_pending_upload_reservations(Some(user.uid.clone()))
            .await
            .expect("list pending after rename");
        assert_eq!(pending.len(), 1);
        let pending = &pending[0];
        assert_eq!(pending.fragment, "fragment-a");
        assert_eq!(pending.object_key, "photos/archived/pending.jpg");

        let shares = db
            .list_shares(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                false,
                false,
            )
            .await
            .expect("list shares");
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].share_id, share.0.share_id);
        assert_eq!(shares[0].object_key, "photos/archived/a.jpg");
        assert_eq!(shares[0].storage_object_id, "photos/archived/a.jpg");

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn delete_prefix_removes_objects_pending_rows_and_shares_and_returns_storage_ids() {
        let path = std::env::temp_dir().join(format!(
            "registry-delete-prefix-test-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "delete-prefix@example.com".to_string(),
                password_hash,
                Some(50_000),
                false,
            )
            .await
            .expect("create user");

        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "docs/bulk/a.txt".to_string(),
            "01HDELPFX000000000000000000001".to_string(),
            1024,
            Some("text/plain".to_string()),
        )
        .await
        .expect("finalize a");
        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "docs/bulk/b.txt".to_string(),
            "01HDELPFX000000000000000000002".to_string(),
            2048,
            Some("text/plain".to_string()),
        )
        .await
        .expect("finalize b");
        db.register_pending_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "docs/bulk/pending.txt".to_string(),
            Some(512),
            Some("text/plain".to_string()),
            None,
            None,
        )
        .await
        .expect("register pending");
        db.upsert_fragment_binding(
            user.uid.clone(),
            "fragment-a".to_string(),
            StorageDeployment::SingleNode,
            Some("https://n1.example.test".to_string()),
        )
        .await
        .expect("bind fragment");
        let share = db
            .create_share(
                user.uid.clone(),
                "fragment-a".to_string(),
                "docs/bulk/a.txt".to_string(),
                ShareDisposition::Download,
                3600,
            )
            .await
            .expect("create share");

        let objects_before = db
            .list_objects_by_prefix_any_status(
                user.uid.clone(),
                "fragment-a".to_string(),
                "docs/bulk".to_string(),
            )
            .await
            .expect("list objects by prefix");
        assert_eq!(objects_before.len(), 2);
        let expected_storage_ids = objects_before
            .iter()
            .map(|object| object.storage_object_id.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let (affected, storage_ids, updated_user) = db
            .delete_objects_by_prefix(
                user.uid.clone(),
                "fragment-a".to_string(),
                "docs/bulk".to_string(),
            )
            .await
            .expect("delete prefix");
        assert_eq!(affected, 3);
        assert_eq!(storage_ids, expected_storage_ids);
        assert_eq!(updated_user.quota_used_bytes, 0);

        let ready_after = db
            .list_objects(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("docs/".to_string()),
                None,
                None,
                100,
                0,
                false,
                true,
            )
            .await
            .expect("list objects after delete prefix");
        assert!(ready_after.objects.is_empty());

        let pending_after = db
            .list_pending_upload_reservations(Some(user.uid.clone()))
            .await
            .expect("list pending after delete prefix");
        assert!(pending_after.is_empty());

        let shares = db
            .list_shares(user.uid.clone(), Some("fragment-a".to_string()), true, true)
            .await
            .expect("list shares after delete prefix");
        assert!(
            shares
                .iter()
                .all(|entry| entry.share_id != share.0.share_id),
            "share rows should be removed by prefix delete"
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn finalize_object_respects_other_pending_reservations() {
        let path = std::env::temp_dir().join(format!(
            "registry-finalize-pending-quota-test-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "finalize-quota@example.com".to_string(),
                password_hash,
                Some(10_000),
                false,
            )
            .await
            .expect("create user");

        db.register_pending_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "uploads/a.bin".to_string(),
            Some(4_000),
            Some("application/octet-stream".to_string()),
            None,
            None,
        )
        .await
        .expect("register pending a");
        db.register_pending_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "uploads/b.bin".to_string(),
            Some(5_000),
            Some("application/octet-stream".to_string()),
            None,
            None,
        )
        .await
        .expect("register pending b");

        let err = db
            .finalize_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "uploads/a.bin".to_string(),
                "01HFINALIZEPENDINGTEST00000001".to_string(),
                6_000,
                Some("application/octet-stream".to_string()),
            )
            .await
            .expect_err("finalize should respect other pending reservations");
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert!(err.message.contains("quota exceeded"));

        let (_object, user_after_finalize) = db
            .finalize_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "uploads/a.bin".to_string(),
                "01HFINALIZEPENDINGTEST00000002".to_string(),
                4_500,
                Some("application/octet-stream".to_string()),
            )
            .await
            .expect("finalize within effective quota");
        assert_eq!(user_after_finalize.quota_used_bytes, 9_500);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn sync_object_from_engine_clears_pending_reservation_on_success() {
        let path = std::env::temp_dir().join(format!(
            "registry-sync-engine-clears-pending-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "sync-engine@example.com".to_string(),
                password_hash,
                Some(10_000),
                false,
            )
            .await
            .expect("create user");

        let pending = db
            .register_pending_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                "videos/demo.mov".to_string(),
                Some(4_096),
                Some("video/quicktime".to_string()),
                Some("01SYNCENGINEPENDING000000000001".to_string()),
                Some("01SYNCENGINESESSION00000000001".to_string()),
            )
            .await
            .expect("register pending object");

        let (object, updated_user) = db
            .sync_object_from_engine(
                user.uid.clone(),
                "fragment-a".to_string(),
                "videos/demo.mov".to_string(),
                pending.storage_object_id.clone(),
                Some(EngineObjectMetadata {
                    version_id: "01SYNCENGINECOMMITTED000000001".to_string(),
                    object_size_bytes: 4_096,
                    content_type: Some("video/quicktime".to_string()),
                    created_at_unix: 1_775_000_000,
                }),
                Vec::new(),
            )
            .await
            .expect("sync object from engine");

        let object = object.expect("object should exist after successful sync");
        assert_eq!(object.storage_object_id, pending.storage_object_id);
        assert_eq!(object.status, "ready");
        assert_eq!(updated_user.quota_used_bytes, 4_096);

        let remaining_pending = db
            .list_pending_upload_reservations(Some(user.uid.clone()))
            .await
            .expect("list pending reservations");
        assert!(
            remaining_pending.is_empty(),
            "successful engine sync should remove the matching pending upload reservation"
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn listing_prefix_without_trailing_slash_returns_folder_contents() {
        let path =
            std::env::temp_dir().join(format!("registry-prefix-test-{}.db", uuid::Uuid::new_v4()));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "folder@example.com".to_string(),
                password_hash,
                Some(50_000),
                false,
            )
            .await
            .expect("create user");

        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "Screenshots/shot-1.png".to_string(),
            "01HAAAAAAATESTVERSION0000001".to_string(),
            2048,
            Some("image/png".to_string()),
        )
        .await
        .expect("finalize shot 1");
        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "Screenshots/shot-2.png".to_string(),
            "01HAAAAAAATESTVERSION0000002".to_string(),
            1024,
            Some("image/png".to_string()),
        )
        .await
        .expect("finalize shot 2");

        let listing = db
            .list_objects(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("Screenshots".to_string()),
                Some("/".to_string()),
                None,
                200,
                0,
                false,
                false,
            )
            .await
            .expect("list screenshots");

        let keys = listing
            .objects
            .iter()
            .map(|object| object.object_key.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                "Screenshots/shot-1.png".to_string(),
                "Screenshots/shot-2.png".to_string()
            ]
        );
        assert!(listing.common_prefixes.is_empty());

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn delimiter_listing_paginates_objects_and_common_prefixes_together() {
        let path = std::env::temp_dir().join(format!(
            "registry-delimiter-pagination-test-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "delimiter-pagination@example.com".to_string(),
                password_hash,
                Some(100_000),
                false,
            )
            .await
            .expect("create user");

        for (key, version_id) in [
            ("page/0.bin", "01HDELIMPAGE0000000000000001"),
            ("page/sub-a/x.bin", "01HDELIMPAGE0000000000000002"),
            ("page/sub-b/y.bin", "01HDELIMPAGE0000000000000003"),
            ("page/z.bin", "01HDELIMPAGE0000000000000004"),
        ] {
            db.finalize_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                key.to_string(),
                version_id.to_string(),
                1024,
                Some("application/octet-stream".to_string()),
            )
            .await
            .expect("finalize object");
        }

        let page1 = db
            .list_objects(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("page".to_string()),
                Some("/".to_string()),
                None,
                2,
                0,
                false,
                false,
            )
            .await
            .expect("list page1");
        assert_eq!(
            page1
                .objects
                .iter()
                .map(|object| object.object_key.as_str())
                .collect::<Vec<_>>(),
            vec!["page/0.bin"]
        );
        assert_eq!(page1.common_prefixes, vec!["page/sub-a/".to_string()]);
        assert!(page1.has_more);

        let page2 = db
            .list_objects(
                user.uid,
                Some("fragment-a".to_string()),
                Some("page".to_string()),
                Some("/".to_string()),
                None,
                2,
                2,
                false,
                false,
            )
            .await
            .expect("list page2");
        assert_eq!(
            page2
                .objects
                .iter()
                .map(|object| object.object_key.as_str())
                .collect::<Vec<_>>(),
            vec!["page/z.bin"]
        );
        assert_eq!(page2.common_prefixes, vec!["page/sub-b/".to_string()]);
        assert!(!page2.has_more);

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn bulk_recover_and_empty_trash_keep_registry_state_and_quota_consistent() {
        let path =
            std::env::temp_dir().join(format!("registry-bulk-trash-{}.db", uuid::Uuid::new_v4()));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "bulk@example.com".to_string(),
                password_hash,
                Some(20_000),
                false,
            )
            .await
            .expect("create user");

        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "trash/a.png".to_string(),
            "01HBULKTRASHVERSION00000001".to_string(),
            2048,
            Some("image/png".to_string()),
        )
        .await
        .expect("finalize a");
        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "trash/b.png".to_string(),
            "01HBULKTRASHVERSION00000002".to_string(),
            1024,
            Some("image/png".to_string()),
        )
        .await
        .expect("finalize b");

        db.set_object_status(
            user.uid.clone(),
            "fragment-a".to_string(),
            "trash/a.png".to_string(),
            "trashed",
        )
        .await
        .expect("trash a");
        db.set_object_status(
            user.uid.clone(),
            "fragment-a".to_string(),
            "trash/b.png".to_string(),
            "trashed",
        )
        .await
        .expect("trash b");

        let (recovered, recovered_user) = db
            .recover_all_trashed(user.uid.clone(), "fragment-a".to_string())
            .await
            .expect("recover all");
        assert_eq!(recovered, 2);
        assert_eq!(recovered_user.quota_used_bytes, 3072);

        let ready_after_recover = db
            .list_objects(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("trash".to_string()),
                Some("/".to_string()),
                None,
                100,
                0,
                false,
                false,
            )
            .await
            .expect("list ready after recover");
        assert_eq!(ready_after_recover.objects.len(), 2);

        db.set_object_status(
            user.uid.clone(),
            "fragment-a".to_string(),
            "trash/a.png".to_string(),
            "trashed",
        )
        .await
        .expect("re-trash a");
        db.set_object_status(
            user.uid.clone(),
            "fragment-a".to_string(),
            "trash/b.png".to_string(),
            "trashed",
        )
        .await
        .expect("re-trash b");

        let (removed, user_after_empty) = db
            .empty_trash(user.uid.clone(), "fragment-a".to_string())
            .await
            .expect("empty trash");
        assert_eq!(removed, 2);
        assert_eq!(user_after_empty.quota_used_bytes, 0);

        let trash_after_empty = db
            .list_objects(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                None,
                None,
                None,
                100,
                0,
                false,
                true,
            )
            .await
            .expect("list trash after empty");
        assert!(trash_after_empty.objects.is_empty());

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn bulk_selected_status_updates_dedupe_keys_and_report_not_found() {
        let path = std::env::temp_dir().join(format!(
            "registry-bulk-selected-status-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "bulk-selected@example.com".to_string(),
                password_hash,
                Some(20_000),
                false,
            )
            .await
            .expect("create user");

        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "trash/a.png".to_string(),
            "01HBULKSELECTSTATUS0000000001".to_string(),
            2048,
            Some("image/png".to_string()),
        )
        .await
        .expect("finalize a");
        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "trash/b.png".to_string(),
            "01HBULKSELECTSTATUS0000000002".to_string(),
            1024,
            Some("image/png".to_string()),
        )
        .await
        .expect("finalize b");

        let (affected, not_found, user_after_trash) = db
            .set_objects_status_bulk(
                user.uid.clone(),
                "fragment-a".to_string(),
                vec![
                    "trash/a.png".to_string(),
                    "/trash/a.png".to_string(),
                    "trash/b.png".to_string(),
                    "trash/missing.png".to_string(),
                ],
                "trashed",
            )
            .await
            .expect("bulk trash selected keys");
        assert_eq!(affected, 2);
        assert_eq!(not_found, 1);
        assert_eq!(user_after_trash.quota_used_bytes, 3072);

        let trash_listing = db
            .list_objects(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                None,
                None,
                None,
                100,
                0,
                false,
                true,
            )
            .await
            .expect("list trash after bulk selected trash");
        assert_eq!(trash_listing.objects.len(), 2);

        let (recovered, recover_not_found, user_after_recover) = db
            .set_objects_status_bulk(
                user.uid.clone(),
                "fragment-a".to_string(),
                vec!["trash/a.png".to_string(), "trash/missing.png".to_string()],
                "ready",
            )
            .await
            .expect("bulk recover selected keys");
        assert_eq!(recovered, 1);
        assert_eq!(recover_not_found, 1);
        assert_eq!(user_after_recover.quota_used_bytes, 3072);

        let ready_listing = db
            .list_objects(
                user.uid,
                Some("fragment-a".to_string()),
                None,
                None,
                None,
                100,
                0,
                false,
                false,
            )
            .await
            .expect("list ready after bulk selected recover");
        assert_eq!(ready_listing.objects.len(), 1);
        assert_eq!(ready_listing.objects[0].object_key, "trash/a.png");

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn archive_selection_returns_prefix_selected_keys_in_order() {
        let path = std::env::temp_dir().join(format!(
            "registry-archive-selection-test-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "archive@example.com".to_string(),
                password_hash,
                Some(50_000),
                false,
            )
            .await
            .expect("create user");

        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "patients/a/one.dcm".to_string(),
            "01HARCHMANIFESTVERSION0000000001".to_string(),
            1024,
            Some("application/dicom".to_string()),
        )
        .await
        .expect("finalize one");
        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "patients/a/two.dcm".to_string(),
            "01HARCHMANIFESTVERSION0000000002".to_string(),
            1024,
            Some("application/dicom".to_string()),
        )
        .await
        .expect("finalize two");
        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "patients/b/three.dcm".to_string(),
            "01HARCHMANIFESTVERSION0000000003".to_string(),
            1024,
            Some("application/dicom".to_string()),
        )
        .await
        .expect("finalize three");

        let selection = db
            .select_archive_objects(
                user.uid,
                "fragment-a".to_string(),
                Vec::new(),
                Some("patients/a".to_string()),
                None,
                false,
            )
            .await
            .expect("select archive objects");
        assert_eq!(selection.objects.len(), 2);
        assert_eq!(
            selection.objects[0].version_id,
            "01HARCHMANIFESTVERSION0000000001"
        );
        assert_eq!(selection.objects[0].object_key, "patients/a/one.dcm");
        assert_eq!(selection.objects[0].storage_object_id, "patients/a/one.dcm");
        assert_eq!(
            selection.objects[1].version_id,
            "01HARCHMANIFESTVERSION0000000002"
        );
        assert_eq!(selection.objects[1].object_key, "patients/a/two.dcm");
        assert_eq!(selection.objects[1].storage_object_id, "patients/a/two.dcm");

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn object_stats_cover_full_filtered_scope_not_just_one_page() {
        let path = std::env::temp_dir().join(format!(
            "registry-object-stats-test-{}.db",
            uuid::Uuid::new_v4()
        ));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user(
                "stats@example.com".to_string(),
                password_hash,
                Some(500_000),
                false,
            )
            .await
            .expect("create user");

        for index in 0..205usize {
            db.finalize_object(
                user.uid.clone(),
                "fragment-a".to_string(),
                format!("Screenshots/shot-{index:03}.png"),
                format!("01HSTATSVERSION{index:013}"),
                1024,
                Some("image/png".to_string()),
            )
            .await
            .expect("finalize png");
        }
        db.finalize_object(
            user.uid.clone(),
            "fragment-a".to_string(),
            "Screenshots/report.pdf".to_string(),
            "01HSTATSVERSIONPDF0000000001".to_string(),
            2048,
            Some("application/pdf".to_string()),
        )
        .await
        .expect("finalize pdf");

        let stats = db
            .object_stats(
                user.uid.clone(),
                Some("fragment-a".to_string()),
                Some("Screenshots".to_string()),
                None,
                false,
                false,
            )
            .await
            .expect("object stats");

        assert_eq!(stats.total_objects, 206);
        assert_eq!(stats.total_size_bytes, 205 * 1024 + 2048);
        assert_eq!(
            stats
                .extension_stats
                .iter()
                .find(|entry| entry.extension == "png")
                .map(|entry| (entry.object_count, entry.total_size_bytes)),
            Some((205, 205 * 1024))
        );
        assert_eq!(
            stats
                .extension_stats
                .iter()
                .find(|entry| entry.extension == "pdf")
                .map(|entry| (entry.object_count, entry.total_size_bytes)),
            Some((1, 2048))
        );

        let _ = std::fs::remove_file(path);
    }
}
