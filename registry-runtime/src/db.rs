use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Duration;

use chrono::Utc;
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use tokio::task;
use uuid::Uuid;

use crate::error::RegistryError;
use crate::models::{
    FragmentBindingRecord, HealthResponse, ObjectRecord, StorageDeployment, UserAuthRecord,
    UserRecord,
};

#[derive(Debug, Clone)]
pub struct RegistryListPage {
    pub objects: Vec<ObjectRecord>,
    pub common_prefixes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ArchiveManifestResult {
    pub manifest_path: PathBuf,
    pub selected_count: usize,
}

#[derive(Clone, Debug)]
pub struct RegistryDb {
    sqlite_path: PathBuf,
    default_quota_bytes: i64,
}

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
                    expected_size_bytes INTEGER,
                    content_type TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (owner_uid, fragment, object_key)
                );

                CREATE INDEX IF NOT EXISTS idx_objects_owner_prefix
                    ON objects (owner_uid, fragment, object_key);
                CREATE INDEX IF NOT EXISTS idx_object_uploads_owner_prefix
                    ON object_uploads (owner_uid, fragment, object_key);
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
                "#,
            )
            .map_err(sql_err)?;
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
                    uid, email, password_hash, quota_limit_bytes, quota_used_bytes, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6)
                "#,
                params![uid, email, password_hash, quota_limit, now, now],
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
                    SELECT uid, email, password_hash, quota_limit_bytes, quota_used_bytes, created_at, updated_at
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
                            quota_limit_bytes: row.get(3)?,
                            quota_used_bytes: row.get(4)?,
                            created_at: row.get(5)?,
                            updated_at: row.get(6)?,
                        },
                        password_hash: row.get(2)?,
                    })
                })
                .optional()
                .map_err(sql_err)?;
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

    pub async fn register_pending_object(
        &self,
        owner_uid: String,
        fragment: String,
        object_key: String,
        expected_size_bytes: Option<i64>,
        content_type: Option<String>,
    ) -> Result<ObjectRecord, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;

            let (quota_limit, quota_used) = quota_for_user(&tx, &owner_uid)?;
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

            if let Some(expected_size) = expected_size_bytes {
                let projected_delta = expected_size.saturating_sub(existing_ready_size);
                let projected_used = quota_used.saturating_add(projected_delta.max(0));
                if projected_used > quota_limit {
                    return Err(RegistryError::conflict("quota exceeded for pending object"));
                }
            }

            let now = now_rfc3339();
            tx.execute(
                r#"
                INSERT INTO object_uploads (
                    owner_uid, fragment, object_key, expected_size_bytes, content_type, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                ON CONFLICT(owner_uid, fragment, object_key)
                DO UPDATE SET
                    expected_size_bytes = excluded.expected_size_bytes,
                    content_type = excluded.content_type,
                    updated_at = excluded.updated_at
                "#,
                params![
                    owner_uid,
                    fragment,
                    object_key,
                    expected_size_bytes,
                    content_type,
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
                version_id: String::new(),
                status: "pending".to_string(),
                object_size_bytes: expected_size_bytes.unwrap_or(0),
                content_type,
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
            let existing = tx
                .query_row(
                    r#"
                    SELECT object_size_bytes, created_at
                    FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, object_key],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(sql_err)?;
            let previous_size = existing.as_ref().map(|v| v.0).unwrap_or(0);
            let created_at = existing
                .as_ref()
                .map(|v| v.1.clone())
                .unwrap_or_else(now_rfc3339);
            let delta = object_size_bytes.saturating_sub(previous_size);
            let next_used = quota_used.saturating_add(delta);
            if next_used > quota_limit {
                return Err(RegistryError::conflict("quota exceeded"));
            }

            let now = now_rfc3339();
            tx.execute(
                r#"
                INSERT INTO objects (
                    owner_uid, fragment, object_key, version_id, status, object_size_bytes, content_type, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, 'ready', ?5, ?6, ?7, ?8)
                ON CONFLICT(owner_uid, fragment, object_key)
                DO UPDATE SET
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
                    owner_uid, fragment, object_key, version_id, object_size_bytes, content_type, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    owner_uid,
                    fragment,
                    object_key,
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

            tx.execute(
                r#"
                UPDATE users
                SET quota_used_bytes = ?2, updated_at = ?3
                WHERE uid = ?1
                "#,
                params![owner_uid, next_used, now],
            )
            .map_err(sql_err)?;

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
                SELECT owner_uid, fragment, object_key, version_id, status, object_size_bytes, content_type, created_at, updated_at
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
                    SELECT owner_uid, fragment, object_key, expected_size_bytes, content_type, created_at, updated_at
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

            let mut common_prefixes = std::collections::BTreeSet::new();
            let objects = if delimiter.as_deref() == Some("/") {
                let mut direct_children = Vec::new();
                for object in out {
                    let suffix = if prefix_filter.is_empty() {
                        object.object_key.as_str()
                    } else if let Some(stripped) = object.object_key.strip_prefix(&prefix_filter) {
                        stripped
                    } else {
                        continue;
                    };
                    if suffix.is_empty() {
                        direct_children.push(object);
                        continue;
                    }
                    if let Some(idx) = suffix.find('/') {
                        common_prefixes.insert(format!("{}{}", prefix_filter, &suffix[..idx + 1]));
                        continue;
                    }
                    direct_children.push(object);
                }
                direct_children
            } else {
                out
            };

            Ok(RegistryListPage {
                objects: objects.into_iter().skip(offset).take(limit).collect::<Vec<_>>(),
                common_prefixes: common_prefixes.into_iter().collect(),
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

    pub async fn write_archive_manifest(
        &self,
        owner_uid: String,
        fragment: String,
        keys: Vec<String>,
        prefix: Option<String>,
        search: Option<String>,
        include_trashed: bool,
    ) -> Result<ArchiveManifestResult, RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let conn = db.open_connection()?;
            let manifest_path =
                std::env::temp_dir().join(format!("registry-archive-{}.lst", Uuid::new_v4()));
            let file = std::fs::File::create(&manifest_path).map_err(|err| {
                RegistryError::internal(format!("failed to create archive manifest: {}", err))
            })?;
            let mut writer = BufWriter::new(file);
            let mut selected_count = 0usize;
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
                    SELECT version_id, object_key
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
                    .query_map(params_from_iter(args), |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(sql_err)?;
                for row in rows {
                    let (version_id, key) = row.map_err(sql_err)?;
                    writeln!(writer, "{}\t{}", version_id, key).map_err(|err| {
                        RegistryError::internal(format!(
                            "failed to write archive manifest entry: {}",
                            err
                        ))
                    })?;
                    selected_count += 1;
                }
            } else {
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
                    SELECT version_id, object_key
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
                    .query_map(params_from_iter(args), |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(sql_err)?;
                for row in rows {
                    let (version_id, key) = row.map_err(sql_err)?;
                    writeln!(writer, "{}\t{}", version_id, key).map_err(|err| {
                        RegistryError::internal(format!(
                            "failed to write archive manifest entry: {}",
                            err
                        ))
                    })?;
                    selected_count += 1;
                }
            }

            writer.flush().map_err(|err| {
                RegistryError::internal(format!("failed to flush archive manifest: {}", err))
            })?;

            if selected_count == 0 {
                let _ = std::fs::remove_file(&manifest_path);
                return Err(RegistryError::not_found(
                    "no matching objects found for archive export",
                ));
            }

            Ok(ArchiveManifestResult {
                manifest_path,
                selected_count,
            })
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

            let exists = scalar_i64(
                &tx,
                r#"
                SELECT COUNT(*) FROM (
                    SELECT 1 FROM objects
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    UNION ALL
                    SELECT 1 FROM object_uploads
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                )
                "#,
                params![owner_uid, fragment, destination_key],
            )?;
            if exists > 0 {
                return Err(RegistryError::conflict(
                    "destination object already exists in registry",
                ));
            }

            let now = now_rfc3339();
            let updated_ready = tx
                .execute(
                    r#"
                    UPDATE objects
                    SET object_key = ?4, updated_at = ?5
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, source_key, destination_key, now],
                )
                .map_err(sql_err)?;
            tx.execute(
                r#"
                UPDATE object_versions
                SET object_key = ?4
                WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                "#,
                params![owner_uid, fragment, source_key, destination_key],
            )
            .map_err(sql_err)?;
            let updated_pending = tx
                .execute(
                    r#"
                    UPDATE object_uploads
                    SET object_key = ?4, updated_at = ?5
                    WHERE owner_uid = ?1 AND fragment = ?2 AND object_key = ?3
                    "#,
                    params![owner_uid, fragment, source_key, destination_key, now],
                )
                .map_err(sql_err)?;
            if updated_ready == 0 && updated_pending == 0 {
                return Err(RegistryError::not_found("object not found"));
            }
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let object = resolve_any_object(&conn, &owner_uid, &fragment, &destination_key)?
                .ok_or_else(|| RegistryError::internal("renamed object missing"))?;
            let user = user_by_uid(&conn, &owner_uid)?;
            Ok((object, user))
        })
        .await
    }

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
            let (quota_limit, quota_used) = quota_for_user(&tx, &owner_uid)?;
            let existing = resolve_any_object(&tx, &owner_uid, &fragment, &object_key)?;
            let Some(existing) = existing else {
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
            let size_delta = if existing.status == "pending" {
                0
            } else {
                existing.object_size_bytes.max(0)
            };
            let next_used = quota_used.saturating_sub(size_delta);
            tx.execute(
                r#"
                UPDATE users
                SET quota_used_bytes = ?2, updated_at = ?3
                WHERE uid = ?1
                "#,
                params![owner_uid, next_used, now_rfc3339()],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok(user)
        })
        .await
    }

    pub async fn empty_trash(
        &self,
        owner_uid: String,
        fragment: String,
    ) -> Result<(usize, UserRecord), RegistryError> {
        let db = self.clone();
        spawn_blocking(move || {
            let mut conn = db.open_connection()?;
            let tx = conn.transaction().map_err(sql_err)?;
            let (quota_limit, quota_used) = quota_for_user(&tx, &owner_uid)?;

            let removed_count = scalar_i64(
                &tx,
                r#"
                SELECT COUNT(*)
                FROM objects
                WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                "#,
                params![owner_uid, fragment],
            )? as usize;

            let reclaimed_bytes = scalar_i64(
                &tx,
                r#"
                SELECT COALESCE(SUM(object_size_bytes), 0)
                FROM objects
                WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                "#,
                params![owner_uid, fragment],
            )?
            .max(0);

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
                DELETE FROM objects
                WHERE owner_uid = ?1 AND fragment = ?2 AND status = 'trashed'
                "#,
                params![owner_uid, fragment],
            )
            .map_err(sql_err)?;

            let next_used = quota_used.saturating_sub(reclaimed_bytes);
            tx.execute(
                r#"
                UPDATE users
                SET quota_used_bytes = ?2, updated_at = ?3
                WHERE uid = ?1
                "#,
                params![owner_uid, next_used, now_rfc3339()],
            )
            .map_err(sql_err)?;

            tx.commit().map_err(sql_err)?;

            let conn = db.open_connection()?;
            let mut user = user_by_uid(&conn, &owner_uid)?;
            user.quota_limit_bytes = quota_limit;
            Ok((removed_count, user))
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
    conn.query_row(
        r#"
        SELECT uid, email, quota_limit_bytes, quota_used_bytes, created_at, updated_at
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
            })
        },
    )
    .map_err(|err| match err {
        rusqlite::Error::QueryReturnedNoRows => RegistryError::not_found("user not found"),
        other => sql_err(other),
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
        SELECT owner_uid, fragment, object_key, version_id, status, object_size_bytes, content_type, created_at, updated_at
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
            SELECT owner_uid, fragment, object_key, version_id, status, object_size_bytes, content_type, created_at, updated_at
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
        SELECT owner_uid, fragment, object_key, expected_size_bytes, content_type, created_at, updated_at
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

fn canonical_archive_key(value: &str) -> String {
    value.trim().trim_start_matches('/').to_string()
}

fn quota_for_user(conn: &Connection, owner_uid: &str) -> Result<(i64, i64), RegistryError> {
    conn.query_row(
        "SELECT quota_limit_bytes, quota_used_bytes FROM users WHERE uid = ?1",
        params![owner_uid],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .map_err(|err| match err {
        rusqlite::Error::QueryReturnedNoRows => RegistryError::not_found("user not found"),
        other => sql_err(other),
    })
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
        version_id: row.get(3)?,
        status: row.get(4)?,
        object_size_bytes: row.get(5)?,
        content_type: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

fn map_pending_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObjectRecord> {
    Ok(ObjectRecord {
        owner_uid: row.get(0)?,
        fragment: row.get(1)?,
        object_key: row.get(2)?,
        version_id: String::new(),
        status: "pending".to_string(),
        object_size_bytes: row.get::<_, Option<i64>>(3)?.unwrap_or(0),
        content_type: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
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

#[cfg(test)]
mod tests {
    use super::RegistryDb;
    use crate::auth::{hash_password, verify_password};
    use crate::models::StorageDeployment;

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
    async fn bulk_recover_and_empty_trash_keep_registry_state_and_quota_consistent() {
        let path =
            std::env::temp_dir().join(format!("registry-bulk-trash-{}.db", uuid::Uuid::new_v4()));
        let db = RegistryDb::new(path.clone(), 1024 * 1024);
        db.init().await.expect("init db");

        let password_hash = hash_password("secret").expect("hash password");
        let user = db
            .create_user("bulk@example.com".to_string(), password_hash, Some(20_000))
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
    async fn archive_manifest_writes_prefix_selected_keys_in_order() {
        let path = std::env::temp_dir().join(format!(
            "registry-archive-manifest-test-{}.db",
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

        let manifest = db
            .write_archive_manifest(
                user.uid,
                "fragment-a".to_string(),
                Vec::new(),
                Some("patients/a".to_string()),
                None,
                false,
            )
            .await
            .expect("write archive manifest");
        assert_eq!(manifest.selected_count, 2);

        let body = std::fs::read_to_string(&manifest.manifest_path).expect("read manifest");
        assert_eq!(
            body,
            concat!(
                "01HARCHMANIFESTVERSION0000000001\tpatients/a/one.dcm\n",
                "01HARCHMANIFESTVERSION0000000002\tpatients/a/two.dcm\n"
            )
        );

        let _ = std::fs::remove_file(&manifest.manifest_path);
        let _ = std::fs::remove_file(path);
    }
}
