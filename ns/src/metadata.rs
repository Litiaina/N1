use crate::enums::app_error_enum::AppError;
use crate::local_auth::nt_hash_hex_for_secret;
use crate::settings::config::NsConfig;
use crate::structs::auth_structs::AuthBody;
use indexdb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use sha2::{Digest as ShaDigest, Sha256};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const SHARES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("shares");
const SYNC_STATE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("sync_state");
const SYNC_WRITE_GUARDS_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("sync_write_guards");
const USERS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("users");
const DISKS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("disks");

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct NasPermission {
    pub read: bool,
    pub write: bool,
    pub delete: bool,
    pub admin: bool,
}

impl NasPermission {
    pub fn read_only() -> Self {
        Self {
            read: true,
            write: false,
            delete: false,
            admin: false,
        }
    }

    pub fn full() -> Self {
        Self {
            read: true,
            write: true,
            delete: true,
            admin: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NasShareRecord {
    pub name: String,
    pub fragment_hash: String,
    pub root_path: String,
    pub read_only: bool,
    pub n1_base_url: Option<String>,
    pub cold_sync_enabled: bool,
    pub cold_n1_prefix: String,
    pub n1_access_token: Option<String>,
    pub n1_access_expires_at_ms: Option<u64>,
    pub n1_refresh_token: Option<String>,
    pub n1_refresh_expires_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NasUserShareAccess {
    pub share: String,
    pub permission: NasPermission,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NasUserRecord {
    pub username: String,
    pub nt_hash_hex: String,
    pub enabled: bool,
    pub shares: Vec<NasUserShareAccess>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NasDiskRecord {
    pub id: String,
    pub root_path: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncCursor {
    pub modified_at_unix_ms: i64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyncWriteGuard {
    active_writers: u64,
    dirty: bool,
    last_started_at_ms: u64,
    last_finished_at_ms: Option<u64>,
    clean_modified_at_unix_ms: Option<i64>,
    clean_size_bytes: Option<u64>,
}

#[derive(Clone)]
pub struct NasMetadataStore {
    db: Arc<Database>,
}

impl NasMetadataStore {
    pub fn open(path: &Path) -> Result<Self, AppError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| AppError::Io(err, format!("mkdir {}", parent.display())))?;
        }
        let db = Database::create(path).map_err(|err| {
            AppError::InternalServerError(format!(
                "open metadata database {}: {}",
                path.display(),
                err
            ))
        })?;
        Ok(Self { db: Arc::new(db) })
    }

    pub fn seed_from_config(&self, config: &NsConfig) -> Result<(), AppError> {
        std::fs::create_dir_all(&config.storage.share_root).map_err(|err| {
            AppError::Io(
                err,
                format!("mkdir {}", config.storage.share_root.display()),
            )
        })?;
        for (disk_id, disk) in &config.disks {
            std::fs::create_dir_all(&disk.root_path)
                .map_err(|err| AppError::Io(err, format!("mkdir {}", disk.root_path.display())))?;
            self.write_disk_record(&NasDiskRecord {
                id: disk_id.clone(),
                root_path: disk.root_path.display().to_string(),
                enabled: disk.enabled,
            })?;
        }
        for (share_name, share) in &config.shares {
            let root_path = if let Some(root_path) = share.root_path.as_ref() {
                root_path.clone()
            } else if let Some(disk_id) = share.disk_id.as_ref() {
                let disk = config.disks.get(disk_id).ok_or_else(|| {
                    AppError::BadRequest(format!(
                        "share '{}' references missing disk '{}'",
                        share_name, disk_id
                    ))
                })?;
                disk.root_path.join(share_name)
            } else {
                config.storage.share_root.join(share_name)
            };
            std::fs::create_dir_all(&root_path)
                .map_err(|err| AppError::Io(err, format!("mkdir {}", root_path.display())))?;
            let fragment_hash = share.fragment_hash.clone().unwrap_or_else(|| {
                fragment_identifier_to_hash(share_name).unwrap_or_else(|| share_name.clone())
            });
            self.write_share_record(&NasShareRecord {
                name: share_name.clone(),
                fragment_hash,
                root_path: root_path.display().to_string(),
                read_only: share.read_only,
                n1_base_url: share
                    .n1_base_url
                    .clone()
                    .or_else(|| config.n1.base_url.clone()),
                cold_sync_enabled: config.sync.enabled && share.sync_enabled,
                cold_n1_prefix: if share.cold_n1_prefix.is_empty() {
                    config.sync.cold_n1_prefix.clone()
                } else {
                    share.cold_n1_prefix.clone()
                },
                n1_access_token: None,
                n1_access_expires_at_ms: None,
                n1_refresh_token: None,
                n1_refresh_expires_at_ms: None,
            })?;
        }
        for (username, user) in &config.users {
            let nt_hash_hex = user
                .nt_hash_hex
                .clone()
                .or_else(|| {
                    user.password
                        .as_ref()
                        .map(|password| nt_hash_hex_for_secret(password))
                })
                .ok_or_else(|| {
                    AppError::BadRequest(format!(
                        "user '{}' is missing password or nt_hash_hex",
                        username
                    ))
                })?;
            let default_permission = NasPermission {
                read: user.read || user.admin,
                write: user.write || user.admin,
                delete: user.delete || user.admin,
                admin: user.admin,
            };
            let shares = if user.shares.is_empty() {
                vec![NasUserShareAccess {
                    share: "*".to_string(),
                    permission: default_permission,
                }]
            } else {
                user.shares
                    .iter()
                    .map(|share| NasUserShareAccess {
                        share: share.trim().to_ascii_lowercase(),
                        permission: default_permission,
                    })
                    .collect()
            };
            self.write_user_record(&NasUserRecord {
                username: username.clone(),
                nt_hash_hex,
                enabled: user.enabled,
                shares,
            })?;
        }
        Ok(())
    }

    pub fn share(&self, share_name: &str) -> Result<Option<NasShareRecord>, AppError> {
        let read_txn = self.db.begin_read().map_err(map_indexdb_error)?;
        let table = match read_txn.open_table(SHARES_TABLE) {
            Ok(table) => table,
            Err(_) => return Ok(None),
        };
        let Some(value) = table.get(share_name).map_err(map_indexdb_error)? else {
            return Ok(None);
        };
        deserialize::<NasShareRecord>(value.value())
    }

    pub fn list_shares(&self) -> Result<Vec<NasShareRecord>, AppError> {
        let read_txn = self.db.begin_read().map_err(map_indexdb_error)?;
        let table = match read_txn.open_table(SHARES_TABLE) {
            Ok(table) => table,
            Err(_) => return Ok(Vec::new()),
        };
        let mut shares = Vec::new();
        let iter = table.iter().map_err(map_indexdb_error)?;
        for row in iter {
            let (_, value) = row.map_err(map_indexdb_error)?;
            shares.push(
                deserialize::<NasShareRecord>(value.value())?.ok_or_else(|| {
                    AppError::InternalServerError("missing share record".to_string())
                })?,
            );
        }
        shares.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(shares)
    }

    pub fn user(&self, username: &str) -> Result<Option<NasUserRecord>, AppError> {
        let username = username.trim().to_ascii_lowercase();
        if username.is_empty() {
            return Ok(None);
        }
        let read_txn = self.db.begin_read().map_err(map_indexdb_error)?;
        let table = match read_txn.open_table(USERS_TABLE) {
            Ok(table) => table,
            Err(_) => return Ok(None),
        };
        let Some(value) = table.get(username.as_str()).map_err(map_indexdb_error)? else {
            return Ok(None);
        };
        deserialize::<NasUserRecord>(value.value())
    }

    pub fn list_users(&self) -> Result<Vec<NasUserRecord>, AppError> {
        let read_txn = self.db.begin_read().map_err(map_indexdb_error)?;
        let table = match read_txn.open_table(USERS_TABLE) {
            Ok(table) => table,
            Err(_) => return Ok(Vec::new()),
        };
        let mut users = Vec::new();
        let iter = table.iter().map_err(map_indexdb_error)?;
        for row in iter {
            let (_, value) = row.map_err(map_indexdb_error)?;
            users.push(
                deserialize::<NasUserRecord>(value.value())?.ok_or_else(|| {
                    AppError::InternalServerError("missing user record".to_string())
                })?,
            );
        }
        users.sort_by(|a, b| a.username.cmp(&b.username));
        Ok(users)
    }

    pub fn list_disks(&self) -> Result<Vec<NasDiskRecord>, AppError> {
        let read_txn = self.db.begin_read().map_err(map_indexdb_error)?;
        let table = match read_txn.open_table(DISKS_TABLE) {
            Ok(table) => table,
            Err(_) => return Ok(Vec::new()),
        };
        let mut disks = Vec::new();
        let iter = table.iter().map_err(map_indexdb_error)?;
        for row in iter {
            let (_, value) = row.map_err(map_indexdb_error)?;
            disks.push(
                deserialize::<NasDiskRecord>(value.value())?.ok_or_else(|| {
                    AppError::InternalServerError("missing disk record".to_string())
                })?,
            );
        }
        disks.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(disks)
    }

    pub fn permission_for_user_share(
        &self,
        username: &str,
        share_name: &str,
    ) -> Result<Option<NasPermission>, AppError> {
        let Some(user) = self.user(username)? else {
            return Ok(None);
        };
        if !user.enabled {
            return Ok(None);
        }
        let share_name = share_name.trim().to_ascii_lowercase();
        let fragment_hash = fragment_identifier_to_hash(share_name.as_str()).unwrap_or_default();
        for access in &user.shares {
            let allowed = access.share.trim().to_ascii_lowercase();
            if allowed == "*"
                || allowed == share_name
                || (!fragment_hash.is_empty() && allowed == fragment_hash)
            {
                return Ok(Some(access.permission));
            }
        }
        Ok(None)
    }

    pub fn resolve_fragment_share(
        &self,
        identifier: &str,
    ) -> Result<Option<NasShareRecord>, AppError> {
        let normalized = normalize_fragment_identifier(identifier);
        if normalized.is_empty() {
            return Ok(None);
        }
        if let Some(share) = self.share(normalized.as_str())? {
            return Ok(Some(share));
        }

        let Some(fragment_hash) = fragment_identifier_to_hash(normalized.as_str()) else {
            return Ok(None);
        };
        if let Some(share) = self.share(fragment_hash.as_str())? {
            return Ok(Some(share));
        }

        Ok(self.list_shares()?.into_iter().find(|share| {
            share
                .fragment_hash
                .eq_ignore_ascii_case(fragment_hash.as_str())
        }))
    }

    pub fn accessible_share(
        &self,
        requested_identifier: &str,
        session_fragment_hash: Option<&str>,
    ) -> Result<Option<NasShareRecord>, AppError> {
        let requested_hash = fragment_identifier_to_hash(requested_identifier);
        if let Some(session_fragment_hash) = session_fragment_hash {
            let Some(requested_hash) = requested_hash else {
                return Ok(None);
            };
            if !requested_hash.eq_ignore_ascii_case(session_fragment_hash.trim()) {
                return Ok(None);
            }
        }

        let Some(share) = self.resolve_fragment_share(requested_identifier)? else {
            return Ok(None);
        };
        if let Some(session_fragment_hash) = session_fragment_hash {
            if !share
                .fragment_hash
                .eq_ignore_ascii_case(session_fragment_hash.trim())
            {
                return Ok(None);
            }
        }
        Ok(Some(share))
    }

    pub fn upsert_authenticated_fragment(
        &self,
        principal: &str,
        fragment_hash: &str,
        config: &NsConfig,
    ) -> Result<NasShareRecord, AppError> {
        let normalized_hash = fragment_hash.trim().to_ascii_lowercase();
        if normalized_hash.is_empty() {
            return Err(AppError::BadRequest(
                "fragment hash must not be empty".to_string(),
            ));
        }

        let existing = self.resolve_fragment_share(principal)?;
        let policy = config.fragment_policy(normalized_hash.as_str());
        let sync_profile = config.fragment_sync(normalized_hash.as_str());
        let mut record = existing.unwrap_or_else(|| NasShareRecord {
            name: normalized_hash.clone(),
            fragment_hash: normalized_hash.clone(),
            root_path: policy
                .and_then(|policy| policy.root_path.clone())
                .unwrap_or_else(|| config.storage.share_root.join(normalized_hash.as_str()))
                .display()
                .to_string(),
            read_only: policy
                .and_then(|policy| policy.read_only)
                .unwrap_or(config.storage.read_only_default),
            n1_base_url: sync_profile
                .and_then(|profile| profile.n1_base_url.clone())
                .or_else(|| config.n1.base_url.clone()),
            cold_sync_enabled: config.sync.enabled
                && sync_profile.map(|profile| profile.enabled).unwrap_or(false),
            cold_n1_prefix: sync_profile
                .map(|profile| profile.cold_n1_prefix.clone())
                .filter(|prefix| !prefix.is_empty())
                .unwrap_or_else(|| config.sync.cold_n1_prefix.clone()),
            n1_access_token: None,
            n1_access_expires_at_ms: None,
            n1_refresh_token: None,
            n1_refresh_expires_at_ms: None,
        });

        record.name = normalized_hash.clone();
        record.fragment_hash = normalized_hash;
        record.n1_base_url = sync_profile
            .and_then(|profile| profile.n1_base_url.clone())
            .or_else(|| config.n1.base_url.clone());
        record.cold_sync_enabled =
            config.sync.enabled && sync_profile.map(|profile| profile.enabled).unwrap_or(false);
        record.cold_n1_prefix = sync_profile
            .map(|profile| profile.cold_n1_prefix.clone())
            .filter(|prefix| !prefix.is_empty())
            .unwrap_or_else(|| config.sync.cold_n1_prefix.clone());
        record.read_only = policy
            .and_then(|policy| policy.read_only)
            .unwrap_or(config.storage.read_only_default);

        std::fs::create_dir_all(record.root_path.as_str())
            .map_err(|err| AppError::Io(err, format!("mkdir {}", record.root_path)))?;

        self.write_share_record(&record)?;
        Ok(record)
    }

    pub fn update_share_auth_tokens(
        &self,
        share_name: &str,
        auth: &AuthBody,
    ) -> Result<Option<NasShareRecord>, AppError> {
        let Some(mut share) = self.share(share_name)? else {
            return Ok(None);
        };
        let now = now_unix_millis();
        share.n1_access_token = Some(auth.access_token.clone());
        share.n1_access_expires_at_ms = Some(now.saturating_add(auth.expires_in * 1000));
        share.n1_refresh_token = auth.refresh_token.clone();
        share.n1_refresh_expires_at_ms = auth
            .refresh_expires_in
            .map(|expires_in| now.saturating_add(expires_in * 1000));
        self.write_share_record(&share)?;
        Ok(Some(share))
    }

    pub fn sync_cursor(
        &self,
        share_name: &str,
        relative_path: &str,
    ) -> Result<Option<SyncCursor>, AppError> {
        let key = sync_key(share_name, relative_path);
        let read_txn = self.db.begin_read().map_err(map_indexdb_error)?;
        let table = match read_txn.open_table(SYNC_STATE_TABLE) {
            Ok(table) => table,
            Err(_) => return Ok(None),
        };
        let Some(value) = table.get(key.as_str()).map_err(map_indexdb_error)? else {
            return Ok(None);
        };
        deserialize::<SyncCursor>(value.value())
    }

    pub fn put_sync_cursor(
        &self,
        share_name: &str,
        relative_path: &str,
        cursor: &SyncCursor,
    ) -> Result<(), AppError> {
        let key = sync_key(share_name, relative_path);
        let write_txn = self.db.begin_write().map_err(map_indexdb_error)?;
        {
            let mut table = write_txn
                .open_table(SYNC_STATE_TABLE)
                .map_err(map_indexdb_error)?;
            table
                .insert(key.as_str(), serialize(cursor)?.as_slice())
                .map_err(map_indexdb_error)?;
        }
        write_txn.commit().map_err(map_indexdb_error)?;
        Ok(())
    }

    pub fn sync_guard_allows_file(
        &self,
        share_name: &str,
        relative_path: &str,
    ) -> Result<bool, AppError> {
        let key = sync_key(share_name, relative_path);
        let read_txn = self.db.begin_read().map_err(map_indexdb_error)?;
        let table = match read_txn.open_table(SYNC_WRITE_GUARDS_TABLE) {
            Ok(table) => table,
            Err(_) => return Ok(true),
        };
        let Some(value) = table.get(key.as_str()).map_err(map_indexdb_error)? else {
            return Ok(true);
        };
        let Some(guard) = deserialize::<SyncWriteGuard>(value.value())? else {
            return Ok(true);
        };
        Ok(guard.active_writers == 0 && !guard.dirty)
    }

    pub fn mark_sync_write_started(
        &self,
        share_name: &str,
        relative_path: &str,
    ) -> Result<(), AppError> {
        let key = sync_key(share_name, relative_path);
        let now = now_unix_millis();
        let write_txn = self.db.begin_write().map_err(map_indexdb_error)?;
        {
            let mut table = write_txn
                .open_table(SYNC_WRITE_GUARDS_TABLE)
                .map_err(map_indexdb_error)?;
            let mut guard = table
                .get(key.as_str())
                .map_err(map_indexdb_error)?
                .map(|value| deserialize::<SyncWriteGuard>(value.value()))
                .transpose()?
                .flatten()
                .unwrap_or_else(|| SyncWriteGuard {
                    active_writers: 0,
                    dirty: false,
                    last_started_at_ms: 0,
                    last_finished_at_ms: None,
                    clean_modified_at_unix_ms: None,
                    clean_size_bytes: None,
                });
            guard.active_writers = guard.active_writers.saturating_add(1);
            guard.dirty = true;
            guard.last_started_at_ms = now;
            guard.last_finished_at_ms = None;
            table
                .insert(key.as_str(), serialize(&guard)?.as_slice())
                .map_err(map_indexdb_error)?;
        }
        write_txn.commit().map_err(map_indexdb_error)?;
        Ok(())
    }

    pub fn mark_sync_write_clean(
        &self,
        share_name: &str,
        relative_path: &str,
        modified_at_unix_ms: i64,
        size_bytes: u64,
    ) -> Result<(), AppError> {
        let key = sync_key(share_name, relative_path);
        let now = now_unix_millis();
        let write_txn = self.db.begin_write().map_err(map_indexdb_error)?;
        {
            let mut table = write_txn
                .open_table(SYNC_WRITE_GUARDS_TABLE)
                .map_err(map_indexdb_error)?;
            let mut guard = table
                .get(key.as_str())
                .map_err(map_indexdb_error)?
                .map(|value| deserialize::<SyncWriteGuard>(value.value()))
                .transpose()?
                .flatten()
                .unwrap_or_else(|| SyncWriteGuard {
                    active_writers: 0,
                    dirty: true,
                    last_started_at_ms: 0,
                    last_finished_at_ms: None,
                    clean_modified_at_unix_ms: None,
                    clean_size_bytes: None,
                });
            guard.active_writers = guard.active_writers.saturating_sub(1);
            if guard.active_writers == 0 {
                guard.dirty = false;
                guard.clean_modified_at_unix_ms = Some(modified_at_unix_ms);
                guard.clean_size_bytes = Some(size_bytes);
            }
            guard.last_finished_at_ms = Some(now);
            table
                .insert(key.as_str(), serialize(&guard)?.as_slice())
                .map_err(map_indexdb_error)?;
        }
        write_txn.commit().map_err(map_indexdb_error)?;
        Ok(())
    }

    pub fn mark_sync_write_aborted(
        &self,
        share_name: &str,
        relative_path: &str,
    ) -> Result<(), AppError> {
        let key = sync_key(share_name, relative_path);
        let now = now_unix_millis();
        let write_txn = self.db.begin_write().map_err(map_indexdb_error)?;
        {
            let mut table = write_txn
                .open_table(SYNC_WRITE_GUARDS_TABLE)
                .map_err(map_indexdb_error)?;
            let mut guard = table
                .get(key.as_str())
                .map_err(map_indexdb_error)?
                .map(|value| deserialize::<SyncWriteGuard>(value.value()))
                .transpose()?
                .flatten()
                .unwrap_or_else(|| SyncWriteGuard {
                    active_writers: 0,
                    dirty: true,
                    last_started_at_ms: 0,
                    last_finished_at_ms: None,
                    clean_modified_at_unix_ms: None,
                    clean_size_bytes: None,
                });
            guard.active_writers = guard.active_writers.saturating_sub(1);
            guard.dirty = true;
            guard.last_finished_at_ms = Some(now);
            table
                .insert(key.as_str(), serialize(&guard)?.as_slice())
                .map_err(map_indexdb_error)?;
        }
        write_txn.commit().map_err(map_indexdb_error)?;
        Ok(())
    }

    pub fn clear_sync_write_guard(
        &self,
        share_name: &str,
        relative_path: &str,
    ) -> Result<(), AppError> {
        let key = sync_key(share_name, relative_path);
        let write_txn = self.db.begin_write().map_err(map_indexdb_error)?;
        {
            let mut table = write_txn
                .open_table(SYNC_WRITE_GUARDS_TABLE)
                .map_err(map_indexdb_error)?;
            table.remove(key.as_str()).map_err(map_indexdb_error)?;
        }
        write_txn.commit().map_err(map_indexdb_error)?;
        Ok(())
    }

    pub fn move_sync_write_guard(
        &self,
        share_name: &str,
        old_relative_path: &str,
        new_relative_path: &str,
    ) -> Result<(), AppError> {
        if old_relative_path == new_relative_path {
            return Ok(());
        }
        let old_key = sync_key(share_name, old_relative_path);
        let new_key = sync_key(share_name, new_relative_path);
        let write_txn = self.db.begin_write().map_err(map_indexdb_error)?;
        {
            let mut table = write_txn
                .open_table(SYNC_WRITE_GUARDS_TABLE)
                .map_err(map_indexdb_error)?;
            let guard_bytes = {
                table
                    .get(old_key.as_str())
                    .map_err(map_indexdb_error)?
                    .map(|value| value.value().to_vec())
            };
            if let Some(bytes) = guard_bytes {
                table
                    .insert(new_key.as_str(), bytes.as_slice())
                    .map_err(map_indexdb_error)?;
                table.remove(old_key.as_str()).map_err(map_indexdb_error)?;
            }
        }
        write_txn.commit().map_err(map_indexdb_error)?;
        Ok(())
    }

    pub fn write_share_record(&self, record: &NasShareRecord) -> Result<(), AppError> {
        let write_txn = self.db.begin_write().map_err(map_indexdb_error)?;
        {
            let mut shares = write_txn
                .open_table(SHARES_TABLE)
                .map_err(map_indexdb_error)?;
            shares
                .insert(record.name.as_str(), serialize(record)?.as_slice())
                .map_err(map_indexdb_error)?;
        }
        write_txn.commit().map_err(map_indexdb_error)?;
        Ok(())
    }

    pub fn write_user_record(&self, record: &NasUserRecord) -> Result<(), AppError> {
        let username = record.username.trim().to_ascii_lowercase();
        if username.is_empty() {
            return Err(AppError::BadRequest(
                "username must not be empty".to_string(),
            ));
        }
        let mut record = record.clone();
        record.username = username;
        let write_txn = self.db.begin_write().map_err(map_indexdb_error)?;
        {
            let mut users = write_txn
                .open_table(USERS_TABLE)
                .map_err(map_indexdb_error)?;
            users
                .insert(record.username.as_str(), serialize(&record)?.as_slice())
                .map_err(map_indexdb_error)?;
        }
        write_txn.commit().map_err(map_indexdb_error)?;
        Ok(())
    }

    pub fn write_disk_record(&self, record: &NasDiskRecord) -> Result<(), AppError> {
        let id = record.id.trim().to_ascii_lowercase();
        if id.is_empty() {
            return Err(AppError::BadRequest(
                "disk id must not be empty".to_string(),
            ));
        }
        let mut record = record.clone();
        record.id = id;
        let write_txn = self.db.begin_write().map_err(map_indexdb_error)?;
        {
            let mut disks = write_txn
                .open_table(DISKS_TABLE)
                .map_err(map_indexdb_error)?;
            disks
                .insert(record.id.as_str(), serialize(&record)?.as_slice())
                .map_err(map_indexdb_error)?;
        }
        write_txn.commit().map_err(map_indexdb_error)?;
        Ok(())
    }
}

fn map_indexdb_error(error: impl std::fmt::Display) -> AppError {
    AppError::InternalServerError(format!("metadata database error: {}", error))
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, AppError> {
    rmp_serde::to_vec(value)
        .map_err(|err| AppError::InternalServerError(format!("metadata serialize: {}", err)))
}

fn deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<Option<T>, AppError> {
    rmp_serde::from_slice(bytes)
        .map(Some)
        .map_err(|err| AppError::InternalServerError(format!("metadata deserialize: {}", err)))
}

fn sync_key(share_name: &str, relative_path: &str) -> String {
    format!("{}\0{}", share_name, relative_path)
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn normalize_fragment_identifier(value: &str) -> String {
    value.trim_matches('\0').trim().to_string()
}

fn is_fragment_hash_identifier(value: &str) -> bool {
    value.len() == 64 && value.as_bytes().iter().all(|b| b.is_ascii_hexdigit())
}

fn fragment_identifier_to_hash(identifier: &str) -> Option<String> {
    let normalized = normalize_fragment_identifier(identifier);
    if normalized.is_empty() {
        return None;
    }
    if is_fragment_hash_identifier(normalized.as_str()) {
        return Some(normalized.to_ascii_lowercase());
    }
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    Some(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::config::{
        FragmentPolicyConfig, FragmentSyncConfig, N1Config, NsAdminConfig, NsDiskConfig,
        NsShareConfig, NsUserConfig, StorageConfig, SyncConfig,
    };
    use crate::smb::config::SmbConfig;
    use crate::structs::auth_structs::AuthBody;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(test_name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "litiaina-ns-metadata-{}-{}-{}",
            test_name,
            std::process::id(),
            unique
        ))
    }

    fn test_config(root: &Path) -> NsConfig {
        NsConfig {
            admin: NsAdminConfig {
                enabled: false,
                bind_addr: "127.0.0.1".to_string(),
                port: 0,
                admin_key: None,
            },
            smb: SmbConfig::default(),
            storage: StorageConfig {
                metadata_db_path: root.join("metadata.ndxdb"),
                staging_root: root.join("staging"),
                share_root: root.join("shares"),
                read_only_default: false,
            },
            n1: N1Config {
                base_url: Some("https://127.0.0.1:50001".to_string()),
                native_url: None,
                accept_invalid_certs: true,
                index_roots: vec![root.join("n1-index")],
            },
            sync: SyncConfig {
                enabled: true,
                scan_interval_seconds: 300,
                request_timeout_seconds: 300,
                min_stable_age_seconds: 120,
                cold_n1_prefix: String::new(),
            },
            users: HashMap::new(),
            disks: HashMap::new(),
            shares: HashMap::new(),
            fragment_aliases: HashMap::new(),
            fragment_policies: HashMap::new(),
            fragment_syncs: HashMap::new(),
        }
    }

    #[test]
    fn dynamic_fragment_share_resolves_name_and_hash() {
        let root = temp_root("fragment-share");
        let config = test_config(root.as_path());
        let expected_hash = fragment_identifier_to_hash("my-fragment").expect("fragment hash");
        let store = NasMetadataStore::open(config.storage.metadata_db_path.as_path())
            .expect("metadata opens");
        store.seed_from_config(&config).expect("seed config");
        store
            .upsert_authenticated_fragment("my-fragment", expected_hash.as_str(), &config)
            .expect("upsert dynamic fragment");

        let by_name = store
            .accessible_share("my-fragment", Some(expected_hash.as_str()))
            .expect("share lookup")
            .expect("fragment share exists");
        assert_eq!(by_name.name, expected_hash);
        assert_eq!(by_name.fragment_hash, expected_hash);

        let by_hash = store
            .accessible_share(
                by_name.fragment_hash.as_str(),
                Some(by_name.fragment_hash.as_str()),
            )
            .expect("hash lookup")
            .expect("hash resolves to same share");
        assert_eq!(by_hash.name, by_name.fragment_hash);
        assert!(
            store
                .accessible_share("my-fragment", Some("different-fragment"))
                .expect("share lookup")
                .is_none()
        );

        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn dynamic_fragment_share_applies_policy_and_sync_overrides() {
        let root = temp_root("policy-sync");
        let mut config = test_config(root.as_path());
        let fragment_hash = fragment_identifier_to_hash("my-fragment").expect("fragment hash");
        config.fragment_policies.insert(
            fragment_hash.clone(),
            FragmentPolicyConfig {
                root_path: Some(root.join("custom-share-root")),
                read_only: Some(true),
            },
        );
        config.fragment_syncs.insert(
            fragment_hash.clone(),
            FragmentSyncConfig {
                enabled: true,
                secret: Some("password".to_string()),
                n1_base_url: Some("https://127.0.0.2:50001".to_string()),
                cold_n1_prefix: "nas/backup".to_string(),
            },
        );

        let store = NasMetadataStore::open(config.storage.metadata_db_path.as_path())
            .expect("metadata opens");
        store.seed_from_config(&config).expect("seed config");
        let share = store
            .upsert_authenticated_fragment("my-fragment", fragment_hash.as_str(), &config)
            .expect("upsert dynamic fragment");

        assert_eq!(
            PathBuf::from(share.root_path.clone()),
            root.join("custom-share-root")
        );
        assert!(share.read_only);
        assert_eq!(
            share.n1_base_url.as_deref(),
            Some("https://127.0.0.2:50001")
        );
        assert!(share.cold_sync_enabled);
        assert_eq!(share.cold_n1_prefix, "nas/backup");

        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn share_auth_tokens_can_be_updated_after_creation() {
        let root = temp_root("share-auth-tokens");
        let config = test_config(root.as_path());
        let fragment_hash = fragment_identifier_to_hash("my-fragment").expect("fragment hash");
        let store = NasMetadataStore::open(config.storage.metadata_db_path.as_path())
            .expect("metadata opens");
        store.seed_from_config(&config).expect("seed config");
        let share = store
            .upsert_authenticated_fragment("my-fragment", fragment_hash.as_str(), &config)
            .expect("upsert dynamic fragment");

        let updated = store
            .update_share_auth_tokens(
                share.name.as_str(),
                &AuthBody {
                    access_token: "token-123".to_string(),
                    token_type: "Bearer".to_string(),
                    expires_in: 3600,
                    refresh_token: Some("refresh-123".to_string()),
                    refresh_expires_in: Some(7200),
                },
            )
            .expect("update tokens")
            .expect("share exists");
        assert_eq!(updated.n1_access_token.as_deref(), Some("token-123"));
        assert_eq!(updated.n1_refresh_token.as_deref(), Some("refresh-123"));
        assert!(updated.n1_access_expires_at_ms.is_some());
        assert!(updated.n1_refresh_expires_at_ms.is_some());

        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn sync_write_guard_blocks_dirty_and_aborted_files() {
        let root = temp_root("sync-write-guard");
        let config = test_config(root.as_path());
        let store = NasMetadataStore::open(config.storage.metadata_db_path.as_path())
            .expect("metadata opens");

        assert!(
            store
                .sync_guard_allows_file("share-a", "doc.txt")
                .expect("unguarded file allowed")
        );

        store
            .mark_sync_write_started("share-a", "doc.txt")
            .expect("mark started");
        assert!(
            !store
                .sync_guard_allows_file("share-a", "doc.txt")
                .expect("active file blocked")
        );

        store
            .mark_sync_write_aborted("share-a", "doc.txt")
            .expect("mark aborted");
        assert!(
            !store
                .sync_guard_allows_file("share-a", "doc.txt")
                .expect("aborted file remains blocked")
        );

        store
            .mark_sync_write_started("share-a", "doc.txt")
            .expect("restart write");
        store
            .mark_sync_write_clean("share-a", "doc.txt", 1234, 64)
            .expect("mark clean");
        assert!(
            store
                .sync_guard_allows_file("share-a", "doc.txt")
                .expect("clean file allowed")
        );

        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn sync_write_guard_moves_with_rename() {
        let root = temp_root("sync-write-guard-rename");
        let config = test_config(root.as_path());
        let store = NasMetadataStore::open(config.storage.metadata_db_path.as_path())
            .expect("metadata opens");

        store
            .mark_sync_write_started("share-a", "old.txt")
            .expect("mark started");
        store
            .move_sync_write_guard("share-a", "old.txt", "new.txt")
            .expect("move guard");

        assert!(
            store
                .sync_guard_allows_file("share-a", "old.txt")
                .expect("old path unguarded")
        );
        assert!(
            !store
                .sync_guard_allows_file("share-a", "new.txt")
                .expect("new path inherits guard")
        );

        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn configured_nas_users_disks_and_shares_seed_permissions() {
        let root = temp_root("configured-nas");
        let mut config = test_config(root.as_path());
        config.disks.insert(
            "disk-a".to_string(),
            NsDiskConfig {
                root_path: root.join("disk-a"),
                enabled: true,
            },
        );
        config.shares.insert(
            "clinical".to_string(),
            NsShareConfig {
                root_path: None,
                disk_id: Some("disk-a".to_string()),
                fragment_hash: None,
                read_only: false,
                n1_base_url: None,
                sync_enabled: false,
                cold_n1_prefix: String::new(),
            },
        );
        config.users.insert(
            "doctor".to_string(),
            NsUserConfig {
                password: Some("rounds-password".to_string()),
                nt_hash_hex: None,
                enabled: true,
                shares: vec!["clinical".to_string()],
                read: true,
                write: true,
                delete: false,
                admin: false,
            },
        );

        let store = NasMetadataStore::open(config.storage.metadata_db_path.as_path())
            .expect("metadata opens");
        store.seed_from_config(&config).expect("seed config");

        let disks = store.list_disks().expect("list disks");
        assert_eq!(disks.len(), 1);
        assert_eq!(disks[0].id, "disk-a");
        assert!(PathBuf::from(disks[0].root_path.as_str()).exists());

        let share = store
            .share("clinical")
            .expect("share lookup")
            .expect("share exists");
        assert_eq!(PathBuf::from(share.root_path), root.join("disk-a/clinical"));

        let permission = store
            .permission_for_user_share("doctor", "clinical")
            .expect("permission lookup")
            .expect("permission exists");
        assert!(permission.read);
        assert!(permission.write);
        assert!(!permission.delete);
        assert!(!permission.admin);
        assert!(
            store
                .permission_for_user_share("doctor", "other")
                .expect("other permission lookup")
                .is_none()
        );

        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }
}
