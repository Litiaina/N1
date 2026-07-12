use crate::core::disk_manager::ObjectPathManager;
use crate::core::fragment_identity;
use crate::core::index_store::{FragmentMeta as IndexFragmentMeta, N1_ENGINE_FORMAT_VERSION};
use crate::core::linux_io;
use crate::{core::constants::FRAGMENT_METADATA_FILE, enums::app_error_enum::AppError};

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use chrono::{SecondsFormat, Utc};
use md4::{Digest as Md4Digest, Md4};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use tokio::fs as tokio_fs;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{LazyLock, Mutex};

pub const DEFAULT_INDEX_PARTITION_COUNT: u16 = 64;
pub const SUPPORTED_INDEX_PARTITION_COUNTS: [u16; 3] = [32, 64, 256];

static CONFIGURED_INDEX_PARTITION_COUNT: LazyLock<AtomicU16> =
    LazyLock::new(|| AtomicU16::new(DEFAULT_INDEX_PARTITION_COUNT));
static FRAGMENT_INDEX_PARTITION_COUNT_CACHE: LazyLock<Mutex<HashMap<String, u16>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentMetadata {
    pub engine_format_version: u16,
    pub stable_fragment_id: String,
    pub key_phc: String,
    #[serde(default)]
    pub nt_hash_hex: Option<String>,
    pub index_partition_count: u16,
    pub created: String,
    pub updated: Option<String>,
}

fn now_utc_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn validate_supported_index_partition_count(count: u16) -> Result<(), AppError> {
    if SUPPORTED_INDEX_PARTITION_COUNTS.contains(&count) {
        return Ok(());
    }

    Err(AppError::BadRequest(format!(
        "unsupported index partition count {} (supported: 32, 64, 256)",
        count
    )))
}

pub fn configured_index_partition_count() -> u16 {
    CONFIGURED_INDEX_PARTITION_COUNT.load(Ordering::Relaxed)
}

pub fn set_configured_index_partition_count(count: u16) -> Result<(), AppError> {
    validate_supported_index_partition_count(count)?;
    CONFIGURED_INDEX_PARTITION_COUNT.store(count, Ordering::Relaxed);
    Ok(())
}

fn enforce_fragment_index_partition_count(
    fragment_hash: &str,
    partition_count: u16,
) -> Result<u16, AppError> {
    validate_supported_index_partition_count(partition_count)?;
    let configured = configured_index_partition_count();
    if partition_count != configured {
        return Err(AppError::Conflict(format!(
            "fragment {} uses {} index partitions, but this setup is configured for {}; run an index rebalance/migration before reopening it",
            fragment_hash, partition_count, configured
        )));
    }
    Ok(partition_count)
}

fn unix_to_rfc3339(seconds: i64) -> String {
    chrono::DateTime::<Utc>::from_timestamp(seconds, 0)
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn decode_index_fragment_meta(raw: &[u8]) -> Result<IndexFragmentMeta, AppError> {
    let meta = rmp_serde::from_slice::<IndexFragmentMeta>(raw).map_err(|e| {
        AppError::InternalServerError(format!("decode N1 fragment metadata: {}", e))
    })?;
    if meta.engine_format_version != N1_ENGINE_FORMAT_VERSION {
        return Err(AppError::Conflict(format!(
            "fragment uses N1 engine format {}, but this binary requires format {}; N1 3.0 does not open or migrate earlier engine formats",
            meta.engine_format_version, N1_ENGINE_FORMAT_VERSION
        )));
    }
    Ok(meta)
}

fn fragment_metadata_from_index(meta: IndexFragmentMeta) -> FragmentMetadata {
    FragmentMetadata {
        engine_format_version: meta.engine_format_version,
        stable_fragment_id: meta.stable_fragment_id,
        key_phc: meta.key_phc.unwrap_or_default(),
        nt_hash_hex: meta.nt_hash_hex,
        index_partition_count: meta.index_partition_count,
        created: unix_to_rfc3339(meta.created_at_unix),
        updated: Some(unix_to_rfc3339(meta.updated_at_unix)),
    }
}

fn validate_fragment_metadata_identity(
    meta: FragmentMetadata,
    root: &Path,
    requested_fragment: &str,
) -> Result<FragmentMetadata, AppError> {
    let resolved = fragment_identity::stable_fragment_id_for_root(root, requested_fragment)
        .ok_or_else(|| {
            AppError::InternalServerError(format!(
                "fragment {} has no stable identity on {}",
                requested_fragment,
                root.display()
            ))
        })?;
    if meta.stable_fragment_id != resolved {
        return Err(AppError::InternalServerError(format!(
            "fragment metadata identity mismatch on {}: record={}, alias={}",
            root.display(),
            meta.stable_fragment_id,
            resolved
        )));
    }
    Ok(meta)
}

fn decode_fragment_metadata(raw: &[u8]) -> Result<FragmentMetadata, AppError> {
    decode_index_fragment_meta(raw).map(fragment_metadata_from_index)
}

fn fragment_meta_path(root: &Path, fragment_hash: &str) -> PathBuf {
    fragment_identity::fragment_root_on(root, fragment_hash).join(FRAGMENT_METADATA_FILE)
}

async fn durable_atomic_write(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    linux_io::atomic_write(path, bytes.to_vec()).await
}

async fn read_index_fragment_meta_if_exists(
    path: &Path,
) -> Result<Option<IndexFragmentMeta>, AppError> {
    if !tokio_fs::try_exists(path).await.unwrap_or(false) {
        return Ok(None);
    }
    let raw = tokio_fs::read(path)
        .await
        .map_err(|e| AppError::Io(e, format!("read {}", path.display())))?;
    decode_index_fragment_meta(&raw).map(Some)
}

fn hash_key_to_phc(key: &str) -> Result<String, AppError> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();

    let phc = argon2
        .hash_password(key.as_bytes(), &salt)
        .map_err(|e| AppError::InternalServerError(format!("argon2 hash: {}", e)))?
        .to_string();

    Ok(phc)
}

fn hash_key_to_nt_hash_hex(key: &str) -> String {
    let mut hasher = Md4::new();
    hasher.update(&utf16le_bytes(key));
    let digest = hasher.finalize();
    hex::encode(digest)
}

fn utf16le_bytes(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len() * 2);
    for codepoint in text.encode_utf16() {
        out.extend_from_slice(&codepoint.to_le_bytes());
    }
    out
}

pub async fn init_fragment_metadata(
    fragment_hash: &str,
    key: &str,
) -> Result<FragmentMetadata, AppError> {
    if key.is_empty() {
        return Err(AppError::BadRequest("key must not be empty".into()));
    }

    let meta = FragmentMetadata {
        engine_format_version: N1_ENGINE_FORMAT_VERSION,
        stable_fragment_id: fragment_hash.to_string(),
        key_phc: hash_key_to_phc(key)?,
        nt_hash_hex: Some(hash_key_to_nt_hash_hex(key)),
        index_partition_count: configured_index_partition_count(),
        created: now_utc_rfc3339(),
        updated: None,
    };

    write_fragment_metadata(&meta).await?;
    Ok(meta)
}

pub async fn init_fragment_metadata_strict(
    fragment_hash: &str,
    key: &str,
) -> Result<FragmentMetadata, AppError> {
    if key.is_empty() {
        return Err(AppError::BadRequest("key must not be empty".into()));
    }

    let meta = FragmentMetadata {
        engine_format_version: N1_ENGINE_FORMAT_VERSION,
        stable_fragment_id: fragment_hash.to_string(),
        key_phc: hash_key_to_phc(key)?,
        nt_hash_hex: Some(hash_key_to_nt_hash_hex(key)),
        index_partition_count: configured_index_partition_count(),
        created: now_utc_rfc3339(),
        updated: None,
    };

    write_fragment_metadata_strict(&meta).await?;
    Ok(meta)
}

pub async fn read_fragment_metadata(fragment_hash: &str) -> Result<FragmentMetadata, AppError> {
    let pm = ObjectPathManager::new();
    let roots = pm.get_all_index_roots();

    for root in roots {
        let p = fragment_meta_path(&root, fragment_hash);
        if !tokio_fs::try_exists(&p).await.unwrap_or(false) {
            continue;
        }

        let raw = tokio_fs::read(&p)
            .await
            .map_err(|e| AppError::Io(e, format!("read {}", p.display())))?;

        let meta = decode_fragment_metadata(&raw)?;

        if !meta.key_phc.trim().is_empty() {
            return validate_fragment_metadata_identity(meta, &root, fragment_hash);
        }
    }

    Err(AppError::NotFound)
}

pub fn read_fragment_metadata_sync(fragment_hash: &str) -> Result<FragmentMetadata, AppError> {
    let pm = ObjectPathManager::new();
    let roots = pm.get_all_index_roots();

    for root in roots {
        let p = fragment_meta_path(&root, fragment_hash);
        if !p.exists() {
            continue;
        }

        let raw =
            std::fs::read(&p).map_err(|e| AppError::Io(e, format!("read {}", p.display())))?;
        let meta = decode_fragment_metadata(&raw)?;
        if !meta.key_phc.trim().is_empty() {
            return validate_fragment_metadata_identity(meta, &root, fragment_hash);
        }
    }

    Err(AppError::NotFound)
}

pub async fn resolve_stable_fragment_id(fragment: &str) -> Result<String, AppError> {
    Ok(read_fragment_metadata(fragment).await?.stable_fragment_id)
}

pub fn resolve_stable_fragment_id_sync(fragment: &str) -> Result<String, AppError> {
    Ok(read_fragment_metadata_sync(fragment)?.stable_fragment_id)
}

pub fn fragment_index_partition_count(fragment_hash: &str) -> Result<u16, AppError> {
    if let Ok(cache) = FRAGMENT_INDEX_PARTITION_COUNT_CACHE.lock() {
        if let Some(count) = cache.get(fragment_hash).copied() {
            return enforce_fragment_index_partition_count(fragment_hash, count);
        }
    }

    let meta = read_fragment_metadata_sync(fragment_hash)?;
    let count = meta.index_partition_count;
    if let Ok(mut cache) = FRAGMENT_INDEX_PARTITION_COUNT_CACHE.lock() {
        cache.insert(fragment_hash.to_string(), count);
    }
    enforce_fragment_index_partition_count(fragment_hash, count)
}

pub async fn write_fragment_metadata(meta: &FragmentMetadata) -> Result<(), AppError> {
    write_fragment_metadata_with_policy(meta, false).await
}

pub async fn write_fragment_metadata_strict(meta: &FragmentMetadata) -> Result<(), AppError> {
    write_fragment_metadata_with_policy(meta, true).await
}

async fn write_fragment_metadata_with_policy(
    meta: &FragmentMetadata,
    require_all_roots: bool,
) -> Result<(), AppError> {
    validate_supported_index_partition_count(meta.index_partition_count)?;
    let pm = ObjectPathManager::new();
    let roots = pm.get_all_index_roots();

    let mut any_ok = false;
    let mut failed_paths = Vec::new();

    for root in roots {
        let p = fragment_meta_path(&root, &meta.stable_fragment_id);
        let now = Utc::now().timestamp();
        let mut edge = match read_index_fragment_meta_if_exists(&p).await {
            Ok(Some(existing)) => existing,
            Ok(None) => IndexFragmentMeta {
                engine_format_version: N1_ENGINE_FORMAT_VERSION,
                stable_fragment_id: meta.stable_fragment_id.clone(),
                state: "active".to_string(),
                committed_bytes: 0,
                reserved_bytes: 0,
                trash_bytes: 0,
                created_at_unix: now,
                updated_at_unix: now,
                key_phc: None,
                nt_hash_hex: None,
                index_partition_count: meta.index_partition_count,
            },
            Err(err) => {
                failed_paths.push(format!("{} ({})", p.display(), err));
                continue;
            }
        };
        if edge.stable_fragment_id != meta.stable_fragment_id {
            failed_paths.push(format!(
                "{} (stable fragment identity mismatch: record={}, requested={})",
                p.display(),
                edge.stable_fragment_id,
                meta.stable_fragment_id
            ));
            continue;
        }
        edge.key_phc = Some(meta.key_phc.clone());
        edge.nt_hash_hex = meta.nt_hash_hex.clone();
        edge.index_partition_count = meta.index_partition_count;
        edge.updated_at_unix = now;
        let bytes = match rmp_serde::to_vec_named(&edge) {
            Ok(bytes) => bytes,
            Err(e) => {
                failed_paths.push(format!(
                    "{} (encode N1 fragment metadata: {})",
                    p.display(),
                    e
                ));
                continue;
            }
        };

        match durable_atomic_write(&p, &bytes).await {
            Ok(()) => {
                any_ok = true;
            }
            Err(err) => {
                failed_paths.push(format!("{} ({})", p.display(), err));
            }
        }
    }

    if require_all_roots && !failed_paths.is_empty() {
        return Err(AppError::ServiceUnavailable(format!(
            "failed to write fragment metadata to all index roots: {}",
            failed_paths.join("; ")
        )));
    }

    if !any_ok {
        return Err(AppError::InternalServerError(
            "failed to write fragment metadata to any index root".into(),
        ));
    }

    if let Ok(mut cache) = FRAGMENT_INDEX_PARTITION_COUNT_CACHE.lock() {
        cache.insert(meta.stable_fragment_id.clone(), meta.index_partition_count);
    }

    Ok(())
}

pub async fn rotate_fragment_key(fragment_hash: &str, new_key: &str) -> Result<(), AppError> {
    if new_key.is_empty() {
        return Err(AppError::BadRequest("new_key must not be empty".into()));
    }

    let mut meta = match read_fragment_metadata(fragment_hash).await {
        Ok(m) => m,
        Err(_) => FragmentMetadata {
            engine_format_version: N1_ENGINE_FORMAT_VERSION,
            stable_fragment_id: fragment_hash.to_string(),
            key_phc: String::new(),
            nt_hash_hex: None,
            index_partition_count: configured_index_partition_count(),
            created: now_utc_rfc3339(),
            updated: None,
        },
    };

    meta.key_phc = hash_key_to_phc(new_key)?;
    meta.nt_hash_hex = Some(hash_key_to_nt_hash_hex(new_key));
    meta.updated = Some(now_utc_rfc3339());
    write_fragment_metadata(&meta).await
}

pub async fn verify_fragment_key(fragment_hash: &str, key: &str) -> Result<bool, AppError> {
    if key.is_empty() {
        return Ok(false);
    }

    let meta = read_fragment_metadata(fragment_hash).await?;

    if meta.key_phc.is_empty() {
        return Ok(false);
    }

    let parsed = PasswordHash::new(&meta.key_phc)
        .map_err(|e| AppError::InternalServerError(format!("bad phc: {}", e)))?;

    Ok(Argon2::default()
        .verify_password(key.as_bytes(), &parsed)
        .is_ok())
}

pub async fn backfill_fragment_nt_hash_if_missing(
    fragment_hash: &str,
    key: &str,
) -> Result<bool, AppError> {
    if key.is_empty() {
        return Ok(false);
    }

    let mut meta = read_fragment_metadata(fragment_hash).await?;
    if meta
        .nt_hash_hex
        .as_ref()
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
    {
        return Ok(false);
    }

    meta.nt_hash_hex = Some(hash_key_to_nt_hash_hex(key));
    meta.updated = Some(now_utc_rfc3339());
    write_fragment_metadata(&meta).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_fragment_metadata_accepts_n1_fragment_ndx_shape() {
        let current = IndexFragmentMeta {
            engine_format_version: N1_ENGINE_FORMAT_VERSION,
            stable_fragment_id: "01KSTABLEFRAGMENT000000000".to_string(),
            state: "active".to_string(),
            committed_bytes: 0,
            reserved_bytes: 0,
            trash_bytes: 0,
            created_at_unix: 1_777_905_000,
            updated_at_unix: 1_777_905_001,
            key_phc: Some("phc".to_string()),
            nt_hash_hex: Some("00112233445566778899aabbccddeeff".to_string()),
            index_partition_count: 64,
        };
        let raw = rmp_serde::to_vec_named(&current).expect("encode current N1 fragment ndx");
        assert!(
            raw.first()
                .is_some_and(|byte| (*byte & 0xf0) == 0x80 || *byte == 0xde || *byte == 0xdf),
            "fragment metadata must be encoded as a named MessagePack map"
        );
        let decoded = decode_fragment_metadata(&raw).expect("decode current");
        assert_eq!(decoded.stable_fragment_id, current.stable_fragment_id);
        assert_eq!(decoded.engine_format_version, N1_ENGINE_FORMAT_VERSION);
        assert_eq!(decoded.key_phc, current.key_phc.unwrap());
        assert_eq!(decoded.nt_hash_hex, current.nt_hash_hex);
        assert_eq!(decoded.index_partition_count, 64);
    }

    #[test]
    fn decode_fragment_metadata_rejects_legacy_tuple_shape() {
        #[derive(Serialize)]
        struct LegacyTuple<'a>(&'a str, &'a str, &'a str, Option<&'a str>);

        let legacy = LegacyTuple("abc", "phc", "2026-01-01T00:00:00Z", None);
        let raw = rmp_serde::to_vec(&legacy).expect("encode legacy");
        let err = decode_fragment_metadata(&raw).expect_err("legacy decode should fail");
        assert!(
            matches!(err, AppError::InternalServerError(_)),
            "unexpected legacy decode error: {:?}",
            err
        );
    }

    #[test]
    fn decode_fragment_metadata_rejects_missing_stable_identity() {
        #[derive(Serialize)]
        struct MissingIdentity<'a> {
            engine_format_version: u16,
            state: &'a str,
            committed_bytes: u64,
            reserved_bytes: u64,
            trash_bytes: u64,
            created_at_unix: i64,
            updated_at_unix: i64,
            key_phc: Option<&'a str>,
            nt_hash_hex: Option<&'a str>,
            index_partition_count: u16,
        }

        let raw = rmp_serde::to_vec_named(&MissingIdentity {
            engine_format_version: N1_ENGINE_FORMAT_VERSION,
            state: "active",
            committed_bytes: 0,
            reserved_bytes: 0,
            trash_bytes: 0,
            created_at_unix: 1,
            updated_at_unix: 1,
            key_phc: Some("phc"),
            nt_hash_hex: None,
            index_partition_count: 64,
        })
        .expect("encode metadata without stable identity");
        assert!(
            decode_fragment_metadata(&raw).is_err(),
            "2.0 must not guess stable fragment identity from a pre-2.0 record"
        );
    }
}
