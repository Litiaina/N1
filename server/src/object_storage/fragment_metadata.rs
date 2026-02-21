use crate::object_storage::disk_manager::ObjectPathManager;
use crate::{enums::app_error_enum::AppError, object_storage::constants::FRAGMENT_METADATA_FILE};

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use chrono::{SecondsFormat, Utc};
use md4::{Digest as Md4Digest, Md4};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use tokio::fs as tokio_fs;
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentMetadata {
    pub fragment: String,
    pub key_phc: String,
    #[serde(default)]
    pub nt_hash_hex: Option<String>,
    pub created: String,
    pub updated: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LegacyFragmentMetadata {
    fragment: String,
    key_phc: String,
    created: String,
    updated: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LegacyFragmentMetadataTuple(String, String, String, Option<String>);

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FragmentMetadataCompat {
    Current(FragmentMetadata),
    LegacyNamed(LegacyFragmentMetadata),
    LegacyTuple(LegacyFragmentMetadataTuple),
}

fn now_utc_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn decode_fragment_metadata(raw: &[u8]) -> Result<FragmentMetadata, AppError> {
    let decoded = rmp_serde::from_slice::<FragmentMetadataCompat>(raw)
        .map_err(|e| AppError::InternalServerError(format!("decode msgpack: {}", e)))?;

    Ok(match decoded {
        FragmentMetadataCompat::Current(meta) => meta,
        FragmentMetadataCompat::LegacyNamed(legacy) => FragmentMetadata {
            fragment: legacy.fragment,
            key_phc: legacy.key_phc,
            nt_hash_hex: None,
            created: legacy.created,
            updated: legacy.updated,
        },
        FragmentMetadataCompat::LegacyTuple(legacy) => FragmentMetadata {
            fragment: legacy.0,
            key_phc: legacy.1,
            nt_hash_hex: None,
            created: legacy.2,
            updated: legacy.3,
        },
    })
}

fn fragment_meta_path(root: &Path, fragment_hash: &str) -> PathBuf {
    root.join(fragment_hash).join(FRAGMENT_METADATA_FILE)
}

async fn sync_dir(dir: &Path) -> Result<(), AppError> {
    let f = tokio_fs::File::open(dir)
        .await
        .map_err(|e| AppError::Io(e, format!("open dir {}", dir.display())))?;
    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync dir {}", dir.display())))?;
    Ok(())
}

async fn durable_atomic_write(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("missing parent for {}", path.display()))
    })?;

    tokio_fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {}", parent.display())))?;

    let file_name = path.file_name().and_then(|v| v.to_str()).unwrap_or("file");

    let tmp = parent.join(format!(".{}.tmp.{}", file_name, Ulid::new().to_string()));

    let mut f = tokio_fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("create tmp {}", tmp.display())))?;

    f.write_all(bytes)
        .await
        .map_err(|e| AppError::Io(e, format!("write tmp {}", tmp.display())))?;

    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync tmp {}", tmp.display())))?;

    drop(f);

    tokio_fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))?;

    sync_dir(parent).await?;
    Ok(())
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
        fragment: fragment_hash.to_string(),
        key_phc: hash_key_to_phc(key)?,
        nt_hash_hex: Some(hash_key_to_nt_hash_hex(key)),
        created: now_utc_rfc3339(),
        updated: None,
    };

    write_fragment_metadata(&meta).await?;
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

        if meta.fragment == fragment_hash {
            return Ok(meta);
        }
    }

    Err(AppError::NotFound)
}

pub async fn write_fragment_metadata(meta: &FragmentMetadata) -> Result<(), AppError> {
    let pm = ObjectPathManager::new();
    let roots = pm.get_all_index_roots();

    let bytes = rmp_serde::to_vec(meta)
        .map_err(|e| AppError::InternalServerError(format!("encode msgpack: {}", e)))?;

    let mut any_ok = false;

    for root in roots {
        let p = fragment_meta_path(&root, &meta.fragment);
        if durable_atomic_write(&p, &bytes).await.is_ok() {
            any_ok = true;
        }
    }

    if !any_ok {
        return Err(AppError::InternalServerError(
            "failed to write fragment metadata to any index root".into(),
        ));
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
            fragment: fragment_hash.to_string(),
            key_phc: String::new(),
            nt_hash_hex: None,
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

    #[derive(Serialize)]
    struct LegacyTuple<'a>(&'a str, &'a str, &'a str, Option<&'a str>);

    #[test]
    fn decode_fragment_metadata_accepts_current_shape() {
        let current = FragmentMetadata {
            fragment: "abc".to_string(),
            key_phc: "phc".to_string(),
            nt_hash_hex: Some("00112233445566778899aabbccddeeff".to_string()),
            created: "2026-01-01T00:00:00Z".to_string(),
            updated: None,
        };
        let raw = rmp_serde::to_vec(&current).expect("encode current");
        let decoded = decode_fragment_metadata(&raw).expect("decode current");
        assert_eq!(decoded.fragment, current.fragment);
        assert_eq!(decoded.key_phc, current.key_phc);
        assert_eq!(decoded.nt_hash_hex, current.nt_hash_hex);
    }

    #[test]
    fn decode_fragment_metadata_accepts_legacy_tuple_shape() {
        let legacy = LegacyTuple("abc", "phc", "2026-01-01T00:00:00Z", None);
        let raw = rmp_serde::to_vec(&legacy).expect("encode legacy");
        let decoded = decode_fragment_metadata(&raw).expect("decode legacy");
        assert_eq!(decoded.fragment, "abc");
        assert_eq!(decoded.key_phc, "phc");
        assert_eq!(decoded.nt_hash_hex, None);
    }
}
