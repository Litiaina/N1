use crate::enums::app_error_enum::AppError;
use crate::n1_core::raw_index;

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use chrono::{SecondsFormat, Utc};
use md4::{Digest as Md4Digest, Md4};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentMetadata {
    pub fragment: String,
    pub key_phc: String,
    #[serde(default)]
    pub nt_hash_hex: Option<String>,
    pub created: String,
    pub updated: Option<String>,
}

fn now_utc_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
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
    raw_index::read_fragment_metadata(fragment_hash)
        .await?
        .ok_or(AppError::NotFound)
}

pub async fn write_fragment_metadata(meta: &FragmentMetadata) -> Result<(), AppError> {
    raw_index::upsert_fragment_metadata(meta).await
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
