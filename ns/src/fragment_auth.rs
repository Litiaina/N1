use crate::enums::app_error_enum::AppError;
use hmac::{Hmac, Mac};
use md5::Md5;
use serde::{Deserialize, Serialize};
use sha2::{Digest as ShaDigest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs;

const FRAGMENT_METADATA_FILE: &str = "fragment.metadata.msgpack";

type HmacMd5 = Hmac<Md5>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FragmentMetadata {
    fragment: String,
    key_phc: String,
    #[serde(default)]
    nt_hash_hex: Option<String>,
    index_partition_count: u16,
    created: String,
    updated: Option<String>,
}

pub async fn verify_fragment_ntlmv2_auth(
    index_roots: &[PathBuf],
    principal: &str,
    domain: &str,
    challenge: &[u8; 8],
    nt_response: &[u8],
) -> Result<Option<([u8; 16], String)>, AppError> {
    if nt_response.len() < 16 {
        return Ok(None);
    }

    let normalized_principal = normalize_fragment_identifier(principal);
    if normalized_principal.is_empty() {
        return Ok(None);
    }
    let Some(fragment_hash) = fragment_identifier_to_hash(normalized_principal.as_str()) else {
        return Ok(None);
    };
    verify_fragment_ntlmv2_auth_for_fragment(
        index_roots,
        normalized_principal.as_str(),
        domain,
        challenge,
        nt_response,
        fragment_hash.as_str(),
    )
    .await
}

pub async fn verify_fragment_ntlmv2_auth_for_fragment(
    index_roots: &[PathBuf],
    principal: &str,
    domain: &str,
    challenge: &[u8; 8],
    nt_response: &[u8],
    fragment_hash: &str,
) -> Result<Option<([u8; 16], String)>, AppError> {
    if nt_response.len() < 16 {
        return Ok(None);
    }

    let normalized_principal = normalize_fragment_identifier(principal);
    if normalized_principal.is_empty() {
        return Ok(None);
    }
    let fragment_hash = fragment_hash.trim().to_ascii_lowercase();
    if !is_fragment_hash_identifier(fragment_hash.as_str()) {
        return Ok(None);
    }
    let Some(metadata) = read_fragment_metadata(index_roots, fragment_hash.as_str()).await? else {
        return Ok(None);
    };
    let Some(nt_hash_hex) = metadata.nt_hash_hex.as_deref() else {
        return Ok(None);
    };
    let Some(signing_key) = verify_ntlmv2_with_nt_hash(
        normalized_principal.as_str(),
        nt_hash_hex,
        challenge,
        normalize_fragment_identifier(domain).as_str(),
        nt_response,
    ) else {
        return Ok(None);
    };
    Ok(Some((signing_key, fragment_hash)))
}

async fn read_fragment_metadata(
    index_roots: &[PathBuf],
    fragment_hash: &str,
) -> Result<Option<FragmentMetadata>, AppError> {
    for root in index_roots {
        let path = fragment_metadata_path(root.as_path(), fragment_hash);
        match fs::read(&path).await {
            Ok(raw) => {
                let metadata = rmp_serde::from_slice::<FragmentMetadata>(&raw).map_err(|err| {
                    AppError::InternalServerError(format!(
                        "decode fragment metadata {}: {}",
                        path.display(),
                        err
                    ))
                })?;
                if metadata.fragment == fragment_hash {
                    return Ok(Some(metadata));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(AppError::Io(err, format!("read {}", path.display())));
            }
        }
    }
    Ok(None)
}

fn fragment_metadata_path(root: &Path, fragment_hash: &str) -> PathBuf {
    root.join(fragment_hash).join(FRAGMENT_METADATA_FILE)
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

fn parse_nt_hash_hex(value: &str) -> Option<[u8; 16]> {
    let raw = hex::decode(value.trim()).ok()?;
    if raw.len() != 16 {
        return None;
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(raw.as_slice());
    Some(out)
}

fn utf16le_bytes(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len() * 2);
    for codepoint in text.encode_utf16() {
        out.extend_from_slice(&codepoint.to_le_bytes());
    }
    out
}

fn hmac_md5(key: &[u8], data: &[u8]) -> Option<[u8; 16]> {
    let mut mac = HmacMd5::new_from_slice(key).ok()?;
    mac.update(data);
    let digest = mac.finalize().into_bytes();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest);
    Some(out)
}

fn verify_ntlmv2_with_nt_hash(
    principal: &str,
    nt_hash_hex: &str,
    challenge: &[u8; 8],
    domain: &str,
    nt_response: &[u8],
) -> Option<[u8; 16]> {
    let stored_nt_hash = parse_nt_hash_hex(nt_hash_hex)?;
    let nt_proof = nt_response.get(..16)?;
    let blob = nt_response.get(16..)?;
    if blob.is_empty() {
        return None;
    }

    let identity = format!("{}{}", principal.to_uppercase(), domain);
    let ntlm_v2_hash = hmac_md5(&stored_nt_hash, &utf16le_bytes(identity.as_str()))?;
    let mut proof_input = Vec::with_capacity(8 + blob.len());
    proof_input.extend_from_slice(challenge);
    proof_input.extend_from_slice(blob);
    let expected_proof = hmac_md5(&ntlm_v2_hash, &proof_input)?;
    if expected_proof.as_slice() != nt_proof {
        return None;
    }
    hmac_md5(&ntlm_v2_hash, nt_proof)
}

#[cfg(test)]
mod tests {
    use super::*;
    use md4::{Digest as Md4Digest, Md4};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(test_name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "litiaina-ns-fragment-auth-{}-{}-{}",
            test_name,
            std::process::id(),
            unique
        ))
    }

    fn nt_hash_hex_for_secret(secret: &str) -> String {
        let mut hasher = Md4::new();
        hasher.update(utf16le_bytes(secret));
        hex::encode(hasher.finalize())
    }

    fn ntlmv2_response_for_principal(
        principal: &str,
        domain: &str,
        secret: &str,
        challenge: &[u8; 8],
        blob: &[u8],
    ) -> Vec<u8> {
        let nt_hash = hex::decode(nt_hash_hex_for_secret(secret)).expect("nt hash decode");
        let identity = format!("{}{}", principal.to_uppercase(), domain);
        let ntlm_v2_hash =
            hmac_md5(&nt_hash, &utf16le_bytes(identity.as_str())).expect("derive ntlmv2 hash");

        let mut proof_input = Vec::with_capacity(8 + blob.len());
        proof_input.extend_from_slice(challenge);
        proof_input.extend_from_slice(blob);
        let nt_proof = hmac_md5(&ntlm_v2_hash, proof_input.as_slice()).expect("derive nt proof");

        let mut response = Vec::with_capacity(16 + blob.len());
        response.extend_from_slice(nt_proof.as_slice());
        response.extend_from_slice(blob);
        response
    }

    fn write_fragment_metadata_fixture(
        root: &Path,
        fragment_hash: &str,
        nt_hash_hex: &str,
    ) -> Result<(), AppError> {
        let dir = root.join(fragment_hash);
        std::fs::create_dir_all(&dir)
            .map_err(|err| AppError::Io(err, format!("mkdir {}", dir.display())))?;
        let metadata = FragmentMetadata {
            fragment: fragment_hash.to_string(),
            key_phc: "unused".to_string(),
            nt_hash_hex: Some(nt_hash_hex.to_string()),
            index_partition_count: 64,
            created: "2026-01-01T00:00:00Z".to_string(),
            updated: None,
        };
        let raw = rmp_serde::to_vec(&metadata)
            .map_err(|err| AppError::InternalServerError(format!("encode fixture: {}", err)))?;
        std::fs::write(dir.join(FRAGMENT_METADATA_FILE), raw)
            .map_err(|err| AppError::Io(err, format!("write {}", dir.display())))?;
        Ok(())
    }

    #[tokio::test]
    async fn verifies_fragment_name_against_local_n1_metadata() {
        let root = temp_root("name");
        let principal = "my-fragment";
        let fragment_hash = fragment_identifier_to_hash(principal).expect("fragment hash");
        let challenge = [0x11, 0x22, 0x33, 0x44, 0x10, 0x20, 0x30, 0x40];
        let blob = [
            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC, 0xDD,
        ];
        let nt_hash_hex = nt_hash_hex_for_secret("password");
        write_fragment_metadata_fixture(
            root.as_path(),
            fragment_hash.as_str(),
            nt_hash_hex.as_str(),
        )
        .expect("write fragment metadata");
        let nt_response =
            ntlmv2_response_for_principal(principal, "LITI", "password", &challenge, &blob);

        let result = verify_fragment_ntlmv2_auth(
            &[root.clone()],
            principal,
            "LITI",
            &challenge,
            nt_response.as_slice(),
        )
        .await
        .expect("verify auth")
        .expect("auth success");
        assert_eq!(result.1, fragment_hash);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn verifies_fragment_hash_username_against_local_n1_metadata() {
        let root = temp_root("hash");
        let fragment_hash = fragment_identifier_to_hash("my-fragment").expect("fragment hash");
        let challenge = [0xA1, 0xB2, 0xC3, 0xD4, 0x01, 0x02, 0x03, 0x04];
        let blob = [
            0x01, 0x01, 0x00, 0x00, 0x10, 0x32, 0x54, 0x76, 0xDE, 0xAD, 0xBE, 0xEF,
        ];
        let nt_hash_hex = nt_hash_hex_for_secret("password");
        write_fragment_metadata_fixture(
            root.as_path(),
            fragment_hash.as_str(),
            nt_hash_hex.as_str(),
        )
        .expect("write fragment metadata");
        let nt_response = ntlmv2_response_for_principal(
            fragment_hash.as_str(),
            "LITI",
            "password",
            &challenge,
            &blob,
        );

        let result = verify_fragment_ntlmv2_auth(
            &[root.clone()],
            fragment_hash.as_str(),
            "LITI",
            &challenge,
            nt_response.as_slice(),
        )
        .await
        .expect("verify auth")
        .expect("auth success");
        assert_eq!(result.1, fragment_hash);

        let _ = std::fs::remove_dir_all(root);
    }
}
