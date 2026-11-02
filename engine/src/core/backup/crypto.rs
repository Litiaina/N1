use super::{Result, invalid};
use rand::{RngCore, rngs::OsRng};
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::hkdf;
use sha2::{Digest, Sha256};
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

pub fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn create_key(path: &Path) -> Result<()> {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    super::fs::sync_parent(path)?;
    Ok(())
}

pub struct Key {
    key: hkdf::Prk,
    pub id: String,
}
impl Key {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = super::fs::read_regular(path, 32)?;
        if bytes.len() != 32 || std::fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
            return Err(invalid(
                "backup key must be exactly 32 random bytes and owner-only (chmod 600)",
            ));
        }
        Ok(Self {
            id: digest(&bytes),
            key: hkdf::Salt::new(hkdf::HKDF_SHA256, b"Litiaina/LINK/backup/v1").extract(&bytes),
        })
    }
    fn record_key(&self, label: &str) -> Result<LessSafeKey> {
        // Isolate each content hash/snapshot identity into its own AEAD key.
        // Random nonces no longer share one collision budget for an entire
        // multi-petabyte repository and its lifetime of incremental captures.
        struct Length;
        impl hkdf::KeyType for Length {
            fn len(&self) -> usize {
                32
            }
        }
        let mut bytes = [0u8; 32];
        self.key
            .expand(&[label.as_bytes()], Length)
            .map_err(|_| invalid("backup key derivation failed"))?
            .fill(&mut bytes)
            .map_err(|_| invalid("backup key derivation failed"))?;
        Ok(LessSafeKey::new(
            UnboundKey::new(&aead::AES_256_GCM, &bytes)
                .map_err(|_| invalid("invalid backup key"))?,
        ))
    }
    pub fn seal(&self, label: &str, bytes: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; 12];
        OsRng.fill_bytes(&mut nonce);
        let mut result = bytes.to_vec();
        self.record_key(label)?
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(label),
                &mut result,
            )
            .map_err(|_| invalid("backup encryption failed"))?;
        result.extend_from_slice(&nonce);
        Ok(result)
    }
    pub fn open(&self, label: &str, mut bytes: Vec<u8>) -> Result<Vec<u8>> {
        if bytes.len() < 28 {
            return Err(invalid("truncated encrypted backup block"));
        }
        let nonce: [u8; 12] = bytes[bytes.len() - 12..].try_into()?;
        bytes.truncate(bytes.len() - 12);
        let length = self
            .record_key(label)?
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(label),
                &mut bytes,
            )
            .map_err(|_| invalid("backup authentication failed: wrong key or corrupt data"))?
            .len();
        bytes.truncate(length);
        Ok(bytes)
    }
}
