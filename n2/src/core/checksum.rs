pub const SHA256_DIGEST_BYTES: usize = 32;
pub const SHA256_HEX_BYTES: usize = SHA256_DIGEST_BYTES * 2;
pub type Sha256Digest = [u8; SHA256_DIGEST_BYTES];

#[derive(Clone)]
pub struct Sha256Stream(ring::digest::Context);

impl Sha256Stream {
    #[inline]
    pub fn new() -> Self {
        Self(ring::digest::Context::new(&ring::digest::SHA256))
    }

    #[inline]
    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    #[inline]
    pub fn finalize(self) -> Sha256Digest {
        self.0
            .finish()
            .as_ref()
            .try_into()
            .expect("SHA-256 digest is always 32 bytes")
    }
}

impl Default for Sha256Stream {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
pub fn sha256_digest(bytes: &[u8]) -> Sha256Digest {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .expect("SHA-256 digest is always 32 bytes")
}

#[inline]
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(sha256_digest(bytes))
}

#[inline]
pub fn format_sha256(digest: &Sha256Digest) -> String {
    hex::encode(digest)
}

pub fn parse_sha256_hex(value: &str) -> Option<Sha256Digest> {
    let value = value.trim();
    if value.len() != SHA256_HEX_BYTES || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut digest = [0u8; SHA256_DIGEST_BYTES];
    hex::decode_to_slice(value, &mut digest).ok()?;
    Some(digest)
}

#[inline]
pub fn sha256_matches(bytes: &[u8], expected: &Sha256Digest) -> bool {
    sha256_digest(bytes) == *expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_helpers_use_canonical_256_bit_digest() {
        let digest = sha256_digest(b"abc");
        assert_eq!(
            format_sha256(&digest),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(parse_sha256_hex(&format_sha256(&digest)), Some(digest));
        assert!(parse_sha256_hex("deadbeef").is_none());
    }
}
