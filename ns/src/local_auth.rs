use hmac::{Hmac, Mac};
use md4::{Digest as Md4Digest, Md4};
use md5::Md5;

type HmacMd5 = Hmac<Md5>;

pub fn nt_hash_hex_for_secret(secret: &str) -> String {
    let mut hasher = Md4::new();
    hasher.update(utf16le_bytes(secret));
    hex::encode(hasher.finalize())
}

pub fn verify_ntlmv2_with_nt_hash(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nt_hash_is_stable_for_secret() {
        assert_eq!(
            nt_hash_hex_for_secret("password"),
            "8846f7eaee8fb117ad06bdd830b7586c"
        );
    }
}
