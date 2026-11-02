//! Retained signed-read capability, bound to an immutable LINK key and range.
//! Grants are client-held and have no object-store record.
use super::signing;
use crate::core::{Error, Result};
use n1_link::{PoolId, noa::ReadGrant};

#[derive(Clone)]
pub struct SignedReads {
    secret: [u8; 32],
}
impl SignedReads {
    pub fn new(secret: &[u8; 32]) -> Self {
        use hmac::{Hmac, Mac};
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret).expect("valid HMAC key");
        mac.update(b"Litiaina/N1/LINK/signed-read/v1\0");
        Self {
            secret: mac.finalize().into_bytes().into(),
        }
    }
    pub fn issue(
        &self,
        fragment: PoolId,
        object_key: String,
        offset: u64,
        length: Option<u64>,
        expires_at: i64,
    ) -> ReadGrant {
        let mut grant = ReadGrant {
            fragment,
            object_key,
            offset,
            length,
            expires_at,
            signature: String::new(),
        };
        grant.signature = signing::sign_with_secret(&self.secret, &payload(&grant));
        grant
    }
    pub fn verify(&self, grant: &ReadGrant) -> Result<()> {
        if grant.object_key.len() > 1024
            || grant.signature.len() != 64
            || grant.expires_at <= signing::now_unix()?
        {
            return Err(Error::Unauthorized);
        }
        signing::verify_with_secret(&self.secret, &payload(grant), &grant.signature)?;
        Ok(())
    }
}
fn payload(grant: &ReadGrant) -> Vec<u8> {
    // Exact LINK key and fragment encodings exclude newline. Numeric fields
    // are canonical decimals; absent length is distinct from an explicit zero.
    format!(
        "N1-LINK-read-v1\n{}\n{}\n{}\n{}\n{}",
        hex::encode(grant.fragment.0),
        grant.object_key,
        grant.expires_at,
        grant.offset,
        grant.length.map(|v| v.to_string()).unwrap_or_default()
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signed_reads_bind_fragment_key_range_expiry_and_signing_key() {
        let reads = SignedReads::new(&[8; 32]);
        let grant = reads.issue(
            PoolId([9; 16]),
            "N1A1.fixture".into(),
            5,
            Some(9),
            signing::now_unix().unwrap() + 60,
        );
        reads.verify(&grant).unwrap();
        for field in 0..6 {
            let mut changed = grant.clone();
            match field {
                0 => changed.fragment = PoolId([10; 16]),
                1 => changed.object_key.push('A'),
                2 => changed.offset += 1,
                3 => changed.length = Some(8),
                4 => changed.expires_at += 1,
                _ => changed.length = None,
            }
            assert!(reads.verify(&changed).is_err());
        }
        assert!(SignedReads::new(&[7; 32]).verify(&grant).is_err());
        assert!(
            reads
                .verify(&reads.issue(
                    grant.fragment,
                    grant.object_key,
                    0,
                    None,
                    signing::now_unix().unwrap()
                ))
                .is_err()
        );
    }
}
