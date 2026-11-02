use crate::{Error, IDENTITY_BYTES, Identity, PoolLayout, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use sha2::Sha256;

const TAG_BYTES: usize = 16;
const PLAN_BYTES: usize = IDENTITY_BYTES + 8 + 8 + 8 + 1;
const OBJECT_PREFIX: &str = "N1A1.";
const UPLOAD_PREFIX: &str = "N1U1.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    Standard,
    Durable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UploadPlan {
    pub identity: Identity,
    pub total_bytes: u64,
    pub part_bytes: u64,
    pub expires_at: u64,
    pub durability: Durability,
}

impl UploadPlan {
    pub fn validate(self, pool: &PoolLayout) -> Result<()> {
        pool.validate()?;
        if !pool.accepts(self.identity) {
            return Err(Error("upload identity does not belong to this pool"));
        }
        if self.part_bytes == 0 || !self.part_bytes.is_multiple_of(pool.stripe_bytes()) {
            return Err(Error(
                "part size must be a positive multiple of the EC stripe size",
            ));
        }
        if self.total_bytes > i64::MAX as u64 || self.parts() > 16_777_216 {
            return Err(Error("object exceeds LINK v1 upload limits"));
        }
        Ok(())
    }

    pub fn parts(self) -> u64 {
        if self.part_bytes == 0 {
            0
        } else {
            self.total_bytes.div_ceil(self.part_bytes)
        }
    }

    pub fn part_length(self, part: u64) -> Result<u64> {
        if self.part_bytes == 0 || part >= self.parts() {
            return Err(Error("part index outside upload"));
        }
        Ok((self.total_bytes - part * self.part_bytes).min(self.part_bytes))
    }

    fn encode(self) -> [u8; PLAN_BYTES] {
        let mut bytes = [0; PLAN_BYTES];
        bytes[..57].copy_from_slice(&self.identity.encode());
        bytes[57..65].copy_from_slice(&self.total_bytes.to_be_bytes());
        bytes[65..73].copy_from_slice(&self.part_bytes.to_be_bytes());
        bytes[73..81].copy_from_slice(&self.expires_at.to_be_bytes());
        bytes[81] = u8::from(self.durability == Durability::Durable);
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != PLAN_BYTES || bytes[81] > 1 {
            return Err(Error("malformed upload plan"));
        }
        let u64_at =
            |start| u64::from_be_bytes(bytes[start..start + 8].try_into().expect("fixed field"));
        Ok(Self {
            identity: Identity::decode(&bytes[..57])?,
            total_bytes: u64_at(57),
            part_bytes: u64_at(65),
            expires_at: u64_at(73),
            durability: if bytes[81] == 0 {
                Durability::Standard
            } else {
                Durability::Durable
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectKey(String);
impl ObjectKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Parsing the address is not authorization. The engine verifies the tag.
    pub fn inspect(value: &str) -> Result<Identity> {
        let bytes = decode(value, OBJECT_PREFIX, IDENTITY_BYTES)?;
        Identity::decode(&bytes[..IDENTITY_BYTES])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadTicket(String);
impl UploadTicket {
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn inspect(value: &str) -> Result<UploadPlan> {
        let bytes = decode(value, UPLOAD_PREFIX, PLAN_BYTES)?;
        UploadPlan::decode(&bytes[..PLAN_BYTES])
    }
}

/// A service signing secret; never a per-object record. Fragment scope enters
/// authentication only, so permission checks need no object ownership catalogue.
/// No Debug implementation: logs must not expose the signing secret.
pub struct KeySigner([u8; 32]);
impl KeySigner {
    pub fn new(secret: [u8; 32]) -> Self {
        Self(secret)
    }

    fn mac(&self, domain: &[u8], scope: &[u8; 16], bytes: &[u8]) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).expect("HMAC accepts 32-byte keys");
        mac.update(domain);
        mac.update(scope);
        mac.update(bytes);
        mac
    }

    fn encode(&self, prefix: &str, domain: &[u8], scope: &[u8; 16], body: &[u8]) -> String {
        let tag = self.mac(domain, scope, body).finalize().into_bytes();
        let mut bytes = Vec::with_capacity(body.len() + TAG_BYTES);
        bytes.extend_from_slice(body);
        bytes.extend_from_slice(&tag[..TAG_BYTES]);
        format!("{prefix}{}", URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn object_key(&self, scope: &[u8; 16], identity: Identity) -> ObjectKey {
        ObjectKey(self.encode(
            OBJECT_PREFIX,
            b"Litiaina/LINK/object-key/v1\0",
            scope,
            &identity.encode(),
        ))
    }

    pub fn upload_ticket(&self, scope: &[u8; 16], plan: UploadPlan) -> UploadTicket {
        UploadTicket(self.encode(
            UPLOAD_PREFIX,
            b"Litiaina/LINK/upload-ticket/v1\0",
            scope,
            &plan.encode(),
        ))
    }

    pub fn verify_object(&self, scope: &[u8; 16], key: &str) -> Result<Identity> {
        let bytes = decode(key, OBJECT_PREFIX, IDENTITY_BYTES)?;
        self.verify(
            b"Litiaina/LINK/object-key/v1\0",
            scope,
            &bytes,
            IDENTITY_BYTES,
        )?;
        Identity::decode(&bytes[..IDENTITY_BYTES])
    }

    pub fn verify_upload(&self, scope: &[u8; 16], ticket: &str) -> Result<UploadPlan> {
        let bytes = decode(ticket, UPLOAD_PREFIX, PLAN_BYTES)?;
        self.verify(
            b"Litiaina/LINK/upload-ticket/v1\0",
            scope,
            &bytes,
            PLAN_BYTES,
        )?;
        UploadPlan::decode(&bytes[..PLAN_BYTES])
    }

    fn verify(&self, domain: &[u8], scope: &[u8; 16], bytes: &[u8], length: usize) -> Result<()> {
        self.mac(domain, scope, &bytes[..length])
            .verify_truncated_left(&bytes[length..])
            .map_err(|_| Error("LINK key or fragment authorization is invalid"))
    }
}

fn decode(value: &str, prefix: &str, body_bytes: usize) -> Result<Vec<u8>> {
    // Bound allocation before decoding any untrusted input.
    let expected =
        (body_bytes + TAG_BYTES).div_ceil(3) * 4 - (3 - (body_bytes + TAG_BYTES) % 3) % 3;
    let encoded = value
        .strip_prefix(prefix)
        .ok_or(Error("wrong LINK key type or version"))?;
    if encoded.len() != expected {
        return Err(Error("incorrect LINK key length"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| Error("noncanonical LINK base64url"))?;
    if bytes.len() != body_bytes + TAG_BYTES || URL_SAFE_NO_PAD.encode(&bytes) != encoded {
        return Err(Error("noncanonical LINK key encoding"));
    }
    Ok(bytes)
}
