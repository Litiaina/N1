use super::*;
use crate::{
    core::storage_pool::Pool,
    settings::config::{ConfiguredPool, open_private},
};
use argon2::{
    Argon2, PasswordHasher, PasswordVerifier,
    password_hash::{PasswordHash, SaltString},
};
use n1_link::{PoolId, PoolLayout, ShardFormat};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
};

const REGISTRY: &str = "pool-enrollment.json";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Registry {
    schema: u32,
    pub principal: PoolId,
    pub principal_alias: String,
    pub permissions: n1_link::noa::Permissions,
    pub operator: bool,
    pub additional_fragments: Vec<crate::settings::config::Fragment>,
    env_managed: bool,
    secret_hash: String,
    signing_hash: [u8; 32],
    bootstrap_key: Option<[u8; 32]>,
    pub pools: BTreeMap<String, Enrollment>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Enrollment {
    pub config: ConfiguredPool,
    ready: bool,
}

fn random_id() -> PoolId {
    let mut bytes = [0; 16];
    OsRng.fill_bytes(&mut bytes);
    PoolId(bytes)
}
fn password_hash(secret: &str) -> Result<String> {
    Argon2::default()
        .hash_password(secret.as_bytes(), &SaltString::generate(&mut OsRng))
        .map(|hash| hash.to_string())
        .map_err(|_| Error::Invalid("cannot hash object service credential".into()))
}

impl Registry {
    pub fn load(control: &Path) -> Result<Self> {
        let bytes = crate::core::backup::fs::read_regular(&control.join(REGISTRY), 4 * 1024 * 1024)
            .map_err(crate::protocol::ops::backup_error)?;
        // The record binds permanent identities. Never recreate it by trusting
        // arbitrary replacement markers when it is missing or damaged.
        let file = open_private(&control.join(REGISTRY))?;
        drop(file);
        let registry: Self = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Corrupt("invalid pool enrollment registry".into()))?;
        if registry.schema != 2 || PasswordHash::new(&registry.secret_hash).is_err() {
            return Err(Error::Invalid(
                "unsupported pool enrollment registry".into(),
            ));
        }
        registry.verify_signing(control)?;
        Ok(registry)
    }
    fn verify_signing(&self, control: &Path) -> Result<()> {
        match open_private(&control.join("link.signing-key")) {
            Ok(mut file) => {
                use std::io::Read;
                if file.metadata()?.len() != 32 {
                    return Err(Error::Corrupt(
                        "registered signing key has the wrong length".into(),
                    ));
                }
                let mut bytes = [0; 32];
                file.read_exact(&mut bytes)?;
                if <[u8; 32]>::from(Sha256::digest(bytes)) != self.signing_hash {
                    return Err(Error::Corrupt("LINK signing key differs from its enrolled identity; restore the original key".into()));
                }
                Ok(())
            }
            Err(Error::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound && self.bootstrap_key.is_some() =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
    pub fn principal_hash(&self, secret: &str) -> Result<String> {
        if !self.env_managed {
            return Ok(self.secret_hash.clone());
        }
        let hash = PasswordHash::new(&self.secret_hash)
            .map_err(|_| Error::Corrupt("invalid registered credential".into()))?;
        if Argon2::default()
            .verify_password(secret.as_bytes(), &hash)
            .is_ok()
        {
            Ok(self.secret_hash.clone())
        } else {
            password_hash(secret)
        }
    }
    pub fn validate(&self, settings: &N1Settings) -> Result<()> {
        self.validate_existing(settings)?;
        if settings.pools.len() != self.pools.len() || self.pools.values().any(|pool| !pool.ready) {
            return Err(Error::Conflict(
                "new or interrupted pool enrollment requires N1 startup or init with this INI"
                    .into(),
            ));
        }
        Ok(())
    }
    fn validate_existing(&self, settings: &N1Settings) -> Result<()> {
        for (name, enrolled) in &self.pools {
            let wanted=settings.pools.iter().find(|pool|&pool.name==name).ok_or_else(||Error::Conflict(format!("enrolled pool {name} cannot be removed from configuration; seal it to stop allocation")))?;
            let layout = &enrolled.config.layout;
            if wanted.generation != layout.generation
                || wanted.data_shards != layout.data_shards
                || wanted.parity_shards != layout.parity_shards
            {
                return Err(Error::Conflict(format!(
                    "pool {name} identity and EC geometry are immutable; add a new complete pool"
                )));
            }
            if !enrolled.ready && wanted.roots != enrolled.config.roots {
                return Err(Error::Conflict(format!(
                    "resume pool {name} enrollment using its recorded member paths"
                )));
            }
        }
        Ok(())
    }
    fn save(&self, control: &Path) -> Result<()> {
        let bytes =
            serde_json::to_vec_pretty(self).map_err(|error| Error::Invalid(error.to_string()))?;
        atomic_replace(&control.join(REGISTRY), &bytes)
    }
}

pub(super) fn control_directory(path: &Path, create: bool) -> Result<()> {
    if create {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)?;
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::Invalid(
            "N1 control path must be a real owner-only directory (chmod 700)".into(),
        ));
    }
    Ok(())
}

pub(super) fn prepare(settings: &N1Settings, env: &BTreeMap<String, String>) -> Result<Registry> {
    let control = &settings.control_dir;
    let registry_path = control.join(REGISTRY);
    let mut registry = match fs::symlink_metadata(&registry_path) {
        Ok(_) => Registry::load(control)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            for entry in fs::read_dir(control)? {
                let name = entry?.file_name();
                if name != ".n1-instance.lock"
                    && name != "link.signing-key"
                    && !name.to_string_lossy().starts_with(".enrollment-")
                {
                    return Err(Error::Conflict("control directory contains unrecognized state without its enrollment registry; restore its original registry".into()));
                }
            }
            if control.join("link.signing-key").symlink_metadata().is_ok() {
                return Err(Error::Conflict("LINK signing state exists without its enrollment registry; restore its original registry".into()));
            }
            let secret = env
                .get("AUTH_KEY")
                .ok_or_else(|| Error::Invalid("AUTH_KEY is required".into()))?;
            let mut key = [0; 32];
            OsRng.fill_bytes(&mut key);
            Registry {
                schema: 2,
                principal: random_id(),
                principal_alias: "root".into(),
                permissions: n1_link::noa::Permissions::all(),
                operator: true,
                additional_fragments: Vec::new(),
                env_managed: true,
                secret_hash: password_hash(secret)?,
                signing_hash: Sha256::digest(key).into(),
                bootstrap_key: Some(key),
                pools: BTreeMap::new(),
            }
        }
        Err(error) => return Err(error.into()),
    };
    registry.validate_existing(settings)?;
    for wanted in &settings.pools {
        if !registry.pools.contains_key(&wanted.name) {
            let mut devices = std::collections::HashSet::new();
            for root in &wanted.roots {
                let metadata = fs::symlink_metadata(root)?;
                if !metadata.is_dir()
                    || !devices.insert(metadata.dev())
                    || fs::read_dir(root)?.next().is_some()
                {
                    return Err(Error::Invalid("new pool roots must be existing empty real directories on independent filesystems".into()));
                }
            }
        }
    }
    // Confirm all already registered roots before preparing new capacity. Only
    // explicitly changed paths may activate a copied/rebuilt immutable member.
    for wanted in &settings.pools {
        if let Some(enrolled) = registry.pools.get(&wanted.name).filter(|pool| pool.ready) {
            let pool = Pool::open(enrolled.config.layout.clone(), &wanted.roots)?;
            crate::core::root_identity::verify_pool(&pool, &enrolled.config.members)?;
            for (slot, (old, new)) in enrolled.config.roots.iter().zip(&wanted.roots).enumerate() {
                if old != new && pool.members[slot].is_none() {
                    return Err(Error::Conflict(
                        "changed member path must contain the verified original logical member"
                            .into(),
                    ));
                }
            }
        }
    }
    for wanted in &settings.pools {
        registry
            .pools
            .entry(wanted.name.clone())
            .or_insert_with(|| Enrollment {
                config: ConfiguredPool {
                    layout: PoolLayout {
                        id: random_id(),
                        generation: wanted.generation,
                        data_shards: wanted.data_shards,
                        parity_shards: wanted.parity_shards,
                        block_bytes: settings.upload.stripe_block_size,
                        placement_version: 1,
                        shard_format: ShardFormat::Crc32cSha256V2,
                    },
                    members: (0..wanted.roots.len()).map(|_| random_id()).collect(),
                    roots: wanted.roots.clone(),
                    sealed: wanted.sealed,
                },
                ready: false,
            });
    }
    super::mapping::config(settings, env, &registry)?;
    let signing = control.join("link.signing-key");
    registry.verify_signing(control)?;
    // Journal every pool identity before the first marker. A crash can resume
    // only these exact identities and may not adopt a different map from disk.
    registry.save(control)?;
    if !signing.exists() {
        let key = registry.bootstrap_key.ok_or_else(|| {
            Error::Conflict(
                "registered LINK signing key is missing; restore it, never regenerate it".into(),
            )
        })?;
        write_new(&signing, &key)?;
    }
    registry.verify_signing(control)?;
    if registry.bootstrap_key.take().is_some() {
        registry.save(control)?;
    }
    for wanted in &settings.pools {
        let enrolled = registry.pools.get(&wanted.name).expect("planned");
        if !enrolled.ready {
            let pool = Pool::resume_enrollment(
                enrolled.config.layout.clone(),
                &enrolled.config.roots,
                enrolled.config.members.clone(),
            )?;
            crate::core::root_identity::verify_pool(&pool, &enrolled.config.members)?;
            registry.pools.get_mut(&wanted.name).expect("planned").ready = true;
            registry.save(control)?;
        }
        let enrolled = registry.pools.get_mut(&wanted.name).expect("planned");
        enrolled.config.roots = wanted.roots.clone();
        enrolled.config.sealed = wanted.sealed;
    }
    registry.secret_hash =
        registry.principal_hash(env.get("AUTH_KEY").expect("validated environment"))?;
    registry.save(control)?;
    Ok(registry)
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    File::open(path.parent().expect("control path"))?.sync_all()?;
    Ok(())
}
fn atomic_replace(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_file_name(format!(".enrollment-{}", ulid::Ulid::new()));
    let result = (|| {
        write_new(&temporary, bytes)?;
        fs::rename(&temporary, path)?;
        File::open(path.parent().expect("control path"))?.sync_all()?;
        Ok(())
    })();
    let _ = fs::remove_file(temporary);
    result
}
