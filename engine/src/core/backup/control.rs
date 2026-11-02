//! Service identity travels as one bounded authenticated backup block. Its
//! plaintext never appears in a snapshot descriptor, job response or report.
use super::{
    Result,
    format::{BLOCK_BYTES, Snapshot},
    invalid,
    repository::Repository,
};
use crate::{
    core::{Error, Store},
    settings::config::{Config, Tls},
};
use serde::{Deserialize, Serialize};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceControl {
    config: Config,
    signing_key: [u8; 32],
    tls_certificate: Option<Vec<u8>>,
    tls_private_key: Option<Vec<u8>>,
}
impl Store {
    pub fn configure_recovery_control(
        &self,
        config: Config,
        signing_key: [u8; 32],
    ) -> crate::core::Result<()> {
        let identity = n1_link::Identity {
            pool: self.pools[0].layout.id,
            generation: self.pools[0].layout.generation,
            token: [0; 32],
        };
        if self.signer.object_key(&[0; 16], identity).as_str()
            != n1_link::KeySigner::new(signing_key)
                .object_key(&[0; 16], identity)
                .as_str()
        {
            return Err(Error::Invalid(
                "recovery signing key differs from the running engine identity".into(),
            ));
        }
        let control = ServiceControl::capture(config, signing_key)
            .map_err(|e| Error::Invalid(e.to_string()))?;
        if control.config.pools.len() != self.pools.len()
            || control
                .config
                .pools
                .iter()
                .zip(&self.pools)
                .any(|(a, b)| a.layout != b.layout)
        {
            return Err(Error::Invalid(
                "recovery configuration does not match open pools".into(),
            ));
        }
        for (configured, opened) in control.config.pools.iter().zip(&self.pools) {
            crate::core::root_identity::verify_pool(opened, &configured.members)?;
        }
        self.recovery_control
            .set(control)
            .map_err(|_| Error::Conflict("service recovery identity already configured".into()))
    }
}
impl ServiceControl {
    fn capture(config: Config, signing_key: [u8; 32]) -> Result<Self> {
        config.validate()?;
        let (tls_certificate, tls_private_key) = match &config.tls {
            Some(tls) => {
                if std::fs::symlink_metadata(&tls.private_key)?
                    .permissions()
                    .mode()
                    & 0o077
                    != 0
                {
                    return Err(invalid("TLS recovery private key must be owner-only"));
                }
                (
                    Some(super::fs::read_regular(&tls.certificate, 256 * 1024)?),
                    Some(super::fs::read_regular(&tls.private_key, 256 * 1024)?),
                )
            }
            None => (None, None),
        };
        let control = Self {
            config,
            signing_key,
            tls_certificate,
            tls_private_key,
        };
        if serde_json::to_vec(&control)?.len() > BLOCK_BYTES {
            return Err(invalid("service recovery control exceeds 4 MiB"));
        }
        Ok(control)
    }
    fn validate(&self, snapshot: &Snapshot) -> Result<()> {
        self.config.validate()?;
        if self.config.pools.len() != snapshot.pools.len()
            || self
                .config
                .pools
                .iter()
                .zip(&snapshot.pools)
                .any(|(a, b)| a.layout != b.layout || a.members != b.members)
            || self.config.tls.is_some()
                != (self.tls_certificate.is_some() && self.tls_private_key.is_some())
            || self
                .tls_certificate
                .as_ref()
                .is_some_and(|v| v.len() > 256 * 1024)
            || self
                .tls_private_key
                .as_ref()
                .is_some_and(|v| v.len() > 256 * 1024)
        {
            return Err(invalid(
                "service recovery identity/configuration differs from snapshot topology",
            ));
        }
        Ok(())
    }
}
pub(crate) fn decode(bytes: &[u8], snapshot: &Snapshot) -> Result<ServiceControl> {
    if bytes.len() > BLOCK_BYTES {
        return Err(invalid("service recovery block exceeds format limit"));
    }
    let control: ServiceControl = serde_json::from_slice(bytes)?;
    control.validate(snapshot)?;
    Ok(control)
}

/// Publish secrets first and configuration last. Existing files are accepted
/// only when their private, no-follow contents are identical, making a retry
/// safe without replacing an operator's edited configuration.
pub fn restore(
    repo: &Repository,
    snapshot: &Snapshot,
    output: &Path,
    roots: &[PathBuf],
) -> Result<()> {
    if !output.is_absolute()
        || !output.parent().is_some_and(|p| p.is_dir())
        || roots.len() != snapshot.roots.len()
    {
        return Err(invalid(
            "service recovery requires an absolute output in an existing directory and every target root",
        ));
    }
    super::format::validate_snapshot(snapshot)?;
    let hash = snapshot
        .service_control
        .as_ref()
        .ok_or_else(|| invalid("snapshot has no service recovery identity"))?;
    let mut control = decode(&repo.get(hash)?, snapshot)?;
    let canonical = roots
        .iter()
        .map(std::fs::canonicalize)
        .collect::<std::io::Result<Vec<_>>>()?;
    if canonical
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len()
        != roots.len()
    {
        return Err(invalid("duplicate service recovery member roots"));
    }
    // Use the same pinned directory leases as shard restore. Configuration
    // must not be published while an engine is already serving these roots.
    let leases = canonical
        .iter()
        .map(|path| super::restore::Target::open(path))
        .collect::<Result<Vec<_>>>()?;
    let output_parent = output.parent().expect("validated output").canonicalize()?;
    for root in &canonical {
        if output_parent.starts_with(root) {
            return Err(invalid(
                "service recovery files must be outside member roots",
            ));
        }
    }
    for (ordinal, root) in canonical.iter().enumerate() {
        leases[ordinal].check()?;
        let marker: crate::core::storage_pool::MemberMarker = serde_json::from_slice(
            &super::fs::read_regular(&root.join(".n1-arc-pool.json"), 65536)?,
        )?;
        if marker != snapshot.marker(ordinal)? {
            return Err(invalid(
                "service recovery target is not the snapshot's restored member",
            ));
        }
    }
    for (root, path) in snapshot.roots.iter().zip(canonical) {
        control.config.pools[root.pool].roots[root.slot] = path;
    }
    control.config.signing_key_file = output.with_extension("signing-key");
    control.config.state_dir = None; // A recovered service starts with fresh login sessions.
    private_exact(&control.config.signing_key_file, &control.signing_key)?;
    if let (Some(certificate), Some(private_key)) =
        (&control.tls_certificate, &control.tls_private_key)
    {
        let tls = Tls {
            certificate: output.with_extension("tls-certificate.pem"),
            private_key: output.with_extension("tls-private-key.pem"),
        };
        private_exact(&tls.certificate, certificate)?;
        private_exact(&tls.private_key, private_key)?;
        control.config.tls = Some(tls);
    }
    control.config.validate()?;
    for lease in &leases {
        lease.check()?;
    }
    private_exact(output, &serde_json::to_vec_pretty(&control.config)?)
}
fn private_exact(path: &Path, bytes: &[u8]) -> Result<()> {
    match super::fs::read_regular(path, 4 * 1024 * 1024) {
        Ok(existing) => {
            if std::fs::symlink_metadata(path)?.permissions().mode() & 0o077 != 0
                || existing != bytes
            {
                return Err(invalid(
                    "refusing to replace a different or nonprivate service recovery file",
                ));
            }
            std::fs::File::open(path)?.sync_all()?;
            super::fs::sync_parent(path)
        }
        Err(e)
            if e.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            super::fs::atomic_new(path, bytes)
        }
        Err(e) => Err(e),
    }
}
