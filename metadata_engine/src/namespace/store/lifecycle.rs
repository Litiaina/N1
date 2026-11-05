//! Transactional maintenance indexes. Routine work seeks these bounded ranges;
//! it never walks the namespace or object-engine member directories.
use super::*;
mod gc;
mod migration;
use crate::fragments::CATALOG;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Target {
    pub fragment: PoolId,
    pub object: String,
    pub version: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct TrashWork {
    pub fragment: PoolId,
    pub trash: String,
    pub object: String,
    pub deleted_at: i64,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct UploadWork {
    pub target: Target,
    pub expires_at: u64,
}
fn upload_due(target: &Target, expires: u64, scoped: bool) -> Result<RecordKey> {
    let scope = if scoped {
        target.fragment.to_string()
    } else {
        "all".into()
    };
    catalog(format!(
        "upload-due/{scope}/{expires:016x}/{}/{}",
        target.fragment, target.version
    ))
}
fn upload_claim(target: &Target) -> Result<RecordKey> {
    RecordKey::new(target.fragment, format!("upload-gc/{}", target.version))
}
pub(crate) fn unindex_upload(
    tx: &mut MetadataTransaction,
    target: &Target,
    expires: u64,
) -> Result<()> {
    tx.remove(Table::Reclamation, &upload_due(target, expires, false)?)?;
    tx.remove(Table::Reclamation, &upload_due(target, expires, true)?)
}
#[derive(Clone)]
pub(crate) struct Blob {
    pub target: Target,
    pub key: String,
    pub stored: Option<StoredVersion>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct MoveWork {
    pub target: Target,
    pub source: String,
    pub upload: ObjectUpload,
    pub switched: bool,
}
fn move_key(job: &str, target: &Target) -> Result<RecordKey> {
    catalog(format!(
        "move/{job}/{}/{}/{}",
        target.fragment, target.object, target.version
    ))
}
pub(crate) fn catalog(value: impl Into<String>) -> Result<RecordKey> {
    RecordKey::new(CATALOG, value)
}
fn hash(key: &str) -> String {
    hex::encode(Sha256::digest(key.as_bytes()))
}
fn reference(target: &Target, key: &str) -> Result<RecordKey> {
    catalog(format!(
        "link-ref/{}/{}/{}/{}",
        hash(key),
        target.fragment,
        target.object,
        target.version
    ))
}
fn purging(key: &str) -> Result<RecordKey> {
    catalog(format!("link-purging/{}", hash(key)))
}
fn owner_index(target: &Target) -> Result<RecordKey> {
    RecordKey::new(
        target.fragment,
        format!("object-upload/{}/{}", target.object, target.version),
    )
}
pub(crate) fn pool_index(target: &Target, key: &str) -> Result<RecordKey> {
    let identity = n1_link::ObjectKey::inspect(key)
        .map_err(|_| Error::Incompatible("invalid stored LINK identity".into()))?;
    catalog(format!(
        "pool-ref/{}/{}/{}/{}",
        identity.pool, target.fragment, target.object, target.version
    ))
}
fn time(value: i64) -> u64 {
    (value as u64) ^ (1u64 << 63)
}
pub(crate) fn trash_index(fragment: PoolId, id: &str, deleted: i64) -> Result<RecordKey> {
    catalog(format!(
        "trash-due/all/{:016x}/{fragment}/{id}",
        time(deleted)
    ))
}
fn scoped_trash_index(work: &TrashWork) -> Result<RecordKey> {
    catalog(format!(
        "trash-due/{}/{:016x}/{}",
        work.fragment,
        time(work.deleted_at),
        work.trash
    ))
}
fn claimed_key(work: &TrashWork) -> Result<RecordKey> {
    catalog(format!("trash-work/{}/{}", work.fragment, work.trash))
}
pub(crate) fn unindex_trash(tx: &mut MetadataTransaction, work: &TrashWork) -> Result<()> {
    tx.remove(
        Table::Reclamation,
        &trash_index(work.fragment, &work.trash, work.deleted_at)?,
    )?;
    tx.remove(Table::Reclamation, &scoped_trash_index(work)?)
}
pub(crate) fn index_trash(
    tx: &mut MetadataTransaction,
    fragment: PoolId,
    id: &str,
    deleted: i64,
) -> Result<()> {
    let trash: Trash = read(
        tx,
        Table::Reclamation,
        &RecordKey::new(fragment, format!("trash/{id}"))?,
    )?
    .ok_or(Error::NotFound)?;
    let work = TrashWork {
        fragment,
        trash: id.into(),
        object: trash.entry.object_id,
        deleted_at: deleted,
    };
    if trash.restored {
        return Ok(());
    }
    put(
        tx,
        Table::Reclamation,
        &trash_index(fragment, id, deleted)?,
        &work,
    )?;
    put(tx, Table::Reclamation, &scoped_trash_index(&work)?, &work)?;
    put(
        tx,
        Table::Reclamation,
        &RecordKey::new(fragment, format!("object-trash/{}/{}", work.object, id))?,
        &work,
    )?;
    if trash.reclaiming {
        put(tx, Table::Reclamation, &claimed_key(&work)?, &work)?;
    }
    Ok(())
}
pub(crate) fn index_upload(
    tx: &mut MetadataTransaction,
    fragment: PoolId,
    object: &str,
    version: &str,
    upload: &ObjectUpload,
) -> Result<()> {
    if tx
        .get(Table::Reclamation, &purging(&upload.object_key)?)?
        .is_some()
    {
        return Err(Error::NameConflict("object purge has started".into()));
    }
    let target = Target {
        fragment,
        object: object.into(),
        version: version.into(),
    };
    put(tx, Table::Reclamation, &owner_index(&target)?, &target)?;
    let work = UploadWork {
        target: target.clone(),
        expires_at: upload.expires_at,
    };
    put(
        tx,
        Table::Reclamation,
        &upload_due(&target, upload.expires_at, false)?,
        &work,
    )?;
    put(
        tx,
        Table::Reclamation,
        &upload_due(&target, upload.expires_at, true)?,
        &work,
    )?;
    put(
        tx,
        Table::Reclamation,
        &reference(&target, &upload.object_key)?,
        &target,
    )
}
pub(crate) fn index_version(
    tx: &mut MetadataTransaction,
    fragment: PoolId,
    stored: &StoredVersion,
) -> Result<()> {
    let target = Target {
        fragment,
        object: stored.metadata.object_id.clone(),
        version: stored.metadata.version_id.clone(),
    };
    put(
        tx,
        Table::Reclamation,
        &pool_index(&target, &stored.object_key)?,
        &target,
    )
}

impl Namespace {
    /// One-time database-only index construction before server admission.
    /// A crashed build repeats idempotently; the marker is committed last.
    pub(crate) fn ensure_maintenance_indexes(&self) -> Result<()> {
        let marker = catalog("maintenance-indexes/1")?;
        if self
            .engine
            .snapshot()
            .get(Table::Reclamation, &marker)?
            .is_some()
        {
            return Ok(());
        }
        for table in [Table::Uploads, Table::Versions, Table::Reclamation] {
            let mut after = None;
            loop {
                let rows = self
                    .engine
                    .snapshot()
                    .scan_all(table, after.as_ref(), 128)?;
                if rows.is_empty() {
                    break;
                }
                after = rows.last().map(|row| row.0.clone());
                self.engine.transact(ATTEMPTS, |tx| {
                    for (key, bytes) in &rows {
                        match table {
                            Table::Uploads => {
                                let upload: PendingUpload = decode(bytes.clone())?;
                                index_upload(
                                    tx,
                                    key.fragment(),
                                    &upload.object_id,
                                    &upload.handle.version_id,
                                    &upload.upload,
                                )?;
                                if upload.completed {
                                    unindex_upload(
                                        tx,
                                        &Target {
                                            fragment: key.fragment(),
                                            object: upload.object_id,
                                            version: upload.handle.version_id,
                                        },
                                        upload.upload.expires_at,
                                    )?;
                                }
                            }
                            Table::Versions if !key.identifier().starts_with('@') => {
                                let stored: StoredVersion = decode(bytes.clone())?;
                                index_version(tx, key.fragment(), &stored)?;
                            }
                            Table::Reclamation if key.identifier().starts_with("trash/") => {
                                let trash: Trash = decode(bytes.clone())?;
                                index_trash(
                                    tx,
                                    key.fragment(),
                                    &key.identifier()[6..],
                                    trash.deleted_at_unix,
                                )?;
                            }
                            _ => (),
                        }
                    }
                    Ok(())
                })?;
            }
        }
        self.engine
            .transact(ATTEMPTS, |tx| tx.put(Table::Reclamation, &marker, b"1"))
    }

    pub(crate) fn blob(&self, target: &Target) -> Result<Blob> {
        let view = self.engine.snapshot();
        let pending: PendingUpload = decode(
            view.get(
                Table::Uploads,
                &RecordKey::new(target.fragment, &target.version)?,
            )?
            .ok_or(Error::NotFound)?,
        )?;
        if pending.object_id != target.object {
            return Err(Error::Incompatible(
                "upload owner differs from maintenance index".into(),
            ));
        }
        let stored = view
            .get(
                Table::Versions,
                &version_key(target.fragment, &target.object, &target.version)?,
            )?
            .map(decode::<StoredVersion>)
            .transpose()?;
        if stored
            .as_ref()
            .is_some_and(|s| s.object_key != pending.upload.object_key)
        {
            return Err(Error::Incompatible(
                "published and upload locations differ".into(),
            ));
        }
        Ok(Blob {
            target: target.clone(),
            key: pending.upload.object_key,
            stored,
        })
    }
    /// Removing a shared reference and testing the remaining references are one
    /// transaction. The last owner retains its key until physical purge ACK.
    pub(crate) fn prepare_purge(&self, target: &Target, key: &str) -> Result<bool> {
        self.engine.transact(ATTEMPTS, |tx| {
            let own = reference(target, key)?;
            let rows = tx.scan(
                Table::Reclamation,
                CATALOG,
                &format!("link-ref/{}/", hash(key)),
                None,
                2,
            )?;
            if rows.iter().any(|(key, _)| *key != own) {
                tx.remove(Table::Reclamation, &own)?;
                return Ok(false);
            }
            if let Some(owner) = read::<Target>(tx, Table::Reclamation, &purging(key)?)?
                && owner != *target
            {
                return Err(Error::NameConflict(
                    "another owner is purging this key".into(),
                ));
            }
            put(tx, Table::Reclamation, &purging(key)?, target)?;
            Ok(true)
        })
    }
    pub(crate) fn pool_versions(
        &self,
        pool: PoolId,
        fragment: Option<PoolId>,
        after: Option<&RecordKey>,
        limit: usize,
    ) -> Result<Vec<(RecordKey, Target)>> {
        let prefix = fragment.map_or_else(
            || format!("pool-ref/{pool}/"),
            |id| format!("pool-ref/{pool}/{id}/"),
        );
        self.engine
            .snapshot()
            .scan(Table::Reclamation, CATALOG, &prefix, after, limit)?
            .into_iter()
            .map(|(key, bytes)| Ok((key, decode(bytes)?)))
            .collect()
    }
    pub(crate) fn pin(&self, target: &Target, job: &str) -> Result<()> {
        valid_ulid(&target.object)?;
        self.engine.transact(ATTEMPTS, |tx| {
            if tx
                .get(
                    Table::Reclamation,
                    &RecordKey::new(
                        target.fragment,
                        format!("move-owner/{}/{}", target.object, target.version),
                    )?,
                )?
                .is_some()
            {
                return Err(Error::NameConflict(
                    "version migration has started; retry after it completes".into(),
                ));
            }
            let binding: ObjectBinding = read(
                tx,
                Table::Versions,
                &binding_key(target.fragment, &target.object)?,
            )?
            .ok_or(Error::NotFound)?;
            if binding.reclaiming {
                return Err(Error::NameConflict("object reclamation has started".into()));
            }
            tx.put(
                Table::Reclamation,
                &RecordKey::new(target.fragment, format!("pin/{}/{job}", target.object))?,
                b"1",
            )?;
            put(
                tx,
                Table::Reclamation,
                &catalog(format!(
                    "pin-owner/{job}/{}/{}",
                    target.fragment, target.object
                ))?,
                target,
            )
        })
    }
    pub(crate) fn unpin_job(&self, job: &str) -> Result<()> {
        loop {
            let count = self.engine.transact(ATTEMPTS, |tx| {
                let rows = tx.scan(
                    Table::Reclamation,
                    CATALOG,
                    &format!("pin-owner/{job}/"),
                    None,
                    128,
                )?;
                for (key, bytes) in &rows {
                    let target: Target = decode(bytes.clone())?;
                    tx.remove(
                        Table::Reclamation,
                        &RecordKey::new(target.fragment, format!("pin/{}/{job}", target.object))?,
                    )?;
                    tx.remove(Table::Reclamation, key)?;
                }
                Ok(rows.len())
            })?;
            if count == 0 {
                return Ok(());
            }
        }
    }
}
