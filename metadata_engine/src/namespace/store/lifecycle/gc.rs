//! Expiry claims and bounded metadata reclamation.
use super::*;
impl Namespace {
    pub(crate) fn due_trash(
        &self,
        fragment: Option<PoolId>,
        cutoff: i64,
        after: Option<&RecordKey>,
        limit: usize,
    ) -> Result<Vec<(RecordKey, TrashWork)>> {
        let prefix =
            fragment.map_or_else(|| "trash-due/all/".into(), |id| format!("trash-due/{id}/"));
        self.engine
            .snapshot()
            .scan(Table::Reclamation, CATALOG, &prefix, after, limit)?
            .into_iter()
            .map(|(key, bytes)| Ok((key, decode::<TrashWork>(bytes)?)))
            .take_while(|row| !matches!(row, Ok((_, work)) if work.deleted_at > cutoff))
            .collect()
    }
    pub(crate) fn claimed_trash(
        &self,
        fragment: Option<PoolId>,
        after: Option<&RecordKey>,
        limit: usize,
    ) -> Result<Vec<(RecordKey, TrashWork)>> {
        let prefix =
            fragment.map_or_else(|| "trash-work/".into(), |id| format!("trash-work/{id}/"));
        self.engine
            .snapshot()
            .scan(Table::Reclamation, CATALOG, &prefix, after, limit)?
            .into_iter()
            .map(|(key, bytes)| Ok((key, decode(bytes)?)))
            .collect()
    }
    pub(crate) fn claim_trash(&self, work: &TrashWork, cutoff: i64) -> Result<bool> {
        self.engine.transact(ATTEMPTS, |tx| {
            let key = RecordKey::new(work.fragment, format!("trash/{}", work.trash))?;
            let Some(mut trash) = read::<Trash>(tx, Table::Reclamation, &key)? else {
                unindex_trash(tx, work)?;
                return Ok(false);
            };
            if trash.restored {
                unindex_trash(tx, work)?;
                return Ok(false);
            }
            if trash.entry.object_id != work.object || trash.deleted_at_unix != work.deleted_at {
                return Err(Error::Incompatible(
                    "trash index differs from its owner".into(),
                ));
            }
            if !trash.reclaiming && trash.deleted_at_unix > cutoff {
                return Ok(false);
            }
            let binding_key = binding_key(work.fragment, &work.object)?;
            let mut binding: ObjectBinding =
                read(tx, Table::Versions, &binding_key)?.ok_or(Error::NotFound)?;
            if !binding.trashed || binding.path != trash.path {
                return Err(Error::Incompatible(
                    "trash binding differs from its owner".into(),
                ));
            }
            if !tx
                .scan(
                    Table::Reclamation,
                    work.fragment,
                    &format!("pin/{}/", work.object),
                    None,
                    1,
                )?
                .is_empty()
            {
                return Ok(false);
            }
            trash.reclaiming = true;
            binding.reclaiming = true;
            put(tx, Table::Reclamation, &key, &trash)?;
            put(tx, Table::Versions, &binding_key, &binding)?;
            unindex_trash(tx, work)?;
            put(tx, Table::Reclamation, &claimed_key(work)?, work)?;
            Ok(true)
        })
    }
    pub(crate) fn object_blobs(&self, work: &TrashWork, limit: usize) -> Result<Vec<Blob>> {
        let rows = self.engine.snapshot().scan(
            Table::Reclamation,
            work.fragment,
            &format!("object-upload/{}/", work.object),
            None,
            limit,
        )?;
        rows.into_iter()
            .map(|(_, bytes)| {
                let target: Target = decode(bytes)?;
                self.blob(&target)
            })
            .collect()
    }
    pub(crate) fn due_uploads(
        &self,
        fragment: Option<PoolId>,
        now: u64,
        after: Option<&RecordKey>,
        limit: usize,
    ) -> Result<Vec<(RecordKey, UploadWork)>> {
        let prefix = format!(
            "upload-due/{}/",
            fragment.map_or_else(|| "all".into(), |id| id.to_string())
        );
        self.engine
            .snapshot()
            .scan(Table::Reclamation, CATALOG, &prefix, after, limit)?
            .into_iter()
            .map(|(key, bytes)| Ok((key, decode::<UploadWork>(bytes)?)))
            .take_while(|row| !matches!(row,Ok((_,work)) if work.expires_at>now))
            .collect()
    }
    pub(crate) fn claim_upload(&self, work: &UploadWork, now: u64) -> Result<Option<Blob>> {
        let claimed = self.engine.transact(ATTEMPTS, |tx| {
            let target = &work.target;
            let Some(pending) = read::<PendingUpload>(
                tx,
                Table::Uploads,
                &RecordKey::new(target.fragment, &target.version)?,
            )?
            else {
                unindex_upload(tx, target, work.expires_at)?;
                return Ok(false);
            };
            if pending.completed {
                unindex_upload(tx, target, work.expires_at)?;
                return Ok(false);
            }
            if read::<ObjectBinding>(
                tx,
                Table::Versions,
                &binding_key(target.fragment, &target.object)?,
            )?
            .is_some_and(|b| b.trashed)
            {
                return Ok(false);
            }
            if pending.object_id != target.object || pending.upload.expires_at != work.expires_at {
                return Err(Error::Incompatible(
                    "upload expiry index differs from owner".into(),
                ));
            }
            if now < work.expires_at {
                return Ok(false);
            }
            tx.put(Table::Reclamation, &upload_claim(target)?, b"1")?;
            Ok(true)
        })?;
        if claimed {
            Ok(Some(self.blob(&work.target)?))
        } else {
            Ok(None)
        }
    }
    /// Bounded deletion of multipart control rows, after native purge. Return
    /// false while another page remains; the owning upload/key survives.
    pub(crate) fn purge_blob(&self, blob: &Blob, limit: usize) -> Result<bool> {
        self.engine.transact(ATTEMPTS, |tx| {
            let pending = read::<PendingUpload>(
                tx,
                Table::Uploads,
                &RecordKey::new(blob.target.fragment, &blob.target.version)?,
            )?;
            for prefix in [
                format!("upload-part/{}/", blob.target.version),
                format!("upload-progress/{}/", blob.target.version),
            ] {
                let rows = tx.scan(
                    Table::Reclamation,
                    blob.target.fragment,
                    &prefix,
                    None,
                    limit,
                )?;
                if !rows.is_empty() {
                    for (key, _) in rows {
                        tx.remove(Table::Reclamation, &key)?;
                    }
                    return Ok(false);
                }
            }
            if let Some(pending) = &pending {
                unindex_upload(tx, &blob.target, pending.upload.expires_at)?;
            }
            if let Some(stored) = &blob.stored {
                tx.remove(
                    Table::Reclamation,
                    &pool_index(&blob.target, &stored.object_key)?,
                )?;
            }
            tx.remove(
                Table::Versions,
                &version_key(
                    blob.target.fragment,
                    &blob.target.object,
                    &blob.target.version,
                )?,
            )?;
            tx.remove(
                Table::Uploads,
                &RecordKey::new(blob.target.fragment, &blob.target.version)?,
            )?;
            for prefix in ["pending/", "upload-plan/"] {
                tx.remove(
                    Table::Reclamation,
                    &RecordKey::new(
                        blob.target.fragment,
                        format!("{prefix}{}", blob.target.version),
                    )?,
                )?;
            }
            tx.remove(Table::Reclamation, &owner_index(&blob.target)?)?;
            if let Some(pending) = read::<PendingUpload>(
                tx,
                Table::Uploads,
                &RecordKey::new(blob.target.fragment, &blob.target.version)?,
            )? {
                unindex_upload(tx, &blob.target, pending.upload.expires_at)?;
            }
            tx.remove(Table::Reclamation, &upload_claim(&blob.target)?)?;
            if blob.stored.is_none()
                && tx
                    .scan(
                        Table::Reclamation,
                        blob.target.fragment,
                        &format!("object-upload/{}/", blob.target.object),
                        None,
                        1,
                    )?
                    .is_empty()
            {
                let key = binding_key(blob.target.fragment, &blob.target.object)?;
                if let Some(binding) = read::<ObjectBinding>(tx, Table::Versions, &key)? {
                    let entry_key = RecordKey::new(blob.target.fragment, &binding.path)?;
                    if read::<Entry>(tx, Table::Entries, &entry_key)?.is_some_and(|entry| {
                        entry.object_id == blob.target.object && entry.head.is_none()
                    }) {
                        tx.remove(Table::Entries, &entry_key)?;
                        tx.remove(Table::Versions, &key)?;
                    }
                }
            }
            tx.remove(Table::Reclamation, &reference(&blob.target, &blob.key)?)?;
            if read::<Target>(tx, Table::Reclamation, &purging(&blob.key)?)?.as_ref()
                == Some(&blob.target)
            {
                tx.remove(Table::Reclamation, &purging(&blob.key)?)?;
            }
            Ok(true)
        })
    }
    pub(crate) fn finish_trash(&self, work: &TrashWork, limit: usize) -> Result<bool> {
        self.engine.transact(ATTEMPTS, |tx| {
            if !tx
                .scan(
                    Table::Reclamation,
                    work.fragment,
                    &format!("object-upload/{}/", work.object),
                    None,
                    1,
                )?
                .is_empty()
            {
                return Ok(false);
            }
            let rows = tx.scan(
                Table::Reclamation,
                work.fragment,
                &format!("object-trash/{}/", work.object),
                None,
                limit,
            )?;
            for (key, bytes) in &rows {
                let old: TrashWork = decode(bytes.clone())?;
                if old.trash != work.trash {
                    tx.remove(
                        Table::Reclamation,
                        &RecordKey::new(old.fragment, format!("trash/{}", old.trash))?,
                    )?;
                }
                unindex_trash(tx, &old)?;
                tx.remove(Table::Reclamation, key)?;
            }
            if !rows.is_empty() {
                return Ok(false);
            }
            tx.remove(
                Table::Reclamation,
                &RecordKey::new(work.fragment, format!("trash/{}", work.trash))?,
            )?;
            tx.remove(Table::Versions, &binding_key(work.fragment, &work.object)?)?;
            tx.remove(Table::Reclamation, &claimed_key(work)?)?;
            Ok(true)
        })
    }
}
