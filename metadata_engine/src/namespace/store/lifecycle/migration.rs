//! Durable placement intents and atomic LINK-reference switching.
use super::*;
impl Namespace {
    pub(crate) fn move_work(&self, job: &str, target: &Target) -> Result<Option<MoveWork>> {
        self.engine
            .snapshot()
            .get(Table::Reclamation, &move_key(job, target)?)?
            .map(decode)
            .transpose()
    }
    pub(crate) fn pending_moves(
        &self,
        job: &str,
        after: Option<&RecordKey>,
        limit: usize,
    ) -> Result<Vec<(RecordKey, MoveWork)>> {
        self.engine
            .snapshot()
            .scan(
                Table::Reclamation,
                CATALOG,
                &format!("move/{job}/"),
                after,
                limit,
            )?
            .into_iter()
            .map(|(key, bytes)| Ok((key, decode(bytes)?)))
            .collect()
    }
    pub(crate) fn create_move(&self, job: &str, work: &MoveWork) -> Result<()> {
        self.engine.transact(ATTEMPTS, |tx| {
            let own_pin = RecordKey::new(
                work.target.fragment,
                format!("pin/{}/{job}", work.target.object),
            )?;
            if tx
                .scan(
                    Table::Reclamation,
                    work.target.fragment,
                    &format!("pin/{}/", work.target.object),
                    None,
                    2,
                )?
                .iter()
                .any(|(key, _)| *key != own_pin)
            {
                return Err(Error::NameConflict(
                    "version is pinned by another operation; retry migration after it completes"
                        .into(),
                ));
            }
            let binding: ObjectBinding = read(
                tx,
                Table::Versions,
                &binding_key(work.target.fragment, &work.target.object)?,
            )?
            .ok_or(Error::NotFound)?;
            if binding.reclaiming {
                return Err(Error::NameConflict("object reclamation has started".into()));
            }
            let current: StoredVersion = read(
                tx,
                Table::Versions,
                &version_key(
                    work.target.fragment,
                    &work.target.object,
                    &work.target.version,
                )?,
            )?
            .ok_or(Error::NotFound)?;
            if current.object_key != work.source {
                return Err(Error::NameConflict("version placement changed".into()));
            }
            let key = move_key(job, &work.target)?;
            if tx.get(Table::Reclamation, &key)?.is_some() {
                return Err(Error::NameConflict(
                    "migration already owns this version".into(),
                ));
            }
            // Different migration jobs cannot replace this version concurrently.
            let lock = RecordKey::new(
                work.target.fragment,
                format!("move-owner/{}/{}", work.target.object, work.target.version),
            )?;
            if tx.get(Table::Reclamation, &lock)?.is_some() {
                return Err(Error::NameConflict(
                    "another migration owns this version".into(),
                ));
            }
            tx.put(Table::Reclamation, &lock, job.as_bytes())?;
            tx.put(
                Table::Reclamation,
                &RecordKey::new(
                    work.target.fragment,
                    format!("pin/{}/{job}", work.target.object),
                )?,
                b"1",
            )?;
            put(
                tx,
                Table::Reclamation,
                &catalog(format!(
                    "pin-owner/{job}/{}/{}",
                    work.target.fragment, work.target.object
                ))?,
                &work.target,
            )?;
            put(
                tx,
                Table::Reclamation,
                &reference(&work.target, &work.upload.object_key)?,
                &work.target,
            )?;
            put(tx, Table::Reclamation, &key, work)
        })
    }
    pub(crate) fn switch_move(
        &self,
        job: &str,
        target: &Target,
        stat: &ObjectStat,
    ) -> Result<MoveWork> {
        self.engine.transact(ATTEMPTS, |tx| {
            let key = move_key(job, target)?;
            let mut work: MoveWork = read(tx, Table::Reclamation, &key)?.ok_or(Error::NotFound)?;
            if work.switched {
                return Ok(work);
            }
            let vkey = version_key(target.fragment, &target.object, &target.version)?;
            let mut stored: StoredVersion =
                read(tx, Table::Versions, &vkey)?.ok_or(Error::NotFound)?;
            let ukey = RecordKey::new(target.fragment, &target.version)?;
            let mut pending: PendingUpload =
                read(tx, Table::Uploads, &ukey)?.ok_or(Error::NotFound)?;
            if stored.object_key != work.source
                || pending.upload.object_key != work.source
                || !pending.completed
                || stat.total_bytes != stored.metadata.total_size_bytes
            {
                return Err(Error::NameConflict("migration source changed".into()));
            }
            tx.remove(Table::Reclamation, &pool_index(target, &work.source)?)?;
            stored.object_key = work.upload.object_key.clone();
            stored.integrity_algorithm = stat.integrity_algorithm.clone();
            stored.integrity_root = stat.integrity_root.clone();
            pending.upload = work.upload.clone();
            put(tx, Table::Versions, &vkey, &stored)?;
            put(tx, Table::Uploads, &ukey, &pending)?;
            index_version(tx, target.fragment, &stored)?;
            work.switched = true;
            put(tx, Table::Reclamation, &key, &work)?;
            Ok(work)
        })
    }
    pub(crate) fn finish_move(&self, job: &str, work: &MoveWork) -> Result<()> {
        self.engine.transact(ATTEMPTS, |tx| {
            let retired = if work.switched {
                &work.source
            } else {
                &work.upload.object_key
            };
            tx.remove(Table::Reclamation, &reference(&work.target, retired)?)?;
            if read::<Target>(tx, Table::Reclamation, &purging(retired)?)?.as_ref()
                == Some(&work.target)
            {
                tx.remove(Table::Reclamation, &purging(retired)?)?;
            }
            tx.remove(Table::Reclamation, &move_key(job, &work.target)?)?;
            tx.remove(
                Table::Reclamation,
                &RecordKey::new(
                    work.target.fragment,
                    format!("move-owner/{}/{}", work.target.object, work.target.version),
                )?,
            )?;
            let remaining = tx.scan(
                Table::Reclamation,
                CATALOG,
                &format!(
                    "move/{job}/{}/{}/",
                    work.target.fragment, work.target.object
                ),
                None,
                1,
            )?;
            if remaining.is_empty() {
                tx.remove(
                    Table::Reclamation,
                    &RecordKey::new(
                        work.target.fragment,
                        format!("pin/{}/{job}", work.target.object),
                    )?,
                )?;
                tx.remove(
                    Table::Reclamation,
                    &catalog(format!(
                        "pin-owner/{job}/{}/{}",
                        work.target.fragment, work.target.object
                    ))?,
                )?;
            }
            Ok(())
        })
    }
}
