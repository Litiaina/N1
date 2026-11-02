//! Normal and batch uploads share inline framing and completion. Batch owns
//! its writes and two filesystem barriers; there is no detached commit queue.
use crate::core::io_scheduler::{DiskIoClass, IoProgress, acquire_disk_io};
use crate::core::{
    Error, Result,
    storage_pool::Pool,
    store::{FinalizedObject, ShardFile, Store, blocking, collect, joined, now},
    upload::{persist_files, required, write_frames},
};
use n1_link::UploadPlan;
use std::{collections::BTreeSet, sync::Arc};
use tokio::io::{AsyncRead, AsyncReadExt};

struct Prepared {
    plan: UploadPlan,
    files: Vec<ShardFile>,
    already_finalized: bool,
}
impl Store {
    pub async fn put<R: AsyncRead + Unpin + Send + 'static>(
        self: &Arc<Self>,
        scope: [u8; 16],
        ticket: String,
        mut reader: R,
    ) -> Result<FinalizedObject> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.operation_gate.ensure_mutations_allowed()?;
            let plan = store.inspect_upload(scope, &ticket)?;
            single_part(plan)?;
            let pool = store.pool(plan.identity)?;
            let io = acquire_disk_io(
                DiskIoClass::ForegroundWrite,
                Some(&hex::encode(scope)),
                "normal upload",
            )
            .await?;
            let _admission = store.admit(&pool).await?;
            let check = pool.clone();
            blocking(move || check.writable()).await?;
            let fp = plan.identity.fingerprint();
            let lease = store.foreground(fp)?;
            let _guard = lease.state().lifecycle.clone().write_owned().await;
            let result = async {
                let prepared = store
                    .prepare_complete(scope, pool, plan, &mut reader, &io.progress_handle())
                    .await?;
                persist_files(&prepared.files).await?;
                let _snapshot = store.snapshot_gate.clone().read_owned().await;
                publish(&prepared, true).await?;
                Ok(store.completed(scope, &prepared))
            }
            .await;
            store.forget(fp);
            if result.is_ok() {
                io.complete();
            }
            result
        })
        .await
    }

    /// Tickets and raw bodies use the same request order. Locks use fingerprint
    /// order, preventing deadlocks between overlapping batches and single puts.
    /// A batch is not an all-or-nothing multi-object transaction: on a lost or
    /// failed reply clients resolve each immutable identity by retrying it.
    pub async fn put_batch<R: AsyncRead + Unpin + Send + 'static>(
        self: &Arc<Self>,
        scope: [u8; 16],
        tickets: Vec<String>,
        mut reader: R,
    ) -> Result<Vec<FinalizedObject>> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.operation_gate.ensure_mutations_allowed()?;
            let plans = store.batch_plans(scope, &tickets)?;
            let largest = plans
                .iter()
                .map(|p| store.pool(p.identity))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .max_by_key(|p| p.layout.width() * p.layout.block_bytes as usize)
                .expect("nonempty batch");
            let io = acquire_disk_io(
                DiskIoClass::ForegroundWrite,
                Some(&hex::encode(scope)),
                "batch upload",
            )
            .await?;
            let _admission = store.admit(&largest).await?;
            let mut fingerprints = plans
                .iter()
                .map(|p| p.identity.fingerprint())
                .collect::<Vec<_>>();
            fingerprints.sort_unstable_by_key(|fp| fp.0);
            let leases = fingerprints
                .iter()
                .map(|fp| store.foreground(*fp))
                .collect::<Result<Vec<_>>>()?;
            let mut guards = Vec::new();
            for lease in &leases {
                guards.push(lease.state().lifecycle.clone().write_owned().await);
            }
            let mut roots = BTreeSet::new();
            let mut pools = Vec::new();
            for plan in &plans {
                let pool = store.pool(plan.identity)?;
                for member in pool.members.iter().flatten() {
                    roots.insert(member.path.clone());
                }
                pools.push(pool);
            }
            let check = pools.clone();
            let roots = roots.into_iter().collect::<Vec<_>>();
            // Open/pin barrier descriptors before any attached batch writes.
            let group = Arc::new(
                blocking(move || {
                    for p in check {
                        p.writable()?;
                    }
                    crate::core::linux_io::group::FilesystemGroup::open(&roots).map_err(Error::from)
                })
                .await?,
            );
            let result = async {
                let mut prepared = Vec::with_capacity(plans.len());
                for (plan, pool) in plans.iter().zip(pools) {
                    let mut body = (&mut reader).take(plan.total_bytes);
                    prepared.push(
                        store
                            .prepare_complete(scope, pool, *plan, &mut body, &io.progress_handle())
                            .await?,
                    );
                }
                let barrier = group.clone();
                blocking(move || barrier.sync_all().map_err(Error::from)).await?;
                // Publication workers are joined even if one member fails. A
                // later group barrier is never skipped after publication starts.
                let _snapshot = store.snapshot_gate.clone().read_owned().await;
                let mut publication = None;
                for object in &prepared {
                    if let Err(e) = publish(object, false).await
                        && publication.is_none()
                    {
                        publication = Some(e);
                    }
                }
                let barrier = group.clone();
                let persisted = blocking(move || barrier.sync_all().map_err(Error::from)).await;
                if let Some(e) = publication {
                    return Err(e);
                }
                persisted?;
                Ok(prepared
                    .iter()
                    .map(|object| store.completed(scope, object))
                    .collect())
            }
            .await;
            for fp in fingerprints {
                store.forget(fp);
            }
            drop(guards);
            if result.is_ok() {
                io.complete();
            }
            result
        })
        .await
    }

    pub fn batch_length(&self, scope: [u8; 16], tickets: &[String]) -> Result<u64> {
        Ok(self
            .batch_plans(scope, tickets)?
            .iter()
            .map(|p| p.total_bytes)
            .sum())
    }
    fn batch_plans(&self, scope: [u8; 16], tickets: &[String]) -> Result<Vec<UploadPlan>> {
        if tickets.is_empty() || tickets.len() > 256 {
            return Err(Error::Invalid("batch requires 1..=256 tickets".into()));
        }
        let mut seen = BTreeSet::new();
        let mut total = 0u64;
        let mut descriptors = 0;
        let plans = tickets
            .iter()
            .map(|ticket| {
                let plan = self.inspect_upload(scope, ticket)?;
                single_part(plan)?;
                if !seen.insert(plan.identity.fingerprint().0) {
                    return Err(Error::Invalid("duplicate identity in batch".into()));
                }
                total = total
                    .checked_add(plan.total_bytes)
                    .ok_or_else(|| Error::Invalid("batch length overflow".into()))?;
                descriptors += self.pool(plan.identity)?.layout.width();
                Ok(plan)
            })
            .collect::<Result<Vec<_>>>()?;
        if total > n1_link::noa::MAX_BODY_BYTES || descriptors > 1024 {
            return Err(Error::Invalid(
                "batch exceeds byte or open-shard bound".into(),
            ));
        }
        Ok(plans)
    }
    async fn prepare_complete<R: AsyncRead + Unpin>(
        &self,
        _scope: [u8; 16],
        pool: Arc<Pool>,
        plan: UploadPlan,
        reader: &mut R,
        progress: &IoProgress,
    ) -> Result<Prepared> {
        let fp = plan.identity.fingerprint();
        let files = required(self.files(pool.clone(), plan, true).await?)?;
        if !files.iter().all(|f| f.finalized) && now() >= plan.expires_at {
            return Err(Error::Conflict("upload ticket has expired".into()));
        }
        if plan.total_bytes > 0 {
            self.forget_part(fp, 0);
            let root = write_frames(&pool, &files, plan, 0, reader, progress).await?;
            self.remember_part(fp, plan, 0, root);
        }
        let already_finalized = self.seal_upload(&pool, plan, &files).await?;
        Ok(Prepared {
            plan,
            files,
            already_finalized,
        })
    }
    fn completed(&self, scope: [u8; 16], prepared: &Prepared) -> FinalizedObject {
        FinalizedObject {
            object_key: self
                .signer
                .object_key(&scope, prepared.plan.identity)
                .as_str()
                .into(),
            already_finalized: prepared.already_finalized,
            total_bytes: prepared.plan.total_bytes,
        }
    }
}
fn single_part(plan: UploadPlan) -> Result<()> {
    if plan.parts() > 1 {
        Err(Error::Invalid(
            "normal/batch upload requires a single-part ticket".into(),
        ))
    } else {
        Ok(())
    }
}
async fn publish(object: &Prepared, strict: bool) -> Result<()> {
    let fp = object.plan.identity.fingerprint();
    collect(
        futures_util::future::join_all(object.files.iter().map(|file| {
            let member = file.member.clone();
            let shard = file.header.shard;
            let finalized = file.finalized;
            blocking(move || {
                if !finalized {
                    member.promote(fp, shard, strict)?;
                } else if strict {
                    member.persist_final_parent(fp)?;
                }
                member.retire_empty_stage(fp, strict)
            })
        }))
        .await,
    )?;
    Ok(())
}
