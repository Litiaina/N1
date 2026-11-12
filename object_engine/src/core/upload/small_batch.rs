//! Normal and batch uploads share inline framing and completion. Each request
//! persists data before durable publication; there is no detached commit queue.
use crate::core::io_scheduler::IoProgress;
use crate::core::{
    Error, Result,
    storage_pool::Pool,
    store::{FinalizedObject, ShardFile, Store, blocking, joined, now},
    upload::{WriteOptions, persist_files, required, write_frames},
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
        key: String,
        mut reader: R,
    ) -> Result<FinalizedObject> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.operation_gate.ensure_mutations_allowed()?;
            let plan = store.inspect_upload(scope, &key).await?;
            single_part(plan)?;
            let pool = store.pool(plan.identity)?;
            let io = store.upload_io(scope, "normal upload").await?;
            let _admission = store.admit(&pool).await?;
            let check = pool.clone();
            blocking(move || check.writable()).await?;
            let fp = plan.identity.fingerprint();
            let lease = store.foreground(fp)?;
            let _guard = lease.state().lifecycle.clone().write_owned().await;
            let roots = pool
                .members
                .iter()
                .flatten()
                .map(|member| member.path.clone())
                .collect::<Vec<_>>();
            // Pin error-accounting descriptors before any payload writes, as
            // in batch upload. Sync this object's sealed bytes before publication,
            // then join all renames/removals in one barrier per filesystem.
            let group = Arc::new(
                blocking(move || {
                    crate::core::linux_io::group::FilesystemGroup::open(&roots).map_err(Error::from)
                })
                .await?,
            );
            let result = async {
                let prepared = store
                    .prepare_complete(scope, pool, plan, &mut reader, &io.progress_handle())
                    .await?;
                persist_files(&prepared.files).await?;
                let _snapshot = store.snapshot_gate.clone().read_owned().await;
                let publication = publish(&prepared).await;
                // Join and persist every started publication even on failure.
                let barrier = group.clone();
                let persisted = blocking(move || barrier.sync_all().map_err(Error::from)).await;
                publication?;
                persisted?;
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

    /// Keys and raw bodies use the same request order. Locks use fingerprint
    /// order, preventing deadlocks between overlapping batches and single puts.
    /// A batch is not an all-or-nothing multi-object transaction: on a lost or
    /// failed reply clients resolve each immutable identity by retrying it.
    pub async fn put_batch<R: AsyncRead + Unpin + Send + 'static>(
        self: &Arc<Self>,
        scope: [u8; 16],
        keys: Vec<String>,
        mut reader: R,
    ) -> Result<Vec<FinalizedObject>> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.operation_gate.ensure_mutations_allowed()?;
            let plans = store.batch_plans(scope, &keys).await?;
            let largest = plans
                .iter()
                .map(|p| store.pool(p.identity))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .max_by_key(|p| p.layout.width() * p.layout.block_bytes as usize)
                .expect("nonempty batch");
            let io = store.upload_io(scope, "batch upload").await?;
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
                    if let Err(e) = publish(object).await
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

    pub async fn batch_length(&self, scope: [u8; 16], keys: &[String]) -> Result<u64> {
        Ok(self
            .batch_plans(scope, keys)
            .await?
            .iter()
            .map(|p| p.total_bytes)
            .sum())
    }
    async fn batch_plans(&self, scope: [u8; 16], keys: &[String]) -> Result<Vec<UploadPlan>> {
        if keys.is_empty() || keys.len() > 256 {
            return Err(Error::Invalid("batch requires 1..=256 keys".into()));
        }
        let mut seen = BTreeSet::new();
        let mut total = 0u64;
        let mut descriptors = 0;
        let mut plans = Vec::with_capacity(keys.len());
        for key in keys {
            let plan = self.inspect_upload(scope, key).await?;
            single_part(plan)?;
            if !seen.insert(plan.identity.fingerprint().0) {
                return Err(Error::Invalid("duplicate identity in batch".into()));
            }
            total = total
                .checked_add(plan.total_bytes)
                .ok_or_else(|| Error::Invalid("batch length overflow".into()))?;
            descriptors += self.pool(plan.identity)?.layout.width();
            plans.push(plan);
        }
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
        let files = required(self.files(pool.clone(), plan).await?)?;
        if !files.iter().all(|f| f.finalized) && now() >= plan.expires_at {
            return Err(Error::Conflict("upload key has expired".into()));
        }
        if plan.total_bytes > 0 {
            self.forget_part(fp, 0);
            let root = write_frames(
                &pool,
                &files,
                plan,
                0,
                reader,
                progress,
                WriteOptions {
                    buffer_size: self.runtime().writer_buffer_size,
                    immutable_part: false,
                },
            )
            .await?;
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
            "normal/batch upload requires a single-part key".into(),
        ))
    } else {
        Ok(())
    }
}
async fn publish(object: &Prepared) -> Result<()> {
    super::publish_files(&object.files, object.plan.identity.fingerprint()).await
}
