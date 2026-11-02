use super::{
    Error, Result,
    storage_pool::Member,
    store::{Store, blocking, collect, joined, now},
};
use n1_link::{
    Fingerprint,
    retirement::{BYTES, Phase, Retirement},
};
use rand::{RngCore, rngs::OsRng};
use std::{fs::File, path::Path, sync::Arc};

pub(crate) fn inspect_retirement(
    file: &File,
    fp: Fingerprint,
    shard: u16,
) -> Result<Option<Retirement>> {
    if file.metadata()?.len() != BYTES as u64 {
        return Ok(None);
    }
    let mut bytes = [0; BYTES];
    crate::core::linux_io::pread_exact_at(file, &mut bytes, 0)?;
    Retirement::decode(&bytes, fp, shard)
        .map(Some)
        .map_err(|_| Error::RetirementCorrupt)
}
pub(crate) fn terminal_error(record: Retirement) -> Error {
    match record.phase {
        Phase::Retiring => Error::Retiring,
        Phase::Retired => Error::Retired,
    }
}

impl Store {
    /// Two phases are required only for delete. A final Retired certificate is
    /// published only after every member durably holds terminal Retiring state.
    /// Thus losing a member after observing deletion can never reveal old data.
    pub async fn delete(self: &Arc<Self>, scope: [u8; 16], key: String) -> Result<usize> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.operation_gate.ensure_mutations_allowed()?;
            let identity = store.signer.verify_object(&scope, &key)?;
            let pool = store.pool(identity)?;
            let io = super::io_scheduler::acquire_disk_io(
                super::io_scheduler::DiskIoClass::ForegroundWrite,
                Some(&hex::encode(scope)),
                "delete object",
            )
            .await?;
            let _admission = store.admit_metadata(&pool).await?;
            let fp = identity.fingerprint();
            let check = pool.clone();
            blocking(move || check.writable()).await?;
            let lease = store.foreground(fp)?;
            let _guard = lease.state().lifecycle.clone().write_owned().await;
            store.forget(fp);
            // Pin the error-accounting descriptors before either phase writes.
            // Retain both phase boundaries; collapse repeated directory/ancestor
            // syncs into the already tested request-owned filesystem barrier.
            let roots = pool
                .members
                .iter()
                .flatten()
                .map(|m| m.path.clone())
                .collect::<Vec<_>>();
            let group = Arc::new(
                blocking(move || {
                    super::linux_io::group::FilesystemGroup::open(&roots).map_err(Error::from)
                })
                .await?,
            );
            let probes = (0..pool.layout.width()).map(|shard| {
                let holder = pool.holder(fp, shard as u16);
                blocking(move || {
                    let member = holder?;
                    let record = match member.open_read(Path::new(&fp.shard_path(shard as u16))) {
                        // An explicit authenticated delete may retire damaged
                        // payload or repair a torn terminal record as well.
                        Ok(file) => match inspect_retirement(&file, fp, shard as u16) {
                            Ok(r) => r,
                            Err(Error::Corrupt(_) | Error::RetirementCorrupt) => None,
                            Err(e) => return Err(e),
                        },
                        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => None,
                        Err(e) => return Err(e),
                    };
                    Ok((member, record))
                })
            });
            let members = collect(futures_util::future::join_all(probes).await)?;
            let mut nonce = [0; 16];
            OsRng.fill_bytes(&mut nonce);
            let seed = members
                .iter()
                .filter_map(|(_, r)| *r)
                .min_by_key(|r| r.nonce)
                .unwrap_or(Retirement {
                    phase: Phase::Retiring,
                    shard: 0,
                    nonce,
                    timestamp: now(),
                });
            let newly_retired = members.iter().filter(|(_, r)| r.is_none()).count();
            let _snapshot = store.snapshot_gate.clone().read_owned().await;
            // Phase one: destroy live visibility everywhere before publishing
            // even one completed retirement certificate. Join all workers.
            let pending = members
                .iter()
                .enumerate()
                .filter(|(_, (_, r))| !r.is_some_and(|r| r.phase == Phase::Retired));
            let published = futures_util::future::join_all(pending.map(|(shard, (member, _))| {
                let member = member.clone();
                let record = Retirement {
                    phase: Phase::Retiring,
                    shard: shard as u16,
                    ..seed
                };
                blocking(move || member.publish_retirement_grouped(fp, record))
            }))
            .await;
            // Partial successes must be synchronized even when another member
            // failed. Never enter the completed-certificate phase on any error.
            let barrier = group.clone();
            let persisted = blocking(move || barrier.sync_all().map_err(Error::from)).await;
            collect(published)?;
            persisted?;
            // Remove unfinished payload after durable terminal authority exists.
            collect(
                futures_util::future::join_all(members.iter().map(|(member, _)| {
                    let member = member.clone();
                    blocking(move || {
                        member.unlink(Path::new(&fp.staging_path()), true)?;
                        for shard in 0..pool_width(&member) {
                            member.unlink(
                                Path::new(&format!(".uploads/{fp}/repair.s{shard}.tmp")),
                                true,
                            )?;
                        }
                        Ok(())
                    })
                }))
                .await,
            )?;
            // Phase two: a surviving completed certificate proves that old
            // payload paths were removed on every member before it was issued.
            let published = futures_util::future::join_all(members.iter().enumerate().map(
                |(shard, (member, old))| {
                    let member = member.clone();
                    let old = *old;
                    let record = Retirement {
                        phase: Phase::Retired,
                        shard: shard as u16,
                        ..seed
                    };
                    blocking(move || {
                        if old.is_some_and(|r| r.phase == Phase::Retired) {
                            member
                                .open_read(Path::new(&fp.shard_path(shard as u16)))?
                                .sync_all()?;
                        } else {
                            member.publish_retirement_grouped(fp, record)?;
                        }
                        member.retire_empty_stage(fp, false)
                    })
                },
            ))
            .await;
            let barrier = group.clone();
            let persisted = blocking(move || barrier.sync_all().map_err(Error::from)).await;
            collect(published)?;
            persisted?;
            io.complete();
            Ok(newly_retired)
        })
        .await
    }
}
fn pool_width(member: &Arc<Member>) -> usize {
    member.marker.layout.width()
}
