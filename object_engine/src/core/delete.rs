//! Key-addressed purge. Completion leaves no per-object terminal state.
use super::{
    Error, Result,
    store::{Store, blocking, collect, joined},
};
use std::{path::Path, sync::Arc};

impl Store {
    /// Acknowledgement requires every calculated member's removal and the
    /// filesystem persistence barriers. Interrupted calls remain retryable;
    /// metadata/callers retain their intent until acknowledgement.
    pub async fn delete(self: &Arc<Self>, scope: [u8; 16], key: String) -> Result<usize> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.operation_gate.ensure_mutations_allowed()?;
            let identity = store.signer.verify_object(&scope, &key)?;
            let pool = store.pool(identity)?;
            let io = super::io_scheduler::acquire_disk_io(
                super::io_scheduler::DiskIoClass::ForegroundWrite,
                Some(&hex::encode(scope)),
                "purge object",
            )
            .await?;
            let _admission = store.admit_metadata(&pool).await?;
            let fp = identity.fingerprint();
            let lease = store.foreground(fp)?;
            let _guard = lease.state().lifecycle.clone().write_owned().await;
            // Online backup pins an exact set of final shard paths under the
            // write side. Purge uses the same publication fence as finalize.
            let _snapshot = store.snapshot_gate.clone().read_owned().await;
            store.forget(fp);
            let check = pool.clone();
            let roots = pool
                .members
                .iter()
                .flatten()
                .map(|m| m.path.clone())
                .collect::<Vec<_>>();
            let group = blocking(move || {
                check.writable()?;
                super::linux_io::group::FilesystemGroup::open(&roots).map_err(Error::from)
            })
            .await?;
            let width = pool.layout.width();
            let jobs = (0..width).map(|shard| {
                let holder = pool.holder(fp, shard as u16);
                blocking(move || {
                    let member = holder?;
                    #[cfg(test)]
                    if shard + 1 == width
                        && std::env::var("N1_TEST_PAUSE_PURGE_LAST")
                            .is_ok_and(|value| value == fp.to_string())
                    {
                        // Only test binaries contain this crash boundary.
                        loop {
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                    }
                    let mut removed =
                        member.unlink(Path::new(&fp.shard_path(shard as u16)), false)?;
                    removed |= member.unlink(Path::new(&fp.staging_path()), false)?;
                    removed |= member.unlink(
                        Path::new(&format!(".uploads/{fp}/repair.s{shard}.tmp")),
                        false,
                    )?;
                    member.retire_empty_stage(fp, false)?;
                    match member.directory(Path::new(&format!(".uploads/{fp}")), false, false) {
                        Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => (),
                        Err(e) => return Err(e),
                        Ok(_) => {
                            return Err(Error::Conflict(
                                "unexpected files remain in purged staging directory".into(),
                            ));
                        }
                    }
                    Ok(usize::from(removed))
                })
            });
            let results = futures_util::future::join_all(jobs).await;
            // Even a failed member cannot skip persistence of successful
            // removals on the others. Never acknowledge a partial purge.
            let persisted = blocking(move || group.sync_all().map_err(Error::from)).await;
            let results = collect(results)?;
            persisted?;
            io.complete();
            Ok(results.into_iter().sum())
        })
        .await
    }
}
