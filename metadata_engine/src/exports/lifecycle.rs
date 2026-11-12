//! Indexed retention and durable byte reservations for retained archives.
//! Source objects remain owned by the namespace and public N1 API.
use super::{models::*, store};
use crate::{Error, MetadataTransaction, Table, fragments, namespace::Namespace};
use std::sync::{Arc, Weak};
use tokio::sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit, RwLock};

const EXPIRY_PREFIX: &str = "export-expiry/";

pub(super) fn expiry(record: &PersistedExport) -> crate::Result<crate::RecordKey> {
    fragments::key(format!(
        "{EXPIRY_PREFIX}{:020}/{}",
        record.expires_at_unix, record.id
    ))
}

pub(super) fn charge(
    tx: &mut MetadataTransaction,
    previous: u64,
    requested: u64,
    limit: u64,
) -> crate::Result<()> {
    if previous == requested {
        return Ok(());
    }
    let key = fragments::key("export-retained-bytes")?;
    let used = match tx.get(Table::Reclamation, &key)? {
        None => 0,
        Some(bytes) => u64::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| Error::Incompatible("invalid export byte counter".into()))?,
        ),
    };
    let total = used
        .checked_sub(previous)
        .and_then(|n| n.checked_add(requested))
        .ok_or_else(|| Error::Incompatible("export byte accounting overflow".into()))?;
    if requested > previous && limit != 0 && total > limit {
        return Err(Error::Busy);
    }
    tx.put(Table::Reclamation, &key, &total.to_be_bytes())
}

pub(super) fn estimated_bytes(record: &PersistedExport) -> u64 {
    // Retain the original N1 bound: paths may occupy 4096 bytes in both ZIP
    // headers. ZIP uses Stored compression; this also covers TAR padding.
    record
        .source_bytes_total
        .saturating_add(record.objects_total.saturating_mul(10 * 1024))
        .saturating_add(1024 * 1024)
}

pub(super) fn gate(id: &str) -> Arc<RwLock<()>> {
    let mut gates = store::context()
        .gates
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(gate) = gates.get(id).and_then(Weak::upgrade) {
        return gate;
    }
    gates.retain(|_, gate| gate.strong_count() != 0);
    let gate = Arc::new(RwLock::new(()));
    gates.insert(id.into(), Arc::downgrade(&gate));
    gate
}

pub(super) struct DownloadLease {
    _job: OwnedRwLockReadGuard<()>,
    _slot: OwnedSemaphorePermit,
}

pub(super) async fn lease(
    id: &str,
) -> crate::server::error::ApiResult<(PersistedExport, DownloadLease)> {
    use crate::server::error::ApiError;
    use axum::http::StatusCode;
    let slot = store::context()
        .downloads
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "Export download limit reached".into(),
            )
        })?;
    let job = gate(id).read_owned().await;
    let record = store::load(id).await?;
    if record.expires_at_unix <= crate::middleware::auth::now() as i64 {
        return Err(ApiError(StatusCode::GONE, "Export has expired".into()));
    }
    if record.phase != ExportPhase::Ready {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "Export artifact is not ready".into(),
        ));
    }
    Ok((
        record,
        DownloadLease {
            _job: job,
            _slot: slot,
        },
    ))
}

pub(super) async fn cleanup_chunks(id: &str) -> crate::server::error::ApiResult<()> {
    store::validate_id(id)?;
    let path = store::artifact_job_directory(&store::context().root, id);
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    sync_directory(store::context().root.join("artifacts")).await
}

async fn sync_directory(path: std::path::PathBuf) -> crate::server::error::ApiResult<()> {
    tokio::task::spawn_blocking(move || std::fs::File::open(path)?.sync_all())
        .await
        .map_err(|_| crate::server::error::ApiError::invalid("export cleanup worker failed"))??;
    Ok(())
}

async fn remove_expired(id: String) -> crate::server::error::ApiResult<()> {
    let ctx = store::context();
    if let Some(progress) = ctx.jobs.lock().unwrap_or_else(|e| e.into_inner()).get(&id) {
        progress
            .cancel
            .store(true, std::sync::atomic::Ordering::Release);
    }
    // Active builds and streams retain their lease. Cleanup never removes a
    // chunk between its validation and its read, or races a still-writing task.
    let Ok(_exclusive) = gate(&id).try_write_owned() else {
        return Ok(());
    };
    let record = store::load(&id).await?;
    if record.expires_at_unix > crate::middleware::auth::now() as i64 {
        return Ok(());
    }
    cleanup_chunks(&id).await?;
    let manifest = store::manifest_path(&id)?;
    for path in [&manifest, &manifest.with_extension("partial")] {
        match tokio::fs::remove_file(path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    sync_directory(ctx.root.join("manifests")).await?;
    ctx.db
        .execute(move |db| {
            Namespace::new(db.clone()).unpin_job(&id)?;
            let key = fragments::key(format!("export/{id}"))?;
            db.transact(64, |tx| {
                let Some(bytes) = tx.get(Table::Reclamation, &key)? else {
                    return Ok(());
                };
                let current: PersistedExport = fragments::decode(bytes)?;
                charge(tx, current.reserved_bytes, 0, 0)?;
                tx.remove(Table::Reclamation, &expiry(&current)?)?;
                tx.remove(Table::Reclamation, &key)
            })
        })
        .await?;
    Ok(())
}

pub(super) async fn sweep(
    after: Option<crate::RecordKey>,
) -> crate::server::error::ApiResult<Option<crate::RecordKey>> {
    let rows = store::context()
        .db
        .execute(move |db| {
            db.snapshot().scan(
                Table::Reclamation,
                fragments::CATALOG,
                EXPIRY_PREFIX,
                after.as_ref(),
                128,
            )
        })
        .await?;
    let now = crate::middleware::auth::now() as i64;
    // Carry a bounded-page cursor between ticks. Leased early rows must not
    // prevent later expired jobs from being reclaimed.
    let next = (rows.len() == 128).then(|| rows.last().unwrap().0.clone());
    for (key, _) in rows {
        let identifier = key.identifier();
        let (timestamp, id) = identifier
            .strip_prefix(EXPIRY_PREFIX)
            .and_then(|tail| tail.split_once('/'))
            .ok_or_else(|| Error::Incompatible("invalid export expiry index".into()))?;
        let timestamp = timestamp
            .parse::<i64>()
            .map_err(|_| Error::Incompatible("invalid export expiry time".into()))?;
        if timestamp > now {
            return Ok(None);
        }
        if let Err(error) = remove_expired(id.to_owned()).await {
            // A failed artifact remains charged and indexed for retry, but
            // cannot prevent unrelated expired jobs from being reclaimed.
            tracing::warn!(export_id = id, %error, "export cleanup deferred");
        }
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn directory() -> tempfile::TempDir {
        tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap()
    }

    #[test]
    fn concurrent_reservations_cannot_oversubscribe_and_reuse_released_capacity() {
        let root = directory();
        let db = crate::MetadataEngine::open(root.path(), crate::Options::default()).unwrap();
        let ready = Arc::new(std::sync::Barrier::new(8));
        let workers = (0..8)
            .map(|_| {
                let db = db.clone();
                let ready = ready.clone();
                std::thread::spawn(move || {
                    ready.wait();
                    db.transact(64, |tx| charge(tx, 0, 1024, 4096))
                })
            })
            .collect::<Vec<_>>();
        let results = workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 4);
        assert!(
            results
                .iter()
                .filter_map(|r| r.as_ref().err())
                .all(|e| matches!(e, Error::Busy))
        );
        db.transact(64, |tx| charge(tx, 1024, 0, 4096)).unwrap();
        db.transact(64, |tx| charge(tx, 0, 1024, 4096)).unwrap();
        let key = fragments::key("export-retained-bytes").unwrap();
        assert_eq!(
            db.snapshot()
                .get(Table::Reclamation, &key)
                .unwrap()
                .unwrap(),
            4096u64.to_be_bytes()
        );
    }

    #[test]
    fn invalid_accounting_fails_without_mutating_the_counter() {
        let root = directory();
        let db = crate::MetadataEngine::open(root.path(), crate::Options::default()).unwrap();
        let key = fragments::key("export-retained-bytes").unwrap();
        assert!(db.transact(64, |tx| charge(tx, 1, 0, 0)).is_err());
        assert!(
            db.snapshot()
                .get(Table::Reclamation, &key)
                .unwrap()
                .is_none()
        );
        db.transact(64, |tx| {
            tx.put(Table::Reclamation, &key, &u64::MAX.to_be_bytes())
        })
        .unwrap();
        assert!(db.transact(64, |tx| charge(tx, 0, 1, 0)).is_err());
        assert_eq!(
            db.snapshot()
                .get(Table::Reclamation, &key)
                .unwrap()
                .unwrap(),
            u64::MAX.to_be_bytes()
        );
    }
}
