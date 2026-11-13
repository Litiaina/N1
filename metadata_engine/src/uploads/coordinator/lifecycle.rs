//! A payload ACK and the GC claim must serialize through the same transaction.
use super::*;
use crate::{Error, MetadataTransaction, Result, namespace::PendingUpload};

pub(super) fn accepting_parts(
    tx: &mut MetadataTransaction,
    fragment: PoolId,
    version: &str,
) -> Result<Plan> {
    let saved: Plan = fragments::decode(
        tx.get(Table::Reclamation, &plan_key(fragment, version)?)?
            .ok_or(Error::NotFound)?,
    )?;
    let pending: PendingUpload = fragments::decode(
        tx.get(Table::Uploads, &RecordKey::new(fragment, version)?)?
            .ok_or(Error::NotFound)?,
    )?;
    crate::namespace::Namespace::check_upload_owner(tx, fragment, &pending.object_id)?;
    if saved.cancelled
        || pending.completed
        || tx
            .get(
                Table::Reclamation,
                &RecordKey::new(fragment, format!("upload-gc/{version}"))?,
            )?
            .is_some()
    {
        return Err(Error::NameConflict(
            "upload is no longer accepting parts".into(),
        ));
    }
    Ok(saved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MetadataEngine, Options, namespace::Namespace, object_store::ObjectUpload};
    const FRAGMENT: PoolId = PoolId([53; 16]);

    #[test]
    fn reclamation_claim_conflicts_with_an_inflight_ack_and_fences_its_retry_after_reopen() {
        let mut builder = tempfile::Builder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o700));
        }
        let root = builder.tempdir().unwrap();
        let db = MetadataEngine::open(root.path(), Options::default()).unwrap();
        let ns = Namespace::new(db.clone());
        let version = ulid::Ulid::new().to_string();
        let handle = ns
            .reserve_upload(
                FRAGMENT,
                "claimed.bin",
                &version,
                &ulid::Ulid::new().to_string(),
                &ObjectUpload {
                    object_key: "private-test-link".into(),
                    total_bytes: 8192,
                    part_bytes: 4096,
                    parts: 2,
                    expires_at: 1,
                    durable: true,
                },
                "application/octet-stream",
                0,
            )
            .unwrap();
        let plan = Plan {
            handle,
            bridge: false,
            cancelled: false,
            durable: true,
            strict: true,
            content_type: "application/octet-stream".into(),
            acknowledgements_indexed: true,
            staging_parents_persisted: false,
        };
        db.transact(64, |tx| {
            tx.put(
                Table::Reclamation,
                &plan_key(FRAGMENT, &version)?,
                &fragments::encode(&plan)?,
            )
        })
        .unwrap();
        let mut receipt = db.write().unwrap();
        accepting_parts(&mut receipt, FRAGMENT, &version).unwrap();
        crate::uploads::progress::acknowledge(&mut receipt, FRAGMENT, &version, 0).unwrap();
        let (_, work) = ns
            .due_uploads(Some(FRAGMENT), 2, None, 1)
            .unwrap()
            .pop()
            .unwrap();
        assert!(ns.claim_upload(&work, 2).unwrap().is_some());
        assert!(receipt.commit().is_err());
        assert!(
            db.transact(64, |tx| {
                accepting_parts(tx, FRAGMENT, &version)?;
                crate::uploads::progress::acknowledge(tx, FRAGMENT, &version, 0)
            })
            .is_err()
        );
        assert!(
            crate::uploads::progress::uploaded(&db.snapshot(), FRAGMENT, &version, 2)
                .unwrap()
                .is_empty()
        );
        drop(ns);
        drop(db);
        let db = MetadataEngine::open(root.path(), Options::default()).unwrap();
        let staged_receipt =
            RecordKey::new(FRAGMENT, format!("upload-part/{version}/000000")).unwrap();
        assert!(
            db.transact(64, |tx| {
                accepting_parts(tx, FRAGMENT, &version)?;
                tx.put(Table::Reclamation, &staged_receipt, b"must not acknowledge")
            })
            .is_err()
        );
        assert!(
            db.snapshot()
                .get(Table::Reclamation, &staged_receipt)
                .unwrap()
                .is_none()
        );
    }
}
