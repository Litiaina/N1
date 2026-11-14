//! Replace only an unpublished upload's absent backing allocation. Public
//! version/session identity and client receipts remain owned by M1.
use super::*;

pub(crate) fn replace_upload_allocation(
    tx: &mut MetadataTransaction,
    fragment: PoolId,
    version: &str,
    expected_key: &str,
    upload: &ObjectUpload,
) -> Result<()> {
    let key = RecordKey::new(fragment, version)?;
    let mut pending: PendingUpload = read(tx, Table::Uploads, &key)?.ok_or(Error::NotFound)?;
    let target = Target {
        fragment,
        object: pending.object_id.clone(),
        version: version.into(),
    };
    if pending.completed
        || pending.upload.object_key != expected_key
        || tx
            .get(
                Table::Reclamation,
                &RecordKey::new(fragment, format!("pending/{version}"))?,
            )?
            .is_none()
        || tx
            .get(Table::Reclamation, &upload_claim(&target)?)?
            .is_some()
        || tx
            .get(Table::Reclamation, &purging(expected_key)?)?
            .is_some()
    {
        return Err(Error::NameConflict(
            "upload allocation is no longer replaceable".into(),
        ));
    }
    Namespace::check_upload_owner(tx, fragment, &pending.object_id)?;
    if upload.total_bytes != pending.upload.total_bytes
        || upload.part_bytes != pending.upload.part_bytes
        || upload.parts != pending.upload.parts
        || upload.durable != pending.upload.durable
    {
        return Err(Error::Invalid(
            "replacement allocation changes the upload shape".into(),
        ));
    }
    unindex_upload(tx, &target, pending.upload.expires_at)?;
    tx.remove(Table::Reclamation, &reference(&target, expected_key)?)?;
    index_upload(tx, fragment, &pending.object_id, version, upload)?;
    pending.upload = upload.clone();
    put(tx, Table::Uploads, &key, &pending)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MetadataEngine, Options};

    #[test]
    fn allocation_reference_and_expiry_switch_atomically_and_gc_fences_replacement() {
        let mut builder = tempfile::Builder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o700));
        }
        let root = builder.tempdir().unwrap();
        let db = MetadataEngine::open(root.path(), Options::default()).unwrap();
        let ns = Namespace::new(db.clone());
        let fragment = PoolId([57; 16]);
        let version = Ulid::new().to_string();
        let session = Ulid::new().to_string();
        let original = ObjectUpload {
            object_key: "private-old-allocation".into(),
            total_bytes: 10,
            part_bytes: 5,
            parts: 2,
            expires_at: 100,
            durable: false,
        };
        let handle = ns
            .reserve_upload(
                fragment,
                "replace.bin",
                &version,
                &session,
                &original,
                "application/octet-stream",
                0,
            )
            .unwrap();
        let pending = ns.pending(fragment, &version, None).unwrap();
        let target = Target {
            fragment,
            object: pending.object_id,
            version: version.clone(),
        };
        let mut replacement = original.clone();
        replacement.object_key = "private-new-allocation".into();
        replacement.expires_at = 200;
        db.transact(64, |tx| {
            replace_upload_allocation(tx, fragment, &version, &original.object_key, &replacement)
        })
        .unwrap();
        assert!(
            ns.due_uploads(Some(fragment), 199, None, 10)
                .unwrap()
                .is_empty()
        );
        let work = ns.due_uploads(Some(fragment), 201, None, 10).unwrap();
        assert_eq!(work.len(), 1);
        let saved = ns.pending(fragment, &version, None).unwrap();
        assert_eq!(
            serde_json::to_value(saved.handle).unwrap(),
            serde_json::to_value(handle).unwrap()
        );
        assert_eq!(saved.upload.object_key, replacement.object_key);
        assert!(
            db.snapshot()
                .get(
                    Table::Reclamation,
                    &reference(&target, &original.object_key).unwrap()
                )
                .unwrap()
                .is_none()
        );
        assert!(
            db.snapshot()
                .get(
                    Table::Reclamation,
                    &reference(&target, &replacement.object_key).unwrap()
                )
                .unwrap()
                .is_some()
        );
        let mut stale = db.write().unwrap();
        let mut third = replacement.clone();
        third.object_key = "private-third-allocation".into();
        replace_upload_allocation(
            &mut stale,
            fragment,
            &version,
            &replacement.object_key,
            &third,
        )
        .unwrap();
        assert!(ns.claim_upload(&work[0].1, 201).unwrap().is_some());
        assert!(stale.commit().is_err());
        assert!(
            db.transact(64, |tx| replace_upload_allocation(
                tx,
                fragment,
                &version,
                &replacement.object_key,
                &third
            ))
            .is_err()
        );
        let bytes = db
            .snapshot()
            .get(Table::Uploads, &RecordKey::new(fragment, &version).unwrap())
            .unwrap()
            .unwrap();
        let pending: PendingUpload = decode(bytes).unwrap();
        assert_eq!(pending.upload.object_key, replacement.object_key);
    }
}
