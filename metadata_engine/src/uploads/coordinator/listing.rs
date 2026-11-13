//! Discover resumable sessions through the existing unfinished-upload index.
//! Completed history never consumes a result slot or adds work to this scan.
use super::{Plan, plan_key};
use crate::{Error, MetadataEngine, RecordKey, Result, Table, fragments, namespace::PendingUpload};
use n1_link::PoolId;

pub(crate) fn list(
    db: MetadataEngine,
    fragment: PoolId,
    durable: bool,
    after: Option<&str>,
    limit: usize,
    now: u64,
) -> Result<(Vec<crate::namespace::UploadHandle>, Option<String>)> {
    if !(1..=1000).contains(&limit) {
        return Err(Error::Invalid("invalid upload page limit".into()));
    }
    let mut cursor = after
        .map(|version| RecordKey::new(fragment, format!("pending/{version}")))
        .transpose()?;
    // Index, upload and coordination plan must belong to the same view: a
    // concurrent finalize/cancel must not turn half of that view into an error.
    let snapshot = db.snapshot();
    let mut sessions = Vec::new();
    loop {
        let rows = snapshot.scan(
            Table::Reclamation,
            fragment,
            "pending/",
            cursor.as_ref(),
            1000,
        )?;
        if rows.is_empty() {
            return Ok((sessions, None));
        }
        cursor = rows.last().map(|(key, _)| key.clone());
        for (key, _) in rows {
            let version = key
                .identifier()
                .strip_prefix("pending/")
                .expect("scoped scan");
            // One-shot uploads also own a pending publication intent; only
            // multipart uploads have a coordination plan and can be resumed.
            let Some(bytes) = snapshot.get(Table::Reclamation, &plan_key(fragment, version)?)?
            else {
                continue;
            };
            let saved: Plan = fragments::decode(bytes)?;
            if saved.cancelled || saved.durable != durable {
                continue;
            }
            let pending: PendingUpload = fragments::decode(
                snapshot
                    .get(Table::Uploads, &RecordKey::new(fragment, version)?)?
                    .ok_or_else(|| Error::Incompatible("upload plan has no reservation".into()))?,
            )?;
            if pending.completed || pending.upload.expires_at <= now {
                continue;
            }
            if saved.handle.version_id != version
                || pending.handle.session_id != saved.handle.session_id
            {
                return Err(Error::Incompatible(
                    "upload plan differs from its reservation".into(),
                ));
            }
            // One eligible lookahead makes continuation truthful even when
            // cancelled, expired or other-mode sessions follow this page.
            if sessions.len() == limit {
                let next = sessions
                    .last()
                    .map(|h: &crate::namespace::UploadHandle| h.version_id.clone());
                return Ok((sessions, next));
            }
            sessions.push(saved.handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Options, namespace::UploadHandle, object_store::ObjectUpload};
    const FRAGMENT: PoolId = PoolId([39; 16]);

    fn version(index: u128) -> String {
        ulid::Ulid::from(index).to_string()
    }
    fn fixture() -> (tempfile::TempDir, MetadataEngine) {
        let mut builder = tempfile::Builder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o700));
        }
        let root = builder.tempdir().unwrap();
        let db = MetadataEngine::open(root.path(), Options::default()).unwrap();
        (root, db)
    }
    fn insert(db: &MetadataEngine, n: u128, durable: bool, completed: bool, expires: u64) {
        let handle = UploadHandle {
            version_id: version(n),
            session_id: version(n + 10000),
            object_key: format!("file-{n}"),
            total_size_bytes: 8192,
            part_size_bytes: 4096,
            expected_parts: 2,
            durable,
        };
        let pending = PendingUpload {
            handle: handle.clone(),
            object_id: version(n + 20000),
            upload: ObjectUpload {
                object_key: "private-test-key".into(),
                total_bytes: 8192,
                part_bytes: 4096,
                parts: 2,
                expires_at: expires,
                durable,
            },
            content_type: "application/octet-stream".into(),
            created_at_unix: 1,
            completed,
        };
        let saved = Plan {
            handle,
            bridge: false,
            cancelled: false,
            durable,
            strict: true,
            content_type: "application/octet-stream".into(),
            acknowledgements_indexed: true,
            staging_parents_persisted: false,
        };
        db.transact(64, |tx| {
            tx.put(
                Table::Uploads,
                &RecordKey::new(FRAGMENT, version(n))?,
                &fragments::encode(&pending)?,
            )?;
            tx.put(
                Table::Reclamation,
                &plan_key(FRAGMENT, &version(n))?,
                &fragments::encode(&saved)?,
            )?;
            if !completed {
                tx.put(
                    Table::Reclamation,
                    &RecordKey::new(FRAGMENT, format!("pending/{}", version(n)))?,
                    b"{}",
                )?;
            }
            Ok(())
        })
        .unwrap();
    }
    #[test]
    fn resumable_pages_skip_history_expiry_and_other_modes_across_scan_boundaries() {
        let (root, db) = fixture();
        insert(&db, 1, false, true, 999);
        // More than one database page of active sessions in the other mode.
        for n in 2..1005 {
            insert(&db, n, true, false, 999);
        }
        insert(&db, 1005, false, false, 10); // expired
        insert(&db, 1006, false, false, 999);
        insert(&db, 1007, false, false, 999);
        let (rows, next) = list(db.clone(), FRAGMENT, false, None, 1, 10).unwrap();
        assert_eq!(rows[0].version_id, version(1006));
        assert_eq!(next, Some(version(1006)));
        let (rows, next) = list(db.clone(), FRAGMENT, false, next.as_deref(), 1, 10).unwrap();
        assert_eq!(rows[0].version_id, version(1007));
        assert_eq!(next, None);
        let (rows, next) = list(db.clone(), PoolId([40; 16]), false, None, 100, 10).unwrap();
        assert!(rows.is_empty());
        assert_eq!(next, None);
        drop(db);
        let db = MetadataEngine::open(root.path(), Options::default()).unwrap();
        assert_eq!(list(db, FRAGMENT, false, None, 100, 10).unwrap().0.len(), 2);
    }
    #[test]
    fn corrupt_plans_are_reported_instead_of_hiding_sessions() {
        let (_root, db) = fixture();
        insert(&db, 1, false, false, 999);
        db.transact(64, |tx| {
            tx.put(
                Table::Reclamation,
                &plan_key(FRAGMENT, &version(1))?,
                b"corrupt",
            )
        })
        .unwrap();
        assert!(list(db, FRAGMENT, false, None, 100, 10).is_err());
    }
}
