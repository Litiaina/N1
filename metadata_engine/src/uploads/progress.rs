//! Durable acknowledgements for named multipart uploads. These are metadata,
//! not object-engine receipts or substitutes for payload integrity checks.
//! A bit is committed only after the native part operation has succeeded.
use crate::{Error, MetadataSnapshot, MetadataTransaction, RecordKey, Result, Table};
use n1_link::PoolId;
use std::collections::BTreeMap;

const PARTS_PER_PAGE: u64 = 256;
const PAGE_BYTES: usize = PARTS_PER_PAGE as usize / 8;

fn prefix(version: &str) -> String {
    format!("upload-progress/{version}/")
}
fn key(fragment: PoolId, version: &str, page: u64) -> Result<RecordKey> {
    RecordKey::new(fragment, format!("{}{page:06}", prefix(version)))
}
fn page(bytes: &[u8]) -> Result<[u8; PAGE_BYTES]> {
    bytes
        .try_into()
        .map_err(|_| Error::Incompatible("invalid upload acknowledgement page".into()))
}

pub(crate) fn acknowledge(
    tx: &mut MetadataTransaction,
    fragment: PoolId,
    version: &str,
    part: u64,
) -> Result<()> {
    let key = key(fragment, version, part / PARTS_PER_PAGE)?;
    let mut bits = tx
        .get(Table::Reclamation, &key)?
        .map(|bytes| page(&bytes))
        .transpose()?
        .unwrap_or([0; PAGE_BYTES]);
    let bit = part % PARTS_PER_PAGE;
    bits[bit as usize / 8] |= 1 << (bit % 8);
    tx.put(Table::Reclamation, &key, &bits)
}

pub(crate) fn uploaded(
    snapshot: &MetadataSnapshot,
    fragment: PoolId,
    version: &str,
    expected: u64,
) -> Result<Vec<u64>> {
    let prefix = prefix(version);
    let mut after = None;
    let mut uploaded = Vec::new();
    loop {
        let rows = snapshot.scan(Table::Reclamation, fragment, &prefix, after.as_ref(), 1000)?;
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|(key, _)| key.clone());
        for (record, bytes) in rows {
            let number = record
                .identifier()
                .strip_prefix(&prefix)
                .and_then(|n| n.parse::<u64>().ok())
                .ok_or_else(|| Error::Incompatible("invalid upload acknowledgement key".into()))?;
            if record != key(fragment, version, number)?
                || number >= expected.div_ceil(PARTS_PER_PAGE)
            {
                return Err(Error::Incompatible(
                    "upload acknowledgement exceeds its shape".into(),
                ));
            }
            for (byte_index, byte) in page(&bytes)?.iter().enumerate() {
                for bit in 0..8 {
                    if byte & (1 << bit) != 0 {
                        let part = number * PARTS_PER_PAGE + byte_index as u64 * 8 + bit;
                        if part >= expected {
                            return Err(Error::Incompatible(
                                "upload acknowledgement exceeds its shape".into(),
                            ));
                        }
                        uploaded.push(part);
                    }
                }
            }
        }
    }
    Ok(uploaded)
}

pub(crate) fn replace(
    tx: &mut MetadataTransaction,
    fragment: PoolId,
    version: &str,
    verified: &[u64],
) -> Result<()> {
    let mut pages = BTreeMap::<u64, [u8; PAGE_BYTES]>::new();
    for part in verified {
        let bits = pages
            .entry(part / PARTS_PER_PAGE)
            .or_insert([0; PAGE_BYTES]);
        let bit = part % PARTS_PER_PAGE;
        bits[bit as usize / 8] |= 1 << (bit % 8);
    }
    remove(tx, fragment, version)?;
    for (number, bits) in pages {
        tx.put(Table::Reclamation, &key(fragment, version, number)?, &bits)?;
    }
    Ok(())
}

/// Publication/cancellation removes the bounded bitmap in its own transaction.
/// At the public maximum of 131,072 parts this is only 512 small records.
pub(crate) fn remove(tx: &mut MetadataTransaction, fragment: PoolId, version: &str) -> Result<()> {
    let prefix = prefix(version);
    let mut after = None;
    loop {
        let rows = tx.scan(Table::Reclamation, fragment, &prefix, after.as_ref(), 1000)?;
        if rows.is_empty() {
            return Ok(());
        }
        after = rows.last().map(|(key, _)| key.clone());
        for (key, _) in rows {
            tx.remove(Table::Reclamation, &key)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MetadataEngine, Options};
    const FRAGMENT: PoolId = PoolId([43; 16]);
    #[test]
    fn maximum_shape_reconciliation_and_cleanup_fit_in_one_transaction() {
        let (_dir, db) = fixture();
        let parts = (0..131072).filter(|i| i % 3 != 1).collect::<Vec<_>>();
        db.transact(64, |tx| replace(tx, FRAGMENT, "maximum", &parts))
            .unwrap();
        assert_eq!(
            uploaded(&db.snapshot(), FRAGMENT, "maximum", 131072).unwrap(),
            parts
        );
        assert_eq!(
            db.snapshot()
                .scan(Table::Reclamation, FRAGMENT, &prefix("maximum"), None, 1000)
                .unwrap()
                .len(),
            512
        );
        db.transact(64, |tx| remove(tx, FRAGMENT, "maximum"))
            .unwrap();
        assert!(
            uploaded(&db.snapshot(), FRAGMENT, "maximum", 131072)
                .unwrap()
                .is_empty()
        );
    }
    fn fixture() -> (tempfile::TempDir, MetadataEngine) {
        let mut builder = tempfile::Builder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o700));
        }
        let dir = builder.tempdir().unwrap();
        let db = MetadataEngine::open(dir.path(), Options::default()).unwrap();
        (dir, db)
    }
    #[test]
    fn concurrent_acknowledgements_merge_and_survive_reopen_without_crossing_uploads_or_fragments()
    {
        let (dir, db) = fixture();
        std::thread::scope(|scope| {
            for writer in 0..8 {
                let db = db.clone();
                scope.spawn(move || {
                    for part in (writer..1025).step_by(8) {
                        db.transact(1024, |tx| acknowledge(tx, FRAGMENT, "one", part))
                            .unwrap();
                    }
                });
            }
        });
        assert!(
            uploaded(&db.snapshot(), PoolId([44; 16]), "one", 1025)
                .unwrap()
                .is_empty()
        );
        assert!(
            uploaded(&db.snapshot(), FRAGMENT, "two", 1025)
                .unwrap()
                .is_empty()
        );
        drop(db);
        let db = MetadataEngine::open(dir.path(), Options::default()).unwrap();
        assert_eq!(
            uploaded(&db.snapshot(), FRAGMENT, "one", 1025).unwrap(),
            (0..1025).collect::<Vec<_>>()
        );
        db.transact(64, |tx| remove(tx, FRAGMENT, "one")).unwrap();
        assert!(
            uploaded(&db.snapshot(), FRAGMENT, "one", 1025)
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn an_uncommitted_acknowledgement_is_never_reported_and_corrupt_pages_fail_closed() {
        let (_dir, db) = fixture();
        let mut tx = db.write().unwrap();
        acknowledge(&mut tx, FRAGMENT, "one", 0).unwrap();
        drop(tx);
        assert!(
            uploaded(&db.snapshot(), FRAGMENT, "one", 1)
                .unwrap()
                .is_empty()
        );
        db.transact(64, |tx| {
            tx.put(Table::Reclamation, &key(FRAGMENT, "one", 0)?, &[1])
        })
        .unwrap();
        assert!(matches!(
            uploaded(&db.snapshot(), FRAGMENT, "one", 1),
            Err(Error::Incompatible(_))
        ));
        db.transact(64, |tx| {
            remove(tx, FRAGMENT, "one")?;
            acknowledge(tx, FRAGMENT, "one", 1)
        })
        .unwrap();
        assert!(matches!(
            uploaded(&db.snapshot(), FRAGMENT, "one", 1),
            Err(Error::Incompatible(_))
        ));
    }
}
