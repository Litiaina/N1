//! Fault fixtures preserve acknowledged bytes and never repair by overwriting.
use m1_storage::{Database, Options};
use std::{fs, ops::Bound, os::unix::fs::PermissionsExt};

fn directory() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}

fn options() -> Options {
    Options {
        cache_bytes: 64 * 1024,
        journal_bytes: 1024 * 1024 * 1024,
        ..Options::default()
    }
}

fn file(root: &std::path::Path) -> std::path::PathBuf {
    root.join("store-00000000000000000001.m1")
}

fn fixture() -> (tempfile::TempDir, Vec<u8>, Vec<u8>, Vec<u8>, usize) {
    let root = directory();
    let db = Database::open(root.path(), options()).unwrap();
    let mut tx = db.write().unwrap();
    tx.put(b"\0first", b"acknowledged original").unwrap();
    tx.put(b"\x01history", b"acknowledged original").unwrap();
    tx.commit().unwrap();
    let prefix = fs::read(file(root.path())).unwrap().len();
    let mut tx = db.write().unwrap();
    tx.put(b"\0first", b"replacement").unwrap();
    tx.put(b"\x01history", b"replacement").unwrap();
    tx.put(b"\x02new", b"atomic companion").unwrap();
    tx.commit().unwrap();
    drop(db);
    let bytes = fs::read(file(root.path())).unwrap();
    let format = fs::read(root.path().join("FORMAT")).unwrap();
    let current = fs::read(root.path().join("CURRENT")).unwrap();
    (root, bytes, format, current, prefix)
}

#[test]
fn every_truncated_suffix_recovers_exactly_the_previous_committed_transaction() {
    let (_root, bytes, format, current, prefix) = fixture();
    // Sweep every byte boundary in the final, 4 KiB-aligned commit group.
    for cut in prefix..=bytes.len() {
        let root = directory();
        fs::write(root.path().join("FORMAT"), &format).unwrap();
        fs::write(root.path().join("CURRENT"), &current).unwrap();
        fs::write(file(root.path()), &bytes[..cut]).unwrap();
        for name in ["FORMAT", "CURRENT", "store-00000000000000000001.m1"] {
            fs::set_permissions(root.path().join(name), fs::Permissions::from_mode(0o600)).unwrap();
        }
        let db = Database::open(root.path(), options())
            .unwrap_or_else(|error| panic!("cut={cut}: {error}"));
        let view = db.snapshot();
        let expected: &[u8] = if cut == bytes.len() {
            b"replacement"
        } else {
            b"acknowledged original"
        };
        assert_eq!(
            view.get(b"\0first").unwrap().unwrap(),
            expected,
            "cut={cut}"
        );
        assert_eq!(
            view.get(b"\x01history").unwrap().unwrap(),
            expected,
            "cut={cut}"
        );
        assert_eq!(view.get(b"\x02new").unwrap().is_some(), cut == bytes.len());
        drop(view);
        drop(db);
        assert_eq!(
            fs::metadata(file(root.path())).unwrap().len() as usize,
            if cut == bytes.len() { cut } else { prefix }
        );
    }
}

#[test]
fn checksums_reject_committed_corruption_without_truncating_evidence() {
    let (root, bytes, _, _, _) = fixture();
    // Header, value, tree, commit and commit padding all participate in checksums.
    for offset in [4096usize, 4120, 4130, 4190, bytes.len() - 1] {
        let mut damaged = bytes.clone();
        damaged[offset] ^= 0x80;
        fs::write(file(root.path()), &damaged).unwrap();
        assert!(
            Database::open(root.path(), options()).is_err(),
            "offset={offset}"
        );
        assert_eq!(fs::read(file(root.path())).unwrap(), damaged);
    }
}

#[test]
fn damaged_record_length_cannot_discard_previously_acknowledged_data() {
    let (root, bytes, _, _, _) = fixture();
    // A length bit flip can describe a plausible payload beyond EOF. Validate
    // the header independently before classifying that record as a torn tail.
    for byte in 4096 + 8..4096 + 12 {
        for bit in 0..8 {
            let mut damaged = bytes.clone();
            damaged[byte] ^= 1 << bit;
            fs::write(file(root.path()), &damaged).unwrap();
            assert!(
                Database::open(root.path(), options()).is_err(),
                "length byte={byte}, bit={bit}"
            );
            assert_eq!(fs::read(file(root.path())).unwrap(), damaged);
        }
    }
}

#[test]
fn deleted_metadata_is_reclaimed_by_checkpoint_and_old_snapshots_stay_readable() {
    let root = directory();
    let db = Database::open(root.path(), options()).unwrap();
    let mut tx = db.write().unwrap();
    for index in 0..128u64 {
        let mut key = vec![0];
        key.extend_from_slice(&index.to_be_bytes());
        tx.put(&key, &vec![index as u8; 64 * 1024]).unwrap();
    }
    tx.commit().unwrap();
    let old = db.snapshot();
    let before = db.disk_bytes().unwrap();
    let mut tx = db.write().unwrap();
    for index in 0..128u64 {
        let mut key = vec![0];
        key.extend_from_slice(&index.to_be_bytes());
        tx.remove(&key).unwrap();
    }
    tx.commit().unwrap();
    db.checkpoint().unwrap();
    assert_eq!(
        old.range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .count(),
        128
    );
    assert_eq!(
        db.snapshot()
            .range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .count(),
        0
    );
    drop(old);
    assert!(db.disk_bytes().unwrap() < before / 10);
    drop(db);
    assert_eq!(
        Database::open(root.path(), options())
            .unwrap()
            .snapshot()
            .range(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn independent_instances_cannot_lock_the_same_database_through_an_alias() {
    let root = directory();
    let db = Database::open(root.path(), options()).unwrap();
    let alias = root
        .path()
        .join("..")
        .join(root.path().file_name().unwrap());
    assert!(Database::open(alias, options()).is_err());
    drop(db);
    Database::open(root.path(), options()).unwrap();
}

#[test]
fn unfinished_checkpoint_header_does_not_hide_the_authoritative_generation() {
    let root = directory();
    let db = Database::open(root.path(), options()).unwrap();
    let mut tx = db.write().unwrap();
    tx.put(b"\0original", b"acknowledged").unwrap();
    tx.commit().unwrap();
    drop(db);
    let abandoned = root.path().join("store-00000000000000000002.m1");
    fs::write(&abandoned, b"M1DA").unwrap();
    fs::set_permissions(&abandoned, fs::Permissions::from_mode(0o600)).unwrap();
    let db = Database::open(root.path(), options()).unwrap();
    db.checkpoint().unwrap();
    assert_eq!(
        db.snapshot().get(b"\0original").unwrap().unwrap(),
        b"acknowledged"
    );
    assert_eq!(db.verify().unwrap().records, 1);
    assert_eq!(fs::read(abandoned).unwrap(), b"M1DA");
}

#[test]
fn explicit_integrity_check_bypasses_warm_cached_index_nodes() {
    use std::os::unix::fs::FileExt;
    let root = directory();
    let db = Database::open(root.path(), options()).unwrap();
    let mut tx = db.write().unwrap();
    tx.put(b"\0key", b"value").unwrap();
    tx.commit().unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.snapshot().get(b"\0key").unwrap().unwrap(), b"value");
    let current = fs::read(root.path().join("CURRENT")).unwrap();
    let generation = u64::from_be_bytes(current[8..16].try_into().unwrap());
    let root_offset = u64::from_be_bytes(current[24..32].try_into().unwrap());
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.path().join(format!("store-{generation:020}.m1")))
        .unwrap();
    file.write_all_at(&[0xff], root_offset + 28).unwrap();
    file.sync_all().unwrap();
    assert!(db.verify().is_err());
    assert!(db.write().is_err());
}
