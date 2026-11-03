// Regression test for large journal values.

use procyon_engine::{Database, KeyspaceCreateOptions, KvSeparationOptions};

#[test_log::test]
fn journal_recover_large_value() -> procyon_engine::Result<()> {
    let folder = tempfile::tempdir()?;

    let large_value = "a".repeat(128_000);

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        tree.insert("a", &large_value)?;
        tree.insert("b", "b")?;
    }

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert_eq!(large_value.as_bytes(), &*tree.get("a")?.unwrap());
        assert_eq!(b"b", &*tree.get("b")?.unwrap());
    }

    Ok(())
}

#[test_log::test]
fn journal_recover_large_value_blob() -> procyon_engine::Result<()> {
    let folder = tempfile::tempdir()?;

    let large_value = "a".repeat(128_000);

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;
        tree.insert("a", &large_value)?;
        tree.insert("b", "b")?;
    }

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;
        assert_eq!(large_value.as_bytes(), &*tree.get("a")?.unwrap());
        assert_eq!(b"b", &*tree.get("b")?.unwrap());
    }

    Ok(())
}
