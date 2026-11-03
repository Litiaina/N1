use procyon_engine::{Database, KeyspaceCreateOptions};
use test_log::test;

#[test]
fn db_lock() -> procyon_engine::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    tree.insert("asd", "def")?;
    tree.insert("efg", "hgf")?;
    tree.insert("hij", "wer")?;

    drop(db);

    assert!(matches!(
        Database::builder(&folder).open(),
        Err(procyon_engine::Error::Locked),
    ));

    Ok(())
}
