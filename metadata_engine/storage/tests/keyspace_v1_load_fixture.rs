use procyon_engine::Database;
use test_log::test;
mod common;

#[test]
fn db_load_v1() -> procyon_engine::Result<()> {
    let folder = common::fixture("v1_keyspace")?;

    let result = Database::builder(folder.path()).open();

    assert!(matches!(
        result,
        // This historical fixture uses a five-byte FJL/BE-u16 header. The
        // current four-byte parser rejects it before journal recovery.
        Err(procyon_engine::Error::InvalidVersion(None))
    ));

    Ok(())
}

#[test]
fn db_load_v1_corrupt_journal() -> procyon_engine::Result<()> {
    let folder = common::fixture("v1_keyspace_corrupt_journal")?;

    let result = Database::builder(folder.path()).open();

    assert!(matches!(
        result,
        Err(procyon_engine::Error::InvalidVersion(None))
    ));

    Ok(())
}
