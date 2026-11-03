use procyon_engine::{Database, FormatVersion};
use test_log::test;
mod common;

#[test]
fn db_load_v2() -> procyon_engine::Result<()> {
    let folder = common::fixture("v2_keyspace")?;

    let result = Database::builder(folder.path()).open();

    assert!(matches!(
        result,
        Err(procyon_engine::Error::InvalidVersion(Some(
            FormatVersion::V2
        )))
    ));

    Ok(())
}

#[test]
fn db_load_v2_corrupt_journal() -> procyon_engine::Result<()> {
    let folder = common::fixture("v2_keyspace_corrupt_journal")?;

    let result = Database::builder(folder.path()).open();

    assert!(matches!(
        result,
        // V2 is rejected before its corrupt journal is interpreted by V3.
        Err(procyon_engine::Error::InvalidVersion(Some(
            FormatVersion::V2
        ))),
    ));

    Ok(())
}
