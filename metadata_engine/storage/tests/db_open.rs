use procyon_engine::Database;

#[test_log::test]
fn db_open() -> procyon_engine::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let _db = Database::builder(&folder).open()?;
    }

    // DB should not be locked
    {
        let _db = Database::builder(&folder).open()?;
    }

    Ok(())
}

#[test_log::test]
fn db_open_with_keyspace() -> procyon_engine::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let _keyspace = db.keyspace("hello", Default::default)?;
    }

    // DB should not be locked
    {
        let _db = Database::builder(&folder).open()?;
    }

    Ok(())
}
