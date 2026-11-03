use procyon_engine::{Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace};
use std::path::Path;

#[derive(Debug)]
pub enum Error {
    Storage(procyon_engine::Error),
    UniqueConstraintFailed,
}

impl From<procyon_engine::Error> for Error {
    fn from(value: procyon_engine::Error) -> Self {
        Self::Storage(value)
    }
}

fn maybe_create_item(
    db: &SingleWriterTxDatabase,
    items: &SingleWriterTxKeyspace,
    uniq: &SingleWriterTxKeyspace,
    id: &str,
    name: &str,
) -> Result<(), Error> {
    let mut tx = db.write_tx();

    if uniq.contains_key(name)? {
        return Err(Error::UniqueConstraintFailed);
    }

    tx.insert(items, id, name);
    tx.insert(uniq, name, id);

    tx.commit()?;

    Ok(())
}

fn main() -> Result<(), Error> {
    let path = Path::new(".procyon_engine_data");

    let db = SingleWriterTxDatabase::builder(path)
        .temporary(true)
        .open()?;

    let items = db.keyspace("items", procyon_engine::KeyspaceCreateOptions::default)?;
    let uniq = db.keyspace("uniq_idx", procyon_engine::KeyspaceCreateOptions::default)?;

    maybe_create_item(&db, &items, &uniq, "a", "Item A")?;
    maybe_create_item(&db, &items, &uniq, "b", "Item B")?;
    maybe_create_item(&db, &items, &uniq, "c", "Item C")?;

    assert!(matches!(
        maybe_create_item(&db, &items, &uniq, "d", "Item A"),
        Err(Error::UniqueConstraintFailed),
    ));

    println!("Listing all unique values and their owners");

    let mut found_count = 0;

    for kv in db.read_tx().iter(uniq.inner()) {
        let (k, v) = kv.into_inner()?;

        println!(
            "unique value: {:?} -> {:?}",
            std::str::from_utf8(&k).unwrap(),
            std::str::from_utf8(&v).unwrap(),
        );

        found_count += 1;
    }

    assert_eq!(3, found_count);

    Ok(())
}
