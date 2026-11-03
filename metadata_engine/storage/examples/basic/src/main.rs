fn main() -> procyon_engine::Result<()> {
    let db = procyon_engine::Database::builder(".procyon_engine_data").open()?;
    let items = db.keyspace("items", procyon_engine::KeyspaceCreateOptions::default)?;

    assert_eq!(0, items.len()?);

    println!("OK");

    Ok(())
}
