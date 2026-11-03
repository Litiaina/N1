use tokio::task::spawn_blocking;

#[tokio::main]
async fn main() -> procyon_engine::Result<()> {
    let db = procyon_engine::Database::builder(".procyon_engine_data").open()?;
    let items = db.keyspace("items", procyon_engine::KeyspaceCreateOptions::default)?;

    {
        let items = items.clone();
        spawn_blocking(move || items.insert("hello", "world"))
            .await
            .expect("join failed")?;
    }

    let item = {
        let items = items.clone();
        spawn_blocking(move || items.get("hello"))
            .await
            .expect("join failed")?
    };

    let item = item.expect("should exist");

    assert_eq!(b"world", &*item);

    println!("OK");

    Ok(())
}
