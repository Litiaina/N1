use mongodb::{
    Client, IndexModel,
    bson::{Document, doc},
    options::{ClientOptions, IndexOptions},
};
use std::error::Error;
use std::time::Duration;

use crate::{
    database::database_constants::OBJECT_STORAGE_DATABASE, statics::database_statics::DATABASE_URL,
};

pub async fn setup_database_locks() -> Result<Client, Box<dyn Error>> {
    let client_options = ClientOptions::parse(&*DATABASE_URL).await?;
    let client = Client::with_options(client_options)?;
    let db = client.database(OBJECT_STORAGE_DATABASE);
    let locks_collection = db.collection::<Document>("upload_locks");
    let unique_index_options = IndexOptions::builder().unique(true).build();

    let uid_index = IndexModel::builder()
        .keys(doc! { "uid": 1 })
        .options(unique_index_options)
        .build();

    let ttl_index_options = IndexOptions::builder()
        .expire_after(Duration::from_secs(30))
        .build();

    let locked_at_index = IndexModel::builder()
        .keys(doc! { "locked_at": 1 })
        .options(ttl_index_options)
        .build();

    locks_collection
        .create_indexes(vec![uid_index, locked_at_index])
        .await?;

    Ok(client)
}
