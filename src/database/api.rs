use crate::database::database_constants::{DATABASE, DATABASE_URL};
use mongodb::bson::Document;
use mongodb::{Client, Collection, Database};

pub async fn database_connect() -> mongodb::error::Result<Database> {
    let client = Client::with_uri_str(DATABASE_URL).await?;
    let database = client.database(DATABASE);
    Ok(database)
}

pub async fn get_collection(name: &str) -> Collection<Document> {
    match database_connect().await {
        Ok(database) => database.collection(name),
        Err(e) => panic!("Error: Database connection failed: {:?}", e),
    }
}
