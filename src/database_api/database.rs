use mongodb::{Client, Collection, Database};
use mongodb::bson::Document;

pub async fn database_connect() -> mongodb::error::Result<Database> {
    let uri = "mongodb://localhost:27017";
    let client = Client::with_uri_str(uri).await?;
    let database = client.database("litiaina-cloud-database");
    Ok(database)
}

pub async fn get_collection(name: &str) -> Collection<Document> {
    match database_connect().await {
        Ok(database) => database.collection(name),
        Err(e) => panic!("Error: Database connection failed: {:?}", e),
    }
}