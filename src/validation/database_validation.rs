use mongodb::bson::doc;
use crate::database_api::database::get_collection;

pub async fn validate_api_key(api_key: &str) -> Result<bool, mongodb::error::Error> {
    get_collection("users")
        .await
        .find_one(doc! { "api_key": api_key })
        .await
        .map(|user| user.is_some())
}

pub async fn validate_uid(uid: &str) -> Result<bool, mongodb::error::Error> {
    get_collection("users")
        .await
        .find_one(doc! { "uid": uid })
        .await
        .map(|user| user.is_some())
}
