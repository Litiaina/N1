use mongodb::bson::doc;
use crate::database::api::get_collection;

pub async fn validate_auth(email: &str, password: &str) -> Result<bool, mongodb::error::Error> {
    get_collection("users")
        .await
        .find_one(doc! {
            "email": email,
            "password": password
        })
        .await
        .map(|user| user.is_some())
}