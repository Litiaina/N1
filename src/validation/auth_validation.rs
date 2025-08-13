use crate::database::api::get_collection;
use crate::fs::fs_constants::AUTH_COLLECTION;
use mongodb::bson::doc;

pub async fn validate_auth(email: &str, password: &str) -> Result<bool, mongodb::error::Error> {
    get_collection(AUTH_COLLECTION)
        .await
        .find_one(doc! {
            "email": email,
            "password": password
        })
        .await
        .map(|user| user.is_some())
}
