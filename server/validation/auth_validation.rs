use crate::database::api::get_collection;
use crate::fs::fs_constants::AUTH_COLLECTION;
use mongodb::bson::doc;

pub async fn validate_auth(email: &str, password: &str) -> Result<Option<String>, mongodb::error::Error> {
    let collection = get_collection(AUTH_COLLECTION).await;
    let user_doc_option = collection
        .find_one(doc! { "email": email, "password": password })
        .await?;
    Ok(user_doc_option.and_then(|doc| {
        doc.get_str("uid").ok().map(String::from)
    }))
}
