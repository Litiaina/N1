use serde::Deserialize;

#[derive(Deserialize)]
pub struct DeleteFileParams {
    pub file_to_delete: String,
}
