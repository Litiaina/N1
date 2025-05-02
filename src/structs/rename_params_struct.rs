use serde::Deserialize;

#[derive(Deserialize)]
pub struct RenameParams {
    pub file_to_modify: String,
    pub new_file_name: String,
}