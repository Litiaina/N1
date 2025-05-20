use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct FileDetail {
    pub name: String,
    pub created: u64,
    pub file_type: String,
    pub size: u64,
}
