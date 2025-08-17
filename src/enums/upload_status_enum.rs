use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum UploadStatus {
    Pending,
    Completed,
}
