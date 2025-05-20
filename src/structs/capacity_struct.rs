use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct CapacityDetailParams {
    pub uid: String
}
