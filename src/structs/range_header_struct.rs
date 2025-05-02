use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct RangeHeader {
    pub range: Option<String>,
}