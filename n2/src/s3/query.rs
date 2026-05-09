use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct S3BucketQuery {
    #[serde(rename = "list-type")]
    pub list_type: Option<u8>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    #[serde(rename = "max-keys")]
    pub max_keys: Option<usize>,
    #[serde(rename = "continuation-token")]
    pub continuation_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct S3ObjectQuery {
    #[serde(rename = "versionId")]
    pub version_id: Option<String>,
    pub uploads: Option<String>,
    #[serde(rename = "uploadId")]
    pub upload_id: Option<String>,
    #[serde(rename = "partNumber")]
    pub part_number: Option<u32>,
}
