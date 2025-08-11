use serde::{Deserialize, Serialize};
use mongodb::bson::Uuid as BsonUuid;
use crate::enums::transcoder_enums::JobStatus;

#[derive(Serialize, Deserialize)]
pub struct TranscodeJob {
    #[serde(rename = "_id")]
    pub job_id: BsonUuid,
    pub status: JobStatus,
}

#[derive(Deserialize, Clone, Debug)]
pub struct QualityParameters {
    pub name: String,
    pub height: u32,
    pub video_bitrate: String,
    pub audio_bitrate: String,
}

#[derive(Deserialize)]
pub struct TranscodeRequest {
    pub uid: String,
    pub source_path: String,
    pub qualities: Option<Vec<QualityParameters>>,
    pub include_source_quality: Option<bool>,
}

#[derive(Serialize)]
pub struct TranscodeResponse {
    pub job_id: String,
    pub message: String,
}

#[derive(Deserialize, Debug)]
pub struct FFProbeStream {
    pub codec_type: String,
    pub height: Option<u32>,
    pub bit_rate: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct FFProbeOutput {
    pub streams: Vec<FFProbeStream>,
}

#[derive(Deserialize, Serialize)]
pub struct TranscodeJobStatus {
    pub job_id: String,
}