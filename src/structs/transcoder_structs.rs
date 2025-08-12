use serde::{Deserialize, Serialize};
use mongodb::bson::Uuid as BsonUuid;
use crate::enums::transcoder_enums::JobStatus;

#[derive(Serialize, Deserialize, Debug)]
pub struct TranscodeJob {
    #[serde(rename = "_id")]
    pub job_id: BsonUuid,
    pub uid: String,
    pub source_path: String,
    pub status: JobStatus,
    pub progress: f32,
    pub renditions: Vec<Rendition>,
    pub last_heartbeat: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Rendition {
    pub name: String,
    pub width: i64,
    pub height: i64,
    pub video_bitrate: u32,
    pub audio_bitrate: u32,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Quality {
    pub height: i64,
    pub bitrate: u32,
    pub audio_bitrate: u32,
}

#[derive(Deserialize)]
pub struct TranscodeRequest {
    pub uid: String,
    pub source_path: String,
    pub qualities: Option<Vec<Quality>>,
    pub preset: Option<String>,
    pub fps: Option<u32>,
}

#[derive(Serialize)]
pub struct TranscodeResponse {
    pub job_id: String,
    pub message: String,
}

#[derive(Deserialize, Debug)]
pub struct FFProbeStream {
    pub codec_type: String,
    pub width: Option<i64>,
    pub height: Option<i64>,
}

#[derive(Deserialize, Debug)]
pub struct FFProbeOutput {
    pub streams: Vec<FFProbeStream>,
}

#[derive(Deserialize, Serialize)]
pub struct TranscodeStatusRequest {
    pub job_id: String,
}
