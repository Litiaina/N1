use crate::enums::transcoder_enums::JobStatus;
use crate::structs::transcode_structs::{
    FFProbeOutput, QualityParameters, TranscodeJob, TranscodeJobStatus, TranscodeRequest,
    TranscodeResponse,
};
use crate::validation::directory_validation::sanitize_path;
use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use mongodb::Database;
use mongodb::bson::{Uuid as BsonUuid, doc};
use std::path::PathBuf;
use serde_json::json;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::task;
use tracing::{error, info};

pub async fn transcode_video_handler(
    State(db): State<Database>,
    Json(payload): Json<TranscodeRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if payload.uid.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "UID cannot be empty".to_string()));
    }

    if payload.source_path.contains("..") {
        return Err((StatusCode::BAD_REQUEST, "Invalid source path".to_string()));
    }

    let sanitized_directory = sanitize_path(&payload.source_path);
    let full_source_path = PathBuf::from(format!(
        "storage-files/{}/{}",
        payload.uid, sanitized_directory
    ));

    if !full_source_path.exists() {
        return Err((StatusCode::NOT_FOUND, "Source file not found".to_string()));
    }

    let job_id = BsonUuid::new();
    let new_job = TranscodeJob {
        job_id,
        status: JobStatus::Processing,
    };

    let jobs_collection = db.collection::<TranscodeJob>("jobs");
    jobs_collection
        .insert_one(new_job)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let source_file_owned = full_source_path.to_string_lossy().to_string();
    let uid_owned = payload.uid.clone();
    let qualities_payload = payload.qualities;
    let include_source = payload.include_source_quality.unwrap_or(true);
    let db_clone = db.clone();

    task::spawn(async move {
        info!(uid = %uid_owned, %job_id, "Starting background transcode job.");

        let mut qualities_to_process: Vec<QualityParameters> = Vec::new();
        if include_source {
            let ffprobe_output = Command::new("ffprobe")
                .arg("-v")
                .arg("quiet")
                .arg("-print_format")
                .arg("json")
                .arg("-show_streams")
                .arg(&source_file_owned)
                .output()
                .await;
            if let Ok(output) = ffprobe_output {
                if let Ok(parsed) = serde_json::from_slice::<FFProbeOutput>(&output.stdout) {
                    let video_stream = parsed.streams.iter().find(|s| s.codec_type == "video");
                    let audio_stream = parsed.streams.iter().find(|s| s.codec_type == "audio");
                    if let (Some(video), Some(audio)) = (video_stream, audio_stream) {
                        qualities_to_process.push(QualityParameters {
                            name: "Source".to_string(),
                            height: video.height.unwrap_or(0),
                            video_bitrate: video
                                .bit_rate
                                .clone()
                                .unwrap_or_else(|| "0".to_string()),
                            audio_bitrate: audio
                                .bit_rate
                                .clone()
                                .unwrap_or_else(|| "0".to_string()),
                        });
                    }
                }
            }
        }
        qualities_to_process.extend(qualities_payload.unwrap_or_else(|| {
            vec![
                QualityParameters {
                    name: "1080p".to_string(),
                    height: 1080,
                    video_bitrate: "5000k".to_string(),
                    audio_bitrate: "192k".to_string(),
                },
                QualityParameters {
                    name: "720p".to_string(),
                    height: 720,
                    video_bitrate: "2800k".to_string(),
                    audio_bitrate: "128k".to_string(),
                },
                QualityParameters {
                    name: "360p".to_string(),
                    height: 360,
                    video_bitrate: "800k".to_string(),
                    audio_bitrate: "96k".to_string(),
                },
            ]
        }));

        let base_output_dir = PathBuf::from(format!("{}_HLS", source_file_owned));
        let _ = std::fs::create_dir_all(&base_output_dir);

        let mut ffmpeg_command = Command::new("ffmpeg");
        ffmpeg_command
            .arg("-i")
            .arg(&source_file_owned)
            .arg("-preset")
            .arg("slow")
            .arg("-g")
            .arg("48")
            .arg("-sc_threshold")
            .arg("0");
        for quality in &qualities_to_process {
            let segment_filename = base_output_dir.join(format!("{}_%03d.ts", quality.name));
            let playlist_filename = base_output_dir.join(format!("{}.m3u8", quality.name));
            ffmpeg_command
                .arg("-map")
                .arg("0:v:0")
                .arg("-map")
                .arg("0:a:0");
            if quality.name == "Source" {
                ffmpeg_command.arg("-c").arg("copy");
            } else {
                let resolution = format!("scale=-2:{}", quality.height);
                ffmpeg_command
                    .arg("-vf")
                    .arg(&resolution)
                    .arg("-c:v")
                    .arg("libx264")
                    .arg("-b:v")
                    .arg(&quality.video_bitrate)
                    .arg("-c:a")
                    .arg("aac")
                    .arg("-b:a")
                    .arg(&quality.audio_bitrate);
            }
            ffmpeg_command
                .arg("-hls_time")
                .arg("4")
                .arg("-hls_playlist_type")
                .arg("vod")
                .arg("-hls_segment_filename")
                .arg(segment_filename)
                .arg(playlist_filename);
        }

        let ffmpeg_process = ffmpeg_command.output().await;

        let final_status = match ffmpeg_process {
            Ok(output) => {
                if output.status.success() {
                    info!(%job_id, "Successfully transcoded video.");
                    let master_playlist_path = base_output_dir.join("master.m3u8");
                    let mut master_playlist_content = String::from("#EXTM3U\n#EXT-X-VERSION:3\n");

                    for quality in &qualities_to_process {
                        let video_bw = quality
                            .video_bitrate
                            .replace('k', "000")
                            .parse::<u32>()
                            .unwrap_or(0);
                        let audio_bw = quality
                            .audio_bitrate
                            .replace('k', "000")
                            .parse::<u32>()
                            .unwrap_or(0);
                        let total_bandwidth = video_bw + audio_bw;

                        master_playlist_content.push_str(&format!(
                            "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION=-1x{}\n{}.m3u8\n",
                            total_bandwidth, quality.height, quality.name
                        ));
                    }

                    if let Ok(mut file) = File::create(master_playlist_path).await {
                        if file
                            .write_all(master_playlist_content.as_bytes())
                            .await
                            .is_ok()
                        {
                            info!(%job_id, "Master playlist created successfully.");
                            JobStatus::Completed
                        } else {
                            let reason = "Failed to write master playlist.".to_string();
                            error!(%job_id, error = %reason);
                            JobStatus::Failed { reason }
                        }
                    } else {
                        let reason = "Failed to create master playlist file.".to_string();
                        error!(%job_id, error = %reason);
                        JobStatus::Failed { reason }
                    }
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                    error!(%job_id, error = %stderr, "FFMPEG transcoding failed.");
                    JobStatus::Failed { reason: stderr }
                }
            }
            Err(e) => {
                let error_msg = e.to_string();
                error!(%job_id, error = %error_msg, "Failed to execute ffmpeg command.");
                JobStatus::Failed { reason: error_msg }
            }
        };

        let jobs_collection = db_clone.collection::<TranscodeJob>("jobs");
        let query = doc! { "_id": job_id };
        let update = doc! { "$set": { "status": mongodb::bson::to_bson(&final_status).unwrap() } };
        if let Err(e) = jobs_collection.update_one(query, update).await {
            error!(%job_id, error = %e, "Failed to update job status in database.");
        }
    });

    let response_body = TranscodeResponse {
        job_id: job_id.to_string(),
        message: "Transcoding job has been started in the background.".to_string(),
    };

    Ok((StatusCode::ACCEPTED, Json(response_body)))
}

pub async fn transcode_status_handler(
    State(db): State<Database>,
    Query(query): Query<TranscodeJobStatus>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let job_id = match BsonUuid::parse_str(&query.job_id) {
        Ok(id) => id,
        Err(_) => return Err((StatusCode::BAD_REQUEST, "Invalid Job ID format".to_string())),
    };

    let jobs_collection = db.collection::<TranscodeJob>("jobs");
    let query = doc! { "_id": job_id };

    match jobs_collection.find_one(query).await {
        Ok(Some(job)) => Ok(Json(json!({"status": job.status}))),
        Ok(None) => Err((StatusCode::NOT_FOUND, "Job not found".to_string())),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}
