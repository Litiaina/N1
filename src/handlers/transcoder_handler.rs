use crate::enums::transcoder_enums::JobStatus;
use crate::structs::transcoder_structs::{
    FFProbeOutput, Quality, Rendition, TranscodeJob, TranscodeRequest, TranscodeResponse,
    TranscodeStatusRequest,
};
use crate::validation::directory_validation::sanitize_path;
use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use mongodb::Database;
use mongodb::bson::{Uuid as BsonUuid, doc};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::{fs, task};
use tracing::{error, info, warn};
use crate::database::database_constants::DATABASE;
use crate::routes::file_system_routes::AppState;

pub async fn transcode_video_handler(
    State(db): State<Arc<AppState>>,
    Json(payload): Json<TranscodeRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if payload.uid.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "UID cannot be empty".to_string()));
    }
    if payload.source_path.contains("..") {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid source path: '..' is not allowed".to_string(),
        ));
    }

    let sanitized_directory = sanitize_path(&payload.source_path);
    let full_source_path = PathBuf::from(format!(
        "storage-files/{}/{}",
        payload.uid, sanitized_directory
    ));

    if !full_source_path.exists() {
        error!(path = %full_source_path.display(), "Source file not found.");
        return Err((StatusCode::NOT_FOUND, "Source file not found".to_string()));
    }

    let jobs_collection = db.db_client.database(DATABASE).collection::<TranscodeJob>("jobs");

    let timeout_duration = chrono::Duration::minutes(5);
    let stale_threshold_timestamp = (Utc::now() - timeout_duration).timestamp_millis();

    let failed_status_bson = match mongodb::bson::to_bson(&JobStatus::Failed {
        reason: "Job timed out due to server inactivity.".to_string(),
    }) {
        Ok(bson) => bson,
        Err(e) => {
            error!(error = %e, "Failed to serialize Failed status for stale job cleanup.");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal configuration error.".to_string(),
            ));
        }
    };

    let stale_job_query = doc! {
        "status": { "$in": ["preparing", "encoding", "packaging"] },
        "last_heartbeat": { "$lt": stale_threshold_timestamp }
    };
    let update_stale = doc! {
        "$set": { "status": failed_status_bson }
    };

    match jobs_collection
        .update_many(stale_job_query, update_stale)
        .await
    {
        Ok(result) => {
            if result.modified_count > 0 {
                warn!("Cleaned up {} stale jobs.", result.modified_count);
            }
        }
        Err(e) => {
            error!(error = %e, "Error during stale job cleanup.");
        }
    }

    let active_job_query = doc! {
        "uid": &payload.uid,
        "source_path": &payload.source_path,
        "status": { "$in": ["queued", "preparing", "encoding", "packaging"] }
    };

    match jobs_collection.find_one(active_job_query).await {
        Ok(Some(active_job)) => {
            warn!(job_id = %active_job.job_id, "Attempted to transcode a video that is already being processed.");
            return Err((
                StatusCode::CONFLICT,
                format!(
                    "This video is already being processed under job ID: {}",
                    active_job.job_id
                ),
            ));
        }
        Ok(None) => {
            info!(
                "No active job found for {}. Proceeding with new transcode.",
                &payload.source_path
            );
        }
        Err(e) => {
            error!(error = %e, "Database error while checking for active jobs.");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
        }
    }

    let job_id = BsonUuid::new();
    let new_job = TranscodeJob {
        job_id,
        uid: payload.uid.clone(),
        source_path: payload.source_path.clone(),
        status: JobStatus::Queued,
        renditions: Vec::new(),
        progress: 0.0,
        last_heartbeat: Utc::now().timestamp_millis(),
    };

    if let Err(e) = jobs_collection.insert_one(&new_job).await {
        error!(error = %e, "Failed to insert new job into database.");
        return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
    }

    let db_clone = db.db_client.database(DATABASE);
    task::spawn(async move {
        if let Err(e) = process_transcoding_job(db_clone, job_id, full_source_path, payload).await {
            error!(%job_id, error = %e, "Transcoding job failed.");
            let jobs_collection = db.db_client.database(DATABASE).collection::<TranscodeJob>("jobs");
            let query = doc! { "_id": job_id };
            let update = doc! { "$set": { "status": mongodb::bson::to_bson(&JobStatus::Failed { reason: e }).unwrap_or_default(), "progress": 100.0 } };
            if let Err(db_err) = jobs_collection.update_one(query, update).await {
                error!(%job_id, error = %db_err, "Failed to update job status to Failed in DB.");
            }
        }
    });

    let response_body = TranscodeResponse {
        job_id: job_id.to_string(),
        message: "Transcoding job has been queued.".to_string(),
    };

    Ok((StatusCode::ACCEPTED, Json(response_body)))
}

async fn process_transcoding_job(
    db: Database,
    job_id: BsonUuid,
    source_path: PathBuf,
    request: TranscodeRequest,
) -> Result<(), String> {
    let jobs_collection = db.collection::<TranscodeJob>("jobs");
    let source_str = source_path.to_string_lossy().to_string();

    update_job_status(&jobs_collection, job_id, JobStatus::Preparing, 0.0).await;
    update_job_heartbeat(&jobs_collection, job_id).await;

    let probe_output = probe_video(&source_str).await?;
    let video_stream = probe_output
        .streams
        .iter()
        .find(|s| s.codec_type == "video")
        .ok_or_else(|| "No video stream found in source file.".to_string())?;

    let source_height = video_stream.height.unwrap_or(0);
    if source_height == 0 {
        return Err("Could not determine source video height.".to_string());
    }
    let source_width = video_stream.width.unwrap_or(0);
    let aspect_ratio = if source_width > 0 {
        source_width as f64 / source_height as f64
    } else {
        16.0 / 9.0
    };

    let preset = request.preset;
    let fps = request.fps;

    let renditions = generate_adaptive_ladder(source_height, aspect_ratio, request.qualities);
    if renditions.is_empty() {
        return Err("No suitable transcode qualities could be determined.".to_string());
    }

    let query = doc! { "_id": job_id };
    let update =
        doc! { "$set": { "renditions": mongodb::bson::to_bson(&renditions).unwrap_or_default() } };
    if let Err(e) = jobs_collection.update_one(query, update).await {
        warn!(%job_id, error = %e, "Failed to update job with renditions list.");
    }

    update_job_status(&jobs_collection, job_id, JobStatus::Encoding, 5.0).await;
    let base_output_dir = PathBuf::from(format!("{}_HLS", source_str));

    if base_output_dir.exists() {
        fs::remove_dir_all(&base_output_dir)
            .await
            .map_err(|e| format!("Failed to remove existing output directory: {}", e))?;
    }

    fs::create_dir_all(&base_output_dir)
        .await
        .map_err(|e| format!("Failed to create output directory: {}", e))?;

    let total_renditions = renditions.len() as f32;
    for (i, rendition) in renditions.iter().enumerate() {
        update_job_heartbeat(&jobs_collection, job_id).await;

        let progress_start = 5.0 + (i as f32 / total_renditions) * 90.0;
        update_job_progress(&jobs_collection, job_id, progress_start).await;

        transcode_rendition(
            &source_str,
            &base_output_dir,
            rendition,
            preset.as_deref(),
            fps,
        )
        .await
        .map_err(|e| {
            error!(%job_id, quality = %rendition.name, error = %e, "Rendition transcode failed.");
            e
        })?;
    }

    update_job_heartbeat(&jobs_collection, job_id).await;
    update_job_status(&jobs_collection, job_id, JobStatus::Packaging, 95.0).await;
    generate_master_playlist(&base_output_dir, &renditions)
        .await
        .map_err(|e| format!("Failed to generate master playlist: {}", e))?;

    update_job_status(&jobs_collection, job_id, JobStatus::Completed, 100.0).await;

    Ok(())
}

async fn probe_video(source_file: &str) -> Result<FFProbeOutput, String> {
    let ffprobe_output = Command::new("ffprobe")
        .arg("-v")
        .arg("quiet")
        .arg("-print_format")
        .arg("json")
        .arg("-show_streams")
        .arg(source_file)
        .output()
        .await
        .map_err(|e| format!("Failed to execute ffprobe: {}", e))?;

    if !ffprobe_output.status.success() {
        let stderr = String::from_utf8_lossy(&ffprobe_output.stderr);
        return Err(format!("ffprobe failed: {}", stderr));
    }

    serde_json::from_slice::<FFProbeOutput>(&ffprobe_output.stdout)
        .map_err(|e| format!("Failed to parse ffprobe output: {}", e))
}

fn generate_adaptive_ladder(
    source_height: i64,
    aspect_ratio: f64,
    custom_qualities: Option<Vec<Quality>>,
) -> Vec<Rendition> {
    let mut renditions = Vec::new();

    let qualities = custom_qualities.unwrap_or_else(|| {
        vec![
            Quality {
                height: 2160,
                bitrate: 18000,
                audio_bitrate: 256,
            },
            Quality {
                height: 1440,
                bitrate: 12000,
                audio_bitrate: 192,
            },
            Quality {
                height: 1080,
                bitrate: 5000,
                audio_bitrate: 192,
            },
            Quality {
                height: 720,
                bitrate: 2800,
                audio_bitrate: 128,
            },
            Quality {
                height: 480,
                bitrate: 1400,
                audio_bitrate: 96,
            },
            Quality {
                height: 360,
                bitrate: 800,
                audio_bitrate: 96,
            },
            Quality {
                height: 144,
                bitrate: 400,
                audio_bitrate: 64,
            },
        ]
    });

    for quality in qualities {
        if quality.height <= source_height {
            let width = (quality.height as f64 * aspect_ratio).round() as i64;
            let even_width = width - (width % 2);

            renditions.push(Rendition {
                name: format!("{}p", quality.height),
                width: even_width,
                height: quality.height,
                video_bitrate: quality.bitrate,
                audio_bitrate: quality.audio_bitrate,
            });
        }
    }

    if renditions.is_empty() {
        let width = (source_height as f64 * aspect_ratio).round() as i64;
        renditions.push(Rendition {
            name: format!("{}p", source_height),
            width: width - (width % 2),
            height: source_height,
            video_bitrate: 6000,
            audio_bitrate: 192,
        });
    }

    renditions
}

async fn transcode_rendition(
    source_file: &str,
    output_dir: &Path,
    rendition: &Rendition,
    preset: Option<&str>,
    fps: Option<u32>,
) -> Result<(), String> {
    let rendition_name = &rendition.name;
    let segment_filename = output_dir.join(format!("{}_%04d.ts", rendition_name));
    let playlist_filename = output_dir.join(format!("{}.m3u8", rendition_name));
    let log_file_path = output_dir.join(format!("ffmpeg_{}_pass", rendition_name));

    let fps_str = fps.map_or("30".to_string(), |f| f.to_string());
    let vf_filter = format!("scale={}:{}", rendition.width, rendition.height);
    let video_bitrate_str = format!("{}k", rendition.video_bitrate);
    let audio_bitrate_str = format!("{}k", rendition.audio_bitrate);
    let preset_str = preset.unwrap_or("slow");
    let base_args = vec!["-y", "-i", source_file];

    let mut pass1_args = base_args.clone();
    pass1_args.extend(vec![
        "-an",
        "-vf",
        &vf_filter,
        "-r",
        &fps_str,
        "-vcodec",
        "libx264",
        "-preset",
        preset_str,
        "-b:v",
        &video_bitrate_str,
        "-pass",
        "1",
        "-passlogfile",
        log_file_path.to_str().unwrap(),
        "-f",
        "null",
        if cfg!(windows) { "NUL" } else { "/dev/null" },
    ]);

    let pass1_output = Command::new("ffmpeg")
        .args(&pass1_args)
        .output()
        .await
        .map_err(|e| format!("FFMPEG pass 1 command failed to execute: {}", e))?;

    if !pass1_output.status.success() {
        let stderr = String::from_utf8_lossy(&pass1_output.stderr);
        return Err(format!(
            "FFMPEG pass 1 failed. Stderr: {}. Check the log file at: {}",
            stderr,
            log_file_path.with_extension("log").display()
        ));
    }

    let mut pass2_args = base_args.clone();
    pass2_args.extend(vec![
        "-vf",
        &vf_filter,
        "-r",
        &fps_str,
        "-c:v",
        "libx264",
        "-preset",
        preset_str,
        "-b:v",
        &video_bitrate_str,
        "-pass",
        "2",
        "-passlogfile",
        log_file_path.to_str().unwrap(),
        "-c:a",
        "aac",
        "-b:a",
        &audio_bitrate_str,
        "-hls_time",
        "4",
        "-hls_playlist_type",
        "vod",
        "-hls_segment_filename",
        segment_filename.to_str().unwrap(),
        playlist_filename.to_str().unwrap(),
    ]);

    let pass2_output = Command::new("ffmpeg")
        .args(&pass2_args)
        .output()
        .await
        .map_err(|e| format!("FFMPEG pass 2 execution failed: {}", e))?;

    if !pass2_output.status.success() {
        let stderr = String::from_utf8_lossy(&pass2_output.stderr);
        return Err(format!(
            "FFMPEG pass 2 failed. Stderr: {}. Check the log file at: {}",
            stderr,
            log_file_path.with_extension("log").display()
        ));
    }

    let log_file_main = log_file_path.with_extension("log");
    let log_file_mbtree = log_file_path.with_extension("log.mbtree");
    let _ = fs::remove_file(log_file_main).await;
    let _ = fs::remove_file(log_file_mbtree).await;

    Ok(())
}

async fn generate_master_playlist(
    output_dir: &Path,
    renditions: &[Rendition],
) -> Result<(), String> {
    let mut master_playlist_content = String::from("#EXTM3U\n#EXT-X-VERSION:3\n");

    for quality in renditions {
        let total_bandwidth = (quality.video_bitrate + quality.audio_bitrate) * 1000;
        master_playlist_content.push_str(&format!(
            "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={}x{},NAME=\"{}\"\n{}.m3u8\n",
            total_bandwidth, quality.width, quality.height, quality.name, quality.name
        ));
    }

    let master_playlist_path = output_dir.join("master.m3u8");
    let mut file = fs::File::create(&master_playlist_path)
        .await
        .map_err(|e| format!("Failed to create master playlist file: {}", e))?;

    file.write_all(master_playlist_content.as_bytes())
        .await
        .map_err(|e| format!("Failed to write to master playlist: {}", e))?;

    Ok(())
}

async fn update_job_heartbeat(collection: &mongodb::Collection<TranscodeJob>, job_id: BsonUuid) {
    let query = doc! { "_id": job_id };
    let update = doc! { "$set": { "last_heartbeat": Utc::now().timestamp_millis() } };
    if let Err(e) = collection.update_one(query, update).await {
        warn!(%job_id, error = %e, "Failed to update job heartbeat.");
    }
}

async fn update_job_status(
    collection: &mongodb::Collection<TranscodeJob>,
    job_id: BsonUuid,
    status: JobStatus,
    progress: f32,
) {
    let status_bson = match mongodb::bson::to_bson(&status) {
        Ok(bson) => bson,
        Err(e) => {
            error!(%job_id, error = %e, "Failed to serialize job status.");
            return;
        }
    };
    let query = doc! { "_id": job_id };
    let update = doc! { "$set": { "status": status_bson, "progress": progress as f64 } };
    if let Err(e) = collection.update_one(query, update).await {
        error!(%job_id, error = %e, "Failed to update job status in database.");
    }
}

async fn update_job_progress(
    collection: &mongodb::Collection<TranscodeJob>,
    job_id: BsonUuid,
    progress: f32,
) {
    let query = doc! { "_id": job_id };
    let update = doc! { "$set": { "progress": progress as f64 } };
    if let Err(e) = collection.update_one(query, update).await {
        error!(%job_id, error = %e, "Failed to update job progress in database.");
    }
}

pub async fn transcode_status_handler(
    State(db): State<Arc<AppState>>,
    Query(query): Query<TranscodeStatusRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let job_id = match BsonUuid::parse_str(&query.job_id) {
        Ok(id) => id,
        Err(_) => return Err((StatusCode::BAD_REQUEST, "Invalid Job ID format".to_string())),
    };

    let jobs_collection = db.db_client.database(DATABASE).collection::<TranscodeJob>("jobs");
    let query = doc! { "_id": job_id };

    match jobs_collection.find_one(query).await {
        Ok(Some(job)) => Ok(Json(json!({
            "job_id": job.job_id.to_string(),
            "status": job.status,
            "progress": job.progress,
            "renditions": job.renditions,
        }))),
        Ok(None) => Err((StatusCode::NOT_FOUND, "Job not found".to_string())),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}
