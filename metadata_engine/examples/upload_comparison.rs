//! Matched live metadata/native upload measurement; never changes server policy.
use clap::Parser;
use cli_core::{Credentials, Result, result};
use futures_util::future::try_join_all;
use n1_native_client::{Command, N1NativeClient};
use reqwest::Client;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
#[path = "upload_comparison/resources.rs"]
mod resources;

const IDENTITY_BLOCK: u64 = 32 << 20;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    mode: String,
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 15_u64 << 30)]
    bytes: u64,
    #[arg(long, default_value_t = 32_u64 << 20)]
    part_bytes: u64,
    #[arg(long, default_value_t = 1)]
    concurrency: usize,
    #[arg(long)]
    object_pid: u32,
    #[arg(long)]
    metadata_pid: u32,
}
fn payload(offset: u64, length: usize) -> Vec<u8> {
    let mut bytes = (0..65536)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>()
        .repeat(length.div_ceil(65536));
    bytes.truncate(length);
    // Same bytes as the retained 15 GiB witness, independent of request size.
    let mut boundary = offset.div_ceil(IDENTITY_BLOCK) * IDENTITY_BLOCK;
    while boundary < offset + length as u64 {
        let index = (boundary / IDENTITY_BLOCK).to_be_bytes();
        let start = (boundary - offset) as usize;
        let count = 8.min(length - start);
        bytes[start..start + count].copy_from_slice(&index[..count]);
        boundary += IDENTITY_BLOCK;
    }
    bytes
}
fn expected(offset: u64, length: usize) -> Vec<u8> {
    (0..length)
        .map(|n| {
            let at = offset + n as u64;
            if at % IDENTITY_BLOCK < 8 {
                (at / IDENTITY_BLOCK).to_be_bytes()[(at % IDENTITY_BLOCK) as usize]
            } else {
                ((at % 65536) % 251) as u8
            }
        })
        .collect()
}
async fn http(response: reqwest::Response, status: u16) -> Result<Value> {
    let actual = response.status().as_u16();
    let bytes = response.bytes().await?;
    if actual != status {
        return Err(format!("HTTP status {actual}, expected {status}").into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn latency(mut values: Vec<f64>) -> Value {
    values.sort_by(f64::total_cmp);
    let p = |q: f64| values[((values.len() as f64 * q).ceil() as usize).saturating_sub(1)];
    json!({"samples":values.len(),"p50_ms":p(0.5),"p95_ms":p(0.95),"p99_ms":p(0.99),"max_ms":p(1.0)})
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if !matches!(args.mode.as_str(), "native" | "metadata")
        || args.bytes < 65536
        || ![8 << 20, 32 << 20].contains(&args.part_bytes)
        || !(1..=16).contains(&args.concurrency)
    {
        return Err("mode=native|metadata, parts=8|32 MiB, concurrency=1..16 required".into());
    }
    std::fs::create_dir(&args.output)?;
    let ca = args.root.join("certs/test-ca.crt");
    let credentials = Credentials::load(&args.root.join("config/object/client.json"))?;
    let mut native = credentials.connect(None, Some(&ca)).await?;
    let client = Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(&ca)?)?)
        .timeout(Duration::from_secs(1800))
        .build()?;
    let access: Value = serde_json::from_slice(&std::fs::read(args.root.join("access.json"))?)?;
    let base = format!(
        "{}/noa/v1",
        access["metadata_url"]
            .as_str()
            .ok_or("missing metadata URL")?
            .trim_end_matches('/')
    );
    let logged = http(client.post(format!("{base}/auth/fragment"))
        .json(&json!({"fragment":access["fragment"],"secret":access["fragment_key"],"read":true,"write":true,"delete":true,"expires_in":3600}))
        .send().await?, 200).await?;
    let token = logged["access_token"]
        .as_str()
        .ok_or("missing token")?
        .to_owned();
    let named = format!(
        "__n1-upload-comparison/{}/{}.bin",
        ulid::Ulid::new(),
        args.mode
    );
    let shape = json!({"content_type":"application/octet-stream","total_size_bytes":args.bytes,"part_size_bytes":args.part_bytes,"expected_parts":args.bytes.div_ceil(args.part_bytes)});
    let mut connections = Vec::new();
    if args.mode == "native" {
        for _ in 0..args.concurrency {
            connections.push(credentials.connect(None, Some(&ca)).await?);
        }
    }
    let watch = resources::Watch::start(args.object_pid, args.metadata_pid, &args.output)?;
    let started = Instant::now();
    let init_start = Instant::now();
    let upload = if args.mode == "native" {
        result(
            native
                .control(Command::UploadInit {
                    pool: None,
                    total_bytes: args.bytes,
                    part_bytes: args.part_bytes,
                    durable: true,
                    ttl_seconds: 86400,
                })
                .await?,
        )?
    } else {
        http(
            client
                .post(format!(
                    "{base}/upload/multipart/durable/init/{named}?durability=strict"
                ))
                .bearer_auth(&token)
                .json(&shape)
                .send()
                .await?,
            200,
        )
        .await?
    };
    let init_seconds = init_start.elapsed().as_secs_f64();
    std::fs::write(
        args.output.join("private-upload.json"),
        serde_json::to_vec(&upload)?,
    )?;
    let count = args.bytes.div_ceil(args.part_bytes);
    let acknowledged = Arc::new(AtomicU64::new(0));
    let parts_start = Instant::now();
    let mut workers = Vec::new();
    for worker in 0..args.concurrency {
        let mut connection: Option<N1NativeClient> = if args.mode == "native" {
            Some(connections.remove(0))
        } else {
            None
        };
        let client = client.clone();
        let base = base.clone();
        let token = token.clone();
        let upload = upload.clone();
        let acknowledged = acknowledged.clone();
        let (bytes, part, concurrency) = (args.bytes, args.part_bytes, args.concurrency);
        workers.push(tokio::spawn(async move {
            let mut rows = Vec::new();
            for index in (worker as u64..count).step_by(concurrency) {
                let length = (bytes - index * part).min(part);
                let data = payload(index * part, length as usize);
                let start = Instant::now();
                if let Some(native) = &mut connection {
                    native.upload_part(upload["upload_key"].as_str().ok_or("missing ticket")?.into(), index, length, &mut &data[..]).await?;
                } else {
                    http(client.put(format!("{base}/upload/multipart/durable/{}/{index}", upload["version_id"].as_str().ok_or("missing version")?))
                        .bearer_auth(&token).body(data).send().await?, 200).await?;
                }
                rows.push(json!({"part":index,"bytes":length,"milliseconds":start.elapsed().as_secs_f64()*1000.0}));
                let done = acknowledged.fetch_add(length, Ordering::Relaxed) + length;
                if done.is_multiple_of(1 << 30) { eprintln!("{done}/{bytes} bytes durably acknowledged"); }
            }
            Ok::<_, cli_core::Error>((rows, connection))
        }));
    }
    let mut rows = Vec::new();
    for worker in try_join_all(workers).await? {
        let (completed, connection) = worker?;
        rows.extend(completed);
        if let Some(connection) = connection {
            // Keep the connection that carried parts active for finalization.
            // The initial control connection may have hit its idle timeout
            // while the separate workers spent minutes uploading to HDDs.
            native = connection;
        }
    }
    let parts_seconds = parts_start.elapsed().as_secs_f64();
    let final_start = Instant::now();
    if args.mode == "native" {
        result(
            native
                .control(Command::UploadFinalize {
                    upload_key: upload["upload_key"]
                        .as_str()
                        .ok_or("missing ticket")?
                        .into(),
                })
                .await?,
        )?;
    } else {
        http(
            client
                .post(format!(
                    "{base}/upload/multipart/durable/{}/{}/finalize",
                    upload["version_id"].as_str().ok_or("missing version")?,
                    upload["session_id"].as_str().ok_or("missing session")?
                ))
                .bearer_auth(&token)
                .json(&shape)
                .send()
                .await?,
            200,
        )
        .await?;
    }
    let finalize_seconds = final_start.elapsed().as_secs_f64();
    let seconds = started.elapsed().as_secs_f64();
    let resource = watch.finish().await?;
    rows.sort_by_key(|v| v["part"].as_u64());
    std::fs::write(args.output.join("parts.json"), serde_json::to_vec(&rows)?)?;
    // Matched sample verification is outside the measured upload interval.
    for offset in [0, (args.bytes / 2) / 65536 * 65536, args.bytes - 65536] {
        let mut body = Vec::new();
        if args.mode == "native" {
            native
                .download(
                    upload["object_key"]
                        .as_str()
                        .ok_or("missing object key")?
                        .into(),
                    offset,
                    Some(65536),
                    &mut body,
                )
                .await?;
        } else {
            let response = client
                .get(format!("{base}/objects/get/{named}"))
                .bearer_auth(&token)
                .header("range", format!("bytes={offset}-{}", offset + 65535))
                .send()
                .await?;
            if response.status() != 206 {
                return Err("range verification failed".into());
            }
            body.extend_from_slice(&response.bytes().await?);
        }
        if body != expected(offset, 65536) {
            return Err("sample payload mismatch".into());
        }
    }
    let report = json!({"passed":true,"mode":args.mode,"bytes":args.bytes,"part_bytes":args.part_bytes,"concurrency":args.concurrency,
        "durability":"strict","init_seconds":init_seconds,"parts_seconds":parts_seconds,"finalize_seconds":finalize_seconds,
        "upload_seconds":seconds,"upload_mib_per_second":args.bytes as f64 / 1048576.0 / seconds,
        "part_latency":latency(rows.iter().map(|v|v["milliseconds"].as_f64().unwrap()).collect()),"resources":resource,
        "verified_ranges":3,"verified_bytes":196608,"full_download_verified":false,"retained_named_path":if args.mode=="metadata" {Some(named)} else {None}});
    std::fs::write(
        args.output.join("PASSED.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{report}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_geometry_preserves_the_same_payload() {
        for offset in [0, 8 << 20, 24 << 20, 32 << 20, 15_u64 << 30] {
            let part = payload(offset, 8 << 20);
            for within in [0, 65536, (8 << 20) - 65536] {
                assert_eq!(
                    &part[within..within + 65536],
                    expected(offset + within as u64, 65536)
                );
            }
        }
    }
}
