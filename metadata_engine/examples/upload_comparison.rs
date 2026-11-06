//! Matched live metadata/native upload measurement; never changes server policy.
use clap::Parser;
use cli_core::{Credentials, Result, result};
use futures_util::StreamExt;
use futures_util::future::try_join_all;
use n1_native_client::{Command, N1NativeClient};
use reqwest::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
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
    metadata_pid: Option<u32>,
    /// Named API credentials/URL; defaults to the local deployment access file.
    #[arg(long)]
    access_file: Option<PathBuf>,
    #[arg(long, default_value = "durable")]
    session_mode: String,
    /// Verify every downloaded byte via length and the expected source SHA-256.
    #[arg(long)]
    full_download: bool,
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
        return Err(format!(
            "HTTP status {actual}, expected {status}: {}",
            String::from_utf8_lossy(&bytes[..bytes.len().min(2048)])
        )
        .into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
async fn part_ack(response: reqwest::Response) -> Result<()> {
    let actual = response.status().as_u16();
    let bytes = response.bytes().await?;
    if actual != 200 {
        return Err(format!(
            "part status {actual}: {}",
            String::from_utf8_lossy(&bytes[..bytes.len().min(2048)])
        )
        .into());
    }
    // Original NOA sends a plain-text ACK; current NOA sends JSON. Both must
    // be completely drained before measuring completion/reusing the transport.
    Ok(())
}
fn latency(mut values: Vec<f64>) -> Value {
    values.sort_by(f64::total_cmp);
    let p = |q: f64| values[((values.len() as f64 * q).ceil() as usize).saturating_sub(1)];
    json!({"samples":values.len(),"p50_ms":p(0.5),"p95_ms":p(0.95),"p99_ms":p(0.99),"max_ms":p(1.0)})
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if !matches!(args.mode.as_str(), "native" | "metadata" | "ncx")
        || args.bytes < 65536
        || ![8 << 20, 32 << 20].contains(&args.part_bytes)
        || !(1..=16).contains(&args.concurrency)
        || !matches!(args.session_mode.as_str(), "standard" | "durable")
        || (args.mode == "ncx" && args.metadata_pid.is_some())
        || (args.mode == "metadata" && args.metadata_pid.is_none())
        || (args.mode == "native" && args.full_download)
    {
        return Err(
            "invalid comparison mode, process set, session mode or payload geometry".into(),
        );
    }
    std::fs::create_dir(&args.output)?;
    let ca = args.root.join("certs/test-ca.crt");
    let credentials = Credentials::load(&args.root.join("config/object/client.json"))?;
    let mut native = if args.mode == "native" {
        Some(credentials.connect(None, Some(&ca)).await?)
    } else {
        None
    };
    let client = Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(&ca)?)?)
        .timeout(Duration::from_secs(1800))
        .build()?;
    let access: Value = serde_json::from_slice(&std::fs::read(
        args.access_file
            .as_ref()
            .cloned()
            .unwrap_or_else(|| args.root.join("access.json")),
    )?)?;
    let base = format!(
        "{}/noa/v1",
        access["metadata_url"]
            .as_str()
            .ok_or("missing metadata URL")?
            .trim_end_matches('/')
    );
    let logged = http(
        client
            .post(format!("{base}/auth/fragment"))
            .json(&json!({
                "fragment": access["fragment"],
                "secret": access["fragment_key"],
                "read": true,
                "write": true,
                "modify": true,
                "delete": true,
                "expires_in": 21600
            }))
            .send()
            .await?,
        200,
    )
    .await?;
    let token = logged["access_token"]
        .as_str()
        .ok_or("missing token")?
        .to_owned();
    let named = format!(
        "__n1-upload-comparison/{}/{}.bin",
        ulid::Ulid::new(),
        args.mode
    );
    let shape = json!({
        "content_type": "application/octet-stream",
        "total_size_bytes": args.bytes,
        "part_size_bytes": args.part_bytes,
        "expected_parts": args.bytes.div_ceil(args.part_bytes)
    });
    let multipart = format!(
        "{base}/upload/multipart{}",
        if args.session_mode == "durable" {
            "/durable"
        } else {
            ""
        }
    );
    let expected_hash = if args.full_download {
        let bytes = args.bytes;
        Some(
            tokio::task::spawn_blocking(move || {
                let mut hash = Sha256::new();
                let mut offset = 0;
                while offset < bytes {
                    let length = (bytes - offset).min(8 << 20) as usize;
                    hash.update(payload(offset, length));
                    offset += length as u64;
                }
                format!("{:x}", hash.finalize())
            })
            .await?,
        )
    } else {
        None
    };
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
                .as_mut()
                .ok_or("missing native connection")?
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
                .post(format!("{multipart}/init/{named}?durability=strict"))
                .bearer_auth(&token)
                .json(&shape)
                .send()
                .await?,
            if args.mode == "ncx" { 201 } else { 200 },
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
        let multipart = multipart.clone();
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
                    native.write_at(upload["object_key"].as_str().ok_or("missing ticket")?.into(), index*part, length, &mut &data[..]).await?;
                } else {
                    part_ack(client.put(format!("{multipart}/{}/{index}?durability=strict", upload["version_id"].as_str().ok_or("missing version")?))
                        .bearer_auth(&token).header("content-type","application/octet-stream").body(data).send().await?).await?;
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
            native = Some(connection);
        }
    }
    let parts_seconds = parts_start.elapsed().as_secs_f64();
    let final_start = Instant::now();
    if args.mode == "native" {
        result(
            native
                .as_mut()
                .ok_or("missing native connection")?
                .control(Command::UploadFinalize {
                    object_key: upload["object_key"]
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
                    "{multipart}/{}/{}/finalize",
                    upload["version_id"].as_str().ok_or("missing version")?,
                    upload["session_id"].as_str().ok_or("missing session")?
                ))
                .bearer_auth(&token)
                .query(&[("object_key", &named)])
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
    let mut download_report = Value::Null;
    if let Some(expected_hash) = &expected_hash {
        let directory = args.output.join("download");
        std::fs::create_dir(&directory)?;
        let download_watch =
            resources::Watch::start(args.object_pid, args.metadata_pid, &directory)?;
        let start = Instant::now();
        let response = client
            .get(format!("{base}/objects/download"))
            .query(&[("object_key", &named)])
            .bearer_auth(&token)
            .send()
            .await?;
        let headers_seconds = start.elapsed().as_secs_f64();
        if response.status() != 200 {
            return Err(format!("full download returned {}", response.status()).into());
        }
        let mut stream = response.bytes_stream();
        let mut hash = Sha256::new();
        let mut length = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            length += chunk.len() as u64;
            hash.update(&chunk);
        }
        let seconds = start.elapsed().as_secs_f64();
        let actual_hash = format!("{:x}", hash.finalize());
        if length != args.bytes || &actual_hash != expected_hash {
            return Err("full download length/SHA-256 mismatch".into());
        }
        download_report = json!({
            "bytes": length,
            "seconds": seconds,
            "mib_per_second": length as f64/1048576.0/seconds,
            "headers_seconds": headers_seconds,
            "sha256": actual_hash,
            "resources": download_watch.finish().await?
        });
    }
    // Verification is outside the measured upload interval.
    for offset in if args.full_download {
        vec![]
    } else {
        vec![0, (args.bytes / 2) / 65536 * 65536, args.bytes - 65536]
    } {
        let mut body = Vec::new();
        if args.mode == "native" {
            native
                .as_mut()
                .ok_or("missing native connection")?
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
    let report = json!({
        "passed": true,
        "mode": args.mode,
        "bytes": args.bytes,
        "part_bytes": args.part_bytes,
        "concurrency": args.concurrency,
        "durability": "strict",
        "init_seconds": init_seconds,
        "parts_seconds": parts_seconds,
        "finalize_seconds": finalize_seconds,
        "upload_seconds": seconds,
        "upload_mib_per_second": args.bytes as f64 / 1048576.0 / seconds,
        "part_latency": latency(rows.iter().map(|v|v["milliseconds"].as_f64().unwrap()).collect()),
        "resources": resource,
        "verified_ranges": if args.full_download {
            0
        } else {
            3
        },
        "verified_bytes": if args.full_download {
            args.bytes
        } else {
            196608
        },
        "full_download_verified": args.full_download,
        "download": download_report,
        "session_mode": args.session_mode,
        "expected_source_sha256": expected_hash,
        "retained_named_path": if args.mode!="native" {
            Some(named)
        } else {
            None
        }
    });
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
