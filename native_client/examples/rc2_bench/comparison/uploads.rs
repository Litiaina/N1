//! Standalone object-path timings. No metadata process or database is started.
use super::super::{
    Object, Payload, Phase, Result, SMALL, append, expected, field, get, init, value,
};
use super::{Client, inventory, start, wait};
use n1_native_client::{Command, N1NativeClient};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

async fn record(
    rows: &mut Vec<Value>,
    events: &Path,
    phase: Phase,
    pid: u32,
    name: &str,
    bytes: u64,
    samples: Vec<f64>,
) -> Result<()> {
    let row = phase.finish(pid, name, bytes, samples).await?;
    println!(
        "{name}: {:.6}s",
        row["wall_seconds"].as_f64().ok_or("phase duration")?
    );
    append(events, &row)?;
    rows.push(row);
    Ok(())
}

async fn connect(credentials: &Value) -> Result<N1NativeClient> {
    let mut client = N1NativeClient::connect(&field(credentials, "native_url")?).await?;
    client
        .auth_fragment(
            &field(credentials, "fragment")?,
            &field(credentials, "secret")?,
        )
        .await?;
    Ok(client)
}

async fn worker(
    mut client: N1NativeClient,
    count: usize,
    block: Arc<Vec<u8>>,
    hash: String,
) -> Result<(Vec<Object>, Vec<f64>)> {
    let mut objects = Vec::new();
    let mut samples = Vec::new();
    for _ in 0..count {
        let timer = Instant::now();
        let object = init(&mut client, SMALL, super::BLOCK, hash.clone()).await?;
        client
            .put(
                object.key.clone(),
                SMALL,
                &mut Payload::new(block.clone(), SMALL),
            )
            .await?;
        samples.push(timer.elapsed().as_secs_f64());
        objects.push(object);
    }
    Ok((objects, samples))
}

pub(super) async fn trial(
    binary: &Path,
    architecture: &str,
    roots: &[PathBuf; 2],
    block: Arc<Vec<u8>>,
    smoke: bool,
) -> Result<Value> {
    let (mut owned, client) = start(binary, architecture, roots).await?;
    let Client::Link(mut client) = client else {
        return Err("upload paths require the key-only native contract".into());
    };
    let pid = owned.0.id();
    let credentials: Value =
        serde_json::from_slice(&fs::read(roots[0].join("service.credentials.json"))?)?;
    let events = roots[0].join("measurements.jsonl");
    let count = if smoke { 3 } else { 100 };
    let concurrency = if smoke { 2 } else { 16 };
    let bytes = if smoke {
        2 * 1024 * 1024
    } else {
        256 * 1024 * 1024
    };
    let part_bytes = if smoke { 1024 * 1024 } else { 8 * 1024 * 1024 };
    let parts = bytes / part_bytes;
    let small_hash = expected(&block, SMALL);
    let multipart_hash = expected(&block, bytes);
    let mut rows = Vec::new();
    let mut objects = Vec::new();

    let phase = Phase::start(pid)?;
    let mut samples = Vec::new();
    for _ in 0..count {
        let timer = Instant::now();
        objects.push(init(&mut client, SMALL, super::BLOCK, small_hash.clone()).await?);
        samples.push(timer.elapsed().as_secs_f64());
    }
    record(
        &mut rows,
        &events,
        phase,
        pid,
        "batch_begin_100",
        0,
        samples,
    )
    .await?;
    let phase = Phase::start(pid)?;
    let timer = Instant::now();
    client
        .put_batch(
            objects.iter().map(|o| o.key.clone()).collect(),
            SMALL * count as u64,
            &mut std::io::Cursor::new(block[..SMALL as usize].repeat(count)),
        )
        .await?;
    record(
        &mut rows,
        &events,
        phase,
        pid,
        "batch_put_100",
        SMALL * count as u64,
        vec![timer.elapsed().as_secs_f64()],
    )
    .await?;

    for durable in [false, true] {
        let name = if durable { "durable" } else { "standard" };
        let phase = Phase::start(pid)?;
        let timer = Instant::now();
        let initialized = value(
            client
                .control(Command::UploadInit {
                    pool: None,
                    total_bytes: bytes,
                    part_bytes,
                    durable,
                    ttl_seconds: 3600,
                })
                .await?,
        )?;
        let key = field(&initialized, "object_key")?;
        if initialized["parts"].as_u64() != Some(parts) {
            return Err("unexpected multipart geometry".into());
        }
        record(
            &mut rows,
            &events,
            phase,
            pid,
            &format!("multipart_{name}_begin"),
            0,
            vec![timer.elapsed().as_secs_f64()],
        )
        .await?;
        for half in 0..2 {
            let phase = Phase::start(pid)?;
            let mut samples = Vec::new();
            for part in half * parts / 2..(half + 1) * parts / 2 {
                let timer = Instant::now();
                client
                    .write_at(
                        key.clone(),
                        part * part_bytes,
                        part_bytes,
                        &mut Payload::new(block.clone(), part_bytes),
                    )
                    .await?;
                samples.push(timer.elapsed().as_secs_f64());
            }
            record(
                &mut rows,
                &events,
                phase,
                pid,
                &format!("multipart_{name}_parts_half_{half}"),
                bytes / 2,
                samples,
            )
            .await?;
            if half == 0 {
                // Reconnection is real; authentication is outside status timing.
                drop(client);
                client = connect(&credentials).await?;
                let phase = Phase::start(pid)?;
                let timer = Instant::now();
                let status = value(
                    client
                        .control(Command::UploadStatus {
                            object_key: key.clone(),
                            start: 0,
                            limit: parts,
                        })
                        .await?,
                )?;
                let missing = serde_json::from_value::<Vec<u64>>(status["missing_parts"].clone())?;
                if missing != (parts / 2..parts).collect::<Vec<_>>() || status["finalized"] != false
                {
                    return Err("resume status disagrees with acknowledged parts".into());
                }
                record(
                    &mut rows,
                    &events,
                    phase,
                    pid,
                    &format!("multipart_{name}_resume_status"),
                    0,
                    vec![timer.elapsed().as_secs_f64()],
                )
                .await?;
            }
        }
        for repeated in [false, true] {
            let phase = Phase::start(pid)?;
            let timer = Instant::now();
            let finalized = value(
                client
                    .control(Command::UploadFinalize {
                        object_key: key.clone(),
                    })
                    .await?,
            )?;
            if finalized["object_key"] != key || finalized["already_finalized"] != repeated {
                return Err("finalization identity/idempotence mismatch".into());
            }
            record(
                &mut rows,
                &events,
                phase,
                pid,
                &format!(
                    "multipart_{name}_finalize{}",
                    if repeated { "_repeat" } else { "" }
                ),
                0,
                vec![timer.elapsed().as_secs_f64()],
            )
            .await?;
        }
        objects.push(Object {
            key: key.clone(),
            ticket: key,
            bytes,
            hash: multipart_hash.clone(),
        });
    }

    let mut clients = Vec::new();
    for _ in 0..concurrency {
        clients.push(connect(&credentials).await?);
    }
    let phase = Phase::start(pid)?;
    let workers = clients
        .into_iter()
        .enumerate()
        .map(|(i, c)| {
            let assigned = count / concurrency + usize::from(i < count % concurrency);
            tokio::spawn(worker(c, assigned, block.clone(), small_hash.clone()))
        })
        .collect::<Vec<_>>();
    let mut results = Vec::new();
    // Drain every started client before propagating either worker or task errors.
    for worker in workers {
        results.push(worker.await);
    }
    let mut samples = Vec::new();
    for result in results {
        let (uploaded, latencies) = result??;
        objects.extend(uploaded);
        samples.extend(latencies);
    }
    record(
        &mut rows,
        &events,
        phase,
        pid,
        "concurrent_normal_put_100",
        SMALL * count as u64,
        samples,
    )
    .await?;
    if objects.len() != 2 * count + 2 {
        return Err("incorrect acknowledged object count".into());
    }
    let phase = Phase::start(pid)?;
    let mut samples = Vec::new();
    for object in &objects {
        let timer = Instant::now();
        get(&mut client, object).await?;
        samples.push(timer.elapsed().as_secs_f64());
    }
    record(
        &mut rows,
        &events,
        phase,
        pid,
        "verified_get_all",
        2 * bytes + 2 * count as u64 * SMALL,
        samples,
    )
    .await?;
    let before_delete = inventory(roots)?;
    let phase = Phase::start(pid)?;
    let mut samples = Vec::new();
    for object in &objects {
        let timer = Instant::now();
        client
            .control(Command::ObjectDelete {
                object_key: object.key.clone(),
            })
            .await?;
        samples.push(timer.elapsed().as_secs_f64());
    }
    record(&mut rows, &events, phase, pid, "purge_all", 0, samples).await?;
    for object in &objects {
        match client
            .control(Command::ObjectStat {
                object_key: object.key.clone(),
            })
            .await
        {
            Err(n1_native_client::NativeClientError::Native { code, .. }) if code == "404" => {}
            other => return Err(format!("purged object is not absent: {other:?}").into()),
        }
    }
    let after_delete = inventory(roots)?;
    if after_delete["files"] != 2
        || after_delete["terminal_files"] != 0
        || after_delete["symlinks"] != 0
        || after_delete["per_identity_staging_directories"] != 0
    {
        return Err("purge residue".into());
    }
    drop(client);
    if unsafe { libc::kill(pid as i32, libc::SIGTERM) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let status = wait(&mut owned.0, 60).await?;
    if !status.success() {
        return Err(format!("engine shutdown: {status}").into());
    }
    let peak = rows
        .iter()
        .filter_map(|r| r["sampled_peak_rss_bytes"].as_u64())
        .max();
    let high = rows
        .iter()
        .filter_map(|r| r["after"]["vm_hwm_bytes"].as_u64())
        .max();
    Ok(json!({
        "architecture": architecture,
        "passed": true,
        "clean_shutdown": true,
        "standalone_object_engine": true,
        "metadata_process_started": false,
        "smoke_only": smoke,
        "roots": roots,
        "small_count_per_method": count,
        "concurrent_clients": concurrency,
        "multipart_bytes": bytes,
        "part_bytes": part_bytes,
        "verified_downloads": objects.len(),
        "objects_deleted": objects.len(),
        "before_delete_inventory": before_delete,
        "after_delete_inventory": after_delete,
        "sampled_peak_rss_bytes": peak,
        "kernel_rss_high_water_bytes": high,
        "phases": rows
    }))
}
