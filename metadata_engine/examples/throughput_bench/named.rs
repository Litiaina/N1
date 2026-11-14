use super::*;
use futures_util::{StreamExt, TryStreamExt};
use measure::{Measure, Pids, distribution};
use std::time::Instant;
#[path = "named/workload.rs"]
mod workload;
pub use workload::{Shape, Workload};
pub struct Api {
    pub client: reqwest::Client,
    pub base: String,
    pub token: String,
    fragment: String,
}
pub async fn expect(response: reqwest::Response, status: u16) -> Result<Value> {
    if response.status().as_u16() != status {
        return Err(format!(
            "benchmark API returned HTTP {}, expected {status}",
            response.status()
        )
        .into());
    }
    Ok(response.json().await?)
}
fn string(value: &Value, name: &str) -> Result<String> {
    Ok(value[name]
        .as_str()
        .ok_or_else(|| format!("missing {name}"))?
        .to_owned())
}
impl Api {
    pub async fn create(
        client: reqwest::Client,
        url: &str,
        master: &str,
        run: &str,
    ) -> Result<Self> {
        let base = format!("{}/noa/v1", url.trim_end_matches('/'));
        let name = format!("Isolated throughput {run}");
        let key = format!("{}{}", ulid::Ulid::new(), ulid::Ulid::new());
        let created = expect(
            client
                .post(format!("{base}/fragments"))
                .json(&json!({"name":name,"secret":master,"key":key}))
                .send()
                .await?,
            201,
        )
        .await?;
        let auth=expect(client.post(format!("{base}/auth/fragment")).json(&json!({"fragment":created["fragment_hash"],"secret":master,"read":true,"write":true,"modify":true,"delete":true,"expires_in":3600})).send().await?,200).await?;
        Ok(Self {
            client,
            base,
            token: string(&auth, "access_token")?,
            fragment: string(&created, "fragment_hash")?,
        })
    }
    async fn get(&self, path: &str, version: &str, length: u64, hash: &str) -> Result<()> {
        let mut response = self
            .client
            .get(format!("{}/objects/get/{path}", self.base))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != 200
            || response
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.trim_matches('"'))
                != Some(version)
        {
            return Err("GET status/version mismatch".into());
        }
        let mut actual = Sha256::new();
        let mut count = 0_u64;
        while let Some(chunk) = response.chunk().await? {
            count += chunk.len() as u64;
            actual.update(&chunk);
        }
        if count != length || hex::encode(actual.finalize()) != hash {
            return Err("GET length/SHA-256 mismatch".into());
        }
        Ok(())
    }
}

pub async fn cleanup(
    api: &Api,
    run: &str,
    repeats: usize,
    batch_only: bool,
    workload: &Workload,
) -> Result<Value> {
    let mut paths = Vec::new();
    for repeat in 0..repeats {
        if batch_only {
            for index in 0..100 {
                paths.push(format!("bench/{run}/r{repeat}/batch-{index:03}.bin"));
            }
            continue;
        }
        for concurrency in [1, 8].into_iter().filter(|_| !workload.large_only) {
            for index in 0..100 {
                paths.push(format!(
                    "bench/{run}/r{repeat}/small-c{concurrency}-{index:03}.bin"
                ));
            }
        }
        for part_bytes in &workload.part_bytes {
            paths.push(format!("bench/{run}/r{repeat}/large-part{part_bytes}.bin"));
        }
    }
    for path in &paths {
        expect(
            api.client
                .delete(format!("{}/objects/{path}", api.base))
                .bearer_auth(&api.token)
                .send()
                .await?,
            200,
        )
        .await?;
    }
    expect(
        api.client
            .post(format!(
                "{}/maintenance/gc/start?fragment={}",
                api.base, api.fragment
            ))
            .bearer_auth(&api.token)
            .send()
            .await?,
        200,
    )
    .await?;
    let started = Instant::now();
    loop {
        let state = expect(
            api.client
                .get(format!("{}/maintenance/gc/status", api.base))
                .bearer_auth(&api.token)
                .send()
                .await?,
            200,
        )
        .await?;
        match state["phase"].as_str() {
            Some("completed")
                if state["failures"].as_u64() == Some(0)
                    && state["deferred"].as_u64() == Some(0) =>
            {
                if state["processed"].as_u64().unwrap_or(0) < paths.len() as u64 {
                    return Err("benchmark GC completed without all scoped objects".into());
                }
                return Ok(json!({"objects_trashed":paths.len(),"gc":state,"timed":false}));
            }
            Some("incomplete" | "stopped") => return Err("benchmark GC did not complete".into()),
            _ => (),
        }
        if started.elapsed() > Duration::from_secs(600) {
            return Err("benchmark GC timeout".into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn batch(
    api: &Api,
    run: &str,
    repeat: usize,
    bytes: Bytes,
    pids: &Pids,
) -> Result<Value> {
    let hash = hex::encode(Sha256::digest(&bytes));
    let prefix = format!("bench/{run}/r{repeat}/batch-");
    let mut form = reqwest::multipart::Form::new();
    for index in 0..100 {
        form = form.part(
            "object",
            reqwest::multipart::Part::bytes(bytes.to_vec())
                .file_name(format!("{prefix}{index:03}.bin"))
                .mime_str("application/octet-stream")?,
        );
    }
    let phase = Measure::start(pids)?;
    let response = expect(
        api.client
            .post(format!("{}/upload/batch?durability=strict", api.base))
            .bearer_auth(&api.token)
            .multipart(form)
            .send()
            .await?,
        201,
    )
    .await?;
    let entries = response["results"]
        .as_array()
        .ok_or("missing batch results")?;
    if entries.len() != 100 {
        return Err("wrong batch acknowledgement count".into());
    }
    let mut objects = std::collections::BTreeMap::new();
    for entry in entries {
        let path = string(entry, "object_key")?;
        let index = path
            .strip_prefix(&prefix)
            .and_then(|v| v.strip_suffix(".bin"))
            .ok_or("foreign batch path")?
            .parse::<usize>()?;
        if entry["status"] != 201
            || index >= 100
            || entry["total_size_bytes"] != 4096
            || objects.insert(path, string(entry, "version_id")?).is_some()
        {
            return Err("invalid or duplicate batch acknowledgement".into());
        }
    }
    let upload = phase.finish(100 * 4096, 100).await?;
    let phase = Measure::start(pids)?;
    let reads: Vec<f64> = futures_util::stream::iter(objects.iter())
        .map(|(path, version)| {
            let hash = &hash;
            async move {
                let started = Instant::now();
                api.get(path, version, 4096, hash).await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    started.elapsed().as_secs_f64() * 1000.0,
                )
            }
        })
        .buffer_unordered(8)
        .try_collect()
        .await?;
    let download = phase.finish(100 * 4096, 100).await?;
    Ok(
        json!({"method":"100 x 4 KiB, one batch","http_upload_requests":1,"upload":upload,
        "download":download,"download_latency":distribution(reads),"verified":100,
        "latency_scope":"upload wall time is one 100-object batch, not individual-object latency"}),
    )
}
pub async fn small(
    api: &Api,
    run: &str,
    repeat: usize,
    concurrency: usize,
    bytes: Bytes,
    pids: &Pids,
) -> Result<Value> {
    let hash = hex::encode(Sha256::digest(&bytes));
    let phase = Measure::start(pids)?;
    let uploaded: Vec<(String, String, f64, Value)> = futures_util::stream::iter(0..100)
        .map(|index| {
            let bytes = bytes.clone();
            async move {
                let path = format!("bench/{run}/r{repeat}/small-c{concurrency}-{index:03}.bin");
                let unix_start_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_micros() as u64;
                let started = Instant::now();
                let response = api
                    .client
                    .post(format!(
                        "{}/upload/one-shot/{path}?durability=strict",
                        api.base
                    ))
                    .bearer_auth(&api.token)
                    .body(bytes)
                    .send()
                    .await?;
                let profile_id = response
                    .headers()
                    .get("x-m1-upload-profile")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok());
                let object = expect(response, 201).await?;
                let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                    path,
                    string(&object, "version_id")?,
                    elapsed_ms,
                    json!({"index":index,"profile_id":profile_id,
                        "unix_start_us":unix_start_us,"elapsed_ms":elapsed_ms}),
                ))
            }
        })
        .buffer_unordered(concurrency)
        .try_collect()
        .await?;
    let upload = phase.finish(100 * 4096, 100).await?;
    let phase = Measure::start(pids)?;
    let reads: Vec<f64> = futures_util::stream::iter(uploaded.iter())
        .map(|(path, version, _, _)| {
            let hash = &hash;
            async move {
                let start = Instant::now();
                api.get(path, version, 4096, hash).await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    start.elapsed().as_secs_f64() * 1000.0,
                )
            }
        })
        .buffer_unordered(concurrency)
        .try_collect()
        .await?;
    let download = phase.finish(100 * 4096, 100).await?;
    let requests = uploaded.iter().map(|v| v.3.clone()).collect::<Vec<_>>();
    Ok(
        json!({"method":format!("100 x 4 KiB c{concurrency}"),"concurrency":concurrency,"upload":upload,"download":download,
        "upload_latency":distribution(uploaded.into_iter().map(|v|v.2).collect()),"requests":requests,"download_latency":distribution(reads),"verified":100}),
    )
}
pub async fn large(
    api: &Api,
    run: &str,
    repeat: usize,
    shape: Shape,
    block: Bytes,
    hash: &str,
    pids: &Pids,
) -> Result<Value> {
    let Shape {
        total_bytes,
        part_bytes,
        negotiate_part_size,
    } = shape;
    let path = format!("bench/{run}/r{repeat}/large-part{part_bytes}.bin");
    let phase = Measure::start(pids)?;
    let (version, details) = if part_bytes == 0 {
        let source = workload::source(block.clone(), 0, total_bytes);
        let published = expect(
            api.client
                .post(format!(
                    "{}/upload/one-shot/{path}?durability=strict",
                    api.base
                ))
                .bearer_auth(&api.token)
                .header(reqwest::header::CONTENT_LENGTH, total_bytes)
                .body(reqwest::Body::wrap_stream(source))
                .send()
                .await?,
            201,
        )
        .await?;
        (
            string(&published, "version_id")?,
            json!({"multipart":false}),
        )
    } else {
        let count = total_bytes.div_ceil(part_bytes);
        let start = Instant::now();
        let session=expect(api.client.post(format!("{}/upload/multipart/durable/init/{path}?durability=strict",api.base)).bearer_auth(&api.token)
            .json(&json!({"total_size_bytes":total_bytes,"expected_parts":count,"part_size_bytes":part_bytes,"negotiate_part_size":negotiate_part_size})).send().await?,200).await?;
        let init_ms = start.elapsed().as_secs_f64() * 1000.0;
        let requested_part_bytes = part_bytes;
        let part_bytes = session["part_size_bytes"]
            .as_u64()
            .ok_or("missing init part size")?;
        let count = session["expected_parts"]
            .as_u64()
            .ok_or("missing init part count")?;
        if part_bytes == 0
            || count != total_bytes.div_ceil(part_bytes)
            || session["total_size_bytes"].as_u64() != Some(total_bytes)
            || (!negotiate_part_size && part_bytes != requested_part_bytes)
        {
            return Err("init returned inconsistent geometry".into());
        }
        let version = string(&session, "version_id")?;
        let session_id = string(&session, "session_id")?;
        let parts_start = Instant::now();
        let samples: Vec<f64> = futures_util::stream::iter(0..count)
            .map(|index| {
                let length = (total_bytes - index * part_bytes).min(part_bytes);
                let payload = workload::source(block.clone(), index * part_bytes, length);
                let version = &version;
                async move {
                    let start = Instant::now();
                    expect(
                        api.client
                            .put(format!(
                                "{}/upload/multipart/durable/{version}/{index}?durability=strict",
                                api.base
                            ))
                            .bearer_auth(&api.token)
                            .header(reqwest::header::CONTENT_LENGTH, length)
                            .body(reqwest::Body::wrap_stream(payload))
                            .send()
                            .await?,
                        200,
                    )
                    .await?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                        start.elapsed().as_secs_f64() * 1000.0,
                    )
                }
            })
            .buffer_unordered(4)
            .try_collect()
            .await?;
        let parts_seconds = parts_start.elapsed().as_secs_f64();
        let start = Instant::now();
        expect(
            api.client
                .get(format!("{}/upload/multipart/durable/{version}", api.base))
                .bearer_auth(&api.token)
                .send()
                .await?,
            200,
        )
        .await?;
        let status_ms = start.elapsed().as_secs_f64() * 1000.0;
        let finalize = format!(
            "{}/upload/multipart/durable/{version}/{session_id}/finalize",
            api.base
        );
        let body = json!({"total_size_bytes":total_bytes,"expected_parts":count});
        let start = Instant::now();
        let published = expect(
            api.client
                .post(&finalize)
                .bearer_auth(&api.token)
                .json(&body)
                .send()
                .await?,
            200,
        )
        .await?;
        let finalize_ms = start.elapsed().as_secs_f64() * 1000.0;
        if string(&published, "version_id")? != version {
            return Err("finalize changed version".into());
        }
        (
            version,
            json!({"multipart":true,"requested_part_bytes":requested_part_bytes,"negotiated":negotiate_part_size,"part_bytes":part_bytes,"part_count":count,"part_concurrency":4,"init_ms":init_ms,"parts_seconds":parts_seconds,"part_latency":distribution(samples),"status_ms":status_ms,"finalize_ms":finalize_ms,"finalize_url":finalize,"finalize_body":body}),
        )
    };
    let upload = phase.finish(total_bytes, 1).await?;
    let mut details = details;
    if part_bytes != 0 {
        let start = Instant::now();
        let again = expect(
            api.client
                .post(details["finalize_url"].as_str().unwrap())
                .bearer_auth(&api.token)
                .json(&details["finalize_body"])
                .send()
                .await?,
            200,
        )
        .await?;
        if string(&again, "version_id")? != version {
            return Err("repeated finalize changed version".into());
        }
        details["repeated_finalize_ms"] = json!(start.elapsed().as_secs_f64() * 1000.0);
        details.as_object_mut().unwrap().remove("finalize_url");
        details.as_object_mut().unwrap().remove("finalize_body");
    }
    let phase = Measure::start(pids)?;
    api.get(&path, &version, total_bytes, hash).await?;
    let download = phase.finish(total_bytes, 1).await?;
    Ok(
        json!({"method":if part_bytes==0 {format!("{total_bytes} bytes one-shot")} else {format!("{total_bytes} bytes durable multipart {part_bytes} byte parts x4")},"upload":upload,"download":download,"details":details,"sha256_verified":true}),
    )
}
