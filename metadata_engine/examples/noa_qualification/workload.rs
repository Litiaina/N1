use super::{Result, setup::Config};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::{BufRead, Write},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::RwLock;
const SIZES: [usize; 8] = [0, 1, 127, 1024, 4096, 8193, 32768, 65536];
pub fn payload(index: u64) -> Vec<u8> {
    (0..SIZES[index as usize % 8])
        .map(|offset| {
            (((index as u32).wrapping_mul(131)
                ^ (index as u32).rotate_left(17)
                ^ (offset as u32).wrapping_mul(17))
                % 251) as u8
        })
        .collect()
}
pub fn logical_bytes(count: u64) -> u64 {
    (0..count).map(|i| SIZES[i as usize % 8] as u64).sum()
}
fn name(mode: &str, index: u64) -> String {
    format!("qual/{mode}/{index:09}.bin")
}
#[derive(Clone)]
pub struct Api {
    client: reqwest::Client,
    url: String,
    pub fragment: String,
    pub secret: String,
    token: Arc<RwLock<(String, u64)>>,
}
impl Api {
    pub async fn open(config: &Config, mode: &str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            // Reqwest 0.12 defaults TCP_USER_TIMEOUT to 30 seconds on Linux.
            // Multipart parsing legitimately applies backpressure while a
            // durable group commits. Keep the transport within the existing
            // 300-second request budget, without changing N1's I/O deadline.
            .tcp_user_timeout(std::time::Duration::from_secs(300))
            .pool_max_idle_per_host(64)
            .build()?;
        let label = format!("N1 qualification {} {mode}", config.identity);
        use sha2::Digest;
        let fragment = hex::encode(sha2::Sha256::digest(label.as_bytes()));
        let created = client
            .post(format!("{}/noa/v1/fragments", config.url))
            .json(&json!({"name":label,"secret":config.master,"key":config.fragment_secret}))
            .send()
            .await?;
        if created.status() != 201 && created.status() != 409 {
            return Err(format!("fragment provisioning returned {}", created.status()).into());
        }
        let api = Self {
            client,
            url: config.url.clone(),
            fragment,
            secret: config.fragment_secret.clone(),
            token: Arc::new(RwLock::new((String::new(), 0))),
        };
        api.token().await?;
        Ok(api)
    }
    pub async fn login(&self) -> Result<Value> {
        let response = self
            .client
            .post(format!("{}/noa/v1/auth/fragment", self.url))
            .json(&json!({
                "fragment": self.fragment,
                "secret": self.secret,
                "read": true,
                "write": true,
                "modify": true,
                "delete": true,
                "expires_in": 86400
            }))
            .send()
            .await?;
        if response.status() != 200 {
            return Err(format!(
                "qualification authentication returned {}",
                response.status()
            )
            .into());
        }
        Ok(response.json().await?)
    }
    async fn token(&self) -> Result<String> {
        let now = m1::middleware::auth::now();
        {
            let token = self.token.read().await;
            if token.1 > now + 60 {
                return Ok(token.0.clone());
            }
        }
        let mut token = self.token.write().await;
        if token.1 <= now + 60 {
            let auth = self.login().await?;
            *token = (
                auth["access_token"]
                    .as_str()
                    .ok_or("login returned no access token")?
                    .into(),
                now + auth["expires_in"]
                    .as_u64()
                    .ok_or("login returned no expiry")?,
            );
        }
        Ok(token.0.clone())
    }
    async fn get(&self, mode: &str, index: u64) -> Result<reqwest::Response> {
        Ok(self
            .client
            .get(format!(
                "{}/noa/v1/objects/get/{}",
                self.url,
                name(mode, index)
            ))
            .bearer_auth(self.token().await?)
            .send()
            .await?)
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct Ack {
    index: u64,
    version: String,
}
fn journal(root: &Path, writer: usize) -> std::path::PathBuf {
    root.join(format!("api-acks-{writer:02}.jsonl"))
}
fn acknowledged(root: &Path, count: u64, mode: &str) -> Result<Vec<Option<[u8; 16]>>> {
    let mut result = vec![None; count as usize];
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !(name.starts_with(&format!("{mode}-acks-")) && name.ends_with(".jsonl")) {
            continue;
        }
        for line in std::io::BufReader::new(std::fs::File::open(entry.path())?).lines() {
            let line = line?;
            let rows: Vec<Ack> = serde_json::from_str(&line)?;
            for ack in rows {
                if ack.index >= count {
                    return Err("ACK index is outside this campaign".into());
                }
                let version = ulid::Ulid::from_string(&ack.version)?;
                if version.to_string() != ack.version {
                    return Err("noncanonical ACK version".into());
                }
                match result[ack.index as usize] {
                    Some(old) if old != version.to_bytes() => {
                        return Err(
                            "conflicting ACK versions; refusing to overwrite evidence".into()
                        );
                    }
                    _ => result[ack.index as usize] = Some(version.to_bytes()),
                }
            }
        }
    }
    Ok(result)
}
pub async fn populate(api: Api, root: &Path, count: u64, writers: usize) -> Result<Value> {
    // Even an empty existing journal means a request may have reached the
    // server before the first complete ACK line was persisted.
    let resuming = std::fs::read_dir(root)?.any(|entry| {
        entry.is_ok_and(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("api-acks-") && name.ends_with(".jsonl"))
        })
    });
    let acknowledged = Arc::new(acknowledged(root, count, "api")?);
    let prior = acknowledged.iter().filter(|v| v.is_some()).count() as u64;
    let done = Arc::new(AtomicU64::new(prior));
    let next = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..writers {
        let api = api.clone();
        let done = done.clone();
        let next = next.clone();
        let old = acknowledged.clone();
        let path = journal(root, writer);
        let samples = root.join(format!("api-latency-{writer:02}.jsonl"));
        tasks.spawn(async move{
  let mut options=std::fs::OpenOptions::new();options.create(true).append(true);#[cfg(unix)]{use std::os::unix::fs::OpenOptionsExt;options.mode(0o600);}let mut journal=options.open(path)?;let mut samples=options.open(samples)?;
  loop{let start=next.fetch_add(256,Ordering::Relaxed);if start>=count{break;}let indices=(start..(start+256).min(count)).filter(|i|old[*i as usize].is_none()).collect::<Vec<_>>();if indices.is_empty(){continue;}
   let mut recovered=Vec::new();let mut missing=Vec::new();
   if resuming{for index in indices{let response=api.get("api",index).await?;if response.status()==200{let tag=response.headers().get("etag").and_then(|v|v.to_str().ok()).ok_or("recovered object has no version ETag")?.trim_matches('"').to_owned();let bytes=response.bytes().await?;if bytes.as_ref()!=payload(index){return Err("uncertain prior upload has different bytes; refusing overwrite".into());}recovered.push(Ack{index,version:tag});}else if response.status()==404{missing.push(index);}else{return Err(format!("uncertain upload lookup returned {}",response.status()).into());}}}else{missing=indices;}
   let mut rows=recovered;if !missing.is_empty(){let mut form=reqwest::multipart::Form::new();for index in &missing{form=form.part("object",reqwest::multipart::Part::bytes(payload(*index)).file_name(name("api",*index)).mime_str("application/octet-stream")?);}let begin=Instant::now();let response=api.client.post(format!("{}/noa/v1/upload/batch?durability=strict",api.url)).bearer_auth(api.token().await?).multipart(form).send().await?;let response=batch_response(response).await?;let entries=response["results"].as_array().ok_or("batch returned no result entries")?;if entries.len()!=missing.len(){return Err("batch result count mismatch".into());}
    let mut seen=std::collections::BTreeSet::new();for result in entries{if result["status"]!=201{return Err("batch returned unconfirmed result".into());}let path=result["object_key"].as_str().ok_or("batch object key missing")?;let index=path.strip_prefix("qual/api/").and_then(|s|s.strip_suffix(".bin")).ok_or("batch returned foreign path")?.parse::<u64>()?;if !missing.contains(&index)||!seen.insert(index){return Err("batch returned duplicate or foreign index".into());}let version=result["version_id"].as_str().ok_or("batch returned no version")?.to_owned();let parsed=ulid::Ulid::from_string(&version)?;if parsed.to_string()!=version||result["total_size_bytes"].as_u64()!=Some(SIZES[index as usize%8]as u64){return Err("batch result has wrong version or byte count".into());}rows.push(Ack{index,version});}
    writeln!(samples,"{}",json!({"count":missing.len(),"milliseconds":begin.elapsed().as_secs_f64()*1000.0}))?;samples.flush()?;
   }
   writeln!(journal,"{}",serde_json::to_string(&rows)?)?;journal.flush()?;journal.sync_all()?;let current=done.fetch_add(rows.len()as u64,Ordering::Relaxed)+rows.len()as u64;
   if current/10000!=(current-rows.len()as u64)/10000{println!("{}",json!({
       "event": "api-upload-progress",
       "acknowledged": current,
       "count": count,
       "elapsed_seconds": started.elapsed().as_secs_f64()
   }));}
  }Ok::<(),Box<dyn std::error::Error+Send+Sync>>(())
 });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    let ack = acknowledged_read(root, count)?;
    if ack != count {
        return Err(format!("only {ack} of {count} distinct objects acknowledged").into());
    }
    Ok(
        json!({"prior_acknowledged":prior,"acknowledged_this_invocation":count-prior,"total_acknowledged":count}),
    )
}
fn acknowledged_read(root: &Path, count: u64) -> Result<u64> {
    Ok(acknowledged(root, count, "api")?
        .iter()
        .filter(|v| v.is_some())
        .count() as u64)
}
async fn batch_response(response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|error| format!("batch response body failed, status={status}: {error:?}"))?;
    let body: Value = serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "batch response is not JSON, status={status}, length={}, prefix={:?}: {error}",
            bytes.len(),
            String::from_utf8_lossy(&bytes[..bytes.len().min(200)])
        )
    })?;
    if status != 201 {
        return Err(format!("batch failed: {status} {body}").into());
    }
    Ok(body)
}
pub async fn verify(
    api: Api,
    root: &Path,
    count: u64,
    readers: usize,
    mode: &str,
) -> Result<Value> {
    let acks = Arc::new(acknowledged(root, count, mode)?);
    if acks.iter().any(Option::is_none) {
        return Err("campaign does not have an ACK for every distinct index".into());
    }
    let next = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..readers {
        let api = api.clone();
        let acks = acks.clone();
        let next = next.clone();
        let done = done.clone();
        let mode = mode.to_owned();
        tasks.spawn(async move {
            let mut bytes = 0u64;
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= count {
                    break;
                }
                bytes += verify_object(
                    &api,
                    &mode,
                    index,
                    acks[index as usize].ok_or("missing expected version")?,
                )
                .await?;
                let current = done.fetch_add(1, Ordering::Relaxed) + 1;
                if current.is_multiple_of(10000) {
                    println!(
                        "{}",
                        json!({
                            "event": "post-restart-verify-progress",
                            "verified": current,
                            "count": count,
                            "elapsed_seconds": start.elapsed().as_secs_f64()
                        })
                    );
                }
            }
            Ok::<u64, Box<dyn std::error::Error + Send + Sync>>(bytes)
        });
    }
    let mut bytes = 0;
    while let Some(result) = tasks.join_next().await {
        bytes += result??;
    }
    if bytes != logical_bytes(count) {
        return Err("verification logical byte count mismatch".into());
    }
    Ok(json!({
        "verified": done.load(Ordering::Relaxed),
        "bytes": bytes,
        "seconds": start.elapsed().as_secs_f64(),
        "payload_mismatches": 0,
        "version_mismatches": 0
    }))
}

async fn verify_object(
    api: &Api,
    mode: &str,
    index: u64,
    expected_version: [u8; 16],
) -> Result<u64> {
    let response = api.get(mode, index).await?;
    if response.status() != 200 {
        return Err(format!("post-restart read {index} returned {}", response.status()).into());
    }
    let version = response
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .ok_or("download version ETag missing")?
        .trim_matches('"');
    if ulid::Ulid::from_string(version)?.to_bytes() != expected_version {
        return Err(format!("post-restart object {index} returned a different version").into());
    }
    let actual = response.bytes().await?;
    if actual.as_ref() != payload(index) {
        return Err(format!("post-restart payload mismatch at index {index}").into());
    }
    Ok(actual.len() as u64)
}
