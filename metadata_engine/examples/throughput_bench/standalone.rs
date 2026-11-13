//! Configurable large-object measurement with the M1 benchmark's source pattern.
//! Only fresh server-issued LINK identities are written and subsequently purged.
use super::*;
use futures_util::future::try_join_all;
use measure::{Measure, Pids, distribution};
use n1_native_client::{Command, N1NativeClient, NativeClientError};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::io::AsyncWrite;

const PART: u64 = 48 * 1024 * 1024;
#[path = "standalone/latency.rs"]
mod latency;
#[path = "standalone/replay.rs"]
mod replay;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    url: String,
    ca: PathBuf,
    secret_env: PathBuf,
    object_pid: u32,
    metadata_pid: u32,
    console_pid: u32,
    #[serde(default = "default_data_shards")]
    expected_data_shards: u64,
    #[serde(default = "default_parity_shards")]
    expected_parity_shards: u64,
    #[serde(default)]
    resume_identity: Option<PathBuf>,
    #[serde(default)]
    read_replay: Option<replay::Config>,
    /// Diagnostic fixture only: preserve the verified identity for later reads.
    #[serde(default)]
    retain_object: bool,
    #[serde(default)]
    latency_objects: usize,
    #[serde(default = "repeats")]
    repeats: usize,
    #[serde(default = "default_bytes")]
    total_bytes: u64,
    #[serde(default = "default_upload_ttl")]
    upload_ttl_seconds: u64,
}
fn default_upload_ttl() -> u64 {
    3600
}
fn default_bytes() -> u64 {
    LARGE
}
fn default_data_shards() -> u64 {
    3
}
fn default_parity_shards() -> u64 {
    1
}

struct Payload {
    total_bytes: u64,
    data_shards: u64,
    width: u64,
    part: Bytes,
    sha256: String,
    upload_ttl_seconds: u64,
}

struct Api {
    url: String,
    tls: Arc<rustls::ClientConfig>,
    token: String,
}
impl Api {
    async fn connect(&self) -> Result<N1NativeClient> {
        let mut client =
            N1NativeClient::connect_with_tls(&self.url, Some(self.tls.clone())).await?;
        client.auth_bearer(self.token.clone()).await?;
        Ok(client)
    }
}

struct HashSink {
    hash: Sha256,
    bytes: u64,
    total_bytes: u64,
    next_report: u64,
    started: Instant,
}
impl HashSink {
    fn new(total_bytes: u64) -> Self {
        Self {
            hash: Sha256::new(),
            bytes: 0,
            total_bytes,
            next_report: LARGE,
            started: Instant::now(),
        }
    }
}
impl AsyncWrite for HashSink {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.hash.update(bytes);
        self.bytes += bytes.len() as u64;
        if self.bytes >= self.next_report {
            println!(
                "{}",
                json!({"phase":"download","received_bytes":self.bytes,"total_bytes":self.total_bytes,
                    "elapsed_seconds":self.started.elapsed().as_secs_f64()})
            );
            self.next_report = (self.bytes / LARGE + 1) * LARGE;
        }
        std::task::Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

pub async fn run(config: &Path, output: &Path) -> Result<()> {
    let config: Config = serde_json::from_slice(&fs::read(config)?)?;
    let loopback_tls = config
        .url
        .strip_prefix("n1s://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .is_some_and(|port| port != 0);
    if !loopback_tls || !(1..=5).contains(&config.repeats) {
        return Err("standalone benchmark requires local loopback TLS N1 and 1..=5 repeats".into());
    }
    if (config.read_replay.is_some()
        && (config.resume_identity.is_some() || config.latency_objects != 0))
        || (config.retain_object
            && (config.resume_identity.is_some()
                || config.read_replay.is_some()
                || config.latency_objects != 0
                || config.repeats != 1))
    {
        return Err("incompatible standalone diagnostic modes".into());
    }
    // Bound this diagnostic workload; these are not N1's object-size limits.
    if !(LARGE..=128 * LARGE).contains(&config.total_bytes)
        || !config.total_bytes.is_multiple_of(1 << 20)
    {
        return Err("benchmark size must be 1..=128 GiB in whole MiB".into());
    }
    let total_bytes = config.total_bytes;
    if !(60..=604800).contains(&config.upload_ttl_seconds) {
        return Err("benchmark upload TTL must be between one minute and one week".into());
    }
    if config.expected_data_shards == 0
        || config.expected_data_shards > 32
        || config.expected_parity_shards > 32
    {
        return Err("invalid expected benchmark topology".into());
    }
    let tls = cli_core::client_tls(&config.ca)?;
    let secret = fs::read_to_string(&config.secret_env)?
        .lines()
        .find_map(|line| line.strip_prefix("AUTH_KEY=").map(str::to_owned))
        .filter(|value| !value.is_empty())
        .ok_or("missing N1 authentication key")?;
    let mut client = N1NativeClient::connect_with_tls(&config.url, Some(tls.clone())).await?;
    let login = cli_core::result(client.auth_fragment("root", &secret).await?)?;
    let api = Api {
        url: config.url,
        tls,
        token: login["token"].as_str().ok_or("missing token")?.into(),
    };
    let pools = cli_core::result(client.control(Command::Pools).await?)?;
    let pools = pools["pools"].as_array().ok_or("missing pools")?;
    let pool = pools.last().ok_or("no pool")?;
    if pools.len() != 1
        || pool["sealed"] != false
        || pool["data_shards"] != config.expected_data_shards
        || pool["parity_shards"] != config.expected_parity_shards
        || pool["block_bytes"] != 262144
        || pool["available_members"] != config.expected_data_shards + config.expected_parity_shards
    {
        return Err(
            "standalone measurement requires its configured healthy EC pool, 256 KiB blocks".into(),
        );
    }
    let pool_id = serde_json::from_value(pool["id"].clone())?;
    if !(8 * 1024 * 1024_u64).is_multiple_of(config.expected_data_shards * 262144) {
        let rejected = client
            .control(Command::UploadInit {
                pool: Some(pool_id),
                total_bytes,
                part_bytes: 8 * 1024 * 1024,
                durable: true,
                ttl_seconds: 3600,
            })
            .await;
        match rejected {
            Err(NativeClientError::Native { ref code, .. }) if code == "400" => (),
            Ok(response) => {
                if let Some(key) = response
                    .result
                    .as_ref()
                    .and_then(|v| v["object_key"].as_str())
                {
                    client
                        .control(Command::ObjectDelete {
                            object_key: key.into(),
                        })
                        .await?;
                }
                return Err("8 MiB geometry unexpectedly accepted".into());
            }
            Err(error) => return Err(error.into()),
        }
    }
    let pids = vec![
        ("N1", config.object_pid),
        ("idle_interactive_M1", config.metadata_pid),
        ("interactive_Console", config.console_pid),
        ("client", std::process::id()),
    ]
    .into_iter()
    .filter(|(_, pid)| *pid != 0)
    .collect();
    let source = block();
    let mut expected = Sha256::new();
    for _ in 0..total_bytes / source.len() as u64 {
        expected.update(&source);
    }
    let payload = Payload {
        total_bytes,
        data_shards: config.expected_data_shards,
        width: config.expected_data_shards + config.expected_parity_shards,
        part: Bytes::from(source.repeat(48)),
        sha256: hex::encode(expected.finalize()),
        upload_ttl_seconds: config.upload_ttl_seconds,
    };
    write_json(
        &output.join("methodology.json"),
        &json!({
            "path":"server-local native TLS -> N1 -> configured EC pool; no M1 calls",
            "repeats":config.repeats,"large_bytes":total_bytes,"large_sha256":payload.sha256,
            "part_bytes":PART,"part_count":total_bytes.div_ceil(PART),"part_concurrency":4,
            "upload_ttl_seconds":payload.upload_ttl_seconds,
            "pool":pool,"strict_durability":true,"incompatible_8_mib_init_status":if !(8 * 1024 * 1024_u64).is_multiple_of(config.expected_data_shards * 262144) { Some(400) } else { None },
            "payload":"same repeated precomputed 1 MiB SplitMix64 block as M1 benchmark; bounded 48 MiB part buffer, no fixture file",
            "download":"streamed SHA-256, exact length and N1 geometry check included in timing; same LINK key used throughout",
            "cache_policy":"no global cache drop; immediate post-upload GET; warm process, no restart",
            "upload_scope":"init + client identity-journal fsync + durable parts + finalize; authentication/preparation excluded; no pre-finalize status scan",
            "finalize_scope":"native response latency, not M1 publication; repeated finalize outside upload timing",
            "rss":"50 ms process samples; not whole-host page cache","pids":pids,
            "client_sha256":hex::encode(Sha256::digest(fs::read(std::env::current_exe()?)?))
        }),
    )?;
    let mut rows = Vec::new();
    if let Some(identity) = config.resume_identity {
        return recover_existing(&api, &payload, &pids, &identity, output).await;
    }
    if let Some(replay) = config.read_replay {
        return replay::run(&api, &payload, &pids, replay, output).await;
    }
    if config.latency_objects > 0 {
        if config.latency_objects > 2000 {
            return Err("latency workload limit is 2,000 objects per durability mode".into());
        }
        return latency::run(&api, &payload, &pids, output, config.latency_objects).await;
    }
    for repeat in 1..=config.repeats {
        let row = large(
            &api,
            &mut client,
            &payload,
            &pids,
            output,
            repeat,
            config.retain_object,
        )
        .await?;
        write_json(&output.join(format!("large-r{repeat}.json")), &row)?;
        println!(
            "{}",
            json!({"repeat":repeat,"init_ms":row["details"]["init_ms"],"parts_seconds":row["details"]["parts_seconds"],"finalize_ms":row["details"]["finalize_ms"],"mib_per_second":row["upload"]["mib_per_second"],"verified_and_purged":!config.retain_object})
        );
        rows.push(row);
    }
    write_json(
        &output.join("PASSED.json"),
        &json!({"passed":true,"rows":rows,"verified_and_purged":if config.retain_object {0} else {config.repeats},"retained_for_replay":config.retain_object,"no_m1_requests":true}),
    )
}

async fn large(
    api: &Api,
    client: &mut N1NativeClient,
    payload: &Payload,
    pids: &Pids,
    output: &Path,
    repeat: usize,
    retain: bool,
) -> Result<Value> {
    let total_bytes = payload.total_bytes;
    let mut writers = Vec::new();
    for _ in 0..4 {
        writers.push(api.connect().await?);
    }
    println!(
        "{}",
        json!({"phase":"init","repeat":repeat,"total_bytes":total_bytes,"parts":total_bytes.div_ceil(PART)})
    );
    // Observe existing live counters outside the upload timer. No monitor
    // polling or new instrumentation runs inside the payload pipeline.
    let backend_before =
        cli_core::result(client.control(Command::Health).await?)?["io"]["backend"].clone();
    let phase = Measure::start(pids)?;
    let started = Instant::now();
    let plan = cli_core::result(
        client
            .control(Command::UploadInit {
                pool: None,
                total_bytes,
                part_bytes: PART,
                durable: true,
                ttl_seconds: payload.upload_ttl_seconds,
            })
            .await?,
    )?;
    let init_ms = started.elapsed().as_secs_f64() * 1000.0;
    let key = plan["object_key"]
        .as_str()
        .ok_or("missing LINK key")?
        .to_owned();
    if plan["parts"] != total_bytes.div_ceil(PART)
        || plan["part_bytes"] != PART
        || plan["durable"] != true
    {
        return Err("unexpected N1 upload geometry or durability".into());
    }
    // Preserve the identity before payload I/O so an interrupted test can be cleaned up.
    let journal_start = Instant::now();
    write_json(
        &output.join(format!("identity-r{repeat}.json")),
        &json!({"object_key":key,"plan":plan,"source_sha256":payload.sha256}),
    )?;
    let identity_journal_ms = journal_start.elapsed().as_secs_f64() * 1000.0;
    let parts_start = Instant::now();
    let acknowledged = Arc::new(AtomicU64::new(0));
    let jobs = writers.into_iter().enumerate().map(|(worker, mut writer)| {
        let key = key.clone();
        let part = payload.part.clone();
        let acknowledged = acknowledged.clone();
        async move {
            let mut samples = Vec::new();
            for index in (worker as u64..total_bytes.div_ceil(PART)).step_by(4) {
                let bytes = (total_bytes - index * PART).min(PART);
                let started = Instant::now();
                writer
                    .write_at(
                        key.clone(),
                        index * PART,
                        bytes,
                        &mut &part[..bytes as usize],
                    )
                    .await?;
                samples.push(started.elapsed().as_secs_f64() * 1000.0);
                let before = acknowledged.fetch_add(bytes, Ordering::Relaxed);
                let after = before + bytes;
                if before / LARGE != after / LARGE {
                    println!("{}", json!({"phase":"upload","acknowledged_bytes":after,"total_bytes":total_bytes,"elapsed_seconds":parts_start.elapsed().as_secs_f64()}));
                }
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(samples)
        }
    });
    let samples = try_join_all(jobs)
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let parts_seconds = parts_start.elapsed().as_secs_f64();
    if acknowledged.load(Ordering::Relaxed) != total_bytes {
        return Err("durable acknowledged byte count mismatch".into());
    }
    println!(
        "{}",
        json!({"phase":"finalize","total_bytes":total_bytes,"parts_seconds":parts_seconds})
    );
    // Long HDD transfers can exceed the idle timeout of the original control
    // socket. Reconnect before timing finalize; never replay the payload.
    *client = api.connect().await?;
    let started = Instant::now();
    let published = cli_core::result(
        client
            .control(Command::UploadFinalize {
                object_key: key.clone(),
            })
            .await?,
    )?;
    let finalize_ms = started.elapsed().as_secs_f64() * 1000.0;
    if published["object_key"] != key
        || published["total_bytes"] != total_bytes
        || published["already_finalized"] != false
    {
        return Err("unexpected first-finalize identity, size or state".into());
    }
    let mut upload = phase.finish(total_bytes, 1).await?;
    let backend_after =
        cli_core::result(client.control(Command::Health).await?)?["io"]["backend"].clone();
    upload["io_backend"] = json!({"before":backend_before,"after":backend_after});
    // Preserve upload measurements before a potentially long readback. A
    // completed upload is not a verification pass until the final report.
    write_json(
        &output.join(format!("upload-r{repeat}.json")),
        &json!({"object_key":key,"upload":upload,"init_ms":init_ms,
            "identity_journal_ms":identity_journal_ms,"parts_seconds":parts_seconds,
            "finalize_ms":finalize_ms,"part_latency":distribution(samples.clone()),
            "readback_verified":false}),
    )?;
    println!(
        "{}",
        json!({"phase":"finalized","finalize_ms":finalize_ms,"init_ms":init_ms,"upload_seconds":upload["wall_seconds"],"mib_per_second":upload["mib_per_second"]})
    );
    let started = Instant::now();
    let again = cli_core::result(
        client
            .control(Command::UploadFinalize {
                object_key: key.clone(),
            })
            .await?,
    )?;
    let repeated_finalize_ms = started.elapsed().as_secs_f64() * 1000.0;
    if again["object_key"] != key || again["already_finalized"] != true {
        return Err("repeated finalize changed identity or state".into());
    }
    let phase = Measure::start(pids)?;
    let mut sink = HashSink::new(total_bytes);
    let (response, bytes) = client.download(key.clone(), 0, None, &mut sink).await?;
    if bytes != total_bytes
        || sink.bytes != total_bytes
        || hex::encode(sink.hash.finalize()) != payload.sha256
        || response.result.as_ref().is_none_or(|v| {
            v["total_bytes"] != total_bytes
                || v["data_shards"] != payload.data_shards
                || v["finalized_shards"] != payload.width
        })
    {
        return Err("native GET length, SHA-256 or LINK identity mismatch".into());
    }
    let download = phase.finish(total_bytes, 1).await?;
    if retain {
        return Ok(json!({"repeat":repeat,"upload":upload,"download":download,
            "details":{"init_ms":init_ms,"identity_journal_ms":identity_journal_ms,
            "parts_seconds":parts_seconds,"part_latency":distribution(samples),
            "finalize_ms":finalize_ms,"repeated_finalize_ms":repeated_finalize_ms},
            "sha256_verified":true,"retained_for_replay":true,"purged_status":null}));
    }
    let started = Instant::now();
    let removed = cli_core::result(
        client
            .control(Command::ObjectDelete {
                object_key: key.clone(),
            })
            .await?,
    )?;
    let purge_ms = started.elapsed().as_secs_f64() * 1000.0;
    match client
        .control(Command::ObjectStat { object_key: key })
        .await
    {
        Err(NativeClientError::Native { ref code, .. }) if code == "404" => (),
        _ => return Err("purged benchmark identity did not return 404".into()),
    }
    Ok(
        json!({"repeat":repeat,"method":format!("N1 alone: {total_bytes} bytes durable multipart 48 MiB x4"), "upload":upload,"download":download,
        "details":{"init_ms":init_ms,"identity_journal_ms":identity_journal_ms,"parts_seconds":parts_seconds,"part_latency":distribution(samples),"finalize_ms":finalize_ms,"repeated_finalize_ms":repeated_finalize_ms,"purge_ms":purge_ms},
        "sha256_verified":true,"purged":removed,"purged_status":404}),
    )
}

async fn recover_existing(
    api: &Api,
    payload: &Payload,
    pids: &Pids,
    identity: &Path,
    output: &Path,
) -> Result<()> {
    let saved: Value = serde_json::from_slice(&fs::read(identity)?)?;
    let key = saved["object_key"]
        .as_str()
        .ok_or("missing saved identity")?
        .to_owned();
    if saved["plan"]["total_bytes"] != payload.total_bytes {
        return Err("saved object size mismatch".into());
    }
    let mut client = api.connect().await?;
    let start = Instant::now();
    let finalized = cli_core::result(
        client
            .control(Command::UploadFinalize {
                object_key: key.clone(),
            })
            .await?,
    )?;
    let finalize_ms = start.elapsed().as_secs_f64() * 1000.0;
    if finalized["object_key"] != key {
        return Err("finalize changed saved identity".into());
    }
    let again = cli_core::result(
        client
            .control(Command::UploadFinalize {
                object_key: key.clone(),
            })
            .await?,
    )?;
    if again["object_key"] != key || again["already_finalized"] != true {
        return Err("repeated finalize mismatch".into());
    }
    let phase = Measure::start(pids)?;
    let mut sink = HashSink::new(payload.total_bytes);
    let (_, bytes) = client.download(key.clone(), 0, None, &mut sink).await?;
    if bytes != payload.total_bytes
        || sink.bytes != bytes
        || hex::encode(sink.hash.finalize()) != payload.sha256
    {
        return Err("recovered download size/checksum mismatch".into());
    }
    let download = phase.finish(bytes, 1).await?;
    let removed = cli_core::result(
        client
            .control(Command::ObjectDelete {
                object_key: key.clone(),
            })
            .await?,
    )?;
    if removed["removed_shards"] != payload.width {
        return Err("recovered purge shard count mismatch".into());
    }
    match client
        .control(Command::ObjectStat { object_key: key })
        .await
    {
        Err(NativeClientError::Native { ref code, .. }) if code == "404" => (),
        _ => return Err("recovered purge did not return 404".into()),
    }
    write_json(
        &output.join("RECOVERED.json"),
        &json!({"passed":true,"resumed_saved_identity":identity,
        "new_uploads":0,"finalize_ms":finalize_ms,"finalized":finalized,"download":download,"sha256_verified":true,
        "purged":removed,"purged_status":404}),
    )
}
