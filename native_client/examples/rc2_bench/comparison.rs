//! Isolated NCX/LINK or LINK before/after native comparison. The old adapter exists only
//! in this benchmark; it is not compatibility code in the SDK or engine.
use super::supervisor::{Owned, private_json, wait};
use super::{
    COUNT, HashSink, LARGE, Object, Payload, Phase, Result, SMALL, append, expected, field, get,
    init, pattern,
};
use n1_native_client::{Command, N1NativeClient};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::{Command as Process, Stdio},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
#[path = "comparison/uploads.rs"]
mod uploads;

fn inventory(roots: &[PathBuf; 2]) -> Result<Value> {
    let mut pending = roots
        .iter()
        .map(|root| root.join("member-0"))
        .collect::<Vec<_>>();
    pending[1] = roots[1].join("member-1");
    let (
        mut files,
        mut directories,
        mut bytes,
        mut allocated,
        mut terminals,
        mut staging,
        mut symlinks,
    ) = (0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64);
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.file_type().is_symlink() {
                // Original NCX has .fragment aliases. Count their inodes but
                // never follow them or count their targets a second time.
                symlinks += 1;
            } else if metadata.is_dir() {
                directories += 1;
                if path.file_name().is_some_and(|name| name == ".uploads") {
                    staging += 1;
                }
                pending.push(entry.path());
            } else if metadata.is_file() {
                files += 1;
                bytes += metadata.len();
                allocated += metadata.blocks() * 512;
                if metadata.len() == 96 {
                    use std::io::Read;
                    let mut magic = [0; 8];
                    fs::File::open(entry.path())?.read_exact(&mut magic)?;
                    if &magic == b"N1LDEL01" {
                        terminals += 1;
                    }
                }
            } else {
                return Err("unexpected entry in benchmark members".into());
            }
        }
    }
    Ok(json!({
        "files": files,
        "directories": directories,
        "symlinks": symlinks,
        "apparent_bytes": bytes,
        "allocated_file_bytes": allocated,
        "terminal_files": terminals,
        "per_identity_staging_directories": staging
    }))
}

const NCX_SHA: &str = "51594bdd793f3af33826c5f0fa76d82616a235349875f7e71b2555b0ae3e0ff1";
const LINK_BASELINE_SHA: &str = "f7c8283633f31175da2968c14364e665f666ea288ee2b0409b8d2713b2370701";
const PURGE_BASELINE_SHA: &str = "2395935dc899c81d685351fd59bcbfd48a97340e68dd9d5acf319a5a154fdc43";
const KEY_BASELINE_SHA: &str = "17c64f33bb7a5fff9d0d8c77c41a9ea75e37ddac6527af1777e28eb92111ae57";
const ALIGNED_BASELINE_SHA: &str =
    "a1e77c3f5f3ade7d952fe79eab77c62f5fb863115fe129a7742c17c954dba47e";
const NATIVE_BASELINE_SHA: &str =
    "f017966129b74894ea80d5c27bde0b446e5e77533ebb9f4fdfd294768e6000ed";
const DIRECT_BASELINE_SHA: &str =
    "252b92024f59f2d339fa4e5af14412fef25272594404ab3ec1e39dc60e4ffff2";
const BLOCK: u64 = 262144;
const MASTER: &str = "isolated-native-comparison-master-20261102";
const SECRET: &str = "isolated-native-comparison-fragment-20261102";

#[derive(Clone, Copy)]
struct Workload {
    large_bytes: u64,
    small_count: usize,
    warm_repeats: usize,
}

#[derive(Serialize)]
struct OldRequest {
    id: String,
    op: String,
    payload: String,
    body: Option<serde_bytes::ByteBuf>,
    body_len: Option<u64>,
}
#[derive(Deserialize)]
struct OldResponse {
    id: String,
    ok: bool,
    status: Option<u16>,
    payload: Option<String>,
    body: Option<serde_bytes::ByteBuf>,
    body_len: Option<u64>,
    error: Option<Value>,
}
struct OldClient {
    stream: TcpStream,
    sequence: u64,
}
// Match the current SDK's 128 KiB copy buffer and stall limits, so the old
// adapter does not impose Tokio's smaller default copy buffer on just NCX.
async fn copy_exact<R: tokio::io::AsyncRead + Unpin, W: tokio::io::AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    mut remaining: u64,
) -> Result<()> {
    let mut buffer = vec![0; 128 * 1024];
    while remaining > 0 {
        let take = remaining.min(buffer.len() as u64) as usize;
        let bytes =
            tokio::time::timeout(Duration::from_secs(120), reader.read(&mut buffer[..take]))
                .await??;
        if bytes == 0 {
            return Err("short native comparison body".into());
        }
        tokio::time::timeout(Duration::from_secs(120), writer.write_all(&buffer[..bytes]))
            .await??;
        remaining -= bytes as u64;
    }
    writer.flush().await?;
    Ok(())
}
impl OldClient {
    async fn connect(address: &str) -> Result<Self> {
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            sequence: 0,
        })
    }
    async fn send(&mut self, op: &str, payload: Value, body_len: Option<u64>) -> Result<String> {
        self.sequence += 1;
        let id = self.sequence.to_string();
        let frame = rmp_serde::to_vec(&OldRequest {
            id: id.clone(),
            op: op.into(),
            payload: serde_json::to_string(&payload)?,
            body: None,
            body_len,
        })?;
        self.stream.write_u32(frame.len().try_into()?).await?;
        self.stream.write_all(&frame).await?;
        self.stream.flush().await?;
        Ok(id)
    }
    async fn header(&mut self, id: &str) -> Result<OldResponse> {
        let bytes = self.stream.read_u32().await? as usize;
        if bytes == 0 || bytes > 8 * 1024 * 1024 {
            return Err("invalid NCX control frame length".into());
        }
        let mut frame = vec![0; bytes];
        self.stream.read_exact(&mut frame).await?;
        let response: OldResponse = rmp_serde::from_slice(&frame)?;
        if response.id != id {
            return Err("NCX response ID mismatch".into());
        }
        Ok(response)
    }
    async fn response(&mut self, id: &str) -> Result<OldResponse> {
        let mut response = self.header(id).await?;
        if response.body.is_none()
            && let Some(bytes) = response.body_len
        {
            if bytes > 8 * 1024 * 1024 {
                return Err("unexpected large NCX control body".into());
            }
            let mut body = vec![0; bytes as usize];
            self.stream.read_exact(&mut body).await?;
            response.body = Some(body.into());
        }
        Ok(response)
    }
    fn check(response: &OldResponse) -> Result<()> {
        if !response.ok || response.status.is_some_and(|s| s >= 400) {
            return Err(format!("NCX status {:?}: {:?}", response.status, response.error).into());
        }
        Ok(())
    }
    async fn control_raw(&mut self, op: &str, payload: Value) -> Result<OldResponse> {
        let id = self.send(op, payload, None).await?;
        self.response(&id).await
    }
    async fn control(&mut self, op: &str, payload: Value) -> Result<Value> {
        let response = self.control_raw(op, payload).await?;
        Self::check(&response)?;
        if let Some(body) = response.body {
            return Ok(rmp_serde::from_slice(&body)?);
        }
        Ok(response
            .payload
            .map(|v| serde_json::from_str(&v))
            .transpose()?
            .unwrap_or(Value::Null))
    }
    async fn put(&mut self, object: &Object, block: Arc<Vec<u8>>) -> Result<Value> {
        let id = self.send("upload.one_shot", json!({"object_key":object.key,"content_type":"application/octet-stream","durability":"strict"}), Some(object.bytes)).await?;
        copy_exact(
            &mut Payload::new(block, object.bytes),
            &mut self.stream,
            object.bytes,
        )
        .await?;
        self.stream.flush().await?;
        let response = self.response(&id).await?;
        Self::check(&response)?;
        if response.status != Some(201) {
            return Err("NCX upload did not return Created".into());
        }
        let metadata: Value = rmp_serde::from_slice(
            response
                .body
                .as_ref()
                .ok_or("NCX upload missing metadata")?,
        )?;
        if metadata["total_size_bytes"].as_u64() != Some(object.bytes)
            || metadata["data_shards"] != 1
            || metadata["parity_shards"] != 1
        {
            return Err("NCX upload returned unexpected size or EC topology".into());
        }
        Ok(json!({
            "block_bytes": metadata["stripe_size_bytes"],
            "data_shards": metadata["data_shards"],
            "parity_shards": metadata["parity_shards"]
        }))
    }
    async fn get(&mut self, object: &Object) -> Result<()> {
        let id = self
            .send(
                "object.download_stream",
                json!({"object_key":object.key}),
                None,
            )
            .await?;
        let response = self.header(&id).await?;
        Self::check(&response)?;
        let mut sink = HashSink::default();
        if let Some(body) = response.body {
            sink.write_all(&body).await?;
        } else {
            let length = response.body_len.ok_or("NCX download missing length")?;
            if length != object.bytes {
                return Err("NCX announced download length mismatch".into());
            }
            copy_exact(&mut self.stream, &mut sink, length).await?;
        }
        if sink.bytes != object.bytes || format!("{:x}", sink.hash.finalize()) != object.hash {
            return Err("NCX download length/SHA-256 mismatch".into());
        }
        Ok(())
    }
}
/// Frozen pre-purge LINK wire adapter, confined to this benchmark. Production
/// APIs and SDKs expose only the key-based contract.
struct FrozenLink {
    stream: TcpStream,
    sequence: u64,
}
impl FrozenLink {
    async fn send(&mut self, op: &str, args: Value, body_len: u64) -> Result<u64> {
        self.sequence += 1;
        let frame = rmp_serde::to_vec_named(
            &json!({"id":self.sequence,"command":{"op":op,"args":args},"body_len":body_len}),
        )?;
        self.stream.write_u32(frame.len().try_into()?).await?;
        self.stream.write_all(&frame).await?;
        self.stream.flush().await?;
        Ok(self.sequence)
    }
    async fn response(&mut self, id: u64) -> Result<n1_native_client::Response> {
        let length = self.stream.read_u32().await? as usize;
        if length == 0 || length > 8 * 1024 * 1024 {
            return Err("invalid frozen LINK response length".into());
        }
        let mut bytes = vec![0; length];
        self.stream.read_exact(&mut bytes).await?;
        let response: n1_native_client::Response = rmp_serde::from_slice(&bytes)?;
        if response.id != id {
            return Err("frozen LINK response ID mismatch".into());
        }
        Ok(response)
    }
    fn check(response: n1_native_client::Response) -> Result<Value> {
        if response.status != 200 || response.body_len != 0 {
            return Err(format!(
                "frozen LINK status {}: {:?}",
                response.status, response.error
            )
            .into());
        }
        Ok(response.result.unwrap_or(Value::Null))
    }
    async fn control(&mut self, op: &str, args: Value) -> Result<Value> {
        let id = self.send(op, args, 0).await?;
        Self::check(self.response(id).await?)
    }
    async fn upload(
        &mut self,
        bytes: u64,
        hash: &str,
        block: Arc<Vec<u8>>,
    ) -> Result<(Object, f64, Value)> {
        let timer = Instant::now();
        let upload = self
            .control(
                "upload.init",
                json!({
                    "pool": null,
                    "total_bytes": bytes,
                    "part_bytes": bytes.max(1).div_ceil(BLOCK)*BLOCK,
                    "durable": true,
                    "ttl_seconds": 86400
                }),
            )
            .await?;
        let init_seconds = timer.elapsed().as_secs_f64();
        let object = Object {
            key: field(&upload, "object_key")?,
            ticket: field(&upload, "upload_key")?,
            bytes,
            hash: hash.into(),
        };
        let id = self
            .send("upload.put", json!({"upload_key":object.ticket}), bytes)
            .await?;
        copy_exact(&mut Payload::new(block, bytes), &mut self.stream, bytes).await?;
        Self::check(self.response(id).await?)?;
        Ok((
            object,
            init_seconds,
            json!({"block_bytes":BLOCK,"data_shards":1,"parity_shards":1}),
        ))
    }
    async fn get(&mut self, object: &Object) -> Result<()> {
        let id = self
            .send(
                "object.get",
                json!({"object_key":object.key,"offset":0,"length":null}),
                0,
            )
            .await?;
        let response = self.response(id).await?;
        if response.status != 200 || response.body_len != object.bytes {
            return Err("frozen LINK GET status/length mismatch".into());
        }
        let mut sink = HashSink::default();
        copy_exact(&mut self.stream, &mut sink, object.bytes).await?;
        if sink.bytes != object.bytes || format!("{:x}", sink.hash.finalize()) != object.hash {
            return Err("frozen LINK GET SHA-256 mismatch".into());
        }
        Ok(())
    }
}
enum Client {
    Ncx(OldClient),
    Link(N1NativeClient),
    Frozen(FrozenLink),
}
impl Client {
    async fn upload(
        &mut self,
        sequence: usize,
        bytes: u64,
        hash: &str,
        block: Arc<Vec<u8>>,
    ) -> Result<(Object, f64, Value)> {
        match self {
            Self::Frozen(client) => client.upload(bytes, hash, block).await,
            Self::Ncx(client) => {
                let object = Object {
                    key: format!("comparison/object-{sequence:04}.bin"),
                    ticket: String::new(),
                    bytes,
                    hash: hash.into(),
                };
                let layout = client.put(&object, block).await?;
                Ok((object, 0.0, layout))
            }
            Self::Link(client) => {
                let start = Instant::now();
                let object = init(client, bytes, BLOCK, hash.into()).await?;
                let init_seconds = start.elapsed().as_secs_f64();
                client
                    .put(
                        object.ticket.clone(),
                        bytes,
                        &mut Payload::new(block, bytes),
                    )
                    .await?;
                Ok((
                    object,
                    init_seconds,
                    json!({"block_bytes":BLOCK,"data_shards":1,"parity_shards":1}),
                ))
            }
        }
    }
    async fn get(&mut self, object: &Object) -> Result<()> {
        match self {
            Self::Ncx(c) => c.get(object).await,
            Self::Link(c) => get(c, object).await,
            Self::Frozen(c) => c.get(object).await,
        }
    }
    async fn stat(&mut self, object: &Object) -> Result<()> {
        match self {
            Self::Frozen(c) => {
                c.control("object.stat", json!({"object_key":object.key}))
                    .await?;
            }
            Self::Ncx(c) => {
                c.control("namespace.stat", json!({"path":object.key}))
                    .await?;
            }
            Self::Link(c) => {
                c.control(Command::ObjectStat {
                    object_key: object.key.clone(),
                })
                .await?;
            }
        }
        Ok(())
    }
    async fn delete(&mut self, object: &Object) -> Result<()> {
        match self {
            Self::Frozen(c) => {
                c.control("object.delete", json!({"object_key":object.key}))
                    .await?;
            }
            Self::Ncx(c) => {
                c.control("object.delete", json!({"object_key":object.key}))
                    .await?;
            }
            Self::Link(c) => {
                c.control(Command::ObjectDelete {
                    object_key: object.key.clone(),
                })
                .await?;
            }
        }
        Ok(())
    }
    async fn absent(&mut self, object: &Object) -> Result<()> {
        match self {
            Self::Frozen(c) => {
                let id = c
                    .send("object.stat", json!({"object_key":object.key}), 0)
                    .await?;
                let response = c.response(id).await?;
                if response.status != 410 || response.body_len != 0 {
                    return Err("frozen LINK terminal status mismatch".into());
                }
            }
            Self::Ncx(c) => {
                let response = c
                    .control_raw("namespace.stat", json!({"path":object.key}))
                    .await?;
                if response.ok || response.status != Some(404) {
                    return Err("NCX trashed object remains visible".into());
                }
            }
            Self::Link(c) => {
                let error = c
                    .control(Command::ObjectStat {
                        object_key: object.key.clone(),
                    })
                    .await
                    .err()
                    .ok_or("LINK retired object remains visible")?;
                if !matches!(&error, n1_native_client::NativeClientError::Native { code, .. } if code == "404")
                {
                    return Err(format!("LINK retirement returned {error}").into());
                }
            }
        }
        Ok(())
    }
}

pub async fn run(args: Vec<String>) -> Result<()> {
    if args.len() != 6 || cfg!(debug_assertions) {
        return Err(
            "release rc2_bench (--compare|--compare-smoke|--compare-link|--compare-reads-link|--compare-reads-direct|--compare-reads-ncx) baseline-binary candidate-binary PROJECT-parent TEST-parent"
                .into(),
        );
    }
    let old = PathBuf::from(&args[2]).canonicalize()?;
    let new = PathBuf::from(&args[3]).canonicalize()?;
    let integrity_pair = args[1] == "--compare-integrity";
    let direct_pair = args[1] == "--compare-reads-direct" || integrity_pair;
    let native_pair = args[1] == "--compare-reads-link" || direct_pair;
    let aligned_pair = args[1].starts_with("--compare-aligned-");
    let large_link = args[1] == "--compare-large-link" || args[1] == "--compare-aligned-large";
    let large_only = large_link || args[1] == "--compare-large-ncx";
    let read_only =
        (native_pair && !integrity_pair) || args[1] == "--compare-reads-ncx" || large_only;
    let purge_pair = args[1] == "--compare-key-purge" || large_link;
    let upload_paths =
        args[1].starts_with("--compare-upload-paths") || args[1] == "--compare-aligned-uploads";
    let key_pair = args[1] == "--compare-key-performance" || upload_paths || aligned_pair;
    let link_pair = args[1] == "--compare-link" || native_pair || purge_pair || key_pair;
    let baseline_sha = if aligned_pair {
        ALIGNED_BASELINE_SHA
    } else if key_pair {
        KEY_BASELINE_SHA
    } else if purge_pair {
        PURGE_BASELINE_SHA
    } else if direct_pair {
        DIRECT_BASELINE_SHA
    } else if native_pair {
        NATIVE_BASELINE_SHA
    } else if link_pair {
        LINK_BASELINE_SHA
    } else {
        NCX_SHA
    };
    if format!("{:x}", Sha256::digest(fs::read(&old)?)) != baseline_sha {
        return Err("binary does not match the preserved comparison baseline".into());
    }
    let parents = [
        PathBuf::from(&args[4]).canonicalize()?,
        PathBuf::from(&args[5]).canonicalize()?,
    ];
    if parents[0] != Path::new("/mnt/test-hdd-a")
        || parents[1] != Path::new("/mnt/test-hdd-b")
        || parents[0].metadata()?.dev() == parents[1].metadata()?.dev()
    {
        return Err(
            "comparison requires the independent /mnt/test-hdd-a and /mnt/test-hdd-b mounts".into(),
        );
    }
    let smoke = args[1] == "--compare-smoke"
        || args[1] == "--compare-upload-paths-smoke"
        || args[1] == "--compare-aligned-smoke";
    let order = if large_link {
        vec![
            "link-before",
            "link-after",
            "link-after",
            "link-before",
            "link-before",
            "link-after",
            "link-after",
            "link-before",
        ]
    } else if large_only {
        vec!["ncx", "link", "link", "ncx", "ncx", "link", "link", "ncx"]
    } else if link_pair && smoke {
        vec!["link-before", "link-after"]
    } else if link_pair {
        vec!["link-before", "link-after", "link-after", "link-before"]
    } else if smoke {
        vec!["ncx", "link"]
    } else {
        vec!["ncx", "link", "link", "ncx"]
    };
    let name = format!(
        "n1-{}-comparison-{}-{}",
        if read_only {
            "read-path"
        } else if link_pair {
            "link-optimization"
        } else {
            "ncx-link"
        },
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let directories = [parents[0].join(&name), parents[1].join(&name)];
    for directory in &directories {
        fs::DirBuilder::new().mode(0o700).create(directory)?;
    }
    println!("Comparison evidence: {}", directories[0].display());
    let provenance = json!({
        "old_binary": old,
        "old_sha256": NCX_SHA,
        "old_clean_source_head": "f1e78c58963b28efb6ae382fe95b1b4d61cc8008",
        "new_binary": new,
        "new_sha256": format!("{:x}",Sha256::digest(fs::read(&new)?)),
        "client_sha256": format!("{:x}",Sha256::digest(fs::read(std::env::current_exe()?)?)),
        "smoke_only": smoke,
        "order": if smoke {
            vec!["ncx","link"]
        } else {
            vec!["ncx","link","link","ncx"]
        },
        "members": {
            "devices": [parents[0].metadata()?.dev(),parents[1].metadata()?.dev()],
            "data_shards": 1,
            "parity_shards": 1,
            "block_bytes": BLOCK
        },
        "large_bytes": if smoke {
            1048576
        } else {
            LARGE
        },
        "small_bytes": SMALL,
        "small_count": if smoke {
            3
        } else {
            COUNT
        },
        "transport": "one authenticated native loopback TCP client; no payload TLS for either",
        "cache": "first GET follows durable PUT; second GET warm; no cold-cache claim or global cache drop",
        "put_timing": "end-to-end including LINK init; NCX one-shot includes internal initialization",
        "durability": "NCX explicit strict; LINK durable ticket; required io_uring both",
        "deletion": "NCX moves to recoverable trash; LINK permanently retires key; not equivalent guarantees",
        "not_measured": ["batch (old native protocol has no batch upload operation)","multipart","repair","GC","backup","replication","concurrent saturation","scale"],
        "kernel": fs::read_to_string("/proc/sys/kernel/osrelease")?.trim(),
        "cpu": fs::read_to_string("/proc/cpuinfo")?.lines().find(|l|l.starts_with("model name")),
        "old_http_listener": "mandatory idle HTTPS listener; benchmark payload uses native only",
        "background": "desktop/editor remain active; no concurrent test or build intended"
    });
    let mut provenance = provenance;
    provenance["order"] = json!(order);
    provenance["old_sha256"] = json!(baseline_sha);
    provenance["geometry_note"] = json!(
        "Both configure 256 KiB: NCX preserves its tested per-object adaptive 4 KiB tiny-object/16 KiB small-object selection; LINK uses its immutable 256 KiB pool block. Upload rows record effective layouts from NCX acknowledgements and LINK pool configuration. This is a product configuration comparison, not isolation of metadata overhead alone."
    );
    if link_pair {
        provenance["not_measured"] = json!([
            "batch (separate full-workload report)",
            "multipart",
            "repair",
            "GC",
            "backup",
            "replication",
            "concurrent saturation",
            "scale"
        ]);
        provenance["old_clean_source_head"] = Value::Null;
        provenance["baseline_source_snapshot"] = json!(if native_pair {
            "restoration-checkpoint-20261102-05.tar.gz"
        } else {
            "restoration-checkpoint-20261102-04.tar.gz"
        });
        provenance["old_http_listener"] = json!("HTTP off for both LINK binaries");
        provenance["geometry_note"] = json!(
            "Both LINK binaries use identical immutable 256 KiB 1+1 geometry and the same shard encoding. The optimized binary may leave canonical zero padding sparse."
        );
        provenance["put_timing"] =
            json!("end-to-end including ticket initialization for both binaries");
        provenance["durability"] =
            json!("durable tickets and required io_uring for both LINK binaries");
        provenance["deletion"] =
            json!("identical permanent retirement guarantees for both LINK binaries");
        if direct_pair {
            provenance["baseline_source_snapshot"] = Value::Null;
            provenance["baseline_source_evidence"] = json!(
                "direct delivery checkpoint: native-final-tests147.log, native-final-clippy148.log, native-final-release149.log; frozen binary hash identifies the baseline; no separate source archive claimed"
            );
        }
    }
    if read_only {
        provenance["small_count"] = json!(0);
        provenance["warm_repeats"] = json!(8);
        provenance["focus"] = json!(
            "one 1 GiB durable PUT, one post-write verified GET, eight consecutive verified warm GETs, stat, delete and absence per trial; no small-object or batch timing"
        );
    }
    if integrity_pair {
        provenance["baseline_source_snapshot"] = json!("restoration-checkpoint-20261102-07.tar.gz");
        provenance["baseline_source_evidence"] =
            json!("selected direct-delivery binary before the hybrid integrity change");
        provenance["geometry_note"] = json!(
            "Identical immutable 256 KiB 1+1 geometry; baseline sha256-v1 frame/source policy versus candidate crc32c-sha256-v2 frame/context-bound source policy. Separate new pools; existing pools are never reinterpreted."
        );
    }
    if purge_pair {
        provenance["baseline_source_snapshot"] =
            json!("/mnt/test-project/n1-key-purge-20261105-01/source/before.tar");
        provenance["geometry_note"] = json!(
            "Identical 256 KiB 1+1 CRC32C/SHA-256 geometry. Frozen LINK uses tickets/terminal records; candidate creates incomplete headers at Begin, uses the same LINK key throughout, and purges to absence."
        );
        provenance["put_timing"] = json!(
            "end-to-end including Begin for both; candidate persists its headers with the first durable payload or completed publication"
        );
        provenance["durability"] = json!("durable payload and required io_uring for both");
        provenance["deletion"] = json!(
            "baseline writes permanent terminal records; candidate unlinks all incomplete/final shards and persists removal; different storage residue"
        );
    } else if key_pair {
        provenance["baseline_source_snapshot"] =
            json!("/mnt/test-project/n1-key-purge-20261105-01/source/final-source.tar");
        provenance["put_timing"] = json!(
            "end-to-end including Begin for both; baseline persists empty allocation, candidate persists it with the first durable payload or completed publication"
        );
        provenance["durability"] = json!(
            "Both preserve durable part and completed-object acknowledgements. Baseline also persisted unused Begin allocations; candidate uses the operator-approved allocation-only Begin and persists headers/paths with the first durable payload. Required io_uring for both."
        );
        provenance["deletion"] = json!("identical key-only purge to durable absence for both");
    } else if !link_pair {
        provenance["durability"] = json!(
            "NCX explicit strict; LINK durable PUT with allocation-only Begin; required io_uring for both"
        );
        provenance["deletion"] = json!(
            "NCX moves to recoverable trash; LINK permanently removes shard files; not equivalent lifecycle operations"
        );
    }
    if upload_paths {
        provenance["focus"] = json!(
            "standalone native object engine: batch, standard/durable multipart, reconnect/status, finalize/repeated finalize, bounded concurrent normal PUT, exact GET and zero-residue purge"
        );
        provenance["large_bytes"] = Value::Null;
        provenance["multipart_bytes"] = json!(if smoke {
            2 * 1024 * 1024
        } else {
            256 * 1024 * 1024
        });
        provenance["part_bytes"] = json!(if smoke { 1024 * 1024 } else { 8 * 1024 * 1024 });
        provenance["concurrent_clients"] = json!(if smoke { 2 } else { 16 });
        provenance["not_measured"] = json!([
            "metadata",
            "Console",
            "population scale",
            "hardware power loss"
        ]);
    }
    if large_only {
        provenance["focus"] = json!(
            "eight standalone 1 GiB trials in ABBA ABBA order; four per binary; no small-object cohort, to isolate large upload/read/stat/delete from the preceding 100-object retirement/purge workload"
        );
        provenance["warm_repeats"] = json!(1);
    }
    if aligned_pair {
        provenance["baseline_source_snapshot"] =
            json!("/mnt/test-hdd-a/n1-xfs-rebenchmark-20261105-01/bootstrap/current-source.tar");
        provenance["geometry_note"] = json!(
            "Identical immutable 256 KiB 1+1 CRC32C/SHA-256 shard bytes and LINK identity. Baseline uses buffered payload I/O; candidate streams aligned complete pages through O_DIRECT, buffering only each part's unaligned ends. Small objects remain buffered. No format change."
        );
        provenance["put_timing"] = json!("end-to-end including allocation-only Begin for both");
        provenance["durability"] = json!(
            "Identical first durable payload/header/path persistence and completed-publication barriers; required io_uring for both"
        );
        provenance["deletion"] = json!("identical key-only purge to durable absence for both");
        provenance["cache"] = json!(
            "First verified GET follows durable PUT: buffered baseline may have resident payload pages, O_DIRECT candidate does not populate interior pages. Second GET is warm for both. No cold-cache claim or global cache drop."
        );
    }
    private_json(&directories[0].join("provenance.json"), &provenance)?;
    let block = pattern(0x20261102);
    let large_bytes = if smoke { 1048576 } else { LARGE };
    let hashes = [expected(&block, large_bytes), expected(&block, SMALL)];
    let mut trials = Vec::new();
    private_json(
        &directories[0].join("expected-hashes.json"),
        &json!({"large_bytes":large_bytes,"large_sha256":hashes[0],"small_bytes":SMALL,"small_sha256":hashes[1]}),
    )?;
    for (index, architecture) in order.into_iter().enumerate() {
        let roots = [
            directories[0].join(format!("trial-{}-{architecture}", index + 1)),
            directories[1].join(format!("trial-{}-{architecture}", index + 1)),
        ];
        for root in &roots {
            fs::DirBuilder::new().mode(0o700).create(root)?;
        }
        let result = tokio::time::timeout(Duration::from_secs(900), async {
            if upload_paths {
                return uploads::trial(
                    if architecture == "link-before" {
                        &old
                    } else {
                        &new
                    },
                    architecture,
                    &roots,
                    block.clone(),
                    smoke,
                )
                .await;
            }
            trial(
                if architecture == "ncx" || architecture == "link-before" {
                    &old
                } else {
                    &new
                },
                architecture,
                &roots,
                block.clone(),
                &hashes,
                Workload {
                    large_bytes,
                    small_count: if read_only {
                        0
                    } else if smoke {
                        3
                    } else {
                        COUNT
                    },
                    warm_repeats: if read_only && !large_only { 8 } else { 1 },
                },
            )
            .await
        })
        .await;
        let result = match result {
            Ok(result) => result,
            Err(_) => Err("comparison trial exceeded 15 minute deadline".into()),
        };
        match result {
            Ok(value) => {
                private_json(&roots[0].join("summary.json"), &value)?;
                trials.push(value);
            }
            Err(error) => {
                private_json(
                    &roots[0].join("failed.json"),
                    &json!({"error":error.to_string()}),
                )?;
                return Err(error);
            }
        }
    }
    private_json(
        &directories[0].join("summary.json"),
        &json!({"passed":true,"smoke_only":smoke,"provenance":provenance,"trials":trials}),
    )?;
    println!(
        "Comparison {} passed, every GET length/SHA checked, all owned servers stopped: {}",
        if smoke { "smoke" } else { "ABBA" },
        directories[0].display()
    );
    Ok(())
}

fn log(path: &Path) -> Result<fs::File> {
    Ok(fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?)
}
fn port() -> Result<String> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .to_string())
}

async fn start(binary: &Path, architecture: &str, roots: &[PathBuf; 2]) -> Result<(Owned, Client)> {
    let address = port()?;
    let members = [roots[0].join("member-0"), roots[1].join("member-1")];
    for member in &members {
        fs::create_dir(member)?;
    }
    let output = log(&roots[0].join("engine.log"))?;
    let (mut owned, credentials) = if architecture != "ncx" {
        let config = roots[0].join("service.json");
        let init_log = log(&roots[0].join("init.log"))?;
        let mut enrolling = Owned(
            Process::new(binary)
                .arg("init")
                .arg("--config")
                .arg(&config)
                .arg("--member")
                .arg(&members[0])
                .arg("--member")
                .arg(&members[1])
                .args([
                    "--data-shards",
                    "1",
                    "--parity-shards",
                    "1",
                    "--block-bytes",
                    "262144",
                    "--native-address",
                ])
                .arg(&address)
                .stdin(Stdio::null())
                .stdout(init_log.try_clone()?)
                .stderr(init_log)
                .spawn()?,
        );
        if !wait(&mut enrolling.0, 60).await?.success() {
            return Err("LINK enrollment failed".into());
        }
        let mut configuration: Value = serde_json::from_reader(fs::File::open(&config)?)?;
        configuration["rate_limits"]["enabled"] = json!(false);
        fs::write(&config, serde_json::to_vec_pretty(&configuration)?)?;
        let credentials: Value =
            serde_json::from_reader(fs::File::open(config.with_extension("credentials.json"))?)?;
        (
            Owned(
                Process::new(binary)
                    .arg("serve")
                    .arg("--config")
                    .arg(config)
                    .env("N1_IO_BACKEND", "uring")
                    .env("RUST_LOG", "info")
                    .stdin(Stdio::null())
                    .stdout(output.try_clone()?)
                    .stderr(output)
                    .spawn()?,
            ),
            Some(credentials),
        )
    } else {
        configure_old(&roots[0], &members, &address)?;
        (
            Owned(
                Process::new(binary)
                    .current_dir(&roots[0])
                    .env("N1_IO_BACKEND", "uring")
                    .env("N1_REFRESH_STORE_ROOT", roots[0].join("refresh"))
                    .env_remove("N1_BACKUP_OPERATOR_CONFIG")
                    .env("RUST_LOG", "info")
                    .stdin(Stdio::null())
                    .stdout(output.try_clone()?)
                    .stderr(output)
                    .spawn()?,
            ),
            None,
        )
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = owned.0.try_wait()? {
            return Err(format!("{architecture} exited during startup: {status}").into());
        }
        if let Ok(probe) = TcpStream::connect(&address).await {
            drop(probe);
            break;
        }
        if Instant::now() >= deadline {
            return Err("native server startup deadline exceeded".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let client = if let Some(credentials) = credentials {
        if architecture == "link-before"
            && ![KEY_BASELINE_SHA, ALIGNED_BASELINE_SHA]
                .contains(&format!("{:x}", Sha256::digest(fs::read(binary)?)).as_str())
        {
            let stream = TcpStream::connect(&address).await?;
            stream.set_nodelay(true)?;
            let mut client = FrozenLink {
                stream,
                sequence: 0,
            };
            client
                .control(
                    "auth.fragment",
                    json!({
                        "fragment": field(&credentials,"fragment")?,
                        "secret": field(&credentials,"secret")?,
                        "permissions": null,
                        "ttl_seconds": null
                    }),
                )
                .await?;
            return Ok((owned, Client::Frozen(client)));
        }
        let mut client = N1NativeClient::connect(&format!("n1://{address}")).await?;
        client
            .auth_fragment(
                &field(&credentials, "fragment")?,
                &field(&credentials, "secret")?,
            )
            .await?;
        Client::Link(client)
    } else {
        let mut client = OldClient::connect(&address).await?;
        client.control("ping", json!({})).await?;
        let created = client
            .control(
                "fragment.create",
                json!({"name":"native-comparison","secret":MASTER,"key":SECRET}),
            )
            .await?;
        let fragment = field(&created, "fragment_hash")?;
        client
            .control(
                "auth.fragment",
                json!({
                    "fragment": fragment,
                    "secret": SECRET,
                    "read": true,
                    "write": true,
                    "modify": true,
                    "delete": true,
                    "expires_in": 21600
                }),
            )
            .await?;
        Client::Ncx(client)
    };
    Ok((owned, client))
}

fn configure_old(directory: &Path, members: &[PathBuf; 2], address: &str) -> Result<()> {
    let cert = directory.join("cert.pem");
    let key = directory.join("key.pem");
    let status = Process::new("openssl")
        .args([
            "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "1", "-nodes", "-keyout",
        ])
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .args([
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        return Err("local old-engine TLS fixture generation failed".into());
    }
    fs::create_dir(directory.join("fs"))?;
    fs::create_dir(directory.join("refresh"))?;
    fs::write(
        directory.join("n1.env"),
        format!(
            concat!(
                "JWT_SECRET=isolated-comparison-jwt-secret-20261102\n",
                "MASTER_KEY={MASTER}\n",
                "AUTH_KEY={MASTER}\n",
                "ADMIN_KEY={MASTER}\n",
                "ACCESS_TOKEN_DEFAULT_TTL_SECS=3600\n",
                "ACCESS_TOKEN_MAX_TTL_SECS=21600\n",
                "REFRESH_TOKEN_TTL_SECS=604800\n",
            ),
            MASTER = MASTER
        ),
    )?;
    let https = port()?.parse::<std::net::SocketAddr>()?.port();
    let native = address.parse::<std::net::SocketAddr>()?.port();
    fs::write(
        directory.join("n1_config.ini"),
        format!(
            r#"[server]
host=127.0.0.1
https_port={https}
native_port={native}
cert_path={cert}
key_path={key}
[disk_config]
fs_storage_path={control}
[storage_pool.A]
generation=1
roots={first},{second}
data_shards=1
parity_shards=1
sealed=false
[index_config]
partition_count=64
per_db_cache_bytes=auto
max_cached_dbs=auto
[namespace_config]
max_loaded_facades=auto
max_loaded_dbs=auto
[garbage_collector]
cleanup_interval_seconds=86400
file_ttl_seconds=86400
trash_retention_seconds=2592000
[auto_healing]
enabled=false
topology_check_interval_seconds=30
topology_recovery_stability_seconds=0
initial_scan_delay_seconds=3600
availability_scan_interval_seconds=3600
checksum_scrub_interval_seconds=604800
object_scan_concurrency=1
shard_repair_concurrency=1
metadata_batch_size=128
[export]
retention_seconds=86400
cleanup_interval_seconds=60
max_concurrent_jobs=2
max_queued_jobs=128
manifest_validation_concurrency=8
object_read_concurrency=8
artifact_chunk_size_bytes=16777216
artifact_write_concurrency=4
max_buffered_bytes_per_job=134217728
max_buffered_bytes_global=268435456
max_concurrent_downloads=32
max_retained_bytes=0
[set_limits]
max_file_size=1099511627776
[jwt_token_config]
login_token_expiration=900
[rate_limiter_config]
enable_rate_limiting=false
auth_limit_per_second=5000
auth_limit_burst_size=10000
view_limit_per_second=5000
view_limit_burst_size=10000
upload_limit_per_second=5000
upload_limit_burst_size=10000
global_limit_per_second=5000
global_limit_burst_size=10000
[upload_config]
max_concurrent_io=16
adaptive_concurrency_enabled=true
adaptive_concurrency_interval_ms=1000
writer_buffer_size=auto
upload_memory_budget_bytes=268435456
upload_part_strict_durability_default=true
upload_io_gate_enabled=true
stream_max_concurrency=128
stream_chunk_size=262144
stream_readahead_bytes=524288
stream_locality_bias_enabled=true
monitor_interval_seconds=auto
monitor_deep_scan_interval_seconds=auto
max_part_size=1073741824
stripe_block_size=262144
"#,
            cert = cert.display(),
            key = key.display(),
            control = directory.join("fs").display(),
            first = members[0].display(),
            second = members[1].display()
        ),
    )?;
    Ok(())
}

async fn trial(
    binary: &Path,
    architecture: &str,
    roots: &[PathBuf; 2],
    block: Arc<Vec<u8>>,
    hashes: &[String; 2],
    workload: Workload,
) -> Result<Value> {
    let (mut owned, mut client) = start(binary, architecture, roots).await?;
    let pid = owned.0.id();
    let events = roots[0].join("measurements.jsonl");
    let mut rows = Vec::new();
    let mut objects = Vec::new();
    for (name, bytes, total, hash) in [
        ("large", workload.large_bytes, 1, &hashes[0]),
        ("small", SMALL, workload.small_count, &hashes[1]),
    ] {
        if total == 0 {
            continue;
        }
        let mut cohort = Vec::new();
        let mut samples = Vec::new();
        let mut init_seconds = 0.0;
        let mut layouts = Vec::new();
        let phase = Phase::start(pid)?;
        for _ in 0..total {
            let timer = Instant::now();
            let (object, initialization, layout) = client
                .upload(objects.len() + cohort.len(), bytes, hash, block.clone())
                .await?;
            samples.push(timer.elapsed().as_secs_f64());
            init_seconds += initialization;
            if !layouts.contains(&layout) {
                layouts.push(layout);
            }
            cohort.push(object);
        }
        let mut row = phase
            .finish(
                pid,
                &format!("{name}_put_end_to_end"),
                bytes * total as u64,
                samples,
            )
            .await?;
        row["link_init_seconds_included"] = json!(init_seconds);
        row["effective_layouts"] = json!(layouts);
        println!(
            "{architecture} {name} durable PUT: {:.3}s",
            row["wall_seconds"].as_f64().unwrap_or(0.0)
        );
        append(&events, &row)?;
        rows.push(row);
        for temperature in ["post_write", "warm"] {
            let phase = Phase::start(pid)?;
            let mut samples = Vec::new();
            let repeats = if temperature == "warm" {
                workload.warm_repeats
            } else {
                1
            };
            for _ in 0..repeats {
                for object in &cohort {
                    let timer = Instant::now();
                    client.get(object).await?;
                    samples.push(timer.elapsed().as_secs_f64());
                }
            }
            let row = phase
                .finish(
                    pid,
                    &format!("{name}_get_{temperature}"),
                    bytes * total as u64 * repeats as u64,
                    samples,
                )
                .await?;
            if repeats > 1 {
                println!(
                    "{architecture} {name} warm GET x{repeats}: {:.3}s, {:.2} MiB/s, {:.3} CPU s",
                    row["wall_seconds"].as_f64().unwrap_or(0.0),
                    row["mib_per_second"].as_f64().unwrap_or(0.0),
                    row["server_cpu_seconds"].as_f64().unwrap_or(0.0)
                );
            }
            append(&events, &row)?;
            rows.push(row);
        }
        let phase = Phase::start(pid)?;
        let mut samples = Vec::new();
        for object in &cohort {
            let timer = Instant::now();
            client.stat(object).await?;
            samples.push(timer.elapsed().as_secs_f64());
        }
        let row = phase
            .finish(pid, &format!("{name}_stat"), 0, samples)
            .await?;
        append(&events, &row)?;
        rows.push(row);
        objects.extend(cohort);
    }
    let before_delete = inventory(roots)?;
    let phase = Phase::start(pid)?;
    let mut samples = Vec::new();
    for object in &objects {
        let timer = Instant::now();
        client.delete(object).await?;
        samples.push(timer.elapsed().as_secs_f64());
    }
    let row = phase.finish(pid, "delete_all", 0, samples).await?;
    println!(
        "{architecture} delete {}: {:.3}s",
        objects.len(),
        row["wall_seconds"].as_f64().unwrap_or(0.0)
    );
    append(&events, &row)?;
    rows.push(row);
    for object in &objects {
        client.absent(object).await?;
    }
    let after_delete = inventory(roots)?;
    if (architecture == "link" || architecture == "link-after")
        && (after_delete["files"] != 2
            || after_delete["symlinks"] != 0
            || after_delete["terminal_files"] != 0
            || after_delete["per_identity_staging_directories"] != 0)
    {
        return Err(format!("purge left identity residue: {after_delete}").into());
    }
    drop(client);
    if unsafe { libc::kill(pid as i32, libc::SIGTERM) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let status = wait(&mut owned.0, 60).await?;
    if !status.success() {
        return Err(format!("{architecture} shutdown failed: {status}").into());
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
        "roots": roots,
        "clean_shutdown": true,
        "verified_downloads": objects.len()*(workload.warm_repeats+1),
        "objects_deleted": objects.len(),
        "before_delete_inventory": before_delete,
        "after_delete_inventory": after_delete,
        "sampled_peak_rss_bytes": peak,
        "kernel_rss_high_water_bytes": high,
        "phases": rows
    }))
}
