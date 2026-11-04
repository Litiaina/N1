//! Verify every journaled ACK and successful mixed-batch row after restart.
//! This does not PUT objects, retry failed rows, or turn a failed campaign green.
use clap::Parser;
use futures_util::{StreamExt, TryStreamExt};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::BufRead,
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    failure_log: PathBuf,
    #[arg(long)]
    output: PathBuf,
}
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn hash(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut bytes = [0; 65536];
    loop {
        let n = file.read(&mut bytes)?;
        if n == 0 {
            break;
        }
        hasher.update(&bytes[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}
fn copy_database(source: &Path, destination: &Path) -> Result<()> {
    std::fs::DirBuilder::new().mode(0o700).create(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let output = destination.join(entry.file_name());
        if kind.is_dir() {
            copy_database(&entry.path(), &output)?;
        } else if kind.is_file() {
            std::fs::copy(entry.path(), &output)?;
            std::fs::File::open(&output)?.sync_all()?;
        } else {
            return Err("unexpected nonregular file in forensic metadata snapshot".into());
        }
    }
    std::fs::File::open(destination)?.sync_all()?;
    Ok(())
}
fn start(root: &Path, component: &str, binary: &Path, output: &Path) -> Result<Server> {
    let log = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(output.join(format!("{component}.log")))?;
    Ok(Server(
        Command::new(binary)
            .arg("--config")
            .arg(root.join(component).join("n1_config.ini"))
            .current_dir(root.join(component))
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?,
    ))
}
async fn listening(address: &str, process: &mut Server) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    while tokio::time::Instant::now() < deadline {
        if process.0.try_wait()?.is_some() {
            return Err("audit server exited; inspect retained log".into());
        }
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("audit server did not become ready within 180 seconds".into())
}
fn expected(index: u64) -> Vec<u8> {
    let sizes = [0, 1, 127, 1024, 4096, 8193, 32768, 65536];
    let i = index as u32;
    let seed = i.wrapping_mul(131) ^ i.rotate_left(17);
    (0..sizes[index as usize % 8])
        .map(|offset| ((seed ^ (offset as u32).wrapping_mul(17)) % 251) as u8)
        .collect()
}
async fn verify(
    client: &reqwest::Client,
    url: &reqwest::Url,
    token: &str,
    index: u64,
    version: &str,
    size: Option<u64>,
) -> Result<u64> {
    let path = format!("qual/api/{index:09}.bin");
    let response = client
        .get(url.join(&format!("/noa/v1/objects/get/{path}"))?)
        .bearer_auth(token)
        .send()
        .await?;
    if response.status() != 200
        || response
            .headers()
            .get("etag")
            .and_then(|s| s.to_str().ok())
            .map(|s| s.trim_matches('"'))
            != Some(version)
    {
        return Err(format!("acknowledged version did not survive restart: index={index}").into());
    }
    let bytes = response.bytes().await?;
    if bytes.as_ref() != expected(index) || size.is_some_and(|size| size != bytes.len() as u64) {
        return Err(format!("acknowledged payload did not survive restart: index={index}").into());
    }
    Ok(bytes.len() as u64)
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config: Value = serde_json::from_slice(&std::fs::read(args.root.join("stack.json"))?)?;
    let campaign: Value = serde_json::from_slice(&std::fs::read(
        args.root.join("qualification-campaign.json"),
    )?)?;
    let count = campaign["count"].as_u64().ok_or("fixture count missing")?;
    let url = reqwest::Url::parse(config["url"].as_str().ok_or("missing fixture URL")?)?;
    if url.scheme() != "http" || url.host_str() != Some("127.0.0.1") {
        return Err("audit requires the original loopback fixture".into());
    }
    let metadata_address = format!("127.0.0.1:{}", url.port().ok_or("missing fixture port")?);
    let object_address = format!(
        "127.0.0.1:{}",
        config["object_native"]
            .as_u64()
            .ok_or("missing native port")?
    );
    for address in [&metadata_address, &object_address] {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return Err(
                "fixture is already running; refusing to modify its process ownership".into(),
            );
        }
    }
    let mut binaries = Vec::new();
    for component in ["object", "metadata"] {
        let path = PathBuf::from(
            config[format!("{component}_binary")]
                .as_str()
                .ok_or("missing frozen binary")?,
        );
        if Some(hash(&path)?.as_str()) != config[format!("{component}_sha256")].as_str() {
            return Err("frozen executable does not match failed fixture".into());
        }
        binaries.push(path);
    }
    let log = std::fs::read_to_string(&args.failure_log)?;
    let response: Option<Value> = log
        .lines()
        .find_map(|line| line.strip_prefix("qualification failed: batch failed: 207 Multi-Status "))
        .map(serde_json::from_str)
        .transpose()?;
    if response.is_none()
        && !(log
            .lines()
            .any(|line| line.starts_with("qualification failed: error sending request"))
            && log.contains("caused by: operation timed out"))
    {
        return Err(
            "no supported terminal mixed-response or request-timeout failure in retained log"
                .into(),
        );
    }
    let rows = match &response {
        Some(response) => response["results"]
            .as_array()
            .ok_or("mixed response has no rows")?
            .clone(),
        None => Vec::new(),
    };
    let successes = rows
        .iter()
        .filter(|row| row["status"] == 201)
        .collect::<Vec<_>>();
    if response.as_ref().is_some_and(|response| {
        successes.is_empty() || response["uploaded"].as_u64() != Some(successes.len() as u64)
    }) {
        return Err("mixed batch success count mismatch".into());
    }
    let mut acknowledgements = BTreeMap::new();
    let writers = campaign["writers"]
        .as_u64()
        .filter(|v| (1..=64).contains(v))
        .ok_or("invalid original writer count")?;
    for writer in 0..writers {
        let file = std::fs::File::open(args.root.join(format!("api-acks-{writer:02}.jsonl")))?;
        for line in std::io::BufReader::new(file).lines() {
            let rows: Vec<Value> = serde_json::from_str(&line?)?;
            for row in rows {
                let index = row["index"].as_u64().ok_or("ACK has no index")?;
                if index >= count {
                    return Err("ACK index outside fixture".into());
                }
                let version = row["version"].as_str().ok_or("ACK has no version")?;
                if ulid::Ulid::from_string(version)?.to_string() != version {
                    return Err("ACK version is not canonical".into());
                }
                if let Some(previous) = acknowledgements.insert(index, version.to_owned())
                    && previous != version
                {
                    return Err("ACK journal contains conflicting versions".into());
                }
            }
        }
    }
    if acknowledgements.is_empty() && successes.is_empty() {
        return Err("no journaled acknowledgements to audit".into());
    }
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&args.output)?;
    copy_database(
        &args.root.join("metadata/control/metadata"),
        &args.output.join("metadata-before-reopen"),
    )?;
    let mut object = start(&args.root, "object", &binaries[0], &args.output)?;
    listening(&object_address, &mut object).await?;
    let mut metadata = start(&args.root, "metadata", &binaries[1], &args.output)?;
    listening(&metadata_address, &mut metadata).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()?;
    let label = format!(
        "N1 qualification {} api",
        config["identity"]
            .as_str()
            .ok_or("fixture identity missing")?
    );
    let alias = hex::encode(Sha256::digest(label.as_bytes()));
    let login=client.post(url.join("/noa/v1/auth/fragment")?).json(&json!({"fragment":alias,"secret":config["fragment_secret"],"read":true,"write":false,"modify":false,"delete":false,"expires_in":3600})).send().await?;
    if login.status() != 200 {
        return Err("audit read-only login failed".into());
    }
    let auth: Value = login.json().await?;
    let token = auth["access_token"].as_str().ok_or("audit token missing")?;
    let verified_ack_bytes = futures_util::stream::iter(acknowledgements.iter())
        .map(|(&index, version)| verify(&client, &url, token, index, version, None))
        .buffer_unordered(8)
        .try_fold(0u64, |total, bytes| async move { Ok(total + bytes) })
        .await?;
    println!(
        "verified {} journaled ACKs and {verified_ack_bytes} payload bytes",
        acknowledgements.len()
    );
    let mut total = 0u64;
    for row in &successes {
        let path = row["object_key"].as_str().ok_or("captured path missing")?;
        let index = path
            .strip_prefix("qual/api/")
            .and_then(|s| s.strip_suffix(".bin"))
            .ok_or("foreign captured path")?
            .parse::<u64>()?;
        if path != format!("qual/api/{index:09}.bin") {
            return Err("noncanonical captured path".into());
        }
        let version = row["version_id"]
            .as_str()
            .ok_or("captured version missing")?;
        if ulid::Ulid::from_string(version)?.to_string() != version {
            return Err("noncanonical captured version".into());
        }
        total += verify(
            &client,
            &url,
            token,
            index,
            version,
            Some(
                row["total_size_bytes"]
                    .as_u64()
                    .ok_or("captured size missing")?,
            ),
        )
        .await?;
    }
    let report = json!({"passed":true,"scope":"Every journaled ACK and any captured successful mixed-batch row, exact original executables, read-only API token after process restart; no PUT or failed-row retry","verified_journaled_acks":acknowledgements.len(),"verified_ack_bytes":verified_ack_bytes,"verified_success_rows":successes.len(),"verified_bytes":total,"mixed_response_captured":response.is_some(),"captured_failed_rows":response.as_ref().map(|_|rows.len()-successes.len()),"version_mismatches":0,"payload_mismatches":0,"failed_campaign_remains_failed":true,"original_metadata_snapshot":"metadata-before-reopen","note":"Unacknowledged outcomes outside a captured response are not classified. Ordinary server startup may reconcile physically completed pending metadata publications; the pre-open database copy is retained."});
    let mut output = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(args.output.join("PASSED.json"))?;
    output.write_all(&serde_json::to_vec_pretty(&report)?)?;
    output.sync_all()?;
    println!("{report}");
    Ok(())
}
