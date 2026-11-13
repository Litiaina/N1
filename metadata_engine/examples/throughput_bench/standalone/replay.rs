//! Read exactly the same immutable identity across server-binary switches.
//! This mode never uploads, finalizes, heals or deletes an object.
use super::*;
use n1_link::{ObjectKey, PoolLayout, shard::ShardFormat};
use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    identity: PathBuf,
    #[serde(default)]
    advise_roots: Vec<PathBuf>,
}

pub(super) async fn run(
    api: &Api,
    payload: &Payload,
    pids: &Pids,
    config: Config,
    output: &Path,
) -> Result<()> {
    if !config.identity.is_absolute()
        || fs::symlink_metadata(&config.identity)?
            .file_type()
            .is_symlink()
    {
        return Err("read replay requires an absolute, real identity journal".into());
    }
    let saved: Value = serde_json::from_slice(&fs::read(&config.identity)?)?;
    let key = saved["object_key"]
        .as_str()
        .ok_or("missing replay identity")?;
    if saved["plan"]["object_key"] != key
        || saved["plan"]["total_bytes"] != payload.total_bytes
        || saved["source_sha256"] != payload.sha256
    {
        return Err("replay source identity, length or checksum mismatch".into());
    }
    let mut client = api.connect().await?;
    let before = cli_core::result(
        client
            .control(Command::ObjectStat {
                object_key: key.into(),
            })
            .await?,
    )?;
    if before["total_bytes"] != payload.total_bytes
        || before["data_shards"] != payload.data_shards
        || before["finalized_shards"] != payload.width
    {
        return Err("replay object geometry mismatch".into());
    }
    let mut advised = Vec::new();
    if !config.advise_roots.is_empty() {
        if config.advise_roots.len() != payload.width as usize {
            return Err("cache advice requires the complete ordered fixture roots".into());
        }
        let id = ObjectKey::inspect(key)?;
        let fp = id.fingerprint();
        let layout = PoolLayout {
            id: id.pool,
            generation: id.generation,
            data_shards: payload.data_shards as u16,
            parity_shards: (payload.width - payload.data_shards) as u16,
            block_bytes: 262144,
            placement_version: 1,
            shard_format: ShardFormat::Crc32cSha256V2,
        };
        for shard in 0..payload.width as u16 {
            let root = &config.advise_roots[layout.member_slot(fp, shard)?];
            if !root.is_absolute() || fs::symlink_metadata(root)?.file_type().is_symlink() {
                return Err("cache advice requires absolute, real fixture roots".into());
            }
            let path = root.join(fp.shard_path(shard));
            let file = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)?;
            if !file.metadata()?.is_file() {
                return Err("replay shard is not a regular file".into());
            }
            let result =
                unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
            if result != 0 {
                return Err(std::io::Error::from_raw_os_error(result).into());
            }
            advised.push(path);
        }
    }
    let phase = Measure::start(pids)?;
    let mut sink = HashSink::new(payload.total_bytes);
    let (response, bytes) = client.download(key.into(), 0, None, &mut sink).await?;
    if bytes != payload.total_bytes
        || sink.bytes != payload.total_bytes
        || hex::encode(sink.hash.finalize()) != payload.sha256
        || response.result.as_ref().is_none_or(|info| {
            info["integrity_root"] != before["integrity_root"]
                || info["total_bytes"] != before["total_bytes"]
                || info["data_shards"] != before["data_shards"]
                || info["finalized_shards"] != before["finalized_shards"]
        })
    {
        return Err("immutable replay length, checksum or root changed".into());
    }
    let measured = phase.finish(payload.total_bytes, 1).await?;
    write_json(
        &output.join("READ-REPLAY.json"),
        &json!({"passed":true,"identity_journal":config.identity,"object_key":key,
            "info":before,"sha256_verified":true,"source_sha256":payload.sha256,
            "download":measured,"advised_shards":advised,
            "cache_policy":"no global cache drop; per-file advice does not evict device/controller caches",
            "upload_requests":0,"finalize_requests":0,"delete_requests":0}),
    )
}
