use crate::core::disk_manager::ObjectPathManager;
use crate::distribution_core::config::n2_runtime_config;
use crate::distribution_core::model::{
    N2ClusterShardRef, N2DiskShardRef, NodeDescriptor, ObjectIdentity,
};
use crate::distribution_core::nmtd::{LOCAL_CLUSTER_SHARD_METADATA_FILE, write_atomic};
use crate::distribution_core::peer::{
    InternalShardAbortResponse, InternalShardChunkAck, InternalShardDeleteResponse,
    InternalShardSessionResponse, add_internal_peer_headers,
};
use crate::distribution_core::shard_session::{
    CoordinatorShardSession, LocalCoordinatorShardSession,
};
use crate::enums::app_error_enum::AppError;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use reqwest::Body as ReqwestBody;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use simd_erasure_core::{ReedSolomonDecoder, ReedSolomonEncoder};
use std::collections::{HashMap, HashSet};
use std::path::Path as StdPath;
use std::pin::Pin;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use ulid::Ulid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct LocalClusterShardMeta {
    fragment: String,
    version_id: String,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
    cluster_size_bytes: u64,
    disk_shard_size_bytes: u64,
    disk_shards: Vec<N2DiskShardRef>,
}

#[derive(Clone, Debug)]
pub(crate) struct MemoryClusterShardSource {
    pub(crate) cluster_shard_index: usize,
    pub(crate) bytes: Vec<u8>,
    pub(crate) cursor: usize,
}

type RemoteShardByteStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

pub(crate) struct RemoteClusterShardStreamSource {
    cluster_shard_index: usize,
    size_bytes: usize,
    cursor: usize,
    pending: BytesMut,
    stream: RemoteShardByteStream,
}

pub(crate) enum ClusterShardReadSource {
    Memory(MemoryClusterShardSource),
    Remote(RemoteClusterShardStreamSource),
}

#[derive(Clone, Debug, Serialize)]
struct InternalStoreShardRemoteQuery<'a> {
    uid: &'a str,
    object_key: &'a str,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
}

pub(crate) async fn store_local_cluster_shard_from_file(
    source_path: &StdPath,
    object: &ObjectIdentity,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
) -> Result<N2ClusterShardRef, AppError> {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    if blob_roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No blob roots configured".to_string(),
        ));
    }
    let kd_usize = usize::from(kd);
    let md_usize = usize::from(md);
    if kd_usize == 0 {
        return Err(AppError::BadRequest("kd must be >= 1".to_string()));
    }
    if blob_roots.len() < kd_usize {
        return Err(AppError::InternalServerError(format!(
            "insufficient blob roots for kd={} (have {})",
            kd_usize,
            blob_roots.len()
        )));
    }

    let nd = kd_usize.saturating_add(md_usize);
    let mut source_file = fs::File::open(source_path)
        .await
        .map_err(|e| AppError::Io(e, format!("open source shard {:?}", source_path)))?;
    let source_size = source_file
        .metadata()
        .await
        .map_err(|e| AppError::Io(e, format!("stat source shard {:?}", source_path)))?
        .len();

    let stripe_data_width = stripe_block_size as u64 * kd_usize as u64;
    let stripe_count = if source_size == 0 {
        0
    } else {
        (source_size + stripe_data_width - 1) / stripe_data_width
    };
    let disk_shard_size_bytes = stripe_count * stripe_block_size as u64;

    let mut writers = Vec::with_capacity(nd);
    let mut disk_shards = Vec::with_capacity(nd);
    for disk_shard_index in 0..nd {
        let disk_idx = (cluster_shard_index + disk_shard_index) % blob_roots.len();
        let cshard_dir = blob_roots[disk_idx]
            .join(object.fragment.as_str())
            .join("n2")
            .join("shards")
            .join(object.version_id.as_str())
            .join(format!("cshard_{}", cluster_shard_index));
        fs::create_dir_all(&cshard_dir)
            .await
            .map_err(|e| AppError::Io(e, format!("mkdir {:?}", cshard_dir)))?;

        let shard_path = cshard_dir.join(format!("disk_shard_{}.bin", disk_shard_index));
        let mut file = fs::File::create(&shard_path)
            .await
            .map_err(|e| AppError::Io(e, format!("create {:?}", shard_path)))?;
        if disk_shard_size_bytes > 0 {
            file.set_len(disk_shard_size_bytes)
                .await
                .map_err(|e| AppError::Io(e, format!("prealloc {:?}", shard_path)))?;
            file.seek(SeekFrom::Start(0))
                .await
                .map_err(|e| AppError::Io(e, format!("seek {:?}", shard_path)))?;
        }
        writers.push(file);

        disk_shards.push(N2DiskShardRef {
            disk_shard_index,
            absolute_path: shard_path.to_string_lossy().to_string(),
            size_bytes: disk_shard_size_bytes,
            checksum: String::new(),
            is_parity: disk_shard_index >= kd_usize,
        });
    }

    let mut cluster_hasher = Sha256::new();
    let mut shard_hashers: Vec<Sha256> = (0..nd).map(|_| Sha256::new()).collect();

    for _ in 0..stripe_count {
        let mut stripe = vec![0u8; stripe_data_width as usize];
        let mut read_total = 0usize;
        while read_total < stripe.len() {
            let n = source_file
                .read(&mut stripe[read_total..])
                .await
                .map_err(|e| AppError::Io(e, "read source shard".to_string()))?;
            if n == 0 {
                break;
            }
            read_total += n;
        }
        if read_total > 0 {
            cluster_hasher.update(&stripe[..read_total]);
        }

        if md_usize > 0 {
            let mut encoder = ReedSolomonEncoder::new(kd_usize, md_usize, stripe_block_size)
                .map_err(|e| AppError::InternalServerError(format!("local RS init: {}", e)))?;
            for i in 0..kd_usize {
                let start = i * stripe_block_size;
                let end = start + stripe_block_size;
                encoder
                    .add_original_shard(&stripe[start..end])
                    .map_err(|e| AppError::InternalServerError(format!("local RS add: {}", e)))?;
            }
            let encoded = encoder
                .encode()
                .map_err(|e| AppError::InternalServerError(format!("local RS encode: {}", e)))?;

            for i in 0..kd_usize {
                let start = i * stripe_block_size;
                let end = start + stripe_block_size;
                let slice = &stripe[start..end];
                shard_hashers[i].update(slice);
                writers[i]
                    .write_all(slice)
                    .await
                    .map_err(|e| AppError::Io(e, "write local data disk shard".to_string()))?;
            }
            for i in 0..md_usize {
                let parity = encoded.recovery(i).ok_or_else(|| {
                    AppError::InternalServerError("local missing parity shard".to_string())
                })?;
                shard_hashers[kd_usize + i].update(parity);
                writers[kd_usize + i]
                    .write_all(parity)
                    .await
                    .map_err(|e| AppError::Io(e, "write local parity disk shard".to_string()))?;
            }
        } else {
            for i in 0..kd_usize {
                let start = i * stripe_block_size;
                let end = start + stripe_block_size;
                let slice = &stripe[start..end];
                shard_hashers[i].update(slice);
                writers[i]
                    .write_all(slice)
                    .await
                    .map_err(|e| AppError::Io(e, "write local disk shard".to_string()))?;
            }
        }
    }

    for w in &mut writers {
        w.flush()
            .await
            .map_err(|e| AppError::Io(e, "flush local disk shard".to_string()))?;
    }

    for (idx, hash) in shard_hashers.iter_mut().enumerate() {
        disk_shards[idx].checksum = hex::encode(hash.clone().finalize());
    }

    let local_meta = LocalClusterShardMeta {
        fragment: object.fragment.clone(),
        version_id: object.version_id.clone(),
        cluster_shard_index,
        kd,
        md,
        stripe_block_size,
        cluster_size_bytes: source_size,
        disk_shard_size_bytes,
        disk_shards: disk_shards.clone(),
    };
    let local_meta_bytes = rmp_serde::to_vec(&local_meta).map_err(|e| {
        AppError::InternalServerError(format!("encode local cshard meta failed: {}", e))
    })?;
    let mut seen_dirs = HashMap::<String, bool>::new();
    for shard in &disk_shards {
        let parent = StdPath::new(&shard.absolute_path).parent().ok_or_else(|| {
            AppError::InternalServerError("disk shard path missing parent".to_string())
        })?;
        let parent_key = parent.to_string_lossy().to_string();
        if seen_dirs.contains_key(&parent_key) {
            continue;
        }
        seen_dirs.insert(parent_key, true);
        let meta_path = parent.join(LOCAL_CLUSTER_SHARD_METADATA_FILE);
        write_atomic(&meta_path, &local_meta_bytes).await?;
    }

    let cfg = n2_runtime_config();
    Ok(N2ClusterShardRef {
        cluster_shard_index,
        node_id: cfg.node_id.clone(),
        endpoint: cfg.node_endpoint.clone(),
        size_bytes: source_size,
        checksum: hex::encode(cluster_hasher.finalize()),
        disk_shards,
    })
}

pub(crate) async fn store_local_cluster_shard_from_bytes(
    bytes: &[u8],
    object: &ObjectIdentity,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
) -> Result<N2ClusterShardRef, AppError> {
    let tmp_root = std::env::temp_dir().join(format!("n2-local-store-{}", Ulid::new()));
    fs::create_dir_all(&tmp_root)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {:?}", tmp_root)))?;
    let tmp_path = tmp_root.join(format!("cluster_shard_{}.bin", cluster_shard_index));
    fs::write(&tmp_path, bytes)
        .await
        .map_err(|e| AppError::Io(e, format!("write {:?}", tmp_path)))?;
    let stored = store_local_cluster_shard_from_file(
        tmp_path.as_path(),
        object,
        cluster_shard_index,
        kd,
        md,
        stripe_block_size,
    )
    .await;
    let _ = fs::remove_dir_all(&tmp_root).await;
    stored
}

pub(crate) async fn begin_local_coordinator_shard_session(
    target: NodeDescriptor,
    cluster_shard_index: usize,
) -> Result<CoordinatorShardSession, AppError> {
    let temp_root = std::env::temp_dir().join(format!(
        "n2-coordinator-local-{}-{}",
        cluster_shard_index,
        Ulid::new()
    ));
    fs::create_dir_all(&temp_root)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir {:?}", temp_root)))?;
    let temp_file_path = temp_root.join("cluster_shard.bin");
    fs::File::create(&temp_file_path)
        .await
        .map_err(|e| AppError::Io(e, format!("create {:?}", temp_file_path)))?;

    Ok(CoordinatorShardSession::Local(
        LocalCoordinatorShardSession {
            target,
            cluster_shard_index,
            temp_root,
            temp_file_path,
            checksum: Sha256::new(),
            size_bytes: 0,
            failed: None,
        },
    ))
}

pub(crate) async fn begin_remote_cluster_shard_session(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    object: &ObjectIdentity,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
) -> Result<String, AppError> {
    let base = target.endpoint.trim_end_matches('/');
    let url = format!(
        "{}/v4/n2/internal/session/begin/{}/{}/{}",
        base, object.fragment, object.version_id, cluster_shard_index
    );
    let query = InternalStoreShardRemoteQuery {
        uid: object.uid.as_str(),
        object_key: object.object_key.as_str(),
        kd,
        md,
        stripe_block_size,
    };
    let response = add_internal_peer_headers(client.post(url).query(&query))
        .await?
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote begin shard session failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote begin shard session status={} body={}",
            status, text
        )));
    }
    let payload = response
        .json::<InternalShardSessionResponse>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("decode shard session begin response: {}", e))
        })?;
    Ok(payload.session_id)
}

pub(crate) async fn append_remote_cluster_shard_chunk(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    session_id: &str,
    bytes: Vec<u8>,
) -> Result<(), AppError> {
    if bytes.is_empty() {
        return Ok(());
    }
    let base = target.endpoint.trim_end_matches('/');
    let url = format!("{}/v4/n2/internal/session/chunk/{}", base, session_id);
    let response = add_internal_peer_headers(client.put(url).body(bytes))
        .await?
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote shard chunk failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote shard chunk status={} body={}",
            status, text
        )));
    }
    let _ = response
        .json::<InternalShardChunkAck>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("decode shard chunk ack failed: {}", e))
        })?;
    Ok(())
}

pub(crate) async fn commit_remote_cluster_shard_session(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    session_id: &str,
) -> Result<N2ClusterShardRef, AppError> {
    let base = target.endpoint.trim_end_matches('/');
    let url = format!("{}/v4/n2/internal/session/commit/{}", base, session_id);
    let response = add_internal_peer_headers(client.post(url))
        .await?
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote shard commit failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote shard commit status={} body={}",
            status, text
        )));
    }
    response.json::<N2ClusterShardRef>().await.map_err(|e| {
        AppError::InternalServerError(format!("decode remote shard commit response failed: {}", e))
    })
}

pub(crate) async fn abort_remote_cluster_shard_session(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    session_id: &str,
) -> Result<(), AppError> {
    let base = target.endpoint.trim_end_matches('/');
    let url = format!("{}/v4/n2/internal/session/abort/{}", base, session_id);
    let response = add_internal_peer_headers(client.post(url))
        .await?
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote shard abort failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote shard abort status={} body={}",
            status, text
        )));
    }
    let _ = response
        .json::<InternalShardAbortResponse>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!(
                "decode remote shard abort response failed: {}",
                e
            ))
        })?;
    Ok(())
}

pub(crate) async fn append_coordinator_shard_data(
    client: &reqwest::Client,
    session: &mut CoordinatorShardSession,
    bytes: &[u8],
    remote_flush_threshold: usize,
) -> Result<(), AppError> {
    if bytes.is_empty() {
        return Ok(());
    }
    match session {
        CoordinatorShardSession::Local(local) => {
            if local.failed.is_some() {
                return Ok(());
            }
            let mut file = OpenOptions::new()
                .append(true)
                .open(&local.temp_file_path)
                .await
                .map_err(|e| AppError::Io(e, format!("open {:?}", local.temp_file_path)))?;
            file.write_all(bytes)
                .await
                .map_err(|e| AppError::Io(e, "append local coordinator shard".to_string()))?;
            local.checksum.update(bytes);
            local.size_bytes = local.size_bytes.saturating_add(bytes.len() as u64);
        }
        CoordinatorShardSession::Remote(remote) => {
            if remote.failed.is_some() {
                return Ok(());
            }
            remote.pending.extend_from_slice(bytes);
            remote.checksum.update(bytes);
            remote.size_bytes = remote.size_bytes.saturating_add(bytes.len() as u64);
            if remote.pending.len() >= remote_flush_threshold {
                let chunk = std::mem::take(&mut remote.pending);
                append_remote_cluster_shard_chunk(
                    client,
                    &remote.target,
                    &remote.session_id,
                    chunk,
                )
                .await?;
            }
        }
    }
    Ok(())
}

pub(crate) async fn flush_coordinator_shard_data(
    client: &reqwest::Client,
    session: &mut CoordinatorShardSession,
) -> Result<(), AppError> {
    match session {
        CoordinatorShardSession::Local(_) => Ok(()),
        CoordinatorShardSession::Remote(remote) => {
            if remote.pending.is_empty() {
                return Ok(());
            }
            let chunk = std::mem::take(&mut remote.pending);
            append_remote_cluster_shard_chunk(client, &remote.target, &remote.session_id, chunk)
                .await
        }
    }
}

pub(crate) async fn abort_coordinator_shard_session(
    client: &reqwest::Client,
    session: &mut CoordinatorShardSession,
) -> Result<(), AppError> {
    match session {
        CoordinatorShardSession::Local(local) => {
            let _ = fs::remove_dir_all(&local.temp_root).await;
            Ok(())
        }
        CoordinatorShardSession::Remote(remote) => {
            let _ = remote.pending.clear();
            abort_remote_cluster_shard_session(client, &remote.target, remote.session_id.as_str())
                .await
        }
    }
}

pub(crate) async fn commit_coordinator_shard_session(
    client: &reqwest::Client,
    session: &mut CoordinatorShardSession,
    object: &ObjectIdentity,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
) -> Result<N2ClusterShardRef, AppError> {
    match session {
        CoordinatorShardSession::Local(local) => {
            let stored = store_local_cluster_shard_from_file(
                local.temp_file_path.as_path(),
                object,
                local.cluster_shard_index,
                kd,
                md,
                stripe_block_size,
            )
            .await;
            let _ = fs::remove_dir_all(&local.temp_root).await;
            stored
        }
        CoordinatorShardSession::Remote(remote) => {
            if !remote.pending.is_empty() {
                let chunk = std::mem::take(&mut remote.pending);
                append_remote_cluster_shard_chunk(
                    client,
                    &remote.target,
                    &remote.session_id,
                    chunk,
                )
                .await?;
            }
            commit_remote_cluster_shard_session(client, &remote.target, remote.session_id.as_str())
                .await
        }
    }
}

pub(crate) async fn load_local_cluster_shard_meta(
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> Result<LocalClusterShardMeta, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_blob_roots();
    for root in roots {
        let meta_path = root
            .join(fragment)
            .join("n2")
            .join("shards")
            .join(version_id)
            .join(format!("cshard_{}", cluster_shard_index))
            .join(LOCAL_CLUSTER_SHARD_METADATA_FILE);
        let exists = fs::try_exists(&meta_path)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", meta_path)))?;
        if !exists {
            continue;
        }
        let bytes = fs::read(&meta_path)
            .await
            .map_err(|e| AppError::Io(e, format!("read {:?}", meta_path)))?;
        let meta = rmp_serde::from_slice::<LocalClusterShardMeta>(&bytes).map_err(|e| {
            AppError::InternalServerError(format!("decode local cshard meta failed: {}", e))
        })?;
        return Ok(meta);
    }

    Err(AppError::NotFound)
}

pub(crate) async fn read_local_cluster_shard(
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> Result<Vec<u8>, AppError> {
    let meta = load_local_cluster_shard_meta(fragment, version_id, cluster_shard_index).await?;
    let kd = usize::from(meta.kd);
    let md = usize::from(meta.md);
    if kd == 0 {
        return Err(AppError::InternalServerError(
            "local cshard meta has invalid kd".to_string(),
        ));
    }
    if meta.cluster_size_bytes == 0 {
        return Ok(Vec::new());
    }
    let stripe_count = if meta.disk_shard_size_bytes == 0 {
        0
    } else {
        (meta.disk_shard_size_bytes / meta.stripe_block_size as u64) as usize
    };

    let mut files = HashMap::<usize, fs::File>::new();
    for shard in &meta.disk_shards {
        if let Ok(file) = fs::File::open(shard.absolute_path.as_str()).await {
            files.insert(shard.disk_shard_index, file);
        }
    }

    let mut out = Vec::with_capacity((stripe_count * kd * meta.stripe_block_size).max(1024));
    if md == 0 {
        for stripe_idx in 0..stripe_count {
            let offset = stripe_idx as u64 * meta.stripe_block_size as u64;
            for disk_shard_index in 0..kd {
                let file = files.get_mut(&disk_shard_index).ok_or_else(|| {
                    AppError::InternalServerError(format!(
                        "missing required data disk shard {} for cshard read",
                        disk_shard_index
                    ))
                })?;
                file.seek(SeekFrom::Start(offset))
                    .await
                    .map_err(|e| AppError::Io(e, "seek local data disk shard".to_string()))?;
                let mut block = vec![0u8; meta.stripe_block_size];
                file.read_exact(&mut block)
                    .await
                    .map_err(|e| AppError::Io(e, "read local data disk shard".to_string()))?;
                out.extend_from_slice(&block);
            }
        }
        out.truncate(meta.cluster_size_bytes as usize);
        return Ok(out);
    }

    for stripe_idx in 0..stripe_count {
        let offset = stripe_idx as u64 * meta.stripe_block_size as u64;
        let mut decoder = ReedSolomonDecoder::new(kd, md, meta.stripe_block_size)
            .map_err(|e| AppError::InternalServerError(format!("local RS decode init: {}", e)))?;

        let mut originals: Vec<Option<Vec<u8>>> = vec![None; kd];
        let mut added = 0usize;

        for shard in &meta.disk_shards {
            let Some(file) = files.get_mut(&shard.disk_shard_index) else {
                continue;
            };
            if file.seek(SeekFrom::Start(offset)).await.is_err() {
                continue;
            }
            let mut block = vec![0u8; meta.stripe_block_size];
            if file.read_exact(&mut block).await.is_err() {
                continue;
            }

            let add_res = if shard.disk_shard_index < kd {
                originals[shard.disk_shard_index] = Some(block.clone());
                decoder.add_original_shard(shard.disk_shard_index, &block)
            } else {
                decoder.add_recovery_shard(shard.disk_shard_index - kd, &block)
            };
            if add_res.is_ok() {
                added = added.saturating_add(1);
            }
        }

        if added < kd {
            return Err(AppError::InternalServerError(format!(
                "insufficient disk shards to rebuild cshard (need {}, have {})",
                kd, added
            )));
        }

        let decoded = decoder
            .decode()
            .map_err(|e| AppError::InternalServerError(format!("local RS decode: {}", e)))?;
        for (idx, data) in decoded.restored_original_iter() {
            if idx < kd {
                originals[idx] = Some(data.to_vec());
            }
        }

        for maybe in originals {
            let block = maybe.ok_or_else(|| {
                AppError::InternalServerError("missing restored original block".to_string())
            })?;
            out.extend_from_slice(&block);
        }
    }

    out.truncate(meta.cluster_size_bytes as usize);
    Ok(out)
}

pub(crate) async fn delete_local_cluster_shard(
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> Result<usize, AppError> {
    let path_manager = ObjectPathManager::new();
    let blob_roots = path_manager.get_blob_roots();
    let mut deleted_paths = 0usize;
    for root in blob_roots {
        let cshard_dir = root
            .join(fragment)
            .join("n2")
            .join("shards")
            .join(version_id)
            .join(format!("cshard_{}", cluster_shard_index));
        let exists = fs::try_exists(&cshard_dir)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", cshard_dir)))?;
        if !exists {
            continue;
        }
        fs::remove_dir_all(&cshard_dir)
            .await
            .map_err(|e| AppError::Io(e, format!("remove {:?}", cshard_dir)))?;
        deleted_paths = deleted_paths.saturating_add(1);

        if let Some(version_dir) = cshard_dir.parent() {
            if let Ok(mut entries) = fs::read_dir(version_dir).await {
                let mut has_children = false;
                while let Ok(Some(_)) = entries.next_entry().await {
                    has_children = true;
                    break;
                }
                if !has_children {
                    let _ = fs::remove_dir(version_dir).await;
                }
            }
        }
    }
    Ok(deleted_paths)
}

pub(crate) async fn store_remote_cluster_shard_bytes(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    object: &ObjectIdentity,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
    bytes: Vec<u8>,
) -> Result<N2ClusterShardRef, AppError> {
    store_remote_cluster_shard_with_body(
        client,
        target,
        object,
        cluster_shard_index,
        kd,
        md,
        stripe_block_size,
        ReqwestBody::from(bytes),
    )
    .await
}

pub(crate) async fn store_remote_cluster_shard_with_body(
    client: &reqwest::Client,
    target: &NodeDescriptor,
    object: &ObjectIdentity,
    cluster_shard_index: usize,
    kd: u8,
    md: u8,
    stripe_block_size: usize,
    body: ReqwestBody,
) -> Result<N2ClusterShardRef, AppError> {
    let base = target.endpoint.trim_end_matches('/');
    let url = format!(
        "{}/v4/n2/internal/store/{}/{}/{}",
        base, object.fragment, object.version_id, cluster_shard_index
    );

    let query = InternalStoreShardRemoteQuery {
        uid: object.uid.as_str(),
        object_key: object.object_key.as_str(),
        kd,
        md,
        stripe_block_size,
    };

    let response = add_internal_peer_headers(client.post(url).query(&query).body(body))
        .await?
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote store shard request failed: {}", e)))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote store shard status={} body={}",
            status, text
        )));
    }

    response.json::<N2ClusterShardRef>().await.map_err(|e| {
        AppError::InternalServerError(format!("decode remote store shard response failed: {}", e))
    })
}

pub(crate) async fn fetch_remote_cluster_shard(
    client: &reqwest::Client,
    endpoint: &str,
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> Result<Vec<u8>, AppError> {
    let base = endpoint.trim_end_matches('/');
    let url = format!(
        "{}/v4/n2/internal/read/{}/{}/{}",
        base, fragment, version_id, cluster_shard_index
    );
    let resp = add_internal_peer_headers(client.get(url))
        .await?
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote read shard request failed: {}", e)))?;
    if !resp.status().is_success() {
        return Err(AppError::InternalServerError(format!(
            "remote read shard returned status {}",
            resp.status()
        )));
    }
    resp.bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| AppError::InternalServerError(format!("read remote shard body failed: {}", e)))
}

pub(crate) async fn open_remote_cluster_shard_stream(
    client: &reqwest::Client,
    endpoint: &str,
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
    expected_size_bytes: usize,
) -> Result<ClusterShardReadSource, AppError> {
    let base = endpoint.trim_end_matches('/');
    let url = format!(
        "{}/v4/n2/internal/read/{}/{}/{}",
        base, fragment, version_id, cluster_shard_index
    );
    let response = add_internal_peer_headers(client.get(url))
        .await?
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote read shard request failed: {}", e)))?;
    if !response.status().is_success() {
        return Err(AppError::InternalServerError(format!(
            "remote read shard returned status {}",
            response.status()
        )));
    }

    Ok(ClusterShardReadSource::Remote(
        RemoteClusterShardStreamSource {
            cluster_shard_index,
            size_bytes: expected_size_bytes,
            cursor: 0,
            pending: BytesMut::new(),
            stream: Box::pin(response.bytes_stream()),
        },
    ))
}

pub(crate) async fn delete_remote_cluster_shard(
    client: &reqwest::Client,
    endpoint: &str,
    fragment: &str,
    version_id: &str,
    cluster_shard_index: usize,
) -> Result<usize, AppError> {
    let base = endpoint.trim_end_matches('/');
    let url = format!(
        "{}/v4/n2/internal/delete/{}/{}/{}",
        base, fragment, version_id, cluster_shard_index
    );
    let response = add_internal_peer_headers(client.delete(url))
        .await?
        .send()
        .await
        .map_err(|e| AppError::Timeout(format!("n2 remote delete shard request failed: {}", e)))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote delete shard status={} body={}",
            status, body
        )));
    }
    let parsed = response
        .json::<InternalShardDeleteResponse>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!(
                "decode remote delete shard response failed: {}",
                e
            ))
        })?;
    Ok(parsed.deleted_paths)
}

fn reconstructed_stream_error(message: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, message)
}

pub(crate) async fn send_reconstructed_stream_err(
    tx: &mpsc::Sender<Result<Bytes, std::io::Error>>,
    err: AppError,
) {
    let _ = tx
        .send(Err(reconstructed_stream_error(err.to_string())))
        .await;
}

impl ClusterShardReadSource {
    fn cluster_shard_index(&self) -> usize {
        match self {
            ClusterShardReadSource::Memory(source) => source.cluster_shard_index,
            ClusterShardReadSource::Remote(source) => source.cluster_shard_index,
        }
    }

    fn size_bytes(&self) -> usize {
        match self {
            ClusterShardReadSource::Memory(source) => source.bytes.len(),
            ClusterShardReadSource::Remote(source) => source.size_bytes,
        }
    }

    async fn read_block(&mut self, stripe_block_size: usize) -> Result<Vec<u8>, AppError> {
        match self {
            ClusterShardReadSource::Memory(source) => {
                let mut block = vec![0u8; stripe_block_size];
                if source.cursor >= source.bytes.len() {
                    return Ok(block);
                }
                let copy_len = (source.bytes.len() - source.cursor).min(stripe_block_size);
                block[..copy_len]
                    .copy_from_slice(&source.bytes[source.cursor..source.cursor + copy_len]);
                source.cursor = source.cursor.saturating_add(copy_len);
                Ok(block)
            }
            ClusterShardReadSource::Remote(source) => {
                let mut block = vec![0u8; stripe_block_size];
                if source.cursor >= source.size_bytes {
                    return Ok(block);
                }
                let copy_len = (source.size_bytes - source.cursor).min(stripe_block_size);
                while source.pending.len() < copy_len {
                    let Some(next_chunk) = source.stream.next().await else {
                        return Err(AppError::InternalServerError(
                            "remote cluster shard stream ended early".to_string(),
                        ));
                    };
                    let chunk = next_chunk.map_err(|e| {
                        AppError::InternalServerError(format!(
                            "read remote cluster shard stream failed: {}",
                            e
                        ))
                    })?;
                    if chunk.is_empty() {
                        continue;
                    }
                    source.pending.extend_from_slice(chunk.as_ref());
                }
                let drained = source.pending.split_to(copy_len);
                block[..copy_len].copy_from_slice(drained.as_ref());
                source.cursor = source.cursor.saturating_add(copy_len);
                Ok(block)
            }
        }
    }
}

pub(crate) fn reconstruct_object_stream_from_cluster_sources(
    kc: usize,
    mc: usize,
    stripe_block_size: usize,
    total_size_bytes: u64,
    mut sources: Vec<ClusterShardReadSource>,
    range: Option<(u64, u64)>,
) -> ReceiverStream<Result<Bytes, std::io::Error>> {
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(8);
    tokio::spawn(async move {
        let run = async {
            if kc == 0 {
                return Err(AppError::InternalServerError("invalid kc=0".to_string()));
            }
            if sources.len() < kc {
                return Err(AppError::InternalServerError(format!(
                    "insufficient cluster shards for reconstruction (need {}, have {})",
                    kc,
                    sources.len()
                )));
            }
            sources.sort_by_key(|s| s.cluster_shard_index());
            let mut seen_indices = HashSet::new();
            sources.retain(|s| seen_indices.insert(s.cluster_shard_index()));

            let max_shard_size = sources
                .iter()
                .map(ClusterShardReadSource::size_bytes)
                .max()
                .unwrap_or(0);
            if max_shard_size == 0 || total_size_bytes == 0 {
                return Ok(());
            }
            let stripe_count = max_shard_size.div_ceil(stripe_block_size);
            let (target_start, target_end) =
                range.unwrap_or((0, total_size_bytes.saturating_sub(1)));
            let mut object_cursor = 0u64;

            for stripe_idx in 0..stripe_count {
                let stripe_result = if mc == 0 {
                    let mut data_blocks = HashMap::<usize, Vec<u8>>::new();
                    for source in &mut sources {
                        let cluster_idx = source.cluster_shard_index();
                        if cluster_idx >= kc || data_blocks.contains_key(&cluster_idx) {
                            continue;
                        }
                        let block = source.read_block(stripe_block_size).await?;
                        data_blocks.insert(cluster_idx, block);
                    }
                    if data_blocks.len() < kc {
                        Err(AppError::InternalServerError(format!(
                            "insufficient data shards without parity (need {}, have {})",
                            kc,
                            data_blocks.len()
                        )))
                    } else {
                        let mut stripe = Vec::with_capacity(kc * stripe_block_size);
                        for data_idx in 0..kc {
                            let block = data_blocks.remove(&data_idx).ok_or_else(|| {
                                AppError::InternalServerError(format!(
                                    "missing data shard {} for no-parity reconstruction",
                                    data_idx
                                ))
                            })?;
                            stripe.extend_from_slice(block.as_slice());
                        }
                        Ok(stripe)
                    }
                } else {
                    let mut decoder =
                        ReedSolomonDecoder::new(kc, mc, stripe_block_size).map_err(|e| {
                            AppError::InternalServerError(format!(
                                "reconstruct decoder init failed: {}",
                                e
                            ))
                        })?;
                    let mut originals: Vec<Option<Vec<u8>>> = vec![None; kc];
                    let mut added = 0usize;

                    for source in &mut sources {
                        let cluster_idx = source.cluster_shard_index();
                        let block = source.read_block(stripe_block_size).await?;
                        let add_res = if cluster_idx < kc {
                            originals[cluster_idx] = Some(block.clone());
                            decoder.add_original_shard(cluster_idx, &block)
                        } else {
                            decoder.add_recovery_shard(cluster_idx - kc, &block)
                        };
                        if add_res.is_ok() {
                            added = added.saturating_add(1);
                        }
                    }

                    if added < kc {
                        Err(AppError::InternalServerError(format!(
                            "insufficient shards for stripe {} (need {}, have {})",
                            stripe_idx, kc, added
                        )))
                    } else {
                        let decoded = decoder.decode().map_err(|e| {
                            AppError::InternalServerError(format!(
                                "reconstruct decode failed: {}",
                                e
                            ))
                        })?;
                        for (idx, data) in decoded.restored_original_iter() {
                            if idx < kc {
                                originals[idx] = Some(data.to_vec());
                            }
                        }
                        let mut stripe = Vec::with_capacity(kc * stripe_block_size);
                        for maybe in originals {
                            let block = maybe.ok_or_else(|| {
                                AppError::InternalServerError(
                                    "missing restored original block".to_string(),
                                )
                            })?;
                            stripe.extend_from_slice(block.as_slice());
                        }
                        Ok(stripe)
                    }
                };

                let stripe = stripe_result?;
                let mut stripe_len = stripe.len() as u64;
                if object_cursor + stripe_len > total_size_bytes {
                    stripe_len = total_size_bytes.saturating_sub(object_cursor);
                }
                if stripe_len == 0 {
                    break;
                }

                let stripe_start = object_cursor;
                let stripe_end = object_cursor + stripe_len - 1;
                object_cursor = object_cursor.saturating_add(stripe_len);

                if stripe_end < target_start || stripe_start > target_end {
                    if stripe_start > target_end {
                        break;
                    }
                    continue;
                }

                let emit_start = target_start.max(stripe_start) - stripe_start;
                let emit_end_exclusive = (target_end.min(stripe_end) - stripe_start + 1) as usize;
                let emit_start_usize = emit_start as usize;
                let payload = Bytes::copy_from_slice(&stripe[emit_start_usize..emit_end_exclusive]);
                if tx.send(Ok(payload)).await.is_err() {
                    return Ok(());
                }

                if stripe_end >= target_end {
                    break;
                }
            }
            Ok::<(), AppError>(())
        }
        .await;

        if let Err(err) = run {
            send_reconstructed_stream_err(&tx, err).await;
        }
    });
    ReceiverStream::new(rx)
}
