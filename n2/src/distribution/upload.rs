use crate::core::constants::{PARITY_SUFFIX, SHARD_SUFFIX};
use crate::core::disk_manager::{
    InitiateUploadParams, InitiateUploadResponse, ObjectMetadata, ObjectPathManager, PartMetadata,
    ShardInfo, UploadPartQuery,
};
use crate::core::ec::ErasureStreamWriter;
use crate::core::index::{self, IndexReplicaWriteMode};
use crate::core::upload::{FinalizeUploadResult, UploadSessionManifest};
use crate::distribution::membership::{active_cluster_runtime, live_active_write_nodes};
use crate::distribution::peer::{PeerShardTarget, RemoteShardWriter, encode_peer_shard_locator};
use crate::distribution::placement::{ClusterPlacementPlan, build_placement_plan};
use crate::distribution::resolve_layout;
use crate::enums::app_error_enum::AppError;
use crate::structs::auth_structs::Claims;
use bytes::Bytes;
use chrono::Utc;
use futures_util::Stream;
use futures_util::StreamExt;
use serde::Serialize;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use ulid::Ulid;

const MIN_DYNAMIC_STRIPE_SIZE: usize = 4 * 1024;
const TINY_OBJECT_STRIPE_4K_LIMIT: u64 = 64 * 1024;
const SMALL_OBJECT_STRIPE_16K_LIMIT: u64 = 1024 * 1024;
pub const CLUSTER_ONE_SHOT_MAX_BYTES: u64 = 16 * 1024 * 1024;

fn validate_cluster_one_shot_size(content_length: u64) -> Result<(), AppError> {
    if content_length > CLUSTER_ONE_SHOT_MAX_BYTES {
        return Err(AppError::PayloadTooLarge(format!(
            "Object too large for one-shot upload; use multipart for objects over {} bytes",
            CLUSTER_ONE_SHOT_MAX_BYTES
        )));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct DistributedPartWrite {
    pub part: PartMetadata,
    pub membership_epoch: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
}

enum ClusterShardWriter {
    Local(LocalShardWriter),
    Remote(RemoteShardWriter),
}

struct LocalShardWriter {
    file: fs::File,
    tmp_path: PathBuf,
    stable_path: PathBuf,
    file_name: String,
    shard_index: usize,
    is_parity: bool,
    expected_len: u64,
    locator: PeerShardTarget,
}

impl AsyncWrite for LocalShardWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.file).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.file).poll_shutdown(cx)
    }
}

impl LocalShardWriter {
    async fn finish(mut self, checksum: String) -> Result<ShardInfo, AppError> {
        self.file.flush().await.map_err(|e| {
            AppError::Io(
                e,
                format!("flush cluster shard {}", self.tmp_path.display()),
            )
        })?;
        drop(self.file);
        if fs::try_exists(&self.stable_path).await.unwrap_or(false) {
            let _ = fs::remove_file(&self.stable_path).await;
        }
        fs::rename(&self.tmp_path, &self.stable_path)
            .await
            .map_err(|e| {
                AppError::Io(
                    e,
                    format!(
                        "publish cluster shard {} -> {}",
                        self.tmp_path.display(),
                        self.stable_path.display()
                    ),
                )
            })?;
        Ok(ShardInfo {
            shard_index: self.shard_index,
            file_name: self.file_name,
            absolute_path: encode_peer_shard_locator(&self.locator),
            size_bytes: self.expected_len,
            checksum,
            is_parity: self.is_parity,
        })
    }
}

impl AsyncWrite for ClusterShardWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match &mut *self {
            Self::Local(writer) => Pin::new(writer).poll_write(cx, buf),
            Self::Remote(writer) => Pin::new(writer).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            Self::Local(writer) => Pin::new(writer).poll_flush(cx),
            Self::Remote(writer) => Pin::new(writer).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            Self::Local(writer) => Pin::new(writer).poll_shutdown(cx),
            Self::Remote(writer) => Pin::new(writer).poll_shutdown(cx),
        }
    }
}

impl ClusterShardWriter {
    async fn finish(self, checksum: String) -> Result<ShardInfo, AppError> {
        match self {
            Self::Local(writer) => writer.finish(checksum).await,
            Self::Remote(writer) => {
                let mut info = writer.finish().await?;
                if !info.checksum.eq_ignore_ascii_case(&checksum) {
                    return Err(AppError::BadRequest(format!(
                        "remote shard checksum mismatch for shard {}",
                        info.shard_index
                    )));
                }
                info.checksum = checksum;
                Ok(info)
            }
        }
    }
}

fn configured_block_size() -> usize {
    crate::core::upload::STRIPE_SIZE_ATOMIC
        .get()
        .map(|value| value.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or_else(crate::core::disk_manager::default_stripe_size)
}

fn choose_upload_stripe_size(content_length: u64) -> usize {
    let configured = configured_block_size();
    if configured <= MIN_DYNAMIC_STRIPE_SIZE {
        return configured;
    }
    let candidate = if content_length <= TINY_OBJECT_STRIPE_4K_LIMIT {
        MIN_DYNAMIC_STRIPE_SIZE
    } else if content_length <= SMALL_OBJECT_STRIPE_16K_LIMIT {
        16 * 1024
    } else {
        configured
    };
    candidate.min(configured)
}

fn planned_part_size(total_size: u64, expected_parts: u64, part_size: u64, index: u64) -> u64 {
    if expected_parts <= 1 {
        return total_size;
    }
    let start = index.saturating_mul(part_size);
    if start >= total_size {
        return 0;
    }
    let end = start.saturating_add(part_size).min(total_size);
    end.saturating_sub(start)
}

fn expected_part_size(manifest: &UploadSessionManifest, part_index: u64) -> u64 {
    let part_size = if manifest.part_size_bytes == 0 {
        manifest.total_size_bytes
    } else {
        manifest.part_size_bytes
    };
    planned_part_size(
        manifest.total_size_bytes,
        manifest.expected_parts.max(1),
        part_size,
        part_index,
    )
}

fn stable_name(part_index: u64, shard_index: usize, is_parity: bool) -> String {
    let ext = if is_parity {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    };
    format!("part_{part_index}_shard_{shard_index}{ext}")
}

async fn open_local_writer(target: PeerShardTarget) -> Result<ClusterShardWriter, AppError> {
    let pm = ObjectPathManager::new();
    let roots = pm.get_blob_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No blob storage disks configured".into(),
        ));
    }
    let disk_idx = (target.part_index as usize + target.shard_index) % roots.len();
    let dir = pm.get_shard_dir_on_disk(&roots[disk_idx], &target.fragment, &target.version_id);
    fs::create_dir_all(&dir)
        .await
        .map_err(|e| AppError::Io(e, format!("create cluster shard dir {}", dir.display())))?;
    let file_name = stable_name(target.part_index, target.shard_index, target.is_parity);
    let tmp_path = dir.join(format!("{file_name}.tmp.{}", Ulid::new()));
    let stable_path = dir.join(&file_name);
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .await
        .map_err(|e| AppError::Io(e, format!("create cluster shard {}", tmp_path.display())))?;
    Ok(ClusterShardWriter::Local(LocalShardWriter {
        file,
        tmp_path,
        stable_path,
        file_name,
        shard_index: target.shard_index,
        is_parity: target.is_parity,
        expected_len: target.expected_len,
        locator: target,
    }))
}

async fn open_writers_for_plan(
    plan: &ClusterPlacementPlan,
    part_index: u64,
    final_shard_len: u64,
) -> Result<Vec<Option<ClusterShardWriter>>, AppError> {
    let runtime = active_cluster_runtime()?;
    let mut writers = Vec::with_capacity(plan.placements.len());
    for placement in &plan.placements {
        let is_parity = matches!(
            placement.role,
            crate::distribution::placement::ClusterShardRole::Parity
        );
        let target = PeerShardTarget {
            node_id: placement.node_id.clone(),
            endpoint: placement
                .native_endpoint
                .clone()
                .unwrap_or_else(|| placement.endpoint.clone()),
            fragment: plan.fragment.clone(),
            version_id: plan.version_id.clone(),
            part_index,
            shard_index: placement.shard_index,
            is_parity,
            expected_len: final_shard_len,
        };
        if placement.node_id == runtime.local_node_id {
            writers.push(Some(open_local_writer(target).await?));
        } else {
            writers.push(Some(ClusterShardWriter::Remote(RemoteShardWriter::spawn(
                target,
            ))));
        }
    }
    Ok(writers)
}

pub async fn write_cluster_part_from_stream<S>(
    fragment: &str,
    object_key: &str,
    version_id: &str,
    part_index: u64,
    content_length: u64,
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
    body_stream: S,
    expected_checksum: Option<&str>,
) -> Result<DistributedPartWrite, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>>,
{
    let runtime = active_cluster_runtime()?;
    let active = live_active_write_nodes(&runtime).await;
    let layout =
        resolve_layout(active.len(), &runtime.layout_policy).map_err(AppError::BadRequest)?;
    if layout.data_shards != data_shards || layout.parity_shards != parity_shards {
        return Err(AppError::Conflict(
            "cluster layout changed during upload session; restart the multipart upload".into(),
        ));
    }
    let placement = build_placement_plan(
        fragment,
        object_key,
        version_id,
        runtime.membership.epoch,
        layout,
        &active,
    )?;
    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = content_length.div_ceil(stripe_data_width.max(1));
    let final_shard_len = stripe_count.saturating_mul(block_size as u64);
    let writers = open_writers_for_plan(&placement, part_index, final_shard_len).await?;
    let mut stream_writer = ErasureStreamWriter::new(
        writers,
        data_shards,
        parity_shards,
        final_shard_len as usize,
        expected_checksum.is_some(),
        block_size,
    )?;
    let mut total_bytes_read = 0u64;
    futures_util::pin_mut!(body_stream);
    while let Some(chunk) = body_stream.next().await {
        let chunk = chunk?;
        if chunk.is_empty() {
            continue;
        }
        total_bytes_read = total_bytes_read.saturating_add(chunk.len() as u64);
        if total_bytes_read > content_length {
            return Err(AppError::BadRequest("Body exceeds Content-Length".into()));
        }
        stream_writer.write(&chunk).await?;
    }
    if total_bytes_read != content_length {
        return Err(AppError::BadRequest(format!(
            "Incomplete body: got {} bytes, expected {}",
            total_bytes_read, content_length
        )));
    }
    stream_writer.finalize().await?;
    if let Some(expected) = expected_checksum {
        let calculated = stream_writer.get_total_checksum();
        if calculated
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(expected))
        {
            return Err(AppError::BadRequest("Checksum mismatch".into()));
        }
    }
    let shard_checksums = stream_writer.get_shard_checksums();
    let total_checksum = stream_writer.get_total_checksum();
    let writers = stream_writer.into_writers();
    let mut shards = Vec::with_capacity(writers.len());
    for (idx, writer) in writers.into_iter().enumerate() {
        let writer = writer.ok_or_else(|| {
            AppError::InternalServerError(format!("cluster shard writer {idx} failed"))
        })?;
        shards.push(
            writer
                .finish(shard_checksums.get(idx).cloned().unwrap_or_default())
                .await?,
        );
    }
    shards.sort_by_key(|shard| shard.shard_index);
    Ok(DistributedPartWrite {
        membership_epoch: runtime.membership.epoch,
        data_shards: layout.data_shards as u16,
        parity_shards: layout.parity_shards as u16,
        part: PartMetadata {
            part_index,
            size_bytes: total_bytes_read,
            created_at: Utc::now().timestamp(),
            shards,
            checksum: total_checksum,
        },
    })
}

pub async fn initiate_cluster_upload(
    claims: Claims,
    fragment: String,
    object_key: String,
    params: InitiateUploadParams,
) -> Result<InitiateUploadResponse, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    if params.expected_parts == 0 {
        return Err(AppError::BadRequest("expected_parts must be >= 1".into()));
    }
    let part_size_bytes = if params.expected_parts <= 1 {
        params.part_size_bytes.unwrap_or(params.total_size_bytes)
    } else {
        params
            .part_size_bytes
            .filter(|value| *value > 0)
            .ok_or_else(|| AppError::BadRequest("part_size_bytes is required".into()))?
    };
    let computed_parts = if params.total_size_bytes == 0 || part_size_bytes == 0 {
        1
    } else {
        params.total_size_bytes.div_ceil(part_size_bytes)
    };
    if computed_parts != params.expected_parts.max(1) {
        return Err(AppError::BadRequest(
            "expected_parts does not match total_size_bytes and part_size_bytes".into(),
        ));
    }
    let runtime = active_cluster_runtime()?;
    let active = live_active_write_nodes(&runtime).await;
    let layout =
        resolve_layout(active.len(), &runtime.layout_policy).map_err(AppError::BadRequest)?;
    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let stripe_size = choose_upload_stripe_size(params.total_size_bytes);
    let manifest = UploadSessionManifest {
        fragment: fragment.clone(),
        object_key,
        version_id: version_id.clone(),
        session_id: session_id.clone(),
        total_size_bytes: params.total_size_bytes,
        expected_parts: params.expected_parts.max(1),
        part_size_bytes,
        content_type: params
            .content_type
            .unwrap_or_else(|| "application/octet-stream".into()),
        created_at: Utc::now().timestamp(),
        stripe_size_bytes: stripe_size,
        membership_epoch: runtime.membership.epoch,
        data_shards: layout.data_shards as u16,
        parity_shards: layout.parity_shards as u16,
    };
    index::open_session(&manifest, None).await?;
    Ok(InitiateUploadResponse {
        session_id,
        version_id,
    })
}

pub async fn put_cluster_part<S>(
    claims: Claims,
    fragment: String,
    version_id: String,
    part_index: u64,
    query: UploadPartQuery,
    body_stream: S,
    content_length: u64,
) -> Result<(), AppError>
where
    S: Stream<Item = Result<Bytes, AppError>>,
{
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let state = index::load_session(&fragment, &version_id)
        .await?
        .ok_or_else(|| AppError::BadRequest("Upload session not found".into()))?;
    if state.part_receipts.contains_key(&part_index) || state.parts.contains_key(&part_index) {
        return Ok(());
    }
    if part_index >= state.manifest.expected_parts {
        return Err(AppError::BadRequest("Part index out of range".into()));
    }
    let expected = expected_part_size(&state.manifest, part_index);
    if expected != content_length {
        return Err(AppError::BadRequest(
            "Part size does not match upload session manifest".into(),
        ));
    }
    let part = write_cluster_part_from_stream(
        &fragment,
        &state.manifest.object_key,
        &version_id,
        part_index,
        content_length,
        usize::from(state.manifest.data_shards),
        usize::from(state.manifest.parity_shards),
        state.manifest.stripe_size_bytes,
        body_stream,
        query.checksum.as_deref(),
    )
    .await?;
    index::commit_session_part(
        &fragment,
        &version_id,
        &part.part,
        IndexReplicaWriteMode::BackgroundBestEffort,
    )
    .await?;
    index::commit_session_part_receipt(&fragment, &version_id, &state.manifest, &part.part).await?;
    Ok(())
}

pub async fn upload_cluster_one_shot<S>(
    claims: Claims,
    fragment: String,
    object_key: String,
    content_type: String,
    query: UploadPartQuery,
    body_stream: S,
    content_length: u64,
) -> Result<ObjectMetadata, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>>,
{
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    validate_cluster_one_shot_size(content_length)?;
    let runtime = active_cluster_runtime()?;
    let active = live_active_write_nodes(&runtime).await;
    let layout =
        resolve_layout(active.len(), &runtime.layout_policy).map_err(AppError::BadRequest)?;
    let version_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    let stripe_size = choose_upload_stripe_size(content_length);
    let part = write_cluster_part_from_stream(
        &fragment,
        &object_key,
        &version_id,
        0,
        content_length,
        layout.data_shards,
        layout.parity_shards,
        stripe_size,
        body_stream,
        query.checksum.as_deref(),
    )
    .await?;
    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type,
        total_size_bytes: content_length,
        created_at: Utc::now().timestamp(),
        membership_epoch: part.membership_epoch,
        data_shards: part.data_shards,
        parity_shards: part.parity_shards,
        parts: vec![part.part],
        is_delete_marker: false,
        stripe_size_bytes: stripe_size,
    };
    let _lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    index::publish_object(&object_meta).await?;
    Ok(object_meta)
}

pub async fn finalize_cluster_upload(
    claims: Claims,
    fragment: String,
    version_id: String,
    session_id: String,
    object_key: String,
    params: InitiateUploadParams,
) -> Result<FinalizeUploadResult, AppError> {
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    if claims.fragment != fragment {
        return Err(AppError::Forbidden("Access denied to this fragment".into()));
    }
    let state = index::load_session(&fragment, &version_id)
        .await?
        .ok_or_else(|| AppError::BadRequest("Upload session not found".into()))?;
    if state.manifest.session_id != session_id {
        return Err(AppError::Forbidden("Session id mismatch".into()));
    }
    let mut parts = Vec::new();
    let mut missing = Vec::new();
    for part_index in 0..state.manifest.expected_parts {
        if let Some(part) = state.parts.get(&part_index) {
            parts.push(part.clone());
        } else {
            missing.push(part_index);
        }
    }
    if !missing.is_empty() {
        return Ok(FinalizeUploadResult::Incomplete(
            crate::core::disk_manager::FinalizeIncompleteResponse {
                uploaded_parts: parts.iter().map(|part| part.part_index).collect(),
                missing_parts: missing,
                expected_parts: state.manifest.expected_parts,
            },
        ));
    }
    parts.sort_by_key(|part| part.part_index);
    let total_size_bytes = parts.iter().map(|part| part.size_bytes).sum();
    let object_meta = ObjectMetadata {
        object_key: object_key.clone(),
        fragment: fragment.clone(),
        version_id: version_id.clone(),
        upload_session_id: session_id,
        owner_uid: fragment.clone(),
        content_type: state.manifest.content_type.clone().if_empty(
            params
                .content_type
                .unwrap_or_else(|| "application/octet-stream".into()),
        ),
        total_size_bytes,
        created_at: state.manifest.created_at,
        membership_epoch: state.manifest.membership_epoch,
        data_shards: state.manifest.data_shards,
        parity_shards: state.manifest.parity_shards,
        parts,
        is_delete_marker: false,
        stripe_size_bytes: state.manifest.stripe_size_bytes,
    };
    let _lock = index::acquire_object_operation_lock(&fragment, &object_key).await?;
    index::publish_object(&object_meta).await?;
    let _ = index::remove_session(&fragment, &version_id, "finalized").await;
    Ok(FinalizeUploadResult::Complete(object_meta))
}

trait EmptyFallback {
    fn if_empty(self, fallback: String) -> String;
}

impl EmptyFallback for String {
    fn if_empty(self, fallback: String) -> String {
        if self.trim().is_empty() {
            fallback
        } else {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(
        total_size_bytes: u64,
        expected_parts: u64,
        part_size_bytes: u64,
    ) -> UploadSessionManifest {
        UploadSessionManifest {
            fragment: "fragment".into(),
            object_key: "video.bin".into(),
            version_id: "01TESTVERSION000000000000".into(),
            session_id: "01TESTSESSION000000000000".into(),
            total_size_bytes,
            expected_parts,
            part_size_bytes,
            content_type: "application/octet-stream".into(),
            created_at: 0,
            stripe_size_bytes: 256 * 1024,
            membership_epoch: 0,
            data_shards: 1,
            parity_shards: 1,
        }
    }

    #[test]
    fn multipart_expected_part_size_uses_manifest_part_size() {
        let m = manifest(1024 * 1024 * 1024, 16, 64 * 1024 * 1024);

        assert_eq!(expected_part_size(&m, 0), 64 * 1024 * 1024);
        assert_eq!(expected_part_size(&m, 14), 64 * 1024 * 1024);
        assert_eq!(expected_part_size(&m, 15), 64 * 1024 * 1024);
    }

    #[test]
    fn multipart_expected_part_size_handles_short_final_part() {
        let m = manifest(130 * 1024 * 1024, 3, 64 * 1024 * 1024);

        assert_eq!(expected_part_size(&m, 0), 64 * 1024 * 1024);
        assert_eq!(expected_part_size(&m, 1), 64 * 1024 * 1024);
        assert_eq!(expected_part_size(&m, 2), 2 * 1024 * 1024);
    }

    #[test]
    fn cluster_one_shot_rejects_large_objects() {
        assert!(validate_cluster_one_shot_size(CLUSTER_ONE_SHOT_MAX_BYTES).is_ok());
        assert!(validate_cluster_one_shot_size(CLUSTER_ONE_SHOT_MAX_BYTES + 1).is_err());
    }
}
