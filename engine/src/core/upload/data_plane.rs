//! Streaming EC data-plane writers, shard planning, publication, and cleanup.

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSessionManifest {
    pub fragment: String,
    pub object_key: String,
    pub version_id: String,
    pub session_id: String,
    pub total_size_bytes: u64,
    pub expected_parts: u64,
    pub part_size_bytes: u64,
    pub content_type: String,
    pub created_at: i64,
    pub stripe_size_bytes: usize,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub placement_roots: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveSessionInfo {
    pub session_id: String,
    pub version_id: String,
    pub object_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_object_key: Option<String>,
    pub created_at: i64,
    pub reserved_size: u64,
    pub total_size_bytes: Option<u64>,
    pub expected_parts: Option<u64>,
    pub committed_parts: u64,
    pub committed_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListUploadsResponse {
    pub sessions: Vec<ActiveSessionInfo>,
    pub total: usize,
}

pub(super) fn committed_session_progress(state: &index::IndexSessionState) -> (u64, u64) {
    if !state.part_receipts.is_empty() {
        let committed_parts = state.part_receipts.len() as u64;
        let committed_bytes = state
            .part_receipts
            .values()
            .map(|receipt| receipt.size_bytes)
            .sum();
        return (committed_parts, committed_bytes);
    }

    let committed_parts = state.parts.len() as u64;
    let committed_bytes = state.parts.values().map(|part| part.size_bytes).sum();
    (committed_parts, committed_bytes)
}

pub struct DirectWriter {
    file: Arc<StdFile>,
    write_buffer: AlignedBuffer,
    next_offset: u64,
    pub(super) write_task: Option<tokio::task::JoinHandle<Result<AlignedBuffer, std::io::Error>>>,
}

enum UploadShardWriter {
    Direct(DirectWriter),
    Buffered(tokio_fs::File),
}

impl UploadShardWriter {
    async fn sync_data(&self) -> Result<(), std::io::Error> {
        match self {
            Self::Direct(writer) => writer.sync_data().await,
            Self::Buffered(file) => file.sync_data().await,
        }
    }
}

impl AsyncWrite for UploadShardWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match &mut *self {
            Self::Direct(writer) => Pin::new(writer).poll_write(cx, buf),
            Self::Buffered(file) => Pin::new(file).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            Self::Direct(writer) => Pin::new(writer).poll_flush(cx),
            Self::Buffered(file) => Pin::new(file).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            Self::Direct(writer) => Pin::new(writer).poll_shutdown(cx),
            Self::Buffered(file) => Pin::new(file).poll_shutdown(cx),
        }
    }
}

impl DirectWriter {
    pub fn new(file: StdFile) -> Self {
        let configured = WRITER_BUFFER_SIZE_ATOMIC
            .get()
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(256 * 1024);
        let aligned = ((configured.max(4096) + 4095) / 4096) * 4096;
        Self {
            file: Arc::new(file),
            write_buffer: AlignedBuffer::new(aligned),
            next_offset: 0,
            write_task: None,
        }
    }

    fn poll_write_task(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(task) = &mut self.write_task {
            match Pin::new(task).poll(cx) {
                Poll::Ready(Ok(Ok(returned_buffer))) => {
                    self.write_buffer = returned_buffer;
                    self.write_task = None;
                }
                Poll::Ready(Ok(Err(e))) => {
                    self.write_task = None;
                    return Poll::Ready(Err(e));
                }
                Poll::Ready(Err(e)) => {
                    self.write_task = None;
                    return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }

    fn spawn_buffer_flush(&mut self) {
        if self.write_buffer.is_empty() || self.write_task.is_some() {
            return;
        }

        let file = self.file.clone();
        let offset = self.next_offset;
        let mut to_write = std::mem::replace(&mut self.write_buffer, AlignedBuffer::new(0));
        self.next_offset = self.next_offset.saturating_add(to_write.len() as u64);

        let task = tokio::task::spawn_blocking(move || {
            linux_io::pwrite_all_at(&*file, to_write.as_slice(), offset)?;
            to_write.clear();
            Ok::<AlignedBuffer, std::io::Error>(to_write)
        });

        self.write_task = Some(task);
    }

    pub async fn sync_data(&self) -> Result<(), std::io::Error> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || linux_io::fdatasync_blocking(&*file)).await?
    }
}

impl AsyncWrite for DirectWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.poll_write_task(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }

        if self.write_buffer.len() == self.write_buffer.capacity() {
            self.spawn_buffer_flush();
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let remaining = self.write_buffer.capacity() - self.write_buffer.len();
        let to_copy = std::cmp::min(remaining, buf.len());
        self.write_buffer.extend_from_slice(&buf[..to_copy]);

        if self.write_buffer.len() == self.write_buffer.capacity() {
            self.spawn_buffer_flush();
        }

        Poll::Ready(Ok(to_copy))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if !self.write_buffer.is_empty() && self.write_task.is_none() {
            self.spawn_buffer_flush();
        }

        match self.poll_write_task(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.as_mut().poll_flush(cx)
    }
}

pub async fn create_direct_file(path: &PathBuf) -> Result<StdFile, AppError> {
    linux_io::open_direct_write(path).await
}

async fn create_buffered_file(path: &PathBuf) -> Result<tokio_fs::File, AppError> {
    let file = linux_io::open_buffered_write(path).await?;
    Ok(tokio_fs::File::from_std(file))
}

pub async fn write_atomic(path: &std::path::Path, data: &[u8]) -> Result<(), AppError> {
    linux_io::atomic_write(path, data.to_vec()).await
}

pub(super) async fn read_manifest_any(
    fragment: &str,
    version_id: &str,
) -> Result<Option<UploadSessionManifest>, AppError> {
    if let Some(state) = index::load_session(fragment, version_id).await? {
        return Ok(Some(state.manifest));
    }
    Ok(None)
}

pub(super) fn configured_data_shards() -> usize {
    if let Ok(pool) = crate::core::storage_pool::write_pool() {
        return usize::from(pool.data_shards);
    }
    DATA_SHARDS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed) as usize)
        .unwrap_or(1)
}

pub(super) fn upload_layout(
    fragment: &str,
    object_id: &str,
    version: &str,
) -> Result<(usize, usize, Vec<String>), AppError> {
    if !crate::core::storage_pool::configured().is_empty() {
        let pool = crate::core::storage_pool::write_pool()?;
        return Ok((
            usize::from(pool.data_shards),
            usize::from(pool.parity_shards),
            pool.roots
                .iter()
                .map(|root| root.to_string_lossy().into_owned())
                .collect(),
        ));
    }
    let data = configured_data_shards();
    let parity = configured_parity_shards();
    let roots = ObjectPathManager::new().select_blob_roots_for_object(
        fragment,
        object_id,
        version,
        data + parity,
    )?;
    Ok((
        data,
        parity,
        roots
            .into_iter()
            .map(|root| root.to_string_lossy().into_owned())
            .collect(),
    ))
}

pub(super) fn configured_parity_shards() -> usize {
    if let Ok(pool) = crate::core::storage_pool::write_pool() {
        return usize::from(pool.parity_shards);
    }
    PARITY_SHARDS_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed) as usize)
        .unwrap_or(0)
}

pub(super) fn manifest_data_shards(manifest: &UploadSessionManifest) -> usize {
    usize::from(manifest.data_shards)
}

pub(super) fn manifest_parity_shards(manifest: &UploadSessionManifest) -> usize {
    usize::from(manifest.parity_shards)
}

fn manifest_total_shards(manifest: &UploadSessionManifest) -> usize {
    manifest_data_shards(manifest) + manifest_parity_shards(manifest)
}

pub(super) fn configured_block_size() -> usize {
    STRIPE_SIZE_ATOMIC
        .get()
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or_else(default_stripe_size)
}

pub(crate) fn choose_upload_stripe_size_for_len_with_ceiling(
    content_length: u64,
    configured: usize,
) -> usize {
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

pub(super) fn choose_upload_stripe_size_for_len(content_length: u64) -> usize {
    choose_upload_stripe_size_for_len_with_ceiling(content_length, configured_block_size())
}

pub(crate) fn target_stripe_size_for_metadata(
    metadata: &ObjectMetadata,
    configured_ceiling: usize,
) -> usize {
    let largest_part = metadata
        .parts
        .iter()
        .map(|part| part.size_bytes)
        .max()
        .unwrap_or(metadata.total_size_bytes);
    choose_upload_stripe_size_for_len_with_ceiling(largest_part, configured_ceiling)
}

pub(super) fn choose_upload_stripe_size_for_parts(part_sizes: &[u64]) -> usize {
    let largest_part = part_sizes.iter().copied().max().unwrap_or(0);
    choose_upload_stripe_size_for_len(largest_part)
}

pub(super) fn ensure_registered_placement_roots(
    placement_roots: &[PathBuf],
) -> Result<(), AppError> {
    for root in placement_roots {
        if !configured_root_identity_is_current(root) {
            return Err(AppError::ServiceUnavailable(format!(
                "N1 refused the write because blob root '{}' does not match its registered disk identity.",
                root.display()
            )));
        }
    }
    Ok(())
}

fn stable_shard_name(part_index: u64, shard_index: usize, data_shards: usize) -> String {
    let ext = if shard_index >= data_shards {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    };
    format!("part_{}_shard_{}{}", part_index, shard_index, ext)
}

pub(super) fn expected_part_size_bytes(
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Option<u64> {
    if manifest.expected_parts == 0 || part_index >= manifest.expected_parts {
        return None;
    }
    if manifest.expected_parts == 1 {
        return Some(manifest.total_size_bytes);
    }
    let part_size = manifest.part_size_bytes;
    if part_size == 0 {
        return None;
    }
    if part_index + 1 < manifest.expected_parts {
        return Some(part_size);
    }
    let prior_bytes = part_size.saturating_mul(manifest.expected_parts.saturating_sub(1));
    if prior_bytes > manifest.total_size_bytes {
        return None;
    }
    Some(manifest.total_size_bytes - prior_bytes)
}

pub(super) fn planned_part_sizes(
    total_size_bytes: u64,
    expected_parts: u64,
    part_size_bytes: u64,
) -> Vec<u64> {
    let mut part_sizes = Vec::with_capacity(expected_parts as usize);
    if expected_parts == 0 {
        return part_sizes;
    }
    for part_index in 0..expected_parts {
        let size = if expected_parts == 1 {
            total_size_bytes
        } else if part_index + 1 < expected_parts {
            part_size_bytes
        } else {
            let prior_bytes = part_size_bytes.saturating_mul(expected_parts.saturating_sub(1));
            total_size_bytes.saturating_sub(prior_bytes)
        };
        part_sizes.push(size);
    }
    part_sizes
}

pub(super) fn final_shard_len_for_part(
    part_size_bytes: u64,
    data_shards: usize,
    block_size: usize,
) -> u64 {
    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = if stripe_data_width == 0 {
        0
    } else {
        part_size_bytes.div_ceil(stripe_data_width)
    };
    stripe_count * block_size as u64
}

pub(super) fn shard_specs_for_part(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Result<Vec<(usize, bool, PathBuf, String)>, AppError> {
    let data_shards = manifest_data_shards(manifest);
    let total_shards = manifest_total_shards(manifest);
    if manifest.placement_roots.len() != total_shards {
        return Err(AppError::InternalServerError(format!(
            "upload manifest EC placement has {} roots but {} shards",
            manifest.placement_roots.len(),
            total_shards
        )));
    }
    let mut out = Vec::with_capacity(total_shards);

    for shard_index in 0..total_shards {
        let disk_root = PathBuf::from(&manifest.placement_roots[shard_index]);
        let shard_dir = path_manager.get_shard_dir_on_disk(
            &disk_root,
            &manifest.fragment,
            &manifest.object_key,
            &manifest.version_id,
        );
        let file_name = stable_shard_name(part_index, shard_index, data_shards);
        let stable_path = shard_dir.join(&file_name);
        out.push((
            shard_index,
            shard_index >= data_shards,
            stable_path,
            file_name,
        ));
    }

    Ok(out)
}

pub(super) async fn part_exists_on_disk(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Result<bool, AppError> {
    let Some(part_size) = expected_part_size_bytes(manifest, part_index) else {
        return Ok(false);
    };

    let data_shards = manifest_data_shards(manifest);
    let block_size = manifest.stripe_size_bytes;
    let expected_shard_len = final_shard_len_for_part(part_size, data_shards, block_size);

    for (_, _, stable_path, _) in shard_specs_for_part(path_manager, manifest, part_index)? {
        let meta = match tokio_fs::metadata(&stable_path).await {
            Ok(meta) => meta,
            Err(_) => return Ok(false),
        };
        if meta.len() != expected_shard_len {
            return Ok(false);
        }
    }

    Ok(true)
}

pub(super) async fn ensure_version_shard_dirs(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
) -> Result<(), AppError> {
    let mut shard_dirs = BTreeSet::new();
    // Every part of one immutable version shares these same K+M directories.
    for (_, _, shard_path, _) in shard_specs_for_part(path_manager, manifest, 0)? {
        if let Some(parent) = shard_path.parent() {
            shard_dirs.insert(parent.to_path_buf());
        }
    }

    let create_futs = shard_dirs
        .into_iter()
        .map(|shard_dir| async move { linux_io::ensure_dir_all(&shard_dir).await });

    for result in join_all(create_futs).await {
        result?;
    }

    Ok(())
}

pub(super) async fn uploaded_parts_from_disk(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
) -> Result<Vec<u64>, AppError> {
    let mut uploaded_parts = Vec::new();
    for part_index in 0..manifest.expected_parts {
        if part_exists_on_disk(path_manager, manifest, part_index).await? {
            uploaded_parts.push(part_index);
        }
    }
    Ok(uploaded_parts)
}

pub(super) fn uploaded_part_bytes(manifest: &UploadSessionManifest, parts: &BTreeSet<u64>) -> u64 {
    parts
        .iter()
        .filter_map(|part_index| expected_part_size_bytes(manifest, *part_index))
        .sum()
}

pub(super) async fn persist_missing_receipt_from_disk(
    path_manager: &ObjectPathManager,
    fragment: &str,
    version_id: &str,
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Result<(), AppError> {
    let receipt_exists = index::load_session_part_receipts(fragment, version_id, manifest)
        .await?
        .and_then(|receipts| receipts.get(part_index as usize).cloned().flatten())
        .is_some();
    if receipt_exists {
        return Ok(());
    }

    if let Some(part_meta) =
        reconstruct_part_meta_from_disk(path_manager, manifest, part_index).await?
    {
        index::commit_session_part_receipt(fragment, version_id, manifest, &part_meta).await?;
    }

    Ok(())
}

pub(super) async fn reconstruct_part_meta_from_disk(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    part_index: u64,
) -> Result<Option<PartMetadata>, AppError> {
    let Some(part_size) = expected_part_size_bytes(manifest, part_index) else {
        return Ok(None);
    };

    let data_shards = manifest_data_shards(manifest);
    let block_size = manifest.stripe_size_bytes;
    let expected_shard_len = final_shard_len_for_part(part_size, data_shards, block_size);

    let mut shards = Vec::with_capacity(manifest_total_shards(manifest));
    for (shard_index, is_parity, stable_path, file_name) in
        shard_specs_for_part(path_manager, manifest, part_index)?
    {
        let meta = match tokio_fs::metadata(&stable_path).await {
            Ok(meta) => meta,
            Err(_) => return Ok(None),
        };
        if meta.len() != expected_shard_len {
            return Ok(None);
        }
        let checksum = crc32_file_hex(&stable_path).await?;
        shards.push(ShardInfo {
            shard_index,
            file_name,
            absolute_path: stable_path.to_string_lossy().to_string(),
            size_bytes: expected_shard_len,
            checksum,
            is_parity,
        });
    }

    Ok(Some(PartMetadata {
        part_index,
        size_bytes: part_size,
        created_at: manifest.created_at,
        shards,
        checksum: None,
    }))
}

pub(super) fn format_crc32_u32(value: u32) -> String {
    format!("{:08x}", value)
}

pub(super) async fn reconstruct_part_meta_from_receipt(
    path_manager: &ObjectPathManager,
    manifest: &UploadSessionManifest,
    receipt: &index::SessionPartReceipt,
) -> Result<Option<PartMetadata>, AppError> {
    let Some(part_size) = expected_part_size_bytes(manifest, receipt.part_index) else {
        return Ok(None);
    };
    if receipt.size_bytes != part_size {
        return Ok(None);
    }

    let data_shards = manifest_data_shards(manifest);
    let total_shards = manifest_total_shards(manifest);
    if receipt.shard_checksums.len() != total_shards {
        return Ok(None);
    }

    let block_size = manifest.stripe_size_bytes;
    let expected_shard_len = final_shard_len_for_part(part_size, data_shards, block_size);
    let shard_size_bytes = if receipt.shard_size_bytes == 0 {
        expected_shard_len
    } else {
        receipt.shard_size_bytes
    };

    let mut shards = Vec::with_capacity(total_shards);
    for ((shard_index, is_parity, stable_path, file_name), checksum) in
        shard_specs_for_part(path_manager, manifest, receipt.part_index)?
            .into_iter()
            .zip(receipt.shard_checksums.iter())
    {
        shards.push(ShardInfo {
            shard_index,
            file_name,
            absolute_path: stable_path.to_string_lossy().to_string(),
            size_bytes: shard_size_bytes,
            checksum: format_crc32_u32(*checksum),
            is_parity,
        });
    }

    Ok(Some(PartMetadata {
        part_index: receipt.part_index,
        size_bytes: receipt.size_bytes,
        created_at: receipt.created_at,
        shards,
        checksum: receipt.checksum.map(format_crc32_u32),
    }))
}

pub(super) async fn read_part_meta_any(
    fragment: &str,
    version_id: &str,
    part_index: u64,
) -> Option<PartMetadata> {
    if let Ok(Some(part)) = index::read_session_part(fragment, version_id, part_index).await {
        return Some(part);
    }
    None
}

pub(super) fn request_content_length(headers: &HeaderMap) -> Result<u64, AppError> {
    headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or(AppError::BadRequest(
            "Content-Length header required".into(),
        ))
}

pub(super) fn request_content_type(headers: &HeaderMap) -> Option<String> {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or(v).trim().to_string())
        .filter(|v| !v.is_empty())
}

pub(super) async fn write_part_blob_to_disk(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    part_index: u64,
    content_length: u64,
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
    recorded_placement_roots: Option<&[String]>,
    body: Body,
    expected_checksum: Option<&str>,
    strict_durability: bool,
    progress: Option<&IoProgress>,
) -> Result<PartMetadata, AppError> {
    let body_stream = body.into_data_stream().map(|chunk_result| {
        chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "stream read".into(),
            )
        })
    });
    write_part_blob_to_disk_from_stream(
        path_manager,
        fragment,
        object_key,
        version_id,
        part_index,
        content_length,
        data_shards,
        parity_shards,
        block_size,
        recorded_placement_roots,
        body_stream,
        expected_checksum,
        strict_durability,
        progress,
    )
    .await
}

pub(super) async fn write_part_blob_to_disk_from_stream<S>(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    part_index: u64,
    content_length: u64,
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
    recorded_placement_roots: Option<&[String]>,
    mut body_stream: S,
    expected_checksum: Option<&str>,
    strict_durability: bool,
    progress: Option<&IoProgress>,
) -> Result<PartMetadata, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>> + Unpin,
{
    let total_shards = data_shards + parity_shards;

    if data_shards == 0 || total_shards == 0 {
        return Err(AppError::InternalServerError(
            "invalid erasure coding layout".into(),
        ));
    }

    if block_size == 0 || block_size % 4096 != 0 {
        return Err(AppError::InternalServerError(format!(
            "stripe_size_bytes must be a non-zero multiple of 4096 for O_DIRECT (got {})",
            block_size
        )));
    }

    let attempt_id = Ulid::new().to_string();
    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = (content_length + stripe_data_width - 1) / stripe_data_width;
    let final_shard_len = stripe_count * block_size as u64;

    let blob_roots = match recorded_placement_roots {
        Some(roots) => roots.iter().map(PathBuf::from).collect::<Vec<_>>(),
        None => path_manager.select_blob_roots_for_object(
            fragment,
            object_key,
            version_id,
            total_shards,
        )?,
    };
    if blob_roots.len() != total_shards {
        return Err(AppError::InternalServerError(format!(
            "object EC placement has {} roots but requires {}",
            blob_roots.len(),
            total_shards
        )));
    }
    ensure_registered_placement_roots(&blob_roots)?;
    let use_buffered_small_write =
        !strict_durability && content_length <= SMALL_BUFFERED_WRITE_LIMIT;
    let mut writers: Vec<Option<UploadShardWriter>> = Vec::with_capacity(total_shards);
    let mut tmp_paths: Vec<PathBuf> = Vec::with_capacity(total_shards);
    let mut stable_paths: Vec<PathBuf> = Vec::with_capacity(total_shards);
    let mut stable_names: Vec<String> = Vec::with_capacity(total_shards);
    let mut successful_opens = 0usize;
    let mut capacity_open_error = None;

    for i in 0..total_shards {
        let is_parity = i >= data_shards;
        let ext = if is_parity {
            PARITY_SUFFIX
        } else {
            SHARD_SUFFIX
        };

        let stable_name = format!("part_{}_shard_{}{}", part_index, i, ext);
        let tmp_name = format!("{}.tmp.{}", stable_name, attempt_id);
        let target_disk = &blob_roots[i];
        let shard_dir =
            path_manager.get_shard_dir_on_disk(target_disk, fragment, object_key, version_id);

        let tmp_path = shard_dir.join(&tmp_name);
        let stable_path = shard_dir.join(&stable_name);

        if use_buffered_small_write {
            match create_buffered_file(&tmp_path).await {
                Ok(file) => {
                    writers.push(Some(UploadShardWriter::Buffered(file)));
                    successful_opens += 1;
                }
                Err(err) => {
                    warn!(
                        "blob shard open failed for {} on {}: {}",
                        stable_name,
                        target_disk.display(),
                        err
                    );
                    if capacity_open_error.is_none() && err.is_capacity_error() {
                        capacity_open_error = Some(err);
                    }
                    writers.push(None);
                }
            }
        } else {
            match create_direct_file(&tmp_path).await {
                Ok(file) => {
                    if strict_durability {
                        let file_clone = file
                            .try_clone()
                            .map_err(|e| AppError::Io(e, "clone file handle".into()))?;
                        let prealloc_result = tokio::task::spawn_blocking(move || {
                            file_clone.set_len(final_shard_len)
                        })
                        .await;

                        match prealloc_result {
                            Ok(Ok(())) => {
                                writers
                                    .push(Some(UploadShardWriter::Direct(DirectWriter::new(file))));
                                successful_opens += 1;
                            }
                            Ok(Err(err)) => {
                                warn!(
                                    "blob shard preallocation failed for {} on {}: {}",
                                    stable_name,
                                    target_disk.display(),
                                    err
                                );
                                if capacity_open_error.is_none()
                                    && matches!(
                                        err.raw_os_error(),
                                        Some(libc::ENOSPC) | Some(libc::EDQUOT)
                                    )
                                {
                                    capacity_open_error = Some(AppError::Io(
                                        err,
                                        format!("preallocate shard {}", tmp_path.display()),
                                    ));
                                }
                                writers.push(None);
                            }
                            Err(err) => {
                                warn!(
                                    "blob shard preallocation task failed for {} on {}: {}",
                                    stable_name,
                                    target_disk.display(),
                                    err
                                );
                                writers.push(None);
                            }
                        }
                    } else {
                        writers.push(Some(UploadShardWriter::Direct(DirectWriter::new(file))));
                        successful_opens += 1;
                    }
                }
                Err(err) => {
                    warn!(
                        "blob shard open failed for {} on {}: {}",
                        stable_name,
                        target_disk.display(),
                        err
                    );
                    if capacity_open_error.is_none() && err.is_capacity_error() {
                        capacity_open_error = Some(err);
                    }
                    writers.push(None);
                }
            }
        }

        tmp_paths.push(tmp_path);
        stable_paths.push(stable_path);
        stable_names.push(stable_name);
    }

    if successful_opens < total_shards {
        cleanup_files_best_effort(&tmp_paths).await;
        if let Some(error) = capacity_open_error {
            return Err(error);
        }
        return Err(AppError::ServiceUnavailable(format!(
            "N1 refused an under-protected write. The {}D+{}P object requires all {} shard roots, but only {} opened.",
            data_shards, parity_shards, total_shards, successful_opens
        )));
    }

    let write_result = async {
        let calc_total_checksum = expected_checksum.is_some();
        let mut stream_writer = ErasureStreamWriter::new(
            writers,
            data_shards,
            parity_shards,
            content_length,
            calc_total_checksum,
            block_size,
        )?;

        let mut total_bytes_read = 0u64;

        while let Some(chunk_result) = body_stream.next().await {
            let chunk = chunk_result?;

            if chunk.is_empty() {
                continue;
            }

            total_bytes_read += chunk.len() as u64;
            if total_bytes_read > content_length {
                return Err(AppError::BadRequest("Body exceeds Content-Length".into()));
            }

            stream_writer.write(&chunk).await?;
            if let Some(progress) = progress {
                progress.record(chunk.len() as u64);
            }
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
            if let Some(calc_str) = calculated.as_ref() {
                if !expected.eq_ignore_ascii_case(calc_str) {
                    return Err(AppError::BadRequest("Checksum mismatch".into()));
                }
            }
        }

        let shard_checksums = stream_writer.get_shard_checksums();
        let total_checksum = stream_writer.get_total_checksum();
        let writers = stream_writer.into_writers();

        if writers.iter().any(Option::is_none) {
            return Err(AppError::ServiceUnavailable(format!(
                "N1 refused an under-protected write because the {}D+{}P shard set did not complete",
                data_shards, parity_shards
            )));
        }

        if strict_durability {
            let sync_futs = writers.into_iter().filter_map(|w_opt| {
                w_opt.map(|w| async move {
                    if let Err(e) = w.sync_data().await {
                        warn!("Failed to fsync shard: {}", e);
                        return Err(AppError::Io(e, "fsync shard".into()));
                    }
                    Ok(())
                })
            });

            let sync_results = join_all(sync_futs).await;
            for r in sync_results {
                r?;
            }
        }

        let publish_results = join_all((0..total_shards).map(|i| {
            let tmp_path = tmp_paths[i].clone();
            let stable_path = stable_paths[i].clone();
            let expected_crc = shard_checksums.get(i).cloned();
            async move {
                publish_one_shard(
                    tmp_path.as_path(),
                    stable_path.as_path(),
                    final_shard_len,
                    expected_crc.as_deref(),
                )
                .await?;
                Ok::<Option<PathBuf>, AppError>(stable_path.parent().map(Path::to_path_buf))
            }
        }))
        .await;

        let mut dirs_to_sync: Vec<PathBuf> = Vec::new();
        for result in publish_results {
            if let Some(dir) = result? {
                dirs_to_sync.push(dir);
            }
        }

        dirs_to_sync.sort();
        dirs_to_sync.dedup();

        if strict_durability {
            let _ = join_all(dirs_to_sync.iter().map(|d| sync_dir(d.as_path()))).await;
        }

        let mut shards_info = Vec::with_capacity(total_shards);
        for i in 0..total_shards {
            shards_info.push(ShardInfo {
                shard_index: i,
                file_name: stable_names[i].clone(),
                absolute_path: stable_paths[i].to_string_lossy().to_string(),
                size_bytes: final_shard_len,
                checksum: shard_checksums.get(i).cloned().unwrap_or_default(),
                is_parity: i >= data_shards,
            });
        }

        Ok(PartMetadata {
            part_index,
            size_bytes: total_bytes_read,
            created_at: Utc::now().timestamp(),
            checksum: if calc_total_checksum {
                total_checksum
            } else {
                None
            },
            shards: shards_info,
        })
    }
    .await;

    if write_result.is_err() {
        cleanup_files_best_effort(&tmp_paths).await;
        cleanup_files_best_effort(&stable_paths).await;
    }

    write_result
}

pub(super) async fn write_part_blob_to_disk_from_reader<R>(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    part_index: u64,
    content_length: u64,
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
    recorded_placement_roots: Option<&[String]>,
    reader: &mut R,
    expected_checksum: Option<&str>,
    strict_durability: bool,
    progress: Option<&IoProgress>,
) -> Result<PartMetadata, AppError>
where
    R: AsyncRead + Unpin + Send,
{
    let total_shards = data_shards + parity_shards;

    if data_shards == 0 || total_shards == 0 {
        return Err(AppError::InternalServerError(
            "invalid erasure coding layout".into(),
        ));
    }

    if block_size == 0 || block_size % 4096 != 0 {
        return Err(AppError::InternalServerError(format!(
            "stripe_size_bytes must be a non-zero multiple of 4096 for O_DIRECT (got {})",
            block_size
        )));
    }

    let attempt_id = Ulid::new().to_string();
    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = (content_length + stripe_data_width - 1) / stripe_data_width;
    let final_shard_len = stripe_count * block_size as u64;

    let blob_roots = match recorded_placement_roots {
        Some(roots) => roots.iter().map(PathBuf::from).collect::<Vec<_>>(),
        None => path_manager.select_blob_roots_for_object(
            fragment,
            object_key,
            version_id,
            total_shards,
        )?,
    };
    if blob_roots.len() != total_shards {
        return Err(AppError::InternalServerError(format!(
            "object EC placement has {} roots but requires {}",
            blob_roots.len(),
            total_shards
        )));
    }
    ensure_registered_placement_roots(&blob_roots)?;
    let use_buffered_small_write =
        !strict_durability && content_length <= SMALL_BUFFERED_WRITE_LIMIT;
    let mut writers: Vec<Option<UploadShardWriter>> = Vec::with_capacity(total_shards);
    let mut tmp_paths: Vec<PathBuf> = Vec::with_capacity(total_shards);
    let mut stable_paths: Vec<PathBuf> = Vec::with_capacity(total_shards);
    let mut stable_names: Vec<String> = Vec::with_capacity(total_shards);
    let mut successful_opens = 0usize;
    let mut capacity_open_error = None;

    for i in 0..total_shards {
        let is_parity = i >= data_shards;
        let ext = if is_parity {
            PARITY_SUFFIX
        } else {
            SHARD_SUFFIX
        };

        let stable_name = format!("part_{}_shard_{}{}", part_index, i, ext);
        let tmp_name = format!("{}.tmp.{}", stable_name, attempt_id);
        let target_disk = &blob_roots[i];
        let shard_dir =
            path_manager.get_shard_dir_on_disk(target_disk, fragment, object_key, version_id);

        let tmp_path = shard_dir.join(&tmp_name);
        let stable_path = shard_dir.join(&stable_name);

        if use_buffered_small_write {
            match create_buffered_file(&tmp_path).await {
                Ok(file) => {
                    writers.push(Some(UploadShardWriter::Buffered(file)));
                    successful_opens += 1;
                }
                Err(err) => {
                    warn!(
                        "blob shard open failed for {} on {}: {}",
                        stable_name,
                        target_disk.display(),
                        err
                    );
                    if capacity_open_error.is_none() && err.is_capacity_error() {
                        capacity_open_error = Some(err);
                    }
                    writers.push(None);
                }
            }
        } else {
            match create_direct_file(&tmp_path).await {
                Ok(file) => {
                    if strict_durability {
                        let file_clone = file
                            .try_clone()
                            .map_err(|e| AppError::Io(e, "clone file handle".into()))?;
                        let prealloc_result = tokio::task::spawn_blocking(move || {
                            file_clone.set_len(final_shard_len)
                        })
                        .await;

                        match prealloc_result {
                            Ok(Ok(())) => {
                                writers
                                    .push(Some(UploadShardWriter::Direct(DirectWriter::new(file))));
                                successful_opens += 1;
                            }
                            Ok(Err(err)) => {
                                warn!(
                                    "blob shard preallocation failed for {} on {}: {}",
                                    stable_name,
                                    target_disk.display(),
                                    err
                                );
                                if capacity_open_error.is_none()
                                    && matches!(
                                        err.raw_os_error(),
                                        Some(libc::ENOSPC) | Some(libc::EDQUOT)
                                    )
                                {
                                    capacity_open_error = Some(AppError::Io(
                                        err,
                                        format!("preallocate shard {}", tmp_path.display()),
                                    ));
                                }
                                writers.push(None);
                            }
                            Err(err) => {
                                warn!(
                                    "blob shard preallocation task failed for {} on {}: {}",
                                    stable_name,
                                    target_disk.display(),
                                    err
                                );
                                writers.push(None);
                            }
                        }
                    } else {
                        writers.push(Some(UploadShardWriter::Direct(DirectWriter::new(file))));
                        successful_opens += 1;
                    }
                }
                Err(err) => {
                    warn!(
                        "blob shard open failed for {} on {}: {}",
                        stable_name,
                        target_disk.display(),
                        err
                    );
                    if capacity_open_error.is_none() && err.is_capacity_error() {
                        capacity_open_error = Some(err);
                    }
                    writers.push(None);
                }
            }
        }

        tmp_paths.push(tmp_path);
        stable_paths.push(stable_path);
        stable_names.push(stable_name);
    }

    if successful_opens < total_shards {
        cleanup_files_best_effort(&tmp_paths).await;
        if let Some(error) = capacity_open_error {
            return Err(error);
        }
        return Err(AppError::ServiceUnavailable(format!(
            "N1 refused an under-protected write. The {}D+{}P object requires all {} shard roots, but only {} opened.",
            data_shards, parity_shards, total_shards, successful_opens
        )));
    }

    let write_result = async {
        let calc_total_checksum = expected_checksum.is_some();
        let mut stream_writer = ErasureStreamWriter::new(
            writers,
            data_shards,
            parity_shards,
            content_length,
            calc_total_checksum,
            block_size,
        )?;

        let mut progress_reader = ProgressReader {
            inner: reader,
            progress,
        };
        let total_bytes_read = stream_writer
            .write_exact_from_reader(&mut progress_reader, content_length)
            .await?;

        stream_writer.finalize().await?;

        if let Some(expected) = expected_checksum {
            let calculated = stream_writer.get_total_checksum();
            if let Some(calc_str) = calculated.as_ref() {
                if !expected.eq_ignore_ascii_case(calc_str) {
                    return Err(AppError::BadRequest("Checksum mismatch".into()));
                }
            }
        }

        let shard_checksums = stream_writer.get_shard_checksums();
        let total_checksum = stream_writer.get_total_checksum();
        let writers = stream_writer.into_writers();

        if writers.iter().any(Option::is_none) {
            return Err(AppError::ServiceUnavailable(format!(
                "N1 refused an under-protected write because the {}D+{}P shard set did not complete",
                data_shards, parity_shards
            )));
        }

        if strict_durability {
            let sync_futs = writers.into_iter().filter_map(|w_opt| {
                w_opt.map(|w| async move {
                    if let Err(e) = w.sync_data().await {
                        warn!("Failed to fsync shard: {}", e);
                        return Err(AppError::Io(e, "fsync shard".into()));
                    }
                    Ok(())
                })
            });

            let sync_results = join_all(sync_futs).await;
            for r in sync_results {
                r?;
            }
        }

        let publish_results = join_all((0..total_shards).map(|i| {
            let tmp_path = tmp_paths[i].clone();
            let stable_path = stable_paths[i].clone();
            let expected_crc = shard_checksums.get(i).cloned();
            async move {
                publish_one_shard(
                    tmp_path.as_path(),
                    stable_path.as_path(),
                    final_shard_len,
                    expected_crc.as_deref(),
                )
                .await?;
                Ok::<Option<PathBuf>, AppError>(stable_path.parent().map(Path::to_path_buf))
            }
        }))
        .await;

        let mut dirs_to_sync: Vec<PathBuf> = Vec::new();
        for result in publish_results {
            if let Some(dir) = result? {
                dirs_to_sync.push(dir);
            }
        }

        dirs_to_sync.sort();
        dirs_to_sync.dedup();

        if strict_durability {
            let _ = join_all(dirs_to_sync.iter().map(|d| sync_dir(d.as_path()))).await;
        }

        let mut shards_info = Vec::with_capacity(total_shards);
        for i in 0..total_shards {
            shards_info.push(ShardInfo {
                shard_index: i,
                file_name: stable_names[i].clone(),
                absolute_path: stable_paths[i].to_string_lossy().to_string(),
                size_bytes: final_shard_len,
                checksum: shard_checksums.get(i).cloned().unwrap_or_default(),
                is_parity: i >= data_shards,
            });
        }

        Ok(PartMetadata {
            part_index,
            size_bytes: total_bytes_read,
            created_at: Utc::now().timestamp(),
            checksum: if calc_total_checksum {
                total_checksum
            } else {
                None
            },
            shards: shards_info,
        })
    }
    .await;

    if write_result.is_err() {
        cleanup_files_best_effort(&tmp_paths).await;
        cleanup_files_best_effort(&stable_paths).await;
    }

    write_result
}

pub(super) async fn cleanup_files_best_effort(paths: &[PathBuf]) {
    let futs = paths.iter().cloned().map(|p| async move {
        let _ = linux_io::remove_file_if_exists(&p).await;
    });
    let _ = join_all(futs).await;
}

pub(super) async fn cleanup_blob_version_dirs_for_version(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) {
    let futs = path_manager.get_blob_roots().into_iter().map(|root| {
        let dir = path_manager.get_shard_dir_on_disk(&root, fragment, object_key, version_id);
        async move {
            let parent = dir.parent().map(Path::to_path_buf);
            let _ = tokio_fs::remove_dir_all(&dir).await;
            if let Some(parent) = parent {
                let _ = sync_dir(&parent).await;
            }
        }
    });
    let _ = join_all(futs).await;
}

async fn crc32_file_hex(path: &Path) -> Result<String, AppError> {
    let file = linux_io::open_read(path).await?;
    let mut hasher = Hasher::new();
    let mut offset = 0u64;

    loop {
        let cloned = file
            .try_clone()
            .map_err(|e| AppError::Io(e, format!("clone for crc32 {}", path.display())))?;
        let chunk = tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; 256 * 1024];
            let read = linux_io::pread_at(&cloned, &mut buf, offset)?;
            buf.truncate(read);
            Ok::<_, std::io::Error>(buf)
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("crc32 read task: {}", e)))?
        .map_err(|e| AppError::Io(e, format!("read crc32 {}", path.display())))?;
        if chunk.is_empty() {
            break;
        }
        offset = offset.saturating_add(chunk.len() as u64);
        hasher.update(&chunk);
    }

    Ok(format!("{:08x}", hasher.finalize()))
}

async fn publish_one_shard(
    tmp_path: &Path,
    stable_path: &Path,
    expected_len: u64,
    expected_crc32: Option<&str>,
) -> Result<(), AppError> {
    if !tokio_fs::try_exists(stable_path).await.unwrap_or(false) {
        linux_io::rename(tmp_path, stable_path).await?;
        return Ok(());
    }

    let len_ok = tokio_fs::metadata(stable_path)
        .await
        .map(|m| m.len() == expected_len)
        .unwrap_or(false);

    if len_ok {
        if let Some(exp) = expected_crc32.filter(|s| !s.is_empty()) {
            let got = crc32_file_hex(stable_path).await?;
            if got.eq_ignore_ascii_case(exp) {
                let _ = linux_io::remove_file_if_exists(tmp_path).await;
                return Ok(());
            }
        } else {
            let _ = linux_io::remove_file_if_exists(tmp_path).await;
            return Ok(());
        }
    }

    let _ = linux_io::remove_file_if_exists(stable_path).await;
    linux_io::rename(tmp_path, stable_path).await?;

    Ok(())
}

fn is_tmp_attempt_shard(name: &str) -> bool {
    if !name.starts_with("part_") || !name.contains("_shard_") || !name.contains(".tmp.") {
        return false;
    }

    name.contains(&format!("{}.tmp.", SHARD_SUFFIX))
        || name.contains(&format!("{}.tmp.", PARITY_SUFFIX))
}

pub(super) async fn cleanup_blob_tmp_attempt_files_for_version(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) {
    let blob_roots = path_manager.get_blob_roots();

    let futs = blob_roots.into_iter().map(|root| {
        let dir = path_manager.get_shard_dir_on_disk(&root, fragment, object_key, version_id);
        async move {
            if !tokio_fs::try_exists(&dir).await.unwrap_or(false) {
                return;
            }

            let mut entries = match tokio_fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(_) => return,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let p = entry.path();
                if !p.is_file() {
                    continue;
                }

                let name = entry.file_name().to_string_lossy().to_string();
                if is_tmp_attempt_shard(&name) {
                    let _ = linux_io::remove_file_if_exists(&p).await;
                }
            }

            let _ = sync_dir(&dir).await;
        }
    });

    let _ = join_all(futs).await;
}
