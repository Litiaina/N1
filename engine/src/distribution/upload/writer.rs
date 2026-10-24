use super::*;

#[derive(Debug, Serialize)]
pub struct DistributedPartWrite {
    pub part: PartMetadata,
    pub pool_id: String,
    pub pool_generation: u64,
    pub membership_epoch: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
}

pub(super) enum ClusterShardWriter {
    Local(LocalShardWriter),
    Remote(RemoteShardWriter),
}

pub(super) struct LocalShardWriter {
    file: DirectWriter,
    tmp_path: PathBuf,
    stable_path: PathBuf,
    file_name: String,
    shard_index: usize,
    is_parity: bool,
    expected_len: u64,
    locator: PeerShardTarget,
    deferred_durability: bool,
    ownership: crate::distribution::upload_lifecycle::UploadReadGuard,
    runtime: Arc<crate::distribution::membership::ClusterRuntime>,
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
    async fn finish(
        mut self,
        checksum: String,
        block_checksums: Vec<Sha256Digest>,
        checksum_block_size: usize,
    ) -> Result<ShardInfo, AppError> {
        self.file.flush().await.map_err(|e| {
            AppError::Io(
                e,
                format!("flush cluster shard {}", self.tmp_path.display()),
            )
        })?;
        if !self.deferred_durability {
            self.file.sync_data().await.map_err(|e| {
                AppError::Io(e, format!("sync cluster shard {}", self.tmp_path.display()))
            })?;
        }
        drop(self.file);
        crate::distribution::peer::publish_guarded_shard(
            self.runtime,
            self.ownership,
            self.tmp_path,
            self.stable_path,
            checksum.clone(),
            self.expected_len,
            false,
            self.deferred_durability,
        )
        .await?;
        invalidate_local_usage_cache(Some(&self.locator.fragment));
        Ok(ShardInfo {
            shard_index: self.shard_index,
            file_name: self.file_name,
            absolute_path: encode_peer_shard_locator(&self.locator),
            size_bytes: self.expected_len,
            checksum,
            block_checksums,
            checksum_block_size,
            is_parity: self.is_parity,
        })
    }
}

pub(super) fn configured_layout(
    runtime: &crate::distribution::membership::ClusterRuntime,
) -> Result<ClusterEcLayout, AppError> {
    let pool = runtime.membership.newest_writable_pool().ok_or_else(|| {
        AppError::ServiceUnavailable("cluster has no writable server pool".into())
    })?;
    resolve_configured_layout(pool.node_ids.len(), &runtime.layout_policy)
        .map_err(AppError::BadRequest)
}

pub(super) fn validate_write_admission(
    runtime: &crate::distribution::membership::ClusterRuntime,
    reachable_namespace_nodes: usize,
    reachable_blob_nodes: usize,
    write_layout: ClusterEcLayout,
) -> Result<(), AppError> {
    let write_pool = runtime.membership.newest_writable_pool().ok_or_else(|| {
        AppError::ServiceUnavailable("cluster has no writable server pool".into())
    })?;
    let configured = configured_layout(runtime)?;
    if write_layout != configured {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival writes preserve the configured {}+{} layout",
            configured.data_shards, configured.parity_shards
        )));
    }
    if reachable_namespace_nodes < configured.data_shards {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival namespace publication requires at least K={} reachable pool nodes, but only {} are reachable",
            configured.data_shards, reachable_namespace_nodes
        )));
    }
    if reachable_blob_nodes < configured.data_shards {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival object writes preserve configured EC {}+{} and require at least K={} distinct writable server-pool nodes",
            configured.data_shards, configured.parity_shards, configured.data_shards
        )));
    }
    debug_assert_eq!(write_pool.policy.object_data_shards, configured.data_shards);
    Ok(())
}

pub(super) fn validate_pinned_upload_layout(
    layout: ClusterEcLayout,
    active_nodes: usize,
) -> Result<(), AppError> {
    if layout.data_shards == 0 || layout.total_shards() > active_nodes {
        return Err(AppError::ServiceUnavailable(format!(
            "upload session layout {}+{} requires {} writable node(s), but only {} are reachable",
            layout.data_shards,
            layout.parity_shards,
            layout.total_shards(),
            active_nodes
        )));
    }
    Ok(())
}

pub(super) fn configured_write_context() -> Result<
    (
        std::sync::Arc<crate::distribution::membership::ClusterRuntime>,
        ClusterEcLayout,
    ),
    AppError,
> {
    crate::core::operation_gate::ensure_client_mutation_allowed()?;
    let runtime = active_cluster_runtime()?;
    if runtime.membership.newest_writable_pool().is_none() {
        return Err(AppError::ServiceUnavailable(
            "cluster has no writable server pool".into(),
        ));
    }
    let layout = configured_layout(&runtime)?;
    Ok((runtime, layout))
}

pub(super) async fn current_write_context() -> Result<
    (
        std::sync::Arc<crate::distribution::membership::ClusterRuntime>,
        Vec<crate::distribution::membership::ClusterNode>,
        ClusterEcLayout,
    ),
    AppError,
> {
    let (runtime, layout) = configured_write_context()?;
    let pool = runtime.membership.newest_writable_pool().ok_or_else(|| {
        AppError::ServiceUnavailable("cluster has no writable server pool".into())
    })?;
    let active = pool_write_nodes(&runtime, &pool.id).await?;
    let configured = pool.node_ids.len();
    let live_layout =
        resolve_survival_write_layout(active.len(), configured, &runtime.layout_policy)
            .map_err(AppError::ServiceUnavailable)?;
    validate_write_admission(&runtime, active.len(), active.len(), layout)?;
    debug_assert_eq!(live_layout, layout);
    Ok((runtime, active, layout))
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
    async fn finish(
        self,
        checksum: String,
        block_checksums: Vec<Sha256Digest>,
        checksum_block_size: usize,
    ) -> Result<ShardInfo, AppError> {
        match self {
            Self::Local(writer) => {
                writer
                    .finish(checksum, block_checksums, checksum_block_size)
                    .await
            }
            Self::Remote(writer) => {
                let mut info = writer.finish().await?;
                if !info.checksum.eq_ignore_ascii_case(&checksum) {
                    return Err(AppError::BadRequest(format!(
                        "remote shard checksum mismatch for shard {}",
                        info.shard_index
                    )));
                }
                info.checksum = checksum;
                info.block_checksums = block_checksums;
                info.checksum_block_size = checksum_block_size;
                Ok(info)
            }
        }
    }
}

pub(super) fn configured_block_size() -> usize {
    crate::core::upload::STRIPE_SIZE_ATOMIC
        .get()
        .map(|value| value.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or_else(crate::core::disk_manager::default_stripe_size)
}

pub(super) fn choose_upload_stripe_size(content_length: u64) -> usize {
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

pub(super) fn planned_part_size(
    total_size: u64,
    expected_parts: u64,
    part_size: u64,
    index: u64,
) -> u64 {
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

pub(super) fn expected_part_size(manifest: &UploadSessionManifest, part_index: u64) -> u64 {
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

pub(super) fn stable_name(part_index: u64, shard_index: usize, is_parity: bool) -> String {
    let ext = if is_parity {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    };
    format!("part_{part_index}_shard_{shard_index}{ext}")
}

pub(super) fn target_for_placement(
    plan: &ClusterPlacementPlan,
    placement: &crate::distribution::placement::ClusterShardPlacement,
    part_index: u64,
    final_shard_len: u64,
    deferred_durability: bool,
) -> PeerShardTarget {
    let is_parity = matches!(
        placement.role,
        crate::distribution::placement::ClusterShardRole::Parity
    );
    PeerShardTarget {
        pool_id: plan.pool_id.clone(),
        pool_generation: plan.pool_generation,
        node_id: placement.node_id.clone(),
        instance_id: placement.instance_id.clone(),
        binding_epoch: plan.binding_epoch,
        root_id: placement.root_id.clone(),
        endpoint: placement
            .native_endpoint
            .clone()
            .unwrap_or_else(|| placement.endpoint.clone()),
        fragment: plan.fragment.clone(),
        object_id: plan.object_key.clone(),
        version_id: plan.version_id.clone(),
        part_index,
        shard_index: placement.shard_index,
        is_parity,
        expected_len: final_shard_len,
        deferred_durability,
        repair: false,
        expected_checksum: None,
    }
}

pub(super) fn planned_shard_info(
    target: &PeerShardTarget,
    checksum: String,
    block_checksums: Vec<Sha256Digest>,
    checksum_block_size: usize,
) -> ShardInfo {
    ShardInfo {
        shard_index: target.shard_index,
        file_name: stable_name(target.part_index, target.shard_index, target.is_parity),
        absolute_path: encode_peer_shard_locator(target),
        size_bytes: target.expected_len,
        checksum,
        block_checksums,
        checksum_block_size,
        is_parity: target.is_parity,
    }
}

pub(super) async fn open_local_writer(
    target: PeerShardTarget,
) -> Result<ClusterShardWriter, AppError> {
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve
        || runtime.local_node_id != target.node_id
        || runtime.local_instance_id != target.instance_id
        || runtime.membership.binding_epoch != target.binding_epoch
    {
        return Err(AppError::Conflict(format!(
            "local logical slot '{}' changed binding before shard writer creation",
            target.node_id
        )));
    }
    let ownership = crate::distribution::upload_lifecycle::guard_shard_write(
        &runtime,
        &target.pool_id,
        &target.fragment,
        &target.object_id,
        &target.version_id,
    )
    .await?;
    let pm = ObjectPathManager::new();
    let file_name = stable_name(target.part_index, target.shard_index, target.is_parity);
    let mut failures = Vec::new();
    for root in writable_local_roots(&runtime, &target.root_id, target.expected_len)? {
        let dir = pm.get_shard_dir_on_disk(
            &PathBuf::from(&root.path),
            &target.fragment,
            &target.object_id,
            &target.version_id,
        );
        if let Err(error) = fs::create_dir_all(&dir).await {
            failures.push(format!("{}: create directory: {}", root.id, error));
            continue;
        }
        let tmp_path = dir.join(format!("{file_name}.tmp.{}", Ulid::new()));
        let stable_path = dir.join(&file_name);
        match create_direct_file(&tmp_path).await {
            Ok(file) => {
                return Ok(ClusterShardWriter::Local(LocalShardWriter {
                    file: DirectWriter::new(file),
                    tmp_path,
                    stable_path,
                    file_name,
                    shard_index: target.shard_index,
                    is_parity: target.is_parity,
                    expected_len: target.expected_len,
                    deferred_durability: target.deferred_durability,
                    ownership,
                    runtime,
                    // The immutable locator keeps the deterministic preferred
                    // root. The node resolves it across current local disks.
                    locator: target,
                }));
            }
            Err(error) => failures.push(format!("{}: {}", root.id, error)),
        }
    }
    Err(AppError::ServiceUnavailable(format!(
        "node '{}' could not open shard {} on any healthy disk: {}",
        runtime.local_node_id,
        target.shard_index,
        failures.join("; ")
    )))
}

pub(super) async fn open_writers_for_plan(
    plan: &ClusterPlacementPlan,
    part_index: u64,
    final_shard_len: u64,
    reachable: &BTreeSet<String>,
    deferred_durability: bool,
) -> Result<Vec<Option<ClusterShardWriter>>, AppError> {
    let runtime = active_cluster_runtime()?;
    let mut writers = Vec::with_capacity(plan.placements.len());
    for placement in &plan.placements {
        if !reachable.contains(&placement.node_id) {
            writers.push(None);
            continue;
        }
        let target = target_for_placement(
            plan,
            placement,
            part_index,
            final_shard_len,
            deferred_durability,
        );
        let writer = if placement.node_id == runtime.local_node_id {
            open_local_writer(target).await
        } else {
            RemoteShardWriter::spawn(target).map(ClusterShardWriter::Remote)
        };
        match writer {
            Ok(writer) => writers.push(Some(writer)),
            Err(error) => {
                warn!(
                    node_id = %placement.node_id,
                    root_id = %placement.root_id,
                    shard_index = placement.shard_index,
                    "N2 survival upload could not open an assigned shard writer: {error}"
                );
                writers.push(None);
            }
        }
    }
    if writers.iter().filter(|writer| writer.is_some()).count() < plan.data_shards {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival upload requires K={} shard writers, but only {} opened",
            plan.data_shards,
            writers.iter().filter(|writer| writer.is_some()).count()
        )));
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
    pool_id: &str,
    pool_generation: u64,
    membership_epoch: u64,
    block_size: usize,
    body_stream: S,
    expected_checksum: Option<&str>,
    deferred_durability: bool,
) -> Result<DistributedPartWrite, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>>,
{
    let runtime = active_cluster_runtime()?;
    let pool = runtime.membership.pool(pool_id).ok_or_else(|| {
        AppError::Conflict(format!(
            "upload is pinned to unknown server pool '{pool_id}'"
        ))
    })?;
    if pool.generation != pool_generation
        || pool.policy.object_data_shards != data_shards
        || pool.policy.object_parity_shards != parity_shards
    {
        return Err(AppError::Conflict(
            "upload session pool generation or static EC contract changed".into(),
        ));
    }
    let configured_nodes = runtime.membership.nodes_for_pool(pool_id);
    let layout = ClusterEcLayout {
        data_shards,
        parity_shards,
    };
    validate_pinned_upload_layout(layout, configured_nodes.len())?;
    let placement = build_placement_plan(
        fragment,
        object_key,
        version_id,
        pool_id,
        pool_generation,
        membership_epoch,
        runtime.membership.binding_epoch,
        layout,
        &configured_nodes,
    )?;
    let reachable = pool_write_nodes(&runtime, pool_id)
        .await?
        .into_iter()
        .map(|node| node.id)
        .collect::<BTreeSet<_>>();
    let unavailable = placement
        .placements
        .iter()
        .filter(|placement| !reachable.contains(&placement.node_id))
        .map(|placement| placement.node_id.clone())
        .collect::<Vec<_>>();
    if reachable.len() < data_shards {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival placement for {fragment}/{object_key} requires K={data_shards} reachable participants; unavailable participant(s): {}",
            unavailable.join(", ")
        )));
    }
    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = content_length.div_ceil(stripe_data_width.max(1));
    let final_shard_len = stripe_count.saturating_mul(block_size as u64);
    let open_started = Instant::now();
    let writers = open_writers_for_plan(
        &placement,
        part_index,
        final_shard_len,
        &reachable,
        deferred_durability,
    )
    .await?;
    record_io_latency(
        "cluster.upload.open_writers",
        open_started.elapsed(),
        0,
        true,
    );
    let mut stream_writer = ErasureStreamWriter::new(
        writers,
        data_shards,
        parity_shards,
        content_length,
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
    let shard_block_checksums = stream_writer.get_shard_block_checksums();
    let total_checksum = stream_writer.get_total_checksum();
    let writers = stream_writer.into_writers();
    let finish_started = Instant::now();
    let finish_tasks = writers.into_iter().enumerate().map(|(idx, writer)| {
        let checksum = shard_checksums.get(idx).cloned().unwrap_or_default();
        let block_checksums = shard_block_checksums.get(idx).cloned().unwrap_or_default();
        async move {
            match writer {
                Some(writer) => writer
                    .finish(checksum, block_checksums, block_size)
                    .await
                    .map(Some),
                None => Ok(None),
            }
            .map(|shard| (idx, shard))
        }
    });
    let finish_results = join_all(finish_tasks).await;
    let successful_finishes = finish_results
        .iter()
        .filter(|result| matches!(result, Ok((_, Some(_)))))
        .count();
    let finish_ok = successful_finishes >= data_shards;
    record_io_latency(
        "cluster.upload.finish_shards",
        finish_started.elapsed(),
        content_length,
        finish_ok,
    );
    let mut finished = vec![None; placement.placements.len()];
    for result in finish_results {
        match result {
            Ok((index, shard)) => finished[index] = shard,
            Err(error) => warn!(
                fragment,
                object_key,
                part_index,
                "N2 survival upload lost a shard while finishing the part: {error}"
            ),
        }
    }
    let written = finished.iter().filter(|shard| shard.is_some()).count();
    if written < data_shards {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 survival upload wrote {written} of {} EC pieces; K={data_shards} is required",
            layout.total_shards()
        )));
    }
    if written < layout.total_shards() {
        warn!(
            fragment,
            object_key,
            part_index,
            written_pieces = written,
            target_pieces = layout.total_shards(),
            "N2 committed a reconstructable degraded part; autonomous repair owns the exact missing slots"
        );
    }
    let mut shards = Vec::with_capacity(placement.placements.len());
    for (index, assigned) in placement.placements.iter().enumerate() {
        let checksum = shard_checksums.get(index).cloned().ok_or_else(|| {
            AppError::InternalServerError(format!(
                "N2 EC writer omitted checksum for shard {index}"
            ))
        })?;
        let block_checksums = shard_block_checksums.get(index).cloned().ok_or_else(|| {
            AppError::InternalServerError(format!(
                "N2 EC writer omitted block checksums for shard {index}"
            ))
        })?;
        let target = target_for_placement(
            &placement,
            assigned,
            part_index,
            final_shard_len,
            deferred_durability,
        );
        shards.push(
            finished[index].take().unwrap_or_else(|| {
                planned_shard_info(&target, checksum, block_checksums, block_size)
            }),
        );
    }
    shards.sort_by_key(|shard| shard.shard_index);
    Ok(DistributedPartWrite {
        pool_id: pool_id.to_string(),
        pool_generation,
        membership_epoch,
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
