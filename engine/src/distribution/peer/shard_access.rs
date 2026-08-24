use super::*;

fn encode_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn decode_segment(value: &str) -> Option<String> {
    percent_decode_str(value)
        .decode_utf8()
        .ok()
        .map(|v| v.to_string())
}

#[derive(Clone, Debug)]
pub struct PeerShardLocator {
    pub pool_id: String,
    pub pool_generation: u64,
    pub node_id: String,
    pub instance_id: String,
    pub binding_epoch: u64,
    pub root_id: String,
    pub endpoint: String,
    pub fragment: String,
    pub object_id: String,
    pub version_id: String,
    pub part_index: u64,
    pub shard_index: usize,
    pub is_parity: bool,
}

pub fn encode_peer_shard_locator(target: &PeerShardTarget) -> String {
    format!(
        "n2-peer-v3|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        encode_segment(&target.pool_id),
        target.pool_generation,
        encode_segment(&target.node_id),
        encode_segment(&target.root_id),
        encode_segment(&target.fragment),
        encode_segment(&target.object_id),
        encode_segment(&target.version_id),
        target.part_index,
        target.shard_index,
        target.is_parity
    )
}

pub fn parse_peer_shard_locator(value: &str) -> Option<PeerShardLocator> {
    let mut parts = value.split('|');
    if parts.next()? != "n2-peer-v3" {
        return None;
    }
    let pool_id = decode_segment(parts.next()?)?;
    let pool_generation = parts.next()?.parse().ok()?;
    let node_id = decode_segment(parts.next()?)?;
    let root_id = decode_segment(parts.next()?)?;
    let fragment = decode_segment(parts.next()?)?;
    let object_id = decode_segment(parts.next()?)?;
    let version_id = decode_segment(parts.next()?)?;
    let part_index = parts.next()?.parse().ok()?;
    let shard_index = parts.next()?.parse().ok()?;
    let is_parity = parts.next()?.parse().ok()?;
    let (instance_id, binding_epoch, endpoint) = try_active_cluster_runtime()
        .and_then(|runtime| {
            let pool = runtime.membership.pool(&pool_id)?;
            if pool.generation != pool_generation
                || !pool.node_ids.iter().any(|slot| slot == &node_id)
            {
                return None;
            }
            let binding = runtime
                .membership
                .nodes
                .iter()
                .find(|node| node.id == node_id)?;
            let endpoint = binding
                .native_endpoint
                .clone()
                .unwrap_or_else(|| binding.endpoint.clone());
            Some((
                binding.instance_id.clone(),
                runtime.membership.binding_epoch,
                endpoint,
            ))
        })
        .unwrap_or_else(|| (String::new(), 0, String::new()));
    Some(PeerShardLocator {
        pool_id,
        pool_generation,
        node_id,
        instance_id,
        binding_epoch,
        root_id,
        endpoint,
        fragment,
        object_id,
        version_id,
        part_index,
        shard_index,
        is_parity,
    })
}

pub fn resolve_peer_shard_locator(
    locator: &PeerShardLocator,
) -> Result<PeerShardLocator, AppError> {
    let runtime = active_cluster_runtime()?;
    let pool = runtime.membership.pool(&locator.pool_id).ok_or_else(|| {
        AppError::Conflict(format!(
            "recorded shard references unknown pool '{}'",
            locator.pool_id
        ))
    })?;
    if pool.generation != locator.pool_generation
        || !pool.node_ids.iter().any(|slot| slot == &locator.node_id)
    {
        return Err(AppError::Conflict(format!(
            "recorded shard logical slot '{}' is not in pool '{}' generation {}",
            locator.node_id, locator.pool_id, locator.pool_generation
        )));
    }
    let binding = runtime
        .membership
        .nodes
        .iter()
        .find(|node| node.id == locator.node_id)
        .ok_or_else(|| {
            AppError::ServiceUnavailable(format!(
                "logical slot '{}' is currently unbound",
                locator.node_id
            ))
        })?;
    let mut resolved = locator.clone();
    resolved.instance_id = binding.instance_id.clone();
    resolved.binding_epoch = runtime.membership.binding_epoch;
    resolved.endpoint = binding
        .native_endpoint
        .clone()
        .unwrap_or_else(|| binding.endpoint.clone());
    Ok(resolved)
}

pub(super) fn peer_shard_url(target: &PeerShardTarget) -> String {
    format!(
        "{}/n2/peer/shards/{}/{}/{}/{}/{}?pool_id={}&pool_generation={}&slot_id={}&instance_id={}&binding_epoch={}&root_id={}&parity={}&expected_len={}&deferred_durability={}",
        target.endpoint.trim_end_matches('/'),
        encode_segment(&target.fragment),
        encode_segment(&target.object_id),
        encode_segment(&target.version_id),
        target.part_index,
        target.shard_index,
        encode_segment(&target.pool_id),
        target.pool_generation,
        encode_segment(&target.node_id),
        encode_segment(&target.instance_id),
        target.binding_epoch,
        encode_segment(&target.root_id),
        target.is_parity,
        target.expected_len,
        target.deferred_durability
    )
}

fn peer_shard_read_url(locator: &PeerShardLocator, offset: u64, length: usize) -> String {
    format!(
        "{}/n2/peer/shards/{}/{}/{}/{}/{}?pool_id={}&pool_generation={}&slot_id={}&instance_id={}&binding_epoch={}&root_id={}&parity={}&offset={}&length={}",
        locator.endpoint.trim_end_matches('/'),
        encode_segment(&locator.fragment),
        encode_segment(&locator.object_id),
        encode_segment(&locator.version_id),
        locator.part_index,
        locator.shard_index,
        encode_segment(&locator.pool_id),
        locator.pool_generation,
        encode_segment(&locator.node_id),
        encode_segment(&locator.instance_id),
        locator.binding_epoch,
        encode_segment(&locator.root_id),
        locator.is_parity,
        offset,
        length
    )
}

pub async fn fetch_peer_shard_chunk(
    locator: &PeerShardLocator,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, AppError> {
    let locator = resolve_peer_shard_locator(locator)?;
    let permit = acquire_peer_operation(&locator.node_id, PeerOperation::Read)?;
    let started = Instant::now();
    let label = if endpoint_is_native(&locator.endpoint) {
        "peer.read.native"
    } else {
        "peer.read.http"
    };
    let result = if endpoint_is_native(&locator.endpoint) {
        native_get_peer_shard(&locator, offset, length).await
    } else {
        http_get_peer_shard(&locator, offset, length).await
    };
    let bytes_len = result.as_ref().map(|bytes| bytes.len() as u64).unwrap_or(0);
    record_io_latency(label, started.elapsed(), bytes_len, result.is_ok());
    match result {
        Ok(bytes) => {
            permit.record_progress(bytes.len() as u64);
            permit.complete();
            Ok(bytes)
        }
        Err(err) => Err(err),
    }
}

async fn http_get_peer_shard(
    locator: &PeerShardLocator,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, AppError> {
    let client = peer_http_client()?;
    let mut req = client.get(peer_shard_read_url(locator, offset, length));
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = req.timeout(PEER_READ_TIMEOUT).send().await.map_err(|e| {
        AppError::ServiceUnavailable(format!(
            "peer shard read failed from {}: {e}",
            locator.node_id
        ))
    })?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(AppError::NotFound);
        }
        return Err(AppError::ServiceUnavailable(format!(
            "peer shard read from {} returned {}: {}",
            locator.node_id, status, text
        )));
    }
    timeout(PEER_READ_TIMEOUT, response.bytes())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "peer shard body read timed out from {}",
                locator.node_id
            ))
        })?
        .map(|bytes| bytes.to_vec())
        .map_err(|e| AppError::ServiceUnavailable(format!("read peer shard body: {e}")))
}

async fn native_get_peer_shard(
    locator: &PeerShardLocator,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, AppError> {
    let addr = native_socket_addr(&locator.endpoint)?;
    let mut stream = timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(&addr))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "connect N2 native peer {} at {} timed out",
                locator.node_id, addr
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!(
                "connect N2 native peer {} at {} failed: {}",
                locator.node_id, addr, e
            ))
        })?;
    let _ = stream.set_nodelay(true);
    let payload = NativePeerShardPayload {
        secret: cluster_secret(),
        pool_id: locator.pool_id.clone(),
        pool_generation: locator.pool_generation,
        slot_id: locator.node_id.clone(),
        instance_id: locator.instance_id.clone(),
        binding_epoch: locator.binding_epoch,
        fragment: locator.fragment.clone(),
        root_id: locator.root_id.clone(),
        object_id: locator.object_id.clone(),
        version_id: locator.version_id.clone(),
        part_index: locator.part_index,
        shard_index: locator.shard_index,
        parity: locator.is_parity,
        expected_len: None,
        offset: Some(offset),
        length: Some(length),
        deferred_durability: false,
    };
    let request = NativeRequest {
        id: format!("peer-get-{}", Ulid::new()),
        op: "peer.shard.get".to_string(),
        payload: Some(serde_json::to_string(&payload).map_err(|e| {
            AppError::InternalServerError(format!("encode native peer get payload: {e}"))
        })?),
        body: None,
        body_len: None,
    };
    timeout(
        PEER_WRITE_TIMEOUT,
        write_request_control(&mut stream, &request),
    )
    .await
    .map_err(|_| {
        AppError::ServiceUnavailable(format!(
            "write N2 native peer get control to {} timed out",
            locator.node_id
        ))
    })?
    .map_err(|e| {
        AppError::ServiceUnavailable(format!("write N2 native peer get control failed: {e}"))
    })?;
    let response = timeout(PEER_READ_TIMEOUT, read_native_peer_response(&mut stream))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "read N2 native peer get response from {} timed out",
                locator.node_id
            ))
        })??;
    if !response.ok {
        return Err(native_peer_locator_response_error(
            "get", locator, &response,
        ));
    }
    let body_len = response.body_len.unwrap_or(0);
    timeout(PEER_READ_TIMEOUT, read_response_body(&mut stream, body_len))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "read N2 native peer shard body from {} timed out",
                locator.node_id
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!("read N2 native peer shard body failed: {e}"))
        })
}

pub(super) async fn read_native_peer_response(
    stream: &mut TcpStream,
) -> Result<NativeResponse, AppError> {
    read_response_control(stream)
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("read N2 native peer response: {e}")))?
        .ok_or_else(|| AppError::ServiceUnavailable("N2 native peer closed early".into()))
}

pub(super) fn native_peer_response_error(
    op: &str,
    target: &PeerShardTarget,
    response: &NativeResponse,
) -> AppError {
    AppError::ServiceUnavailable(format!(
        "N2 native peer shard {op} returned status {:?} from {}: {}",
        response.status,
        target.node_id,
        response
            .error
            .as_ref()
            .map(|err| err.message.as_str())
            .unwrap_or("unknown native peer error")
    ))
}

fn native_peer_locator_response_error(
    op: &str,
    locator: &PeerShardLocator,
    response: &NativeResponse,
) -> AppError {
    if response.status == Some(StatusCode::NOT_FOUND.as_u16()) {
        return AppError::NotFound;
    }
    AppError::ServiceUnavailable(format!(
        "N2 native peer shard {op} returned status {:?} from {}: {}",
        response.status,
        locator.node_id,
        response
            .error
            .as_ref()
            .map(|err| err.message.as_str())
            .unwrap_or("unknown native peer error")
    ))
}

fn stable_name(part_index: u64, shard_index: usize, is_parity: bool) -> String {
    let ext = if is_parity {
        PARITY_SUFFIX
    } else {
        SHARD_SUFFIX
    };
    format!("part_{part_index}_shard_{shard_index}{ext}")
}

struct OpenLocalShard {
    dir: std::path::PathBuf,
    tmp_path: std::path::PathBuf,
    stable_path: std::path::PathBuf,
    file: std::fs::File,
}

async fn open_local_shard_for_write(
    runtime: &ClusterRuntime,
    preferred_root_id: &str,
    fragment: &str,
    object_id: &str,
    version_id: &str,
    file_name: &str,
    expected_len: u64,
) -> Result<OpenLocalShard, AppError> {
    let pm = ObjectPathManager::new();
    let mut failures = Vec::new();
    for root in writable_local_roots(runtime, preferred_root_id, expected_len)? {
        let dir = pm.get_shard_dir_on_disk(
            &std::path::PathBuf::from(&root.path),
            fragment,
            object_id,
            version_id,
        );
        if let Err(error) = fs::create_dir_all(&dir).await {
            failures.push(format!("{}: create directory: {}", root.id, error));
            continue;
        }
        let tmp_path = dir.join(format!("{file_name}.tmp.{}", Ulid::new()));
        let stable_path = dir.join(file_name);
        match create_direct_file(&tmp_path).await {
            Ok(file) => {
                return Ok(OpenLocalShard {
                    dir,
                    tmp_path,
                    stable_path,
                    file,
                });
            }
            Err(error) => failures.push(format!("{}: {}", root.id, error)),
        }
    }
    Err(AppError::ServiceUnavailable(format!(
        "node '{}' could not open shard on any healthy disk: {}",
        runtime.local_node_id,
        failures.join("; ")
    )))
}

pub(super) async fn open_local_shard_for_read(
    runtime: &ClusterRuntime,
    preferred_root_id: &str,
    fragment: &str,
    object_id: &str,
    version_id: &str,
    file_name: &str,
) -> Result<(tokio::fs::File, std::path::PathBuf), AppError> {
    let pm = ObjectPathManager::new();
    let mut first_io_error = None;
    let mut checked_readable_root = false;
    for root in ordered_local_roots(runtime, preferred_root_id) {
        let path = pm
            .get_shard_dir_on_disk(
                &std::path::PathBuf::from(&root.path),
                fragment,
                object_id,
                version_id,
            )
            .join(file_name);
        match fs::File::open(&path).await {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                checked_readable_root = true;
            }
            Err(error) => {
                first_io_error.get_or_insert_with(|| {
                    AppError::Io(error, format!("open peer shard {}", path.display()))
                });
            }
        }
    }
    // The node is the EC failure domain. If at least one enrolled local root
    // was readable, absence there is repairable local disk loss even when the
    // preferred root returned an I/O error. Returning NotFound lets the
    // recorded-layout healer reconstruct onto a healthy root on this same
    // node. Preserve Unavailable only when no local root could be checked.
    if checked_readable_root {
        return Err(AppError::NotFound);
    }
    Err(first_io_error.unwrap_or(AppError::NotFound))
}

fn validate_peer_binding(
    pool_id: &str,
    pool_generation: u64,
    slot_id: &str,
    instance_id: &str,
    binding_epoch: u64,
    _root_id: &str,
) -> Result<Arc<ClusterRuntime>, AppError> {
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve {
        return Err(AppError::ServiceUnavailable(format!(
            "reserve instance '{}' is not bound to a logical slot",
            runtime.local_instance_id
        )));
    }
    if binding_epoch != runtime.membership.binding_epoch {
        return Err(AppError::Conflict(format!(
            "stale N2 binding epoch {binding_epoch}; current epoch is {}",
            runtime.membership.binding_epoch
        )));
    }
    let pool = runtime
        .membership
        .pool(pool_id)
        .ok_or_else(|| AppError::Conflict(format!("unknown server pool '{pool_id}'")))?;
    if pool.generation != pool_generation
        || !pool.node_ids.iter().any(|candidate| candidate == slot_id)
    {
        return Err(AppError::Conflict(format!(
            "logical slot '{slot_id}' is not part of pool '{pool_id}' generation {pool_generation}"
        )));
    }
    let binding = runtime
        .membership
        .nodes
        .iter()
        .find(|node| node.id == slot_id)
        .ok_or_else(|| AppError::Conflict(format!("logical slot '{slot_id}' is unbound")))?;
    if binding.instance_id != instance_id
        || runtime.local_instance_id != instance_id
        || runtime.local_node_id != slot_id
        || binding.binding_epoch > binding_epoch
    {
        return Err(AppError::Conflict(format!(
            "logical slot '{slot_id}' is not bound to physical instance '{instance_id}'"
        )));
    }
    Ok(runtime)
}

pub async fn peer_put_shard(
    headers: HeaderMap,
    Path((fragment, object_id, version_id, part_index, shard_index)): Path<(
        String,
        String,
        String,
        u64,
        usize,
    )>,
    Query(query): Query<PeerShardQuery>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    let runtime = validate_peer_binding(
        &query.pool_id,
        query.pool_generation,
        &query.slot_id,
        &query.instance_id,
        query.binding_epoch,
        &query.root_id,
    )?;
    let started = Instant::now();
    let is_parity = query.parity.unwrap_or(false);
    let expected_len = query.expected_len.unwrap_or(0);
    let file_name = stable_name(part_index, shard_index, is_parity);
    let opened = open_local_shard_for_write(
        &runtime,
        &query.root_id,
        &fragment,
        &object_id,
        &version_id,
        &file_name,
        expected_len,
    )
    .await?;
    let dir = opened.dir;
    let tmp_path = opened.tmp_path;
    let stable_path = opened.stable_path;
    let mut file = DirectWriter::new(opened.file);
    let mut stream = body.into_data_stream();
    let mut hasher = Sha256Stream::new();
    let mut written = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "read peer shard body".into(),
            )
        })?;
        if chunk.is_empty() {
            continue;
        }
        written = written.saturating_add(chunk.len() as u64);
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|e| AppError::Io(e, format!("write peer shard {}", tmp_path.display())))?;
    }
    file.flush()
        .await
        .map_err(|e| AppError::Io(e, format!("flush peer shard {}", tmp_path.display())))?;
    if expected_len > 0 && written != expected_len {
        drop(file);
        let _ = fs::remove_file(&tmp_path).await;
        return Err(AppError::BadRequest(format!(
            "peer shard length mismatch: got {}, expected {}",
            written, expected_len
        )));
    }
    if !query.deferred_durability {
        file.sync_data()
            .await
            .map_err(|e| AppError::Io(e, format!("sync peer shard {}", tmp_path.display())))?;
    }
    drop(file);
    if fs::try_exists(&stable_path).await.unwrap_or(false) {
        let _ = fs::remove_file(&stable_path).await;
    }
    fs::rename(&tmp_path, &stable_path).await.map_err(|e| {
        AppError::Io(
            e,
            format!(
                "publish peer shard {} -> {}",
                tmp_path.display(),
                stable_path.display()
            ),
        )
    })?;
    if !query.deferred_durability {
        crate::core::upload::sync_dir(&dir).await?;
    }
    let info = ShardInfo {
        shard_index,
        file_name,
        absolute_path: stable_path.to_string_lossy().to_string(),
        size_bytes: written,
        checksum: format_sha256(&hasher.finalize()),
        block_checksums: Vec::new(),
        checksum_block_size: 0,
        is_parity,
    };
    invalidate_local_usage_cache(Some(&fragment));
    record_io_latency("peer.local_shard_put", started.elapsed(), written, true);
    Ok((StatusCode::CREATED, Json(info)))
}

pub async fn peer_get_shard(
    headers: HeaderMap,
    Path((fragment, object_id, version_id, part_index, shard_index)): Path<(
        String,
        String,
        String,
        u64,
        usize,
    )>,
    Query(query): Query<PeerShardQuery>,
) -> Result<Response, AppError> {
    authorize_peer(&headers)?;
    let runtime = validate_peer_binding(
        &query.pool_id,
        query.pool_generation,
        &query.slot_id,
        &query.instance_id,
        query.binding_epoch,
        &query.root_id,
    )?;
    let file_name = stable_name(part_index, shard_index, query.parity.unwrap_or(false));
    let (mut file, path) = open_local_shard_for_read(
        &runtime,
        &query.root_id,
        &fragment,
        &object_id,
        &version_id,
        &file_name,
    )
    .await?;
    if let Some(length) = query.length {
        let offset = query.offset.unwrap_or(0);
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| AppError::Io(e, format!("seek peer shard {}", path.display())))?;
        let mut buf = vec![0u8; length];
        let mut read = 0usize;
        while read < length {
            let n = file.read(&mut buf[read..]).await.map_err(|e| {
                AppError::Io(e, format!("read peer shard range {}", path.display()))
            })?;
            if n == 0 {
                break;
            }
            read += n;
        }
        buf.truncate(read);
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(buf))
            .unwrap());
    }
    let stream = ReaderStream::new(file);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::from_stream(stream.map(|result| {
            result.map(Bytes::from).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("peer shard read: {e}"))
            })
        })))
        .unwrap())
}

pub async fn peer_usage(
    headers: HeaderMap,
    Query(query): Query<PeerUsageQuery>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(
        local_peer_usage_snapshot_refresh(query.fragment, query.refresh).await?,
    ))
}

pub async fn peer_storage_readiness(headers: HeaderMap) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(
        crate::core::capacity_manager::get_storage_readiness().await?,
    ))
}

pub async fn peer_monitor(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<PeerMonitorQuery>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(
        local_peer_monitor_snapshot(
            state,
            query.fragment,
            query.include_usage,
            query.refresh,
            query.deep_scan,
        )
        .await?,
    ))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NativePeerShardPayload {
    #[serde(default)]
    pub secret: Option<String>,
    pub pool_id: String,
    pub pool_generation: u64,
    pub slot_id: String,
    pub instance_id: String,
    pub binding_epoch: u64,
    pub fragment: String,
    pub root_id: String,
    pub object_id: String,
    pub version_id: String,
    pub part_index: u64,
    pub shard_index: usize,
    #[serde(default)]
    pub parity: bool,
    #[serde(default)]
    pub expected_len: Option<u64>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub length: Option<usize>,
    #[serde(default)]
    pub deferred_durability: bool,
}

fn authorize_peer_secret(secret: Option<&str>) -> Result<(), AppError> {
    let Some(expected) = cluster_secret() else {
        return Ok(());
    };
    if secret.unwrap_or_default() == expected {
        Ok(())
    } else {
        Err(AppError::Forbidden("N2 peer secret mismatch".into()))
    }
}

pub async fn native_peer_put_shard_reader<R>(
    payload: NativePeerShardPayload,
    reader: &mut R,
    body_len: u64,
) -> Result<ShardInfo, AppError>
where
    R: AsyncRead + Unpin,
{
    authorize_peer_secret(payload.secret.as_deref())?;
    let runtime = validate_peer_binding(
        &payload.pool_id,
        payload.pool_generation,
        &payload.slot_id,
        &payload.instance_id,
        payload.binding_epoch,
        &payload.root_id,
    )?;
    let expected_len = payload.expected_len.unwrap_or(body_len);
    let file_name = stable_name(payload.part_index, payload.shard_index, payload.parity);
    let opened = open_local_shard_for_write(
        &runtime,
        &payload.root_id,
        &payload.fragment,
        &payload.object_id,
        &payload.version_id,
        &file_name,
        expected_len,
    )
    .await?;
    let dir = opened.dir;
    let tmp_path = opened.tmp_path;
    let stable_path = opened.stable_path;
    let mut file = DirectWriter::new(opened.file);
    let mut hasher = Sha256Stream::new();
    let mut written = 0u64;
    let mut buf = vec![0u8; 256 * 1024];
    while written < body_len {
        let wanted = ((body_len - written) as usize).min(buf.len());
        let n = timeout(
            peer_transfer_idle_timeout(),
            reader.read(&mut buf[..wanted]),
        )
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "native peer shard body stalled while writing {}",
                tmp_path.display()
            ))
        })?
        .map_err(|e| {
            AppError::Io(
                e,
                format!("read native peer shard body {}", tmp_path.display()),
            )
        })?;
        if n == 0 {
            break;
        }
        written = written.saturating_add(n as u64);
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n]).await.map_err(|e| {
            AppError::Io(e, format!("write native peer shard {}", tmp_path.display()))
        })?;
    }
    file.flush()
        .await
        .map_err(|e| AppError::Io(e, format!("flush native peer shard {}", tmp_path.display())))?;
    if written != body_len || (expected_len > 0 && written != expected_len) {
        drop(file);
        let _ = fs::remove_file(&tmp_path).await;
        return Err(AppError::BadRequest(format!(
            "native peer shard length mismatch: got {}, body_len {}, expected {}",
            written, body_len, expected_len
        )));
    }
    if !payload.deferred_durability {
        file.sync_data().await.map_err(|e| {
            AppError::Io(e, format!("sync native peer shard {}", tmp_path.display()))
        })?;
    }
    drop(file);
    if fs::try_exists(&stable_path).await.unwrap_or(false) {
        let _ = fs::remove_file(&stable_path).await;
    }
    fs::rename(&tmp_path, &stable_path).await.map_err(|e| {
        AppError::Io(
            e,
            format!(
                "publish native peer shard {} -> {}",
                tmp_path.display(),
                stable_path.display()
            ),
        )
    })?;
    if !payload.deferred_durability {
        crate::core::upload::sync_dir(&dir).await?;
    }
    invalidate_local_usage_cache(Some(&payload.fragment));
    Ok(ShardInfo {
        shard_index: payload.shard_index,
        file_name,
        absolute_path: stable_path.to_string_lossy().to_string(),
        size_bytes: written,
        checksum: format_sha256(&hasher.finalize()),
        block_checksums: Vec::new(),
        checksum_block_size: 0,
        is_parity: payload.parity,
    })
}

pub async fn native_peer_get_shard_bytes(
    payload: NativePeerShardPayload,
) -> Result<Vec<u8>, AppError> {
    authorize_peer_secret(payload.secret.as_deref())?;
    let runtime = validate_peer_binding(
        &payload.pool_id,
        payload.pool_generation,
        &payload.slot_id,
        &payload.instance_id,
        payload.binding_epoch,
        &payload.root_id,
    )?;
    let file_name = stable_name(payload.part_index, payload.shard_index, payload.parity);
    let (mut file, path) = open_local_shard_for_read(
        &runtime,
        &payload.root_id,
        &payload.fragment,
        &payload.object_id,
        &payload.version_id,
        &file_name,
    )
    .await?;
    if let Some(length) = payload.length {
        file.seek(SeekFrom::Start(payload.offset.unwrap_or(0)))
            .await
            .map_err(|e| AppError::Io(e, format!("seek native peer shard {}", path.display())))?;
        let mut buf = vec![0u8; length];
        let mut read = 0usize;
        while read < length {
            let n = file.read(&mut buf[read..]).await.map_err(|e| {
                AppError::Io(
                    e,
                    format!("read native peer shard range {}", path.display()),
                )
            })?;
            if n == 0 {
                break;
            }
            read += n;
        }
        buf.truncate(read);
        return Ok(buf);
    }
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .await
        .map_err(|e| AppError::Io(e, format!("read native peer shard {}", path.display())))?;
    Ok(buf)
}

pub fn native_peer_bytes_response(id: String, bytes: Vec<u8>) -> NativeResponse {
    NativeResponse {
        id,
        ok: true,
        status: Some(StatusCode::OK.as_u16()),
        payload: Some(json!({"bytes": bytes.len()}).to_string()),
        body_len: Some(bytes.len() as u64),
        body: Some(ByteBuf::from(bytes)),
        error: None,
    }
}

pub fn build_peer_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n2/peer/usage", axum::routing::get(peer_usage))
        .route(
            "/n2/peer/storage-readiness",
            axum::routing::get(peer_storage_readiness),
        )
        .route("/n2/peer/monitor", axum::routing::get(peer_monitor))
        .route(
            "/n2/peer/objects/publish",
            axum::routing::post(peer_publish_object_metadata),
        )
        .route(
            "/n2/peer/objects/resolve",
            axum::routing::post(peer_resolve_object_metadata),
        )
        .route(
            "/n2/peer/objects/resolve-batch",
            axum::routing::post(peer_resolve_object_metadata_batch),
        )
        .route(
            "/n2/peer/objects/inventory",
            axum::routing::post(peer_object_inventory),
        )
        .route(
            "/n2/peer/fragments/control",
            axum::routing::post(peer_fragment_control),
        )
        .route(
            "/n2/peer/fragments/snapshot",
            axum::routing::get(peer_fragment_snapshot),
        )
        .route(
            "/n2/peer/fragments/sync",
            axum::routing::post(peer_fragment_sync),
        )
        .route(
            "/n2/peer/shards/{:fragment}/{:object_id}/{:version_id}/{:part_index}/{:shard_index}",
            put(peer_put_shard).get(peer_get_shard),
        )
}
