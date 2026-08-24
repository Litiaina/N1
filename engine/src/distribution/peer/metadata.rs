use super::*;

async fn post_object_metadata(
    node: &ClusterNode,
    binding_epoch: u64,
    metadata: &ObjectMetadata,
    sync_shards_before_publish: bool,
) -> Result<(), AppError> {
    let client = peer_http_client()?;
    let url = format!(
        "{}/n2/peer/objects/publish",
        node.endpoint.trim_end_matches('/')
    );
    let mut request = client.post(url).json(&PeerObjectMetadataRequest {
        target_slot_id: node.id.clone(),
        target_instance_id: node.instance_id.clone(),
        binding_epoch,
        metadata: metadata.clone(),
        sync_shards_before_publish,
    });
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = request
        .timeout(PEER_WRITE_TIMEOUT)
        .send()
        .await
        .map_err(|err| {
            AppError::ServiceUnavailable(format!(
                "publish N2 object metadata to {}: {err}",
                node.endpoint
            ))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "publish N2 object metadata to {} returned {status}: {body}",
            node.endpoint
        )));
    }
    Ok(())
}

fn validate_metadata_pool_contract(
    runtime: &ClusterRuntime,
    metadata: &ObjectMetadata,
) -> Result<BTreeSet<String>, AppError> {
    let pool = runtime.membership.pool(&metadata.pool_id).ok_or_else(|| {
        AppError::Conflict(format!(
            "object metadata references unknown server pool '{}'",
            metadata.pool_id
        ))
    })?;
    if pool.generation != metadata.pool_generation
        || pool.policy.object_data_shards != usize::from(metadata.data_shards)
        || pool.policy.object_parity_shards != usize::from(metadata.parity_shards)
    {
        return Err(AppError::Conflict(
            "object metadata does not match its immutable server-pool EC contract".into(),
        ));
    }
    let mut participant_ids = BTreeSet::new();
    for shard in metadata.parts.iter().flat_map(|part| part.shards.iter()) {
        let locator = parse_peer_shard_locator(&shard.absolute_path).ok_or_else(|| {
            AppError::InternalServerError(format!(
                "N2 committed shard {} has no native placement locator",
                shard.file_name
            ))
        })?;
        if !pool
            .node_ids
            .iter()
            .any(|node_id| node_id == &locator.node_id)
        {
            return Err(AppError::Conflict(format!(
                "object shard references node '{}' outside pool '{}'",
                locator.node_id, pool.id
            )));
        }
        participant_ids.insert(locator.node_id);
    }
    if participant_ids.is_empty() {
        return Err(AppError::InternalServerError(
            "N2 object metadata has no shard-bearing participants".into(),
        ));
    }
    let required_width = pool.policy.object_width();
    if participant_ids.len() != required_width {
        return Err(AppError::Conflict(format!(
            "object metadata participants do not match pool '{}' static {}+{} layout",
            pool.id, pool.policy.object_data_shards, pool.policy.object_parity_shards
        )));
    }
    for part in &metadata.parts {
        let mut part_nodes = BTreeSet::new();
        let mut shard_indices = BTreeSet::new();
        for shard in &part.shards {
            let locator = parse_peer_shard_locator(&shard.absolute_path).ok_or_else(|| {
                AppError::Conflict(format!(
                    "object part {} shard '{}' has no exact peer locator",
                    part.part_index, shard.file_name
                ))
            })?;
            part_nodes.insert(locator.node_id);
            shard_indices.insert(shard.shard_index);
        }
        if part.shards.len() != required_width
            || part_nodes != participant_ids
            || shard_indices != (0..required_width).collect::<BTreeSet<_>>()
        {
            return Err(AppError::Conflict(format!(
                "object part {} does not contain exactly one indexed shard on each of {} pool participants",
                part.part_index, required_width
            )));
        }
    }
    Ok(participant_ids)
}

async fn sync_local_object_shards(
    runtime: &ClusterRuntime,
    metadata: &ObjectMetadata,
) -> Result<(), AppError> {
    let mut directories = BTreeSet::new();
    for shard in metadata.parts.iter().flat_map(|part| part.shards.iter()) {
        let locator = parse_peer_shard_locator(&shard.absolute_path).ok_or_else(|| {
            AppError::Conflict(format!(
                "standard multipart shard '{}' has no exact N2 locator",
                shard.file_name
            ))
        })?;
        if locator.node_id != runtime.local_node_id {
            continue;
        }
        let (file, path) = open_local_shard_for_read(
            runtime,
            &locator.root_id,
            &locator.fragment,
            &locator.object_id,
            &locator.version_id,
            &shard.file_name,
        )
        .await?;
        let actual = file
            .metadata()
            .await
            .map_err(|error| AppError::Io(error, format!("stat shard {}", path.display())))?
            .len();
        if actual != shard.size_bytes {
            return Err(AppError::Conflict(format!(
                "standard multipart shard '{}' has {} bytes, expected {}",
                shard.file_name, actual, shard.size_bytes
            )));
        }
        file.sync_data()
            .await
            .map_err(|error| AppError::Io(error, format!("sync shard {}", path.display())))?;
        if let Some(parent) = path.parent() {
            directories.insert(parent.to_path_buf());
        }
    }
    for directory in directories {
        crate::core::upload::sync_dir(&directory).await?;
    }
    Ok(())
}

pub async fn publish_cluster_object_metadata(metadata: &ObjectMetadata) -> Result<(), AppError> {
    publish_cluster_object_metadata_mode(metadata, false).await
}

/// Standard multipart's single durable commit boundary. Every participant
/// fsyncs its already-written shard files before it publishes the immutable
/// NCDX replica. NCNX publication remains the caller's final visibility step.
pub async fn publish_cluster_object_metadata_after_shard_sync(
    metadata: &ObjectMetadata,
) -> Result<(), AppError> {
    publish_cluster_object_metadata_mode(metadata, true).await
}

async fn publish_cluster_object_metadata_mode(
    metadata: &ObjectMetadata,
    sync_shards_before_publish: bool,
) -> Result<(), AppError> {
    let runtime = active_cluster_runtime()?;
    let participant_ids = validate_metadata_pool_contract(&runtime, metadata)?;

    let required_metadata_writes = usize::from(metadata.data_shards);
    let mut tasks = futures_util::stream::FuturesUnordered::new();
    for node_id in participant_ids {
        let node = runtime
            .membership
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .cloned()
            .ok_or_else(|| {
                AppError::InternalServerError(format!(
                    "N2 shard-bearing participant {node_id} is absent from membership"
                ))
            })?;
        let metadata = metadata.clone();
        let local_node_id = runtime.local_node_id.clone();
        let binding_epoch = runtime.membership.binding_epoch;
        let runtime = runtime.clone();
        tasks.push(async move {
            let participant = node_id.clone();
            if node_id == local_node_id {
                if sync_shards_before_publish {
                    sync_local_object_shards(&runtime, &metadata).await?;
                }
                index::publish_object(&metadata).await.map(|_| participant)
            } else {
                post_object_metadata(&node, binding_epoch, &metadata, sync_shards_before_publish)
                    .await
                    .map(|_| participant)
            }
        });
    }

    let mut successes = 0usize;
    let mut failures = Vec::new();
    while let Some(result) = tasks.next().await {
        match result {
            Ok(_) => successes = successes.saturating_add(1),
            Err(err) => failures.push(err.to_string()),
        }
    }
    if successes < required_metadata_writes {
        return Err(AppError::ServiceUnavailable(format!(
            "survival NCDX publication failed for {}/{} version {}: {} successful, K={} required; {}",
            metadata.fragment,
            metadata.object_key,
            metadata.version_id,
            successes,
            required_metadata_writes,
            failures.join("; ")
        )));
    }
    Ok(())
}

fn object_metadata_bytes(metadata: &ObjectMetadata) -> Result<Vec<u8>, AppError> {
    rmp_serde::to_vec_named(metadata).map_err(AppError::from)
}

fn metadata_participant_nodes(metadata: &ObjectMetadata) -> Result<BTreeSet<String>, AppError> {
    let mut nodes = BTreeSet::new();
    for shard in metadata.parts.iter().flat_map(|part| part.shards.iter()) {
        let locator = parse_peer_shard_locator(&shard.absolute_path).ok_or_else(|| {
            AppError::Conflict(format!(
                "object metadata shard '{}' has no exact peer locator",
                shard.file_name
            ))
        })?;
        nodes.insert(locator.node_id);
    }
    if nodes.is_empty() && metadata.total_size_bytes != 0 {
        return Err(AppError::Conflict(
            "non-empty object metadata has no shard-bearing participants".into(),
        ));
    }
    Ok(nodes)
}

async fn metadata_on_node(
    runtime: &ClusterRuntime,
    node: &ClusterNode,
    metadata: &ObjectMetadata,
) -> Result<Option<ObjectMetadata>, AppError> {
    let request = PeerObjectResolveRequest {
        fragment: metadata.fragment.clone(),
        object_id: metadata.object_key.clone(),
        version_id: Some(metadata.version_id.clone()),
    };
    let records = if node.id == runtime.local_node_id {
        local_object_metadata(&request)?
    } else {
        post_object_resolve(&node.endpoint, &request).await?
    };
    Ok(records.into_iter().next())
}

async fn repair_metadata_on_node(
    runtime: &ClusterRuntime,
    node: &ClusterNode,
    metadata: &ObjectMetadata,
) -> Result<(), AppError> {
    if node.id == runtime.local_node_id {
        index::publish_object(metadata).await.map(|_| ())
    } else {
        post_object_metadata(node, runtime.membership.binding_epoch, metadata, false).await
    }
}

/// Verify and restore the immutable NCDX metadata copy on every exact
/// shard-bearing participant. Divergent immutable metadata is never replaced.
pub async fn inspect_and_repair_metadata_replicas(
    metadata: &ObjectMetadata,
) -> Result<MetadataReplicaRepairOutcome, AppError> {
    let runtime = active_cluster_runtime()?;
    let participant_ids = metadata_participant_nodes(metadata)?;
    let expected_bytes = object_metadata_bytes(metadata)?;
    let mut observations = Vec::with_capacity(participant_ids.len());
    let mut missing = Vec::new();
    for node_id in &participant_ids {
        let node = runtime
            .membership
            .nodes
            .iter()
            .find(|node| node.id == *node_id)
            .ok_or_else(|| {
                AppError::Conflict(format!(
                    "object metadata participant '{node_id}' is absent from membership"
                ))
            })?;
        match metadata_on_node(&runtime, node, metadata).await {
            Ok(Some(found)) if object_metadata_bytes(&found)? == expected_bytes => {
                observations.push(MetadataReplicaObservation {
                    node_id: node_id.clone(),
                    status: "healthy".into(),
                    error: None,
                });
            }
            Ok(Some(_)) => observations.push(MetadataReplicaObservation {
                node_id: node_id.clone(),
                status: "divergent".into(),
                error: Some("immutable NCDX metadata differs".into()),
            }),
            Ok(None) => {
                observations.push(MetadataReplicaObservation {
                    node_id: node_id.clone(),
                    status: "missing".into(),
                    error: None,
                });
                missing.push(node.clone());
            }
            Err(error) => {
                observations.push(MetadataReplicaObservation {
                    node_id: node_id.clone(),
                    status: "unavailable".into(),
                    error: Some(error.to_string()),
                });
                missing.push(node.clone());
            }
        }
    }

    let mut repaired_replicas = 0u64;
    let mut repaired_nodes = BTreeSet::new();
    for node in missing {
        if repair_metadata_on_node(&runtime, &node, metadata)
            .await
            .is_ok()
        {
            repaired_replicas = repaired_replicas.saturating_add(1);
            repaired_nodes.insert(node.id.clone());
        }
    }

    let mut healthy = true;
    let mut final_observations = Vec::with_capacity(participant_ids.len());
    for node_id in participant_ids {
        let node = runtime
            .membership
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .expect("metadata participant membership checked");
        match metadata_on_node(&runtime, node, metadata).await {
            Ok(Some(found)) if object_metadata_bytes(&found)? == expected_bytes => {
                final_observations.push(MetadataReplicaObservation {
                    status: if repaired_nodes.contains(&node_id) {
                        "repaired"
                    } else {
                        "healthy"
                    }
                    .into(),
                    node_id,
                    error: None,
                });
            }
            Ok(Some(_)) => {
                healthy = false;
                final_observations.push(MetadataReplicaObservation {
                    node_id,
                    status: "divergent".into(),
                    error: Some("immutable NCDX metadata differs after repair".into()),
                });
            }
            Ok(None) => {
                healthy = false;
                final_observations.push(MetadataReplicaObservation {
                    node_id,
                    status: "missing".into(),
                    error: Some("NCDX metadata remained missing after repair".into()),
                });
            }
            Err(error) => {
                healthy = false;
                final_observations.push(MetadataReplicaObservation {
                    node_id,
                    status: "unavailable".into(),
                    error: Some(error.to_string()),
                });
            }
        }
    }

    Ok(MetadataReplicaRepairOutcome {
        expected_replicas: final_observations.len(),
        repaired_replicas,
        healthy,
        observations: final_observations,
    })
}

async fn post_object_inventory(
    endpoint: &str,
    fragment: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let client = peer_http_client()?;
    let mut request = client
        .post(format!(
            "{}/n2/peer/objects/inventory",
            endpoint.trim_end_matches('/')
        ))
        .json(&PeerObjectInventoryRequest {
            fragment: fragment.to_string(),
        });
    if let Some(secret) = cluster_secret() {
        request = request.header("x-n2-cluster-secret", secret);
    }
    let response = request
        .timeout(PEER_READ_TIMEOUT)
        .send()
        .await
        .map_err(|error| {
            AppError::ServiceUnavailable(format!(
                "read N2 object inventory from {endpoint}: {error}"
            ))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "N2 object inventory peer {endpoint} returned {}",
            response.status()
        )));
    }
    response.json().await.map_err(|error| {
        AppError::InternalServerError(format!(
            "decode N2 object inventory from {endpoint}: {error}"
        ))
    })
}

pub async fn collect_cluster_object_inventory(
    fragment: &str,
) -> Result<ClusterObjectInventory, AppError> {
    let runtime = active_cluster_runtime()?;
    let nodes = runtime.membership.nodes.clone();
    let expected_blob_nodes = nodes.len();
    let local_id = runtime.local_node_id.clone();
    let results = futures_util::future::join_all(nodes.into_iter().map(|node| {
        let fragment = fragment.to_string();
        let local_id = local_id.clone();
        async move {
            let result = if node.id == local_id {
                index::list_fragment_live_objects(&fragment).await
            } else {
                post_object_inventory(&node.endpoint, &fragment).await
            };
            (node.id, result)
        }
    }))
    .await;
    let mut inventory = ClusterObjectInventory {
        expected_blob_nodes,
        ..ClusterObjectInventory::default()
    };
    for (node_id, result) in results {
        let records = match result {
            Ok(records) => {
                inventory.responding_blob_nodes = inventory.responding_blob_nodes.saturating_add(1);
                records
            }
            Err(error) => {
                inventory.errors.push(format!("{node_id}: {error}"));
                continue;
            }
        };
        for record in records {
            match inventory.objects.get(&record.object_key) {
                Some(existing) if existing.version_id == record.version_id => {
                    if object_metadata_bytes(existing)? != object_metadata_bytes(&record)? {
                        return Err(AppError::Conflict(format!(
                            "divergent immutable NCDX inventory for object {} version {}",
                            record.object_key, record.version_id
                        )));
                    }
                }
                Some(existing) if existing.version_id > record.version_id => {}
                _ => {
                    inventory.objects.insert(record.object_key.clone(), record);
                }
            }
        }
    }
    Ok(inventory)
}

pub async fn peer_publish_object_metadata(
    headers: HeaderMap,
    Json(request): Json<PeerObjectMetadataRequest>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    if runtime.local_is_reserve
        || request.binding_epoch != runtime.membership.binding_epoch
        || request.target_slot_id != runtime.local_node_id
        || request.target_instance_id != runtime.local_instance_id
    {
        return Err(AppError::Conflict(
            "stale or misbound NCDX publication request".into(),
        ));
    }
    validate_metadata_pool_contract(&runtime, &request.metadata)?;
    if request.sync_shards_before_publish {
        sync_local_object_shards(&runtime, &request.metadata).await?;
    }
    index::publish_object(&request.metadata).await?;
    Ok(StatusCode::CREATED)
}

pub async fn peer_object_inventory(
    headers: HeaderMap,
    Json(request): Json<PeerObjectInventoryRequest>,
) -> Result<Json<Vec<ObjectMetadata>>, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(
        index::list_fragment_live_objects(&request.fragment).await?,
    ))
}

fn local_object_metadata(
    request: &PeerObjectResolveRequest,
) -> Result<Vec<ObjectMetadata>, AppError> {
    if let Some(version_id) = request.version_id.as_deref() {
        return Ok(index_store::read_live_version(
            &request.fragment,
            &request.object_id,
            version_id,
        )?
        .into_iter()
        .collect());
    }
    index_store::list_live_versions(&request.fragment, &request.object_id)
}

fn local_object_metadata_heads(
    request: &PeerObjectBatchResolveRequest,
) -> Result<Vec<ObjectMetadata>, AppError> {
    if request.object_ids.len() > 10_000 {
        return Err(AppError::BadRequest(
            "object metadata batch exceeds 10000 ids".into(),
        ));
    }
    let mut out = Vec::new();
    for object_id in &request.object_ids {
        if let Some(metadata) = index_store::read_current_live_head(&request.fragment, object_id)? {
            out.push(metadata);
        }
    }
    Ok(out)
}

async fn post_object_resolve(
    endpoint: &str,
    request: &PeerObjectResolveRequest,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let client = peer_http_client()?;
    let url = format!("{}/n2/peer/objects/resolve", endpoint.trim_end_matches('/'));
    let mut builder = client.post(url).json(request);
    if let Some(secret) = cluster_secret() {
        builder = builder.header("x-n2-cluster-secret", secret);
    }
    let response = builder
        .timeout(PEER_READ_TIMEOUT)
        .send()
        .await
        .map_err(|err| {
            AppError::ServiceUnavailable(format!(
                "resolve N2 object metadata from {endpoint}: {err}"
            ))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "resolve N2 object metadata from {endpoint} returned {}",
            response.status()
        )));
    }
    response.json::<Vec<ObjectMetadata>>().await.map_err(|err| {
        AppError::InternalServerError(format!("decode N2 object metadata from {endpoint}: {err}"))
    })
}

async fn post_object_batch_resolve(
    endpoint: &str,
    request: &PeerObjectBatchResolveRequest,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let client = peer_http_client()?;
    let url = format!(
        "{}/n2/peer/objects/resolve-batch",
        endpoint.trim_end_matches('/')
    );
    let mut builder = client.post(url).json(request);
    if let Some(secret) = cluster_secret() {
        builder = builder.header("x-n2-cluster-secret", secret);
    }
    let response = builder
        .timeout(PEER_READ_TIMEOUT)
        .send()
        .await
        .map_err(|err| {
            AppError::ServiceUnavailable(format!(
                "resolve N2 object metadata batch from {endpoint}: {err}"
            ))
        })?;
    if !response.status().is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "resolve N2 object metadata batch from {endpoint} returned {}",
            response.status()
        )));
    }
    response.json::<Vec<ObjectMetadata>>().await.map_err(|err| {
        AppError::InternalServerError(format!(
            "decode N2 object metadata batch from {endpoint}: {err}"
        ))
    })
}

fn merge_immutable_object_record(
    by_object: &mut BTreeMap<String, ObjectMetadata>,
    record: ObjectMetadata,
) -> Result<(), AppError> {
    let object_id = record.object_key.clone();
    match by_object.get(&object_id) {
        Some(existing)
            if existing.version_id == record.version_id
                && object_metadata_bytes(existing)? != object_metadata_bytes(&record)? =>
        {
            Err(AppError::InternalServerError(format!(
                "divergent immutable N2 metadata for object {} version {}",
                object_id, record.version_id
            )))
        }
        Some(existing) if existing.version_id >= record.version_id => Ok(()),
        _ => {
            by_object.insert(object_id, record);
            Ok(())
        }
    }
}

pub(super) fn exact_metadata_node_batches(
    runtime: &ClusterRuntime,
    fragment: &str,
    object_ids: &[String],
) -> Vec<(ClusterNode, Vec<String>)> {
    let readable = runtime.membership.readable_nodes();
    if readable.is_empty() {
        return Vec::new();
    }
    let readable_by_id = readable
        .into_iter()
        .map(|node| (node.id.clone(), node))
        .collect::<BTreeMap<_, _>>();
    let mut per_node = BTreeMap::<String, Vec<String>>::new();

    for object_id in object_ids {
        for pool in &runtime.membership.pools {
            let nodes = runtime.membership.nodes_for_pool(&pool.id);
            let layout = ClusterEcLayout {
                data_shards: pool.policy.object_data_shards,
                parity_shards: pool.policy.object_parity_shards,
            };
            if let Ok(plan) = build_placement_plan(
                fragment,
                object_id,
                "",
                &pool.id,
                pool.generation,
                runtime.membership.epoch,
                runtime.membership.binding_epoch,
                layout,
                &nodes,
            ) {
                for placement in plan.placements {
                    if readable_by_id.contains_key(&placement.node_id) {
                        per_node
                            .entry(placement.node_id)
                            .or_default()
                            .push(object_id.clone());
                    }
                }
            }
        }
    }

    per_node
        .into_iter()
        .filter_map(|(node_id, ids)| {
            readable_by_id
                .get(&node_id)
                .cloned()
                .map(|node| (node, ids))
        })
        .collect()
}

async fn resolve_object_heads_from_batches(
    runtime: &ClusterRuntime,
    fragment: &str,
    batches: Vec<(ClusterNode, Vec<String>)>,
) -> Result<(BTreeMap<String, ObjectMetadata>, BTreeSet<(String, String)>), AppError> {
    let mut tasks = futures_util::stream::FuturesUnordered::new();
    for (node, object_ids) in batches {
        if object_ids.is_empty() {
            continue;
        }
        let request = PeerObjectBatchResolveRequest {
            fragment: fragment.to_string(),
            object_ids,
        };
        let local_node_id = runtime.local_node_id.clone();
        tasks.push(async move {
            let result = if node.id == local_node_id {
                local_object_metadata_heads(&request)
            } else {
                post_object_batch_resolve(&node.endpoint, &request).await
            };
            (node, request.object_ids, result)
        });
    }

    let mut by_object = BTreeMap::new();
    let mut proven = BTreeSet::new();
    while let Some((node, object_ids, result)) = tasks.next().await {
        let Ok(records) = result else {
            continue;
        };
        let pool_id = runtime
            .membership
            .pool_for_node(&node.id)
            .map(|pool| pool.id.clone())
            .ok_or_else(|| {
                AppError::Conflict(format!(
                    "metadata source node '{}' has no server pool",
                    node.id
                ))
            })?;
        for object_id in object_ids {
            proven.insert((pool_id.clone(), object_id));
        }
        for record in records {
            validate_metadata_pool_contract(runtime, &record)?;
            merge_immutable_object_record(&mut by_object, record)?;
        }
    }
    Ok((by_object, proven))
}

pub(super) fn ensure_exact_head_visibility(
    runtime: &ClusterRuntime,
    object_ids: &[String],
    proven: &BTreeSet<(String, String)>,
) -> Result<(), AppError> {
    let unproven = runtime
        .membership
        .pools
        .iter()
        .flat_map(|pool| {
            object_ids
                .iter()
                .map(move |object_id| (pool.id.clone(), object_id.clone()))
        })
        .filter(|pair| !proven.contains(pair))
        .count();
    if unproven > 0 {
        return Err(AppError::ServiceUnavailable(format!(
            "exact NCDX visibility is unproven for {unproven} server-pool/object pair(s)"
        )));
    }
    Ok(())
}

pub(super) fn ensure_exact_pool_visibility(
    runtime: &ClusterRuntime,
    proven_pools: &BTreeSet<String>,
) -> Result<(), AppError> {
    if runtime
        .membership
        .pools
        .iter()
        .any(|pool| !proven_pools.contains(&pool.id))
    {
        return Err(AppError::ServiceUnavailable(
            "exact NCDX visibility is unproven for one or more server pools".into(),
        ));
    }
    Ok(())
}

pub async fn resolve_cluster_object_heads(
    fragment: &str,
    object_ids: &[String],
) -> Result<BTreeMap<String, ObjectMetadata>, AppError> {
    if object_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    if object_ids.len() > 10_000 {
        return Err(AppError::BadRequest(
            "object metadata batch exceeds 10000 ids".into(),
        ));
    }
    let request = PeerObjectBatchResolveRequest {
        fragment: fragment.to_string(),
        object_ids: object_ids.to_vec(),
    };
    let Some(runtime) = crate::distribution::membership::try_active_cluster_runtime() else {
        return local_object_metadata_heads(&request).map(|records| {
            records
                .into_iter()
                .map(|record| (record.object_key.clone(), record))
                .collect()
        });
    };

    let exact = exact_metadata_node_batches(&runtime, fragment, object_ids);
    let (by_object, proven) = resolve_object_heads_from_batches(&runtime, fragment, exact).await?;
    ensure_exact_head_visibility(&runtime, object_ids, &proven)?;
    Ok(by_object)
}

async fn resolve_object_versions_from_nodes(
    runtime: &ClusterRuntime,
    request: &PeerObjectResolveRequest,
    nodes: Vec<ClusterNode>,
) -> Result<(BTreeMap<String, ObjectMetadata>, BTreeSet<String>), AppError> {
    let mut tasks = futures_util::stream::FuturesUnordered::new();
    for node in nodes {
        let request = request.clone();
        let local_node_id = runtime.local_node_id.clone();
        tasks.push(async move {
            let result = if node.id == local_node_id {
                local_object_metadata(&request)
            } else {
                post_object_resolve(&node.endpoint, &request).await
            };
            (node, result)
        });
    }

    let mut by_version = BTreeMap::<String, ObjectMetadata>::new();
    let mut proven_pools = BTreeSet::new();
    while let Some((node, result)) = tasks.next().await {
        let Ok(records) = result else {
            continue;
        };
        let pool_id = runtime
            .membership
            .pool_for_node(&node.id)
            .map(|pool| pool.id.clone())
            .ok_or_else(|| {
                AppError::Conflict(format!(
                    "metadata source node '{}' has no server pool",
                    node.id
                ))
            })?;
        proven_pools.insert(pool_id);
        for record in records {
            validate_metadata_pool_contract(runtime, &record)?;
            match by_version.get(&record.version_id) {
                Some(existing)
                    if object_metadata_bytes(existing)? != object_metadata_bytes(&record)? =>
                {
                    return Err(AppError::InternalServerError(format!(
                        "divergent immutable N2 metadata for object {} version {}",
                        request.object_id, record.version_id
                    )));
                }
                Some(_) => {}
                None => {
                    by_version.insert(record.version_id.clone(), record);
                }
            }
        }
    }
    Ok((by_version, proven_pools))
}

pub async fn resolve_cluster_object_versions(
    fragment: &str,
    object_id: &str,
) -> Result<Vec<ObjectMetadata>, AppError> {
    let request = PeerObjectResolveRequest {
        fragment: fragment.to_string(),
        object_id: object_id.to_string(),
        version_id: None,
    };
    let Some(runtime) = crate::distribution::membership::try_active_cluster_runtime() else {
        return local_object_metadata(&request);
    };
    let exact_nodes = exact_metadata_node_batches(&runtime, fragment, &[object_id.to_string()])
        .into_iter()
        .map(|(node, _)| node)
        .collect::<Vec<_>>();
    let (by_version, proven_pools) =
        resolve_object_versions_from_nodes(&runtime, &request, exact_nodes).await?;
    ensure_exact_pool_visibility(&runtime, &proven_pools)?;
    Ok(by_version.into_values().collect())
}

pub async fn resolve_cluster_object_version(
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> Result<Option<ObjectMetadata>, AppError> {
    let request = PeerObjectResolveRequest {
        fragment: fragment.to_string(),
        object_id: object_id.to_string(),
        version_id: Some(version_id.to_string()),
    };
    let Some(runtime) = crate::distribution::membership::try_active_cluster_runtime() else {
        return Ok(local_object_metadata(&request)?.into_iter().next());
    };

    let exact_nodes = exact_metadata_node_batches(&runtime, fragment, &[object_id.to_string()])
        .into_iter()
        .map(|(node, _)| node)
        .collect::<Vec<_>>();
    let (mut by_version, proven_pools) =
        resolve_object_versions_from_nodes(&runtime, &request, exact_nodes).await?;
    ensure_exact_pool_visibility(&runtime, &proven_pools)?;
    Ok(by_version.remove(version_id))
}

pub async fn peer_resolve_object_metadata(
    headers: HeaderMap,
    Json(request): Json<PeerObjectResolveRequest>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(local_object_metadata(&request)?))
}

pub async fn peer_resolve_object_metadata_batch(
    headers: HeaderMap,
    Json(request): Json<PeerObjectBatchResolveRequest>,
) -> Result<impl IntoResponse, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(local_object_metadata_heads(&request)?))
}
