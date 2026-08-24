use super::*;

pub async fn local_peer_usage_snapshot(
    fragment: Option<String>,
) -> Result<PeerUsageResponse, AppError> {
    local_peer_usage_snapshot_refresh(fragment, false).await
}

pub async fn local_peer_usage_snapshot_refresh(
    fragment: Option<String>,
    refresh: bool,
) -> Result<PeerUsageResponse, AppError> {
    let key = usage_cache_key(&fragment);
    let ttl = StdDuration::from_secs(usage_cache_ttl_seconds());
    if !refresh && let Ok(cache) = USAGE_CACHE.lock() {
        publish_usage_cache_size(&cache);
        if let Some(cached) = cache.get(&key)
            && cached.inserted.elapsed() < ttl
        {
            record_capacity_cache_hit();
            return Ok(cached.response.clone());
        }
    }
    record_capacity_cache_miss();
    let started = Instant::now();
    let result = collect_local_peer_usage(fragment.clone()).await;
    record_io_latency("capacity.local_usage", started.elapsed(), 0, result.is_ok());
    match result {
        Ok(response) => {
            if let Ok(mut cache) = USAGE_CACHE.lock() {
                cache.insert(
                    key,
                    CachedPeerUsage {
                        inserted: Instant::now(),
                        response: response.clone(),
                    },
                );
                publish_usage_cache_size(&cache);
            }
            record_capacity_cache_refresh();
            Ok(response)
        }
        Err(err) => {
            record_capacity_cache_failure();
            Err(err)
        }
    }
}

async fn collect_local_peer_usage(fragment: Option<String>) -> Result<PeerUsageResponse, AppError> {
    let runtime = active_cluster_runtime()?;
    let capacity = get_storage_usage().await?;
    let roots = get_storage_root_usage().await?;
    let fragment_usage = match fragment.as_deref() {
        Some(value) if !value.trim().is_empty() => Some(get_fragment_blob_usage(value).await?),
        _ => None,
    };
    Ok(PeerUsageResponse {
        node_id: runtime.local_node_id.clone(),
        capacity,
        roots,
        fragment,
        fragment_usage,
    })
}

fn usage_cache_key(fragment: &Option<String>) -> String {
    fragment
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("__cluster__")
        .to_string()
}

fn publish_usage_cache_size(cache: &HashMap<String, CachedPeerUsage>) {
    set_capacity_cache_size(cache.len(), 0);
}

pub fn invalidate_local_usage_cache(_fragment: Option<&str>) {
    if let Ok(mut cache) = USAGE_CACHE.lock() {
        cache.clear();
        publish_usage_cache_size(&cache);
    }
    record_capacity_cache_invalidation();
}

pub async fn local_peer_monitor_snapshot(
    state: Arc<AppState>,
    fragment: Option<String>,
    include_usage: bool,
    refresh: bool,
    deep_scan: bool,
) -> Result<PeerMonitorResponse, AppError> {
    let runtime = active_cluster_runtime()?;
    let claims = Claims {
        fragment: fragment.clone().unwrap_or_default(),
        fragment_alias: None,
        read: true,
        write: false,
        modify: false,
        delete: false,
        exp: usize::MAX,
    };
    let mut params = std::collections::HashMap::new();
    if let Some(fragment) = fragment.as_deref().filter(|value| !value.trim().is_empty()) {
        params.insert("fragment".to_string(), fragment.to_string());
    }
    if refresh {
        params.insert("refresh".to_string(), "true".to_string());
    }
    if deep_scan {
        params.insert("deep_scan".to_string(), "true".to_string());
    }
    let health = system_health_snapshot(state, claims, params).await?;
    let sampled_at_unix = health.sampled_at;
    let health = serde_json::to_value(health).map_err(|err| {
        AppError::InternalServerError(format!("encode N2 peer monitor snapshot: {err}"))
    })?;
    let usage = if include_usage {
        Some(local_peer_usage_snapshot_refresh(fragment, refresh).await?)
    } else {
        None
    };
    Ok(PeerMonitorResponse {
        node_id: runtime.local_node_id.clone(),
        sampled_at_unix,
        health,
        usage,
    })
}
