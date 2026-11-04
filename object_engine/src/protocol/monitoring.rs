//! Quick, read-only observations of this object's own node and member roots.
//! Never enumerates an object, upload, fingerprint directory or shard.
use super::ops::Service;
use cli_core::{monitoring::Observation, system};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Arc, time::Duration};

pub(super) async fn observe(service: Arc<Service>) -> Observation {
    let interval = Duration::from_secs(service.config.runtime.monitor_interval_seconds);
    service.observations.clone().observe(interval, move || async move {
        let system = system::get_system_metrics().await;
        let node = system::node_identity().await;
        match tokio::task::spawn_blocking(move || pools(&service)).await {
            Ok((pools, capacity, status)) => json!({"node":node,"system":system,"pools":pools,"capacity":capacity,"status":status}),
            Err(_) => json!({"node":node,"system":system,"pools":[],"capacity":null,"status":"unavailable","error":"member observer failed"}),
        }
    }).await
}
fn pools(service: &Service) -> (Vec<Value>, Value, &'static str) {
    let mut seen = HashSet::new();
    let (mut total, mut used, mut free) = (0u64, 0u64, 0u64);
    let mut all_complete = true;
    let mut pools = Vec::new();
    for (config, pool) in service.config.pools.iter().zip(&service.store.pools) {
        let mut members = Vec::new();
        let (mut readable, mut writable) = (0, 0);
        let mut smallest = u64::MAX;
        for slot in 0..pool.layout.width() {
            let mut row = json!({"slot":slot,"member_id":config.members[slot],"path":config.roots[slot],"status":"offline", "role":"object_member"});
            let probe = pool.members[slot]
                .as_ref()
                .ok_or_else(|| "configured member is unavailable".to_string())
                .and_then(|member| member.filesystem_metrics().map_err(|e| e.to_string()));
            match probe {
                Ok(stat) => {
                    readable += 1;
                    if !stat.read_only
                        && stat.available_bytes > 0
                        && stat.available_inodes != Some(0)
                    {
                        writable += 1;
                    }
                    smallest = smallest.min(stat.available_bytes);
                    if seen.insert(stat.device_id) {
                        total = total.saturating_add(stat.total_bytes);
                        used = used.saturating_add(stat.used_bytes);
                        free = free.saturating_add(stat.available_bytes);
                    }
                    let warning = if stat.available_bytes == 0 {
                        Some("no available filesystem bytes")
                    } else if stat.available_inodes == Some(0) {
                        Some("no available filesystem inodes")
                    } else {
                        None
                    };
                    row["status"] = json!(if stat.read_only { "readonly" } else { "online" });
                    row["capacity_warning"] = json!(warning);
                    let fields = serde_json::to_value(stat).expect("filesystem counters serialize");
                    row.as_object_mut()
                        .unwrap()
                        .extend(fields.as_object().unwrap().clone());
                }
                Err(error) => {
                    row["error"] = json!(error);
                    all_complete = false;
                }
            }
            members.push(row);
        }
        let readiness = if readable < pool.layout.data_shards as usize {
            "unavailable"
        } else if readable != pool.layout.width() {
            "degraded"
        } else if writable != pool.layout.width() {
            "read_only"
        } else {
            "ready"
        };
        if readiness != "ready" {
            all_complete = false;
        }
        pools.push(json!({"id":pool.layout.id,"generation":pool.layout.generation,"data_shards":pool.layout.data_shards,
            "parity_shards":pool.layout.parity_shards,"block_bytes":pool.layout.block_bytes,"sealed":config.sealed,
            "available_members":readable,"writable_members":writable,"readiness":readiness,"members":members,
            "estimated_data_available_bytes":if readable==pool.layout.width() {Some(smallest.saturating_mul(pool.layout.data_shards.into()))} else {None},
            "integrity":{"status":"not_scanned","scope":"member bindings and filesystem counters only"}}));
    }
    let write = pools
        .iter()
        .rposition(|p| p["readiness"] == "ready" && p["sealed"] == false);
    for (i, pool) in pools.iter_mut().enumerate() {
        pool["current_write_pool"] = json!(write == Some(i));
    }
    (
        pools,
        json!({"capacity_allocated":total,"capacity_used":used,"capacity_available":free,"filesystem_count":seen.len(),"complete":all_complete}),
        if all_complete { "healthy" } else { "degraded" },
    )
}
