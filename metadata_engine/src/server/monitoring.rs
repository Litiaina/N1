//! The metadata host and object host are independent observations. This layer
//! never opens an object member path, walks the namespace or reads a payload.
use super::App;
use cli_core::{monitoring::Observation, system};
use n1_link::{PoolId, noa::Command};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

pub(super) async fn metadata(app: &Arc<App>) -> Observation {
    let cache = app.metadata_observations.clone();
    let engine = app.db.observation_handle();
    let data = app.config.data_dir.clone();
    let staging = app.config.staging_dir.clone();
    let interval = Duration::from_secs(app.config.monitor_interval_seconds);
    cache
        .observe(interval, move || async move {
            let system = system::get_system_metrics().await;
            let node = system::node_identity().await;
            let storage = tokio::task::spawn_blocking(move || {
                let disk = |path: &std::path::Path, role| match system::filesystem_at(path) {
                    Ok(stat) => {
                        let mut value =
                            serde_json::to_value(&stat).expect("filesystem counters serialize");
                        value["path"] = json!(path);
                        value["role"] = json!(role);
                        value["status"] = json!(if stat.read_only { "readonly" } else { "online" });
                        value
                    }
                    Err(error) => json!({
                        "path": path,
                        "role": role,
                        "status": "offline",
                        "error": error.to_string()
                    }),
                };
                let db_bytes = engine.disk_bytes();
                json!({
                    "database_bytes": db_bytes.as_ref().ok(),
                    "database_error": db_bytes.err().map(|e|e.to_string()),
                    "disks": [disk(&data,"metadata_database"),disk(&staging,"metadata_staging")]
                })
            })
            .await;
            match storage {
                Ok(storage) => json!({
                    "node": node,
                    "system": system,
                    "storage": storage,
                    "engine": "m1_storage",
                    "database": "M1"
                }),
                Err(_) => json!({
                    "node": node,
                    "system": system,
                    "error": "M1 observer failed"
                }),
            }
        })
        .await
}
pub(super) async fn object(app: &Arc<App>) -> Observation {
    let observed = object_sample(app, app.config.monitor_interval_seconds).await;
    // A nested N1 cold sample is not a completed health verdict. Retry only
    // that warm-up state, under the same cache admission and a five-second
    // minimum interval. Healthy and unavailable samples retain operator policy.
    if !observed.refreshing
        && observed
            .value
            .as_ref()
            .is_some_and(|v| v["status"] == "pending")
    {
        return object_sample(app, app.config.monitor_interval_seconds.min(5)).await;
    }
    observed
}

async fn object_sample(app: &Arc<App>, interval_seconds: u64) -> Observation {
    let client = app.monitor_objects.clone();
    app.object_observations
        .clone()
        .observe(Duration::from_secs(interval_seconds), move || async move {
            // N1's own cold observation can wait two seconds. Give that
            // response time to arrive instead of racing an equal timeout.
            match tokio::time::timeout(Duration::from_secs(5), client.control(Command::Health))
                .await
            {
                Ok(Ok(value)) => value,
                Ok(Err(error)) => json!({"status": "unavailable", "error": error.to_string()}),
                Err(_) => json!({
                    "status": "unavailable",
                    "error": "N1 monitoring request timed out"
                }),
            }
        })
        .await
}
fn component(observed: &Observation, role: &str, endpoint: String) -> Value {
    let mut value = observed
        .value
        .clone()
        .unwrap_or_else(|| json!({"status":"pending"}));
    value["component"] = json!(role);
    value["endpoint"] = json!(endpoint);
    if value.get("observation").is_some() {
        value["node_observation"] = value["observation"].clone();
    }
    value["observation"] = json!(observed);
    if observed
        .age_seconds
        .is_some_and(|age| age > observed.interval_seconds.saturating_mul(2))
    {
        value["status"] = json!("stale");
    }
    value
}
pub(super) async fn health(app: &Arc<App>, fragment: Option<PoolId>) -> Value {
    let (meta, objects) = tokio::join!(metadata(app), object(app));
    let scheme = if app.config.cert_path.is_some() {
        "https"
    } else {
        "http"
    };
    let mut meta = component(
        &meta,
        "metadata_engine",
        format!("{scheme}://{}", app.config.http),
    );
    meta["product"] = json!("M1");
    meta["runtime"] = json!({
        "metadata_workers": app.config.metadata_workers,
        "active_upload_requests": app.config.upload_concurrency.saturating_sub(app.upload_slots.available_permits()),
        "upload_request_limit": app.config.upload_concurrency,
        "upload_memory_in_use_bytes": app.config.upload_memory_budget.div_ceil(65536).saturating_sub(app.upload_memory.available_permits()).saturating_mul(65536),
        "upload_memory_budget_bytes": app.config.upload_memory_budget,
        "backup_job": app.backups.state.lock().unwrap_or_else(|e|e.into_inner()).clone(),
        "recovery_warnings": app.recovery_errors.lock().unwrap_or_else(|e|e.into_inner()).clone()
    });
    let mut object = component(
        &objects,
        "object_engine",
        app.config.object_native_url.clone(),
    );
    object["product"] = json!("N1");
    let mut disks = meta["storage"]["disks"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let metadata_ready = meta["status"] != "stale"
        && meta["error"].is_null()
        && meta["storage"]["database_bytes"].is_number()
        && disks.iter().all(|d| {
            d["status"] == "online"
                && d["available_bytes"].as_u64().is_some_and(|n| n > 0)
                && d["available_inodes"] != 0
        });
    let metadata_observation_state = meta["status"].as_str().unwrap_or("unknown").to_owned();
    meta["status"] = json!(if metadata_ready {
        "healthy"
    } else if matches!(metadata_observation_state.as_str(), "pending" | "stale") {
        metadata_observation_state.as_str()
    } else {
        "degraded"
    });
    let mut available = 0;
    let mut configured = 0;
    let mut writable = 0;
    let mut devices = std::collections::HashSet::new();
    for pool in object["pools"].as_array().into_iter().flatten() {
        configured +=
            pool["data_shards"].as_u64().unwrap_or(0) + pool["parity_shards"].as_u64().unwrap_or(0);
        available += pool["available_members"].as_u64().unwrap_or(0);
        writable += pool["writable_members"].as_u64().unwrap_or(0);
        for member in pool["members"].as_array().into_iter().flatten() {
            if member["status"] == "online"
                && let Some(id) = member["device_id"].as_u64()
            {
                devices.insert(id);
            }
            disks.push(member.clone());
        }
    }
    let status = overall_status(
        meta["status"].as_str().unwrap_or("unknown"),
        object["status"].as_str().unwrap_or("unknown"),
    );
    let capacity = &object["capacity"];
    let mut maintenance = object["maintenance"].clone();
    if !maintenance.is_object() {
        maintenance = json!({});
    }
    let mode = match maintenance["access_mode"].as_str() {
        Some("ReadOnly") => "read_only",
        Some("Quiesced") => "quiesced",
        Some("Availability") => "availability",
        _ => "unknown",
    };
    maintenance["access_mode"] = json!(mode);
    let schedule = &object["configuration"]["scheduled_maintenance"];
    let scrub_scheduled = !schedule["scrub"].is_null() || !schedule["calendar_scrub"].is_null();
    maintenance["auto_healing"] = json!({
        "enabled": scrub_scheduled,
        "phase": if scrub_scheduled {
            "scheduled"
        }else{
            "topology_only"
        },
        "write_safe": object["maintenance"]["write_safe"],
        "read_only_reason": object["maintenance"]["read_only_reason"],
        "configured_blob_roots": configured,
        "writable_blob_roots": writable,
        "distinct_writable_blob_devices": devices.len(),
        "scheduled_scrub": object["configuration"]["scheduled_maintenance"],
        "note": "Read repair is separate; scrub and repair run only on request or configured schedule."
    });
    let job = &object["job"];
    let running = matches!(job["phase"].as_str(), Some("running" | "stopping"));
    maintenance["repair_running"] = json!(running && job["kind"] == "scrub");
    maintenance["repair_phase"] = json!(if job["kind"] == "scrub" {
        job["phase"].as_str().unwrap_or("idle")
    } else {
        "idle"
    });
    maintenance["gc_running"] = json!(running && job["kind"] == "gc");
    maintenance["gc_phase"] = json!(if job["kind"] == "gc" {
        job["phase"].as_str().unwrap_or("idle")
    } else {
        "idle"
    });
    for kind in [
        crate::maintenance::Kind::Gc,
        crate::maintenance::Kind::Repair,
        crate::maintenance::Kind::Redistribution,
    ] {
        if let Ok(state) = app.maintenance.status(kind, fragment).await {
            maintenance[format!("{}_running", kind.name())] = state["running"].clone();
            maintenance[format!("{}_phase", kind.name())] = state["phase"].clone();
            maintenance[format!("{}_job", kind.name())] = state;
        }
    }
    json!({
        "sampled_at": cli_core::monitoring::now(),
        "status": status,
        "summary": format!(
            "M1: {}. N1: {}. Cached read-only node and pool counters; payload integrity is not scanned.",
            meta["status"].as_str().unwrap_or("unknown"),
            object["status"].as_str().unwrap_or("pending")
        ),
        "components": {
            "metadata": meta,
            "object": object
        },
        "system": meta["system"],
        "disks": disks,
        "storage": {
            "blob_devices_total": configured,
            "blob_devices_online": available,
            "raw_total_bytes": capacity["capacity_allocated"],
            "raw_used_bytes": capacity["capacity_used"],
            "raw_available_bytes": capacity["capacity_available"],
            "complete": capacity["complete"]
        },
        "maintenance": maintenance,
        "io_control": object["io"],
        "io": object["io"],
        "capacity": object["capacity"],
        "job": object["job"],
        "operations": object["operations"],
        "configuration": object["configuration"],
        "consistency": {
            "status": "not_scanned",
            "total_objects_scanned": null
        },
        "monitoring": {
            "automatic_content_scan": false,
            "forced_refresh_bypasses_cache": false,
            "interval_seconds": app.config.monitor_interval_seconds
        }
    })
}

fn overall_status(metadata: &str, object: &str) -> &'static str {
    let states = [metadata, object];
    if states
        .iter()
        .any(|state| matches!(*state, "critical" | "failed" | "lost"))
    {
        "critical"
    } else if states
        .iter()
        .any(|state| matches!(*state, "degraded" | "warning"))
    {
        "degraded"
    } else if states.contains(&"pending") {
        "pending"
    } else if states.iter().all(|state| *state == "healthy") {
        "healthy"
    } else {
        "unknown"
    }
}

#[cfg(test)]
mod tests {
    use super::overall_status;

    #[test]
    fn cold_and_unavailable_samples_are_not_storage_damage_verdicts() {
        for (metadata, object, expected) in [
            ("healthy", "healthy", "healthy"),
            ("pending", "healthy", "pending"),
            ("healthy", "pending", "pending"),
            ("healthy", "stale", "unknown"),
            ("stale", "healthy", "unknown"),
            ("healthy", "unavailable", "unknown"),
            ("pending", "degraded", "degraded"),
            ("healthy", "critical", "critical"),
        ] {
            assert_eq!(overall_status(metadata, object), expected);
        }
    }
}
