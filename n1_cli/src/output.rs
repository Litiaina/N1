use chrono::{Local, TimeZone};

use cli_core::models::{
    GcJobStatus, HealthResponse, ListUploadsResponse, RedistributionJobStatus, RepairJobStatus,
    UploadStatusResponse,
};

fn format_timestamp(ts: i64) -> String {
    Local
        .timestamp_opt(ts, 0)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut idx = 0usize;
    while value >= 1024.0 && idx < UNITS.len() - 1 {
        value /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{bytes} {}", UNITS[idx])
    } else {
        format!("{value:.2} {}", UNITS[idx])
    }
}

fn human_duration(seconds: u64) -> String {
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    let rem_seconds = seconds % 60;
    if minutes < 60 {
        return format!("{minutes}m {rem_seconds}s");
    }
    let hours = minutes / 60;
    let rem_minutes = minutes % 60;
    format!("{hours}h {rem_minutes}m")
}

fn format_rows(title: &str, rows: &[(&str, String)]) -> String {
    let width = rows.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
    let mut out = String::new();
    out.push_str(title);
    out.push('\n');
    out.push_str(&"-".repeat(title.len()));
    out.push('\n');
    for (key, value) in rows {
        out.push_str(&format!("{key:width$}  {value}\n", width = width));
    }
    out.trim_end().to_string()
}

fn format_optional_error(error: &Option<String>) -> String {
    error.clone().unwrap_or_else(|| "-".to_string())
}

pub fn format_health(response: &HealthResponse) -> String {
    let placement_limiter = response
        .storage
        .placement_limiter_path
        .as_ref()
        .map(|path| {
            let free = response
                .storage
                .placement_limiter_available_bytes
                .map(human_bytes)
                .unwrap_or_else(|| "unknown".to_string());
            let usage = response
                .storage
                .placement_limiter_usage_percent
                .map(|value| format!("{value:.2}% used"))
                .unwrap_or_else(|| "usage unknown".to_string());
            format!("{path} ({free} free, {usage})")
        })
        .unwrap_or_else(|| "-".to_string());
    let blob_shape = if response.storage.heterogeneous_blob_capacity {
        response
            .storage
            .blob_capacity_imbalance_ratio
            .map(|ratio| format!("heterogeneous ({ratio:.2}x max/min)"))
            .unwrap_or_else(|| "heterogeneous".to_string())
    } else {
        "balanced".to_string()
    };
    let rows = vec![
        ("sampled_at", format_timestamp(response.sampled_at)),
        ("status", response.status.clone()),
        ("summary", response.summary.clone()),
        (
            "cpu",
            format!(
                "{:.2}% on {} core(s)",
                response.system.cpu_usage_percent, response.system.cpu_cores
            ),
        ),
        (
            "ram",
            format!(
                "{:.2}% ({} / {})",
                response.system.ram_usage_percent,
                human_bytes(response.system.ram_used_bytes),
                human_bytes(response.system.ram_total_bytes)
            ),
        ),
        (
            "process_rss",
            human_bytes(response.system.process_resident_bytes),
        ),
        (
            "process_vmem",
            human_bytes(response.system.process_virtual_bytes),
        ),
        (
            "open_fds",
            response
                .system
                .process_open_fds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        ),
        (
            "storage",
            format!(
                "{} free / {} raw",
                human_bytes(response.storage.raw_available_bytes),
                human_bytes(response.storage.raw_total_bytes)
            ),
        ),
        (
            "index_health",
            response.consistency.index_mirror_health.clone(),
        ),
        (
            "blob_devices",
            format!(
                "{}/{} online",
                response.storage.blob_devices_online, response.storage.blob_devices_total
            ),
        ),
        ("blob_shape", blob_shape),
        ("placement_limiter", placement_limiter),
        (
            "index_roots",
            format!(
                "{}/{} online",
                response.storage.index_roots_online, response.storage.index_roots_total
            ),
        ),
        (
            "uploads",
            format!(
                "{} active, {} reserved, {} committed",
                response.uploads.active_sessions,
                human_bytes(response.uploads.reserved_bytes),
                human_bytes(response.uploads.committed_bytes)
            ),
        ),
        (
            "stale_uploads",
            format!(
                "{} session(s), {} reserved{}",
                response.uploads.stale_sessions,
                human_bytes(response.uploads.stale_reserved_bytes),
                response
                    .uploads
                    .oldest_stale_session_age_seconds
                    .map(|age| format!(", oldest {}", human_duration(age)))
                    .unwrap_or_default()
            ),
        ),
        (
            "maintenance",
            format!(
                "mode={} repair={} redist={} gc={}",
                if response.maintenance.access_mode.is_empty() {
                    "availability"
                } else {
                    response.maintenance.access_mode.as_str()
                },
                if response.maintenance.repair_running {
                    response.maintenance.repair_phase.as_str()
                } else {
                    "idle"
                },
                if response.maintenance.redistribution_running {
                    response.maintenance.redistribution_phase.as_str()
                } else {
                    "idle"
                },
                if response.maintenance.gc_running {
                    response.maintenance.gc_phase.as_str()
                } else {
                    "idle"
                }
            ),
        ),
        (
            "redistribution_debt",
            format!(
                "{} pending, {} failed, {} fragment(s){}",
                response.maintenance.pending_redistribution_intents,
                response.maintenance.failed_redistribution_intents,
                response.maintenance.fragments_with_pending_redistribution,
                response
                    .maintenance
                    .oldest_pending_redistribution_intent_age_seconds
                    .map(|age| format!(", oldest {}", human_duration(age)))
                    .unwrap_or_default()
            ),
        ),
    ];

    let mut out = format_rows("N1 2.0 Health", &rows);

    if !response.disks.is_empty() {
        out.push_str("\n\ndisks");
        out.push_str("\n-----");
        for disk in &response.disks {
            let usage = match (disk.used_bytes, disk.total_bytes, disk.available_bytes) {
                (Some(used), Some(total), Some(available)) => format!(
                    "{} used / {} total / {} free ({:.2}%)",
                    human_bytes(used),
                    human_bytes(total),
                    human_bytes(available),
                    disk.usage_percent.unwrap_or(0.0)
                ),
                _ => "usage unavailable".to_string(),
            };
            let latency = disk
                .latency_ms
                .map(|value| format!("{value}ms"))
                .unwrap_or_else(|| "-".to_string());
            let error = disk.error.clone().unwrap_or_else(|| "-".to_string());
            out.push_str(&format!(
                "\n{} [{}] {}  latency={}  usage={}  path={}  error={}",
                disk.role,
                disk.status,
                if disk.status == "online" {
                    "healthy"
                } else {
                    "attention"
                },
                latency,
                usage,
                disk.path,
                error
            ));
        }
    }

    out
}

pub fn format_health_watch_line(response: &HealthResponse) -> String {
    let disk_summary = response
        .disks
        .iter()
        .map(|disk| {
            let usage = disk
                .usage_percent
                .map(|value| format!("{value:.0}%"))
                .unwrap_or_else(|| "?".to_string());
            format!("{}:{}@{}", disk.role, disk.status, usage)
        })
        .collect::<Vec<_>>()
        .join(",");
    let limiter_summary = response
        .storage
        .placement_limiter_path
        .as_ref()
        .map(|path| {
            let free = response
                .storage
                .placement_limiter_available_bytes
                .map(human_bytes)
                .unwrap_or_else(|| "?".to_string());
            format!("{}@{}", path, free)
        })
        .unwrap_or_else(|| "-".to_string());
    let blob_shape = if response.storage.heterogeneous_blob_capacity {
        response
            .storage
            .blob_capacity_imbalance_ratio
            .map(|ratio| format!("{ratio:.2}x"))
            .unwrap_or_else(|| "yes".to_string())
    } else {
        "balanced".to_string()
    };
    format!(
        "{}  status={} cpu={:.2}% ram={:.2}% rss={} uploads={} stale={} redist_debt={} blob={}/{} shape={} limit={} maint={} R{}D{}G{} disks={} summary={}",
        format_timestamp(response.sampled_at),
        response.status,
        response.system.cpu_usage_percent,
        response.system.ram_usage_percent,
        human_bytes(response.system.process_resident_bytes),
        response.uploads.active_sessions,
        response.uploads.stale_sessions,
        response.maintenance.pending_redistribution_intents,
        response.storage.blob_devices_online,
        response.storage.blob_devices_total,
        blob_shape,
        limiter_summary,
        if response.maintenance.access_mode.is_empty() {
            "availability"
        } else {
            response.maintenance.access_mode.as_str()
        },
        if response.maintenance.repair_running {
            1
        } else {
            0
        },
        if response.maintenance.redistribution_running {
            1
        } else {
            0
        },
        if response.maintenance.gc_running {
            1
        } else {
            0
        },
        disk_summary,
        response.summary
    )
}

pub fn format_upload_list(response: &ListUploadsResponse) -> String {
    let mut out = format_rows(
        "Active Uploads",
        &[
            ("total", response.total.to_string()),
            ("sessions", response.sessions.len().to_string()),
        ],
    );
    if response.sessions.is_empty() {
        out.push_str("\n\nNo active upload sessions.");
        return out;
    }

    out.push_str("\n\nsession_id  version_id  committed_parts  committed_bytes  object_key");
    for session in &response.sessions {
        out.push_str(&format!(
            "\n{}  {}  {}  {}  {}",
            session.session_id,
            session.version_id,
            session.committed_parts,
            human_bytes(session.committed_bytes),
            session
                .object_key
                .clone()
                .unwrap_or_else(|| "-".to_string())
        ));
    }
    out
}

pub fn format_upload_status(response: &UploadStatusResponse) -> String {
    format_rows(
        "Upload Status",
        &[
            (
                "session_id",
                response
                    .session_id
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
            ),
            (
                "version_id",
                response
                    .version_id
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
            ),
            (
                "object_key",
                response
                    .object_key
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
            ),
            ("committed_parts", response.committed_parts.to_string()),
            ("committed_bytes", human_bytes(response.committed_bytes)),
            (
                "expected_parts",
                response
                    .expected_parts
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ),
            (
                "uploaded_parts",
                if response.uploaded_parts.is_empty() {
                    "-".to_string()
                } else {
                    response
                        .uploaded_parts
                        .iter()
                        .map(|part| part.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                },
            ),
        ],
    )
}

pub fn format_gc_status(status: &GcJobStatus) -> String {
    format_rows(
        "Garbage Collector",
        &[
            ("running", status.is_running.to_string()),
            ("phase", status.phase.clone()),
            ("fragment", status.current_fragment.clone()),
            ("duration", format!("{}s", status.duration_seconds)),
            (
                "fragments",
                format!(
                    "{}/{}",
                    status.total_fragments_scanned, status.total_fragments
                ),
            ),
            (
                "trash_expired",
                status.total_trash_versions_expired.to_string(),
            ),
            (
                "orphan_blob_dirs",
                status.total_orphan_blob_dirs_deleted.to_string(),
            ),
            ("refund_bytes", human_bytes(status.total_refund_bytes)),
            ("errors", status.total_errors.to_string()),
            ("last_error", format_optional_error(&status.last_error)),
        ],
    )
}

pub fn format_repair_status(status: &RepairJobStatus) -> String {
    format_rows(
        "Repair",
        &[
            ("running", status.is_running.to_string()),
            ("phase", status.phase.clone()),
            ("fragment", status.current_fragment.clone()),
            ("duration", format!("{}s", status.duration_seconds)),
            ("scanned", status.total_scanned.to_string()),
            ("repaired", status.total_repaired.to_string()),
            ("corrupt_found", status.corrupt_found.to_string()),
            ("scan_rate", format!("{:.2}/s", status.scan_rate_per_sec)),
            (
                "last_repaired",
                status
                    .last_repaired
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
            ),
            ("last_error", format_optional_error(&status.last_error)),
        ],
    )
}

pub fn format_redistribution_status(status: &RedistributionJobStatus) -> String {
    format_rows(
        "Redistribution",
        &[
            ("running", status.is_running.to_string()),
            ("phase", status.phase.clone()),
            ("fragment", status.current_fragment.clone()),
            ("duration", format!("{}s", status.duration_seconds)),
            (
                "fragments_scanned",
                status.total_fragments_scanned.to_string(),
            ),
            ("objects_scanned", status.total_objects_scanned.to_string()),
            (
                "objects_need_move",
                status.total_objects_need_redistribution.to_string(),
            ),
            (
                "objects_migrated",
                status.total_objects_migrated.to_string(),
            ),
            ("scan_rate", format!("{:.2}/s", status.scan_rate_per_sec)),
            ("last_error", format_optional_error(&status.last_error)),
        ],
    )
}
