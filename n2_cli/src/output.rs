use crate::client::{
    ClusterMembership, N2ClusterHealthResponse, N2ClusterViewResponse, NodeDescriptor,
    ResolvePlacementResponse,
};
use cli_core::models::{
    GcJobStatus, ListUploadsResponse, RedistributionJobStatus, RepairJobStatus,
    UploadStatusResponse,
};

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

fn format_node(node: &NodeDescriptor) -> String {
    format!(
        "{} {} epoch={} endpoint={}",
        node.node_id,
        if node.active {
            "[active]"
        } else {
            "[inactive]"
        },
        node.epoch,
        node.endpoint
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
                "objects_migrated",
                status.total_objects_migrated.to_string(),
            ),
            ("scan_rate", format!("{:.2}/s", status.scan_rate_per_sec)),
            ("last_error", format_optional_error(&status.last_error)),
        ],
    )
}

pub fn format_cluster_view(response: &N2ClusterViewResponse) -> String {
    format_rows(
        "N2 Cluster View",
        &[
            ("node_id", response.node_id.clone()),
            ("endpoint", response.node_endpoint.clone()),
            ("membership_epoch", response.membership_epoch.to_string()),
            (
                "expected_epoch",
                response
                    .expected_epoch
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ),
            (
                "nodes",
                format!("{}/{} active", response.active_nodes, response.total_nodes),
            ),
            ("local_present", response.local_node_present.to_string()),
            ("local_active", response.local_node_active.to_string()),
            ("write_ready", response.write_ready.to_string()),
            (
                "reasons",
                if response.reasons.is_empty() {
                    "-".to_string()
                } else {
                    response.reasons.join("; ")
                },
            ),
        ],
    )
}

pub fn format_cluster_health(response: &N2ClusterHealthResponse) -> String {
    format_rows(
        "N2 Cluster Health",
        &[
            ("level", response.level.clone()),
            (
                "membership_epoch",
                response.view.membership_epoch.to_string(),
            ),
            ("write_ready", response.view.write_ready.to_string()),
            (
                "reachable_active",
                format!(
                    "{} reachable / {} unreachable",
                    response.reachable_active_nodes, response.unreachable_active_nodes
                ),
            ),
            (
                "storage_ready_nodes",
                response.storage_ready_nodes.to_string(),
            ),
            (
                "shards",
                format!(
                    "required={} expected={}",
                    response.required_shards, response.expected_shards
                ),
            ),
            ("deep", response.deep.to_string()),
            (
                "reasons",
                if response.reasons.is_empty() {
                    "-".to_string()
                } else {
                    response.reasons.join("; ")
                },
            ),
        ],
    )
}

pub fn format_cluster_health_watch_line(response: &N2ClusterHealthResponse) -> String {
    format!(
        "level={} epoch={} reachable_active={} unreachable_active={} storage_ready={} write_ready={}",
        response.level,
        response.view.membership_epoch,
        response.reachable_active_nodes,
        response.unreachable_active_nodes,
        response.storage_ready_nodes,
        response.view.write_ready
    )
}

pub fn format_membership(response: &ClusterMembership) -> String {
    let mut out = format_rows(
        "N2 Membership",
        &[
            ("epoch", response.epoch.to_string()),
            ("nodes", response.nodes.len().to_string()),
        ],
    );
    if !response.nodes.is_empty() {
        out.push_str("\n\nnodes");
        out.push_str("\n-----");
        for node in &response.nodes {
            out.push_str(&format!("\n{}", format_node(node)));
        }
    }
    out
}

pub fn format_placement(response: &ResolvePlacementResponse) -> String {
    let mut out = format_rows(
        "N2 Placement",
        &[
            ("epoch", response.epoch.to_string()),
            ("fragment", response.object.fragment.clone()),
            ("object_key", response.object.object_key.clone()),
            ("version_id", response.object.version_id.clone()),
            ("kc", response.kc.to_string()),
            ("mc", response.mc.to_string()),
            ("nc", response.nc.to_string()),
            ("write_quorum", response.write_quorum.to_string()),
        ],
    );
    if !response.targets.is_empty() {
        out.push_str("\n\ntargets");
        out.push_str("\n-------");
        for node in &response.targets {
            out.push_str(&format!("\n{}", format_node(node)));
        }
    }
    out
}
