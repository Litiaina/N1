use super::*;

pub(super) async fn print_op(
    client: &mut N2NativeClient,
    op: &str,
    payload: Value,
    json_output: bool,
) -> NativeClientResult<()> {
    let response = client.operation(op, &payload, None).await?;
    print_response(response, json_output)
}

pub(super) async fn print_dashboard(
    client: &mut N2NativeClient,
    args: DashboardArgs,
    cli: &Cli,
) -> NativeClientResult<()> {
    let value = client
        .operation_json(
            "cluster.monitor",
            &dashboard_payload(args, &cli.fragment),
            None,
        )
        .await?;
    if cli.json {
        return print_value(value, true);
    }

    println!("N2 Operator Dashboard");
    println!("---------------------");
    println!(
        "cluster: {} | access: {} | epoch: {}",
        value_str(&value, &["cluster", "status"])
            .or_else(|| value_str(&value, &["status"]))
            .unwrap_or("unknown"),
        value_str(&value, &["cluster", "access_mode"])
            .or_else(|| value_str(&value, &["access_mode"]))
            .unwrap_or("unknown"),
        value_u64(&value, &["cluster", "membership_epoch"])
            .or_else(|| value_u64(&value, &["membership", "epoch"]))
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string()),
    );

    if let Some(layout) = value.get("topology").or_else(|| value.get("layout")) {
        println!(
            "layout: data={} parity={} active_nodes={} old_layout={} current_layout={}",
            value_u64(layout, &["data_shards"]).unwrap_or(0),
            value_u64(layout, &["parity_shards"]).unwrap_or(0),
            value_u64(layout, &["active_write_nodes"]).unwrap_or_else(|| {
                layout
                    .get("active_nodes")
                    .and_then(Value::as_array)
                    .map(|nodes| nodes.len() as u64)
                    .unwrap_or(0)
            }),
            value_u64(layout, &["old_layout_objects"]).unwrap_or(0),
            value_u64(layout, &["current_layout_objects"]).unwrap_or(0),
        );
    }

    if let Some(nodes) = value.get("nodes").and_then(Value::as_array) {
        let online = nodes
            .iter()
            .filter(|node| {
                value_str(node, &["status"]) == Some("online")
                    || value_str(node, &["state"]) == Some("online")
            })
            .count();
        println!("nodes: {online}/{} online", nodes.len());
        for node in nodes {
            println!(
                "  {} {:<10} rss={} rtt={}ms disk_free={}",
                value_str(node, &["id"])
                    .or_else(|| value_str(node, &["node_id"]))
                    .unwrap_or("node"),
                value_str(node, &["status"])
                    .or_else(|| value_str(node, &["state"]))
                    .unwrap_or("unknown"),
                value_u64(node, &["rss_bytes"])
                    .map(human_bytes)
                    .unwrap_or_else(|| "-".to_string()),
                value_u64(node, &["rtt_ms"]).unwrap_or(0),
                value_u64(node, &["disk_free_bytes"])
                    .map(human_bytes)
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
    }

    if let Some(health) = value.get("objects").or_else(|| value.get("object_health")) {
        println!(
            "objects: healthy={} degraded={} unavailable={} repairable={}",
            value_u64(health, &["healthy"]).unwrap_or(0),
            value_u64(health, &["degraded"]).unwrap_or(0),
            value_u64(health, &["unavailable"]).unwrap_or(0),
            value_u64(health, &["repairable"]).unwrap_or(0),
        );
    }

    if let Some(jobs) = value.get("jobs") {
        print_job_status_line("repair", jobs.get("repair").unwrap_or(&Value::Null));
        print_job_status_line("rebalance", jobs.get("rebalance").unwrap_or(&Value::Null));
    }

    if let Some(io_control) = value.get("io_control") {
        println!(
            "io: peer_read_pressure={} peer_write_pressure={} stalled_peers={}",
            value_u64(io_control, &["peer_read_pressure"]).unwrap_or(0),
            value_u64(io_control, &["peer_write_pressure"]).unwrap_or(0),
            value_u64(io_control, &["stalled_peers"]).unwrap_or(0),
        );
    }

    Ok(())
}

pub(super) fn print_job_status_line(label: &str, value: &Value) {
    let running = value
        .get("running")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let job = value_str(value, &["job"]).unwrap_or(label);
    let scanned = value_u64(value, &["scanned"]).unwrap_or(0);
    let rewritten = value_u64(value, &["rewritten"]).unwrap_or(0);
    let skipped = value_u64(value, &["skipped"])
        .or_else(|| value_u64(value, &["skipped_current_layout"]))
        .unwrap_or(0);
    let failed = value_u64(value, &["failed"]).unwrap_or(0);
    let last_error = value_str(value, &["last_error"]).unwrap_or("-");
    println!(
        "{job}: running={running} scanned={scanned} rewritten={rewritten} skipped={skipped} failed={failed} last_error={last_error}"
    );
}

pub(super) fn value_str<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_str()
}

pub(super) fn value_u64(value: &Value, path: &[&str]) -> Option<u64> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_u64()
}

pub(super) fn print_response(
    response: n2_native_client::NativeOperationResponse,
    json_output: bool,
) -> NativeClientResult<()> {
    if response.body.is_empty() {
        println!("null");
        return Ok(());
    }
    if let Ok(value) = rmp_serde::from_slice::<Value>(&response.body) {
        return print_value(value, json_output);
    }
    if let Ok(value) = serde_json::from_slice::<Value>(&response.body) {
        return print_value(value, json_output);
    }
    println!("{}", String::from_utf8_lossy(&response.body));
    Ok(())
}

pub(super) fn print_value(value: Value, json_output: bool) -> NativeClientResult<()> {
    if json_output {
        println!("{}", serde_json::to_string(&value)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&value)?);
    }
    Ok(())
}

pub(super) fn parse_json(raw: &str) -> NativeClientResult<Value> {
    serde_json::from_str(raw).map_err(Into::into)
}

pub(super) fn required_response_string(value: &Value, field: &str) -> NativeClientResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| NativeClientError::protocol(format!("native response missing {field}")))
}

pub(super) fn content_type_from_path(path: &Path) -> String {
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("html") => "text/html",
        Some("txt") => "text/plain",
        Some("json") => "application/json",
        Some("pdf") => "application/pdf",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("mp4") => "video/mp4",
        Some("webm") => "video/webm",
        Some("zip") => "application/zip",
        _ => "application/octet-stream",
    }
    .to_string()
}

pub(super) fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}
