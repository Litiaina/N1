use super::*;

pub(super) fn render_membership(spec: &ClusterSpec) -> Result<Value, String> {
    spec.validate()?;
    let pools = spec
        .pools
        .values()
        .map(|pool| {
            json!({
                "id": pool.id,
                "generation": pool.generation,
                "policy": {
                    "object_data_shards": pool.data_shards,
                    "object_parity_shards": pool.parity_shards,
                },
                "state": if pool.writable { "writable" } else { "sealed" },
                "slot_ids": pool.node_ids,
            })
        })
        .collect::<Vec<_>>();
    let active_ids = spec
        .pools
        .values()
        .flat_map(|pool| pool.node_ids.iter())
        .collect::<BTreeSet<_>>();
    let bindings = active_ids
        .into_iter()
        .map(|node_id| {
            let node = spec.nodes.get(node_id).expect("validated active node");
            let host = spec.host_for_node(node);
            json!({
                "slot_id": node.id,
                "instance_id": node.id,
                "binding_epoch": 1,
                "endpoint": format!("https://{}:{}", node.advertised_host, node.https_port),
                "native_endpoint": format!("n2://{}:{}", node.advertised_host, node.native_port),
                "state": "online",
                "weight": 1,
            "storage_roots": node.disks.keys().map(|label| json!({
                    "id": root_id(&node.id, label),
                    "path": Path::new(&host.agent.mount_root).join(label).join("n2"),
                })).collect::<Vec<_>>(),
                "joined_epoch": 1,
            })
        })
        .collect::<Vec<_>>();
    let reserves = spec
        .reserve_nodes
        .iter()
        .map(|node_id| {
            let node = spec.nodes.get(node_id).expect("validated reserve");
            let host = spec.host_for_node(node);
            json!({
                "instance_id": node.id,
                "endpoint": format!("https://{}:{}", node.advertised_host, node.https_port),
                "native_endpoint": format!("n2://{}:{}", node.advertised_host, node.native_port),
                "state": "online",
            "storage_roots": node.disks.keys().map(|label| json!({
                    "id": root_id(&node.id, label),
                    "path": Path::new(&host.agent.mount_root).join(label).join("n2"),
                })).collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "format_version": MEMBERSHIP_FORMAT_VERSION,
        "cluster_id": spec.cluster_id,
        "epoch": spec.topology_epoch,
        "binding_epoch": 1,
        "pools": pools,
        "slot_bindings": bindings,
        "reserve_nodes": reserves,
        "retired_instances": [],
    }))
}

pub(super) fn render_node_config(
    spec: &ClusterSpec,
    node_id: &str,
    settings: &InstallSettings,
) -> Result<String, String> {
    let node = spec
        .nodes
        .get(node_id)
        .ok_or_else(|| format!("unknown node '{node_id}'"))?;
    let pool = spec
        .pools
        .values()
        .find(|pool| pool.node_ids.contains(&node_id.to_string()));
    let (pool_id, generation, data, parity) = if let Some(pool) = pool {
        (
            pool.id.as_str(),
            pool.generation,
            pool.data_shards,
            pool.parity_shards,
        )
    } else {
        let first = spec
            .pools
            .values()
            .next()
            .ok_or("no server pool configured")?;
        (
            first.id.as_str(),
            first.generation,
            first.data_shards,
            first.parity_shards,
        )
    };
    let roots = node
        .disks
        .keys()
        .map(|label| {
            format!(
                "{}={}",
                root_id(node_id, label),
                settings.mount_path(label).join("n2").display()
            )
        })
        .collect::<Vec<_>>();
    let node_dir = settings.node_dir(node_id);
    Ok(format!(
        "[server]\nhost=0.0.0.0\nhttps_port={}\nnative_port={}\ncert_path={}\nkey_path={}\n\n[disk_config]\nstorage_roots={}\nruntime_path={}\n\n[cluster_config]\nnode_id={}\npool_id={}\npool_generation={}\nnode_endpoint=https://{}:{}\nnative_endpoint=n2://{}:{}\nmembership_file={}\nobject_data_shards={}\nobject_parity_shards={}\nauto_rebalance=false\n\n[garbage_collector]\ncleanup_interval_seconds=3600\nfile_ttl_seconds=3600\nretention_seconds=2592000\n\n[auto_healing]\nenabled=true\ninitial_delay_seconds=30\nscan_interval_seconds=900\ncoordinator_nodes=auto\nmax_parallel_fragments=2\nmax_objects_per_fragment_cycle=256\nmax_duration_seconds=30\nlease_seconds=90\nchecksum_scan_every_cycles=24\n\n[set_limits]\nmax_file_size=5368709120\n\n[jwt_token_config]\nlogin_token_expiration=900\n\n[rate_limiter_config]\nenable_rate_limiting=true\nauth_limit_per_second=2\nauth_limit_burst_size=5\nview_limit_per_second=30\nview_limit_burst_size=100\nupload_limit_per_second=300\nupload_limit_burst_size=3000\nglobal_limit_per_second=200\nglobal_limit_burst_size=300\n\n[upload_config]\nmax_concurrent_io=auto\nadaptive_concurrency_enabled=true\nadaptive_concurrency_interval_ms=auto\nwriter_buffer_size=auto\nupload_memory_budget_bytes=auto\nupload_part_strict_durability_default=false\nupload_io_gate_enabled=true\nstream_max_concurrency=auto\nstream_chunk_size=auto\nstream_readahead_bytes=auto\nstream_locality_bias_enabled=true\nmonitor_interval_seconds=auto\nmonitor_deep_scan_interval_seconds=auto\nmax_part_size=52428800\ndata_shards={}\nparity_shards={}\nstripe_block_size=262144\n",
        node.https_port,
        node.native_port,
        node_dir.join("certificates/cert.pem").display(),
        node_dir.join("certificates/key.pem").display(),
        roots.join(","),
        node_dir.join("state").display(),
        node.id,
        pool_id,
        generation,
        node.advertised_host,
        node.https_port,
        node.advertised_host,
        node.native_port,
        node_dir.join("state/n2_membership.json").display(),
        data,
        parity,
        data,
        parity,
    ))
}

pub(super) fn render_gateway_config(
    spec: &ClusterSpec,
    host_id: &str,
    settings: &InstallSettings,
) -> Result<String, String> {
    let gateway = spec
        .gateway_for_host(host_id)
        .ok_or("gateway is not assigned")?;
    let mut output = format!(
        "[server]\nhost={}\nhttps_port={}\ncert_path={}\nkey_path={}\n\n[routing]\nhealth_interval_seconds=5\nrequest_timeout_seconds=300\n\n",
        gateway.bind_host,
        gateway.https_port,
        settings
            .gateway_runtime
            .join("certificates/cert.pem")
            .display(),
        settings
            .gateway_runtime
            .join("certificates/key.pem")
            .display(),
    );
    for node in spec.nodes.values() {
        output.push_str(&format!(
            "[node.{}]\nid={}\nhttp_url=https://{}:{}\nnative_url=n2://{}:{}\nweight=1\n\n",
            node.id,
            node.id,
            node.advertised_host,
            node.https_port,
            node.advertised_host,
            node.native_port
        ));
    }
    Ok(output)
}

pub(super) fn deployment_hash(
    spec: &ClusterSpec,
    env: &str,
    certificate: &[u8],
    private_key: &[u8],
) -> Result<String, String> {
    let canonical = json!({
        "spec": spec.sanitized(),
        "env_sha256": sha256_hex(env.as_bytes()),
        "certificate_sha256": sha256_hex(certificate),
        "private_key_sha256": sha256_hex(private_key),
    });
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|error| format!("serialize canonical deployment: {error}"))?;
    Ok(sha256_hex(&bytes))
}

pub(super) fn ensure_agent_success(response: &AgentResponse) -> Result<(), String> {
    if response.success {
        Ok(())
    } else {
        Err(response.message.clone())
    }
}

pub(super) fn emit_agent_response(response: &AgentResponse, emit: Emit<'_>) -> Result<(), String> {
    ensure_agent_success(response)?;
    for line in &response.lines {
        emit(
            match line.kind.as_str() {
                "success" => OutputKind::Success,
                "warning" => OutputKind::Warning,
                "error" => OutputKind::Error,
                "info" => OutputKind::Info,
                _ => OutputKind::Normal,
            },
            line.text.clone(),
        );
    }
    emit(OutputKind::Success, response.message.clone());
    Ok(())
}

pub(super) fn output_kind_name(kind: OutputKind) -> &'static str {
    match kind {
        OutputKind::Normal => "normal",
        OutputKind::Info => "info",
        OutputKind::Success => "success",
        OutputKind::Warning => "warning",
        OutputKind::Error => "error",
    }
}

pub(super) fn service_action_name(action: &AgentServiceAction) -> &'static str {
    match action {
        AgentServiceAction::Start => "start",
        AgentServiceAction::Stop => "stop",
        AgentServiceAction::Restart => "restart",
        AgentServiceAction::Enable => "enable",
        AgentServiceAction::Disable => "disable",
    }
}

pub(super) fn write_json<T: Serialize>(path: &Path, value: &T, mode: u32) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| format!("serialize {}: {error}", path.display()))?;
    atomic_write(path, &bytes, mode)
}

pub(super) fn read_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
    label: &str,
) -> Result<T, String> {
    let bytes =
        fs::read(path).map_err(|error| format!("read {label} {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse {label} {}: {error}", path.display()))
}

pub(super) fn copy_atomic(source: &Path, target: &Path, mode: u32) -> Result<(), String> {
    let bytes =
        fs::read(source).map_err(|error| format!("read staged {}: {error}", source.display()))?;
    atomic_write(target, &bytes, mode)
}

pub(super) fn stage_content_hash(stage: &Path) -> Result<String, String> {
    let mut relative_paths = Vec::new();
    collect_stage_files(stage, stage, &mut relative_paths)?;
    relative_paths.sort();
    let mut hasher = Sha256::new();
    for relative in relative_paths {
        hasher.update(relative.as_os_str().as_encoded_bytes());
        hasher.update([0]);
        let bytes = fs::read(stage.join(&relative))
            .map_err(|error| format!("read staged {}: {error}", relative.display()))?;
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(super) fn collect_stage_files(
    root: &Path,
    directory: &Path,
    output: &mut Vec<PathBuf>,
) -> Result<(), String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("read staged directory {}: {error}", directory.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("read staged entry: {error}"))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("inspect staged entry {}: {error}", path.display()))?;
        if file_type.is_dir() {
            collect_stage_files(root, &path, output)?;
        } else if file_type.is_file() && entry.file_name() != "manifest.json" {
            output.push(
                path.strip_prefix(root)
                    .map_err(|error| format!("resolve staged path {}: {error}", path.display()))?
                    .to_path_buf(),
            );
        }
    }
    Ok(())
}

pub(super) fn read_trimmed(path: &Path) -> Result<String, String> {
    fs::read_to_string(path)
        .map(|value| value.trim().to_string())
        .map_err(|error| format!("read {}: {error}", path.display()))
}

pub(super) fn machine_fingerprint() -> String {
    let source = fs::read("/etc/machine-id")
        .or_else(|_| fs::read("/var/lib/dbus/machine-id"))
        .unwrap_or_else(|_| {
            hostname::get()
                .unwrap_or_default()
                .to_string_lossy()
                .as_bytes()
                .to_vec()
        });
    sha256_hex(&source)
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub(super) fn validate_hash(value: &str) -> Result<(), String> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err("plan hash must be exactly 64 hexadecimal characters".into())
    }
}

pub(super) fn random_secret() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

pub(super) fn generated_env() -> String {
    format!(
        "JWT_SECRET={}\nACCESS_TOKEN_DEFAULT_TTL_SECS=900\nACCESS_TOKEN_MAX_TTL_SECS=3600\nREFRESH_TOKEN_TTL_SECS=604800\nAUTH_KEY={}\nMASTER_KEY={}\nADMIN_KEY={}\nN2_CLUSTER_SECRET={}\n",
        random_secret(),
        random_secret(),
        random_secret(),
        random_secret(),
        random_secret()
    )
}

pub(super) fn validate_env(value: &str) -> Result<(), String> {
    for key in [
        "JWT_SECRET",
        "AUTH_KEY",
        "MASTER_KEY",
        "ADMIN_KEY",
        "N2_CLUSTER_SECRET",
    ] {
        if !value.lines().any(|line| {
            line.split_once('=')
                .is_some_and(|(name, data)| name == key && !data.is_empty())
        }) {
            return Err(format!("generated cluster environment is missing {key}"));
        }
    }
    Ok(())
}

pub(super) fn disk_serial(disk: &BlockDisk) -> &str {
    if disk.serial.is_empty() {
        disk.name.as_str()
    } else {
        disk.serial.as_str()
    }
}

pub(super) fn root_id(node_id: &str, label: &str) -> String {
    format!("{node_id}-{}", label.to_ascii_lowercase())
}

pub(super) fn absolute_path(value: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(value);
    if !path.is_absolute() || value.chars().any(char::is_control) {
        return Err("path must be absolute and contain no control characters".into());
    }
    Ok(path)
}

pub(super) fn ensure_regular_file(path: &Path, label: &str) -> Result<(), String> {
    let metadata = path
        .metadata()
        .map_err(|error| format!("inspect {label} {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{label} {} is not a regular file", path.display()));
    }
    Ok(())
}

pub(super) fn ensure_executable(path: &Path, label: &str) -> Result<(), String> {
    ensure_regular_file(path, label)?;
    let mode = path
        .metadata()
        .map_err(|error| format!("inspect {label} {}: {error}", path.display()))?
        .permissions()
        .mode();
    if mode & 0o111 == 0 {
        return Err(format!("{label} {} is not executable", path.display()));
    }
    Ok(())
}

pub(super) fn normalize_agent_url(value: &str) -> Result<String, String> {
    let value = value.trim_end_matches('/');
    if !(value.starts_with("http://") || value.starts_with("https://"))
        || value.contains(['\n', '\r'])
    {
        return Err("agent URL must be an http:// or https:// URL".into());
    }
    Ok(value.to_string())
}

pub(super) fn validate_id(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!(
            "{label} must contain only letters, numbers, '-' or '_'"
        ));
    }
    Ok(())
}

pub(super) fn validate_host(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 253
        || value.contains(['/', '\\', ':'])
        || value.chars().any(char::is_whitespace)
    {
        return Err("host must be an IP address or DNS name without scheme or port".into());
    }
    Ok(())
}

pub(super) fn parse_port(value: Option<&String>, usage: &str) -> Result<u16, String> {
    value
        .ok_or(usage)?
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| usage.to_string())
}

pub(super) fn parse_usize(value: Option<&String>, usage: &str) -> Result<usize, String> {
    value
        .ok_or(usage)?
        .parse::<usize>()
        .map_err(|_| usage.to_string())
}

pub(super) fn parse_u64(value: Option<&String>, usage: &str) -> Result<u64, String> {
    value
        .ok_or(usage)?
        .parse::<u64>()
        .map_err(|_| usage.to_string())
}

pub(super) fn split_command(input: &str) -> Result<Vec<String>, String> {
    let mut arguments = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in input.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        if character == '\\' && quote != Some('\'') {
            escaped = true;
            continue;
        }
        if let Some(active) = quote {
            if character == active {
                quote = None;
            } else {
                current.push(character);
            }
            continue;
        }
        match character {
            '\'' | '"' => quote = Some(character),
            character if character.is_whitespace() => {
                if !current.is_empty() {
                    arguments.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(character),
        }
    }
    if escaped || quote.is_some() {
        return Err("incomplete quote or escape".into());
    }
    if !current.is_empty() {
        arguments.push(current);
    }
    Ok(arguments)
}

pub(super) fn success_outcome() -> CommandOutcome {
    CommandOutcome {
        success: true,
        ..CommandOutcome::default()
    }
}

pub(super) fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}
