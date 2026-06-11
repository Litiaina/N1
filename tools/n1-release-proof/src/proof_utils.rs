fn encode_path(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC)
        .to_string()
        .replace("%2F", "/")
}

fn encode_query_value(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn split_parent_name(path: &str) -> (&str, &str) {
    match path.rsplit_once('/') {
        Some((parent, name)) => (parent, name),
        None => ("", path),
    }
}

fn collect_unhealthy_strings(path: &str, value: &Value, bad: &mut Vec<(String, String)>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let next = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                collect_unhealthy_strings(&next, child, bad);
            }
        }
        Value::Array(items) => {
            for (idx, child) in items.iter().enumerate() {
                collect_unhealthy_strings(&format!("{path}[{idx}]"), child, bad);
            }
        }
        Value::String(text) => {
            let key = path.rsplit('.').next().unwrap_or(path).to_ascii_lowercase();
            let status_like = key.contains("status")
                || key.contains("state")
                || key.contains("health")
                || key.contains("severity")
                || key == "phase"
                || key == "level";
            let normalized = text.to_ascii_lowercase().replace(' ', "_");
            if status_like
                && matches!(
                    normalized.as_str(),
                    "critical" | "critical_failure" | "failed" | "failure" | "degraded"
                )
            {
                bad.push((path.to_string(), text.clone()));
            }
        }
        _ => {}
    }
}

fn count_files_with_suffix(root: &Path, suffix: &str) -> ProofResult<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let ty = entry.file_type()?;
            if ty.is_dir() {
                stack.push(path);
            } else if ty.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(suffix))
            {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn count_live_namespace_links(root: &Path) -> ProofResult<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let ty = fs::symlink_metadata(&path)?.file_type();
            let rel = path.strip_prefix(root).unwrap_or(&path);
            let in_trash = rel
                .components()
                .any(|part| part.as_os_str() == ".trash");
            if in_trash {
                continue;
            }
            if ty.is_symlink() {
                let target = fs::read_link(&path)?;
                let is_object_link = target
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("@o_"));
                if is_object_link {
                    count += 1;
                }
            } else if ty.is_dir() {
                stack.push(path);
            }
        }
    }
    Ok(count)
}

fn fragment_alias_stable_id(root: &Path, fragment: &str) -> ProofResult<String> {
    let alias = root.join(".fragment").join(fragment);
    let target = fs::read_link(&alias)
        .map_err(|err| format!("read fragment alias {}: {err}", alias.display()))?;
    let name = target
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("fragment alias has invalid target: {}", alias.display()))?;
    name.strip_prefix("@f_")
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("fragment alias target is not an @f_ root: {name}").into())
}

fn physical_fragment_root(root: &Path, stable_fragment_id: &str) -> PathBuf {
    root.join(format!("@f_{stable_fragment_id}"))
}

fn collect_object_dirs(root: &Path) -> ProofResult<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let path = entry.path();
            let name = entry.file_name();
            if name.to_string_lossy().starts_with("@o_") {
                out.push(path);
            } else {
                stack.push(path);
            }
        }
    }
    Ok(out)
}

fn verify_colocated_object_dir(object_dir: &Path) -> ProofResult<(usize, usize)> {
    let mut metadata_versions = BTreeSet::new();
    let mut payload_versions = BTreeSet::new();
    for entry in fs::read_dir(object_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(version) = entry.file_name().to_str() {
                payload_versions.insert(version.to_string());
            }
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(version) = name.strip_suffix(".ndx").and_then(|stem| stem.split('.').next())
        else {
            continue;
        };
        metadata_versions.insert(version.to_string());
    }
    if metadata_versions != payload_versions {
        return Err(format!(
            "colocated object version mismatch under {}: metadata={metadata_versions:?} payload={payload_versions:?}",
            object_dir.display()
        )
        .into());
    }
    Ok((metadata_versions.len(), payload_versions.len()))
}

fn deterministic_payload(size: usize, seed: &[u8]) -> Vec<u8> {
    let digest = Sha256::digest(seed);
    let mut out = Vec::with_capacity(size);
    while out.len() < size {
        out.extend_from_slice(&digest);
    }
    out.truncate(size);
    out
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

async fn response_bytes_limited(
    response: reqwest::Response,
    max_bytes: usize,
    label: &str,
) -> ProofResult<Vec<u8>> {
    let mut out = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let next_len = out.len().saturating_add(chunk.len());
        if next_len > max_bytes {
            return Err(format!(
                "{label} response exceeded proof memory cap: {} > {} bytes",
                next_len, max_bytes
            )
            .into());
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

fn tar_names(raw: &[u8]) -> ProofResult<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    let mut offset = 0usize;
    while offset + 512 <= raw.len() {
        let header = &raw[offset..offset + 512];
        if header.iter().all(|byte| *byte == 0) {
            break;
        }
        let name = parse_tar_string(&header[0..100]);
        let prefix = parse_tar_string(&header[345..500]);
        let full_name = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        if !full_name.is_empty() {
            names.insert(full_name);
        }
        let size_text = parse_tar_string(&header[124..136]);
        let size = usize::from_str_radix(size_text.trim(), 8).unwrap_or(0);
        let blocks = size.div_ceil(512);
        offset += 512 + blocks * 512;
    }
    Ok(names)
}

fn parse_tar_string(bytes: &[u8]) -> String {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).trim().to_string()
}

fn read_rss_kib(pid: u32) -> ProofResult<Option<u64>> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let value = rest
                .split_whitespace()
                .next()
                .and_then(|part| part.parse::<u64>().ok());
            return Ok(value);
        }
    }
    Ok(None)
}

fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}
