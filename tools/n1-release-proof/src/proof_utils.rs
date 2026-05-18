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

fn count_symlinks_excluding(root: &Path, excluded_segment: &str) -> ProofResult<usize> {
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
            let excluded = rel
                .components()
                .any(|part| part.as_os_str() == excluded_segment);
            if excluded {
                continue;
            }
            if ty.is_symlink() {
                count += 1;
            } else if ty.is_dir() {
                stack.push(path);
            }
        }
    }
    Ok(count)
}

fn count_object_record_dirs(root: &Path) -> ProofResult<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if name.starts_with("@o_") {
                if count_files_with_suffix(&path, ".ndx")? == 0 {
                    return Err(format!(
                        "object record directory has no committed .ndx: {}",
                        path.display()
                    )
                    .into());
                }
                count += 1;
            } else {
                stack.push(path);
            }
        }
    }
    Ok(count)
}

fn count_dirs(root: &Path) -> ProofResult<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in fs::read_dir(root)? {
        if entry?.file_type()?.is_dir() {
            count += 1;
        }
    }
    Ok(count)
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
