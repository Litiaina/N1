use super::*;
use crate::Result;
use ini::Ini;
use std::{collections::BTreeSet, fs::OpenOptions, io::Read, str::FromStr};

pub(super) fn read_regular(path: &Path, private: bool) -> Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > 4 * 1024 * 1024 {
        return Err("N1 configuration must be a regular file of at most 4 MiB".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if private && metadata.permissions().mode() & 0o077 != 0 {
            return Err("secrets file must be owner-only (chmod 600)".into());
        }
    }
    #[cfg(not(unix))]
    let _ = private;
    let mut bytes = Vec::new();
    (&mut file)
        .take(4 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 4 * 1024 * 1024 {
        return Err("N1 configuration grew beyond 4 MiB".into());
    }
    Ok(bytes)
}
struct Section<'a> {
    ini: &'a Ini,
    name: String,
}
impl Section<'_> {
    fn text(&self, key: &str, default: &str) -> String {
        self.ini
            .section(Some(self.name.as_str()))
            .and_then(|s| s.get(key))
            .unwrap_or(default)
            .trim()
            .to_owned()
    }
    fn number(&self, key: &str, default: u64, minimum: u64, maximum: u64) -> Result<u64> {
        let value = self.text(key, &default.to_string());
        let number: u64 = value
            .parse()
            .map_err(|_| format!("[{}] {key} must be an unsigned integer", self.name))?;
        if number < minimum || number > maximum {
            return Err(format!("[{}] {key} must be {minimum}..={maximum}", self.name).into());
        }
        Ok(number)
    }
    fn boolean(&self, key: &str, default: bool) -> Result<bool> {
        match self
            .text(key, if default { "true" } else { "false" })
            .to_ascii_lowercase()
            .as_str()
        {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(format!("[{}] {key} must be a boolean", self.name).into()),
        }
    }
    fn optional_size(&self, key: &str) -> Result<Option<u64>> {
        let value = self.text(key, "unlimited");
        if value.eq_ignore_ascii_case("unlimited") {
            return Ok(None);
        }
        let bytes = value
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=i64::MAX as u64).contains(n));
        bytes.map(Some).ok_or_else(|| {
            format!(
                "[{}] {key} must be unlimited or 1..={} bytes",
                self.name,
                i64::MAX
            )
            .into()
        })
    }
    fn auto(&self, key: &str, default: usize, maximum: usize) -> Result<usize> {
        let value = self.text(key, "auto");
        if value.is_empty() || value.eq_ignore_ascii_case("auto") || value == "0" {
            return Ok(default);
        }
        let number: usize = value
            .parse()
            .map_err(|_| format!("[{}] {key} must be a positive integer or auto", self.name))?;
        if number > maximum {
            return Err(format!("[{}] {key} exceeds {maximum}", self.name).into());
        }
        Ok(number)
    }
    fn parsed<T: FromStr>(&self, key: &str, default: &str) -> Result<T>
    where
        T::Err: std::fmt::Display,
    {
        self.text(key, default)
            .parse()
            .map_err(|error| format!("[{}] {key}: {error}", self.name).into())
    }
}
fn absolute(base: &Path, value: &str) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    let path = if path.is_absolute() {
        path
    } else {
        base.join(path)
    };
    // Resolve existing parents so an alias or '..' cannot hide overlap between
    // control state and payload roots. Keep the final component un-followed;
    // enrollment still rejects a symlink member/control directory.
    if let (Some(parent), Some(name)) = (path.parent(), path.file_name())
        && let Ok(parent) = std::fs::canonicalize(parent)
    {
        return Ok(parent.join(name));
    }
    if path
        .components()
        .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err("paths containing '..' require an existing parent directory".into());
    }
    Ok(path)
}

pub(super) fn load(path: &Path) -> Result<N1Settings> {
    load_for_component(path, true)
}
pub(super) fn load_metadata(path: &Path) -> Result<N1Settings> {
    load_for_component(path, false)
}
fn load_for_component(path: &Path, owns_members: bool) -> Result<N1Settings> {
    let source = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let base = source.parent().ok_or("N1 configuration has no parent")?;
    let bytes = read_regular(&source, false)?;
    let ini = Ini::load_from_str(std::str::from_utf8(&bytes)?)?;
    let section = |name: &str| Section {
        ini: &ini,
        name: name.to_owned(),
    };
    if ini.section(Some("server")).is_none() || ini.section(Some("disk_config")).is_none() {
        return Err("N1 configuration requires [server] and [disk_config]".into());
    }
    let server = section("server");
    let cert = server.text("cert_path", "");
    let key = server.text("key_path", "");
    let env_file = server.text("env_file", "");
    if cert.is_empty() != key.is_empty() {
        return Err("[server] cert_path and key_path must be configured together".into());
    }
    let server = Server {
        host: server.parsed("host", "127.0.0.1")?,
        https_port: server.number("https_port", 50001, 1, 65535)? as u16,
        native_port: server.number("native_port", 50002, 1, 65535)? as u16,
        native_max_connections: server.number("native_max_connections", 32, 1, 4096)? as usize,
        cert_path: (!cert.is_empty())
            .then(|| absolute(base, &cert))
            .transpose()?,
        key_path: (!key.is_empty())
            .then(|| absolute(base, &key))
            .transpose()?,
        env_file: (!env_file.is_empty())
            .then(|| absolute(base, &env_file))
            .transpose()?,
    };
    if server.https_port == server.native_port {
        return Err("N1 HTTPS and native ports must differ".into());
    }
    let control = section("disk_config").text("fs_storage_path", "");
    if control.is_empty() {
        return Err("[disk_config] fs_storage_path must be configured".into());
    }
    let control_dir = absolute(base, &control)?;
    let mut pools = Vec::new();
    let mut all_roots = BTreeSet::new();
    for (name, _) in &ini {
        let Some(name) = name.and_then(|s| s.strip_prefix("storage_pool.")) else {
            continue;
        };
        if !owns_members {
            continue;
        }
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err("invalid storage_pool name".into());
        }
        let full_name = format!("storage_pool.{name}");
        let pool = section(&full_name);
        let roots = pool
            .text("roots", "")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| absolute(base, s))
            .collect::<Result<Vec<_>>>()?;
        let k = pool.number("data_shards", 3, 1, 255)? as u16;
        let p = pool.number("parity_shards", 1, 0, 254)? as u16;
        if k + p > 255 || roots.len() != usize::from(k + p) {
            return Err(format!("[{full_name}] roots must contain exactly data_shards + parity_shards distinct paths").into());
        }
        for root in &roots {
            if root == Path::new("/")
                || root.starts_with(&control_dir)
                || control_dir.starts_with(root)
            {
                return Err(
                    "pool roots must not overlap the N1 control directory or filesystem root"
                        .into(),
                );
            }
            if all_roots
                .iter()
                .any(|other: &PathBuf| root.starts_with(other) || other.starts_with(root))
            {
                return Err("pool roots must be distinct and non-overlapping".into());
            }
            all_roots.insert(root.clone());
        }
        pools.push(StoragePool {
            name: name.to_owned(),
            generation: pool.number("generation", 1, 1, u64::MAX)?,
            roots,
            data_shards: k,
            parity_shards: p,
            sealed: pool.boolean("sealed", false)?,
        });
    }
    if owns_members && pools.is_empty() {
        return Err("N1 requires at least one complete [storage_pool.NAME] section".into());
    }
    let mut repositories = Vec::new();
    for (name, _) in &ini {
        let Some(alias) = name.and_then(|name| name.strip_prefix("backup_repository.")) else {
            continue;
        };
        if alias.is_empty()
            || alias.len() > 64
            || !alias
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        {
            return Err("invalid backup repository alias".into());
        }
        let full = format!("backup_repository.{alias}");
        let repository = section(&full);
        let path = repository.text("path", "");
        let key_file = repository.text("key_file", "");
        if path.is_empty() || key_file.is_empty() {
            return Err(format!("[{full}] requires path and key_file").into());
        }
        repositories.push(BackupRepository {
            alias: alias.into(),
            path: absolute(base, &path)?,
            key_file: absolute(base, &key_file)?,
        });
    }
    // Preserve the original ordered pool allocation rule.
    pools.sort_by(|a, b| a.name.cmp(&b.name));
    let cpu = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    let ram = autotune::detect_total_ram_bytes();
    #[cfg(unix)]
    let disks = {
        use std::os::unix::fs::MetadataExt;
        let devices = all_roots
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.dev()))
            .collect::<BTreeSet<_>>();
        if devices.is_empty() {
            all_roots.len()
        } else {
            devices.len()
        }
    };
    #[cfg(not(unix))]
    let disks = all_roots.len();
    let width = pools
        .iter()
        .map(|p| usize::from(p.data_shards + p.parity_shards))
        .max()
        .unwrap_or(1);
    let u = section("upload_config");
    let chunk = autotune::align_up_4k(u.auto(
        "stream_chunk_size",
        autotune::adaptive_stream_chunk_size(cpu, ram),
        4 * 1024 * 1024,
    )?);
    let ahead = autotune::align_up_4k(u.auto(
        "stream_readahead_bytes",
        autotune::adaptive_stream_readahead_bytes(chunk, ram),
        8 * 1024 * 1024,
    )?)
    .max(chunk);
    let block = u.number("stripe_block_size", 262144, 4096, 4 * 1024 * 1024)? as u32;
    if !block.is_power_of_two() {
        return Err("[upload_config] stripe_block_size must be a power of two".into());
    }
    let upload = Upload {
        max_concurrent_io: u.auto(
            "max_concurrent_io",
            autotune::adaptive_max_concurrent_io(cpu, disks, ram),
            4096,
        )?,
        adaptive_concurrency_enabled: u.boolean("adaptive_concurrency_enabled", true)?,
        adaptive_concurrency_interval_ms: u.auto(
            "adaptive_concurrency_interval_ms",
            autotune::adaptive_concurrency_interval_ms(cpu, ram),
            300000,
        )? as u64,
        writer_buffer_size: autotune::align_up_4k(u.auto(
            "writer_buffer_size",
            autotune::adaptive_writer_buffer_size(cpu, ram, disks, width),
            64 * 1024 * 1024,
        )?),
        upload_memory_budget_bytes: u.auto(
            "upload_memory_budget_bytes",
            autotune::adaptive_upload_memory_budget_bytes(cpu, ram, disks, width),
            usize::MAX / 2,
        )?,
        upload_part_strict_durability_default: u
            .boolean("upload_part_strict_durability_default", false)?,
        upload_io_gate_enabled: u.boolean("upload_io_gate_enabled", true)?,
        stream_max_concurrency: u.auto(
            "stream_max_concurrency",
            autotune::adaptive_stream_max_concurrency(cpu, disks, ram, chunk, ahead),
            4096,
        )?,
        stream_chunk_size: chunk,
        stream_readahead_bytes: ahead,
        stream_locality_bias_enabled: u.boolean("stream_locality_bias_enabled", true)?,
        monitor_interval_seconds: u.auto(
            "monitor_interval_seconds",
            autotune::adaptive_monitor_interval_seconds(cpu, ram),
            86400,
        )? as u64,
        monitor_deep_scan_interval_seconds: u.auto(
            "monitor_deep_scan_interval_seconds",
            autotune::adaptive_monitor_deep_scan_interval_seconds(cpu, ram),
            30 * 86400,
        )? as u64,
        max_part_size: u.number("max_part_size", 52428800, 1, i64::MAX as u64)?,
        stripe_block_size: block,
    };
    if upload.adaptive_concurrency_interval_ms < 1000
        || upload.upload_memory_budget_bytes < 1024 * 1024
    {
        return Err(
            "adaptive interval must be at least 1000 ms and upload memory budget at least 1 MiB"
                .into(),
        );
    }
    let io = section("io_control");
    let io_control = IoControl {
        foreground_wait_timeout_ms: io.number("foreground_wait_timeout_ms", 30000, 100, 300000)?,
        stall_threshold_ms: io.number("stall_threshold_ms", 20000, 1000, 600000)?,
        slow_io_threshold_ms: io.number("slow_io_threshold_ms", 1000, 10, 300000)?,
        latency_samples_per_class: io.number("latency_samples_per_class", 1024, 128, 65536)?
            as usize,
    };
    let g = section("garbage_collector");
    let garbage_collector = GarbageCollector {
        cleanup_interval_seconds: g.number("cleanup_interval_seconds", 3600, 1, 90 * 86400)?,
        file_ttl_seconds: g.number("file_ttl_seconds", 3600, 1, 30 * 86400)?,
        trash_retention_seconds: g.number("trash_retention_seconds", 2592000, 0, u64::MAX)?,
    };
    let a = section("auto_healing");
    let auto_healing = AutoHealing {
        enabled: a.boolean("enabled", false)?,
        scrub_schedule_utc: a.text("scrub_schedule_utc", ""),
        scheduled_scrub_pause_millis: a.number("scheduled_scrub_pause_millis", 25, 0, 60000)?,
        topology_check_interval_seconds: a.number(
            "topology_check_interval_seconds",
            30,
            1,
            86400,
        )?,
        topology_recovery_stability_seconds: a.number(
            "topology_recovery_stability_seconds",
            10,
            0,
            86400,
        )?,
        initial_scan_delay_seconds: a.number("initial_scan_delay_seconds", 60, 0, 30 * 86400)?,
        availability_scan_interval_seconds: a.number(
            "availability_scan_interval_seconds",
            3600,
            1,
            90 * 86400,
        )?,
        checksum_scrub_interval_seconds: a.number(
            "checksum_scrub_interval_seconds",
            604800,
            1,
            90 * 86400,
        )?,
        object_scan_concurrency: a.number("object_scan_concurrency", 1, 1, 4096)? as usize,
        shard_repair_concurrency: a.number("shard_repair_concurrency", 1, 1, 4096)? as usize,
        metadata_batch_size: a.number("metadata_batch_size", 128, 1, 1000)? as usize,
    };
    let r = section("rate_limiter_config");
    let enabled = r.boolean("enable_rate_limiting", true)?;
    let rate = |key, default, burst| {
        r.number(
            key,
            default,
            if enabled { 1 } else { 0 },
            if burst {
                u32::MAX as u64
            } else {
                1_000_000_000
            },
        )
    };
    let rate_limits = RateLimits {
        enable_rate_limiting: enabled,
        auth_limit_per_second: rate("auth_limit_per_second", 2, false)?,
        auth_limit_burst_size: rate("auth_limit_burst_size", 5, true)? as u32,
        view_limit_per_second: rate("view_limit_per_second", 30, false)?,
        view_limit_burst_size: rate("view_limit_burst_size", 100, true)? as u32,
        upload_limit_per_second: rate("upload_limit_per_second", 300, false)?,
        upload_limit_burst_size: rate("upload_limit_burst_size", 3000, true)? as u32,
        global_limit_per_second: rate("global_limit_per_second", 200, false)?,
        global_limit_burst_size: rate("global_limit_burst_size", 300, true)? as u32,
    };
    let e = section("export");
    let export = Export {
        retention_seconds: e.number("retention_seconds", 86400, 1, u64::MAX)?,
        cleanup_interval_seconds: e.number("cleanup_interval_seconds", 60, 1, 86400)?,
        max_concurrent_jobs: e.number("max_concurrent_jobs", 2, 1, 128)? as usize,
        max_queued_jobs: e.number("max_queued_jobs", 128, 1, 100000)? as usize,
        manifest_validation_concurrency: e.number("manifest_validation_concurrency", 8, 1, 4096)?
            as usize,
        object_read_concurrency: e.number("object_read_concurrency", 8, 1, 4096)? as usize,
        artifact_chunk_size_bytes: e.number(
            "artifact_chunk_size_bytes",
            16777216,
            4096,
            1024 * 1024 * 1024,
        )? as usize,
        artifact_write_concurrency: e.number("artifact_write_concurrency", 4, 1, 4096)? as usize,
        max_buffered_bytes_per_job: e.number(
            "max_buffered_bytes_per_job",
            134217728,
            4096,
            usize::MAX as u64 / 2,
        )? as usize,
        max_buffered_bytes_global: e.number(
            "max_buffered_bytes_global",
            268435456,
            4096,
            usize::MAX as u64 / 2,
        )? as usize,
        max_concurrent_downloads: e.number("max_concurrent_downloads", 32, 1, 4096)? as usize,
        max_retained_bytes: e.number("max_retained_bytes", 0, 0, u64::MAX)?,
    };
    let partition = section("index_config").number("partition_count", 64, 32, 256)? as u16;
    if !matches!(partition, 32 | 64 | 256) {
        return Err("[index_config] partition_count must be 32, 64 or 256".into());
    }
    Ok(N1Settings {
        source,
        server,
        control_dir,
        pools,
        repositories,
        upload,
        io_control,
        garbage_collector,
        auto_healing,
        rate_limits,
        export,
        max_file_size: section("set_limits").optional_size("max_file_size")?,
        login_token_expiration: section("jwt_token_config").number(
            "login_token_expiration",
            900,
            1,
            86400,
        )?,
        index_partition_count: partition,
    })
}
