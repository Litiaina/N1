use super::registry::Registry;
use super::*;
use crate::{
    core::maintenance::jobs::{MaintenanceConfig, Schedule},
    middleware::rate_limiter_middleware::RateLimitSettings,
    settings::{
        config::{CONFIG_FORMAT, ConcurrencyConfig, Fragment, Tls},
        runtime::RuntimeConfig,
    },
};
use std::{collections::BTreeMap, net::SocketAddr};

pub(super) fn validate(settings: &N1Settings) -> Result<()> {
    runtime(settings).validate()?;
    // Validate scheduling even before the first pool marker is written.
    maintenance(settings).validate()?;
    for pool in &settings.pools {
        n1_link::PoolLayout {
            id: n1_link::PoolId([0; 16]),
            generation: pool.generation,
            data_shards: pool.data_shards,
            parity_shards: pool.parity_shards,
            block_bytes: settings.upload.stripe_block_size,
            placement_version: 1,
            shard_format: n1_link::ShardFormat::Crc32cSha256V2,
        }
        .validate()?;
    }
    Ok(())
}

fn runtime(s: &N1Settings) -> RuntimeConfig {
    RuntimeConfig {
        io_control: crate::io_control::IoControlConfig {
            foreground_wait_timeout_ms: s.io_control.foreground_wait_timeout_ms,
            stall_threshold_ms: s.io_control.stall_threshold_ms,
            slow_io_threshold_ms: s.io_control.slow_io_threshold_ms,
            latency_samples_per_class: s.io_control.latency_samples_per_class,
        },
        writer_buffer_size: s.upload.writer_buffer_size,
        upload_io_gate_enabled: s.upload.upload_io_gate_enabled,
        upload_part_strict_durability_default: s.upload.upload_part_strict_durability_default,
        max_part_size: s.upload.max_part_size,
        max_file_size: s.max_file_size,
        stream_locality_bias_enabled: s.upload.stream_locality_bias_enabled,
        staging_min_age_seconds: s.garbage_collector.file_ttl_seconds,
        scheduled_scrub_pause_millis: s.auto_healing.scheduled_scrub_pause_millis,
        object_scan_concurrency: s.auto_healing.object_scan_concurrency,
        shard_repair_concurrency: s.auto_healing.shard_repair_concurrency,
        metadata_batch_size: s.auto_healing.metadata_batch_size,
        topology_check_interval_seconds: s.auto_healing.topology_check_interval_seconds,
        topology_recovery_stability_seconds: s.auto_healing.topology_recovery_stability_seconds,
        monitor_interval_seconds: s.upload.monitor_interval_seconds,
        monitor_enabled: true,
        monitor_deep_scan_interval_seconds: s.upload.monitor_deep_scan_interval_seconds,
        login_token_expiration: s.login_token_expiration,
        ..Default::default()
    }
}

fn maintenance(s: &N1Settings) -> MaintenanceConfig {
    MaintenanceConfig {
        gc: Some(Schedule {
            every_seconds: s.garbage_collector.cleanup_interval_seconds,
            start_delay_seconds: s.garbage_collector.cleanup_interval_seconds,
        }),
        scrub: None,
        calendar_scrub: (s.auto_healing.enabled && !s.auto_healing.scrub_schedule_utc.is_empty())
            .then(|| crate::core::auto_healing::schedule::CalendarScrub {
                schedule_utc: s.auto_healing.scrub_schedule_utc.clone(),
                no_scan_timeout: true,
                initial_delay_seconds: s.auto_healing.initial_scan_delay_seconds,
                // No whole-scan timeout: the calendar controls when a job begins.
                options: n1_link::noa::ScrubOptions {
                    repair: true,
                    bytes_per_second: 4 * 1024 * 1024 * 1024,
                    window_seconds: 7 * 86400,
                },
            }),
    }
}

pub(super) fn config(
    s: &N1Settings,
    env: &BTreeMap<String, String>,
    registry: &Registry,
) -> Result<Config> {
    validate(s)?;
    let secret_hash = registry.principal_hash(
        env.get("AUTH_KEY")
            .ok_or_else(|| Error::Invalid("AUTH_KEY is required".into()))?,
    )?;
    let r = &s.rate_limits;
    let mut controls = runtime(s);
    let seconds = |name: &str, default: u64| -> Result<u64> {
        env.get(name).map_or(Ok(default), |value| {
            value
                .parse()
                .map_err(|_| Error::Invalid(format!("{name} must be seconds as an integer")))
        })
    };
    controls.login_token_expiration =
        seconds("ACCESS_TOKEN_DEFAULT_TTL_SECS", s.login_token_expiration)?;
    controls.access_token_max_ttl_seconds = seconds("ACCESS_TOKEN_MAX_TTL_SECS", 3600)?;
    controls.refresh_token_ttl_seconds = seconds("REFRESH_TOKEN_TTL_SECS", 604800)?;
    let config = Config {
        format: CONFIG_FORMAT.into(),
        native_address: SocketAddr::new(s.server.host, s.server.native_port),
        http_address: Some(SocketAddr::new(s.server.host, s.server.https_port)),
        signing_key_file: s.control_dir.join("link.signing-key"),
        state_dir: Some(s.control_dir.clone()),
        max_connections: s.server.native_max_connections,
        max_operations: s.upload.max_concurrent_io,
        operation_memory_bytes: s.upload.upload_memory_budget_bytes,
        read_repair: true,
        concurrency: ConcurrencyConfig {
            adaptive_enabled: s.upload.adaptive_concurrency_enabled,
            interval_ms: s.upload.adaptive_concurrency_interval_ms,
            max_streams: s.upload.stream_max_concurrency,
            stream_chunk_bytes: s.upload.stream_chunk_size,
            stream_readahead_bytes: s.upload.stream_readahead_bytes,
        },
        rate_limits: RateLimitSettings {
            enabled: r.enable_rate_limiting,
            auth_per_sec: r.auth_limit_per_second,
            auth_burst: r.auth_limit_burst_size,
            view_per_sec: r.view_limit_per_second,
            view_burst: r.view_limit_burst_size,
            upload_per_sec: r.upload_limit_per_second,
            upload_burst: r.upload_limit_burst_size,
            global_per_sec: r.global_limit_per_second,
            global_burst: r.global_limit_burst_size,
            ..Default::default()
        },
        repositories: s
            .repositories
            .iter()
            .map(|repo| {
                (
                    repo.alias.clone(),
                    crate::core::backup::network::ConfiguredRepository {
                        path: repo.path.clone(),
                        key_file: repo.key_file.clone(),
                    },
                )
            })
            .collect(),
        maintenance: maintenance(s),
        runtime: controls,
        tls: s
            .server
            .cert_path
            .clone()
            .zip(s.server.key_path.clone())
            .map(|(certificate, private_key)| Tls {
                certificate,
                private_key,
            }),
        pools: s
            .pools
            .iter()
            .map(|pool| {
                let mut config = registry.pools[&pool.name].config.clone();
                config.roots = pool.roots.clone();
                config.sealed = pool.sealed;
                config
            })
            .collect(),
        fragments: std::iter::once(Fragment {
            id: registry.principal,
            alias: registry.principal_alias.clone(),
            secret_hash,
            permissions: registry.permissions,
            operator: registry.operator,
        })
        .chain(registry.additional_fragments.iter().cloned())
        .collect(),
    };
    config.validate()?;
    Ok(config)
}
