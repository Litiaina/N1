use super::*;
use std::fs;
fn fixture() -> (PathBuf, impl Drop) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let root = std::env::temp_dir().join(format!(
        "n1-settings-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    let path = root.join("n1_config.ini");
    create_default_config_ini(&path).unwrap();
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    ini.with_section(Some("disk_config"))
        .set("fs_storage_path", root.join("control").to_str().unwrap());
    ini.with_section(Some("storage_pool.A"))
        .set("data_shards", "1")
        .set(
            "roots",
            format!(
                "{},{}",
                root.join("member0").display(),
                root.join("member1").display()
            ),
        );
    ini.write_to_file(&path).unwrap();
    (path, Cleanup(root))
}
#[test]
fn original_controls_are_loaded_without_renaming_or_json() {
    let (path, _guard) = fixture();
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    ini.with_section(Some("server"))
        .set("host", "192.168.0.12")
        .set("https_port", "21001")
        .set("native_port", "21002")
        .set("native_max_connections", "81")
        .set("cert_path", "server.pem")
        .set("key_path", "server.key");
    ini.with_section(Some("upload_config"))
        .set("max_concurrent_io", "7")
        .set("adaptive_concurrency_enabled", "false")
        .set("adaptive_concurrency_interval_ms", "5300")
        .set("upload_memory_budget_bytes", "268435456")
        .set("stream_max_concurrency", "13")
        .set("stream_chunk_size", "131072")
        .set("stream_readahead_bytes", "393216")
        .set("upload_part_strict_durability_default", "true")
        .set("upload_io_gate_enabled", "false");
    ini.with_section(Some("io_control"))
        .set("foreground_wait_timeout_ms", "1300")
        .set("stall_threshold_ms", "2300")
        .set("slow_io_threshold_ms", "90")
        .set("latency_samples_per_class", "257");
    ini.write_to_file(&path).unwrap();
    let config = N1Settings::load(&path).unwrap();
    assert_eq!(config.server.host.to_string(), "192.168.0.12");
    assert_eq!(config.server.https_port, 21001);
    assert_eq!(config.server.native_port, 21002);
    assert_eq!(config.server.native_max_connections, 81);
    assert_eq!(
        config.server.cert_path,
        Some(path.parent().unwrap().join("server.pem"))
    );
    assert_eq!(config.upload.max_concurrent_io, 7);
    assert!(!config.upload.adaptive_concurrency_enabled);
    assert_eq!(config.upload.adaptive_concurrency_interval_ms, 5300);
    assert_eq!(config.upload.stream_max_concurrency, 13);
    assert_eq!(config.upload.stream_readahead_bytes, 393216);
    assert!(config.upload.upload_part_strict_durability_default);
    assert!(!config.upload.upload_io_gate_enabled);
    assert_eq!(config.io_control.foreground_wait_timeout_ms, 1300);
    assert_eq!(config.io_control.latency_samples_per_class, 257);
}
#[test]
fn adding_pool_b_preserves_pool_a_configuration() {
    let (path, _guard) = fixture();
    let first = N1Settings::load(&path).unwrap();
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    let base = path.parent().unwrap();
    ini.with_section(Some("storage_pool.B"))
        .set("generation", "1")
        .set(
            "roots",
            format!(
                "{},{}",
                base.join("b0").display(),
                base.join("b1").display()
            ),
        )
        .set("data_shards", "1")
        .set("parity_shards", "1")
        .set("sealed", "false");
    ini.write_to_file(&path).unwrap();
    let updated = N1Settings::load(&path).unwrap();
    assert_eq!(updated.pools.len(), 2);
    assert_eq!(updated.pools[0], first.pools[0]);
    assert_eq!(updated.pools[1].name, "B");
}
#[test]
fn metadata_load_does_not_require_or_parse_object_pool_members() {
    let (path, _guard) = fixture();
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    ini.delete(Some("storage_pool.A"));
    ini.write_to_file(&path).unwrap();
    let metadata = N1Settings::load_metadata(&path).unwrap();
    assert!(metadata.pools.is_empty());
    assert!(N1Settings::load(&path).is_err());

    // Member paths and geometry belong to the independent object process.
    ini.with_section(Some("storage_pool.remote"))
        .set("roots", "/unmounted/object/members")
        .set("data_shards", "invalid");
    ini.write_to_file(&path).unwrap();
    assert!(N1Settings::load_metadata(&path).unwrap().pools.is_empty());
    assert!(N1Settings::load(&path).is_err());
}
#[test]
fn invalid_limits_and_overlapping_roots_are_rejected() {
    for (section, key, value) in [
        ("server", "native_port", "50001"),
        ("upload_config", "max_concurrent_io", "invalid"),
        ("upload_config", "adaptive_concurrency_interval_ms", "1"),
        ("rate_limiter_config", "auth_limit_per_second", "0"),
        ("io_control", "foreground_wait_timeout_ms", "0"),
        ("upload_config", "stripe_block_size", "250000"),
    ] {
        let (path, _guard) = fixture();
        let mut ini = ini::Ini::load_from_file(&path).unwrap();
        ini.with_section(Some(section)).set(key, value);
        ini.write_to_file(&path).unwrap();
        assert!(
            N1Settings::load(&path).is_err(),
            "{section}.{key} accepted {value}"
        );
    }
}
