use ini::Ini;
use std::{io, path::Path};

pub fn create_default_config_ini<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let mut conf = Ini::new();

    conf.with_section(Some("storage"))
        .set("metadata_db_path", "ns-data/metadata.ndxdb")
        .set("staging_root", "ns-data/staging")
        .set("share_root", "ns-shares/fragments")
        .set("read_only", "false");

    conf.with_section(Some("n1"))
        .set("base_url", "https://127.0.0.1:50001")
        .set("native_url", "n1://127.0.0.1:50002")
        .set("accept_invalid_certs", "true")
        .set("index_roots", "/srv/litiaina/n1/index");

    conf.with_section(Some("auth"))
        .set("domain", "LITI")
        .set("allow_guest", "false");

    conf.with_section(Some("admin"))
        .set("enabled", "true")
        .set("bind_addr", "127.0.0.1")
        .set("port", "50730")
        .set("admin_key", "");

    conf.with_section(Some("sync"))
        .set("enabled", "false")
        .set("scan_interval_seconds", "300")
        .set("request_timeout_seconds", "300")
        .set("min_stable_age_seconds", "120")
        .set("cold_n1_prefix", "");

    conf.with_section(Some("smb"))
        .set("enabled", "true")
        .set("bind_addr", "0.0.0.0")
        .set("smb_port", "445")
        .set("max_sessions", "4096")
        .set("max_frame_bytes", "16777216")
        .set("memory_budget_bytes", "134217728")
        .set("async_close_commit", "false")
        .set("async_close_threshold_bytes", "67108864")
        .set("require_signing", "false")
        .set("auth_domain", "LITI")
        .set("allow_guest", "false")
        .set("query_directory_page_limit", "1000")
        .set("query_directory_max_buffered_entries", "4096")
        .set("query_directory_max_seeded_entries", "512");

    conf.with_section(Some("disk.local"))
        .set("root_path", "ns-shares/fragments")
        .set("enabled", "true");

    conf.with_section(Some("share.public"))
        .set("disk_id", "local")
        .set("read_only", "false")
        .set("sync_enabled", "false");

    conf.write_to_file(path).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_config_path(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "litiaina-ns-{}-{}-{}.ini",
            name,
            std::process::id(),
            unique
        ))
    }

    #[test]
    fn default_config_includes_standard_ns_sections() {
        let path = temp_config_path("default-config");
        create_default_config_ini(&path).expect("create default ns config");

        let ini = Ini::load_from_file(&path).expect("load generated ns config");
        assert!(ini.section(Some("storage")).is_some());
        assert!(ini.section(Some("n1")).is_some());
        assert!(ini.section(Some("auth")).is_some());
        assert!(ini.section(Some("admin")).is_some());
        assert!(ini.section(Some("sync")).is_some());
        assert!(ini.section(Some("smb")).is_some());
        assert!(ini.section(Some("disk.local")).is_some());
        assert!(ini.section(Some("share.public")).is_some());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn default_config_enables_smb_and_dynamic_fragment_auth() {
        let path = temp_config_path("smb-share");
        create_default_config_ini(&path).expect("create default ns config");

        let ini = Ini::load_from_file(&path).expect("load generated ns config");
        let smb = ini.section(Some("smb")).expect("smb section");
        let storage = ini.section(Some("storage")).expect("storage section");
        let n1 = ini.section(Some("n1")).expect("n1 section");
        let sync = ini.section(Some("sync")).expect("sync section");

        assert_eq!(smb.get("enabled"), Some("true"));
        assert_eq!(smb.get("auth_domain"), Some("LITI"));
        assert_eq!(storage.get("share_root"), Some("ns-shares/fragments"));
        assert_eq!(n1.get("base_url"), Some("https://127.0.0.1:50001"));
        assert_eq!(sync.get("min_stable_age_seconds"), Some("120"));
        assert_eq!(sync.get("cold_n1_prefix"), Some(""));

        let _ = fs::remove_file(path);
    }
}
