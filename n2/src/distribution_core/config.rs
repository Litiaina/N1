use crate::distribution_core::model::{ClusterCommitMode, CrossParams};
use crate::settings::constants::CONFIG_FILE;
use ini::Ini;
use once_cell::sync::OnceCell;
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum N2AutoMaintenanceMode {
    Off,
    Observe,
    RepairAndRedistribute,
}

impl N2AutoMaintenanceMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Observe => "observe",
            Self::RepairAndRedistribute => "repair_and_redistribute",
        }
    }
}

#[derive(Clone, Debug)]
pub struct N2RuntimeConfig {
    pub node_id: String,
    pub node_endpoint: String,
    pub membership_file: PathBuf,
    pub internal_token: String,
    pub cluster_commit_mode: ClusterCommitMode,
    pub auto_maintenance_mode: N2AutoMaintenanceMode,
    pub auto_maintenance_max_fragments: usize,
    pub auto_maintenance_max_objects_per_fragment: usize,
    pub auto_maintenance_cleanup_orphans: bool,
    pub auto_cross_params: bool,
    pub cluster_stripe_block_size: usize,
    pub remote_request_timeout_ms: u64,
    pub cross_params: CrossParams,
}

fn parse_u8_or_default(raw: Option<&str>, default: u8) -> u8 {
    raw.and_then(|v| v.parse::<u8>().ok()).unwrap_or(default)
}

fn parse_u64_or_default(raw: Option<&str>, default: u64) -> u64 {
    raw.and_then(|v| v.parse::<u64>().ok()).unwrap_or(default)
}

fn parse_usize_or_default(raw: Option<&str>, default: usize) -> usize {
    raw.and_then(|v| v.parse::<usize>().ok()).unwrap_or(default)
}

fn default_node_id() -> String {
    std::env::var("N2_NODE_ID").unwrap_or_else(|_| {
        hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string()
    })
}

fn default_node_endpoint() -> String {
    std::env::var("N2_NODE_ENDPOINT").unwrap_or_else(|_| "https://127.0.0.1:50001".to_string())
}

fn default_internal_token() -> String {
    std::env::var("N2_INTERNAL_TOKEN").unwrap_or_else(|_| "change-me-n2-internal-token".to_string())
}

fn default_auto_cross_params() -> bool {
    false
}

fn default_cluster_commit_mode() -> ClusterCommitMode {
    ClusterCommitMode::StrictFullPlacement
}

fn default_auto_maintenance_mode() -> N2AutoMaintenanceMode {
    N2AutoMaintenanceMode::Off
}

fn normalize_stripe_block_size(v: usize) -> usize {
    let clamped = v.clamp(4 * 1024, 4 * 1024 * 1024);
    ((clamped + 4095) / 4096) * 4096
}

fn parse_cluster_commit_mode(raw: Option<&str>, default: ClusterCommitMode) -> ClusterCommitMode {
    match raw.map(str::trim).filter(|v| !v.is_empty()) {
        Some(v) if v.eq_ignore_ascii_case("quorum") => ClusterCommitMode::Quorum,
        Some(v)
            if v.eq_ignore_ascii_case("strict_full_placement")
                || v.eq_ignore_ascii_case("strict")
                || v.eq_ignore_ascii_case("full_placement") =>
        {
            ClusterCommitMode::StrictFullPlacement
        }
        _ => default,
    }
}

fn parse_auto_maintenance_mode(
    raw: Option<&str>,
    default: N2AutoMaintenanceMode,
) -> N2AutoMaintenanceMode {
    match raw.map(str::trim).filter(|v| !v.is_empty()) {
        Some(v) if v.eq_ignore_ascii_case("off") => N2AutoMaintenanceMode::Off,
        Some(v) if v.eq_ignore_ascii_case("observe") => N2AutoMaintenanceMode::Observe,
        Some(v)
            if v.eq_ignore_ascii_case("repair")
                || v.eq_ignore_ascii_case("repair_and_redistribute")
                || v.eq_ignore_ascii_case("heal") =>
        {
            N2AutoMaintenanceMode::RepairAndRedistribute
        }
        _ => default,
    }
}

fn resolve_membership_path(raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        return path;
    }
    let cfg = PathBuf::from(CONFIG_FILE);
    if let Some(parent) = cfg.parent() {
        parent.join(path)
    } else {
        path
    }
}

fn load_runtime_config() -> N2RuntimeConfig {
    let defaults = N2RuntimeConfig {
        node_id: default_node_id(),
        node_endpoint: default_node_endpoint(),
        membership_file: resolve_membership_path("n2_cluster_membership.json"),
        internal_token: default_internal_token(),
        cluster_commit_mode: default_cluster_commit_mode(),
        auto_maintenance_mode: default_auto_maintenance_mode(),
        auto_maintenance_max_fragments: 64,
        auto_maintenance_max_objects_per_fragment: 256,
        auto_maintenance_cleanup_orphans: true,
        auto_cross_params: default_auto_cross_params(),
        cluster_stripe_block_size: 256 * 1024,
        remote_request_timeout_ms: 60_000,
        cross_params: CrossParams {
            kc: 1,
            mc: 0,
            kd: 1,
            md: 0,
        },
    };

    let Ok(conf) = Ini::load_from_file(CONFIG_FILE) else {
        return defaults;
    };
    let Some(sec) = conf.section(Some("n2")) else {
        return defaults;
    };

    let node_id = sec
        .get("node_id")
        .map(ToOwned::to_owned)
        .unwrap_or(defaults.node_id);
    let node_endpoint = sec
        .get("node_endpoint")
        .map(ToOwned::to_owned)
        .unwrap_or(defaults.node_endpoint);
    let membership_file = sec
        .get("cluster_membership_file")
        .map(resolve_membership_path)
        .unwrap_or(defaults.membership_file);
    let internal_token = sec
        .get("internal_token")
        .map(ToOwned::to_owned)
        .unwrap_or(defaults.internal_token);
    let cluster_commit_mode =
        parse_cluster_commit_mode(sec.get("cluster_commit_mode"), defaults.cluster_commit_mode);
    let auto_maintenance_mode = parse_auto_maintenance_mode(
        sec.get("auto_maintenance_mode"),
        defaults.auto_maintenance_mode,
    );
    let auto_maintenance_max_fragments = parse_usize_or_default(
        sec.get("auto_maintenance_max_fragments"),
        defaults.auto_maintenance_max_fragments,
    )
    .clamp(1, 4_096);
    let auto_maintenance_max_objects_per_fragment = parse_usize_or_default(
        sec.get("auto_maintenance_max_objects_per_fragment"),
        defaults.auto_maintenance_max_objects_per_fragment,
    )
    .clamp(1, 100_000);
    let auto_maintenance_cleanup_orphans = sec
        .get("auto_maintenance_cleanup_orphans")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(defaults.auto_maintenance_cleanup_orphans);
    let auto_cross_params = sec
        .get("auto_cross_params")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(defaults.auto_cross_params);
    let cluster_stripe_block_size = normalize_stripe_block_size(parse_usize_or_default(
        sec.get("cluster_stripe_block_size"),
        defaults.cluster_stripe_block_size,
    ));
    let remote_request_timeout_ms = parse_u64_or_default(
        sec.get("remote_request_timeout_ms"),
        defaults.remote_request_timeout_ms,
    )
    .clamp(1_000, 600_000);
    let cross_params = CrossParams {
        kc: parse_u8_or_default(sec.get("kc"), defaults.cross_params.kc),
        mc: parse_u8_or_default(sec.get("mc"), defaults.cross_params.mc),
        kd: parse_u8_or_default(sec.get("kd"), defaults.cross_params.kd),
        md: parse_u8_or_default(sec.get("md"), defaults.cross_params.md),
    };

    N2RuntimeConfig {
        node_id,
        node_endpoint,
        membership_file,
        internal_token,
        cluster_commit_mode,
        auto_maintenance_mode,
        auto_maintenance_max_fragments,
        auto_maintenance_max_objects_per_fragment,
        auto_maintenance_cleanup_orphans,
        auto_cross_params,
        cluster_stripe_block_size,
        remote_request_timeout_ms,
        cross_params,
    }
}

static N2_CONFIG: OnceCell<N2RuntimeConfig> = OnceCell::new();
static EFFECTIVE_CROSS_PARAMS: OnceCell<CrossParams> = OnceCell::new();

pub fn n2_runtime_config() -> &'static N2RuntimeConfig {
    N2_CONFIG.get_or_init(load_runtime_config)
}

fn auto_cluster_cross_params(active_nodes: usize) -> (u8, u8) {
    match active_nodes {
        0 | 1 => (1, 0),
        2 => (1, 1),
        n => ((n.saturating_sub(1)).min(u8::MAX as usize) as u8, 1),
    }
}

fn auto_local_cross_params(blob_roots: usize) -> (u8, u8) {
    let _ = blob_roots;
    (1, 0)
}

pub fn validate_cross_params(
    params: CrossParams,
    active_nodes: usize,
    blob_roots: usize,
) -> Result<(), String> {
    if params.kc == 0 {
        return Err("kc must be at least 1".to_string());
    }
    if params.kd != 1 || params.md != 0 {
        return Err(format!(
            "N2 0.4 uses one cluster-level EC layer; local N1 Edge shard storage must be kd=1 md=0, got kd={} md={}",
            params.kd, params.md
        ));
    }

    let cluster_width = usize::from(params.kc).saturating_add(usize::from(params.mc));
    if cluster_width == 0 {
        return Err("kc+mc must be at least 1".to_string());
    }
    if active_nodes == 0 {
        return Err("membership has zero active nodes".to_string());
    }
    if cluster_width > active_nodes {
        return Err(format!(
            "kc+mc={} exceeds active node count {}",
            cluster_width, active_nodes
        ));
    }

    if blob_roots == 0 {
        return Err("blob_store_disks has zero usable roots".to_string());
    }

    Ok(())
}

pub fn resolve_effective_cross_params(
    active_nodes: usize,
    blob_roots: usize,
) -> Result<(CrossParams, &'static str), String> {
    let cfg = n2_runtime_config();
    let params = if cfg.auto_cross_params {
        let (kc, mc) = auto_cluster_cross_params(active_nodes);
        let (kd, md) = auto_local_cross_params(blob_roots);
        CrossParams { kc, mc, kd, md }
    } else {
        cfg.cross_params
    };
    validate_cross_params(params, active_nodes, blob_roots)?;
    let source = if cfg.auto_cross_params {
        "auto"
    } else {
        "configured"
    };
    if let Some(existing) = EFFECTIVE_CROSS_PARAMS.get() {
        if *existing != params {
            return Err(format!(
                "effective cross params already initialized as kc={} mc={} kd={} md={}, cannot replace with kc={} mc={} kd={} md={}",
                existing.kc,
                existing.mc,
                existing.kd,
                existing.md,
                params.kc,
                params.mc,
                params.kd,
                params.md
            ));
        }
    } else {
        let _ = EFFECTIVE_CROSS_PARAMS.set(params);
    }
    Ok((params, source))
}

pub fn effective_cross_params() -> CrossParams {
    EFFECTIVE_CROSS_PARAMS
        .get()
        .copied()
        .unwrap_or_else(|| n2_runtime_config().cross_params)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_cross_params_rejects_too_wide_cluster_layout() {
        let err = validate_cross_params(
            CrossParams {
                kc: 2,
                mc: 1,
                kd: 1,
                md: 0,
            },
            2,
            2,
        )
        .expect_err("cluster width should fail");
        assert!(err.contains("kc+mc=3 exceeds active node count 2"));
    }

    #[test]
    fn validate_cross_params_rejects_second_local_ec_layer() {
        let err = validate_cross_params(
            CrossParams {
                kc: 1,
                mc: 0,
                kd: 3,
                md: 1,
            },
            3,
            2,
        )
        .expect_err("second local EC layer should fail");
        assert!(err.contains("kd=1 md=0"));
    }

    #[test]
    fn auto_layout_uses_single_parity_when_capacity_exists() {
        let (kc, mc) = auto_cluster_cross_params(3);
        let (kd, md) = auto_local_cross_params(4);
        assert_eq!((kc, mc), (2, 1));
        assert_eq!((kd, md), (1, 0));
    }

    #[test]
    fn cluster_commit_mode_parser_accepts_strict_aliases() {
        assert_eq!(
            parse_cluster_commit_mode(Some("strict_full_placement"), ClusterCommitMode::Quorum),
            ClusterCommitMode::StrictFullPlacement
        );
        assert_eq!(
            parse_cluster_commit_mode(Some("strict"), ClusterCommitMode::Quorum),
            ClusterCommitMode::StrictFullPlacement
        );
        assert_eq!(
            parse_cluster_commit_mode(Some("quorum"), ClusterCommitMode::StrictFullPlacement),
            ClusterCommitMode::Quorum
        );
    }

    #[test]
    fn auto_maintenance_mode_parser_accepts_repair_aliases() {
        assert_eq!(
            parse_auto_maintenance_mode(Some("observe"), N2AutoMaintenanceMode::Off),
            N2AutoMaintenanceMode::Observe
        );
        assert_eq!(
            parse_auto_maintenance_mode(Some("repair"), N2AutoMaintenanceMode::Off),
            N2AutoMaintenanceMode::RepairAndRedistribute
        );
        assert_eq!(
            parse_auto_maintenance_mode(
                Some("repair_and_redistribute"),
                N2AutoMaintenanceMode::Off
            ),
            N2AutoMaintenanceMode::RepairAndRedistribute
        );
    }
}
