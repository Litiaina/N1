use crate::settings::constants::CONFIG_FILE;
use crate::smb::config::{
    SmbConfig, clamp_query_directory_max_buffered_entries,
    clamp_query_directory_max_seeded_entries, clamp_query_directory_page_limit,
};
use ini::Ini;
use sha2::{Digest as ShaDigest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub metadata_db_path: PathBuf,
    pub staging_root: PathBuf,
    pub share_root: PathBuf,
    pub read_only_default: bool,
}

#[derive(Clone, Debug)]
pub struct N1Config {
    pub base_url: Option<String>,
    pub accept_invalid_certs: bool,
    pub index_roots: Vec<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct SyncConfig {
    pub enabled: bool,
    pub scan_interval_seconds: u64,
    pub request_timeout_seconds: u64,
    pub min_stable_age_seconds: u64,
    pub cold_n1_prefix: String,
}

#[derive(Clone, Debug)]
pub struct FragmentPolicyConfig {
    pub root_path: Option<PathBuf>,
    pub read_only: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct FragmentSyncConfig {
    pub enabled: bool,
    pub secret: Option<String>,
    pub n1_base_url: Option<String>,
    pub cold_n1_prefix: String,
}

#[derive(Clone, Debug)]
pub struct NsConfig {
    pub smb: SmbConfig,
    pub storage: StorageConfig,
    pub n1: N1Config,
    pub sync: SyncConfig,
    pub fragment_aliases: HashMap<String, String>,
    pub fragment_policies: HashMap<String, FragmentPolicyConfig>,
    pub fragment_syncs: HashMap<String, FragmentSyncConfig>,
}

fn parse_bool(value: Option<&str>, default: bool) -> bool {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn parse_u64(value: Option<&str>) -> Option<u64> {
    value.and_then(|value| value.trim().parse::<u64>().ok())
}

fn parse_usize(value: Option<&str>) -> Option<usize> {
    value.and_then(|value| value.trim().parse::<usize>().ok())
}

fn optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn is_fragment_hash_identifier(value: &str) -> bool {
    value.len() == 64 && value.as_bytes().iter().all(|b| b.is_ascii_hexdigit())
}

fn fragment_identifier_to_hash(identifier: &str) -> Option<String> {
    let normalized = identifier.trim();
    if normalized.is_empty() {
        return None;
    }
    if is_fragment_hash_identifier(normalized) {
        return Some(normalized.to_ascii_lowercase());
    }
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    Some(hex::encode(hasher.finalize()))
}

fn fragment_identifier_to_configured_hash(
    identifier: &str,
    fragment_aliases: &HashMap<String, String>,
) -> Option<String> {
    let normalized = normalize_fragment_alias_key(identifier);
    if normalized.is_empty() {
        return None;
    }
    fragment_aliases
        .get(normalized.as_str())
        .cloned()
        .or_else(|| fragment_identifier_to_hash(identifier))
}

fn normalize_fragment_alias_key(identifier: &str) -> String {
    identifier.trim().to_ascii_lowercase()
}

fn parse_csv_paths(value: Option<&str>) -> Vec<PathBuf> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(PathBuf::from)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

pub fn load_config() -> Result<NsConfig, String> {
    let ini = Ini::load_from_file(CONFIG_FILE)
        .map_err(|err| format!("failed to load {}: {}", CONFIG_FILE, err))?;

    let storage_section = ini.section(Some("storage"));
    let n1_section = ini.section(Some("n1"));
    let sync_section = ini.section(Some("sync"));
    let smb_section = ini.section(Some("smb"));
    let auth_section = ini.section(Some("auth"));

    let mut smb = SmbConfig::default();
    if let Some(section) = smb_section {
        if let Some(value) = section.get("enabled") {
            smb.enabled = parse_bool(Some(value), smb.enabled);
        }
        if let Some(value) = section.get("bind_addr") {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                smb.bind_addr = trimmed.to_string();
            }
        }
        if let Some(value) = section
            .get("smb_port")
            .and_then(|value| value.parse::<u16>().ok())
        {
            smb.smb_port = value;
        }
        if let Some(value) = parse_usize(section.get("max_sessions")) {
            smb.max_sessions = value.clamp(64, 65_536);
        }
        if let Some(value) = parse_usize(section.get("max_frame_bytes")) {
            smb.max_frame_bytes = value.clamp(4 * 1024, 64 * 1024 * 1024);
        }
        if let Some(value) = parse_usize(section.get("memory_budget_bytes")) {
            smb.memory_budget_bytes = value
                .clamp(8 * 1024 * 1024, 4 * 1024 * 1024 * 1024usize)
                .max(smb.max_frame_bytes);
        }
        if let Some(value) = section.get("async_close_commit") {
            smb.async_close_commit = parse_bool(Some(value), smb.async_close_commit);
        }
        if let Some(value) = parse_u64(section.get("async_close_threshold_bytes")) {
            smb.async_close_threshold_bytes =
                value.clamp(1 * 1024 * 1024, 1024 * 1024 * 1024 * 1024);
        }
        if let Some(value) = section.get("require_signing") {
            smb.require_signing = parse_bool(Some(value), smb.require_signing);
        }
        if let Some(value) = parse_usize(section.get("query_directory_page_limit")) {
            smb.query_directory_page_limit = clamp_query_directory_page_limit(value);
        }
        if let Some(value) = parse_usize(section.get("query_directory_max_buffered_entries")) {
            smb.query_directory_max_buffered_entries =
                clamp_query_directory_max_buffered_entries(value);
        }
        if let Some(value) = parse_usize(section.get("query_directory_max_seeded_entries")) {
            smb.query_directory_max_seeded_entries = clamp_query_directory_max_seeded_entries(
                value,
                smb.query_directory_max_buffered_entries,
            );
        }
        if let Some(value) = section.get("auth_domain") {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                smb.auth_domain = trimmed.to_string();
            }
        }
        if let Some(value) = section.get("allow_guest") {
            smb.allow_guest = parse_bool(Some(value), smb.allow_guest);
        }
    }

    if let Some(section) = auth_section {
        if let Some(value) = section.get("domain") {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                smb.auth_domain = trimmed.to_string();
            }
        }
        if let Some(value) = section.get("allow_guest") {
            smb.allow_guest = parse_bool(Some(value), smb.allow_guest);
        }
    }

    let storage = StorageConfig {
        metadata_db_path: storage_section
            .and_then(|section| section.get("metadata_db_path"))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("ns-data").join("metadata.ndxdb")),
        staging_root: storage_section
            .and_then(|section| section.get("staging_root"))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("ns-data").join("staging")),
        share_root: storage_section
            .and_then(|section| section.get("share_root"))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("ns-shares").join("fragments")),
        read_only_default: storage_section
            .map(|section| parse_bool(section.get("read_only"), false))
            .unwrap_or(false),
    };

    let n1 = N1Config {
        base_url: n1_section
            .and_then(|section| optional_string(section.get("base_url")))
            .or_else(|| {
                sync_section.and_then(|section| optional_string(section.get("n1_base_url")))
            }),
        accept_invalid_certs: n1_section
            .map(|section| parse_bool(section.get("accept_invalid_certs"), true))
            .unwrap_or(true),
        index_roots: n1_section
            .map(|section| {
                parse_csv_paths(
                    section
                        .get("index_roots")
                        .or_else(|| section.get("index_store_disks")),
                )
            })
            .unwrap_or_default(),
    };

    let sync = SyncConfig {
        enabled: sync_section
            .map(|section| parse_bool(section.get("enabled"), false))
            .unwrap_or(false),
        scan_interval_seconds: sync_section
            .and_then(|section| parse_u64(section.get("scan_interval_seconds")))
            .unwrap_or(300)
            .clamp(30, 24 * 60 * 60),
        request_timeout_seconds: sync_section
            .and_then(|section| parse_u64(section.get("request_timeout_seconds")))
            .unwrap_or(300)
            .clamp(15, 24 * 60 * 60),
        min_stable_age_seconds: sync_section
            .and_then(|section| parse_u64(section.get("min_stable_age_seconds")))
            .unwrap_or(120)
            .min(24 * 60 * 60),
        cold_n1_prefix: sync_section
            .and_then(|section| optional_string(section.get("cold_n1_prefix")))
            .unwrap_or_default()
            .trim_matches('/')
            .to_string(),
    };

    let mut fragment_aliases = HashMap::new();
    for (section_name, properties) in ini.iter() {
        let Some(section_name) = section_name else {
            continue;
        };
        if section_name == "fragment_aliases" || section_name == "share_aliases" {
            for (alias, target) in properties.iter() {
                let alias = normalize_fragment_alias_key(alias);
                if alias.is_empty() {
                    continue;
                }
                let Some(fragment_hash) = fragment_identifier_to_hash(target) else {
                    continue;
                };
                fragment_aliases.insert(alias, fragment_hash);
            }
            continue;
        }

        if let Some(alias) = section_name
            .strip_prefix("fragment_alias.")
            .or_else(|| section_name.strip_prefix("share_alias."))
        {
            let alias = normalize_fragment_alias_key(alias);
            if alias.is_empty() {
                continue;
            }
            let Some(fragment_hash) = properties
                .get("hash")
                .or_else(|| properties.get("fragment"))
                .and_then(fragment_identifier_to_hash)
            else {
                return Err(format!("{} is missing hash", section_name));
            };
            fragment_aliases.insert(alias, fragment_hash);
        }
    }

    let mut fragment_policies = HashMap::new();
    let mut fragment_syncs = HashMap::new();
    for (section_name, properties) in ini.iter() {
        let Some(section_name) = section_name else {
            continue;
        };

        if let Some(identifier) = section_name
            .strip_prefix("fragment_policy.")
            .or_else(|| section_name.strip_prefix("share_policy."))
        {
            let identifier = identifier.trim();
            if identifier.is_empty() {
                continue;
            }
            let Some(fragment_hash) =
                fragment_identifier_to_configured_hash(identifier, &fragment_aliases)
            else {
                continue;
            };
            fragment_policies.insert(
                fragment_hash.clone(),
                FragmentPolicyConfig {
                    root_path: properties
                        .get("root")
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(PathBuf::from),
                    read_only: properties
                        .get("read_only")
                        .map(|value| parse_bool(Some(value), false)),
                },
            );
            continue;
        }

        if let Some(identifier) = section_name
            .strip_prefix("fragment_sync.")
            .or_else(|| section_name.strip_prefix("share_sync."))
        {
            let identifier = identifier.trim();
            if identifier.is_empty() {
                continue;
            }
            let Some(fragment_hash) =
                fragment_identifier_to_configured_hash(identifier, &fragment_aliases)
            else {
                continue;
            };
            let enabled = parse_bool(properties.get("enabled"), true);
            let secret = properties
                .get("secret")
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            if enabled && secret.is_none() {
                return Err(format!("{} is enabled but missing secret", section_name));
            }
            fragment_syncs.insert(
                fragment_hash.clone(),
                FragmentSyncConfig {
                    enabled,
                    secret,
                    n1_base_url: properties
                        .get("n1_base_url")
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned),
                    cold_n1_prefix: properties
                        .get("cold_n1_prefix")
                        .map(str::trim)
                        .unwrap_or_default()
                        .trim_matches('/')
                        .to_string(),
                },
            );
        }
    }

    if !smb.allow_guest && n1.index_roots.is_empty() {
        return Err(
            "NS dynamic fragment authentication requires [n1].index_roots or index_store_disks"
                .to_string(),
        );
    }

    Ok(NsConfig {
        smb,
        storage,
        n1,
        sync,
        fragment_aliases,
        fragment_policies,
        fragment_syncs,
    })
}

impl NsConfig {
    pub fn fragment_identifier_hash(&self, identifier: &str) -> Option<String> {
        fragment_identifier_to_configured_hash(identifier, &self.fragment_aliases)
    }

    pub fn fragment_policy(&self, fragment_hash: &str) -> Option<&FragmentPolicyConfig> {
        self.fragment_policies
            .get(fragment_hash.trim().to_ascii_lowercase().as_str())
    }

    pub fn fragment_sync(&self, fragment_hash: &str) -> Option<&FragmentSyncConfig> {
        self.fragment_syncs
            .get(fragment_hash.trim().to_ascii_lowercase().as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FRAGMENT_HASH: &str = "0de266d684b84fe81dc1d792b2fb50577823b4164839ab622203bb61edd3341b";

    #[test]
    fn configured_alias_resolves_to_target_fragment_hash() {
        let mut aliases = HashMap::new();
        aliases.insert("main".to_string(), FRAGMENT_HASH.to_string());

        assert_eq!(
            fragment_identifier_to_configured_hash("main", &aliases).as_deref(),
            Some(FRAGMENT_HASH)
        );
        assert_eq!(
            fragment_identifier_to_configured_hash("MAIN", &aliases).as_deref(),
            Some(FRAGMENT_HASH)
        );
    }

    #[test]
    fn unaliased_fragment_name_still_resolves_deterministically() {
        let aliases = HashMap::new();

        assert_eq!(
            fragment_identifier_to_configured_hash("main", &aliases).as_deref(),
            fragment_identifier_to_hash("main").as_deref()
        );
    }
}
