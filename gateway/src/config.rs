use ini::Ini;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub host: String,
    pub https_port: u16,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub health_interval: Duration,
    pub request_timeout: Duration,
    pub nodes: Vec<NodeConfig>,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: String,
    pub http_url: String,
    pub native_url: String,
    pub weight: u32,
}

impl GatewayConfig {
    pub fn load() -> Result<Self, String> {
        let path = PathBuf::from("n2_gateway_config.ini");
        ensure_default_config(&path)
            .map_err(|err| format!("failed to create n2_gateway_config.ini: {err}"))?;
        Self::load_from_path(&path)
    }

    pub fn load_from_path(path: &Path) -> Result<Self, String> {
        let ini = Ini::load_from_file(&path)
            .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
        let server = ini.section(Some("server"));
        let routing = ini.section(Some("routing"));

        let host = read_string(server, "host", "0.0.0.0");
        let https_port = read_parse(server, "https_port", 51002u16)?;
        let cert_path = PathBuf::from(read_string(server, "cert_path", "./certificates/cert.pem"));
        let key_path = PathBuf::from(read_string(server, "key_path", "./certificates/key.pem"));
        let health_interval =
            Duration::from_secs(read_parse(routing, "health_interval_seconds", 5u64)?);
        let request_timeout =
            Duration::from_secs(read_parse(routing, "request_timeout_seconds", 300u64)?);
        let nodes = load_nodes(&ini)?;
        if nodes.is_empty() {
            return Err("n2_gateway_config.ini must define at least one [node.*] section".into());
        }

        validate_file_target(&cert_path, "cert_path")?;
        validate_file_target(&key_path, "key_path")?;

        Ok(Self {
            host,
            https_port,
            cert_path,
            key_path,
            health_interval,
            request_timeout,
            nodes,
        })
    }
}

fn read_string(section: Option<&ini::Properties>, key: &str, default_value: &str) -> String {
    section
        .and_then(|section| section.get(key))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default_value)
        .to_string()
}

fn read_parse<T>(
    section: Option<&ini::Properties>,
    key: &str,
    default_value: T,
) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match section
        .and_then(|section| section.get(key))
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(value) => value
            .parse::<T>()
            .map_err(|err| format!("invalid {key}: {err}")),
        None => Ok(default_value),
    }
}

fn load_nodes(ini: &Ini) -> Result<Vec<NodeConfig>, String> {
    let mut nodes = Vec::new();
    let mut seen = HashSet::new();
    for (section_name, props) in ini.iter() {
        let Some(name) = section_name else {
            continue;
        };
        let Some(id) = name.strip_prefix("node.") else {
            continue;
        };
        let id = props
            .get("id")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(id)
            .to_string();
        let http_url = required_node_value(&id, props, "http_url")?;
        let native_url = required_node_value(&id, props, "native_url")?;
        validate_http_url(&id, &http_url)?;
        validate_native_url(&id, &native_url)?;
        if !seen.insert(id.clone()) {
            return Err(format!("duplicate node id {id}"));
        }
        let weight = props
            .get("weight")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("1")
            .parse::<u32>()
            .map_err(|err| format!("node {id} invalid weight: {err}"))?
            .max(1);
        nodes.push(NodeConfig {
            id,
            http_url: http_url.trim_end_matches('/').to_string(),
            native_url: native_url.trim_end_matches('/').to_string(),
            weight,
        });
    }
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(nodes)
}

fn validate_http_url(id: &str, raw: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(raw)
        .map_err(|err| format!("node {id} invalid http_url '{}': {err}", raw.trim()))?;
    match url.scheme() {
        "http" | "https" => {}
        other => return Err(format!("node {id} http_url has unsupported scheme {other}")),
    }
    if url.host_str().is_none() {
        return Err(format!("node {id} http_url is missing a host"));
    }
    Ok(())
}

fn validate_native_url(id: &str, raw: &str) -> Result<(), String> {
    let Some(rest) = raw.strip_prefix("n2://") else {
        return Err(format!("node {id} native_url must start with n2://"));
    };
    let Some((host, port)) = rest.rsplit_once(':') else {
        return Err(format!("node {id} native_url must include host:port"));
    };
    if host.trim().is_empty() {
        return Err(format!("node {id} native_url is missing a host"));
    }
    let port = port.trim_end_matches('/');
    let parsed = port
        .parse::<u16>()
        .map_err(|err| format!("node {id} native_url has invalid port: {err}"))?;
    if parsed == 0 {
        return Err(format!("node {id} native_url port must be non-zero"));
    }
    Ok(())
}

fn validate_file_target(path: &Path, field: &str) -> Result<(), String> {
    if path.as_os_str().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    Ok(())
}

fn required_node_value(id: &str, props: &ini::Properties, key: &str) -> Result<String, String> {
    props
        .get(key)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .ok_or_else(|| format!("node {id} missing {key}"))
}

fn ensure_default_config(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        path,
        r#"[server]
host=0.0.0.0
https_port=51002
cert_path=./certificates/cert.pem
key_path=./certificates/key.pem

[routing]
health_interval_seconds=5
request_timeout_seconds=300

[node.node-a]
http_url=https://127.0.0.1:50001
native_url=n2://127.0.0.1:50002
weight=1
"#,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_config_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("n2-gateway-{name}-{}.ini", std::process::id()))
    }

    fn write_config(name: &str, body: &str) -> PathBuf {
        let path = temp_config_path(name);
        let _ = fs::remove_file(&path);
        fs::write(&path, body).unwrap();
        path
    }

    fn valid_config(nodes: &str) -> String {
        format!(
            r#"[server]
host=127.0.0.1
https_port=51002
cert_path=./certificates/cert.pem
key_path=./certificates/key.pem

[routing]
health_interval_seconds=5
request_timeout_seconds=30

{nodes}
"#
        )
    }

    #[test]
    fn valid_config_loads_multiple_nodes() {
        let path = write_config(
            "valid",
            &valid_config(
                r#"[node.node-b]
http_url=https://127.0.0.1:51003
native_url=n2://127.0.0.1:51004
weight=2

[node.node-a]
http_url=https://127.0.0.1:51001
native_url=n2://127.0.0.1:51002
weight=1
"#,
            ),
        );

        let config = GatewayConfig::load_from_path(&path).unwrap();
        assert_eq!(config.nodes.len(), 2);
        assert_eq!(config.nodes[0].id, "node-a");
        assert_eq!(config.nodes[1].weight, 2);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn duplicate_node_ids_are_rejected() {
        let path = write_config(
            "duplicate",
            &valid_config(
                r#"[node.first]
id=node-a
http_url=https://127.0.0.1:51001
native_url=n2://127.0.0.1:51002

[node.second]
id=node-a
http_url=https://127.0.0.1:51003
native_url=n2://127.0.0.1:51004
"#,
            ),
        );

        let err = GatewayConfig::load_from_path(&path).unwrap_err();
        assert!(err.contains("duplicate node id"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_node_urls_are_rejected() {
        let path = write_config(
            "invalid-url",
            &valid_config(
                r#"[node.node-a]
http_url=ftp://127.0.0.1:51001
native_url=https://127.0.0.1:51002
"#,
            ),
        );

        let err = GatewayConfig::load_from_path(&path).unwrap_err();
        assert!(err.contains("unsupported scheme") || err.contains("native_url"));
        let _ = fs::remove_file(path);
    }
}
