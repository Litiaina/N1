use super::super::{Result, invalid};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Service {
    pub unit: String,
    #[serde(default)]
    pub user: bool,
}
impl Service {
    pub fn validate(&self) -> Result<()> {
        if self.unit.len() > 200
            || !self.unit.ends_with(".service")
            || self.unit.starts_with('-')
            || !self
                .unit
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.@".contains(&b))
        {
            return Err(invalid(
                "engine service must be a plain systemd .service unit name",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Replica {
    pub repository: PathBuf,
    pub key_file: PathBuf,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    #[serde(default)]
    pub allow_plaintext: bool,
    /// Zero disables automatic destination retention, independently of source.
    #[serde(default)]
    pub keep_last: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub service: Service,
    pub engine_config: PathBuf,
    pub repository: PathBuf,
    pub key_file: PathBuf,
    pub state_dir: PathBuf,
    pub workers: usize,
    pub capture_timeout_seconds: u64,
    pub operation_timeout_seconds: u64,
    pub service_timeout_seconds: u64,
    pub replication_attempts: usize,
    pub keep_last: usize,
    pub max_age_seconds: u64,
    pub replica: Option<Replica>,
    /// Some = committed-state online capture; None = full offline recovery.
    pub online_source: Option<super::super::network::Destination>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            service: Service {
                unit: "litiaina-n1.service".into(),
                user: false,
            },
            engine_config: "/etc/litiaina/n1/n1_config.json".into(),
            repository: "/backup/n1-primary".into(),
            key_file: "/etc/litiaina/n1/backup.key".into(),
            state_dir: "/var/lib/litiaina/n1-backup".into(),
            workers: 4,
            capture_timeout_seconds: 3600,
            operation_timeout_seconds: 3600,
            service_timeout_seconds: 330,
            replication_attempts: 3,
            keep_last: 0,
            max_age_seconds: 172800,
            replica: None,
            online_source: None,
        }
    }
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::symlink_metadata(path)?.permissions().mode() & 0o022 != 0 {
            return Err(invalid(
                "backup job config must not be group/world writable",
            ));
        }
        let value: Self = serde_json::from_slice(&super::super::fs::read_regular(path, 65536)?)?;
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<()> {
        if let Some(source) = &self.online_source {
            source.validate()?;
        }
        self.service.validate()?;
        for path in [
            &self.engine_config,
            &self.repository,
            &self.key_file,
            &self.state_dir,
        ] {
            absolute(path)?;
        }
        if !(1..=32).contains(&self.workers)
            || !(1..=10).contains(&self.replication_attempts)
            || !(1..=604800).contains(&self.capture_timeout_seconds)
            || !(1..=604800).contains(&self.operation_timeout_seconds)
            || !(1..=3600).contains(&self.service_timeout_seconds)
            || self.max_age_seconds == 0
            || self.keep_last > 10000
        {
            return Err(invalid("backup job limits are outside supported bounds"));
        }
        if let Some(replica) = &self.replica {
            if replica.url.is_none() {
                absolute(&replica.repository)?;
                absolute(&replica.key_file)?;
            } else if replica.token_file.is_none() {
                return Err(invalid("remote replica requires an operator token file"));
            }
            if replica.keep_last > 10000 {
                return Err(invalid("invalid replica settings"));
            }
            if let Some(path) = &replica.token_file {
                absolute(path)?;
            }
            if let Some(path) = &replica.ca_file {
                absolute(path)?;
            }
            if let Some(url) = &replica.url {
                super::super::network::Destination {
                    url: url.clone(),
                    repository: replica.repository.to_string_lossy().into_owned(),
                    token_file: replica
                        .token_file
                        .clone()
                        .ok_or_else(|| invalid("missing replica token"))?,
                    ca_file: replica.ca_file.clone(),
                    allow_plaintext: replica.allow_plaintext,
                }
                .validate()?;
            }
        }
        Ok(())
    }
}
fn absolute(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|p| matches!(p, std::path::Component::ParentDir))
    {
        return Err(invalid(
            "backup job paths must be absolute without parent traversal",
        ));
    }
    Ok(())
}
