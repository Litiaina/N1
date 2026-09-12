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
    pub ssh: Option<String>,
    #[serde(default)]
    pub ssh_config: Option<PathBuf>,
    #[serde(default = "remote_binary")]
    pub remote_binary: String,
    /// Zero disables automatic destination retention, independently of source.
    #[serde(default)]
    pub keep_last: usize,
}
fn remote_binary() -> String {
    "litiaina-n1-backup".into()
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
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            service: Service {
                unit: "litiaina-n1.service".into(),
                user: false,
            },
            engine_config: "/etc/litiaina/n1/n1_config.ini".into(),
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
            absolute(&replica.repository)?;
            absolute(&replica.key_file)?;
            if replica.keep_last > 10000 || replica.remote_binary.is_empty() {
                return Err(invalid("invalid replica settings"));
            }
            if replica.ssh_config.is_some() && replica.ssh.is_none() {
                return Err(invalid("replica ssh_config requires an SSH destination"));
            }
            if let Some(path) = &replica.ssh_config {
                absolute(path)?;
            }
            if let Some(host) = &replica.ssh
                && (host.is_empty()
                    || host.starts_with('-')
                    || host.contains(char::is_whitespace)
                    || host.contains('\0'))
            {
                return Err(invalid("invalid replica SSH destination"));
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
