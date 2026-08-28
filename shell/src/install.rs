use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt, chown};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use ini::Ini;
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::args::ServeArgs;

const DEFAULT_CONFIG: &str = include_str!("../web/n1_config.default.ini");
const SERVICE_TEMPLATE: &str = include_str!("../../deploy/systemd/litiaina-n1.service.in");
const MANAGED_FSTAB_PREFIX: &str = "# Litiaina N1 managed disk: ";

pub type Emit<'a> = &'a mut dyn FnMut(OutputKind, String);

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputKind {
    Normal,
    Info,
    Success,
    Warning,
    Error,
}

#[derive(Debug, Clone)]
pub struct InstallSettings {
    pub install_dir: PathBuf,
    pub engine_binary: PathBuf,
    pub mount_root: PathBuf,
    pub fstab_path: PathBuf,
    pub service_name: String,
    pub systemd_unit_dir: PathBuf,
    pub run_user: String,
    pub run_group: String,
}

impl InstallSettings {
    pub fn from_serve_args(args: &ServeArgs) -> Result<Self, String> {
        let install_dir = absolute_path(&args.install_dir, "--install-dir")?;
        let engine_binary = if args.engine_binary.trim().is_empty() {
            install_dir.join("litiaina-n1")
        } else {
            absolute_path(&args.engine_binary, "--engine-binary")?
        };
        let service_name = args.service_name.trim();
        if service_name.is_empty()
            || !service_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err("--service-name must contain only letters, numbers, '-' or '_'".into());
        }
        if args.run_user.trim().is_empty() || args.run_group.trim().is_empty() {
            return Err("--run-user and --run-group cannot be empty".into());
        }
        Ok(Self {
            install_dir,
            engine_binary,
            mount_root: absolute_path(&args.mount_root, "--mount-root")?,
            fstab_path: absolute_path(&args.fstab, "--fstab")?,
            service_name: service_name.to_string(),
            systemd_unit_dir: absolute_path(&args.systemd_unit_dir, "--systemd-unit-dir")?,
            run_user: args.run_user.trim().to_string(),
            run_group: args.run_group.trim().to_string(),
        })
    }

    pub fn config_path(&self) -> PathBuf {
        self.install_dir.join("n1_config.ini")
    }

    pub fn env_path(&self) -> PathBuf {
        self.install_dir.join("n1.env")
    }

    pub fn unit_path(&self) -> PathBuf {
        self.systemd_unit_dir
            .join(format!("{}.service", self.service_name))
    }

    pub fn mount_path(&self, label: &str) -> PathBuf {
        self.mount_root.join(label)
    }
}

fn absolute_path(value: &str, name: &str) -> Result<PathBuf, String> {
    let value = value.trim();
    if value.chars().any(|character| character.is_control()) {
        return Err(format!("{name} cannot contain control characters"));
    }
    if value.chars().any(char::is_whitespace) {
        return Err(format!("{name} cannot contain whitespace"));
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(format!("{name} must be an absolute path"));
    }
    Ok(path)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockDisk {
    pub name: String,
    pub path: String,
    pub stable_id: String,
    pub model: String,
    pub serial: String,
    pub size_bytes: u64,
    pub read_only: bool,
    pub removable: bool,
    pub system: bool,
    pub mountpoints: Vec<String>,
    pub filesystem: Option<String>,
    pub label: Option<String>,
    pub uuid: Option<String>,
    pub partition_path: Option<String>,
}

impl BlockDisk {
    pub fn state(&self) -> &'static str {
        if self.system {
            "protected"
        } else if self.read_only {
            "read-only"
        } else if !self.mountpoints.is_empty() {
            "mounted"
        } else if self.filesystem.as_deref() == Some("xfs")
            && self.label.as_deref().is_some_and(valid_ndisk_label)
        {
            "unassigned"
        } else if self.filesystem.is_some() || self.partition_path.is_some() {
            "foreign"
        } else {
            "candidate"
        }
    }

    pub fn display_id(&self) -> &str {
        if self.stable_id.is_empty() {
            &self.path
        } else {
            &self.stable_id
        }
    }
}

#[derive(Debug, Clone)]
pub struct FormatResult {
    pub label: String,
    pub partition_path: String,
    pub uuid: String,
    pub mount_path: PathBuf,
}

mod host;

pub use host::{HostOps, LinuxHost, ServiceAction};
use host::{atomic_write, ensure_fstab_target_available, path_text};

#[cfg(test)]
use host::parse_lsblk_json;

fn valid_ndisk_label(label: &str) -> bool {
    label.len() == 8
        && label.starts_with("NDISK")
        && label[5..].bytes().all(|byte| byte.is_ascii_digit())
}

fn find_disk<'a>(disks: &'a [BlockDisk], target: &str) -> Option<&'a BlockDisk> {
    disks.iter().find(|disk| {
        disk.path == target
            || disk.stable_id == target
            || disk.name == target
            || disk.label.as_deref() == Some(target)
    })
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiskRole {
    Index,
    Blob,
    Both,
}

impl DiskRole {
    fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().replace(',', "+").as_str() {
            "index" => Ok(Self::Index),
            "blob" => Ok(Self::Blob),
            "both" | "index+blob" | "blob+index" => Ok(Self::Both),
            _ => Err("role must be index, blob, or both".into()),
        }
    }

    fn index(self) -> bool {
        matches!(self, Self::Index | Self::Both)
    }

    fn blob(self) -> bool {
        matches!(self, Self::Blob | Self::Both)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Confirmation {
    pub prompt: String,
    pub expected: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CommandOutcome {
    pub success: bool,
    pub clear: bool,
    pub confirmation: Option<Confirmation>,
    pub prompt_state: String,
}

pub struct InstallShell {
    settings: InstallSettings,
    host: Arc<dyn HostOps>,
}

impl InstallShell {
    pub fn new(settings: InstallSettings, host: Arc<dyn HostOps>) -> Self {
        Self { settings, host }
    }

    pub fn execute(
        &self,
        line: &str,
        confirmation: Option<&str>,
        emit: Emit<'_>,
    ) -> CommandOutcome {
        let result = self.execute_inner(line, confirmation, emit);
        match result {
            Ok(mut outcome) => {
                outcome.prompt_state = self.prompt_state();
                outcome
            }
            Err(error) => {
                emit(OutputKind::Error, error);
                CommandOutcome {
                    success: false,
                    prompt_state: self.prompt_state(),
                    ..CommandOutcome::default()
                }
            }
        }
    }

    fn execute_inner(
        &self,
        line: &str,
        confirmation: Option<&str>,
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let arguments = split_command(line)?;
        if arguments.is_empty() {
            return Ok(CommandOutcome {
                success: true,
                ..CommandOutcome::default()
            });
        }
        let completed = || CommandOutcome {
            success: true,
            ..CommandOutcome::default()
        };
        match arguments[0].as_str() {
            "help" => {
                emit(OutputKind::Normal, HELP_TEXT.into());
                Ok(completed())
            }
            "clear" => Ok(CommandOutcome {
                success: true,
                clear: true,
                ..CommandOutcome::default()
            }),
            "version" => {
                emit(
                    OutputKind::Normal,
                    format!("N1 Shell {}", env!("CARGO_PKG_VERSION")),
                );
                Ok(completed())
            }
            "prerequisites" => {
                for program in self.host.check_prerequisites()? {
                    emit(OutputKind::Success, program);
                }
                emit(
                    OutputKind::Success,
                    "Installation prerequisites: PASS".into(),
                );
                Ok(completed())
            }
            "lsblk" => {
                self.list_disks(emit)?;
                Ok(completed())
            }
            "disk" => self.disk_command(&arguments[1..], confirmation, emit),
            "config" => self.config_command(&arguments[1..], emit),
            "secrets" => self.secrets_command(&arguments[1..], emit),
            "n1" => self.n1_command(&arguments[1..], emit),
            "logs" => {
                let lines = arguments
                    .get(1)
                    .map(|value| value.parse::<usize>())
                    .transpose()
                    .map_err(|_| "logs count must be an integer".to_string())?
                    .unwrap_or(100);
                emit(
                    OutputKind::Normal,
                    self.host.recent_logs(&self.settings.service_name, lines)?,
                );
                Ok(completed())
            }
            command => Err(format!("unknown command '{command}'; type 'help'")),
        }
    }

    fn list_disks(&self, emit: Emit<'_>) -> Result<(), String> {
        let disks = self.host.list_disks()?;
        emit(
            OutputKind::Normal,
            format!(
                "{:<12} {:>10}  {:<10} {:<12} {:<16} {}",
                "NAME", "SIZE", "FORMAT", "STATE", "LABEL", "MOUNT"
            ),
        );
        for disk in disks {
            emit(
                if disk.system {
                    OutputKind::Warning
                } else {
                    OutputKind::Normal
                },
                format!(
                    "{:<12} {:>10}  {:<10} {:<12} {:<16} {}",
                    disk.name,
                    human_bytes(disk.size_bytes),
                    disk.filesystem.as_deref().unwrap_or("—"),
                    disk.state(),
                    disk.label.as_deref().unwrap_or("—"),
                    if disk.mountpoints.is_empty() {
                        "—".into()
                    } else {
                        disk.mountpoints.join(",")
                    }
                ),
            );
        }
        Ok(())
    }

    fn disk_command(
        &self,
        arguments: &[String],
        confirmation: Option<&str>,
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        let action = arguments.first().map(String::as_str).unwrap_or("");
        match action {
            "inspect" => {
                let target = arguments
                    .get(1)
                    .ok_or("usage: disk inspect <device-or-label>")?;
                let disks = self.host.list_disks()?;
                let disk = find_disk(&disks, target)
                    .ok_or_else(|| format!("disk '{target}' was not found"))?;
                emit(OutputKind::Normal, format_disk_details(disk));
                Ok(success_outcome())
            }
            "format" => {
                let target = arguments
                    .get(1)
                    .ok_or("usage: disk format <device> <NDISKxxx>")?;
                let label = arguments
                    .get(2)
                    .ok_or("usage: disk format <device> <NDISKxxx>")?;
                if !valid_ndisk_label(label) {
                    return Err("disk label must be NDISK followed by exactly three digits".into());
                }
                let disks = self.host.list_disks()?;
                let disk = find_disk(&disks, target)
                    .ok_or_else(|| format!("disk '{target}' was not found"))?;
                if disks.iter().any(|existing| {
                    existing.path != disk.path && existing.label.as_deref() == Some(label)
                }) {
                    return Err(format!(
                        "NDISK label {label} is already used by another disk"
                    ));
                }
                ensure_fstab_target_available(
                    &self.settings.fstab_path,
                    label,
                    &self.settings.mount_path(label),
                )?;
                if disk.system {
                    return Err(format!("{} is a protected system disk", disk.path));
                }
                if disk.read_only {
                    return Err(format!("{} is read-only", disk.path));
                }
                if !disk.mountpoints.is_empty() {
                    return Err(format!(
                        "{} is mounted at {}; unmount it before formatting",
                        disk.path,
                        disk.mountpoints.join(", ")
                    ));
                }
                let serial = if disk.serial.is_empty() {
                    disk.name.as_str()
                } else {
                    disk.serial.as_str()
                };
                let expected = format!("FORMAT {label} {serial}");
                if confirmation != Some(expected.as_str()) {
                    return Ok(CommandOutcome {
                        success: false,
                        confirmation: Some(Confirmation {
                            prompt: format!(
                                "WARNING: formatting removes partition/filesystem signatures from {} ({}, {}, serial {}).\nType '{}' to continue:",
                                disk.path,
                                disk.model,
                                human_bytes(disk.size_bytes),
                                serial,
                                expected
                            ),
                            expected,
                        }),
                        ..CommandOutcome::default()
                    });
                }
                let result = self.host.format_disk(disk, label, &self.settings, emit)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "{} is mounted at {} and remains unassigned\nPartition: {}\nUUID: {}",
                        result.label,
                        result.mount_path.display(),
                        result.partition_path,
                        result.uuid
                    ),
                );
                Ok(success_outcome())
            }
            "mount" => {
                let label = arguments.get(1).ok_or("usage: disk mount <NDISKxxx>")?;
                if !valid_ndisk_label(label) {
                    return Err("disk label must be NDISK followed by exactly three digits".into());
                }
                let disks = self.host.list_disks()?;
                let disk = find_disk(&disks, label)
                    .ok_or_else(|| format!("formatted disk '{label}' was not found"))?;
                let result = self.host.mount_disk(disk, label, &self.settings, emit)?;
                emit(
                    OutputKind::Success,
                    format!(
                        "{} is ready at {}\nPartition: {}\nUUID: {}",
                        result.label,
                        result.mount_path.display(),
                        result.partition_path,
                        result.uuid
                    ),
                );
                Ok(success_outcome())
            }
            "assign" => {
                let label = arguments
                    .get(1)
                    .ok_or("usage: disk assign <NDISKxxx> <index|blob|both>")?;
                let role = DiskRole::parse(
                    arguments
                        .get(2)
                        .ok_or("usage: disk assign <NDISKxxx> <index|blob|both>")?,
                )?;
                self.assign_disk(label, role)?;
                emit(
                    OutputKind::Success,
                    format!("{label} assigned as {}", arguments[2]),
                );
                Ok(success_outcome())
            }
            "unassign" => {
                let label = arguments
                    .get(1)
                    .ok_or("usage: disk unassign <NDISKxxx> <index|blob|both>")?;
                let role = DiskRole::parse(
                    arguments
                        .get(2)
                        .ok_or("usage: disk unassign <NDISKxxx> <index|blob|both>")?,
                )?;
                self.unassign_disk(label, role)?;
                emit(OutputKind::Success, format!("{label} assignment removed"));
                Ok(success_outcome())
            }
            _ => Err("usage: disk <inspect|format|mount|assign|unassign> ...".into()),
        }
    }

    fn assign_disk(&self, label: &str, role: DiskRole) -> Result<(), String> {
        self.ensure_service_stopped()?;
        if !valid_ndisk_label(label) {
            return Err("disk label must be NDISK followed by exactly three digits".into());
        }
        let disks = self.host.list_disks()?;
        let disk = find_disk(&disks, label)
            .ok_or_else(|| format!("formatted disk '{label}' was not found"))?;
        if disk.system || disk.filesystem.as_deref() != Some("xfs") {
            return Err(format!("{label} is not an eligible XFS N1 disk"));
        }
        let mount_path = self.settings.mount_path(label);
        if !disk
            .mountpoints
            .iter()
            .any(|mounted| Path::new(mounted) == mount_path)
        {
            return Err(format!(
                "{label} is not mounted at {}",
                mount_path.display()
            ));
        }
        let mut config = self.load_or_create_config()?;
        if role.index() {
            add_config_path(&mut config, "index_store_disks", &mount_path)?;
        }
        if role.blob() {
            add_config_path(&mut config, "blob_store_disks", &mount_path)?;
        }
        self.write_storage_config(&config)
    }

    fn unassign_disk(&self, label: &str, role: DiskRole) -> Result<(), String> {
        self.ensure_service_stopped()?;
        let mut config = self.load_or_create_config()?;
        let mount_path = self.settings.mount_path(label);
        if role.index() {
            remove_config_path(&mut config, "index_store_disks", &mount_path)?;
        }
        if role.blob() {
            remove_config_path(&mut config, "blob_store_disks", &mount_path)?;
        }
        self.write_storage_config(&config)
    }

    fn config_command(
        &self,
        arguments: &[String],
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        match arguments.first().map(String::as_str).unwrap_or("") {
            "ec" => {
                self.ensure_service_stopped()?;
                let data = parse_positive(arguments.get(1), "usage: config ec <data> <parity>")?;
                let parity =
                    parse_nonnegative(arguments.get(2), "usage: config ec <data> <parity>")?;
                let mut config = self.load_or_create_config()?;
                config
                    .with_section(Some("upload_config"))
                    .set("data_shards", data.to_string())
                    .set("parity_shards", parity.to_string());
                self.write_config(&config)?;
                emit(
                    OutputKind::Success,
                    format!("EC configured as {data} + {parity}"),
                );
                Ok(success_outcome())
            }
            "endpoint" => {
                self.ensure_service_stopped()?;
                if arguments.len() != 4 {
                    return Err("usage: config endpoint <host> <https-port> <native-port>".into());
                }
                let https = arguments[2]
                    .parse::<u16>()
                    .map_err(|_| "HTTPS port must be 1-65535".to_string())?;
                let native = arguments[3]
                    .parse::<u16>()
                    .map_err(|_| "native port must be 1-65535".to_string())?;
                if https == native {
                    return Err("HTTPS and native ports must be different".into());
                }
                if arguments[1].parse::<std::net::IpAddr>().is_err() {
                    return Err("host must be an IPv4 or IPv6 address".into());
                }
                let mut config = self.load_or_create_config()?;
                config
                    .with_section(Some("server"))
                    .set("host", &arguments[1])
                    .set("https_port", https.to_string())
                    .set("native_port", native.to_string());
                self.write_config(&config)?;
                emit(OutputKind::Success, "server endpoint updated".into());
                Ok(success_outcome())
            }
            "tls" => {
                self.ensure_service_stopped()?;
                if arguments.len() != 3 {
                    return Err("usage: config tls <certificate-path> <private-key-path>".into());
                }
                let certificate = absolute_path(&arguments[1], "certificate path")?;
                let key = absolute_path(&arguments[2], "private key path")?;
                let mut config = self.load_or_create_config()?;
                config
                    .with_section(Some("server"))
                    .set("cert_path", certificate.to_string_lossy().as_ref())
                    .set("key_path", key.to_string_lossy().as_ref());
                self.write_config(&config)?;
                emit(OutputKind::Success, "TLS paths updated".into());
                Ok(success_outcome())
            }
            "show" => {
                emit(OutputKind::Normal, self.config_summary()?);
                Ok(success_outcome())
            }
            "validate" => {
                let report = self.validate_installation()?;
                emit(OutputKind::Success, report);
                Ok(success_outcome())
            }
            _ => Err("usage: config <ec|endpoint|tls|show|validate> ...".into()),
        }
    }

    fn secrets_command(
        &self,
        arguments: &[String],
        emit: Emit<'_>,
    ) -> Result<CommandOutcome, String> {
        match arguments.first().map(String::as_str).unwrap_or("") {
            "show" => {
                let content = fs::read_to_string(self.settings.env_path()).map_err(|error| {
                    format!(
                        "read environment file {}: {error}; run 'n1 init' first",
                        self.settings.env_path().display()
                    )
                })?;
                let mut selected = Vec::new();
                for key in ["AUTH_KEY", "MASTER_KEY", "ADMIN_KEY"] {
                    let line = content
                        .lines()
                        .find(|line| line.split_once('=').is_some_and(|(name, _)| name == key))
                        .ok_or_else(|| format!("{key} is missing from n1.env"))?;
                    selected.push(line.to_string());
                }
                emit(
                    OutputKind::Warning,
                    "These credentials grant N1 access. Copy them securely and stop the setup server after provisioning.".into(),
                );
                emit(OutputKind::Normal, selected.join("\n"));
                Ok(success_outcome())
            }
            _ => Err("usage: secrets show".into()),
        }
    }

    fn n1_command(&self, arguments: &[String], emit: Emit<'_>) -> Result<CommandOutcome, String> {
        match arguments.first().map(String::as_str).unwrap_or("") {
            "init" => {
                self.initialize(emit)?;
                Ok(success_outcome())
            }
            "start" => {
                emit(OutputKind::Info, self.validate_installation()?);
                self.host
                    .service_action(&self.settings.service_name, ServiceAction::Start)?;
                emit(OutputKind::Success, "N1 service started".into());
                Ok(success_outcome())
            }
            "stop" => {
                self.host
                    .service_action(&self.settings.service_name, ServiceAction::Stop)?;
                emit(OutputKind::Success, "N1 service stopped".into());
                Ok(success_outcome())
            }
            "restart" => {
                emit(OutputKind::Info, self.validate_installation()?);
                self.host
                    .service_action(&self.settings.service_name, ServiceAction::Restart)?;
                emit(OutputKind::Success, "N1 service restarted".into());
                Ok(success_outcome())
            }
            "enable" => {
                self.host
                    .service_action(&self.settings.service_name, ServiceAction::Enable)?;
                emit(OutputKind::Success, "N1 service enabled for boot".into());
                Ok(success_outcome())
            }
            "disable" => {
                self.host
                    .service_action(&self.settings.service_name, ServiceAction::Disable)?;
                emit(OutputKind::Success, "N1 service disabled for boot".into());
                Ok(success_outcome())
            }
            "status" => {
                let active = self.host.service_is_active(&self.settings.service_name);
                emit(
                    if active {
                        OutputKind::Success
                    } else {
                        OutputKind::Warning
                    },
                    format!(
                        "N1 service: {}\nConfiguration: {}\nInstall directory: {}",
                        if active { "active" } else { "inactive" },
                        if self.validate_installation().is_ok() {
                            "valid"
                        } else {
                            "incomplete"
                        },
                        self.settings.install_dir.display()
                    ),
                );
                Ok(success_outcome())
            }
            _ => Err("usage: n1 <init|start|stop|restart|enable|disable|status>".into()),
        }
    }

    fn initialize(&self, emit: Emit<'_>) -> Result<(), String> {
        self.host.require_privileged()?;
        self.host.check_prerequisites()?;
        self.ensure_service_stopped()?;
        self.host
            .identity_exists(&self.settings.run_user, &self.settings.run_group)?;
        fs::create_dir_all(&self.settings.install_dir).map_err(|error| {
            format!(
                "create install directory {}: {error}",
                self.settings.install_dir.display()
            )
        })?;
        emit(OutputKind::Info, "[1/4] Preparing N1 configuration".into());
        if !self.settings.config_path().exists() {
            atomic_write(
                &self.settings.config_path(),
                DEFAULT_CONFIG.as_bytes(),
                0o640,
            )?;
        }
        let mut config = self.load_or_create_config()?;
        let fs_root = self.settings.install_dir.join("fs-storage-path");
        fs::create_dir_all(&fs_root)
            .map_err(|error| format!("create {}: {error}", fs_root.display()))?;
        config
            .with_section(Some("disk_config"))
            .set("fs_storage_path", fs_root.to_string_lossy().as_ref());
        if config
            .section(Some("server"))
            .and_then(|section| section.get("cert_path"))
            .unwrap_or("")
            .is_empty()
        {
            config
                .with_section(Some("server"))
                .set(
                    "cert_path",
                    self.settings
                        .install_dir
                        .join("cert.pem")
                        .to_string_lossy()
                        .as_ref(),
                )
                .set(
                    "key_path",
                    self.settings
                        .install_dir
                        .join("key.pem")
                        .to_string_lossy()
                        .as_ref(),
                );
        }
        self.write_config(&config)?;
        emit(OutputKind::Success, "[1/4] Configuration prepared".into());

        emit(
            OutputKind::Info,
            "[2/4] Preparing machine-local secrets".into(),
        );
        if !self.settings.env_path().exists() {
            let secrets = generated_env();
            atomic_write(&self.settings.env_path(), secrets.as_bytes(), 0o640)?;
            emit(
                OutputKind::Warning,
                format!(
                    "[2/4] New secrets generated in {}. Read this root-owned file to retrieve the initial administrative keys.",
                    self.settings.env_path().display()
                ),
            );
        } else {
            emit(
                OutputKind::Success,
                "[2/4] Existing secrets preserved".into(),
            );
        }

        emit(
            OutputKind::Info,
            "[3/4] Installing disabled systemd unit".into(),
        );
        self.install_service_unit()?;
        self.host.reload_systemd()?;
        let _ = self
            .host
            .service_action(&self.settings.service_name, ServiceAction::Disable);
        emit(
            OutputKind::Success,
            "[3/4] Service installed and disabled".into(),
        );

        emit(OutputKind::Info, "[4/4] Applying runtime ownership".into());
        self.host.apply_ownership(
            &self.settings.install_dir,
            &self.settings.run_user,
            &self.settings.run_group,
        )?;
        fs::set_permissions(self.settings.env_path(), fs::Permissions::from_mode(0o640))
            .map_err(|error| format!("chmod n1.env: {error}"))?;
        emit(
            OutputKind::Success,
            "[4/4] N1 initialized but not started".into(),
        );
        Ok(())
    }

    fn install_service_unit(&self) -> Result<(), String> {
        if !self.settings.engine_binary.is_file() {
            return Err(format!(
                "N1 engine binary not found at {}",
                self.settings.engine_binary.display()
            ));
        }
        let mode = self
            .settings
            .engine_binary
            .metadata()
            .map_err(|error| format!("inspect engine binary: {error}"))?
            .permissions()
            .mode();
        if mode & 0o111 == 0 {
            return Err(format!(
                "N1 engine binary is not executable: {}",
                self.settings.engine_binary.display()
            ));
        }
        let config = self.load_or_create_config()?;
        let disk = config
            .section(Some("disk_config"))
            .ok_or("missing [disk_config]")?;
        let mut required_mounts = BTreeSet::new();
        required_mounts.extend(config_paths(disk.get("index_store_disks").unwrap_or("")));
        required_mounts.extend(config_paths(disk.get("blob_store_disks").unwrap_or("")));
        let fs_root = PathBuf::from(disk.get("fs_storage_path").unwrap_or(""));
        if fs_root.is_absolute() {
            required_mounts.insert(fs_root);
        }
        let required_mounts = required_mounts
            .iter()
            .map(|path| systemd_escape(path))
            .collect::<Vec<_>>()
            .join(" ");
        if required_mounts.is_empty() {
            return Err("N1 service has no configured storage mounts".into());
        }

        let rendered = SERVICE_TEMPLATE
            .replace("__SERVICE_NAME__", &self.settings.service_name)
            .replace("__RUN_USER__", &self.settings.run_user)
            .replace("__RUN_GROUP__", &self.settings.run_group)
            .replace(
                "__WORKING_DIRECTORY__",
                &systemd_escape(&self.settings.install_dir),
            )
            .replace("__BINARY__", &systemd_escape(&self.settings.engine_binary))
            .replace("__ENV_FILE__", &systemd_escape(&self.settings.env_path()))
            .replace(
                "__CONFIG_FILE__",
                &systemd_escape(&self.settings.config_path()),
            )
            .replace("__REQUIRES_MOUNTS_FOR__", &required_mounts)
            .replace("__RUST_LOG__", "info");
        atomic_write(&self.settings.unit_path(), rendered.as_bytes(), 0o644)
    }

    fn load_or_create_config(&self) -> Result<Ini, String> {
        let path = self.settings.config_path();
        if !path.exists() {
            atomic_write(&path, DEFAULT_CONFIG.as_bytes(), 0o640)?;
        }
        Ini::load_from_file(&path).map_err(|error| format!("load {}: {error}", path.display()))
    }

    fn write_config(&self, config: &Ini) -> Result<(), String> {
        let mut bytes = Vec::new();
        config
            .write_to(&mut bytes)
            .map_err(|error| format!("serialize N1 config: {error}"))?;
        atomic_write(&self.settings.config_path(), &bytes, 0o640)
    }

    fn write_storage_config(&self, config: &Ini) -> Result<(), String> {
        self.write_config(config)?;
        if self.settings.unit_path().is_file() {
            self.install_service_unit()?;
            self.host.reload_systemd()?;
        }
        Ok(())
    }

    fn config_summary(&self) -> Result<String, String> {
        let config = self.load_or_create_config()?;
        let disk = config
            .section(Some("disk_config"))
            .ok_or("missing [disk_config]")?;
        let upload = config
            .section(Some("upload_config"))
            .ok_or("missing [upload_config]")?;
        let server = config.section(Some("server")).ok_or("missing [server]")?;
        Ok(format!(
            "N1 INSTALLATION\n  directory: {}\n  engine:    {}\n  service:   {}\n\nSERVER\n  HTTPS:  {}:{}\n  native: {}:{}\n  cert:   {}\n  key:    {}\n\nERASURE CODING\n  data:   {}\n  parity: {}\n\nINDEX ROOTS\n{}\n\nBLOB ROOTS\n{}\n\nCONTROL ROOT\n  {}",
            self.settings.install_dir.display(),
            self.settings.engine_binary.display(),
            self.settings.service_name,
            server.get("host").unwrap_or(""),
            server.get("https_port").unwrap_or(""),
            server.get("host").unwrap_or(""),
            server.get("native_port").unwrap_or(""),
            server.get("cert_path").unwrap_or(""),
            server.get("key_path").unwrap_or(""),
            upload.get("data_shards").unwrap_or(""),
            upload.get("parity_shards").unwrap_or(""),
            format_roots(disk.get("index_store_disks").unwrap_or("")),
            format_roots(disk.get("blob_store_disks").unwrap_or("")),
            disk.get("fs_storage_path").unwrap_or("")
        ))
    }

    fn validate_installation(&self) -> Result<String, String> {
        let config = self.load_or_create_config()?;
        let disk = config
            .section(Some("disk_config"))
            .ok_or("missing [disk_config]")?;
        let upload = config
            .section(Some("upload_config"))
            .ok_or("missing [upload_config]")?;
        let server = config.section(Some("server")).ok_or("missing [server]")?;
        let data = upload
            .get("data_shards")
            .ok_or("missing upload_config.data_shards")?
            .parse::<usize>()
            .map_err(|_| "upload_config.data_shards is invalid".to_string())?;
        let parity = upload
            .get("parity_shards")
            .ok_or("missing upload_config.parity_shards")?
            .parse::<usize>()
            .map_err(|_| "upload_config.parity_shards is invalid".to_string())?;
        if data == 0 {
            return Err("EC data shard count must be at least 1".into());
        }
        let index = config_paths(disk.get("index_store_disks").unwrap_or(""));
        let blob = config_paths(disk.get("blob_store_disks").unwrap_or(""));
        if index.is_empty() {
            return Err("at least one index root must be assigned".into());
        }
        if blob.len() < data + parity {
            return Err(format!(
                "EC {data}+{parity} requires at least {} blob roots; {} configured",
                data + parity,
                blob.len()
            ));
        }
        if unique_paths(&index).len() != index.len() || unique_paths(&blob).len() != blob.len() {
            return Err("duplicate index or blob root in configuration".into());
        }
        let discovered = self.host.list_disks()?;
        let mut index_devices = BTreeSet::new();
        let mut blob_devices = BTreeSet::new();
        for root in index.iter().chain(blob.iter()) {
            let disk = discovered
                .iter()
                .find(|disk| {
                    disk.mountpoints
                        .iter()
                        .any(|mount| Path::new(mount) == root)
                })
                .ok_or_else(|| format!("configured root {} is not mounted", root.display()))?;
            if disk.system || disk.filesystem.as_deref() != Some("xfs") {
                return Err(format!(
                    "configured root {} is not on an eligible XFS disk",
                    root.display()
                ));
            }
            let identity = if disk.stable_id.is_empty() {
                disk.path.clone()
            } else {
                disk.stable_id.clone()
            };
            if index.iter().any(|candidate| candidate == root) {
                index_devices.insert(identity.clone());
            }
            if blob.iter().any(|candidate| candidate == root) {
                blob_devices.insert(identity);
            }
        }
        if index_devices.len() != index.len() {
            return Err("index roots must resolve to distinct physical disks".into());
        }
        if blob_devices.len() != blob.len() {
            return Err("blob roots must resolve to distinct physical disks".into());
        }
        let fs_root = PathBuf::from(disk.get("fs_storage_path").unwrap_or(""));
        if !fs_root.is_dir() {
            return Err(format!("control root {} does not exist", fs_root.display()));
        }
        for (name, value) in [
            ("TLS certificate", server.get("cert_path").unwrap_or("")),
            ("TLS private key", server.get("key_path").unwrap_or("")),
        ] {
            if !Path::new(value).is_file() {
                return Err(format!("{name} does not exist at {value}"));
            }
        }
        validate_env(&self.settings.env_path())?;
        self.host
            .identity_exists(&self.settings.run_user, &self.settings.run_group)?;
        if !self.settings.engine_binary.is_file() {
            return Err(format!(
                "engine binary missing at {}",
                self.settings.engine_binary.display()
            ));
        }
        if self
            .settings
            .engine_binary
            .metadata()
            .map_err(|error| format!("inspect engine binary: {error}"))?
            .permissions()
            .mode()
            & 0o111
            == 0
        {
            return Err(format!(
                "engine binary is not executable: {}",
                self.settings.engine_binary.display()
            ));
        }
        if !self.settings.unit_path().is_file() {
            return Err(format!(
                "systemd unit is missing at {}; run 'n1 init'",
                self.settings.unit_path().display()
            ));
        }
        Ok(format!(
            "Validation: PASS\nEC: {data} + {parity}\nIndex roots: {}\nBlob roots: {}\nUnique mounted disks: {}",
            index.len(),
            blob.len(),
            unique_paths(&blob).len()
        ))
    }

    fn prompt_state(&self) -> String {
        if self.host.service_is_active(&self.settings.service_name) {
            "active".into()
        } else if !self.settings.config_path().exists() {
            "unconfigured".into()
        } else if self.validate_installation().is_ok() {
            "stopped".into()
        } else {
            "incomplete".into()
        }
    }

    pub fn current_prompt_state(&self) -> String {
        self.prompt_state()
    }

    fn ensure_service_stopped(&self) -> Result<(), String> {
        if self.host.service_is_active(&self.settings.service_name) {
            return Err("stop N1 before changing installation or storage configuration".into());
        }
        Ok(())
    }
}

fn success_outcome() -> CommandOutcome {
    CommandOutcome {
        success: true,
        ..CommandOutcome::default()
    }
}

fn parse_positive(value: Option<&String>, usage: &str) -> Result<usize, String> {
    let parsed = value
        .ok_or_else(|| usage.to_string())?
        .parse::<usize>()
        .map_err(|_| usage.to_string())?;
    if parsed == 0 {
        return Err("value must be at least 1".into());
    }
    Ok(parsed)
}

fn parse_nonnegative(value: Option<&String>, usage: &str) -> Result<usize, String> {
    value
        .ok_or_else(|| usage.to_string())?
        .parse::<usize>()
        .map_err(|_| usage.to_string())
}

fn add_config_path(config: &mut Ini, key: &str, path: &Path) -> Result<(), String> {
    let text = path_text(path)?;
    let current = config
        .section(Some("disk_config"))
        .and_then(|section| section.get(key))
        .unwrap_or("");
    let mut paths = config_paths(current);
    if !paths.iter().any(|existing| existing == path) {
        paths.push(path.to_path_buf());
    }
    paths.sort();
    config.with_section(Some("disk_config")).set(
        key,
        paths
            .iter()
            .map(|path| path.to_string_lossy())
            .collect::<Vec<_>>()
            .join(","),
    );
    let _ = text;
    Ok(())
}

fn remove_config_path(config: &mut Ini, key: &str, path: &Path) -> Result<(), String> {
    let current = config
        .section(Some("disk_config"))
        .and_then(|section| section.get(key))
        .unwrap_or("");
    let mut paths = config_paths(current);
    paths.retain(|existing| existing != path);
    config.with_section(Some("disk_config")).set(
        key,
        paths
            .iter()
            .map(|path| path.to_string_lossy())
            .collect::<Vec<_>>()
            .join(","),
    );
    Ok(())
}

fn config_paths(value: &str) -> Vec<PathBuf> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .collect()
}

fn unique_paths(paths: &[PathBuf]) -> BTreeSet<PathBuf> {
    paths.iter().cloned().collect()
}

fn format_roots(value: &str) -> String {
    let roots = config_paths(value);
    if roots.is_empty() {
        return "  —".into();
    }
    roots
        .iter()
        .map(|path| format!("  {}", path.display()))
        .collect::<Vec<_>>()
        .join("\n")
}

fn validate_env(path: &Path) -> Result<(), String> {
    let content = fs::read_to_string(path)
        .map_err(|error| format!("read environment file {}: {error}", path.display()))?;
    for key in ["JWT_SECRET", "AUTH_KEY", "MASTER_KEY", "ADMIN_KEY"] {
        let present = content.lines().any(|line| {
            line.split_once('=')
                .is_some_and(|(name, value)| name.trim() == key && !value.trim().is_empty())
        });
        if !present {
            return Err(format!("{key} is missing or empty in {}", path.display()));
        }
    }
    Ok(())
}

fn generated_env() -> String {
    format!(
        "JWT_SECRET={}\nACCESS_TOKEN_DEFAULT_TTL_SECS=900\nACCESS_TOKEN_MAX_TTL_SECS=3600\nREFRESH_TOKEN_TTL_SECS=604800\nAUTH_KEY={}\nMASTER_KEY={}\nADMIN_KEY={}\n",
        random_secret(),
        random_secret(),
        random_secret(),
        random_secret()
    )
}

fn random_secret() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn systemd_escape(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "\\\\")
        .replace(' ', "\\x20")
        .replace('"', "\\x22")
        .replace('\'', "\\x27")
        .replace(';', "\\x3b")
        .replace('%', "%%")
}

fn format_disk_details(disk: &BlockDisk) -> String {
    format!(
        "Device:       {}\nStable ID:    {}\nModel:        {}\nSerial:       {}\nCapacity:     {}\nFilesystem:   {}\nLabel:        {}\nUUID:         {}\nMounted:      {}\nSystem disk:  {}\nRead-only:    {}\nEligibility:  {}",
        disk.path,
        disk.display_id(),
        if disk.model.is_empty() {
            "—"
        } else {
            &disk.model
        },
        if disk.serial.is_empty() {
            "—"
        } else {
            &disk.serial
        },
        human_bytes(disk.size_bytes),
        disk.filesystem.as_deref().unwrap_or("—"),
        disk.label.as_deref().unwrap_or("—"),
        disk.uuid.as_deref().unwrap_or("—"),
        if disk.mountpoints.is_empty() {
            "no".into()
        } else {
            disk.mountpoints.join(", ")
        },
        if disk.system { "yes" } else { "no" },
        if disk.read_only { "yes" } else { "no" },
        disk.state()
    )
}

fn split_command(input: &str) -> Result<Vec<String>, String> {
    let mut arguments = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in input.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        if character == '\\' && quote != Some('\'') {
            escaped = true;
            continue;
        }
        if let Some(active) = quote {
            if character == active {
                quote = None;
            } else {
                current.push(character);
            }
            continue;
        }
        match character {
            '\'' | '"' => quote = Some(character),
            character if character.is_whitespace() => {
                if !current.is_empty() {
                    arguments.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(character),
        }
    }
    if escaped {
        return Err("command ends with an incomplete escape".into());
    }
    if quote.is_some() {
        return Err("command contains an unterminated quote".into());
    }
    if !current.is_empty() {
        arguments.push(current);
    }
    Ok(arguments)
}

const HELP_TEXT: &str = "COMMANDS
  help                                      show this command list
  clear                                     clear the visible terminal
  version                                   show the N1 Shell version
  prerequisites                             verify Debian installation tools
  lsblk                                     list physical disks and safety state
  disk inspect <device-or-label>            inspect one physical disk
  disk format <device> <NDISKxxx>           create one GPT/XFS N1 disk
  disk mount <NDISKxxx>                     finish mounting an initialized N1 disk
  disk assign <NDISKxxx> <index|blob|both>  assign an initialized disk
  disk unassign <NDISKxxx> <role>           remove an assignment while N1 is stopped
  config ec <data> <parity>                 set the static N1 EC layout
  config endpoint <host> <https> <native>   set engine listeners
  config tls <certificate> <private-key>    set existing TLS files
  config show                               show the complete installation draft
  config validate                           prove startup prerequisites
  secrets show                              display initial N1 access keys once
  n1 init                                   initialize config, secrets, and systemd
  n1 start|stop|restart                     control the N1 service
  n1 enable|disable                         control boot activation
  n1 status                                 show service and configuration state
  logs [count]                              show recent N1 journal lines";

#[cfg(test)]
mod tests;
