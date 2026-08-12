use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt, chown};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::ServeArgs;

const MANAGED_FSTAB_PREFIX: &str = "# Litiaina N2 managed disk: ";

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
    pub engine_binary: PathBuf,
    pub gateway_binary: PathBuf,
    pub runtime_root: PathBuf,
    pub gateway_runtime: PathBuf,
    pub env_path: PathBuf,
    pub setup_root: PathBuf,
    pub mount_root: PathBuf,
    pub fstab_path: PathBuf,
    pub service_name: String,
    pub gateway_service_name: String,
    pub systemd_unit_dir: PathBuf,
    pub run_user: String,
    pub run_group: String,
}

impl InstallSettings {
    pub fn from_serve_args(args: &ServeArgs) -> Result<Self, String> {
        let service_name = validated_service_name(&args.service_name, "--service-name")?;
        let gateway_service_name =
            validated_service_name(&args.gateway_service_name, "--gateway-service-name")?;
        if args.run_user.trim().is_empty() || args.run_group.trim().is_empty() {
            return Err("--run-user and --run-group cannot be empty".into());
        }
        Ok(Self {
            engine_binary: absolute_path(&args.engine_binary, "--engine-binary")?,
            gateway_binary: absolute_path(&args.gateway_binary, "--gateway-binary")?,
            runtime_root: absolute_path(&args.runtime_root, "--runtime-root")?,
            gateway_runtime: absolute_path(&args.gateway_runtime, "--gateway-runtime")?,
            env_path: absolute_path(&args.env_file, "--env-file")?,
            setup_root: absolute_path(&args.setup_root, "--setup-root")?,
            mount_root: absolute_path(&args.mount_root, "--mount-root")?,
            fstab_path: absolute_path(&args.fstab, "--fstab")?,
            service_name,
            gateway_service_name,
            systemd_unit_dir: absolute_path(&args.systemd_unit_dir, "--systemd-unit-dir")?,
            run_user: args.run_user.trim().to_string(),
            run_group: args.run_group.trim().to_string(),
        })
    }

    pub fn node_dir(&self, node_id: &str) -> PathBuf {
        self.runtime_root.join("nodes").join(node_id)
    }

    pub fn node_unit_path(&self) -> PathBuf {
        self.systemd_unit_dir
            .join(format!("{}@.service", self.service_name))
    }

    pub fn gateway_unit_path(&self) -> PathBuf {
        self.systemd_unit_dir
            .join(format!("{}.service", self.gateway_service_name))
    }

    pub fn mount_path(&self, label: &str) -> PathBuf {
        self.mount_root.join(label)
    }
}

fn validated_service_name(value: &str, name: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!(
            "{name} must contain only letters, numbers, '-' or '_'"
        ));
    }
    Ok(value.to_string())
}

fn absolute_path(value: &str, name: &str) -> Result<PathBuf, String> {
    let value = value.trim();
    if value.chars().any(|character| character.is_control())
        || value.chars().any(char::is_whitespace)
    {
        return Err(format!(
            "{name} cannot contain whitespace or control characters"
        ));
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

    fn display_id(&self) -> &str {
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

pub trait HostOps: Send + Sync {
    fn require_privileged(&self) -> Result<(), String>;
    fn check_prerequisites(&self) -> Result<Vec<String>, String>;
    fn list_disks(&self) -> Result<Vec<BlockDisk>, String>;
    fn format_disk(
        &self,
        disk: &BlockDisk,
        label: &str,
        settings: &InstallSettings,
        emit: Emit<'_>,
    ) -> Result<FormatResult, String>;
    fn mount_disk(
        &self,
        disk: &BlockDisk,
        label: &str,
        settings: &InstallSettings,
        emit: Emit<'_>,
    ) -> Result<FormatResult, String>;
    fn service_action(&self, service: &str, action: ServiceAction) -> Result<String, String>;
    fn service_is_active(&self, service: &str) -> bool;
    fn reload_systemd(&self) -> Result<(), String>;
    fn identity_exists(&self, user: &str, group: &str) -> Result<(), String>;
    fn apply_ownership(&self, path: &Path, user: &str, group: &str) -> Result<(), String>;
    fn recent_logs(&self, service: &str, lines: usize) -> Result<String, String>;
}

#[derive(Debug, Clone, Copy)]
pub enum ServiceAction {
    Start,
    Stop,
    Restart,
    Enable,
    Disable,
}

impl ServiceAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Restart => "restart",
            Self::Enable => "enable",
            Self::Disable => "disable",
        }
    }
}

#[derive(Default)]
pub struct LinuxHost;

impl HostOps for LinuxHost {
    fn require_privileged(&self) -> Result<(), String> {
        require_root()
    }

    fn check_prerequisites(&self) -> Result<Vec<String>, String> {
        const PROGRAMS: [&str; 12] = [
            "lsblk",
            "wipefs",
            "sfdisk",
            "mkfs.xfs",
            "blkid",
            "udevadm",
            "mount",
            "chown",
            "systemctl",
            "journalctl",
            "id",
            "getent",
        ];
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for program in PROGRAMS {
            if let Some(path) = find_program(program) {
                found.push(format!("{program}: {}", path.display()));
            } else {
                missing.push(program);
            }
        }
        if missing.is_empty() {
            Ok(found)
        } else {
            Err(format!(
                "missing required programs: {}; on Debian install xfsprogs, util-linux, and fdisk",
                missing.join(", ")
            ))
        }
    }

    fn list_disks(&self) -> Result<Vec<BlockDisk>, String> {
        let output = run_command(
            "lsblk",
            &[
                "--json",
                "--bytes",
                "--paths",
                "--output",
                "NAME,KNAME,PATH,TYPE,SIZE,FSTYPE,LABEL,UUID,MODEL,SERIAL,MOUNTPOINTS,PKNAME,RO,RM",
            ],
            None,
        )?;
        parse_lsblk_json(&output, stable_disk_ids())
    }

    fn format_disk(
        &self,
        disk: &BlockDisk,
        label: &str,
        settings: &InstallSettings,
        emit: Emit<'_>,
    ) -> Result<FormatResult, String> {
        require_root()?;
        ensure_host_mount_namespace()?;
        validate_format_candidate(disk)?;
        ensure_fstab_target_available(&settings.fstab_path, label, &settings.mount_path(label))?;

        emit(
            OutputKind::Info,
            "[1/5] Removing existing signatures".into(),
        );
        run_command("wipefs", &["--all", "--force", &disk.path], None)?;
        emit(
            OutputKind::Success,
            "[1/5] Existing signatures removed".into(),
        );
        emit(
            OutputKind::Info,
            "[2/5] Creating one GPT data partition".into(),
        );
        run_command(
            "sfdisk",
            &["--wipe", "always", &disk.path],
            Some("label: gpt\n,,L\n"),
        )?;
        let _ = run_command("udevadm", &["settle"], None);
        let partition = wait_for_partition(&disk.path)?;
        emit(
            OutputKind::Success,
            format!("[2/5] Data partition created at {partition}"),
        );
        emit(OutputKind::Info, "[3/5] Creating XFS filesystem".into());
        run_command("mkfs.xfs", &["-f", "-L", label, &partition], None)?;
        let _ = run_command("udevadm", &["settle"], None);
        let uuid = run_command("blkid", &["-s", "UUID", "-o", "value", &partition], None)?
            .trim()
            .to_string();
        if uuid.is_empty() {
            return Err(format!("blkid returned no UUID for {partition}"));
        }
        emit(OutputKind::Success, format!("[3/5] XFS UUID {uuid}"));
        let mount_path = settings.mount_path(label);
        emit(
            OutputKind::Info,
            format!("[4/5] Registering {}", mount_path.display()),
        );
        fs::create_dir_all(&mount_path)
            .map_err(|error| format!("create {}: {error}", mount_path.display()))?;
        update_fstab(&settings.fstab_path, label, &uuid, &mount_path)?;
        self.reload_systemd()?;
        run_command("mount", &[path_text(&mount_path)?], None)?;
        emit(
            OutputKind::Success,
            "[4/5] Persistent mount registered".into(),
        );
        emit(
            OutputKind::Info,
            "[5/5] Applying N2 service ownership".into(),
        );
        run_command(
            "chown",
            &[
                "-R",
                &format!("{}:{}", settings.run_user, settings.run_group),
                path_text(&mount_path)?,
            ],
            None,
        )?;
        emit(
            OutputKind::Success,
            "[5/5] Disk is ready and unassigned".into(),
        );
        Ok(FormatResult {
            label: label.to_string(),
            partition_path: partition,
            uuid,
            mount_path,
        })
    }

    fn mount_disk(
        &self,
        disk: &BlockDisk,
        label: &str,
        settings: &InstallSettings,
        emit: Emit<'_>,
    ) -> Result<FormatResult, String> {
        require_root()?;
        ensure_host_mount_namespace()?;
        if disk.system || disk.read_only {
            return Err(format!("{} is not eligible for an N2 mount", disk.path));
        }
        if disk.filesystem.as_deref() != Some("xfs") || disk.label.as_deref() != Some(label) {
            return Err(format!("{label} is not an initialized XFS N2 disk"));
        }
        if !disk.mountpoints.is_empty() {
            return Err(format!(
                "{label} is already mounted at {}",
                disk.mountpoints.join(", ")
            ));
        }
        let partition = disk
            .partition_path
            .clone()
            .unwrap_or_else(|| disk.path.clone());
        let uuid = disk
            .uuid
            .clone()
            .ok_or_else(|| format!("{label} has no filesystem UUID"))?;
        let mount_path = settings.mount_path(label);
        ensure_fstab_target_available(&settings.fstab_path, label, &mount_path)?;
        emit(
            OutputKind::Info,
            format!("Registering {}", mount_path.display()),
        );
        fs::create_dir_all(&mount_path)
            .map_err(|error| format!("create {}: {error}", mount_path.display()))?;
        update_fstab(&settings.fstab_path, label, &uuid, &mount_path)?;
        self.reload_systemd()?;
        run_command("mount", &[path_text(&mount_path)?], None)?;
        run_command(
            "chown",
            &[
                "-R",
                &format!("{}:{}", settings.run_user, settings.run_group),
                path_text(&mount_path)?,
            ],
            None,
        )?;
        emit(OutputKind::Success, format!("{label} mounted"));
        Ok(FormatResult {
            label: label.to_string(),
            partition_path: partition,
            uuid,
            mount_path,
        })
    }

    fn service_action(&self, service: &str, action: ServiceAction) -> Result<String, String> {
        require_root()?;
        run_command(
            "systemctl",
            &[action.as_str(), &format!("{service}.service")],
            None,
        )
    }

    fn service_is_active(&self, service: &str) -> bool {
        Command::new("systemctl")
            .args(["is-active", "--quiet", &format!("{service}.service")])
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn reload_systemd(&self) -> Result<(), String> {
        require_root()?;
        run_command("systemctl", &["daemon-reload"], None).map(|_| ())
    }

    fn identity_exists(&self, user: &str, group: &str) -> Result<(), String> {
        run_command("id", &["-u", user], None)
            .map_err(|_| format!("operating-system user '{user}' does not exist"))?;
        run_command("getent", &["group", group], None)
            .map_err(|_| format!("operating-system group '{group}' does not exist"))?;
        Ok(())
    }

    fn apply_ownership(&self, path: &Path, user: &str, group: &str) -> Result<(), String> {
        run_command(
            "chown",
            &["-R", &format!("{user}:{group}"), path_text(path)?],
            None,
        )
        .map(|_| ())
    }

    fn recent_logs(&self, service: &str, lines: usize) -> Result<String, String> {
        run_command(
            "journalctl",
            &[
                "--no-pager",
                "--unit",
                &format!("{service}.service"),
                "--lines",
                &lines.clamp(1, 1000).to_string(),
            ],
            None,
        )
    }
}

fn validate_format_candidate(disk: &BlockDisk) -> Result<(), String> {
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
    Ok(())
}

fn require_root() -> Result<(), String> {
    let uid = run_command("id", &["-u"], None)?;
    if uid.trim() != "0" {
        return Err("this operation requires root; start the setup server with sudo".into());
    }
    Ok(())
}

fn ensure_host_mount_namespace() -> Result<(), String> {
    let process = fs::metadata("/proc/self/ns/mnt")
        .map_err(|error| format!("inspect process mount namespace: {error}"))?;
    let init = fs::metadata("/proc/1/ns/mnt")
        .map_err(|error| format!("inspect host mount namespace: {error}"))?;
    if process.ino() != init.ino() || process.dev() != init.dev() {
        return Err(
            "disk provisioning is running inside an isolated mount namespace; start N2 Shell without PrivateTmp, PrivateMounts, or mount-namespace isolation"
                .into(),
        );
    }
    Ok(())
}

fn find_program(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(name))
        .find(|candidate| {
            candidate.metadata().is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            })
        })
}

fn run_command(program: &str, args: &[&str], stdin: Option<&str>) -> Result<String, String> {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    }
    let mut child = command
        .spawn()
        .map_err(|error| format!("could not run {program}: {error}"))?;
    if let Some(input) = stdin {
        child
            .stdin
            .as_mut()
            .ok_or_else(|| format!("could not open {program} stdin"))?
            .write_all(input.as_bytes())
            .map_err(|error| format!("write {program} stdin: {error}"))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|error| format!("wait for {program}: {error}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout)
        .trim_end()
        .to_string();
    if output.status.success() {
        return Ok(stdout);
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    Err(format!(
        "{} {} failed ({}): {}",
        program,
        args.join(" "),
        output.status,
        if stderr.is_empty() { stdout } else { stderr }
    ))
}

fn wait_for_partition(disk: &str) -> Result<String, String> {
    for _ in 0..50 {
        if let Ok(output) = run_command("lsblk", &["-nrpo", "PATH,TYPE", disk], None)
            && let Some(path) = output.lines().find_map(|line| {
                let mut columns = line.split_whitespace();
                let path = columns.next()?;
                (columns.next()? == "part").then(|| path.to_string())
            })
        {
            return Ok(path);
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(format!("partition did not appear after formatting {disk}"))
}

fn stable_disk_ids() -> Vec<(PathBuf, String)> {
    let mut result = Vec::new();
    let Ok(entries) = fs::read_dir("/dev/disk/by-id") else {
        return result;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.contains("-part") {
            continue;
        }
        let Ok(target) = fs::canonicalize(entry.path()) else {
            continue;
        };
        result.push((target, format!("/dev/disk/by-id/{name}")));
    }
    result
}

fn parse_lsblk_json(
    raw: &str,
    stable_ids: Vec<(PathBuf, String)>,
) -> Result<Vec<BlockDisk>, String> {
    let root: Value =
        serde_json::from_str(raw).map_err(|error| format!("parse lsblk JSON: {error}"))?;
    let entries = root
        .get("blockdevices")
        .and_then(Value::as_array)
        .ok_or_else(|| "lsblk JSON did not contain blockdevices".to_string())?;
    let mut disks = Vec::new();
    for entry in entries {
        if text_value(entry.get("type")).as_deref() != Some("disk") {
            continue;
        }
        let path = text_value(entry.get("path"))
            .or_else(|| text_value(entry.get("name")))
            .unwrap_or_default();
        let canonical = fs::canonicalize(&path).unwrap_or_else(|_| PathBuf::from(&path));
        let stable_id = stable_ids
            .iter()
            .find(|(target, _)| target == &canonical)
            .map(|(_, id)| id.clone())
            .unwrap_or_default();
        let mut descendants = Vec::new();
        collect_descendants(entry, &mut descendants);
        let mut mountpoints = BTreeSet::new();
        let mut filesystem = text_value(entry.get("fstype"));
        let mut label = text_value(entry.get("label"));
        let mut uuid = text_value(entry.get("uuid"));
        let mut partition_path = None;
        for child in &descendants {
            mountpoints.extend(mountpoint_values(child));
            if filesystem.is_none() {
                filesystem = text_value(child.get("fstype"));
                label = text_value(child.get("label"));
                uuid = text_value(child.get("uuid"));
            }
            if partition_path.is_none() && text_value(child.get("type")).as_deref() == Some("part")
            {
                partition_path = text_value(child.get("path"));
            }
        }
        mountpoints.extend(mountpoint_values(entry));
        let system = mountpoints.iter().any(|mount| {
            matches!(
                mount.as_str(),
                "/" | "/boot" | "/boot/efi" | "[SWAP]" | "swap"
            )
        });
        disks.push(BlockDisk {
            name: text_value(entry.get("kname"))
                .or_else(|| text_value(entry.get("name")))
                .unwrap_or_default()
                .trim_start_matches("/dev/")
                .to_string(),
            path,
            stable_id,
            model: text_value(entry.get("model")).unwrap_or_default(),
            serial: text_value(entry.get("serial")).unwrap_or_default(),
            size_bytes: number_value(entry.get("size")),
            read_only: bool_value(entry.get("ro")),
            removable: bool_value(entry.get("rm")),
            system,
            mountpoints: mountpoints.into_iter().collect(),
            filesystem,
            label,
            uuid,
            partition_path,
        });
    }
    disks.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(disks)
}

fn collect_descendants<'a>(entry: &'a Value, output: &mut Vec<&'a Value>) {
    if let Some(children) = entry.get("children").and_then(Value::as_array) {
        for child in children {
            output.push(child);
            collect_descendants(child, output);
        }
    }
}

fn mountpoint_values(entry: &Value) -> Vec<String> {
    if let Some(values) = entry.get("mountpoints").and_then(Value::as_array) {
        return values.iter().filter_map(text_value_ref).collect();
    }
    entry
        .get("mountpoint")
        .and_then(text_value_ref)
        .into_iter()
        .collect()
}

fn text_value(value: Option<&Value>) -> Option<String> {
    value.and_then(text_value_ref)
}

fn text_value_ref(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn number_value(value: Option<&Value>) -> u64 {
    value
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        })
        .unwrap_or(0)
}

fn bool_value(value: Option<&Value>) -> bool {
    value
        .and_then(|value| {
            value.as_bool().or_else(|| {
                value
                    .as_u64()
                    .map(|number| number != 0)
                    .or_else(|| value.as_str().map(|text| matches!(text, "1" | "true")))
            })
        })
        .unwrap_or(false)
}

fn update_fstab(path: &Path, label: &str, uuid: &str, mount_path: &Path) -> Result<(), String> {
    let mut current = if path.exists() {
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?
    } else {
        String::new()
    };
    let marker = format!("{MANAGED_FSTAB_PREFIX}{label}");
    let mount_text = mount_path.to_string_lossy();
    let exact_entry = current.lines().any(|line| {
        let mut columns = line.split_whitespace();
        columns.next() == Some(&format!("UUID={uuid}"))
            && columns.next() == Some(mount_text.as_ref())
            && columns.next() == Some("xfs")
    });
    if current.lines().any(|line| line.trim() == marker) && exact_entry {
        return Ok(());
    }
    if current.lines().any(|line| {
        line.trim() == marker
            || line.split_whitespace().next() == Some(&format!("UUID={uuid}"))
            || line.split_whitespace().nth(1) == Some(mount_text.as_ref())
    }) {
        return Err(format!(
            "{} already contains a conflicting entry for {label}, UUID {uuid}, or {}",
            path.display(),
            mount_path.display()
        ));
    }
    if !current.is_empty() && !current.ends_with('\n') {
        current.push('\n');
    }
    current.push_str(&marker);
    current.push('\n');
    current.push_str(&format!(
        "UUID={uuid} {} xfs defaults,noatime,nofail,x-systemd.device-timeout=30 0 2\n",
        mount_path.display()
    ));
    atomic_write(path, current.as_bytes(), 0o644)
}

fn ensure_fstab_target_available(
    path: &Path,
    label: &str,
    mount_path: &Path,
) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let current =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let marker = format!("{MANAGED_FSTAB_PREFIX}{label}");
    let mount_text = mount_path.to_string_lossy();
    if current.lines().any(|line| {
        line.trim() == marker || line.split_whitespace().nth(1) == Some(mount_text.as_ref())
    }) {
        return Err(format!(
            "{} already reserves {label} or {}; inspect the existing mount before formatting",
            path.display(),
            mount_path.display()
        ));
    }
    Ok(())
}

fn path_text(path: &Path) -> Result<&str, String> {
    path.to_str()
        .ok_or_else(|| format!("path is not valid UTF-8: {}", path.display()))
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8], mode: u32) -> Result<(), String> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| format!("create {}: {error}", parent.display()))?;
    let previous_identity = fs::metadata(path).ok().map(|metadata| {
        (
            metadata.uid(),
            metadata.gid(),
            metadata.permissions().mode(),
        )
    });
    let temporary = parent.join(format!(
        ".{}.{}.{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        std::process::id(),
        Uuid::new_v4()
    ));
    let mut options = OpenOptions::new();
    options.create_new(true).write(true).mode(mode);
    let mut file = options
        .open(&temporary)
        .map_err(|error| format!("create {}: {error}", temporary.display()))?;
    let result = (|| {
        file.write_all(bytes)
            .map_err(|error| format!("write {}: {error}", temporary.display()))?;
        file.sync_all()
            .map_err(|error| format!("fsync {}: {error}", temporary.display()))?;
        let final_mode = previous_identity
            .map(|(_, _, previous_mode)| previous_mode & 0o7777)
            .unwrap_or(mode);
        fs::set_permissions(&temporary, fs::Permissions::from_mode(final_mode))
            .map_err(|error| format!("chmod {}: {error}", temporary.display()))?;
        if let Some((uid, gid, _)) = previous_identity {
            chown(&temporary, Some(uid), Some(gid))
                .map_err(|error| format!("chown {}: {error}", temporary.display()))?;
        }
        fs::rename(&temporary, path)
            .map_err(|error| format!("replace {}: {error}", path.display()))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("fsync {}: {error}", parent.display()))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub(crate) fn valid_ndisk_label(label: &str) -> bool {
    label.len() == 9
        && label.starts_with("N2DISK")
        && label[6..].bytes().all(|byte| byte.is_ascii_digit())
}

pub(crate) fn find_disk<'a>(disks: &'a [BlockDisk], target: &str) -> Option<&'a BlockDisk> {
    disks.iter().find(|disk| {
        disk.path == target
            || disk.stable_id == target
            || disk.name == target
            || disk.label.as_deref() == Some(target)
    })
}

pub(crate) fn human_bytes(bytes: u64) -> String {
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

pub(crate) fn render_node_unit(settings: &InstallSettings) -> String {
    format!(
        "[Unit]\nDescription=Litiaina N2 Node %i\nWants=network-online.target\nAfter=network-online.target\n\n[Service]\nType=simple\nUser={}\nGroup={}\nWorkingDirectory={}/nodes/%i\nEnvironmentFile={}\nEnvironment=RUST_LOG=n2=info,tower_http=info\nExecStart={}\nRestart=always\nRestartSec=3\nLimitNOFILE=1048576\nTasksMax=infinity\nTimeoutStopSec=90\nKillSignal=SIGTERM\nNoNewPrivileges=true\nPrivateTmp=true\nProtectSystem=full\nProtectHome=read-only\nReadWritePaths={} {}\n\n[Install]\nWantedBy=multi-user.target\n",
        settings.run_user,
        settings.run_group,
        systemd_escape(&settings.runtime_root),
        systemd_escape(&settings.env_path),
        systemd_escape(&settings.engine_binary),
        systemd_escape(&settings.runtime_root),
        systemd_escape(&settings.mount_root)
    )
}

pub(crate) fn render_gateway_unit(settings: &InstallSettings) -> String {
    format!(
        "[Unit]\nDescription=Litiaina N2 Gateway\nWants=network-online.target\nAfter=network-online.target\n\n[Service]\nType=simple\nUser={}\nGroup={}\nWorkingDirectory={}\nEnvironment=RUST_LOG=n2_gateway=info,tower_http=info\nExecStart={}\nRestart=always\nRestartSec=3\nLimitNOFILE=262144\nTasksMax=infinity\nTimeoutStopSec=60\nKillSignal=SIGTERM\nNoNewPrivileges=true\nPrivateTmp=true\nProtectSystem=full\nProtectHome=read-only\nReadWritePaths={}\n\n[Install]\nWantedBy=multi-user.target\n",
        settings.run_user,
        settings.run_group,
        systemd_escape(&settings.gateway_runtime),
        systemd_escape(&settings.gateway_binary),
        systemd_escape(&settings.gateway_runtime)
    )
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

pub(crate) fn format_disk_details(disk: &BlockDisk) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsblk_marks_parent_of_root_partition_protected() {
        let raw = r#"{"blockdevices":[{"name":"/dev/sda","kname":"sda","path":"/dev/sda","type":"disk","size":100,"ro":false,"rm":false,"children":[{"name":"/dev/sda1","path":"/dev/sda1","type":"part","fstype":"ext4","mountpoints":["/"]}]}]}"#;
        let disks = parse_lsblk_json(raw, Vec::new()).unwrap();
        assert_eq!(disks.len(), 1);
        assert!(disks[0].system);
        assert_eq!(disks[0].state(), "protected");
    }

    #[test]
    fn labels_are_strict() {
        assert!(valid_ndisk_label("N2DISK001"));
        assert!(!valid_ndisk_label("N2DISK01"));
        assert!(!valid_ndisk_label("N1DISK001"));
    }
}
