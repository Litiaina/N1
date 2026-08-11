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

        emit(OutputKind::Info, "[5/5] Applying N1 ownership".into());
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
        if disk.system || disk.read_only {
            return Err(format!("{} is not eligible for an N1 mount", disk.path));
        }
        if disk.filesystem.as_deref() != Some("xfs") || disk.label.as_deref() != Some(label) {
            return Err(format!("{label} is not an initialized XFS N1 disk"));
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

fn require_root() -> Result<(), String> {
    let uid = run_command("id", &["-u"], None)?;
    if uid.trim() != "0" {
        return Err("this operation requires root; start the setup server with sudo".into());
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
            "{} already contains an entry for {label}, UUID {uuid}, or {}",
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

fn atomic_write(path: &Path, bytes: &[u8], mode: u32) -> Result<(), String> {
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
        self.write_config(&config)
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
        self.write_config(&config)
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
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Default)]
    struct FakeHost {
        disks: Mutex<Vec<BlockDisk>>,
        active: Mutex<bool>,
        actions: Mutex<Vec<String>>,
    }

    impl FakeHost {
        fn with_disks(disks: Vec<BlockDisk>) -> Self {
            Self {
                disks: Mutex::new(disks),
                ..Self::default()
            }
        }
    }

    impl HostOps for FakeHost {
        fn require_privileged(&self) -> Result<(), String> {
            Ok(())
        }

        fn check_prerequisites(&self) -> Result<Vec<String>, String> {
            Ok(vec!["fake prerequisites".into()])
        }

        fn list_disks(&self) -> Result<Vec<BlockDisk>, String> {
            Ok(self.disks.lock().unwrap().clone())
        }

        fn format_disk(
            &self,
            disk: &BlockDisk,
            label: &str,
            settings: &InstallSettings,
            emit: Emit<'_>,
        ) -> Result<FormatResult, String> {
            emit(OutputKind::Info, "fake format".into());
            let mount = settings.mount_path(label);
            fs::create_dir_all(&mount).map_err(|error| error.to_string())?;
            let mut disks = self.disks.lock().unwrap();
            let stored = disks
                .iter_mut()
                .find(|item| item.path == disk.path)
                .unwrap();
            stored.filesystem = Some("xfs".into());
            stored.label = Some(label.into());
            stored.uuid = Some(format!("uuid-{label}"));
            stored.partition_path = Some(format!("{}1", disk.path));
            stored.mountpoints = vec![mount.to_string_lossy().to_string()];
            Ok(FormatResult {
                label: label.into(),
                partition_path: format!("{}1", disk.path),
                uuid: format!("uuid-{label}"),
                mount_path: mount,
            })
        }

        fn mount_disk(
            &self,
            disk: &BlockDisk,
            label: &str,
            settings: &InstallSettings,
            emit: Emit<'_>,
        ) -> Result<FormatResult, String> {
            emit(OutputKind::Info, "fake mount".into());
            let mount = settings.mount_path(label);
            fs::create_dir_all(&mount).map_err(|error| error.to_string())?;
            let mut disks = self.disks.lock().unwrap();
            let stored = disks
                .iter_mut()
                .find(|item| item.path == disk.path)
                .unwrap();
            stored.mountpoints = vec![mount.to_string_lossy().to_string()];
            Ok(FormatResult {
                label: label.into(),
                partition_path: stored
                    .partition_path
                    .clone()
                    .unwrap_or_else(|| stored.path.clone()),
                uuid: stored.uuid.clone().unwrap_or_else(|| "test-uuid".into()),
                mount_path: mount,
            })
        }

        fn service_action(&self, _service: &str, action: ServiceAction) -> Result<String, String> {
            self.actions.lock().unwrap().push(action.as_str().into());
            match action {
                ServiceAction::Start | ServiceAction::Restart => {
                    *self.active.lock().unwrap() = true
                }
                ServiceAction::Stop => *self.active.lock().unwrap() = false,
                _ => {}
            }
            Ok(String::new())
        }

        fn service_is_active(&self, _service: &str) -> bool {
            *self.active.lock().unwrap()
        }

        fn reload_systemd(&self) -> Result<(), String> {
            Ok(())
        }

        fn identity_exists(&self, _user: &str, _group: &str) -> Result<(), String> {
            Ok(())
        }

        fn apply_ownership(&self, _path: &Path, _user: &str, _group: &str) -> Result<(), String> {
            Ok(())
        }

        fn recent_logs(&self, _service: &str, _lines: usize) -> Result<String, String> {
            Ok("test log".into())
        }
    }

    fn temporary_root(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("n1-shell-{name}-{}-{unique}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn settings(root: &Path) -> InstallSettings {
        InstallSettings {
            install_dir: root.join("install"),
            engine_binary: root.join("install/litiaina-n1"),
            mount_root: root.join("mnt"),
            fstab_path: root.join("fstab"),
            service_name: "litiaina-n1-test".into(),
            systemd_unit_dir: root.join("systemd"),
            run_user: "test".into(),
            run_group: "test".into(),
        }
    }

    fn disk(path: &str, system: bool) -> BlockDisk {
        BlockDisk {
            name: path.trim_start_matches("/dev/").into(),
            path: path.into(),
            stable_id: format!("/dev/disk/by-id/test-{}", path.trim_start_matches("/dev/")),
            model: "Test Disk".into(),
            serial: format!("SERIAL-{}", path.trim_start_matches("/dev/")),
            size_bytes: 4 * 1024 * 1024 * 1024,
            read_only: false,
            removable: false,
            system,
            mountpoints: if system { vec!["/".into()] } else { Vec::new() },
            filesystem: if system { Some("ext4".into()) } else { None },
            label: None,
            uuid: None,
            partition_path: None,
        }
    }

    fn execute(
        shell: &InstallShell,
        command: &str,
        confirmation: Option<&str>,
    ) -> (CommandOutcome, Vec<String>) {
        let mut lines = Vec::new();
        let result = shell.execute(command, confirmation, &mut |_, line| lines.push(line));
        (result, lines)
    }

    #[test]
    fn parser_handles_quotes_and_rejects_incomplete_input() {
        assert_eq!(
            split_command("config tls '/tmp/a cert.pem' /tmp/key.pem").unwrap(),
            vec!["config", "tls", "/tmp/a cert.pem", "/tmp/key.pem"]
        );
        assert!(split_command("config tls 'broken").is_err());
    }

    #[test]
    fn protected_system_disk_can_never_reach_confirmation() {
        let root = temporary_root("protected");
        let host = Arc::new(FakeHost::with_disks(vec![disk("/dev/sda", true)]));
        let shell = InstallShell::new(settings(&root), host);
        let (result, output) = execute(&shell, "disk format /dev/sda NDISK001", None);
        assert!(!result.success);
        assert!(result.confirmation.is_none());
        assert!(output.join("\n").contains("protected system disk"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn format_requires_exact_model_bound_confirmation() {
        let root = temporary_root("confirmation");
        let host = Arc::new(FakeHost::with_disks(vec![disk("/dev/sdb", false)]));
        let shell = InstallShell::new(settings(&root), host);
        let (pending, _) = execute(&shell, "disk format /dev/sdb NDISK001", None);
        let confirmation = pending.confirmation.expect("confirmation");
        assert_eq!(confirmation.expected, "FORMAT NDISK001 SERIAL-sdb");
        let (rejected, _) = execute(
            &shell,
            "disk format /dev/sdb NDISK001",
            Some("FORMAT NDISK001"),
        );
        assert!(rejected.confirmation.is_some());
        let (completed, _) = execute(
            &shell,
            "disk format /dev/sdb NDISK001",
            Some("FORMAT NDISK001 SERIAL-sdb"),
        );
        assert!(completed.success);
        assert_eq!(shell.host.list_disks().unwrap()[0].state(), "mounted");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn existing_fstab_target_blocks_reformat_before_disk_changes() {
        let root = temporary_root("fstab-collision");
        let settings = settings(&root);
        fs::write(
            &settings.fstab_path,
            format!(
                "{MANAGED_FSTAB_PREFIX}NDISK001\nUUID=old {} xfs defaults 0 2\n",
                settings.mount_path("NDISK001").display()
            ),
        )
        .unwrap();
        let host = Arc::new(FakeHost::with_disks(vec![disk("/dev/sdb", false)]));
        let shell = InstallShell::new(settings, host.clone());
        let (result, output) = execute(
            &shell,
            "disk format /dev/sdb NDISK001",
            Some("FORMAT NDISK001 SERIAL-sdb"),
        );
        assert!(!result.success);
        assert!(output.join("\n").contains("already reserves"));
        assert!(host.list_disks().unwrap()[0].filesystem.is_none());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn initialized_unmounted_xfs_disk_can_resume_at_mount_stage() {
        let root = temporary_root("resume-mount");
        let settings = settings(&root);
        let mut initialized = disk("/dev/sdb", false);
        initialized.filesystem = Some("xfs".into());
        initialized.label = Some("NDISK001".into());
        initialized.uuid = Some("uuid-NDISK001".into());
        initialized.partition_path = Some("/dev/sdb1".into());
        let host = Arc::new(FakeHost::with_disks(vec![initialized]));
        let shell = InstallShell::new(settings.clone(), host.clone());
        let (result, output) = execute(&shell, "disk mount NDISK001", None);
        assert!(result.success, "{}", output.join("\n"));
        assert_eq!(
            host.list_disks().unwrap()[0].mountpoints,
            vec![
                settings
                    .mount_path("NDISK001")
                    .to_string_lossy()
                    .to_string()
            ]
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn four_disk_assignment_and_ec_validation_is_deterministic() {
        let root = temporary_root("topology");
        let settings = settings(&root);
        fs::create_dir_all(&settings.install_dir).unwrap();
        fs::write(&settings.engine_binary, b"engine").unwrap();
        let mut permissions = fs::metadata(&settings.engine_binary).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&settings.engine_binary, permissions).unwrap();
        fs::write(settings.install_dir.join("cert.pem"), b"cert").unwrap();
        fs::write(settings.install_dir.join("key.pem"), b"key").unwrap();
        fs::write(settings.env_path(), generated_env()).unwrap();
        fs::create_dir_all(&settings.systemd_unit_dir).unwrap();
        fs::write(settings.unit_path(), b"test unit").unwrap();
        let host = Arc::new(FakeHost::with_disks(vec![
            disk("/dev/sdb", false),
            disk("/dev/sdc", false),
            disk("/dev/sdd", false),
            disk("/dev/sde", false),
        ]));
        let shell = InstallShell::new(settings.clone(), host);
        for (index, device) in ["sdb", "sdc", "sdd", "sde"].iter().enumerate() {
            let label = format!("NDISK{:03}", index + 1);
            let confirmation = format!("FORMAT {label} SERIAL-{device}");
            assert!(
                execute(
                    &shell,
                    &format!("disk format /dev/{device} {label}"),
                    Some(&confirmation)
                )
                .0
                .success
            );
            assert!(
                execute(&shell, &format!("disk assign {label} both"), None)
                    .0
                    .success
            );
        }
        assert!(execute(&shell, "config ec 3 1", None).0.success);
        let mut config = shell.load_or_create_config().unwrap();
        let control = settings.install_dir.join("fs-storage-path");
        fs::create_dir_all(&control).unwrap();
        config
            .with_section(Some("disk_config"))
            .set("fs_storage_path", control.to_string_lossy().as_ref());
        config
            .with_section(Some("server"))
            .set(
                "cert_path",
                settings
                    .install_dir
                    .join("cert.pem")
                    .to_string_lossy()
                    .as_ref(),
            )
            .set(
                "key_path",
                settings
                    .install_dir
                    .join("key.pem")
                    .to_string_lossy()
                    .as_ref(),
            );
        shell.write_config(&config).unwrap();
        let (validated, output) = execute(&shell, "config validate", None);
        assert!(validated.success, "{}", output.join("\n"));
        assert!(output.join("\n").contains("EC: 3 + 1"));
        let (started, output) = execute(&shell, "n1 start", None);
        assert!(started.success, "{}", output.join("\n"));
        let (rejected, output) = execute(&shell, "config ec 2 1", None);
        assert!(!rejected.success);
        assert!(output.join("\n").contains("stop N1"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn initialization_is_restart_safe_and_keeps_service_disabled() {
        let root = temporary_root("init");
        let settings = settings(&root);
        fs::create_dir_all(&settings.install_dir).unwrap();
        fs::write(&settings.engine_binary, b"engine").unwrap();
        let mut permissions = fs::metadata(&settings.engine_binary).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&settings.engine_binary, permissions).unwrap();
        let host = Arc::new(FakeHost::default());
        let shell = InstallShell::new(settings.clone(), host.clone());

        let (first, output) = execute(&shell, "n1 init", None);
        assert!(first.success, "{}", output.join("\n"));
        assert!(settings.config_path().is_file());
        assert!(settings.env_path().is_file());
        assert!(settings.unit_path().is_file());
        assert!(!host.service_is_active(&settings.service_name));
        assert_eq!(host.actions.lock().unwrap().as_slice(), ["disable"]);

        let original_env = fs::read(settings.env_path()).unwrap();
        let (second, output) = execute(&shell, "n1 init", None);
        assert!(second.success, "{}", output.join("\n"));
        assert_eq!(fs::read(settings.env_path()).unwrap(), original_env);
        assert_eq!(
            host.actions.lock().unwrap().as_slice(),
            ["disable", "disable"]
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn lsblk_parser_propagates_system_mounts_to_parent_disk() {
        let raw = r#"{
          "blockdevices": [
            {"name":"/dev/nvme0n1","kname":"nvme0n1","path":"/dev/nvme0n1","type":"disk","size":1000,"ro":false,"rm":false,
             "children":[{"name":"/dev/nvme0n1p1","path":"/dev/nvme0n1p1","type":"part","fstype":"ext4","mountpoints":["/"]}]},
            {"name":"/dev/sdb","kname":"sdb","path":"/dev/sdb","type":"disk","size":2000,"ro":false,"rm":false,"mountpoints":[null]}
          ]
        }"#;
        let parsed = parse_lsblk_json(raw, Vec::new()).unwrap();
        assert!(parsed[0].system);
        assert_eq!(parsed[0].state(), "protected");
        assert!(!parsed[1].system);
        assert_eq!(parsed[1].state(), "candidate");
    }
}
