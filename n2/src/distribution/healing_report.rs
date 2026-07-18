use crate::core::disk_manager::FS_STORAGE_ROOT;
use crate::enums::app_error_enum::AppError;
use crate::namespace::inode_store::InodeLayout;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AutoHealingReport {
    pub report_id: String,
    pub scan_id: String,
    pub membership_epoch: u64,
    pub fragment_id: String,
    pub node_id: String,
    pub finding: String,
    pub entry_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inode_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_path: Option<String>,
    pub initial_status: String,
    pub repair_attempted: bool,
    pub repair_outcome: String,
    pub cluster_read_only_triggered: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_inode_layout: Option<InodeLayout>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observations: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_health_before: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_health_after: Option<Value>,
    #[serde(default)]
    pub repaired_replicas: u64,
    #[serde(default)]
    pub repaired_shards: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub observed_at_unix_nanos: i64,
    pub completed_at_unix_nanos: i64,
}

impl AutoHealingReport {
    pub fn new(
        scan_id: &str,
        membership_epoch: u64,
        fragment_id: &str,
        node_id: &str,
        finding: &str,
        entry_type: &str,
    ) -> Self {
        let now = now_nanos();
        Self {
            report_id: Ulid::new().to_string(),
            scan_id: scan_id.to_string(),
            membership_epoch,
            fragment_id: fragment_id.to_string(),
            node_id: node_id.to_string(),
            finding: finding.to_string(),
            entry_type: entry_type.to_string(),
            inode_id: None,
            object_id: None,
            version_id: None,
            logical_path: None,
            initial_status: "degraded".into(),
            repair_attempted: false,
            repair_outcome: "not_attempted".into(),
            cluster_read_only_triggered: false,
            expected_inode_layout: None,
            observations: Vec::new(),
            object_health_before: None,
            object_health_after: None,
            repaired_replicas: 0,
            repaired_shards: 0,
            reason: None,
            observed_at_unix_nanos: now,
            completed_at_unix_nanos: now,
        }
    }

    pub fn filename(&self) -> String {
        let stable_id = self
            .object_id
            .as_deref()
            .or(self.inode_id.as_deref())
            .unwrap_or(&self.fragment_id);
        format!(
            "{}__node-{}__{}__{}__{}.json",
            sanitize(&self.report_id),
            sanitize(&self.node_id),
            sanitize(&self.entry_type),
            sanitize(stable_id),
            sanitize(&self.repair_outcome)
        )
    }
}

pub fn reports_root() -> Result<PathBuf, AppError> {
    FS_STORAGE_ROOT
        .get()
        .map(|root| PathBuf::from(root).join(".reports").join("auto-heal"))
        .ok_or_else(|| AppError::InternalServerError("N2 storage root is not initialized".into()))
}

pub async fn write_local_report(report: &AutoHealingReport) -> Result<PathBuf, AppError> {
    let directory = reports_root()?.join(sanitize(&report.fragment_id));
    fs::create_dir_all(&directory)
        .await
        .map_err(|error| AppError::Io(error, format!("create {}", directory.display())))?;
    let path = directory.join(report.filename());
    let temporary = directory.join(format!(".{}.tmp", report.report_id));
    let bytes = serde_json::to_vec_pretty(report).map_err(|error| {
        AppError::InternalServerError(format!("encode N2 auto-healing report: {error}"))
    })?;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await
        .map_err(|error| AppError::Io(error, format!("create {}", temporary.display())))?;
    if let Err(error) = file.write_all(&bytes).await {
        let _ = fs::remove_file(&temporary).await;
        return Err(AppError::Io(
            error,
            format!("write {}", temporary.display()),
        ));
    }
    file.sync_all()
        .await
        .map_err(|error| AppError::Io(error, format!("sync {}", temporary.display())))?;
    drop(file);
    fs::rename(&temporary, &path)
        .await
        .map_err(|error| AppError::Io(error, format!("publish {}", path.display())))?;
    sync_directory(&directory).await?;
    Ok(path)
}

fn now_nanos() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp().saturating_mul(1_000_000_000))
}

fn sanitize(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "unknown".into()
    } else {
        sanitized
    }
}

async fn sync_directory(path: &Path) -> Result<(), AppError> {
    let directory = fs::File::open(path)
        .await
        .map_err(|error| AppError::Io(error, format!("open {}", path.display())))?;
    directory
        .sync_all()
        .await
        .map_err(|error| AppError::Io(error, format!("sync {}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_filename_is_stable_safe_and_specific() {
        let mut report = AutoHealingReport::new(
            "scan",
            7,
            "fragment-a",
            "namespace/a",
            "directory_inode_degraded",
            "directory-inode",
        );
        report.inode_id = Some("inode-a".into());
        report.repair_outcome = "repair_succeeded".into();
        let filename = report.filename();
        assert!(filename.contains("node-namespace_a"));
        assert!(filename.contains("directory-inode"));
        assert!(filename.contains("inode-a"));
        assert!(filename.ends_with("repair_succeeded.json"));
        assert!(!filename.contains('/'));
    }
}
