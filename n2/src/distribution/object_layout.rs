use crate::core::disk_manager::ObjectPathManager;
use crate::distribution::placement::ClusterPlacementPlan;
use crate::enums::app_error_enum::AppError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterObjectLayoutRecord {
    pub format: String,
    pub fragment: String,
    pub logical_path: String,
    pub physical_object_id: String,
    pub version_id: String,
    pub placement: ClusterPlacementPlan,
}

fn object_hash(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

fn layout_dir(root: PathBuf, fragment: &str, physical_object_id: &str) -> PathBuf {
    let hash = object_hash(physical_object_id);
    root.join(fragment)
        .join(".cluster")
        .join("objects")
        .join(&hash[0..2])
        .join(&hash[2..4])
        .join(format!("@o_{physical_object_id}"))
}

pub fn write_cluster_object_layout(
    fragment: &str,
    logical_path: &str,
    physical_object_id: &str,
    version_id: &str,
    placement: ClusterPlacementPlan,
) -> Result<(), AppError> {
    let record = ClusterObjectLayoutRecord {
        format: "n2.cluster.object.layout.v1".to_string(),
        fragment: fragment.to_string(),
        logical_path: logical_path.to_string(),
        physical_object_id: physical_object_id.to_string(),
        version_id: version_id.to_string(),
        placement,
    };
    let bytes = serde_json::to_vec_pretty(&record)
        .map_err(|e| AppError::InternalServerError(format!("encode N2 object layout: {e}")))?;
    let pm = ObjectPathManager::new();
    let mut wrote = false;
    for root in pm.get_all_index_roots() {
        let dir = layout_dir(root, fragment, physical_object_id);
        fs::create_dir_all(&dir).map_err(|e| {
            AppError::Io(e, format!("create N2 object layout dir {}", dir.display()))
        })?;
        let path = dir.join(format!("{version_id}.nmtd"));
        fs::write(&path, &bytes)
            .map_err(|e| AppError::Io(e, format!("write N2 object layout {}", path.display())))?;
        wrote = true;
    }
    if !wrote {
        return Err(AppError::InternalServerError(
            "no index roots available for N2 object layout".into(),
        ));
    }
    Ok(())
}
