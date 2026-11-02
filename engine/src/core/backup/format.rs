//! Recovery repository format; deliberately separate from foreground shards.
use super::{Result, invalid};
use crate::core::storage_pool::MemberMarker;
use n1_link::{PoolId, PoolLayout};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
pub const FORMAT: &str = "N1-LINK-backup-v1";
pub const ENGINE_FORMAT: &str = "LINK-v1";
pub const BLOCK_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolDescriptor {
    pub layout: PoolLayout,
    pub members: Vec<PoolId>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Root {
    pub pool: usize,
    pub slot: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    #[serde(default)]
    pub service_control: Option<String>,
    pub format: String,
    pub engine_format: String,
    pub id: String,
    pub created_at: i64,
    pub capture_mode: String,
    pub pools: Vec<PoolDescriptor>,
    pub roots: Vec<Root>,
    pub records: u64,
    pub files: u64,
    pub source_bytes: u64,
    pub manifest_depth: u8,
    pub manifest: Vec<String>,
}
/// File chunks are independent bounded records. Even a multi-terabyte shard
/// does not require one enormous vector of block hashes in a manifest record.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub root: usize,
    pub path: String,
    pub offset: u64,
    pub total_bytes: u64,
    pub bytes: u32,
    pub hash: String,
    pub last: bool,
}
impl Snapshot {
    pub fn marker(&self, root: usize) -> Result<MemberMarker> {
        let root = self
            .roots
            .get(root)
            .ok_or_else(|| invalid("backup root ordinal out of range"))?;
        let pool = self
            .pools
            .get(root.pool)
            .ok_or_else(|| invalid("backup pool ordinal out of range"))?;
        Ok(MemberMarker {
            format: "LINK-pool-v1".into(),
            layout: pool.layout.clone(),
            members: pool.members.clone(),
            slot: root.slot,
        })
    }
}
pub fn validate_snapshot(snapshot: &Snapshot) -> Result<()> {
    if let Some(hash) = &snapshot.service_control {
        super::fs::safe_hash(hash)?;
    }
    super::fs::safe_id(&snapshot.id)?;
    if snapshot.format != FORMAT
        || snapshot.engine_format != ENGINE_FORMAT
        || snapshot.pools.is_empty()
        || !matches!(
            snapshot.capture_mode.as_str(),
            "online_committed" | "offline_all"
        )
    {
        return Err(invalid("unsupported LINK recovery format or capture mode"));
    }
    let mut pools = HashSet::new();
    let mut width = 0;
    for pool in &snapshot.pools {
        pool.layout.validate()?;
        width += pool.layout.width();
        if !pools.insert((pool.layout.id, pool.layout.generation))
            || pool.members.len() != pool.layout.width()
            || pool.members.iter().collect::<HashSet<_>>().len() != pool.layout.width()
        {
            return Err(invalid("invalid recovery pool membership"));
        }
    }
    let mut slots = HashSet::new();
    for root in &snapshot.roots {
        let pool = snapshot
            .pools
            .get(root.pool)
            .ok_or_else(|| invalid("recovery root refers to an unknown pool"))?;
        if root.slot >= pool.layout.width() || !slots.insert((root.pool, root.slot)) {
            return Err(invalid("invalid or duplicate recovery slot"));
        }
    }
    if slots.len() != width || snapshot.manifest.len() > 4096 || snapshot.manifest_depth > 8 {
        return Err(invalid(
            "incomplete recovery membership or oversized manifest",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Summary {
    pub id: String,
    pub created_at: i64,
    pub capture_mode: String,
    pub pools: usize,
    pub members: usize,
    pub files: u64,
    pub records: u64,
    pub source_bytes: u64,
    pub manifest_depth: u8,
}
impl From<&Snapshot> for Summary {
    fn from(point: &Snapshot) -> Self {
        Self {
            id: point.id.clone(),
            created_at: point.created_at,
            capture_mode: point.capture_mode.clone(),
            pools: point.pools.len(),
            members: point.roots.len(),
            files: point.files,
            records: point.records,
            source_bytes: point.source_bytes,
            manifest_depth: point.manifest_depth,
        }
    }
}
