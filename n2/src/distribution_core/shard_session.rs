use crate::distribution_core::model::{NodeDescriptor, ObjectIdentity};
use sha2::Sha256;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub(crate) struct LocalCoordinatorShardSession {
    pub(crate) target: NodeDescriptor,
    pub(crate) cluster_shard_index: usize,
    pub(crate) temp_root: PathBuf,
    pub(crate) temp_file_path: PathBuf,
    pub(crate) checksum: Sha256,
    pub(crate) size_bytes: u64,
    pub(crate) failed: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct RemoteCoordinatorShardSession {
    pub(crate) target: NodeDescriptor,
    pub(crate) cluster_shard_index: usize,
    pub(crate) session_id: String,
    pub(crate) pending: Vec<u8>,
    pub(crate) checksum: Sha256,
    pub(crate) size_bytes: u64,
    pub(crate) failed: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) enum CoordinatorShardSession {
    Local(LocalCoordinatorShardSession),
    Remote(RemoteCoordinatorShardSession),
}

impl CoordinatorShardSession {
    pub(crate) fn cluster_shard_index(&self) -> usize {
        match self {
            CoordinatorShardSession::Local(v) => v.cluster_shard_index,
            CoordinatorShardSession::Remote(v) => v.cluster_shard_index,
        }
    }

    pub(crate) fn target(&self) -> &NodeDescriptor {
        match self {
            CoordinatorShardSession::Local(v) => &v.target,
            CoordinatorShardSession::Remote(v) => &v.target,
        }
    }

    pub(crate) fn is_failed(&self) -> bool {
        match self {
            CoordinatorShardSession::Local(v) => v.failed.is_some(),
            CoordinatorShardSession::Remote(v) => v.failed.is_some(),
        }
    }

    pub(crate) fn mark_failed(&mut self, reason: String) {
        match self {
            CoordinatorShardSession::Local(v) => {
                if v.failed.is_none() {
                    v.failed = Some(reason);
                }
            }
            CoordinatorShardSession::Remote(v) => {
                if v.failed.is_none() {
                    v.failed = Some(reason);
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct InternalShardUploadSession {
    pub(crate) object: ObjectIdentity,
    pub(crate) cluster_shard_index: usize,
    pub(crate) kd: u8,
    pub(crate) md: u8,
    pub(crate) stripe_block_size: usize,
    pub(crate) temp_root: PathBuf,
    pub(crate) temp_file_path: PathBuf,
}

type InternalShardUploadSessionStore = Mutex<HashMap<String, InternalShardUploadSession>>;
static INTERNAL_SHARD_UPLOAD_SESSIONS: OnceLock<InternalShardUploadSessionStore> = OnceLock::new();

pub(crate) fn internal_shard_upload_sessions() -> &'static InternalShardUploadSessionStore {
    INTERNAL_SHARD_UPLOAD_SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}
