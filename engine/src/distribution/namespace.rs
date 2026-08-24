use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use futures_util::stream::{FuturesUnordered, StreamExt};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::core::disk_manager::ObjectMetadata;
use crate::distribution::ncnx::{
    LocatedNcnx, NcnxRecoverBatchItem, NcnxRenameBatchItem, NcnxTrashBatchItem,
    PEER_NCNX_BATCH_MAX, PeerNcnxName, copy_live_to_key, copy_live_to_key_with_record,
    copy_live_to_trash, find_live, find_live_for_survival_write, list_canonical_names, ncnx_cursor,
    publish_new, read_canonical_record, recover_trash_batch, remove_live, remove_trash,
    rename_live_batch, trash_is_expired, trash_live_batch,
};
use crate::enums::app_error_enum::AppError;
use crate::namespace::models::{
    LifecycleState, NamespaceEntry, NamespaceEntryKind, NamespaceMutationSummary,
};
use crate::namespace::ncnx_store::{NcnxRecord, TrashLocator, validate_logical_key};
use crate::namespace::paths::{base_name, normalize_path, parent_path, rename_prefix};
use crate::namespace::runtime::{NamespaceObjectRef, NamespaceObjectRefPage};

mod bulk;
mod commit;
mod entries;
mod mutation;
mod query;

use bulk::*;
pub use commit::*;
use entries::*;
use mutation::*;
pub use query::*;

const LIST_BATCH_MAX: usize = 1_000;
const PREFIX_MUTATION_CONCURRENCY: usize = 32;
const PREFIX_PLANNING_GLOBAL_CONCURRENCY: usize = 128;
const PREFIX_BATCH_GLOBAL_CONCURRENCY: usize = 4;

static PREFIX_PLANNING_BUDGET: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::new(PREFIX_PLANNING_GLOBAL_CONCURRENCY)));
static PREFIX_BATCH_BUDGET: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::new(PREFIX_BATCH_GLOBAL_CONCURRENCY)));

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceMutationPhase {
    Planning,
    Executing,
}

#[derive(Clone, Debug)]
pub struct NamespaceMutationProgress {
    pub phase: NamespaceMutationPhase,
    pub discovered: usize,
    pub total: Option<usize>,
    pub completed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub current_path: Option<String>,
}

pub type NamespaceProgressObserver = Arc<dyn Fn(NamespaceMutationProgress) + Send + Sync>;

#[derive(Clone, Default)]
pub struct NamespaceMutationControl {
    observer: Option<NamespaceProgressObserver>,
    cancel: Option<CancellationToken>,
}

impl NamespaceMutationControl {
    pub fn new(observer: NamespaceProgressObserver, cancel: CancellationToken) -> Self {
        Self {
            observer: Some(observer),
            cancel: Some(cancel),
        }
    }

    fn ensure_running(&self) -> Result<(), AppError> {
        if self
            .cancel
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(AppError::Conflict(
                "namespace operation was cancelled".into(),
            ));
        }
        Ok(())
    }

    fn report(&self, progress: NamespaceMutationProgress) {
        if let Some(observer) = &self.observer {
            observer(progress);
        }
    }

    fn planning(&self, discovered: usize, current_path: Option<String>) {
        self.report(NamespaceMutationProgress {
            phase: NamespaceMutationPhase::Planning,
            discovered,
            total: None,
            completed: 0,
            succeeded: 0,
            failed: 0,
            current_path,
        });
    }

    fn executing(
        &self,
        total: usize,
        completed: usize,
        succeeded: usize,
        failed: usize,
        current_path: Option<String>,
    ) {
        self.report(NamespaceMutationProgress {
            phase: NamespaceMutationPhase::Executing,
            discovered: total,
            total: Some(total),
            completed,
            succeeded,
            failed,
            current_path,
        });
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerNamespaceReadKind {
    ListDir,
    Entry,
    EntryForWrite,
    TrashedEntry,
    ResolveLiveObject,
    ObjectRefs,
    TrashObjectRefs,
    Reachability,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerNamespaceReadRequest {
    pub fragment: String,
    pub kind: PeerNamespaceReadKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub include_trashed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerNamespaceReadResponse {
    ListDir {
        entries: Vec<NamespaceEntry>,
        continuation_token: Option<String>,
    },
    Entry(Option<NamespaceEntry>),
    ObjectId(Option<String>),
    ObjectRefs(NamespaceObjectRefPage),
    Reachability(BTreeSet<String>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceMutationKind {
    Mkdir,
    Rename,
    Trash,
    Recover,
    UploadCommit,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NamespaceApplyOutcome {
    Created(bool),
    Summary(NamespaceMutationSummary),
    CommittedObject(String),
}

impl NamespaceApplyOutcome {
    fn created(&self) -> Option<bool> {
        match self {
            Self::Created(created) => Some(*created),
            _ => None,
        }
    }

    fn summary(&self) -> Option<NamespaceMutationSummary> {
        match self {
            Self::Summary(summary) => Some(summary.clone()),
            _ => None,
        }
    }

    fn committed_object(&self) -> Option<String> {
        match self {
            Self::CommittedObject(object_id) => Some(object_id.clone()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerNamespaceApplyRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_id: Option<String>,
    pub kind: NamespaceMutationKind,
    pub fragment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMetadata>,
}

/// N2 3.4 namespace operations are already distributed by deterministic NCNX
/// placement. This facade preserves the NOA/native protocol boundary without
/// reintroducing a coordinator database or inode quorum.

#[cfg(test)]
#[path = "namespace/tests.rs"]
mod tests;
