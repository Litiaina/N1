//! NOA 2 operation contract shared by the native transport, HTTP adapter and
//! clients. Control is MessagePack on native connections; object bodies are
//! separate bounded streams, never JSON/base64 payloads.
use crate::PoolId;
use serde::{Deserialize, Serialize};

pub const NOA_VERSION: u16 = 2;
pub const WIRE_VERSION: &str = "n1/1";
pub const MAX_CONTROL_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_PREAUTH_BYTES: usize = 64 * 1024;
pub const MAX_BODY_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Permissions {
    pub read: bool,
    pub write: bool,
    pub delete: bool,
    pub repair: bool,
}
impl Permissions {
    pub fn all() -> Self {
        Self {
            read: true,
            write: true,
            delete: true,
            repair: true,
        }
    }
    pub fn intersect(self, allowed: Self) -> Self {
        Self {
            read: self.read && allowed.read,
            write: self.write && allowed.write,
            delete: self.delete && allowed.delete,
            repair: self.repair && allowed.repair,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub id: u64,
    pub command: Command,
    #[serde(default)]
    pub body_len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", content = "args", deny_unknown_fields)]
pub enum Command {
    #[serde(rename = "object.signed_read")]
    SignedRead {
        object_key: String,
        offset: u64,
        length: Option<u64>,
        ttl_seconds: u64,
    },
    #[serde(rename = "object.get_signed")]
    SignedGet { grant: ReadGrant },
    #[serde(rename = "session.refresh")]
    Refresh { refresh_token: String },
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "auth.fragment")]
    Login {
        fragment: String,
        secret: String,
        permissions: Option<Permissions>,
        ttl_seconds: Option<u64>,
    },
    #[serde(rename = "auth.bearer")]
    Bearer { token: String },
    #[serde(rename = "auth.logout")]
    Logout,
    #[serde(rename = "session.info")]
    SessionInfo,
    #[serde(rename = "upload.init")]
    UploadInit {
        pool: Option<PoolId>,
        total_bytes: u64,
        part_bytes: u64,
        durable: bool,
        ttl_seconds: u64,
    },
    #[serde(rename = "upload.part")]
    UploadPart { upload_key: String, part: u64 },
    #[serde(rename = "upload.put")]
    UploadPut { upload_key: String },
    #[serde(rename = "upload.batch")]
    UploadBatch { upload_keys: Vec<String> },
    #[serde(rename = "upload.status")]
    UploadStatus {
        upload_key: String,
        start: u64,
        limit: u64,
    },
    #[serde(rename = "upload.finalize")]
    UploadFinalize { upload_key: String },
    #[serde(rename = "upload.cancel")]
    UploadCancel { upload_key: String },
    #[serde(rename = "object.stat")]
    ObjectStat { object_key: String },
    #[serde(rename = "object.get")]
    ObjectGet {
        object_key: String,
        offset: u64,
        length: Option<u64>,
    },
    #[serde(rename = "object.health")]
    ObjectHealth { object_key: String, deep: bool },
    #[serde(rename = "object.repair")]
    ObjectRepair { object_key: String },
    #[serde(rename = "object.delete")]
    ObjectDelete { object_key: String },
    #[serde(rename = "object.bulk_delete")]
    BulkDelete { object_keys: Vec<String> },
    #[serde(rename = "maintenance.health")]
    Health,
    #[serde(rename = "maintenance.pools")]
    Pools,
    #[serde(rename = "backup.receive")]
    BackupReceive { repository: String },
    #[serde(rename = "backup.list")]
    BackupList {
        repository: String,
        after: String,
        limit: usize,
    },
    #[serde(rename = "backup.capture")]
    BackupCapture {
        repository: String,
        boundary_seconds: u64,
    },
    #[serde(rename = "backup.verify")]
    BackupVerify {
        repository: String,
        snapshot: String,
    },
    #[serde(rename = "backup.replicate")]
    BackupReplicate {
        repository: String,
        destination: String,
        snapshot: String,
    },
    #[serde(rename = "backup.prune")]
    BackupPrune {
        repository: String,
        keep_last: usize,
        protect: String,
    },
    #[serde(rename = "backup.gc")]
    BackupGc {
        repository: String,
        discard_incomplete: bool,
    },
    #[serde(rename = "backup.job")]
    BackupJob,
    #[serde(rename = "backup.stop")]
    BackupStop { job_id: String },
    #[serde(rename = "maintenance.gc")]
    GarbageCollect,
    #[serde(rename = "maintenance.scrub")]
    Scrub { options: ScrubOptions },
    #[serde(rename = "maintenance.job")]
    MaintenanceJob,
    #[serde(rename = "maintenance.stop")]
    MaintenanceStop { job_id: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadGrant {
    pub fragment: PoolId,
    pub object_key: String,
    pub offset: u64,
    pub length: Option<u64>,
    pub expires_at: i64,
    pub signature: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    pub status: u16,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub body_len: u64,
}
impl Response {
    pub fn ok(id: u64, result: serde_json::Value) -> Self {
        Self {
            id,
            status: 200,
            result: Some(result),
            error: None,
            body_len: 0,
        }
    }
    pub fn error(id: u64, status: u16, message: impl Into<String>) -> Self {
        Self {
            id,
            status,
            result: None,
            error: Some(message.into()),
            body_len: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScrubOptions {
    pub repair: bool,
    pub bytes_per_second: u64,
    pub window_seconds: u64,
}
impl ScrubOptions {
    pub fn validate(&self) -> crate::Result<()> {
        if self.bytes_per_second == 0
            || self.bytes_per_second > 4 * 1024 * 1024 * 1024
            || self.window_seconds == 0
            || self.window_seconds > 7 * 86400
        {
            return Err(crate::Error(
                "scrub requires a positive rate (at most 4 GiB/s) and a 1-second through 7-day window",
            ));
        }
        Ok(())
    }
}
