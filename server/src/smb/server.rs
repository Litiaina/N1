use super::config::SmbConfig;
use crate::enums::app_error_enum::AppError;
use crate::fuse::backend::{DirEntry, InProcessBackend};
use crate::fuse::config::FsdConfig;
use crate::fuse::gateway::FsdGateway;
use crate::fuse::model::{NodeKind, NodeMetadata, WriteHandle};
use crate::object_storage::fragment_metadata::read_fragment_metadata;
use crate::validation::fragment_validation::does_fragment_exist;
use crate::websocket::manager::global_ws_manager;
use crate::websocket::ws_structs::{SmbTransferProgressPayload, StorageEventPayload};
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use md4::{Digest as Md4Digest, Md4};
use md5::Md5;
use rand::RngCore;
use sha2::Sha256;
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use ulid::Ulid;

const NETBIOS_HEADER_LEN: usize = 4;
const SMB2_HEADER_LEN: usize = 64;

const SMB2_MAGIC: [u8; 4] = [0xFE, b'S', b'M', b'B'];
const SMB1_MAGIC: [u8; 4] = [0xFF, b'S', b'M', b'B'];
const SMB2_SERVER_TO_REDIR_FLAG: u32 = 0x0000_0001;
const SMB2_FLAGS_SIGNED: u32 = 0x0000_0008;

const SMB2_COMMAND_NEGOTIATE: u16 = 0x0000;
const SMB2_COMMAND_SESSION_SETUP: u16 = 0x0001;
const SMB2_COMMAND_LOGOFF: u16 = 0x0002;
const SMB2_COMMAND_TREE_CONNECT: u16 = 0x0003;
const SMB2_COMMAND_TREE_DISCONNECT: u16 = 0x0004;
const SMB2_COMMAND_CREATE: u16 = 0x0005;
const SMB2_COMMAND_CLOSE: u16 = 0x0006;
const SMB2_COMMAND_FLUSH: u16 = 0x0007;
const SMB2_COMMAND_READ: u16 = 0x0008;
const SMB2_COMMAND_WRITE: u16 = 0x0009;
const SMB2_COMMAND_LOCK: u16 = 0x000A;
const SMB2_COMMAND_IOCTL: u16 = 0x000B;
const SMB2_COMMAND_ECHO: u16 = 0x000D;
const SMB2_COMMAND_QUERY_DIRECTORY: u16 = 0x000E;
const SMB2_COMMAND_QUERY_INFO: u16 = 0x0010;
const SMB2_COMMAND_SET_INFO: u16 = 0x0011;
const SMB1_COMMAND_NEGOTIATE: u8 = 0x72;

const SMB2_STATUS_SUCCESS: u32 = 0x0000_0000;
const SMB2_STATUS_MORE_PROCESSING_REQUIRED: u32 = 0xC000_0016;
const SMB2_STATUS_NOT_SUPPORTED: u32 = 0xC000_00BB;
const SMB2_STATUS_INVALID_PARAMETER: u32 = 0xC000_000D;
const SMB2_STATUS_INVALID_HANDLE: u32 = 0xC000_0008;
const SMB2_STATUS_ACCESS_DENIED: u32 = 0xC000_0022;
const SMB2_STATUS_LOGON_FAILURE: u32 = 0xC000_006D;
const SMB2_STATUS_SHARING_VIOLATION: u32 = 0xC000_0043;
const SMB2_STATUS_LOCK_NOT_GRANTED: u32 = 0xC000_0055;
const SMB2_STATUS_OBJECT_NAME_NOT_FOUND: u32 = 0xC000_0034;
const SMB2_STATUS_OBJECT_NAME_COLLISION: u32 = 0xC000_0035;
const SMB2_STATUS_BAD_NETWORK_NAME: u32 = 0xC000_00CC;
const SMB2_STATUS_NO_MORE_FILES: u32 = 0x8000_0006;
const SMB2_STATUS_DIRECTORY_NOT_EMPTY: u32 = 0xC000_0101;
const SMB2_STATUS_NOT_A_DIRECTORY: u32 = 0xC000_0103;
const SMB2_STATUS_FILE_IS_A_DIRECTORY: u32 = 0xC000_00BA;
const SMB2_STATUS_USER_SESSION_DELETED: u32 = 0xC000_0203;
const SMB2_STATUS_UNSUCCESSFUL: u32 = 0xC000_0001;
const SMB2_STATUS_IO_TIMEOUT: u32 = 0xC000_00B5;
const SMB2_STATUS_DISK_FULL: u32 = 0xC000_007F;

const FILETIME_UNIX_EPOCH_DELTA_SECS: u64 = 11_644_473_600;

const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x0000_0010;
const FILE_ATTRIBUTE_NORMAL: u32 = 0x0000_0080;
const FILE_READ_DATA: u32 = 0x0000_0001;
const FILE_WRITE_DATA: u32 = 0x0000_0002;
const FILE_APPEND_DATA: u32 = 0x0000_0004;
const FILE_EXECUTE: u32 = 0x0000_0020;
const FILE_DELETE: u32 = 0x0001_0000;
const FILE_GENERIC_READ: u32 = 0x8000_0000;
const FILE_GENERIC_WRITE: u32 = 0x4000_0000;
const FILE_GENERIC_ALL: u32 = 0x1000_0000;
const FILE_SHARE_READ: u32 = 0x0000_0001;
const FILE_SHARE_WRITE: u32 = 0x0000_0002;
const FILE_SHARE_DELETE: u32 = 0x0000_0004;
const FILE_DIRECTORY_FILE: u32 = 0x0000_0001;
const FILE_NON_DIRECTORY_FILE: u32 = 0x0000_0040;
const FILE_DELETE_ON_CLOSE: u32 = 0x0000_1000;
const SMB2_LOCKFLAG_UNLOCK: u32 = 0x0000_0004;

const FILE_CREATE_DISPOSITION_SUPERSEDE: u32 = 0;
const FILE_CREATE_DISPOSITION_OPEN: u32 = 1;
const FILE_CREATE_DISPOSITION_CREATE: u32 = 2;
const FILE_CREATE_DISPOSITION_OPEN_IF: u32 = 3;
const FILE_CREATE_DISPOSITION_OVERWRITE: u32 = 4;
const FILE_CREATE_DISPOSITION_OVERWRITE_IF: u32 = 5;

const FILE_CREATE_ACTION_SUPERSEDED: u32 = 0;
const FILE_CREATE_ACTION_OPENED: u32 = 1;
const FILE_CREATE_ACTION_CREATED: u32 = 2;
const FILE_CREATE_ACTION_OVERWRITTEN: u32 = 3;

const FILE_INFORMATION_CLASS_DIRECTORY_INFORMATION: u8 = 0x01;
const FILE_INFORMATION_CLASS_BOTH_DIRECTORY_INFORMATION: u8 = 0x03;
const FILE_INFORMATION_CLASS_ID_BOTH_DIRECTORY_INFORMATION: u8 = 0x25;
const SMB2_INFO_TYPE_FILE: u8 = 0x01;
const SMB2_INFO_TYPE_FILESYSTEM: u8 = 0x02;
const FILE_INFO_CLASS_BASIC_INFORMATION: u8 = 0x04;
const FILE_INFO_CLASS_STANDARD_INFORMATION: u8 = 0x05;
const FILE_INFO_CLASS_RENAME_INFORMATION: u8 = 0x0A;
const FILE_INFO_CLASS_POSITION_INFORMATION: u8 = 0x0E;
const FILE_INFO_CLASS_DISPOSITION_INFORMATION: u8 = 0x0D;
const FILE_INFO_CLASS_MODE_INFORMATION: u8 = 0x10;
const FILE_INFO_CLASS_DISPOSITION_INFORMATION_EX: u8 = 0x40;
const FILE_INFO_CLASS_ALL_INFORMATION: u8 = 0x12;
const FILE_INFO_CLASS_ALLOCATION_INFORMATION: u8 = 0x13;
const FILE_INFO_CLASS_END_OF_FILE_INFORMATION: u8 = 0x14;
const FILE_INFO_CLASS_NETWORK_OPEN_INFORMATION: u8 = 0x22;
const FS_INFO_CLASS_VOLUME_INFORMATION: u8 = 0x01;
const FS_INFO_CLASS_SIZE_INFORMATION: u8 = 0x03;
const FS_INFO_CLASS_DEVICE_INFORMATION: u8 = 0x04;
const FSCTL_VALIDATE_NEGOTIATE_INFO: u32 = 0x0014_0204;
const FSCTL_QUERY_NETWORK_INTERFACE_INFO: u32 = 0x0014_01FC;
const FSCTL_DFS_GET_REFERRALS: u32 = 0x0006_0194;
const FS_INFO_CLASS_ATTRIBUTE_INFORMATION: u8 = 0x05;
const FS_INFO_CLASS_FULL_SIZE_INFORMATION: u8 = 0x07;
const SMB2_QUERY_DIRECTORY_RESTART_SCANS: u8 = 0x01;
const SMB2_QUERY_DIRECTORY_REOPEN: u8 = 0x10;
const SMB2_NEGOTIATE_DIALECT_0202: u16 = 0x0202;
const SMB2_NEGOTIATE_DIALECT_0210: u16 = 0x0210;
const SMB2_NEGOTIATE_DIALECT_WILDCARD: u16 = 0x02FF;
const SMB2_NEGOTIATE_DIALECT_0300: u16 = 0x0300;
const SMB2_NEGOTIATE_DIALECT_0302: u16 = 0x0302;
const SMB2_NEGOTIATE_DIALECT_0311: u16 = 0x0311;
const SMB2_NEGOTIATE_SIGNING_ENABLED: u16 = 0x0001;
const SMB2_NEGOTIATE_SIGNING_REQUIRED: u16 = 0x0002;
const SMB2_SESSION_FLAG_IS_GUEST: u16 = 0x0001;
const SMB2_NEGOTIATE_RESPONSE_FIXED_LEN: usize = 64;
const SMB2_SERVER_GUID: [u8; 16] = [
    0x6A, 0xA2, 0x15, 0x39, 0xB8, 0x04, 0x4A, 0x4F, 0x83, 0xD4, 0x4F, 0x31, 0x8E, 0x10, 0xC2,
    0x7B,
];
const SMB2_GLOBAL_CAP_DFS: u32 = 0x0000_0001;
const SMB2_GLOBAL_CAP_LARGE_MTU: u32 = 0x0000_0004;
const SMB2_NEGOTIATE_SPNEGO_NTLM_BLOB: [u8; 30] = [
    0x60, 0x1C, 0x06, 0x06, 0x2B, 0x06, 0x01, 0x05, 0x05, 0x02, 0xA0, 0x12, 0x30, 0x10, 0xA0,
    0x0E, 0x30, 0x0C, 0x06, 0x0A, 0x2B, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0A,
];
const SMB2_NEGOTIATE_CONTEXT_PREAUTH_INTEGRITY_CAPABILITIES: u16 = 0x0001;
const SMB2_NEGOTIATE_CONTEXT_ENCRYPTION_CAPABILITIES: u16 = 0x0002;
const SMB2_NEGOTIATE_CONTEXT_SIGNING_CAPABILITIES: u16 = 0x0008;
const SMB2_PREAUTH_HASH_ALGO_SHA512: u16 = 0x0001;
const SMB2_ENCRYPTION_CIPHER_AES128_GCM: u16 = 0x0002;
const SMB2_ENCRYPTION_CIPHER_AES128_CCM: u16 = 0x0001;
const SMB2_ENCRYPTION_CIPHER_AES256_CCM: u16 = 0x0003;
const SMB2_ENCRYPTION_CIPHER_AES256_GCM: u16 = 0x0004;
const SMB2_SIGNING_ALGORITHM_AES_GMAC: u16 = 0x0002;
const SMB2_SIGNING_ALGORITHM_AES_CMAC: u16 = 0x0001;
const SMB2_SIGNING_ALGORITHM_HMAC_SHA256: u16 = 0x0000;
const SMB2_PREAUTH_SALT: [u8; 32] = [
    0x11, 0x22, 0x31, 0x44, 0x52, 0x61, 0x73, 0x84, 0x95, 0xA6, 0xB1, 0xC2, 0xD3, 0xE4, 0xF5,
    0x06, 0x17, 0x28, 0x39, 0x4A, 0x5B, 0x6C, 0x7D, 0x8E, 0x9F, 0x10, 0x21, 0x32, 0x43, 0x54,
    0x65, 0x76,
];
const NTLMSSP_SIGNATURE: [u8; 8] = [b'N', b'T', b'L', b'M', b'S', b'S', b'P', 0x00];
const NTLM_NEGOTIATE_UNICODE: u32 = 0x0000_0001;
const NTLM_NEGOTIATE_SIGN: u32 = 0x0000_0010;
const NTLM_NEGOTIATE_NTLM: u32 = 0x0000_0200;
const NTLM_NEGOTIATE_ALWAYS_SIGN: u32 = 0x0000_8000;
const NTLM_NEGOTIATE_EXTENDED_SESSIONSECURITY: u32 = 0x0008_0000;
const NTLM_NEGOTIATE_TARGET_INFO: u32 = 0x0080_0000;
const NTLM_NEGOTIATE_VERSION: u32 = 0x0200_0000;
const NTLM_NEGOTIATE_128: u32 = 0x2000_0000;
const NTLM_NEGOTIATE_56: u32 = 0x8000_0000;
const NTLM_CHALLENGE_FLAGS: u32 = NTLM_NEGOTIATE_UNICODE
    | NTLM_NEGOTIATE_SIGN
    | NTLM_NEGOTIATE_NTLM
    | NTLM_NEGOTIATE_ALWAYS_SIGN
    | NTLM_NEGOTIATE_EXTENDED_SESSIONSECURITY
    | NTLM_NEGOTIATE_TARGET_INFO
    | NTLM_NEGOTIATE_VERSION
    | NTLM_NEGOTIATE_128
    | NTLM_NEGOTIATE_56;
const SMB_TRANSFER_EMIT_INTERVAL_MS: u64 = 500;
const SMB_TRANSFER_RETENTION_MS: u64 = 30 * 60 * 1000;
const SMB_TRANSFER_MAX_TRACKED: usize = 10_000;

type HmacMd5 = Hmac<Md5>;
type HmacSha256 = Hmac<Sha256>;
static SMB_TRANSFER_PROGRESS: OnceLock<DashMap<String, TransferProgressEntry>> = OnceLock::new();

#[derive(Clone, Debug)]
struct TreeContext {
    session_id: u64,
    fragment: String,
}

#[derive(Clone, Debug)]
struct OpenHandleContext {
    session_id: u64,
    tree_id: u32,
    fragment: String,
    object_key: String,
    is_dir: bool,
    writable: bool,
    delete_on_close: bool,
    dir_query_cursor: usize,
    dir_query_pattern: String,
    dir_query_info_class: u8,
    expected_size_bytes: Option<u64>,
    max_written_end: u64,
    transfer_id: Option<String>,
    materialize_on_close: bool,
    pending_write: Option<WriteHandle>,
}

#[derive(Clone, Debug)]
struct TransferProgressEntry {
    payload: SmbTransferProgressPayload,
    last_emit_at_ms: u64,
}

#[derive(Clone, Debug)]
struct OpenShareEntry {
    connection_id: u64,
    file_id: [u8; 16],
    share_access: u32,
    requested_read: bool,
    requested_write: bool,
    requested_delete: bool,
}

#[derive(Debug, Default)]
struct GlobalSmbState {
    opens: HashMap<String, Vec<OpenShareEntry>>,
    locks: HashMap<String, (u64, [u8; 16])>,
}

type SharedSmbState = Arc<Mutex<GlobalSmbState>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionAuthState {
    Negotiating,
    ChallengeSent,
    Established,
}

#[derive(Clone, Debug)]
struct ServerAuthConfig {
    username: String,
    password: String,
    domain: String,
}

#[derive(Clone, Debug)]
struct SessionContext {
    auth_state: SessionAuthState,
    server_challenge: Option<[u8; 8]>,
    signing_required: bool,
    signing_key: Option<[u8; 16]>,
    fragment_scope: Option<String>,
}

#[derive(Debug)]
struct ConnectionState {
    connection_id: u64,
    next_session_id: u64,
    next_tree_id: u32,
    next_handle_id: u64,
    auth_config: Option<ServerAuthConfig>,
    require_signing: bool,
    sessions: HashMap<u64, SessionContext>,
    trees: HashMap<u32, TreeContext>,
    handles: HashMap<[u8; 16], OpenHandleContext>,
}

impl ConnectionState {
    fn new(auth_config: Option<ServerAuthConfig>, require_signing: bool) -> Self {
        let mut connection_id_bytes = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut connection_id_bytes);
        Self {
            connection_id: u64::from_le_bytes(connection_id_bytes),
            next_session_id: 1,
            next_tree_id: 1,
            next_handle_id: 1,
            auth_config,
            require_signing,
            sessions: HashMap::new(),
            trees: HashMap::new(),
            handles: HashMap::new(),
        }
    }

    fn create_session(&mut self) -> u64 {
        let id = self.next_session_id;
        self.next_session_id = self.next_session_id.saturating_add(1);
        self.sessions.insert(
            id,
            SessionContext {
                auth_state: SessionAuthState::Negotiating,
                server_challenge: None,
                signing_required: self.require_signing && self.auth_config.is_some(),
                signing_key: None,
                fragment_scope: None,
            },
        );
        id
    }

    fn has_session(&self, session_id: u64) -> bool {
        self.sessions.contains_key(&session_id)
    }

    fn remove_session(&mut self, session_id: u64) {
        self.sessions.remove(&session_id);
        self.trees.retain(|_, tree| tree.session_id != session_id);
        self.handles
            .retain(|_, handle| handle.session_id != session_id);
    }

    fn create_tree(&mut self, session_id: u64, fragment: String) -> u32 {
        let id = self.next_tree_id;
        self.next_tree_id = self.next_tree_id.saturating_add(1);
        self.trees.insert(
            id,
            TreeContext {
                session_id,
                fragment,
            },
        );
        id
    }

    fn get_tree(&self, tree_id: u32) -> Option<&TreeContext> {
        self.trees.get(&tree_id)
    }

    fn remove_tree(&mut self, tree_id: u32) {
        self.trees.remove(&tree_id);
        self.handles.retain(|_, handle| handle.tree_id != tree_id);
    }

    fn new_file_id(&mut self) -> [u8; 16] {
        let base = self.next_handle_id;
        self.next_handle_id = self.next_handle_id.saturating_add(1);
        let mut out = [0u8; 16];
        out[0..8].copy_from_slice(&base.to_le_bytes());
        out[8..16].copy_from_slice(&base.rotate_left(17).to_le_bytes());
        out
    }
}

#[derive(Debug, Clone, Copy)]
struct Smb2HeaderView {
    credit_charge: u16,
    command: u16,
    credit_request: u16,
    flags: u32,
    message_id: u64,
    tree_id: u32,
    session_id: u64,
    signature: [u8; 16],
}

#[derive(Debug, Clone)]
struct NegotiateRequestMeta {
    security_mode: u16,
    capabilities: u32,
    context_offset: u32,
    context_count: u16,
    client_guid: [u8; 16],
    context_types: Vec<u16>,
    preauth_algorithms: Vec<u16>,
    encryption_ciphers: Vec<u16>,
    signing_algorithms: Vec<u16>,
}

#[derive(Debug, Clone, Copy)]
struct NegotiateSelection {
    signing_mode: u16,
    preauth_hash: u16,
    encryption_cipher: u16,
    signing_algorithm: u16,
}

impl Smb2HeaderView {
    fn parse(payload: &[u8]) -> Option<Self> {
        if payload.len() < SMB2_HEADER_LEN {
            return None;
        }
        if payload[0..4] != SMB2_MAGIC {
            return None;
        }

        Some(Self {
            credit_charge: read_u16(payload, 6)?,
            command: read_u16(payload, 12)?,
            credit_request: read_u16(payload, 14)?,
            flags: read_u32(payload, 16)?,
            message_id: read_u64(payload, 24)?,
            tree_id: read_u32(payload, 36)?,
            session_id: read_u64(payload, 40)?,
            signature: payload.get(48..64)?.try_into().ok()?,
        })
    }
}

type SharedGateway = Arc<FsdGateway<InProcessBackend>>;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn transfer_progress_store() -> &'static DashMap<String, TransferProgressEntry> {
    SMB_TRANSFER_PROGRESS.get_or_init(DashMap::new)
}

fn emit_transfer_progress(entry: &TransferProgressEntry) {
    if let Some(ws) = global_ws_manager() {
        ws.publish(
            entry.payload.fragment.as_str(),
            StorageEventPayload::SmbTransferProgress(entry.payload.clone()),
        );
    }
}

fn maybe_emit_transfer_progress(entry: &mut TransferProgressEntry, force: bool) {
    let now = now_ms();
    if force || now.saturating_sub(entry.last_emit_at_ms) >= SMB_TRANSFER_EMIT_INTERVAL_MS {
        emit_transfer_progress(entry);
        entry.last_emit_at_ms = now;
    }
}

fn prune_transfer_progress_store() {
    let now = now_ms();
    transfer_progress_store().retain(|_, entry| {
        if entry.payload.state == "running" {
            true
        } else {
            now.saturating_sub(entry.payload.updated_at_ms) <= SMB_TRANSFER_RETENTION_MS
        }
    });
    let len = transfer_progress_store().len();
    if len > SMB_TRANSFER_MAX_TRACKED {
        let mut completed = transfer_progress_store()
            .iter()
            .filter(|v| v.payload.state != "running")
            .map(|v| (v.key().clone(), v.payload.updated_at_ms))
            .collect::<Vec<_>>();
        completed.sort_by_key(|(_, ts)| *ts);
        let mut drop_count = len.saturating_sub(SMB_TRANSFER_MAX_TRACKED);
        for (id, _) in completed {
            if drop_count == 0 {
                break;
            }
            if transfer_progress_store().remove(id.as_str()).is_some() {
                drop_count = drop_count.saturating_sub(1);
            }
        }
    }
}

fn transfer_started(
    transfer_id: &str,
    fragment: &str,
    object_key: &str,
    expected_bytes: Option<u64>,
) {
    let now = now_ms();
    let mut entry = TransferProgressEntry {
        payload: SmbTransferProgressPayload {
            transfer_id: transfer_id.to_string(),
            fragment: fragment.to_string(),
            object_key: object_key.to_string(),
            phase: "ingress".to_string(),
            state: "running".to_string(),
            ingress_bytes: 0,
            expected_bytes,
            started_at_ms: now,
            updated_at_ms: now,
            completed_at_ms: None,
            error: None,
        },
        last_emit_at_ms: 0,
    };
    maybe_emit_transfer_progress(&mut entry, true);
    transfer_progress_store().insert(transfer_id.to_string(), entry);
    prune_transfer_progress_store();
}

fn transfer_ingress_progress(transfer_id: &str, ingress_bytes: u64, expected_bytes: Option<u64>) {
    if let Some(mut entry) = transfer_progress_store().get_mut(transfer_id) {
        entry.payload.phase = "ingress".to_string();
        entry.payload.state = "running".to_string();
        entry.payload.ingress_bytes = ingress_bytes;
        if expected_bytes.is_some() {
            entry.payload.expected_bytes = expected_bytes;
        }
        entry.payload.updated_at_ms = now_ms();
        maybe_emit_transfer_progress(&mut entry, false);
    }
}

fn transfer_commit_started(transfer_id: &str) {
    if let Some(mut entry) = transfer_progress_store().get_mut(transfer_id) {
        entry.payload.phase = "commit".to_string();
        entry.payload.state = "running".to_string();
        entry.payload.updated_at_ms = now_ms();
        maybe_emit_transfer_progress(&mut entry, true);
    }
}

fn transfer_completed(transfer_id: &str) {
    if let Some(mut entry) = transfer_progress_store().get_mut(transfer_id) {
        let now = now_ms();
        entry.payload.phase = "commit".to_string();
        entry.payload.state = "completed".to_string();
        entry.payload.updated_at_ms = now;
        entry.payload.completed_at_ms = Some(now);
        entry.payload.error = None;
        maybe_emit_transfer_progress(&mut entry, true);
    }
}

fn transfer_failed(transfer_id: &str, err: &str) {
    if let Some(mut entry) = transfer_progress_store().get_mut(transfer_id) {
        let now = now_ms();
        entry.payload.state = "failed".to_string();
        entry.payload.updated_at_ms = now;
        entry.payload.completed_at_ms = Some(now);
        entry.payload.error = Some(err.to_string());
        maybe_emit_transfer_progress(&mut entry, true);
    }
}

pub fn list_transfer_progress_for_fragment(
    fragment: &str,
    include_completed: bool,
    limit: usize,
) -> Vec<SmbTransferProgressPayload> {
    prune_transfer_progress_store();
    let mut rows = transfer_progress_store()
        .iter()
        .filter(|v| v.payload.fragment == fragment)
        .filter(|v| include_completed || v.payload.state == "running")
        .map(|v| v.payload.clone())
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b.updated_at_ms.cmp(&a.updated_at_ms));
    rows.truncate(limit.max(1).min(1000));
    rows
}

fn pending_commit_expected_size(fragment: &str, object_key: &str) -> Option<u64> {
    transfer_progress_store().iter().find_map(|entry| {
        let payload = &entry.payload;
        if payload.state == "running"
            && payload.phase == "commit"
            && payload.fragment == fragment
            && payload.object_key == object_key
        {
            payload.expected_bytes.or(Some(payload.ingress_bytes))
        } else {
            None
        }
    })
}

fn pending_commit_entries_for_dir(fragment: &str, object_key: &str) -> Vec<DirEntry> {
    let parent = object_key.trim_matches('/').to_string();
    let prefix = if parent.is_empty() {
        String::new()
    } else {
        format!("{}/", parent)
    };
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for entry in transfer_progress_store().iter() {
        let payload = &entry.payload;
        if payload.state != "running" || payload.phase != "commit" || payload.fragment != fragment {
            continue;
        }
        let key = payload.object_key.trim_matches('/');
        if key.is_empty() {
            continue;
        }
        if !prefix.is_empty() && !key.starts_with(prefix.as_str()) {
            continue;
        }
        let rest = if prefix.is_empty() {
            key
        } else {
            &key[prefix.len()..]
        };
        if rest.is_empty() {
            continue;
        }
        let mut parts = rest.split('/');
        let Some(name) = parts.next() else {
            continue;
        };
        if name.is_empty() || name == "." || name == ".." {
            continue;
        }
        let lowered = name.to_ascii_lowercase();
        if !seen.insert(lowered) {
            continue;
        }
        let is_dir = parts.next().is_some();
        let child_key = if parent.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent, name)
        };
        let size_bytes = if is_dir {
            0
        } else {
            payload.expected_bytes.unwrap_or(payload.ingress_bytes)
        };
        out.push(DirEntry {
            name: name.to_string(),
            path: crate::fuse::model::FsdPath {
                fragment: fragment.to_string(),
                object_key: child_key,
            },
            metadata: NodeMetadata {
                kind: if is_dir {
                    NodeKind::Directory
                } else {
                    NodeKind::File
                },
                size_bytes,
                modified_at_unix: now_unix_secs(),
            },
        });
    }
    out
}

pub async fn spawn_listener(config: SmbConfig) -> io::Result<(SocketAddr, JoinHandle<()>)> {
    let bind_addr = config
        .socket_addr()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    let listener = TcpListener::bind(bind_addr).await?;
    let local_addr = listener.local_addr()?;

    let fsd_config = FsdConfig::default();
    let backend = Arc::new(InProcessBackend::new(
        fsd_config.staging_root,
        fsd_config.upload_part_size_bytes,
    ));
    let gateway = Arc::new(FsdGateway::new(backend));

    let max_sessions = config.max_sessions;
    let max_frame_bytes = config.max_frame_bytes;
    let async_close_commit = config.async_close_commit;
    let async_close_threshold_bytes = config.async_close_threshold_bytes;
    let auth_config = parse_server_auth_config(&config);
    let require_signing = config.require_signing || auth_config.is_some();
    let shared_smb_state = Arc::new(Mutex::new(GlobalSmbState::default()));
    let semaphore = Arc::new(Semaphore::new(max_sessions));

    let join = tokio::spawn(async move {
        info!(
            "smb loop started on {} (max_sessions={}, max_frame_bytes={}, auth_required={}, require_signing={}, close_ack_mode={}, close_async_threshold_bytes={})",
            local_addr,
            max_sessions,
            max_frame_bytes,
            auth_config.is_some(),
            require_signing,
            if async_close_commit { "immediate" } else { "after_commit" },
            async_close_threshold_bytes
        );
        run_accept_loop(
            listener,
            semaphore,
            max_frame_bytes,
            gateway,
            shared_smb_state,
            auth_config,
            require_signing,
            async_close_commit,
            async_close_threshold_bytes,
        )
        .await;
    });

    Ok((local_addr, join))
}

async fn run_accept_loop(
    listener: TcpListener,
    semaphore: Arc<Semaphore>,
    max_frame_bytes: usize,
    gateway: SharedGateway,
    shared_smb_state: SharedSmbState,
    auth_config: Option<ServerAuthConfig>,
    require_signing: bool,
    async_close_commit: bool,
    async_close_threshold_bytes: u64,
) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!("smb accept failed: {}", e);
                continue;
            }
        };
        debug!("smb accepted connection from {}", peer);

        let permit = match semaphore.clone().acquire_owned().await {
            Ok(v) => v,
            Err(_) => {
                warn!("smb semaphore closed");
                break;
            }
        };

        let gateway = gateway.clone();
        let shared_smb_state = shared_smb_state.clone();
        let auth_config = auth_config.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = handle_connection(
                stream,
                peer,
                max_frame_bytes,
                gateway,
                shared_smb_state,
                auth_config,
                require_signing,
                async_close_commit,
                async_close_threshold_bytes,
            )
            .await
            {
                debug!("smb connection {} ended with error: {}", peer, e);
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    max_frame_bytes: usize,
    gateway: SharedGateway,
    shared_smb_state: SharedSmbState,
    auth_config: Option<ServerAuthConfig>,
    require_signing: bool,
    async_close_commit: bool,
    async_close_threshold_bytes: u64,
) -> io::Result<()> {
    let mut state = ConnectionState::new(auth_config, require_signing);

    loop {
        let mut netbios_header = [0u8; NETBIOS_HEADER_LEN];
        match stream.read_exact(&mut netbios_header).await {
            Ok(_) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::BrokenPipe
                ) =>
            {
                break;
            }
            Err(e) => {
                cleanup_connection_state(&mut state, &gateway, &shared_smb_state).await;
                return Err(e);
            }
        }

        if netbios_header[0] != 0x00 {
            debug!("smb {} unsupported netbios message type", peer);
            break;
        }

        let length = ((netbios_header[1] as usize) << 16)
            | ((netbios_header[2] as usize) << 8)
            | (netbios_header[3] as usize);
        if length == 0 || length > max_frame_bytes {
            debug!(
                "smb {} invalid frame length {} (max={})",
                peer, length, max_frame_bytes
            );
            break;
        }

        let mut smb_payload = vec![0u8; length];
        stream.read_exact(&mut smb_payload).await?;
        if smb_payload.len() >= 4 {
            debug!(
                "smb {} frame len={} magic={:02X?}",
                peer,
                smb_payload.len(),
                &smb_payload[0..4]
            );
        } else {
            debug!("smb {} short frame len={}", peer, smb_payload.len());
        }

        let Some(response) =
            process_smb2_request(
                &smb_payload,
                &mut state,
                &gateway,
                &shared_smb_state,
                async_close_commit,
                async_close_threshold_bytes,
            )
            .await
        else {
            if let Some(response) = process_smb1_negotiate_compat(&smb_payload) {
                debug!("smb {} smb1 negotiate compat response", peer);
                stream.write_all(&response).await?;
                stream.flush().await?;
                continue;
            }
            debug!("smb {} non-smb2 frame dropped", peer);
            break;
        };

        stream.write_all(&response).await?;
        stream.flush().await?;
    }

    cleanup_connection_state(&mut state, &gateway, &shared_smb_state).await;
    Ok(())
}

async fn process_smb2_request(
    request: &[u8],
    state: &mut ConnectionState,
    gateway: &SharedGateway,
    shared_smb_state: &SharedSmbState,
    async_close_commit: bool,
    async_close_threshold_bytes: u64,
) -> Option<Vec<u8>> {
    let header = Smb2HeaderView::parse(request)?;

    if should_enforce_signed_request(&header, state) {
        let Some(session) = state.sessions.get(&header.session_id) else {
            return Some(build_framed_smb2_response(
                &header,
                SMB2_STATUS_USER_SESSION_DELETED,
                header.tree_id,
                header.session_id,
                &build_not_supported_error_body(),
                None,
            ));
        };
        let Some(signing_key) = session.signing_key else {
            return Some(build_framed_smb2_response(
                &header,
                SMB2_STATUS_ACCESS_DENIED,
                header.tree_id,
                header.session_id,
                &build_not_supported_error_body(),
                None,
            ));
        };
        if !verify_smb2_request_signature(request, &header, &signing_key) {
            return Some(build_framed_smb2_response(
                &header,
                SMB2_STATUS_ACCESS_DENIED,
                header.tree_id,
                header.session_id,
                &build_not_supported_error_body(),
                None,
            ));
        }
    }

    let (status, tree_id, session_id, body) = match header.command {
        SMB2_COMMAND_NEGOTIATE => {
            debug!(
                "smb negotiate raw_len={} ss@64={:?} ss@66={:?}",
                request.len(),
                read_u16(request, SMB2_HEADER_LEN),
                read_u16(request, SMB2_HEADER_LEN + 2)
            );
            let negotiate_meta = parse_negotiate_request_meta(request);
            if let Some(meta) = negotiate_meta.as_ref() {
                debug!(
                    "smb negotiate meta security_mode=0x{:04X} capabilities=0x{:08X} context_offset={} context_count={} client_guid={:02X?} context_types={:X?} preauth_algorithms={:X?} encryption_ciphers={:X?} signing_algorithms={:X?}",
                    meta.security_mode,
                    meta.capabilities,
                    meta.context_offset,
                    meta.context_count,
                    meta.client_guid,
                    meta.context_types,
                    meta.preauth_algorithms,
                    meta.encryption_ciphers,
                    meta.signing_algorithms
                );
            }

            match parse_negotiate_request_dialects(request) {
                Some(dialects) if dialects.is_empty() => (
                    SMB2_STATUS_INVALID_PARAMETER,
                    0,
                    0,
                    build_not_supported_error_body(),
                ),
                Some(dialects) => {
                    let has_0202 = dialects
                        .iter()
                        .any(|d| *d == SMB2_NEGOTIATE_DIALECT_0202);
                    let has_0210 = dialects
                        .iter()
                        .any(|d| *d == SMB2_NEGOTIATE_DIALECT_0210);
                    let has_0302 = dialects
                        .iter()
                        .any(|d| *d == SMB2_NEGOTIATE_DIALECT_0302);
                    let has_0311 = dialects
                        .iter()
                        .any(|d| *d == SMB2_NEGOTIATE_DIALECT_0311);
                    let selected_dialect = select_supported_dialect(&dialects);
                    if let Some(dialect) = selected_dialect {
                        debug!(
                            "smb negotiate client_dialects={:?} has_0202={} has_0210={} has_0302={} has_0311={} selected=0x{:04X}",
                            dialects, has_0202, has_0210, has_0302, has_0311, dialect
                        );
                        let selection = negotiate_selection(negotiate_meta.as_ref(), state.require_signing);
                        let body = build_negotiate_response_body(dialect, selection);
                        (SMB2_STATUS_SUCCESS, 0, 0, body)
                    } else {
                        (
                            SMB2_STATUS_NOT_SUPPORTED,
                            0,
                            0,
                            build_not_supported_error_body(),
                        )
                    }
                }
                None => (
                    SMB2_STATUS_INVALID_PARAMETER,
                    0,
                    0,
                    build_not_supported_error_body(),
                ),
            }
        }
        SMB2_COMMAND_SESSION_SETUP => handle_session_setup_command(request, &header, state).await,
        SMB2_COMMAND_LOGOFF => {
            if !state.has_session(header.session_id) {
                (
                    SMB2_STATUS_USER_SESSION_DELETED,
                    header.tree_id,
                    header.session_id,
                    build_simple_response_body(),
                )
            } else {
                release_session_handles(state, gateway, shared_smb_state, header.session_id).await;
                state.remove_session(header.session_id);
                (
                    SMB2_STATUS_SUCCESS,
                    header.tree_id,
                    header.session_id,
                    build_simple_response_body(),
                )
            }
        }
        SMB2_COMMAND_TREE_CONNECT => {
            if !state.has_session(header.session_id) {
                (
                    SMB2_STATUS_USER_SESSION_DELETED,
                    0,
                    header.session_id,
                    build_tree_connect_response_body(0x01),
                )
            } else {
                let session_scope = state
                    .sessions
                    .get(&header.session_id)
                    .and_then(|s| s.fragment_scope.clone());
                match parse_tree_connect_fragment(request) {
                    Some(fragment_candidate) if !fragment_candidate.is_empty() => {
                        let is_ipc_share = fragment_candidate.eq_ignore_ascii_case("IPC$");
                        match resolve_tree_connect_fragment(
                        fragment_candidate.as_str(),
                        session_scope.as_deref(),
                    )
                    .await
                    {
                        Some(fragment_hash) => {
                            let tree_id = state.create_tree(header.session_id, fragment_hash);
                            (
                                SMB2_STATUS_SUCCESS,
                                tree_id,
                                header.session_id,
                                build_tree_connect_response_body(if is_ipc_share {
                                    0x02
                                } else {
                                    0x01
                                }),
                            )
                        }
                        None => (
                            SMB2_STATUS_BAD_NETWORK_NAME,
                            0,
                            header.session_id,
                            build_tree_connect_response_body(0x01),
                        ),
                    }
                }
                    _ => (
                        SMB2_STATUS_BAD_NETWORK_NAME,
                        0,
                        header.session_id,
                        build_tree_connect_response_body(0x01),
                    ),
                }
            }
        }
        SMB2_COMMAND_TREE_DISCONNECT => {
            if !state.has_session(header.session_id) {
                (
                    SMB2_STATUS_USER_SESSION_DELETED,
                    header.tree_id,
                    header.session_id,
                    build_simple_response_body(),
                )
            } else if state.get_tree(header.tree_id).is_none() {
                (
                    SMB2_STATUS_INVALID_PARAMETER,
                    header.tree_id,
                    header.session_id,
                    build_simple_response_body(),
                )
            } else {
                release_tree_handles(state, gateway, shared_smb_state, header.tree_id).await;
                state.remove_tree(header.tree_id);
                (
                    SMB2_STATUS_SUCCESS,
                    header.tree_id,
                    header.session_id,
                    build_simple_response_body(),
                )
            }
        }
        SMB2_COMMAND_ECHO => {
            (
                SMB2_STATUS_SUCCESS,
                header.tree_id,
                header.session_id,
                build_simple_response_body(),
            )
        }
        SMB2_COMMAND_CREATE => {
            let (status, body) = handle_create(request, &header, state, gateway, shared_smb_state).await;
            (status, header.tree_id, header.session_id, body)
        }
        SMB2_COMMAND_READ => {
            let (status, body) = handle_read(request, &header, state, gateway).await;
            (status, header.tree_id, header.session_id, body)
        }
        SMB2_COMMAND_WRITE => {
            let (status, body) = handle_write(request, &header, state, gateway).await;
            (status, header.tree_id, header.session_id, body)
        }
        SMB2_COMMAND_CLOSE => {
            let (status, body) = handle_close(
                request,
                &header,
                state,
                gateway,
                shared_smb_state,
                async_close_commit,
                async_close_threshold_bytes,
            )
            .await;
            (status, header.tree_id, header.session_id, body)
        }
        SMB2_COMMAND_FLUSH => {
            let (status, body) = handle_flush(request, &header, state).await;
            (status, header.tree_id, header.session_id, body)
        }
        SMB2_COMMAND_LOCK => {
            let (status, body) =
                handle_lock(request, &header, state, shared_smb_state).await;
            (status, header.tree_id, header.session_id, body)
        }
        SMB2_COMMAND_QUERY_DIRECTORY => {
            let (status, body) = handle_query_directory(request, &header, state, gateway).await;
            (status, header.tree_id, header.session_id, body)
        }
        SMB2_COMMAND_QUERY_INFO => {
            let (status, body) = handle_query_info(request, &header, state, gateway).await;
            (status, header.tree_id, header.session_id, body)
        }
        SMB2_COMMAND_SET_INFO => {
            let (status, body) =
                handle_set_info(request, &header, state, gateway, shared_smb_state).await;
            (status, header.tree_id, header.session_id, body)
        }
        SMB2_COMMAND_IOCTL => {
            let (status, body) = handle_ioctl(request).await;
            (status, header.tree_id, header.session_id, body)
        }
        _ => (
            SMB2_STATUS_NOT_SUPPORTED,
            header.tree_id,
            header.session_id,
            build_not_supported_error_body(),
        ),
    };

    debug!(
        "smb command={}({:#06X}) msg_id={} session_id={} tree_id={} -> status=0x{:08X}",
        smb2_command_name(header.command),
        header.command,
        header.message_id,
        header.session_id,
        header.tree_id,
        status
    );

    let response_signing_key = state
        .sessions
        .get(&session_id)
        .and_then(|session| session.signing_key)
        .filter(|_| should_sign_smb2_response(&header, status, session_id, state));

    let response = build_framed_smb2_response(
        &header,
        status,
        tree_id,
        session_id,
        &body,
        response_signing_key,
    );
    if header.command == SMB2_COMMAND_NEGOTIATE {
        debug!(
            "smb negotiate response len={} hex={}",
            response.len(),
            hex_prefix(&response, 196)
        );
    }
    Some(response)
}

fn parse_server_auth_config(config: &SmbConfig) -> Option<ServerAuthConfig> {
    let username = config.auth_username.as_ref()?.trim().to_string();
    let password = config.auth_password.as_ref()?.trim().to_string();
    if username.is_empty() || password.is_empty() {
        return None;
    }
    let domain = config
        .auth_domain
        .as_ref()
        .map(|v| v.trim().to_string())
        .unwrap_or_default();
    Some(ServerAuthConfig {
        username,
        password,
        domain,
    })
}

fn should_enforce_signed_request(header: &Smb2HeaderView, state: &ConnectionState) -> bool {
    if header.command == SMB2_COMMAND_NEGOTIATE || header.command == SMB2_COMMAND_SESSION_SETUP {
        return false;
    }
    let Some(session) = state.sessions.get(&header.session_id) else {
        return false;
    };
    session.signing_required && session.auth_state == SessionAuthState::Established
}

fn should_sign_smb2_response(
    header: &Smb2HeaderView,
    status: u32,
    session_id: u64,
    state: &ConnectionState,
) -> bool {
    if header.command == SMB2_COMMAND_NEGOTIATE || session_id == 0 {
        return false;
    }
    let Some(session) = state.sessions.get(&session_id) else {
        return false;
    };
    if !session.signing_required || session.signing_key.is_none() {
        return false;
    }
    if header.command == SMB2_COMMAND_SESSION_SETUP {
        return status == SMB2_STATUS_SUCCESS && session.auth_state == SessionAuthState::Established;
    }
    session.auth_state == SessionAuthState::Established
}

fn verify_smb2_request_signature(
    request: &[u8],
    header: &Smb2HeaderView,
    signing_key: &[u8; 16],
) -> bool {
    if request.len() < SMB2_HEADER_LEN || (header.flags & SMB2_FLAGS_SIGNED) == 0 {
        return false;
    }
    let mut payload = request.to_vec();
    payload[48..64].fill(0);
    let expected = smb2_hmac_sha256_signature(&payload, signing_key);
    expected == header.signature
}

fn smb2_hmac_sha256_signature(payload: &[u8], signing_key: &[u8; 16]) -> [u8; 16] {
    let mut mac =
        HmacSha256::new_from_slice(signing_key).expect("hmac sha256 key accepts any length");
    mac.update(payload);
    let digest = mac.finalize().into_bytes();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

async fn handle_session_setup_command(
    request: &[u8],
    header: &Smb2HeaderView,
    state: &mut ConnectionState,
) -> (u32, u32, u64, Vec<u8>) {
    let session_id = if header.session_id != 0 && state.has_session(header.session_id) {
        header.session_id
    } else {
        state.create_session()
    };

    let security_blob = match parse_session_setup_security_buffer(request) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                0,
                session_id,
                build_not_supported_error_body(),
            );
        }
    };

    let ntlm_message_type = parse_ntlm_message_type(security_blob);
    let auth_config = state.auth_config.clone();
    let Some(session) = state.sessions.get_mut(&session_id) else {
        return (
            SMB2_STATUS_USER_SESSION_DELETED,
            0,
            session_id,
            build_not_supported_error_body(),
        );
    };

    let (status, session_flags, response_blob) = if let Some(auth) = auth_config {
        match session.auth_state {
            SessionAuthState::Established => (SMB2_STATUS_SUCCESS, 0, Vec::new()),
            SessionAuthState::Negotiating | SessionAuthState::ChallengeSent => {
                if ntlm_message_type == Some(1) {
                    let challenge = random_ntlm_challenge();
                    session.server_challenge = Some(challenge);
                    session.auth_state = SessionAuthState::ChallengeSent;
                    let challenge_blob =
                        build_spnego_ntlm_challenge_blob(challenge, auth.domain.as_str());
                    (SMB2_STATUS_MORE_PROCESSING_REQUIRED, 0, challenge_blob)
                } else if ntlm_message_type == Some(3) {
                    let Some(challenge) = session.server_challenge else {
                        session.auth_state = SessionAuthState::Negotiating;
                        session.signing_key = None;
                        session.fragment_scope = None;
                        return (
                            SMB2_STATUS_LOGON_FAILURE,
                            0,
                            session_id,
                            build_session_setup_response_body(0, &[]),
                        );
                    };
                    let Some(type3) = parse_ntlm_type3_auth_data(security_blob) else {
                        session.auth_state = SessionAuthState::Negotiating;
                        session.server_challenge = None;
                        session.signing_key = None;
                        session.fragment_scope = None;
                        return (
                            SMB2_STATUS_LOGON_FAILURE,
                            0,
                            session_id,
                            build_session_setup_response_body(0, &[]),
                        );
                    };
                    let mut matched =
                        verify_ntlmv2_auth(&auth, &challenge, &type3).map(|k| (k, None));
                    if matched.is_none() {
                        matched = verify_fragment_ntlmv2_auth(&challenge, &type3)
                            .await
                            .ok()
                            .flatten()
                            .map(|(k, fragment_hash)| (k, Some(fragment_hash)));
                    }
                    let Some((signing_key, fragment_scope)) = matched else {
                        session.auth_state = SessionAuthState::Negotiating;
                        session.server_challenge = None;
                        session.signing_key = None;
                        session.fragment_scope = None;
                        return (
                            SMB2_STATUS_LOGON_FAILURE,
                            0,
                            session_id,
                            build_session_setup_response_body(0, &[]),
                        );
                    };
                    session.auth_state = SessionAuthState::Established;
                    session.server_challenge = None;
                    session.signing_required = true;
                    session.signing_key = Some(signing_key);
                    session.fragment_scope = fragment_scope;
                    (SMB2_STATUS_SUCCESS, 0, build_spnego_accept_complete_blob())
                } else {
                    session.auth_state = SessionAuthState::Negotiating;
                    session.server_challenge = None;
                    session.signing_key = None;
                    session.fragment_scope = None;
                    return (
                        SMB2_STATUS_LOGON_FAILURE,
                        0,
                        session_id,
                        build_session_setup_response_body(0, &[]),
                    );
                }
            }
        }
    } else {
        match session.auth_state {
            SessionAuthState::Established => (SMB2_STATUS_SUCCESS, 0, Vec::new()),
            SessionAuthState::Negotiating => {
                if security_blob.is_empty() {
                    session.auth_state = SessionAuthState::Established;
                    session.fragment_scope = None;
                    (SMB2_STATUS_SUCCESS, SMB2_SESSION_FLAG_IS_GUEST, Vec::new())
                } else if ntlm_message_type == Some(1) {
                    let challenge = random_ntlm_challenge();
                    session.server_challenge = Some(challenge);
                    session.auth_state = SessionAuthState::ChallengeSent;
                    (
                        SMB2_STATUS_MORE_PROCESSING_REQUIRED,
                        0,
                        build_spnego_ntlm_challenge_blob(challenge, "N1"),
                    )
                } else if ntlm_message_type == Some(3) {
                    let hint_scope = match parse_ntlm_type3_auth_data(security_blob) {
                        Some(t3) => fragment_scope_hint_from_type3(&t3).await,
                        None => None,
                    };
                    session.auth_state = SessionAuthState::Established;
                    session.fragment_scope = hint_scope;
                    (
                        SMB2_STATUS_SUCCESS,
                        SMB2_SESSION_FLAG_IS_GUEST,
                        build_spnego_accept_complete_blob(),
                    )
                } else {
                    session.auth_state = SessionAuthState::Established;
                    session.fragment_scope = None;
                    (SMB2_STATUS_SUCCESS, SMB2_SESSION_FLAG_IS_GUEST, Vec::new())
                }
            }
            SessionAuthState::ChallengeSent => {
                if ntlm_message_type == Some(3) {
                    let Some(challenge) = session.server_challenge else {
                        session.auth_state = SessionAuthState::Established;
                        session.fragment_scope = None;
                        return (
                            SMB2_STATUS_SUCCESS,
                            0,
                            session_id,
                            build_session_setup_response_body(
                                SMB2_SESSION_FLAG_IS_GUEST,
                                &build_spnego_accept_complete_blob(),
                            ),
                        );
                    };
                    let parsed_type3 = parse_ntlm_type3_auth_data(security_blob);
                    let maybe_verified = match parsed_type3.as_ref() {
                        Some(t3) => verify_fragment_ntlmv2_auth(&challenge, t3)
                            .await
                            .ok()
                            .flatten(),
                        None => None,
                    };
                    if let Some((signing_key, fragment_hash)) = maybe_verified {
                        session.auth_state = SessionAuthState::Established;
                        session.server_challenge = None;
                        session.signing_required = true;
                        session.signing_key = Some(signing_key);
                        session.fragment_scope = Some(fragment_hash);
                        return (
                            SMB2_STATUS_SUCCESS,
                            0,
                            session_id,
                            build_session_setup_response_body(0, &build_spnego_accept_complete_blob()),
                        );
                    }
                    let hint_scope = match parsed_type3.as_ref() {
                        Some(t3) => fragment_scope_hint_from_type3(t3).await,
                        None => None,
                    };
                    session.auth_state = SessionAuthState::Established;
                    session.server_challenge = None;
                    session.fragment_scope = hint_scope;
                    (
                        SMB2_STATUS_SUCCESS,
                        SMB2_SESSION_FLAG_IS_GUEST,
                        build_spnego_accept_complete_blob(),
                    )
                } else if security_blob.is_empty() {
                    session.auth_state = SessionAuthState::Established;
                    session.server_challenge = None;
                    session.fragment_scope = None;
                    (
                        SMB2_STATUS_SUCCESS,
                        SMB2_SESSION_FLAG_IS_GUEST,
                        build_spnego_accept_complete_blob(),
                    )
                } else if ntlm_message_type == Some(1) {
                    let challenge = random_ntlm_challenge();
                    session.server_challenge = Some(challenge);
                    (
                        SMB2_STATUS_MORE_PROCESSING_REQUIRED,
                        0,
                        build_spnego_ntlm_challenge_blob(challenge, "N1"),
                    )
                } else {
                    session.auth_state = SessionAuthState::Established;
                    session.server_challenge = None;
                    session.fragment_scope = None;
                    (SMB2_STATUS_SUCCESS, SMB2_SESSION_FLAG_IS_GUEST, Vec::new())
                }
            }
        }
    };

    let body = build_session_setup_response_body(session_flags, &response_blob);
    (status, 0, session_id, body)
}

#[derive(Debug, Clone)]
struct NtlmType3AuthData {
    username: String,
    domain: String,
    nt_response: Vec<u8>,
}

fn random_ntlm_challenge() -> [u8; 8] {
    let mut challenge = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut challenge);
    challenge
}

fn build_ntlm_type2_challenge_blob(challenge: [u8; 8], domain: &str) -> Vec<u8> {
    let target_name_bytes = utf16le_bytes(domain);
    let mut target_info = Vec::new();
    if !target_name_bytes.is_empty() {
        target_info.extend_from_slice(&2u16.to_le_bytes());
        target_info.extend_from_slice(&(target_name_bytes.len() as u16).to_le_bytes());
        target_info.extend_from_slice(&target_name_bytes);
    }
    target_info.extend_from_slice(&0u16.to_le_bytes());
    target_info.extend_from_slice(&0u16.to_le_bytes());

    let payload_offset = 56usize;
    let target_name_offset = payload_offset;
    let target_info_offset = target_name_offset + target_name_bytes.len();

    let mut out = vec![0u8; payload_offset + target_name_bytes.len() + target_info.len()];
    out[0..8].copy_from_slice(&NTLMSSP_SIGNATURE);
    out[8..12].copy_from_slice(&2u32.to_le_bytes());
    write_ntlm_security_buffer(
        &mut out,
        12,
        target_name_bytes.len(),
        target_name_offset,
    );
    out[20..24].copy_from_slice(&NTLM_CHALLENGE_FLAGS.to_le_bytes());
    out[24..32].copy_from_slice(&challenge);
    write_ntlm_security_buffer(&mut out, 40, target_info.len(), target_info_offset);
    out[payload_offset..payload_offset + target_name_bytes.len()].copy_from_slice(&target_name_bytes);
    out[target_info_offset..target_info_offset + target_info.len()].copy_from_slice(&target_info);
    out
}

fn build_spnego_ntlm_challenge_blob(challenge: [u8; 8], domain: &str) -> Vec<u8> {
    let ntlm = build_ntlm_type2_challenge_blob(challenge, domain);
    let mut response_token = Vec::with_capacity(ntlm.len() + 8);
    der_push_tlv(0x04, &ntlm, &mut response_token);

    let mut neg_state = Vec::with_capacity(5);
    neg_state.push(0x0A);
    neg_state.push(0x01);
    neg_state.push(0x01);

    let mut supported_mech = Vec::with_capacity(16);
    supported_mech.extend_from_slice(&[
        0x06, 0x0A, 0x2B, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0A,
    ]);

    let mut seq_content = Vec::with_capacity(neg_state.len() + supported_mech.len() + response_token.len() + 12);
    der_push_tlv(0xA0, &neg_state, &mut seq_content);
    der_push_tlv(0xA1, &supported_mech, &mut seq_content);
    der_push_tlv(0xA2, &response_token, &mut seq_content);

    let mut neg_token_resp = Vec::with_capacity(seq_content.len() + 8);
    der_push_tlv(0x30, &seq_content, &mut neg_token_resp);

    let mut out = Vec::with_capacity(neg_token_resp.len() + 8);
    der_push_tlv(0xA1, &neg_token_resp, &mut out);
    out
}

fn build_spnego_accept_complete_blob() -> Vec<u8> {
    let neg_state = [0x0A, 0x01, 0x00];
    let mut seq_content = Vec::with_capacity(8);
    der_push_tlv(0xA0, &neg_state, &mut seq_content);
    let mut neg_token_resp = Vec::with_capacity(12);
    der_push_tlv(0x30, &seq_content, &mut neg_token_resp);
    let mut out = Vec::with_capacity(16);
    der_push_tlv(0xA1, &neg_token_resp, &mut out);
    out
}

fn der_push_tlv(tag: u8, value: &[u8], out: &mut Vec<u8>) {
    out.push(tag);
    der_push_len(value.len(), out);
    out.extend_from_slice(value);
}

fn der_push_len(len: usize, out: &mut Vec<u8>) {
    if len < 0x80 {
        out.push(len as u8);
        return;
    }
    let mut bytes = [0u8; 8];
    let mut idx = bytes.len();
    let mut remaining = len;
    while remaining > 0 {
        idx -= 1;
        bytes[idx] = (remaining & 0xFF) as u8;
        remaining >>= 8;
    }
    let used = bytes.len() - idx;
    out.push(0x80 | (used as u8));
    out.extend_from_slice(&bytes[idx..]);
}

fn write_ntlm_security_buffer(out: &mut [u8], field_offset: usize, len: usize, offset: usize) {
    out[field_offset..field_offset + 2].copy_from_slice(&(len as u16).to_le_bytes());
    out[field_offset + 2..field_offset + 4].copy_from_slice(&(len as u16).to_le_bytes());
    out[field_offset + 4..field_offset + 8].copy_from_slice(&(offset as u32).to_le_bytes());
}

fn parse_ntlm_type3_auth_data(blob: &[u8]) -> Option<NtlmType3AuthData> {
    let base = find_ntlmssp_signature(blob)?;
    if read_u32(blob, base + 8)? != 3 {
        return None;
    }
    let flags = read_u32(blob, base + 60).unwrap_or(NTLM_NEGOTIATE_UNICODE);
    let unicode = (flags & NTLM_NEGOTIATE_UNICODE) != 0;
    let domain = decode_ntlm_text(read_ntlm_security_buffer(blob, base, base + 28)?, unicode)?;
    let username = decode_ntlm_text(read_ntlm_security_buffer(blob, base, base + 36)?, unicode)?;
    let nt_response = read_ntlm_security_buffer(blob, base, base + 20)?.to_vec();

    Some(NtlmType3AuthData {
        username,
        domain,
        nt_response,
    })
}

fn verify_ntlmv2_auth(
    auth: &ServerAuthConfig,
    challenge: &[u8; 8],
    type3: &NtlmType3AuthData,
) -> Option<[u8; 16]> {
    if !type3.username.eq_ignore_ascii_case(auth.username.as_str()) {
        return None;
    }
    if !auth.domain.is_empty() && !type3.domain.eq_ignore_ascii_case(auth.domain.as_str()) {
        return None;
    }
    if type3.nt_response.len() < 16 {
        return None;
    }

    let nt_proof = &type3.nt_response[..16];
    let blob = &type3.nt_response[16..];
    if blob.is_empty() {
        return None;
    }

    let nt_hash = nt_hash(auth.password.as_str());
    let user_upper = auth.username.to_uppercase();
    let domain = if auth.domain.is_empty() {
        type3.domain.as_str()
    } else {
        auth.domain.as_str()
    };
    let identity = format!("{}{}", user_upper, domain);
    let ntlm_v2_hash = hmac_md5(&nt_hash, &utf16le_bytes(identity.as_str()))?;

    let mut proof_input = Vec::with_capacity(8 + blob.len());
    proof_input.extend_from_slice(challenge);
    proof_input.extend_from_slice(blob);
    let expected_proof = hmac_md5(&ntlm_v2_hash, &proof_input)?;
    if expected_proof.as_slice() != nt_proof {
        return None;
    }

    hmac_md5(&ntlm_v2_hash, nt_proof)
}

fn is_fragment_hash_identifier(value: &str) -> bool {
    value.len() == 64
        && value
            .as_bytes()
            .iter()
            .all(|b| b.is_ascii_hexdigit())
}

fn fragment_identifier_to_hash(identifier: &str) -> Option<String> {
    let normalized = identifier.trim();
    if normalized.is_empty() {
        return None;
    }
    if is_fragment_hash_identifier(normalized) {
        return Some(normalized.to_ascii_lowercase());
    }
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    Some(hex::encode(hasher.finalize()))
}

fn parse_nt_hash_hex(value: &str) -> Option<[u8; 16]> {
    let raw = hex::decode(value.trim()).ok()?;
    if raw.len() != 16 {
        return None;
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&raw);
    Some(out)
}

async fn verify_fragment_ntlmv2_auth(
    challenge: &[u8; 8],
    type3: &NtlmType3AuthData,
) -> Result<Option<([u8; 16], String)>, AppError> {
    if type3.nt_response.len() < 16 {
        return Ok(None);
    }
    let principal = type3.username.trim();
    if principal.is_empty() {
        return Ok(None);
    }
    let Some(fragment_hash) = fragment_identifier_to_hash(principal) else {
        return Ok(None);
    };
    if !does_fragment_exist(fragment_hash.as_str()).await {
        return Ok(None);
    }

    let metadata = match read_fragment_metadata(fragment_hash.as_str()).await {
        Ok(v) => v,
        Err(AppError::NotFound) => return Ok(None),
        Err(e) => return Err(e),
    };
    let Some(stored_nt_hash_hex) = metadata.nt_hash_hex.as_deref() else {
        return Ok(None);
    };
    let Some(stored_nt_hash) = parse_nt_hash_hex(stored_nt_hash_hex) else {
        return Ok(None);
    };

    let nt_proof = &type3.nt_response[..16];
    let blob = &type3.nt_response[16..];
    if blob.is_empty() {
        return Ok(None);
    }

    let identity = format!("{}{}", principal.to_uppercase(), type3.domain.trim());
    let Some(ntlm_v2_hash) = hmac_md5(&stored_nt_hash, &utf16le_bytes(identity.as_str())) else {
        return Ok(None);
    };

    let mut proof_input = Vec::with_capacity(8 + blob.len());
    proof_input.extend_from_slice(challenge);
    proof_input.extend_from_slice(blob);
    let Some(expected_proof) = hmac_md5(&ntlm_v2_hash, &proof_input) else {
        return Ok(None);
    };
    if expected_proof.as_slice() != nt_proof {
        return Ok(None);
    }

    let Some(signing_key) = hmac_md5(&ntlm_v2_hash, nt_proof) else {
        return Ok(None);
    };

    Ok(Some((signing_key, fragment_hash)))
}

async fn fragment_scope_hint_from_type3(type3: &NtlmType3AuthData) -> Option<String> {
    let principal = type3.username.trim();
    if principal.is_empty() {
        return None;
    }
    let fragment_hash = fragment_identifier_to_hash(principal)?;
    if does_fragment_exist(fragment_hash.as_str()).await {
        Some(fragment_hash)
    } else {
        None
    }
}

fn find_ntlmssp_signature(blob: &[u8]) -> Option<usize> {
    if blob.len() < NTLMSSP_SIGNATURE.len() {
        return None;
    }
    for idx in 0..=blob.len().saturating_sub(NTLMSSP_SIGNATURE.len()) {
        if blob[idx..idx + NTLMSSP_SIGNATURE.len()] == NTLMSSP_SIGNATURE {
            return Some(idx);
        }
    }
    None
}

fn read_ntlm_security_buffer<'a>(
    blob: &'a [u8],
    message_base: usize,
    secbuf_offset: usize,
) -> Option<&'a [u8]> {
    let len = read_u16(blob, secbuf_offset)? as usize;
    let offset = read_u32(blob, secbuf_offset + 4)? as usize;
    if len == 0 {
        return Some(&[][..]);
    }
    let start = message_base.checked_add(offset)?;
    let end = start.checked_add(len)?;
    if end > blob.len() {
        return None;
    }
    Some(&blob[start..end])
}

fn decode_ntlm_text(bytes: &[u8], unicode: bool) -> Option<String> {
    if unicode {
        if (bytes.len() % 2) != 0 {
            return None;
        }
        let mut utf16 = Vec::with_capacity(bytes.len() / 2);
        for chunk in bytes.chunks_exact(2) {
            utf16.push(u16::from_le_bytes([chunk[0], chunk[1]]));
        }
        String::from_utf16(&utf16).ok()
    } else {
        String::from_utf8(bytes.to_vec()).ok()
    }
}

fn utf16le_bytes(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len() * 2);
    for codepoint in text.encode_utf16() {
        out.extend_from_slice(&codepoint.to_le_bytes());
    }
    out
}

fn nt_hash(password: &str) -> [u8; 16] {
    let mut hasher = Md4::new();
    hasher.update(&utf16le_bytes(password));
    let digest = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest);
    out
}

fn hmac_md5(key: &[u8], data: &[u8]) -> Option<[u8; 16]> {
    let mut mac = HmacMd5::new_from_slice(key).ok()?;
    mac.update(data);
    let digest = mac.finalize().into_bytes();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest);
    Some(out)
}

fn process_smb1_negotiate_compat(request: &[u8]) -> Option<Vec<u8>> {
    if request.len() < 5 || request[0..4] != SMB1_MAGIC {
        return None;
    }
    if request[4] != SMB1_COMMAND_NEGOTIATE {
        return None;
    }

    let req = Smb2HeaderView {
        credit_charge: 1,
        command: SMB2_COMMAND_NEGOTIATE,
        credit_request: 1,
        flags: 0,
        message_id: 0,
        tree_id: 0,
        session_id: 0,
        signature: [0u8; 16],
    };

    let body = build_negotiate_response_body(
        SMB2_NEGOTIATE_DIALECT_WILDCARD,
        NegotiateSelection {
            signing_mode: SMB2_NEGOTIATE_SIGNING_ENABLED,
            preauth_hash: SMB2_PREAUTH_HASH_ALGO_SHA512,
            encryption_cipher: SMB2_ENCRYPTION_CIPHER_AES128_GCM,
            signing_algorithm: SMB2_SIGNING_ALGORITHM_AES_GMAC,
        },
    );
    Some(build_framed_smb2_response(
        &req,
        SMB2_STATUS_SUCCESS,
        0,
        0,
        &body,
        None,
    ))
}

fn smb2_command_name(command: u16) -> &'static str {
    match command {
        SMB2_COMMAND_NEGOTIATE => "NEGOTIATE",
        SMB2_COMMAND_SESSION_SETUP => "SESSION_SETUP",
        SMB2_COMMAND_LOGOFF => "LOGOFF",
        SMB2_COMMAND_TREE_CONNECT => "TREE_CONNECT",
        SMB2_COMMAND_TREE_DISCONNECT => "TREE_DISCONNECT",
        SMB2_COMMAND_CREATE => "CREATE",
        SMB2_COMMAND_CLOSE => "CLOSE",
        SMB2_COMMAND_FLUSH => "FLUSH",
        SMB2_COMMAND_READ => "READ",
        SMB2_COMMAND_WRITE => "WRITE",
        SMB2_COMMAND_LOCK => "LOCK",
        SMB2_COMMAND_IOCTL => "IOCTL",
        SMB2_COMMAND_ECHO => "ECHO",
        SMB2_COMMAND_QUERY_DIRECTORY => "QUERY_DIRECTORY",
        SMB2_COMMAND_QUERY_INFO => "QUERY_INFO",
        SMB2_COMMAND_SET_INFO => "SET_INFO",
        _ => "UNKNOWN",
    }
}

async fn cleanup_connection_state(
    state: &mut ConnectionState,
    gateway: &SharedGateway,
    shared_smb_state: &SharedSmbState,
) {
    let handles: Vec<([u8; 16], OpenHandleContext)> = state.handles.drain().collect();
    for handle in handles {
        if let Some(write_handle) = handle.1.pending_write {
            let abort_res = gateway.abort_write(&write_handle).await;
            if let Some(transfer_id) = handle.1.transfer_id.as_deref() {
                if let Err(e) = abort_res {
                    transfer_failed(transfer_id, e.to_string().as_str());
                } else {
                    transfer_failed(transfer_id, "connection closed before commit");
                }
            }
        }
    }
    unregister_connection_state(shared_smb_state, state.connection_id).await;
}

fn spawn_async_close_finalize(
    gateway: SharedGateway,
    handle: OpenHandleContext,
    transfer_id: Option<String>,
) {
    tokio::spawn(async move {
        if let Some(write_handle) = handle.pending_write.clone() {
            if let Err(e) = gateway.commit_write(&write_handle).await {
                if let Some(id) = transfer_id.as_deref() {
                    transfer_failed(id, e.to_string().as_str());
                }
                warn!(
                    "smb async close commit failed for {}/{}: {}",
                    handle.fragment, handle.object_key, e
                );
                return;
            }
        }

        if handle.delete_on_close {
            let path = mount_path(handle.fragment.as_str(), handle.object_key.as_str());
            let result = if handle.is_dir {
                gateway.rmdir(path.as_str()).await
            } else {
                gateway.delete(path.as_str()).await
            };
            if let Err(e) = result {
                if let Some(id) = transfer_id.as_deref() {
                    transfer_failed(id, e.to_string().as_str());
                }
                warn!(
                    "smb async delete-on-close failed for {}/{}: {}",
                    handle.fragment, handle.object_key, e
                );
                return;
            }
        }
        if let Some(id) = transfer_id.as_deref() {
            transfer_completed(id);
        }
    });
}

async fn release_session_handles(
    state: &mut ConnectionState,
    gateway: &SharedGateway,
    shared_smb_state: &SharedSmbState,
    session_id: u64,
) {
    let file_ids: Vec<[u8; 16]> = state
        .handles
        .iter()
        .filter(|(_, handle)| handle.session_id == session_id)
        .map(|(file_id, _)| *file_id)
        .collect();

    for file_id in file_ids {
        if let Some(handle) = state.handles.remove(&file_id) {
            if let Some(write_handle) = handle.pending_write.clone() {
                if let Some(transfer_id) = handle.transfer_id.as_deref() {
                    transfer_commit_started(transfer_id);
                }
                if let Err(e) = gateway.commit_write(&write_handle).await {
                    if let Some(transfer_id) = handle.transfer_id.as_deref() {
                        transfer_failed(transfer_id, e.to_string().as_str());
                    }
                } else if let Some(transfer_id) = handle.transfer_id.as_deref() {
                    transfer_completed(transfer_id);
                }
            }
            if handle.delete_on_close {
                let path = mount_path(handle.fragment.as_str(), handle.object_key.as_str());
                let _ = if handle.is_dir {
                    gateway.rmdir(path.as_str()).await
                } else {
                    gateway.delete(path.as_str()).await
                };
            }
            unregister_open_handle(
                shared_smb_state,
                state.connection_id,
                file_id,
                handle.fragment.as_str(),
                handle.object_key.as_str(),
            )
            .await;
        }
    }
}

async fn release_tree_handles(
    state: &mut ConnectionState,
    gateway: &SharedGateway,
    shared_smb_state: &SharedSmbState,
    tree_id: u32,
) {
    let file_ids: Vec<[u8; 16]> = state
        .handles
        .iter()
        .filter(|(_, handle)| handle.tree_id == tree_id)
        .map(|(file_id, _)| *file_id)
        .collect();

    for file_id in file_ids {
        if let Some(handle) = state.handles.remove(&file_id) {
            if let Some(write_handle) = handle.pending_write.clone() {
                if let Some(transfer_id) = handle.transfer_id.as_deref() {
                    transfer_commit_started(transfer_id);
                }
                if let Err(e) = gateway.commit_write(&write_handle).await {
                    if let Some(transfer_id) = handle.transfer_id.as_deref() {
                        transfer_failed(transfer_id, e.to_string().as_str());
                    }
                } else if let Some(transfer_id) = handle.transfer_id.as_deref() {
                    transfer_completed(transfer_id);
                }
            }
            if handle.delete_on_close {
                let path = mount_path(handle.fragment.as_str(), handle.object_key.as_str());
                let _ = if handle.is_dir {
                    gateway.rmdir(path.as_str()).await
                } else {
                    gateway.delete(path.as_str()).await
                };
            }
            unregister_open_handle(
                shared_smb_state,
                state.connection_id,
                file_id,
                handle.fragment.as_str(),
                handle.object_key.as_str(),
            )
            .await;
        }
    }
}

fn share_conflict(
    existing: &OpenShareEntry,
    incoming_share_access: u32,
    incoming_read: bool,
    incoming_write: bool,
    incoming_delete: bool,
) -> bool {
    (incoming_read && (existing.share_access & FILE_SHARE_READ) == 0)
        || (incoming_write && (existing.share_access & FILE_SHARE_WRITE) == 0)
        || (incoming_delete && (existing.share_access & FILE_SHARE_DELETE) == 0)
        || (existing.requested_read && (incoming_share_access & FILE_SHARE_READ) == 0)
        || (existing.requested_write && (incoming_share_access & FILE_SHARE_WRITE) == 0)
        || (existing.requested_delete && (incoming_share_access & FILE_SHARE_DELETE) == 0)
}

async fn register_open_handle(
    shared_smb_state: &SharedSmbState,
    connection_id: u64,
    file_id: [u8; 16],
    fragment: &str,
    object_key: &str,
    share_access: u32,
    requested_read: bool,
    requested_write: bool,
    requested_delete: bool,
) -> Result<(), u32> {
    let path = mount_path(fragment, object_key);
    let mut smb_state = shared_smb_state.lock().await;
    if let Some(existing) = smb_state.opens.get(path.as_str()) {
        for entry in existing {
            if share_conflict(
                entry,
                share_access,
                requested_read,
                requested_write,
                requested_delete,
            ) {
                return Err(SMB2_STATUS_SHARING_VIOLATION);
            }
        }
    }

    smb_state
        .opens
        .entry(path)
        .or_default()
        .push(OpenShareEntry {
            connection_id,
            file_id,
            share_access,
            requested_read,
            requested_write,
            requested_delete,
        });
    Ok(())
}

async fn unregister_open_handle(
    shared_smb_state: &SharedSmbState,
    connection_id: u64,
    file_id: [u8; 16],
    fragment: &str,
    object_key: &str,
) {
    let path = mount_path(fragment, object_key);
    let mut smb_state = shared_smb_state.lock().await;

    let mut remove_path = false;
    if let Some(entries) = smb_state.opens.get_mut(path.as_str()) {
        entries.retain(|entry| !(entry.connection_id == connection_id && entry.file_id == file_id));
        remove_path = entries.is_empty();
    }
    if remove_path {
        smb_state.opens.remove(path.as_str());
    }

    if matches!(
        smb_state.locks.get(path.as_str()),
        Some((owner_connection, owner_file_id))
            if *owner_connection == connection_id && *owner_file_id == file_id
    ) {
        smb_state.locks.remove(path.as_str());
    }
}

async fn move_open_handle_path(
    shared_smb_state: &SharedSmbState,
    connection_id: u64,
    file_id: [u8; 16],
    old_fragment: &str,
    old_object_key: &str,
    new_fragment: &str,
    new_object_key: &str,
) -> Result<(), u32> {
    let old_path = mount_path(old_fragment, old_object_key);
    let new_path = mount_path(new_fragment, new_object_key);
    if old_path == new_path {
        return Ok(());
    }

    let mut smb_state = shared_smb_state.lock().await;

    let moving_entry = smb_state
        .opens
        .get(old_path.as_str())
        .and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry.connection_id == connection_id && entry.file_id == file_id)
                .cloned()
        });

    let Some(entry) = moving_entry else {
        return Ok(());
    };

    if let Some(existing_entries) = smb_state.opens.get(new_path.as_str()) {
        for existing in existing_entries {
            if existing.connection_id == connection_id && existing.file_id == file_id {
                continue;
            }
            if share_conflict(
                existing,
                entry.share_access,
                entry.requested_read,
                entry.requested_write,
                entry.requested_delete,
            ) {
                return Err(SMB2_STATUS_SHARING_VIOLATION);
            }
        }
    }

    if matches!(
        smb_state.locks.get(new_path.as_str()),
        Some((owner_connection, owner_file_id))
            if !(*owner_connection == connection_id && *owner_file_id == file_id)
    ) {
        return Err(SMB2_STATUS_LOCK_NOT_GRANTED);
    }

    let mut remove_old = false;
    if let Some(entries) = smb_state.opens.get_mut(old_path.as_str()) {
        entries.retain(|e| !(e.connection_id == connection_id && e.file_id == file_id));
        remove_old = entries.is_empty();
    }
    if remove_old {
        smb_state.opens.remove(old_path.as_str());
    }

    smb_state
        .opens
        .entry(new_path.clone())
        .or_default()
        .push(entry);

    if matches!(
        smb_state.locks.get(old_path.as_str()),
        Some((owner_connection, owner_file_id))
            if *owner_connection == connection_id && *owner_file_id == file_id
    ) {
        smb_state.locks.remove(old_path.as_str());
        smb_state
            .locks
            .insert(new_path, (connection_id, file_id));
    }

    Ok(())
}

async fn unregister_connection_state(shared_smb_state: &SharedSmbState, connection_id: u64) {
    let mut smb_state = shared_smb_state.lock().await;

    let mut empty_keys = Vec::new();
    for (path, entries) in smb_state.opens.iter_mut() {
        entries.retain(|entry| entry.connection_id != connection_id);
        if entries.is_empty() {
            empty_keys.push(path.clone());
        }
    }
    for key in empty_keys {
        smb_state.opens.remove(key.as_str());
    }

    smb_state
        .locks
        .retain(|_, (owner_connection, _)| *owner_connection != connection_id);
}

async fn handle_create(
    request: &[u8],
    header: &Smb2HeaderView,
    state: &mut ConnectionState,
    gateway: &SharedGateway,
    shared_smb_state: &SharedSmbState,
) -> (u32, Vec<u8>) {
    if !state.has_session(header.session_id) {
        return (
            SMB2_STATUS_USER_SESSION_DELETED,
            build_not_supported_error_body(),
        );
    }

    let Some(tree) = state.get_tree(header.tree_id) else {
        return (
            SMB2_STATUS_BAD_NETWORK_NAME,
            build_not_supported_error_body(),
        );
    };
    let fragment = tree.fragment.clone();

    let base = SMB2_HEADER_LEN;
    if request.len() < base + 56 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let desired_access = match read_u32(request, base + 24) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let share_access = match read_u32(request, base + 32) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let create_disposition = match read_u32(request, base + 36) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let create_options = match read_u32(request, base + 40) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let name_offset = match read_u16(request, base + 44) {
        Some(v) => v as usize,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let name_length = match read_u16(request, base + 46) {
        Some(v) => v as usize,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };

    let name = if name_length == 0 {
        String::new()
    } else {
        match decode_utf16_from_absolute_offset(request, name_offset, name_length) {
            Some(v) => v,
            None => {
                return (
                    SMB2_STATUS_INVALID_PARAMETER,
                    build_not_supported_error_body(),
                );
            }
        }
    };

    let object_key = normalize_object_key(&name);
    let requested_dir = object_key.is_empty() || (create_options & FILE_DIRECTORY_FILE) != 0;
    let requested_non_dir = (create_options & FILE_NON_DIRECTORY_FILE) != 0;
    let delete_on_close = (create_options & FILE_DELETE_ON_CLOSE) != 0;
    let writable =
        (desired_access & (FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_GENERIC_WRITE)) != 0;
    let requested_read =
        (desired_access & (FILE_READ_DATA | FILE_EXECUTE | FILE_GENERIC_READ | FILE_GENERIC_ALL))
            != 0;
    let requested_write =
        (desired_access & (FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_GENERIC_WRITE | FILE_GENERIC_ALL))
            != 0;
    let requested_delete = (desired_access & (FILE_DELETE | FILE_GENERIC_ALL)) != 0;

    debug!(
        "smb create path='{}' disposition={} desired_access=0x{:08X} create_options=0x{:08X}",
        object_key,
        create_disposition,
        desired_access,
        create_options
    );

    if !matches!(
        create_disposition,
        FILE_CREATE_DISPOSITION_SUPERSEDE
            | FILE_CREATE_DISPOSITION_OPEN
            | FILE_CREATE_DISPOSITION_CREATE
            | FILE_CREATE_DISPOSITION_OPEN_IF
            | FILE_CREATE_DISPOSITION_OVERWRITE
            | FILE_CREATE_DISPOSITION_OVERWRITE_IF
    ) {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let mount_path = mount_path(&fragment, &object_key);
    let existing = match stat_optional(gateway, &mount_path).await {
        Ok(v) => v,
        Err(e) => return (app_error_to_status(e), build_not_supported_error_body()),
    };
    let pending_commit_size = if existing.is_none() && !object_key.is_empty() {
        pending_commit_expected_size(fragment.as_str(), object_key.as_str())
    } else {
        None
    };
    if pending_commit_size.is_some() && (requested_write || writable) {
        return (SMB2_STATUS_SHARING_VIOLATION, build_not_supported_error_body());
    }

    if object_key.is_empty() {
        let file_id = state.new_file_id();
        if let Err(status) = register_open_handle(
            shared_smb_state,
            state.connection_id,
            file_id,
            fragment.as_str(),
            object_key.as_str(),
            share_access,
            requested_read,
            requested_write,
            requested_delete,
        )
        .await
        {
            return (status, build_not_supported_error_body());
        }
        state.handles.insert(
            file_id,
            OpenHandleContext {
                session_id: header.session_id,
                tree_id: header.tree_id,
                fragment: fragment.clone(),
                object_key,
                is_dir: true,
                writable: false,
                delete_on_close,
                dir_query_cursor: 0,
                dir_query_pattern: "*".to_string(),
                dir_query_info_class: FILE_INFORMATION_CLASS_DIRECTORY_INFORMATION,
                expected_size_bytes: None,
                max_written_end: 0,
                transfer_id: None,
                materialize_on_close: false,
                pending_write: None,
            },
        );
        return (
            SMB2_STATUS_SUCCESS,
            build_create_response_body(file_id, true, 0, FILE_CREATE_ACTION_OPENED),
        );
    }

    let exists = existing.is_some() || pending_commit_size.is_some();
    let existing_is_dir = existing
        .as_ref()
        .map(|m| matches!(m.kind, NodeKind::Directory))
        .unwrap_or(false);

    if exists && requested_dir && !existing_is_dir {
        return (
            SMB2_STATUS_NOT_A_DIRECTORY,
            build_not_supported_error_body(),
        );
    }
    if exists && requested_non_dir && existing_is_dir {
        return (
            SMB2_STATUS_FILE_IS_A_DIRECTORY,
            build_not_supported_error_body(),
        );
    }

    let is_dir = if exists { existing_is_dir } else { requested_dir };

    if exists && create_disposition == FILE_CREATE_DISPOSITION_CREATE {
        return (
            SMB2_STATUS_OBJECT_NAME_COLLISION,
            build_not_supported_error_body(),
        );
    }

    if !exists
        && (create_disposition == FILE_CREATE_DISPOSITION_OPEN
            || create_disposition == FILE_CREATE_DISPOSITION_OVERWRITE)
    {
        return (
            SMB2_STATUS_OBJECT_NAME_NOT_FOUND,
            build_not_supported_error_body(),
        );
    }

    if is_dir {
        let should_create_dir = !exists
            && matches!(
                create_disposition,
                FILE_CREATE_DISPOSITION_SUPERSEDE
                    | FILE_CREATE_DISPOSITION_CREATE
                    | FILE_CREATE_DISPOSITION_OPEN_IF
                    | FILE_CREATE_DISPOSITION_OVERWRITE_IF
            );

        if should_create_dir {
            if let Err(e) = gateway.mkdir(&mount_path).await {
                return (app_error_to_status(e), build_not_supported_error_body());
            }
        }
    } else {
        let create_or_overwrite = matches!(
            create_disposition,
            FILE_CREATE_DISPOSITION_SUPERSEDE
                | FILE_CREATE_DISPOSITION_CREATE
                | FILE_CREATE_DISPOSITION_OPEN_IF
                | FILE_CREATE_DISPOSITION_OVERWRITE
                | FILE_CREATE_DISPOSITION_OVERWRITE_IF
        );

        if create_or_overwrite {
            if !writable {
                return (SMB2_STATUS_ACCESS_DENIED, build_not_supported_error_body());
            }
        }
    }

    let create_action = if !exists {
        FILE_CREATE_ACTION_CREATED
    } else if create_disposition == FILE_CREATE_DISPOSITION_SUPERSEDE {
        FILE_CREATE_ACTION_SUPERSEDED
    } else if matches!(
        create_disposition,
        FILE_CREATE_DISPOSITION_OVERWRITE | FILE_CREATE_DISPOSITION_OVERWRITE_IF
    ) {
        FILE_CREATE_ACTION_OVERWRITTEN
    } else {
        FILE_CREATE_ACTION_OPENED
    };
    let materialize_on_close = !is_dir
        && matches!(
            create_action,
            FILE_CREATE_ACTION_CREATED
                | FILE_CREATE_ACTION_SUPERSEDED
                | FILE_CREATE_ACTION_OVERWRITTEN
        );

    let file_size = existing
        .map(|m| m.size_bytes)
        .or(pending_commit_size)
        .unwrap_or(0);

    let file_id = state.new_file_id();
    if let Err(status) = register_open_handle(
        shared_smb_state,
        state.connection_id,
        file_id,
        fragment.as_str(),
        object_key.as_str(),
        share_access,
        requested_read,
        requested_write,
        requested_delete,
    )
    .await
    {
        return (status, build_not_supported_error_body());
    }
    state.handles.insert(
        file_id,
        OpenHandleContext {
            session_id: header.session_id,
            tree_id: header.tree_id,
            fragment,
            object_key,
            is_dir,
            writable,
            delete_on_close,
            dir_query_cursor: 0,
            dir_query_pattern: "*".to_string(),
            dir_query_info_class: FILE_INFORMATION_CLASS_DIRECTORY_INFORMATION,
            expected_size_bytes: None,
            max_written_end: 0,
            transfer_id: None,
            materialize_on_close,
            pending_write: None,
        },
    );

    (
        SMB2_STATUS_SUCCESS,
        build_create_response_body(file_id, is_dir, file_size, create_action),
    )
}

async fn handle_read(
    request: &[u8],
    header: &Smb2HeaderView,
    state: &mut ConnectionState,
    gateway: &SharedGateway,
) -> (u32, Vec<u8>) {
    if !state.has_session(header.session_id) {
        return (
            SMB2_STATUS_USER_SESSION_DELETED,
            build_not_supported_error_body(),
        );
    }

    let base = SMB2_HEADER_LEN;
    if request.len() < base + 48 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let length = match read_u32(request, base + 4) {
        Some(v) => v as usize,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let offset = match read_u64(request, base + 8) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let file_id = match read_file_id(request, base + 16) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let Some(handle) = state.handles.get(&file_id).cloned() else {
        return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
    };

    if handle.session_id != header.session_id || handle.tree_id != header.tree_id {
        return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
    }
    if handle.is_dir {
        return (
            SMB2_STATUS_NOT_A_DIRECTORY,
            build_not_supported_error_body(),
        );
    }

    let path = mount_path(&handle.fragment, &handle.object_key);
    match gateway.read_range(&path, offset, length).await {
        Ok(bytes) => (SMB2_STATUS_SUCCESS, build_read_response_body(&bytes)),
        Err(e) => (app_error_to_status(e), build_not_supported_error_body()),
    }
}

async fn handle_write(
    request: &[u8],
    header: &Smb2HeaderView,
    state: &mut ConnectionState,
    gateway: &SharedGateway,
) -> (u32, Vec<u8>) {
    if !state.has_session(header.session_id) {
        return (
            SMB2_STATUS_USER_SESSION_DELETED,
            build_not_supported_error_body(),
        );
    }

    let base = SMB2_HEADER_LEN;
    if request.len() < base + 48 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let data_offset = match read_u16(request, base + 2) {
        Some(v) => v as usize,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let length = match read_u32(request, base + 4) {
        Some(v) => v as usize,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let offset = match read_u64(request, base + 8) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let file_id = match read_file_id(request, base + 16) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let remaining_bytes = match read_u32(request, base + 36) {
        Some(v) => v as u64,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };

    if length == 0 {
        return (SMB2_STATUS_SUCCESS, build_write_response_body(0));
    }

    if data_offset < SMB2_HEADER_LEN {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let end = match data_offset.checked_add(length) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    if end > request.len() {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let write_end_offset = match offset.checked_add(length as u64) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };

    let mut begin_write: Option<(String, Option<u64>)> = None;
    {
        let Some(handle) = state.handles.get(&file_id) else {
            return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
        };

        if handle.session_id != header.session_id || handle.tree_id != header.tree_id {
            return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
        }
        if handle.is_dir {
            return (
                SMB2_STATUS_NOT_A_DIRECTORY,
                build_not_supported_error_body(),
            );
        }
        if !handle.writable {
            return (SMB2_STATUS_ACCESS_DENIED, build_not_supported_error_body());
        }

        if handle.pending_write.is_none() {
            let inferred_expected_size = if remaining_bytes > 0 {
                write_end_offset
                    .checked_add(remaining_bytes)
            } else {
                None
            };
            let expected_size = handle.expected_size_bytes.or(inferred_expected_size);
            begin_write = Some((mount_path(&handle.fragment, &handle.object_key), expected_size));
        }
    }

    if let Some((path, expected_size)) = begin_write {
        let write_handle = match gateway.begin_write(&path, expected_size).await {
            Ok(v) => v,
            Err(e) => return (app_error_to_status(e), build_not_supported_error_body()),
        };

        if let Some(handle) = state.handles.get_mut(&file_id) {
            if handle.transfer_id.is_none() {
                let transfer_id = Ulid::new().to_string();
                transfer_started(
                    transfer_id.as_str(),
                    handle.fragment.as_str(),
                    handle.object_key.as_str(),
                    expected_size,
                );
                handle.transfer_id = Some(transfer_id);
            }
            handle.pending_write = Some(write_handle);
            if handle.expected_size_bytes.is_none() {
                handle.expected_size_bytes = expected_size;
            }
            handle.materialize_on_close = true;
        } else {
            return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
        }
    }

    let write_handle = match state
        .handles
        .get(&file_id)
        .and_then(|h| h.pending_write.clone())
    {
        Some(v) => v,
        None => return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body()),
    };

    let bytes = request[data_offset..end].to_vec();
    match gateway.write_chunk(&write_handle, offset, bytes).await {
        Ok(_) => {
            if let Some(handle) = state.handles.get_mut(&file_id) {
                handle.max_written_end = handle.max_written_end.max(write_end_offset);
                if let Some(transfer_id) = handle.transfer_id.as_deref() {
                    transfer_ingress_progress(
                        transfer_id,
                        handle.max_written_end,
                        handle.expected_size_bytes,
                    );
                }
            }
            (
                SMB2_STATUS_SUCCESS,
                build_write_response_body(length as u32),
            )
        }
        Err(e) => {
            if let Some(handle) = state.handles.get(&file_id) {
                if let Some(transfer_id) = handle.transfer_id.as_deref() {
                    transfer_failed(transfer_id, e.to_string().as_str());
                }
            }
            (app_error_to_status(e), build_not_supported_error_body())
        }
    }
}

async fn handle_close(
    request: &[u8],
    header: &Smb2HeaderView,
    state: &mut ConnectionState,
    gateway: &SharedGateway,
    shared_smb_state: &SharedSmbState,
    async_close_commit: bool,
    async_close_threshold_bytes: u64,
) -> (u32, Vec<u8>) {
    if !state.has_session(header.session_id) {
        return (
            SMB2_STATUS_USER_SESSION_DELETED,
            build_not_supported_error_body(),
        );
    }

    let base = SMB2_HEADER_LEN;
    if request.len() < base + 24 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let file_id = match read_file_id(request, base + 8) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };

    let Some(mut handle) = state.handles.remove(&file_id) else {
        return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
    };

    if handle.session_id != header.session_id || handle.tree_id != header.tree_id {
        return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
    }

    unregister_open_handle(
        shared_smb_state,
        state.connection_id,
        file_id,
        handle.fragment.as_str(),
        handle.object_key.as_str(),
    )
    .await;

    if handle.pending_write.is_none() && !handle.is_dir && handle.materialize_on_close {
        let path = mount_path(&handle.fragment, &handle.object_key);
        let write_handle = match gateway.begin_write(&path, handle.expected_size_bytes).await {
            Ok(v) => v,
            Err(e) => return (app_error_to_status(e), build_not_supported_error_body()),
        };
        if handle.transfer_id.is_none() {
            let transfer_id = Ulid::new().to_string();
            transfer_started(
                transfer_id.as_str(),
                handle.fragment.as_str(),
                handle.object_key.as_str(),
                handle.expected_size_bytes,
            );
            handle.transfer_id = Some(transfer_id);
        }
        handle.pending_write = Some(write_handle);
    }

    if handle.pending_write.is_some() {
        let write_size_hint = handle.expected_size_bytes.unwrap_or(0).max(handle.max_written_end);
        let should_async_close = async_close_commit || write_size_hint >= async_close_threshold_bytes;
        if let Some(transfer_id) = handle.transfer_id.as_deref() {
            transfer_commit_started(transfer_id);
        }

        if should_async_close {
            spawn_async_close_finalize(gateway.clone(), handle.clone(), handle.transfer_id.clone());
        } else if let Some(write_handle) = handle.pending_write.clone() {
            if let Err(e) = gateway.commit_write(&write_handle).await {
                if let Some(transfer_id) = handle.transfer_id.as_deref() {
                    transfer_failed(transfer_id, e.to_string().as_str());
                }
                return (app_error_to_status(e), build_not_supported_error_body());
            }
            if handle.delete_on_close {
                let path = mount_path(handle.fragment.as_str(), handle.object_key.as_str());
                let delete_result = if handle.is_dir {
                    gateway.rmdir(&path).await
                } else {
                    gateway.delete(&path).await
                };
                if let Err(e) = delete_result {
                    if let Some(transfer_id) = handle.transfer_id.as_deref() {
                        transfer_failed(transfer_id, e.to_string().as_str());
                    }
                    return (app_error_to_status(e), build_not_supported_error_body());
                }
                if let Some(transfer_id) = handle.transfer_id.as_deref() {
                    transfer_completed(transfer_id);
                }
                return (
                    SMB2_STATUS_SUCCESS,
                    build_close_response_body(0, FILE_ATTRIBUTE_NORMAL),
                );
            }
            if let Some(transfer_id) = handle.transfer_id.as_deref() {
                transfer_completed(transfer_id);
            }
        }
        return (
            SMB2_STATUS_SUCCESS,
            build_close_response_body(0, FILE_ATTRIBUTE_NORMAL),
        );
    }

    if handle.delete_on_close {
        let path = mount_path(&handle.fragment, &handle.object_key);
        let delete_result = if handle.is_dir {
            gateway.rmdir(&path).await
        } else {
            gateway.delete(&path).await
        };
        if let Err(e) = delete_result {
            return (app_error_to_status(e), build_not_supported_error_body());
        }
        return (
            SMB2_STATUS_SUCCESS,
            build_close_response_body(0, FILE_ATTRIBUTE_NORMAL),
        );
    }

    let path = mount_path(&handle.fragment, &handle.object_key);
    let (size, attrs) = if handle.is_dir {
        (0, FILE_ATTRIBUTE_DIRECTORY)
    } else {
        match stat_optional(gateway, &path).await {
            Ok(Some(meta)) => (meta.size_bytes, FILE_ATTRIBUTE_NORMAL),
            Ok(None) => (0, FILE_ATTRIBUTE_NORMAL),
            Err(_) => (0, FILE_ATTRIBUTE_NORMAL),
        }
    };

    (SMB2_STATUS_SUCCESS, build_close_response_body(size, attrs))
}

async fn handle_lock(
    request: &[u8],
    header: &Smb2HeaderView,
    state: &mut ConnectionState,
    shared_smb_state: &SharedSmbState,
) -> (u32, Vec<u8>) {
    if !state.has_session(header.session_id) {
        return (
            SMB2_STATUS_USER_SESSION_DELETED,
            build_not_supported_error_body(),
        );
    }

    let base = SMB2_HEADER_LEN;
    if request.len() < base + 24 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let lock_count = match read_u16(request, base + 2) {
        Some(v) => v as usize,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let file_id = match read_file_id(request, base + 8) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };

    if lock_count == 0 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let lock_bytes = match lock_count.checked_mul(24) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    if request.len() < base + 24 + lock_bytes {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let Some(handle) = state.handles.get(&file_id).cloned() else {
        return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
    };
    if handle.session_id != header.session_id || handle.tree_id != header.tree_id {
        return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
    }
    if handle.is_dir {
        return (
            SMB2_STATUS_NOT_A_DIRECTORY,
            build_not_supported_error_body(),
        );
    }

    let path = mount_path(handle.fragment.as_str(), handle.object_key.as_str());
    let mut smb_state = shared_smb_state.lock().await;

    for idx in 0..lock_count {
        let elem_offset = base + 24 + idx * 24;
        let flags = match read_u32(request, elem_offset + 16) {
            Some(v) => v,
            None => {
                return (
                    SMB2_STATUS_INVALID_PARAMETER,
                    build_not_supported_error_body(),
                );
            }
        };
        let unlock = (flags & SMB2_LOCKFLAG_UNLOCK) != 0;
        if unlock {
            match smb_state.locks.get(path.as_str()) {
                Some((conn_id, fid))
                    if *conn_id == state.connection_id && *fid == file_id =>
                {
                    smb_state.locks.remove(path.as_str());
                }
                _ => {
                    return (
                        SMB2_STATUS_LOCK_NOT_GRANTED,
                        build_not_supported_error_body(),
                    );
                }
            }
            continue;
        }

        match smb_state.locks.get(path.as_str()) {
            Some((conn_id, fid))
                if *conn_id == state.connection_id && *fid == file_id => {}
            Some(_) => {
                return (
                    SMB2_STATUS_LOCK_NOT_GRANTED,
                    build_not_supported_error_body(),
                );
            }
            None => {
                smb_state
                    .locks
                    .insert(path.clone(), (state.connection_id, file_id));
            }
        }
    }

    (SMB2_STATUS_SUCCESS, build_simple_response_body())
}

async fn handle_flush(
    request: &[u8],
    header: &Smb2HeaderView,
    state: &mut ConnectionState,
) -> (u32, Vec<u8>) {
    if !state.has_session(header.session_id) {
        return (
            SMB2_STATUS_USER_SESSION_DELETED,
            build_not_supported_error_body(),
        );
    }

    let base = SMB2_HEADER_LEN;
    if request.len() < base + 24 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let file_id = match read_file_id(request, base + 8) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };

    let Some(handle) = state.handles.get(&file_id) else {
        return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
    };
    if handle.session_id != header.session_id || handle.tree_id != header.tree_id {
        return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
    }

    (SMB2_STATUS_SUCCESS, build_flush_response_body())
}

async fn handle_query_info(
    request: &[u8],
    header: &Smb2HeaderView,
    state: &mut ConnectionState,
    gateway: &SharedGateway,
) -> (u32, Vec<u8>) {
    if !state.has_session(header.session_id) {
        return (
            SMB2_STATUS_USER_SESSION_DELETED,
            build_not_supported_error_body(),
        );
    }

    let base = SMB2_HEADER_LEN;
    if request.len() < base + 40 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let info_type = request[base + 2];
    let file_info_class = request[base + 3];
    let output_limit = match read_u32(request, base + 4) {
        Some(v) => v as usize,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let file_id = match read_file_id(request, base + 24) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };

    let Some(handle) = state.handles.get(&file_id).cloned() else {
        return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
    };
    if handle.session_id != header.session_id || handle.tree_id != header.tree_id {
        return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
    }

    let metadata = match metadata_for_handle(gateway, &handle).await {
        Ok(v) => v,
        Err(e) => return (app_error_to_status(e), build_not_supported_error_body()),
    };

    let buffer = match info_type {
        SMB2_INFO_TYPE_FILE => match file_info_class {
            FILE_INFO_CLASS_BASIC_INFORMATION => {
                build_file_basic_information_buffer(&metadata, handle.is_dir)
            }
            FILE_INFO_CLASS_STANDARD_INFORMATION => build_file_standard_information_buffer(
                &metadata,
                handle.is_dir,
                handle.delete_on_close,
            ),
            FILE_INFO_CLASS_ALL_INFORMATION => build_file_all_information_buffer(
                &metadata,
                handle.is_dir,
                handle.delete_on_close,
                &handle.object_key,
            ),
            FILE_INFO_CLASS_NETWORK_OPEN_INFORMATION => {
                build_file_network_open_information_buffer(&metadata, handle.is_dir)
            }
            _ => {
                debug!(
                    "smb query_info unsupported file_info_class=0x{:02X}",
                    file_info_class
                );
                return (SMB2_STATUS_NOT_SUPPORTED, build_not_supported_error_body());
            }
        },
        SMB2_INFO_TYPE_FILESYSTEM => match file_info_class {
            FS_INFO_CLASS_VOLUME_INFORMATION => build_fs_volume_information_buffer("N1"),
            FS_INFO_CLASS_SIZE_INFORMATION => build_fs_size_information_buffer(),
            FS_INFO_CLASS_DEVICE_INFORMATION => build_fs_device_information_buffer(),
            FS_INFO_CLASS_ATTRIBUTE_INFORMATION => build_fs_attribute_information_buffer("N1FS"),
            FS_INFO_CLASS_FULL_SIZE_INFORMATION => build_fs_full_size_information_buffer(),
            _ => {
                debug!(
                    "smb query_info unsupported fs_info_class=0x{:02X}",
                    file_info_class
                );
                return (SMB2_STATUS_NOT_SUPPORTED, build_not_supported_error_body());
            }
        },
        _ => {
            debug!("smb query_info unsupported info_type=0x{:02X}", info_type);
            return (SMB2_STATUS_NOT_SUPPORTED, build_not_supported_error_body());
        }
    };

    let limited = if output_limit == 0 {
        Vec::new()
    } else if buffer.len() > output_limit {
        buffer[..output_limit].to_vec()
    } else {
        buffer
    };

    (
        SMB2_STATUS_SUCCESS,
        build_query_info_response_body(limited.as_slice()),
    )
}

async fn handle_set_info(
    request: &[u8],
    header: &Smb2HeaderView,
    state: &mut ConnectionState,
    gateway: &SharedGateway,
    shared_smb_state: &SharedSmbState,
) -> (u32, Vec<u8>) {
    if !state.has_session(header.session_id) {
        return (
            SMB2_STATUS_USER_SESSION_DELETED,
            build_not_supported_error_body(),
        );
    }

    let base = SMB2_HEADER_LEN;
    if request.len() < base + 32 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let info_type = request[base + 2];
    let file_info_class = request[base + 3];
    let buffer_length = match read_u32(request, base + 4) {
        Some(v) => v as usize,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let buffer_offset = match read_u16(request, base + 8) {
        Some(v) => v as usize,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    let file_id = match read_file_id(request, base + 16) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };

    let end = match buffer_offset.checked_add(buffer_length) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };
    if buffer_length > 0 && (buffer_offset < SMB2_HEADER_LEN || end > request.len()) {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }
    let info_buf = if buffer_length == 0 {
        &[][..]
    } else {
        &request[buffer_offset..end]
    };

    let current = match state.handles.get(&file_id).cloned() {
        Some(v) => v,
        None => return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body()),
    };
    if current.session_id != header.session_id || current.tree_id != header.tree_id {
        return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
    }

    if info_type != SMB2_INFO_TYPE_FILE {
        return (SMB2_STATUS_NOT_SUPPORTED, build_not_supported_error_body());
    }

    match file_info_class {
        FILE_INFO_CLASS_DISPOSITION_INFORMATION => {
            if info_buf.is_empty() {
                return (
                    SMB2_STATUS_INVALID_PARAMETER,
                    build_not_supported_error_body(),
                );
            }
            let delete_on_close = info_buf[0] != 0;
            if let Some(handle) = state.handles.get_mut(&file_id) {
                handle.delete_on_close = delete_on_close;
            }
            (SMB2_STATUS_SUCCESS, build_set_info_response_body())
        }
        FILE_INFO_CLASS_DISPOSITION_INFORMATION_EX => {
            if info_buf.len() < 4 {
                return (
                    SMB2_STATUS_INVALID_PARAMETER,
                    build_not_supported_error_body(),
                );
            }
            let flags = match read_u32(info_buf, 0) {
                Some(v) => v,
                None => {
                    return (
                        SMB2_STATUS_INVALID_PARAMETER,
                        build_not_supported_error_body(),
                    );
                }
            };
            let delete_on_close = (flags & 0x0000_0001) != 0;
            if let Some(handle) = state.handles.get_mut(&file_id) {
                handle.delete_on_close = delete_on_close;
            }
            (SMB2_STATUS_SUCCESS, build_set_info_response_body())
        }
        FILE_INFO_CLASS_RENAME_INFORMATION => {
            if current.object_key.is_empty() || info_buf.len() < 20 {
                return (
                    SMB2_STATUS_INVALID_PARAMETER,
                    build_not_supported_error_body(),
                );
            }
            let name_len = match read_u32(info_buf, 16) {
                Some(v) => v as usize,
                None => {
                    return (
                        SMB2_STATUS_INVALID_PARAMETER,
                        build_not_supported_error_body(),
                    );
                }
            };
            if info_buf.len() < 20 + name_len || (name_len % 2) != 0 {
                return (
                    SMB2_STATUS_INVALID_PARAMETER,
                    build_not_supported_error_body(),
                );
            }
            let target_raw = match decode_utf16_bytes(&info_buf[20..20 + name_len]) {
                Some(v) => v,
                None => {
                    return (
                        SMB2_STATUS_INVALID_PARAMETER,
                        build_not_supported_error_body(),
                    );
                }
            };
            let target_key = normalize_object_key(target_raw.as_str());
            if target_key.is_empty() {
                return (
                    SMB2_STATUS_INVALID_PARAMETER,
                    build_not_supported_error_body(),
                );
            }

            let from = mount_path(&current.fragment, &current.object_key);
            let to = mount_path(&current.fragment, &target_key);
            if let Err(e) = gateway.rename(&from, &to).await {
                return (app_error_to_status(e), build_not_supported_error_body());
            }
            if let Err(status) = move_open_handle_path(
                shared_smb_state,
                state.connection_id,
                file_id,
                current.fragment.as_str(),
                current.object_key.as_str(),
                current.fragment.as_str(),
                target_key.as_str(),
            )
            .await
            {
                return (status, build_not_supported_error_body());
            }
            if let Some(handle) = state.handles.get_mut(&file_id) {
                handle.object_key = target_key;
            }
            (SMB2_STATUS_SUCCESS, build_set_info_response_body())
        }
        FILE_INFO_CLASS_POSITION_INFORMATION => {
            if current.is_dir {
                return (SMB2_STATUS_ACCESS_DENIED, build_not_supported_error_body());
            }
            (SMB2_STATUS_SUCCESS, build_set_info_response_body())
        }
        FILE_INFO_CLASS_MODE_INFORMATION => (SMB2_STATUS_SUCCESS, build_set_info_response_body()),
        FILE_INFO_CLASS_ALLOCATION_INFORMATION => {
            if info_buf.len() < 8 {
                return (
                    SMB2_STATUS_INVALID_PARAMETER,
                    build_not_supported_error_body(),
                );
            }
            if current.is_dir || !current.writable {
                return (SMB2_STATUS_ACCESS_DENIED, build_not_supported_error_body());
            }
            let size = match read_u64(info_buf, 0) {
                Some(v) => v,
                None => {
                    return (
                        SMB2_STATUS_INVALID_PARAMETER,
                        build_not_supported_error_body(),
                    );
                }
            };
            if let Some(handle) = state.handles.get_mut(&file_id) {
                handle.expected_size_bytes = Some(size);
                handle.materialize_on_close = true;
            }
            (SMB2_STATUS_SUCCESS, build_set_info_response_body())
        }
        FILE_INFO_CLASS_END_OF_FILE_INFORMATION => {
            if info_buf.len() < 8 {
                return (
                    SMB2_STATUS_INVALID_PARAMETER,
                    build_not_supported_error_body(),
                );
            }
            if current.is_dir || !current.writable {
                return (SMB2_STATUS_ACCESS_DENIED, build_not_supported_error_body());
            }
            let size = match read_u64(info_buf, 0) {
                Some(v) => v,
                None => {
                    return (
                        SMB2_STATUS_INVALID_PARAMETER,
                        build_not_supported_error_body(),
                    );
                }
            };
            if let Some(handle) = state.handles.get_mut(&file_id) {
                handle.expected_size_bytes = Some(size);
                handle.materialize_on_close = true;
            }
            (SMB2_STATUS_SUCCESS, build_set_info_response_body())
        }
        FILE_INFO_CLASS_BASIC_INFORMATION => (SMB2_STATUS_SUCCESS, build_set_info_response_body()),
        _ => (SMB2_STATUS_NOT_SUPPORTED, build_not_supported_error_body()),
    }
}

async fn handle_ioctl(request: &[u8]) -> (u32, Vec<u8>) {
    let base = SMB2_HEADER_LEN;
    if request.len() < base + 57 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let structure_size = read_u16(request, base).unwrap_or(0);
    if structure_size < 57 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let ctl_code = read_u32(request, base + 4).unwrap_or(0);
    let max_output_response = read_u32(request, base + 44).unwrap_or(0);
    let flags = read_u32(request, base + 48).unwrap_or(0);

    debug!(
        "smb ioctl ctl_code=0x{:08X} flags=0x{:08X} max_output={}",
        ctl_code, flags, max_output_response
    );

    match ctl_code {
        FSCTL_VALIDATE_NEGOTIATE_INFO => {
            let mut out = Vec::with_capacity(24);
            out.extend_from_slice(&(SMB2_GLOBAL_CAP_DFS | SMB2_GLOBAL_CAP_LARGE_MTU).to_le_bytes());
            out.extend_from_slice(&SMB2_SERVER_GUID);
            out.extend_from_slice(&SMB2_NEGOTIATE_SIGNING_ENABLED.to_le_bytes());
            out.extend_from_slice(&SMB2_NEGOTIATE_DIALECT_0210.to_le_bytes());
            (
                SMB2_STATUS_SUCCESS,
                build_ioctl_response_body(ctl_code, out.as_slice(), 0),
            )
        }
        FSCTL_QUERY_NETWORK_INTERFACE_INFO => (
            SMB2_STATUS_SUCCESS,
            build_ioctl_response_body(ctl_code, &[], 0),
        ),
        FSCTL_DFS_GET_REFERRALS => (
            SMB2_STATUS_SUCCESS,
            build_ioctl_response_body(ctl_code, &[], 0),
        ),
        _ => (SMB2_STATUS_NOT_SUPPORTED, build_not_supported_error_body()),
    }
}

async fn handle_query_directory(
    request: &[u8],
    header: &Smb2HeaderView,
    state: &mut ConnectionState,
    gateway: &SharedGateway,
) -> (u32, Vec<u8>) {
    if !state.has_session(header.session_id) {
        return (
            SMB2_STATUS_USER_SESSION_DELETED,
            build_not_supported_error_body(),
        );
    }

    let base = SMB2_HEADER_LEN;
    if request.len() < base + 32 {
        return (
            SMB2_STATUS_INVALID_PARAMETER,
            build_not_supported_error_body(),
        );
    }

    let file_info_class = request[base + 2];
    let flags = request[base + 3];
    if file_info_class != FILE_INFORMATION_CLASS_DIRECTORY_INFORMATION
        && file_info_class != FILE_INFORMATION_CLASS_BOTH_DIRECTORY_INFORMATION
        && file_info_class != FILE_INFORMATION_CLASS_ID_BOTH_DIRECTORY_INFORMATION
    {
        debug!(
            "smb query_directory unsupported file_info_class=0x{:02X}",
            file_info_class
        );
        return (SMB2_STATUS_NOT_SUPPORTED, build_not_supported_error_body());
    }

    let file_id = match read_file_id(request, base + 8) {
        Some(v) => v,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };

    let output_limit = match read_u32(request, base + 28) {
        Some(v) => v as usize,
        None => {
            return (
                SMB2_STATUS_INVALID_PARAMETER,
                build_not_supported_error_body(),
            );
        }
    };

    let pattern_offset = read_u16(request, base + 24).unwrap_or(0) as usize;
    let pattern_length = read_u16(request, base + 26).unwrap_or(0) as usize;

    let pattern = if pattern_offset >= SMB2_HEADER_LEN && pattern_length > 0 {
        decode_utf16_from_absolute_offset(request, pattern_offset, pattern_length)
            .unwrap_or_else(|| "*".to_string())
    } else {
        "*".to_string()
    };

    let restart_scan =
        (flags & SMB2_QUERY_DIRECTORY_RESTART_SCANS) != 0 || (flags & SMB2_QUERY_DIRECTORY_REOPEN) != 0;

    let (fragment, object_key, resume_index) = {
        let Some(handle) = state.handles.get_mut(&file_id) else {
            return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
        };

        if handle.session_id != header.session_id || handle.tree_id != header.tree_id {
            return (SMB2_STATUS_INVALID_HANDLE, build_not_supported_error_body());
        }
        if !handle.is_dir {
            return (
                SMB2_STATUS_NOT_A_DIRECTORY,
                build_not_supported_error_body(),
            );
        }

        if restart_scan
            || handle.dir_query_pattern != pattern
            || handle.dir_query_info_class != file_info_class
        {
            handle.dir_query_cursor = 0;
            handle.dir_query_pattern = pattern.clone();
            handle.dir_query_info_class = file_info_class;
        }

        (
            handle.fragment.clone(),
            handle.object_key.clone(),
            handle.dir_query_cursor,
        )
    };

    let path = mount_path(&fragment, &object_key);
    let mut entries = match gateway.list_dir(&path).await {
        Ok(v) => v,
        Err(e) => return (app_error_to_status(e), build_not_supported_error_body()),
    };

    entries.insert(
        0,
        synthetic_directory_entry(".", fragment.as_str(), object_key.as_str()),
    );
    entries.insert(
        1,
        synthetic_directory_entry(
            "..",
            fragment.as_str(),
            parent_object_key(object_key.as_str()).as_str(),
        ),
    );
    let mut existing_names = entries
        .iter()
        .map(|entry| entry.name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    for pending in pending_commit_entries_for_dir(fragment.as_str(), object_key.as_str()) {
        let lowered = pending.name.to_ascii_lowercase();
        if existing_names.insert(lowered) {
            entries.push(pending);
        }
    }

    let filtered: Vec<DirEntry> = entries
        .into_iter()
        .filter(|entry| matches_pattern(entry.name.as_str(), pattern.as_str()))
        .collect();

    if filtered.is_empty() {
        return (
            SMB2_STATUS_NO_MORE_FILES,
            build_query_directory_response_body(&[]),
        );
    }

    if resume_index >= filtered.len() {
        return (
            SMB2_STATUS_NO_MORE_FILES,
            build_query_directory_response_body(&[]),
        );
    }

    let (buffer, emitted_count) = build_directory_information_buffer(
        &filtered[resume_index..],
        output_limit,
        file_info_class,
    );
    if emitted_count == 0 {
        return (
            SMB2_STATUS_NO_MORE_FILES,
            build_query_directory_response_body(&[]),
        );
    }

    if let Some(handle) = state.handles.get_mut(&file_id) {
        handle.dir_query_cursor = resume_index.saturating_add(emitted_count);
    }

    (
        SMB2_STATUS_SUCCESS,
        build_query_directory_response_body(&buffer),
    )
}

fn build_framed_smb2_response(
    req: &Smb2HeaderView,
    status: u32,
    tree_id: u32,
    session_id: u64,
    body: &[u8],
    signing_key: Option<[u8; 16]>,
) -> Vec<u8> {
    let mut header = [0u8; SMB2_HEADER_LEN];
    header[0..4].copy_from_slice(&SMB2_MAGIC);
    header[4..6].copy_from_slice(&64u16.to_le_bytes());

    let credit_charge = req.credit_charge.max(1);
    header[6..8].copy_from_slice(&credit_charge.to_le_bytes());
    header[8..12].copy_from_slice(&status.to_le_bytes());
    header[12..14].copy_from_slice(&req.command.to_le_bytes());

    let credit_response = if req.command == SMB2_COMMAND_NEGOTIATE {
        1
    } else {
        req.credit_request.max(1)
    };
    header[14..16].copy_from_slice(&credit_response.to_le_bytes());

    let mut flags = SMB2_SERVER_TO_REDIR_FLAG;
    if signing_key.is_some() {
        flags |= SMB2_FLAGS_SIGNED;
    }
    header[16..20].copy_from_slice(&flags.to_le_bytes());
    header[24..32].copy_from_slice(&req.message_id.to_le_bytes());
    header[32..36].copy_from_slice(&0u32.to_le_bytes());
    header[36..40].copy_from_slice(&tree_id.to_le_bytes());
    header[40..48].copy_from_slice(&session_id.to_le_bytes());

    let mut payload = Vec::with_capacity(header.len() + body.len());
    payload.extend_from_slice(&header);
    payload.extend_from_slice(body);
    if let Some(key) = signing_key {
        payload[48..64].fill(0);
        let signature = smb2_hmac_sha256_signature(&payload, &key);
        payload[48..64].copy_from_slice(&signature);
    }

    let payload_len = payload.len();
    let mut framed = Vec::with_capacity(NETBIOS_HEADER_LEN + payload_len);
    framed.push(0x00);
    framed.push(((payload_len >> 16) & 0xFF) as u8);
    framed.push(((payload_len >> 8) & 0xFF) as u8);
    framed.push((payload_len & 0xFF) as u8);
    framed.extend_from_slice(&payload);
    framed
}

fn build_not_supported_error_body() -> Vec<u8> {
    let mut body = vec![0u8; 8];
    body[0..2].copy_from_slice(&9u16.to_le_bytes());
    body
}

fn build_simple_response_body() -> Vec<u8> {
    let mut body = vec![0u8; 4];
    body[0..2].copy_from_slice(&4u16.to_le_bytes());
    body
}

fn build_tree_connect_response_body(share_type: u8) -> Vec<u8> {
    let mut body = vec![0u8; 16];
    body[0..2].copy_from_slice(&16u16.to_le_bytes());
    body[2] = share_type;
    body[12..16].copy_from_slice(&0x001F_01FFu32.to_le_bytes());
    body
}

fn build_session_setup_response_body(session_flags: u16, security_blob: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(8 + security_blob.len());
    body.extend_from_slice(&9u16.to_le_bytes());
    body.extend_from_slice(&session_flags.to_le_bytes());
    let security_offset = if security_blob.is_empty() {
        0u16
    } else {
        (SMB2_HEADER_LEN + 8) as u16
    };
    body.extend_from_slice(&security_offset.to_le_bytes());
    body.extend_from_slice(&(security_blob.len() as u16).to_le_bytes());
    body.extend_from_slice(security_blob);
    body
}

fn build_negotiate_response_body(
    dialect: u16,
    selection: NegotiateSelection,
) -> Vec<u8> {
    let include_security_blob = false;
    let security_blob_len = if include_security_blob {
        SMB2_NEGOTIATE_SPNEGO_NTLM_BLOB.len()
    } else {
        0
    };
    let mut body = Vec::with_capacity(SMB2_NEGOTIATE_RESPONSE_FIXED_LEN + security_blob_len + 128);
    body.resize(SMB2_NEGOTIATE_RESPONSE_FIXED_LEN, 0);
    body[0..2].copy_from_slice(&65u16.to_le_bytes());
    body[2..4].copy_from_slice(&selection.signing_mode.to_le_bytes());
    body[4..6].copy_from_slice(&dialect.to_le_bytes());
    body[8..24].copy_from_slice(&SMB2_SERVER_GUID);

    let system_time = current_filetime();
    body[40..48].copy_from_slice(&system_time.to_le_bytes());
    body[48..56].copy_from_slice(&0u64.to_le_bytes());
    let mut capabilities = SMB2_GLOBAL_CAP_DFS;
    if dialect != SMB2_NEGOTIATE_DIALECT_0202 {
        capabilities |= SMB2_GLOBAL_CAP_LARGE_MTU;
    }
    body[24..28].copy_from_slice(&capabilities.to_le_bytes());

    let max_size = if dialect == SMB2_NEGOTIATE_DIALECT_0202 {
        64 * 1024u32
    } else {
        8 * 1024 * 1024u32
    };
    body[28..32].copy_from_slice(&max_size.to_le_bytes());
    body[32..36].copy_from_slice(&max_size.to_le_bytes());
    body[36..40].copy_from_slice(&max_size.to_le_bytes());
    let security_buffer_offset = if include_security_blob {
        (SMB2_HEADER_LEN + SMB2_NEGOTIATE_RESPONSE_FIXED_LEN) as u16
    } else {
        0u16
    };
    body[56..58].copy_from_slice(&security_buffer_offset.to_le_bytes());
    body[58..60].copy_from_slice(&(security_blob_len as u16).to_le_bytes());
    if include_security_blob {
        body.extend_from_slice(&SMB2_NEGOTIATE_SPNEGO_NTLM_BLOB);
    }

    if dialect == SMB2_NEGOTIATE_DIALECT_0311 {
        let mut negotiate_context_count = 0u16;
        let context_offset = align_up(
            SMB2_HEADER_LEN + SMB2_NEGOTIATE_RESPONSE_FIXED_LEN + security_blob_len,
            8,
        );
        let context_offset_in_body = context_offset.saturating_sub(SMB2_HEADER_LEN);
        while body.len() < context_offset_in_body {
            body.push(0);
        }

        let mut preauth = Vec::with_capacity(6 + SMB2_PREAUTH_SALT.len());
        preauth.extend_from_slice(&1u16.to_le_bytes());
        preauth.extend_from_slice(&(SMB2_PREAUTH_SALT.len() as u16).to_le_bytes());
        preauth.extend_from_slice(&selection.preauth_hash.to_le_bytes());
        preauth.extend_from_slice(&SMB2_PREAUTH_SALT);
        append_negotiate_context(
            &mut body,
            SMB2_NEGOTIATE_CONTEXT_PREAUTH_INTEGRITY_CAPABILITIES,
            &preauth,
        );
        negotiate_context_count = negotiate_context_count.saturating_add(1);
        let mut encryption = Vec::with_capacity(4);
        encryption.extend_from_slice(&1u16.to_le_bytes());
        encryption.extend_from_slice(&selection.encryption_cipher.to_le_bytes());
        append_negotiate_context(
            &mut body,
            SMB2_NEGOTIATE_CONTEXT_ENCRYPTION_CAPABILITIES,
            &encryption,
        );
        negotiate_context_count = negotiate_context_count.saturating_add(1);

        let mut signing = Vec::with_capacity(4);
        signing.extend_from_slice(&1u16.to_le_bytes());
        signing.extend_from_slice(&selection.signing_algorithm.to_le_bytes());
        append_negotiate_context(
            &mut body,
            SMB2_NEGOTIATE_CONTEXT_SIGNING_CAPABILITIES,
            &signing,
        );
        negotiate_context_count = negotiate_context_count.saturating_add(1);

        body[6..8].copy_from_slice(&negotiate_context_count.to_le_bytes());
        body[60..64].copy_from_slice(&(context_offset as u32).to_le_bytes());
    } else {
        body[6..8].copy_from_slice(&0u16.to_le_bytes());
        body[60..64].copy_from_slice(&0u32.to_le_bytes());
    }

    body
}

fn append_negotiate_context(body: &mut Vec<u8>, context_type: u16, data: &[u8]) {
    body.extend_from_slice(&context_type.to_le_bytes());
    body.extend_from_slice(&(data.len() as u16).to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(data);
    while (body.len() % 8) != 0 {
        body.push(0);
    }
}

fn build_create_response_body(
    file_id: [u8; 16],
    is_dir: bool,
    size_bytes: u64,
    create_action: u32,
) -> Vec<u8> {
    let mut body = vec![0u8; 88];
    body[0..2].copy_from_slice(&89u16.to_le_bytes());
    body[4..8].copy_from_slice(&create_action.to_le_bytes());
    body[40..48].copy_from_slice(&size_bytes.to_le_bytes());
    body[48..56].copy_from_slice(&size_bytes.to_le_bytes());
    let attrs = if is_dir {
        FILE_ATTRIBUTE_DIRECTORY
    } else {
        FILE_ATTRIBUTE_NORMAL
    };
    body[56..60].copy_from_slice(&attrs.to_le_bytes());
    body[64..80].copy_from_slice(&file_id);
    body
}

fn build_read_response_body(data: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(16 + data.len());
    body.extend_from_slice(&17u16.to_le_bytes());
    body.push(80u8);
    body.push(0u8);
    body.extend_from_slice(&(data.len() as u32).to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(data);
    body
}

fn build_write_response_body(written: u32) -> Vec<u8> {
    let mut body = vec![0u8; 16];
    body[0..2].copy_from_slice(&17u16.to_le_bytes());
    body[4..8].copy_from_slice(&written.to_le_bytes());
    body
}

fn build_close_response_body(size_bytes: u64, attrs: u32) -> Vec<u8> {
    let mut body = vec![0u8; 60];
    body[0..2].copy_from_slice(&60u16.to_le_bytes());
    body[40..48].copy_from_slice(&size_bytes.to_le_bytes());
    body[48..56].copy_from_slice(&size_bytes.to_le_bytes());
    body[56..60].copy_from_slice(&attrs.to_le_bytes());
    body
}

fn build_query_directory_response_body(buffer: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(8 + buffer.len());
    body.extend_from_slice(&9u16.to_le_bytes());
    let data_offset = if buffer.is_empty() {
        0u16
    } else {
        (SMB2_HEADER_LEN + 8) as u16
    };
    body.extend_from_slice(&data_offset.to_le_bytes());
    body.extend_from_slice(&(buffer.len() as u32).to_le_bytes());
    body.extend_from_slice(buffer);
    body
}

fn build_query_info_response_body(buffer: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(8 + buffer.len());
    body.extend_from_slice(&9u16.to_le_bytes());
    let data_offset = if buffer.is_empty() {
        0u16
    } else {
        (SMB2_HEADER_LEN + 8) as u16
    };
    body.extend_from_slice(&data_offset.to_le_bytes());
    body.extend_from_slice(&(buffer.len() as u32).to_le_bytes());
    body.extend_from_slice(buffer);
    body
}

fn build_ioctl_response_body(ctl_code: u32, output: &[u8], flags: u32) -> Vec<u8> {
    let mut body = vec![0u8; 48];
    body[0..2].copy_from_slice(&49u16.to_le_bytes());
    body[4..8].copy_from_slice(&ctl_code.to_le_bytes());
    let output_offset = if output.is_empty() {
        0u32
    } else {
        (SMB2_HEADER_LEN + 48) as u32
    };
    body[32..36].copy_from_slice(&output_offset.to_le_bytes());
    body[36..40].copy_from_slice(&(output.len() as u32).to_le_bytes());
    body[40..44].copy_from_slice(&flags.to_le_bytes());
    if !output.is_empty() {
        body.extend_from_slice(output);
    }
    body
}

fn build_set_info_response_body() -> Vec<u8> {
    2u16.to_le_bytes().to_vec()
}

fn build_flush_response_body() -> Vec<u8> {
    4u16.to_le_bytes().to_vec()
}

fn build_file_basic_information_buffer(metadata: &NodeMetadata, is_dir: bool) -> Vec<u8> {
    let mut out = vec![0u8; 40];
    let t = unix_secs_to_filetime(metadata.modified_at_unix);
    out[0..8].copy_from_slice(&t.to_le_bytes());
    out[8..16].copy_from_slice(&t.to_le_bytes());
    out[16..24].copy_from_slice(&t.to_le_bytes());
    out[24..32].copy_from_slice(&t.to_le_bytes());
    let attrs = if is_dir {
        FILE_ATTRIBUTE_DIRECTORY
    } else {
        FILE_ATTRIBUTE_NORMAL
    };
    out[32..36].copy_from_slice(&attrs.to_le_bytes());
    out
}

fn build_file_standard_information_buffer(
    metadata: &NodeMetadata,
    is_dir: bool,
    delete_on_close: bool,
) -> Vec<u8> {
    let mut out = vec![0u8; 24];
    out[0..8].copy_from_slice(&metadata.size_bytes.to_le_bytes());
    out[8..16].copy_from_slice(&metadata.size_bytes.to_le_bytes());
    out[16..20].copy_from_slice(&1u32.to_le_bytes());
    out[20] = if delete_on_close { 1 } else { 0 };
    out[21] = if is_dir { 1 } else { 0 };
    out
}

fn build_file_all_information_buffer(
    metadata: &NodeMetadata,
    is_dir: bool,
    delete_on_close: bool,
    object_key: &str,
) -> Vec<u8> {
    let name_utf16: Vec<u16> = object_key.encode_utf16().collect();
    let mut name_bytes = Vec::with_capacity(name_utf16.len() * 2);
    for ch in name_utf16 {
        name_bytes.extend_from_slice(&ch.to_le_bytes());
    }

    let mut out = vec![0u8; 100 + name_bytes.len()];
    let basic = build_file_basic_information_buffer(metadata, is_dir);
    let standard = build_file_standard_information_buffer(metadata, is_dir, delete_on_close);
    out[0..40].copy_from_slice(&basic);
    out[40..64].copy_from_slice(&standard);
    out[72..76].copy_from_slice(&0u32.to_le_bytes());
    out[76..80].copy_from_slice(&0u32.to_le_bytes());
    out[80..88].copy_from_slice(&0u64.to_le_bytes());
    out[88..92].copy_from_slice(&0u32.to_le_bytes());
    out[92..96].copy_from_slice(&0u32.to_le_bytes());
    out[96..100].copy_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    out[100..100 + name_bytes.len()].copy_from_slice(&name_bytes);
    out
}

fn build_file_network_open_information_buffer(metadata: &NodeMetadata, is_dir: bool) -> Vec<u8> {
    let mut out = vec![0u8; 56];
    let t = unix_secs_to_filetime(metadata.modified_at_unix);
    out[0..8].copy_from_slice(&t.to_le_bytes());
    out[8..16].copy_from_slice(&t.to_le_bytes());
    out[16..24].copy_from_slice(&t.to_le_bytes());
    out[24..32].copy_from_slice(&t.to_le_bytes());
    out[32..40].copy_from_slice(&metadata.size_bytes.to_le_bytes());
    out[40..48].copy_from_slice(&metadata.size_bytes.to_le_bytes());
    let attrs = if is_dir {
        FILE_ATTRIBUTE_DIRECTORY
    } else {
        FILE_ATTRIBUTE_NORMAL
    };
    out[48..52].copy_from_slice(&attrs.to_le_bytes());
    out
}

fn build_fs_volume_information_buffer(volume_label: &str) -> Vec<u8> {
    let label_utf16: Vec<u16> = volume_label.encode_utf16().collect();
    let mut label_bytes = Vec::with_capacity(label_utf16.len() * 2);
    for ch in label_utf16 {
        label_bytes.extend_from_slice(&ch.to_le_bytes());
    }
    let mut out = vec![0u8; 18 + label_bytes.len()];
    out[0..8].copy_from_slice(&current_filetime().to_le_bytes());
    out[8..12].copy_from_slice(&0x4E31_4653u32.to_le_bytes());
    out[12..16].copy_from_slice(&(label_bytes.len() as u32).to_le_bytes());
    out[16] = 0;
    out[18..18 + label_bytes.len()].copy_from_slice(&label_bytes);
    out
}

fn build_fs_size_information_buffer() -> Vec<u8> {
    let mut out = vec![0u8; 24];
    out[0..8].copy_from_slice(&8_388_608u64.to_le_bytes());
    out[8..16].copy_from_slice(&8_388_608u64.to_le_bytes());
    out[16..20].copy_from_slice(&8u32.to_le_bytes());
    out[20..24].copy_from_slice(&512u32.to_le_bytes());
    out
}

fn build_fs_device_information_buffer() -> Vec<u8> {
    let mut out = vec![0u8; 8];
    out[0..4].copy_from_slice(&0x0000_0007u32.to_le_bytes());
    out[4..8].copy_from_slice(&0x0000_0010u32.to_le_bytes());
    out
}

fn build_fs_full_size_information_buffer() -> Vec<u8> {
    let mut out = vec![0u8; 32];
    out[0..8].copy_from_slice(&8_388_608u64.to_le_bytes());
    out[8..16].copy_from_slice(&8_388_608u64.to_le_bytes());
    out[16..24].copy_from_slice(&8_388_608u64.to_le_bytes());
    out[24..28].copy_from_slice(&8u32.to_le_bytes());
    out[28..32].copy_from_slice(&512u32.to_le_bytes());
    out
}

fn build_fs_attribute_information_buffer(name: &str) -> Vec<u8> {
    let name_utf16: Vec<u16> = name.encode_utf16().collect();
    let mut name_bytes = Vec::with_capacity(name_utf16.len() * 2);
    for ch in name_utf16 {
        name_bytes.extend_from_slice(&ch.to_le_bytes());
    }
    let mut out = vec![0u8; 12 + name_bytes.len()];
    out[0..4].copy_from_slice(&0x0000_0040u32.to_le_bytes());
    out[4..8].copy_from_slice(&255u32.to_le_bytes());
    out[8..12].copy_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    out[12..12 + name_bytes.len()].copy_from_slice(&name_bytes);
    out
}

fn build_directory_information_buffer(
    entries: &[DirEntry],
    output_limit: usize,
    file_info_class: u8,
) -> (Vec<u8>, usize) {
    let max_len = output_limit.max(64);
    let mut out = Vec::new();
    let mut record_starts = Vec::new();
    let mut record_lens = Vec::new();
    let mut emitted_count = 0usize;

    for entry in entries.iter() {
        let name_utf16: Vec<u16> = entry.name.encode_utf16().collect();
        let mut name_bytes = Vec::with_capacity(name_utf16.len() * 2);
        for ch in name_utf16 {
            name_bytes.extend_from_slice(&ch.to_le_bytes());
        }

        let fixed_len = if file_info_class == FILE_INFORMATION_CLASS_ID_BOTH_DIRECTORY_INFORMATION {
            104usize
        } else {
            64usize
        };
        let mut record_len = fixed_len + name_bytes.len();
        let aligned_len = (record_len + 7) & !7;
        record_len = aligned_len;

        if out.len() + record_len > max_len {
            break;
        }

        let start = out.len();
        out.resize(start + record_len, 0);

        let mtime = unix_secs_to_filetime(entry.metadata.modified_at_unix);
        let eof = entry.metadata.size_bytes;
        let alloc = eof;
        let attrs = match entry.metadata.kind {
            NodeKind::Directory => FILE_ATTRIBUTE_DIRECTORY,
            NodeKind::File => FILE_ATTRIBUTE_NORMAL,
        };

        out[start + 8..start + 16].copy_from_slice(&mtime.to_le_bytes());
        out[start + 16..start + 24].copy_from_slice(&mtime.to_le_bytes());
        out[start + 24..start + 32].copy_from_slice(&mtime.to_le_bytes());
        out[start + 32..start + 40].copy_from_slice(&mtime.to_le_bytes());
        out[start + 40..start + 48].copy_from_slice(&(eof as i64).to_le_bytes());
        out[start + 48..start + 56].copy_from_slice(&(alloc as i64).to_le_bytes());
        out[start + 56..start + 60].copy_from_slice(&attrs.to_le_bytes());
        out[start + 60..start + 64].copy_from_slice(&(name_bytes.len() as u32).to_le_bytes());

        if file_info_class == FILE_INFORMATION_CLASS_ID_BOTH_DIRECTORY_INFORMATION {
            out[start + 64..start + 68].copy_from_slice(&0u32.to_le_bytes());
            out[start + 68] = 0;
            out[start + 69] = 0;

            let file_id = if entry.path.object_key.is_empty() {
                0u64
            } else {
                let mut id = 0xcbf2_9ce4_8422_2325u64;
                for b in entry.path.object_key.as_bytes() {
                    id ^= *b as u64;
                    id = id.wrapping_mul(0x1000_0000_01b3);
                }
                id
            };
            out[start + 96..start + 104].copy_from_slice(&file_id.to_le_bytes());
            out[start + 104..start + 104 + name_bytes.len()].copy_from_slice(&name_bytes);
        } else {
            out[start + 64..start + 64 + name_bytes.len()].copy_from_slice(&name_bytes);
        }

        record_starts.push(start);
        record_lens.push(record_len);
        emitted_count = emitted_count.saturating_add(1);
    }

    for idx in 0..record_starts.len() {
        let start = record_starts[idx];
        if idx + 1 < record_starts.len() {
            out[start..start + 4].copy_from_slice(&(record_lens[idx] as u32).to_le_bytes());
        } else {
            out[start..start + 4].copy_from_slice(&0u32.to_le_bytes());
        }
    }

    (out, emitted_count)
}

fn synthetic_directory_entry(name: &str, fragment: &str, object_key: &str) -> DirEntry {
    DirEntry {
        name: name.to_string(),
        path: crate::fuse::model::FsdPath {
            fragment: fragment.to_string(),
            object_key: object_key.to_string(),
        },
        metadata: NodeMetadata {
            kind: NodeKind::Directory,
            size_bytes: 0,
            modified_at_unix: now_unix_secs(),
        },
    }
}

fn parent_object_key(object_key: &str) -> String {
    let trimmed = object_key.trim_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }
    let mut parts: Vec<&str> = trimmed.split('/').collect();
    parts.pop();
    parts.join("/")
}

async fn metadata_for_handle(
    gateway: &SharedGateway,
    handle: &OpenHandleContext,
) -> Result<NodeMetadata, AppError> {
    if handle.object_key.is_empty() {
        return Ok(NodeMetadata {
            kind: NodeKind::Directory,
            size_bytes: 0,
            modified_at_unix: now_unix_secs(),
        });
    }

    let path = mount_path(&handle.fragment, &handle.object_key);
    match stat_optional(gateway, &path).await? {
        Some(v) => Ok(v),
        None => Ok(NodeMetadata {
            kind: if handle.is_dir {
                NodeKind::Directory
            } else {
                NodeKind::File
            },
            size_bytes: 0,
            modified_at_unix: now_unix_secs(),
        }),
    }
}

async fn stat_optional(
    gateway: &SharedGateway,
    mount_path: &str,
) -> Result<Option<NodeMetadata>, AppError> {
    match gateway.stat(mount_path).await {
        Ok(meta) => Ok(Some(meta)),
        Err(AppError::NotFound) => Ok(None),
        Err(e) => Err(e),
    }
}

fn mount_path(fragment: &str, object_key: &str) -> String {
    if object_key.is_empty() {
        fragment.to_string()
    } else {
        format!("{}/{}", fragment, object_key)
    }
}

fn parse_tree_connect_fragment(request: &[u8]) -> Option<String> {
    if request.len() < SMB2_HEADER_LEN + 8 {
        debug!(
            "smb tree_connect invalid length: req_len={} expected_min={}",
            request.len(),
            SMB2_HEADER_LEN + 8
        );
        return None;
    }

    let path_offset = read_u16(request, SMB2_HEADER_LEN + 4)? as usize;
    let path_length = read_u16(request, SMB2_HEADER_LEN + 6)? as usize;

    debug!(
        "smb tree_connect offsets: path_offset={} path_length={} req_len={}",
        path_offset,
        path_length,
        request.len()
    );

    if path_offset < SMB2_HEADER_LEN || path_length == 0 {
        debug!(
            "smb tree_connect rejected offset/length: path_offset={} path_length={}",
            path_offset, path_length
        );
        return None;
    }

    let path = decode_utf16_from_absolute_offset(request, path_offset, path_length)?;
    let segments: Vec<&str> = path
        .split(['\\', '/'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if segments.is_empty() {
        return None;
    }
    let candidate = if segments.len() >= 2 {
        segments[1].to_string()
    } else {
        segments[0].to_string()
    };

    debug!(
        "smb tree_connect raw_path='{}' segments={:?} candidate='{}'",
        path, segments, candidate
    );

    if candidate.is_empty() {
        return None;
    }

    Some(candidate)
}

async fn resolve_tree_connect_fragment(
    candidate: &str,
    session_scope: Option<&str>,
) -> Option<String> {
    let requested = candidate.trim();
    if requested.is_empty() {
        return None;
    }

    if requested.eq_ignore_ascii_case("IPC$") {
        return Some("IPC$".to_string());
    }

    if let Some(scope) = session_scope {
        let scoped = scope.trim().to_ascii_lowercase();
        if scoped.is_empty() {
            return None;
        }
        if requested.eq_ignore_ascii_case("N1") {
            return Some(scoped);
        }
        let requested_hash = fragment_identifier_to_hash(requested)?;
        if requested_hash == scoped {
            return Some(scoped);
        }
        return None;
    }

    if requested.eq_ignore_ascii_case("N1") {
        return None;
    }

    fragment_identifier_to_hash(requested)
}

fn parse_negotiate_request_dialects(request: &[u8]) -> Option<Vec<u16>> {
    if request.len() < SMB2_HEADER_LEN + 36 {
        return None;
    }

    let structure_size = read_u16(request, SMB2_HEADER_LEN)?;
    if structure_size < 36 {
        return None;
    }

    let dialect_count = read_u16(request, SMB2_HEADER_LEN + 2)? as usize;
    let dialects_offset = SMB2_HEADER_LEN + 36;
    let dialects_byte_len = dialect_count.checked_mul(2)?;
    let dialects_end = dialects_offset.checked_add(dialects_byte_len)?;
    if dialects_end > request.len() {
        return None;
    }

    let mut dialects = Vec::with_capacity(dialect_count);
    let mut idx = dialects_offset;
    while idx + 1 < dialects_end {
        dialects.push(u16::from_le_bytes([request[idx], request[idx + 1]]));
        idx += 2;
    }
    Some(dialects)
}

fn parse_session_setup_security_buffer(request: &[u8]) -> Option<&[u8]> {
    let base = SMB2_HEADER_LEN;
    if request.len() < base + 16 {
        return Some(&[]);
    }

    let buffer_offset = read_u16(request, base + 12).unwrap_or(0) as usize;
    let buffer_length = read_u16(request, base + 14).unwrap_or(0) as usize;
    if buffer_length == 0 {
        return Some(&[]);
    }

    let end = buffer_offset.checked_add(buffer_length)?;
    if buffer_offset < SMB2_HEADER_LEN || end > request.len() {
        return None;
    }

    Some(&request[buffer_offset..end])
}

fn parse_ntlm_message_type(blob: &[u8]) -> Option<u32> {
    let base = find_ntlmssp_signature(blob)?;
    read_u32(blob, base + 8)
}

fn parse_negotiate_request_meta(request: &[u8]) -> Option<NegotiateRequestMeta> {
    if request.len() < SMB2_HEADER_LEN + 36 {
        return None;
    }
    let security_mode = read_u16(request, SMB2_HEADER_LEN + 4)?;
    let capabilities = read_u32(request, SMB2_HEADER_LEN + 8)?;
    let client_guid: [u8; 16] = request
        .get(SMB2_HEADER_LEN + 12..SMB2_HEADER_LEN + 28)?
        .try_into()
        .ok()?;
    let context_offset = read_u32(request, SMB2_HEADER_LEN + 28)?;
    let context_count = read_u16(request, SMB2_HEADER_LEN + 32)?;
    let (context_types, preauth_algorithms, encryption_ciphers, signing_algorithms) =
        parse_negotiate_context_capabilities(request, context_offset as usize, context_count);
    Some(NegotiateRequestMeta {
        security_mode,
        capabilities,
        context_offset,
        context_count,
        client_guid,
        context_types,
        preauth_algorithms,
        encryption_ciphers,
        signing_algorithms,
    })
}

fn parse_negotiate_context_capabilities(
    request: &[u8],
    context_offset: usize,
    context_count: u16,
) -> (Vec<u16>, Vec<u16>, Vec<u16>, Vec<u16>) {
    if context_count == 0 || context_offset < SMB2_HEADER_LEN || context_offset >= request.len() {
        return (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    }

    let mut cursor = context_offset;
    let mut context_types = Vec::with_capacity(context_count as usize);
    let mut preauth_algorithms = Vec::new();
    let mut encryption_ciphers = Vec::new();
    let mut signing_algorithms = Vec::new();
    for _ in 0..context_count {
        if cursor + 8 > request.len() {
            break;
        }

        let ctx_type = match read_u16(request, cursor) {
            Some(v) => v,
            None => break,
        };
        let data_len = match read_u16(request, cursor + 2) {
            Some(v) => v as usize,
            None => break,
        };

        let data_start = cursor + 8;
        let data_end = match data_start.checked_add(data_len) {
            Some(v) => v,
            None => break,
        };
        if data_end > request.len() {
            break;
        }

        context_types.push(ctx_type);
        let data = &request[data_start..data_end];
        match ctx_type {
            SMB2_NEGOTIATE_CONTEXT_PREAUTH_INTEGRITY_CAPABILITIES => {
                if data.len() >= 4 {
                    let algo_count = u16::from_le_bytes([data[0], data[1]]) as usize;
                    let mut pos = 4usize;
                    for _ in 0..algo_count {
                        if pos + 1 >= data.len() {
                            break;
                        }
                        preauth_algorithms.push(u16::from_le_bytes([data[pos], data[pos + 1]]));
                        pos += 2;
                    }
                }
            }
            SMB2_NEGOTIATE_CONTEXT_ENCRYPTION_CAPABILITIES => {
                if data.len() >= 2 {
                    let cipher_count = u16::from_le_bytes([data[0], data[1]]) as usize;
                    let mut pos = 2usize;
                    for _ in 0..cipher_count {
                        if pos + 1 >= data.len() {
                            break;
                        }
                        encryption_ciphers.push(u16::from_le_bytes([data[pos], data[pos + 1]]));
                        pos += 2;
                    }
                }
            }
            SMB2_NEGOTIATE_CONTEXT_SIGNING_CAPABILITIES => {
                if data.len() >= 2 {
                    let signing_count = u16::from_le_bytes([data[0], data[1]]) as usize;
                    let mut pos = 2usize;
                    for _ in 0..signing_count {
                        if pos + 1 >= data.len() {
                            break;
                        }
                        signing_algorithms.push(u16::from_le_bytes([data[pos], data[pos + 1]]));
                        pos += 2;
                    }
                }
            }
            _ => {}
        }

        cursor = align_up(data_end, 8);
        if cursor > request.len() {
            break;
        }
    }

    (
        context_types,
        preauth_algorithms,
        encryption_ciphers,
        signing_algorithms,
    )
}

fn select_supported_dialect(client_dialects: &[u16]) -> Option<u16> {
    if client_dialects
        .iter()
        .any(|d| *d == SMB2_NEGOTIATE_DIALECT_0210)
    {
        return Some(SMB2_NEGOTIATE_DIALECT_0210);
    }
    if client_dialects
        .iter()
        .any(|d| *d == SMB2_NEGOTIATE_DIALECT_0202)
    {
        return Some(SMB2_NEGOTIATE_DIALECT_0202);
    }
    if client_dialects
        .iter()
        .any(|d| *d == SMB2_NEGOTIATE_DIALECT_0311)
    {
        return Some(SMB2_NEGOTIATE_DIALECT_0311);
    }
    if client_dialects
        .iter()
        .any(|d| *d == SMB2_NEGOTIATE_DIALECT_0302)
    {
        return Some(SMB2_NEGOTIATE_DIALECT_0302);
    }
    if client_dialects
        .iter()
        .any(|d| *d == SMB2_NEGOTIATE_DIALECT_0300)
    {
        return Some(SMB2_NEGOTIATE_DIALECT_0300);
    }
    None
}

fn negotiate_selection(
    negotiate_meta: Option<&NegotiateRequestMeta>,
    require_signing: bool,
) -> NegotiateSelection {
    let signing_mode = if require_signing {
        SMB2_NEGOTIATE_SIGNING_ENABLED | SMB2_NEGOTIATE_SIGNING_REQUIRED
    } else {
        SMB2_NEGOTIATE_SIGNING_ENABLED
    };

    let preauth_hash = if negotiate_meta
        .map(|m| m.preauth_algorithms.contains(&SMB2_PREAUTH_HASH_ALGO_SHA512))
        .unwrap_or(true)
    {
        SMB2_PREAUTH_HASH_ALGO_SHA512
    } else {
        negotiate_meta
            .and_then(|m| m.preauth_algorithms.first().copied())
            .unwrap_or(SMB2_PREAUTH_HASH_ALGO_SHA512)
    };

    let encryption_preference = [
        SMB2_ENCRYPTION_CIPHER_AES128_GCM,
        SMB2_ENCRYPTION_CIPHER_AES256_GCM,
        SMB2_ENCRYPTION_CIPHER_AES128_CCM,
        SMB2_ENCRYPTION_CIPHER_AES256_CCM,
    ];
    let encryption_cipher = negotiate_meta
        .and_then(|m| {
            encryption_preference
                .iter()
                .copied()
                .find(|c| m.encryption_ciphers.contains(c))
        })
        .unwrap_or(SMB2_ENCRYPTION_CIPHER_AES128_GCM);

    let signing_preference = [
        SMB2_SIGNING_ALGORITHM_AES_GMAC,
        SMB2_SIGNING_ALGORITHM_AES_CMAC,
        SMB2_SIGNING_ALGORITHM_HMAC_SHA256,
    ];
    let signing_algorithm = negotiate_meta
        .and_then(|m| {
            signing_preference
                .iter()
                .copied()
                .find(|a| m.signing_algorithms.contains(a))
        })
        .unwrap_or(SMB2_SIGNING_ALGORITHM_AES_GMAC);

    NegotiateSelection {
        signing_mode,
        preauth_hash,
        encryption_cipher,
        signing_algorithm,
    }
}

fn align_up(value: usize, align: usize) -> usize {
    if align == 0 {
        return value;
    }
    let rem = value % align;
    if rem == 0 {
        value
    } else {
        value + (align - rem)
    }
}

fn decode_utf16_from_absolute_offset(
    request: &[u8],
    absolute_offset: usize,
    byte_len: usize,
) -> Option<String> {
    if absolute_offset < SMB2_HEADER_LEN || byte_len == 0 || (byte_len % 2) != 0 {
        return None;
    }
    let end = absolute_offset.checked_add(byte_len)?;
    if end > request.len() {
        return None;
    }

    let mut utf16 = Vec::with_capacity(byte_len / 2);
    let mut idx = absolute_offset;
    while idx < end {
        utf16.push(u16::from_le_bytes([request[idx], request[idx + 1]]));
        idx += 2;
    }

    Some(String::from_utf16_lossy(&utf16))
}

fn decode_utf16_bytes(bytes: &[u8]) -> Option<String> {
    if (bytes.len() % 2) != 0 {
        return None;
    }
    let mut utf16 = Vec::with_capacity(bytes.len() / 2);
    let mut idx = 0usize;
    while idx < bytes.len() {
        utf16.push(u16::from_le_bytes([bytes[idx], bytes[idx + 1]]));
        idx += 2;
    }
    Some(String::from_utf16_lossy(&utf16))
}

fn normalize_object_key(raw: &str) -> String {
    raw.replace('\\', "/").trim_matches('/').to_string()
}

fn read_u16(buf: &[u8], offset: usize) -> Option<u16> {
    let bytes: [u8; 2] = buf.get(offset..offset + 2)?.try_into().ok()?;
    Some(u16::from_le_bytes(bytes))
}

fn read_u32(buf: &[u8], offset: usize) -> Option<u32> {
    let bytes: [u8; 4] = buf.get(offset..offset + 4)?.try_into().ok()?;
    Some(u32::from_le_bytes(bytes))
}

fn read_u64(buf: &[u8], offset: usize) -> Option<u64> {
    let bytes: [u8; 8] = buf.get(offset..offset + 8)?.try_into().ok()?;
    Some(u64::from_le_bytes(bytes))
}

fn read_file_id(buf: &[u8], offset: usize) -> Option<[u8; 16]> {
    buf.get(offset..offset + 16)?.try_into().ok()
}

fn current_filetime() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs().saturating_add(FILETIME_UNIX_EPOCH_DELTA_SECS);
    let nanos_100 = (u64::from(now.subsec_nanos())) / 100;
    secs.saturating_mul(10_000_000).saturating_add(nanos_100)
}

fn unix_secs_to_filetime(unix_secs: i64) -> u64 {
    if unix_secs <= 0 {
        return 0;
    }
    let secs = (unix_secs as u64).saturating_add(FILETIME_UNIX_EPOCH_DELTA_SECS);
    secs.saturating_mul(10_000_000)
}

fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn matches_pattern(name: &str, pattern: &str) -> bool {
    if pattern.is_empty() || pattern == "*" || pattern == "*.*" {
        return true;
    }

    if !pattern.contains('*') {
        return name.eq_ignore_ascii_case(pattern);
    }

    if pattern.starts_with('*') && pattern.ends_with('*') && pattern.len() > 2 {
        let needle = &pattern[1..pattern.len() - 1];
        return name.contains(needle);
    }

    if let Some(suffix) = pattern.strip_prefix('*') {
        return name.ends_with(suffix);
    }

    if let Some(prefix) = pattern.strip_suffix('*') {
        return name.starts_with(prefix);
    }

    true
}

fn app_error_to_status(err: AppError) -> u32 {
    match err {
        AppError::NotFound => SMB2_STATUS_OBJECT_NAME_NOT_FOUND,
        AppError::Conflict(message) => {
            if message
                .to_ascii_lowercase()
                .contains("directory is not empty")
            {
                SMB2_STATUS_DIRECTORY_NOT_EMPTY
            } else {
                SMB2_STATUS_OBJECT_NAME_COLLISION
            }
        }
        AppError::BadRequest(_) => SMB2_STATUS_INVALID_PARAMETER,
        AppError::RangeNotSatisfiable => SMB2_STATUS_INVALID_PARAMETER,
        AppError::PayloadTooLarge(_) => SMB2_STATUS_INVALID_PARAMETER,
        AppError::Unauthorized => SMB2_STATUS_ACCESS_DENIED,
        AppError::Forbidden(_) => SMB2_STATUS_ACCESS_DENIED,
        AppError::InsufficientStorage => SMB2_STATUS_DISK_FULL,
        AppError::Timeout(_) => SMB2_STATUS_IO_TIMEOUT,
        AppError::Io(_, _) => SMB2_STATUS_UNSUCCESSFUL,
        AppError::InternalServerError(_) => SMB2_STATUS_UNSUCCESSFUL,
        AppError::Multipart(_) => SMB2_STATUS_INVALID_PARAMETER,
    }
}

fn hex_prefix(bytes: &[u8], max_len: usize) -> String {
    let take = bytes.len().min(max_len);
    let mut out = String::with_capacity(take.saturating_mul(3));
    for (idx, b) in bytes.iter().take(take).enumerate() {
        if idx > 0 {
            out.push(' ');
        }
        out.push_str(format!("{:02X}", b).as_str());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_smb2_header(command: u16, message_id: u64, tree_id: u32, session_id: u64) -> Vec<u8> {
        let mut req = vec![0u8; SMB2_HEADER_LEN];
        req[0..4].copy_from_slice(&SMB2_MAGIC);
        req[4..6].copy_from_slice(&64u16.to_le_bytes());
        req[6..8].copy_from_slice(&1u16.to_le_bytes());
        req[12..14].copy_from_slice(&command.to_le_bytes());
        req[14..16].copy_from_slice(&1u16.to_le_bytes());
        req[24..32].copy_from_slice(&message_id.to_le_bytes());
        req[36..40].copy_from_slice(&tree_id.to_le_bytes());
        req[40..48].copy_from_slice(&session_id.to_le_bytes());
        req
    }

    fn netbios_frame(payload: &[u8]) -> Vec<u8> {
        let len = payload.len();
        let mut out = Vec::with_capacity(4 + len);
        out.push(0x00);
        out.push(((len >> 16) & 0xFF) as u8);
        out.push(((len >> 8) & 0xFF) as u8);
        out.push((len & 0xFF) as u8);
        out.extend_from_slice(payload);
        out
    }

    async fn send_req(stream: &mut TcpStream, payload: &[u8]) -> Vec<u8> {
        stream
            .write_all(&netbios_frame(payload))
            .await
            .expect("write request");

        let mut nb = [0u8; 4];
        stream
            .read_exact(&mut nb)
            .await
            .expect("read netbios header");
        let resp_len = ((nb[1] as usize) << 16) | ((nb[2] as usize) << 8) | (nb[3] as usize);

        let mut response = vec![0u8; resp_len];
        stream
            .read_exact(&mut response)
            .await
            .expect("read response body");
        response
    }

    fn response_status(response: &[u8]) -> u32 {
        u32::from_le_bytes([response[8], response[9], response[10], response[11]])
    }

    fn response_session_id(response: &[u8]) -> u64 {
        u64::from_le_bytes(response[40..48].try_into().expect("session id bytes"))
    }

    fn response_tree_id(response: &[u8]) -> u32 {
        u32::from_le_bytes(response[36..40].try_into().expect("tree id bytes"))
    }

    fn tree_connect_body(unc: &str) -> Vec<u8> {
        let encoded: Vec<u16> = unc.encode_utf16().collect();
        let mut name_bytes = Vec::with_capacity(encoded.len() * 2);
        for ch in encoded {
            name_bytes.extend_from_slice(&ch.to_le_bytes());
        }

        let mut body = vec![0u8; 8 + name_bytes.len()];
        body[0..2].copy_from_slice(&9u16.to_le_bytes());
        body[2..4].copy_from_slice(&0u16.to_le_bytes());
        body[4..6].copy_from_slice(&(SMB2_HEADER_LEN as u16 + 8u16).to_le_bytes());
        body[6..8].copy_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        body[8..].copy_from_slice(&name_bytes);
        body
    }

    fn session_setup_request(message_id: u64, session_id: u64, security_blob: &[u8]) -> Vec<u8> {
        let mut req = build_smb2_header(SMB2_COMMAND_SESSION_SETUP, message_id, 0, session_id);
        let mut body = vec![0u8; 24 + security_blob.len()];
        body[0..2].copy_from_slice(&25u16.to_le_bytes());
        body[2] = 0;
        body[3] = SMB2_NEGOTIATE_SIGNING_ENABLED as u8;
        body[4..8].copy_from_slice(&0u32.to_le_bytes());
        body[8..12].copy_from_slice(&0u32.to_le_bytes());
        let security_offset = if security_blob.is_empty() {
            0u16
        } else {
            (SMB2_HEADER_LEN + 24) as u16
        };
        body[12..14].copy_from_slice(&security_offset.to_le_bytes());
        body[14..16].copy_from_slice(&(security_blob.len() as u16).to_le_bytes());
        body[16..24].copy_from_slice(&0u64.to_le_bytes());
        if !security_blob.is_empty() {
            body[24..24 + security_blob.len()].copy_from_slice(security_blob);
        }
        req.extend_from_slice(&body);
        req
    }

    fn negotiate_body(dialects: &[u16]) -> Vec<u8> {
        let mut body = vec![0u8; 36 + (dialects.len() * 2)];
        body[0..2].copy_from_slice(&36u16.to_le_bytes());
        body[2..4].copy_from_slice(&(dialects.len() as u16).to_le_bytes());
        body[4..6].copy_from_slice(&SMB2_NEGOTIATE_SIGNING_ENABLED.to_le_bytes());
        let mut idx = 36usize;
        for dialect in dialects {
            body[idx..idx + 2].copy_from_slice(&dialect.to_le_bytes());
            idx += 2;
        }
        body
    }

    fn response_session_setup_security_length(response: &[u8]) -> u16 {
        let body_offset = SMB2_HEADER_LEN;
        u16::from_le_bytes([response[body_offset + 6], response[body_offset + 7]])
    }

    fn build_test_ntlmv2_response(
        username: &str,
        domain: &str,
        password: &str,
        challenge: &[u8; 8],
        blob: &[u8],
    ) -> Vec<u8> {
        let nt_hash = nt_hash(password);
        let identity = format!("{}{}", username.to_uppercase(), domain);
        let ntlm_v2_hash = hmac_md5(&nt_hash, &utf16le_bytes(identity.as_str())).expect("ntlmv2 hash");
        let mut proof_input = Vec::with_capacity(8 + blob.len());
        proof_input.extend_from_slice(challenge);
        proof_input.extend_from_slice(blob);
        let nt_proof = hmac_md5(&ntlm_v2_hash, &proof_input).expect("nt proof");
        let mut out = Vec::with_capacity(16 + blob.len());
        out.extend_from_slice(&nt_proof);
        out.extend_from_slice(blob);
        out
    }

    #[test]
    fn tree_connect_fragment_parses_share_segment() {
        let mut req = build_smb2_header(SMB2_COMMAND_TREE_CONNECT, 1, 0, 1);
        req.extend_from_slice(&tree_connect_body(r"\\192.168.1.10\ward-a\folder\file.bin"));
        let fragment = parse_tree_connect_fragment(&req).expect("fragment");
        assert_eq!(fragment, "ward-a");
    }

    #[test]
    fn smb1_negotiate_compat_responds_with_smb2_wildcard() {
        let mut req = vec![0u8; 35];
        req[0..4].copy_from_slice(&SMB1_MAGIC);
        req[4] = SMB1_COMMAND_NEGOTIATE;

        let resp = process_smb1_negotiate_compat(&req).expect("smb1 compat response");
        assert_eq!(&resp[4..8], &SMB2_MAGIC);

        let body_offset = 4 + SMB2_HEADER_LEN;
        let dialect = u16::from_le_bytes([resp[body_offset + 4], resp[body_offset + 5]]);
        assert_eq!(dialect, SMB2_NEGOTIATE_DIALECT_WILDCARD);
    }

    #[tokio::test]
    async fn smb_loop_supports_negotiate_session_tree_and_echo() {
        let config = SmbConfig {
            enabled: true,
            bind_addr: "127.0.0.1".to_string(),
            smb_port: 0,
            max_sessions: 8,
            max_frame_bytes: 1024 * 1024,
            async_close_commit: false,
            async_close_threshold_bytes: 64 * 1024 * 1024,
            auth_username: None,
            auth_password: None,
            auth_domain: None,
            require_signing: false,
        };

        let (addr, join) = spawn_listener(config).await.expect("spawn listener");
        let mut stream = TcpStream::connect(addr).await.expect("connect");

        let mut negotiate_req = build_smb2_header(SMB2_COMMAND_NEGOTIATE, 1, 0, 0);
        negotiate_req.extend_from_slice(&negotiate_body(&[
            SMB2_NEGOTIATE_DIALECT_0202,
            SMB2_NEGOTIATE_DIALECT_0210,
        ]));
        let negotiate_resp = send_req(&mut stream, &negotiate_req).await;
        assert_eq!(response_status(&negotiate_resp), SMB2_STATUS_SUCCESS);

        let session_req = build_smb2_header(SMB2_COMMAND_SESSION_SETUP, 2, 0, 0);
        let session_resp = send_req(&mut stream, &session_req).await;
        assert_eq!(response_status(&session_resp), SMB2_STATUS_SUCCESS);
        let session_id = response_session_id(&session_resp);
        assert!(session_id > 0);

        let mut tree_req = build_smb2_header(SMB2_COMMAND_TREE_CONNECT, 3, 0, session_id);
        tree_req.extend_from_slice(&tree_connect_body(r"\\127.0.0.1\my-fragment"));
        let tree_resp = send_req(&mut stream, &tree_req).await;
        assert_eq!(response_status(&tree_resp), SMB2_STATUS_SUCCESS);
        let tree_id = response_tree_id(&tree_resp);
        assert!(tree_id > 0);

        let echo_req = build_smb2_header(SMB2_COMMAND_ECHO, 4, tree_id, session_id);
        let echo_resp = send_req(&mut stream, &echo_req).await;
        assert_eq!(response_status(&echo_resp), SMB2_STATUS_SUCCESS);

        let td_req = build_smb2_header(SMB2_COMMAND_TREE_DISCONNECT, 5, tree_id, session_id);
        let td_resp = send_req(&mut stream, &td_req).await;
        assert_eq!(response_status(&td_resp), SMB2_STATUS_SUCCESS);

        let logoff_req = build_smb2_header(SMB2_COMMAND_LOGOFF, 6, 0, session_id);
        let logoff_resp = send_req(&mut stream, &logoff_req).await;
        assert_eq!(response_status(&logoff_resp), SMB2_STATUS_SUCCESS);

        join.abort();
    }

    #[tokio::test]
    async fn smb_tree_connect_without_session_is_rejected() {
        let config = SmbConfig {
            enabled: true,
            bind_addr: "127.0.0.1".to_string(),
            smb_port: 0,
            max_sessions: 8,
            max_frame_bytes: 1024 * 1024,
            async_close_commit: false,
            async_close_threshold_bytes: 64 * 1024 * 1024,
            auth_username: None,
            auth_password: None,
            auth_domain: None,
            require_signing: false,
        };

        let (addr, join) = spawn_listener(config).await.expect("spawn listener");
        let mut stream = TcpStream::connect(addr).await.expect("connect");

        let mut req = build_smb2_header(SMB2_COMMAND_TREE_CONNECT, 11, 0, 44);
        req.extend_from_slice(&tree_connect_body(r"\\127.0.0.1\fragment"));
        let resp = send_req(&mut stream, &req).await;
        assert_eq!(response_status(&resp), SMB2_STATUS_USER_SESSION_DELETED);

        join.abort();
    }

    #[tokio::test]
    async fn smb_session_setup_supports_ntlm_challenge_then_auth_completion() {
        let config = SmbConfig {
            enabled: true,
            bind_addr: "127.0.0.1".to_string(),
            smb_port: 0,
            max_sessions: 8,
            max_frame_bytes: 1024 * 1024,
            async_close_commit: false,
            async_close_threshold_bytes: 64 * 1024 * 1024,
            auth_username: None,
            auth_password: None,
            auth_domain: None,
            require_signing: false,
        };

        let (addr, join) = spawn_listener(config).await.expect("spawn listener");
        let mut stream = TcpStream::connect(addr).await.expect("connect");

        let type1 = [b'N', b'T', b'L', b'M', b'S', b'S', b'P', 0, 1, 0, 0, 0];
        let setup1 = session_setup_request(1, 0, &type1);
        let resp1 = send_req(&mut stream, &setup1).await;
        assert_eq!(
            response_status(&resp1),
            SMB2_STATUS_MORE_PROCESSING_REQUIRED
        );
        let session_id = response_session_id(&resp1);
        assert!(session_id > 0);
        assert!(response_session_setup_security_length(&resp1) > 0);

        let type3 = [b'N', b'T', b'L', b'M', b'S', b'S', b'P', 0, 3, 0, 0, 0];
        let setup2 = session_setup_request(2, session_id, &type3);
        let resp2 = send_req(&mut stream, &setup2).await;
        assert_eq!(response_status(&resp2), SMB2_STATUS_SUCCESS);
        assert_eq!(response_session_id(&resp2), session_id);
        assert!(response_session_setup_security_length(&resp2) > 0);

        join.abort();
    }

    #[tokio::test]
    async fn smb_session_setup_without_blob_falls_back_to_guest_success() {
        let config = SmbConfig {
            enabled: true,
            bind_addr: "127.0.0.1".to_string(),
            smb_port: 0,
            max_sessions: 8,
            max_frame_bytes: 1024 * 1024,
            async_close_commit: false,
            async_close_threshold_bytes: 64 * 1024 * 1024,
            auth_username: None,
            auth_password: None,
            auth_domain: None,
            require_signing: false,
        };

        let (addr, join) = spawn_listener(config).await.expect("spawn listener");
        let mut stream = TcpStream::connect(addr).await.expect("connect");

        let setup = session_setup_request(1, 0, &[]);
        let resp = send_req(&mut stream, &setup).await;
        assert_eq!(response_status(&resp), SMB2_STATUS_SUCCESS);
        assert!(response_session_id(&resp) > 0);
        assert_eq!(response_session_setup_security_length(&resp), 0);

        join.abort();
    }

    #[tokio::test]
    async fn smb_session_setup_without_blob_is_rejected_when_auth_is_configured() {
        let config = SmbConfig {
            enabled: true,
            bind_addr: "127.0.0.1".to_string(),
            smb_port: 0,
            max_sessions: 8,
            max_frame_bytes: 1024 * 1024,
            async_close_commit: false,
            async_close_threshold_bytes: 64 * 1024 * 1024,
            auth_username: Some("n1user".to_string()),
            auth_password: Some("N1Secret!234".to_string()),
            auth_domain: Some("N1".to_string()),
            require_signing: true,
        };

        let (addr, join) = spawn_listener(config).await.expect("spawn listener");
        let mut stream = TcpStream::connect(addr).await.expect("connect");

        let setup = session_setup_request(1, 0, &[]);
        let resp = send_req(&mut stream, &setup).await;
        assert_eq!(response_status(&resp), SMB2_STATUS_LOGON_FAILURE);

        join.abort();
    }

    #[test]
    fn ntlmv2_verifier_accepts_valid_response() {
        let auth = ServerAuthConfig {
            username: "n1user".to_string(),
            password: "N1Secret!234".to_string(),
            domain: "N1".to_string(),
        };
        let challenge = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        let blob = vec![
            0x01, 0x01, 0x00, 0x00, 0, 0, 0, 0, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x01,
            0x02, 0x10, 0x20, 0x30, 0x40, 0, 0, 0, 0,
        ];
        let nt_response = build_test_ntlmv2_response(
            auth.username.as_str(),
            auth.domain.as_str(),
            auth.password.as_str(),
            &challenge,
            &blob,
        );
        let type3 = NtlmType3AuthData {
            username: auth.username.clone(),
            domain: auth.domain.clone(),
            nt_response,
        };

        let signing_key = verify_ntlmv2_auth(&auth, &challenge, &type3);
        assert!(signing_key.is_some());
    }

    #[test]
    fn ntlmv2_verifier_rejects_invalid_response() {
        let auth = ServerAuthConfig {
            username: "n1user".to_string(),
            password: "N1Secret!234".to_string(),
            domain: "N1".to_string(),
        };
        let challenge = [0x21, 0x22, 0x23, 0x24, 0x31, 0x32, 0x33, 0x34];
        let blob = vec![
            0x01, 0x01, 0x00, 0x00, 0, 0, 0, 0, 0xBA, 0xDB, 0xEE, 0xF0, 0xAB, 0xCD, 0xEF,
            0x12, 0x05, 0x06, 0x07, 0x08, 0, 0, 0, 0,
        ];
        let mut nt_response = build_test_ntlmv2_response(
            auth.username.as_str(),
            auth.domain.as_str(),
            auth.password.as_str(),
            &challenge,
            &blob,
        );
        nt_response[0] ^= 0xFF;
        let type3 = NtlmType3AuthData {
            username: auth.username.clone(),
            domain: auth.domain.clone(),
            nt_response,
        };

        let signing_key = verify_ntlmv2_auth(&auth, &challenge, &type3);
        assert!(signing_key.is_none());
    }

    #[tokio::test]
    async fn register_open_handle_enforces_share_conflicts() {
        let shared = Arc::new(Mutex::new(GlobalSmbState::default()));
        let conn_a = 100u64;
        let conn_b = 200u64;
        let file_a = [1u8; 16];
        let file_b = [2u8; 16];

        let ok = register_open_handle(
            &shared,
            conn_a,
            file_a,
            "frag",
            "file.bin",
            FILE_SHARE_READ,
            true,
            false,
            false,
        )
        .await;
        assert!(ok.is_ok());

        let conflict = register_open_handle(
            &shared,
            conn_b,
            file_b,
            "frag",
            "file.bin",
            FILE_SHARE_READ,
            false,
            true,
            false,
        )
        .await;
        assert_eq!(conflict, Err(SMB2_STATUS_SHARING_VIOLATION));

        let compatible = register_open_handle(
            &shared,
            conn_b,
            file_b,
            "frag",
            "file.bin",
            FILE_SHARE_READ,
            true,
            false,
            false,
        )
        .await;
        assert!(compatible.is_ok());
    }

    #[tokio::test]
    async fn unregister_connection_state_cleans_open_and_lock_state() {
        let shared = Arc::new(Mutex::new(GlobalSmbState::default()));
        let conn_a = 100u64;
        let conn_b = 200u64;
        let file_a = [3u8; 16];
        let file_b = [4u8; 16];

        let _ = register_open_handle(
            &shared,
            conn_a,
            file_a,
            "frag",
            "locked.bin",
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            true,
            true,
            false,
        )
        .await;
        let _ = register_open_handle(
            &shared,
            conn_b,
            file_b,
            "frag",
            "other.bin",
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            true,
            false,
            false,
        )
        .await;

        {
            let mut guard = shared.lock().await;
            guard
                .locks
                .insert("frag/locked.bin".to_string(), (conn_a, file_a));
        }

        unregister_connection_state(&shared, conn_a).await;

        let guard = shared.lock().await;
        assert!(!guard.locks.contains_key("frag/locked.bin"));
        assert!(guard.opens.contains_key("frag/other.bin"));
        let remaining = guard.opens.get("frag/other.bin").expect("remaining open");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].connection_id, conn_b);
    }

    #[test]
    fn pending_commit_size_is_visible_only_while_running_commit() {
        let fragment = format!("frag-{}", Ulid::new());
        let object_key = "pending-size-check.bin";
        transfer_started("t-1", fragment.as_str(), object_key, Some(4096));
        assert_eq!(
            pending_commit_expected_size(fragment.as_str(), object_key),
            None
        );
        transfer_commit_started("t-1");
        assert_eq!(
            pending_commit_expected_size(fragment.as_str(), object_key),
            Some(4096)
        );
        transfer_completed("t-1");
        assert_eq!(
            pending_commit_expected_size(fragment.as_str(), object_key),
            None
        );
    }

    #[test]
    fn pending_commit_listing_includes_direct_children() {
        let fragment = format!("frag-{}", Ulid::new());
        transfer_started("t-2", fragment.as_str(), "videos/a.mp4", Some(8192));
        transfer_started("t-3", fragment.as_str(), "videos/sub/b.mp4", Some(16384));
        transfer_commit_started("t-2");
        transfer_commit_started("t-3");

        let root_entries = pending_commit_entries_for_dir(fragment.as_str(), "videos");
        let root_names = root_entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<HashSet<_>>();
        assert!(root_names.contains("a.mp4"));
        assert!(root_names.contains("sub"));

        let sub_entries = pending_commit_entries_for_dir(fragment.as_str(), "videos/sub");
        let sub_names = sub_entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<HashSet<_>>();
        assert!(sub_names.contains("b.mp4"));
    }

    #[test]
    fn directory_information_buffer_supports_continuation_paging() {
        let entries = vec![
            DirEntry {
                name: "a.txt".to_string(),
                path: crate::fuse::model::FsdPath {
                    fragment: "frag".to_string(),
                    object_key: "a.txt".to_string(),
                },
                metadata: NodeMetadata {
                    kind: NodeKind::File,
                    size_bytes: 11,
                    modified_at_unix: 1,
                },
            },
            DirEntry {
                name: "b.txt".to_string(),
                path: crate::fuse::model::FsdPath {
                    fragment: "frag".to_string(),
                    object_key: "b.txt".to_string(),
                },
                metadata: NodeMetadata {
                    kind: NodeKind::File,
                    size_bytes: 22,
                    modified_at_unix: 2,
                },
            },
            DirEntry {
                name: "c.txt".to_string(),
                path: crate::fuse::model::FsdPath {
                    fragment: "frag".to_string(),
                    object_key: "c.txt".to_string(),
                },
                metadata: NodeMetadata {
                    kind: NodeKind::File,
                    size_bytes: 33,
                    modified_at_unix: 3,
                },
            },
        ];

        let mut consumed = 0usize;
        let mut rounds = 0usize;
        while consumed < entries.len() {
            let (chunk, count) = build_directory_information_buffer(
                &entries[consumed..],
                120,
                FILE_INFORMATION_CLASS_DIRECTORY_INFORMATION,
            );
            assert!(!chunk.is_empty());
            assert!(count > 0);
            consumed = consumed.saturating_add(count);
            rounds = rounds.saturating_add(1);
            assert!(rounds < 10);
        }
        assert_eq!(consumed, entries.len());
    }
}
