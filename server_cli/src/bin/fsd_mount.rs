#[cfg(not(all(target_os = "linux", feature = "fsd-mount")))]
fn main() {
    eprintln!(
        "fsd mount binary requires Linux and feature 'fsd-mount'\nrun: cargo run -p server_cli --features fsd-mount --bin fsd_mount -- --help"
    );
    std::process::exit(1);
}

#[cfg(all(target_os = "linux", feature = "fsd-mount"))]
mod linux_mount {
    use clap::Parser;
    use fuser::{
        FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData,
        ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
    };
    use libc::{EACCES, EEXIST, EFBIG, EINVAL, EIO, ENOENT, ENOTEMPTY};
    use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
    use reqwest::blocking::Client;
    use reqwest::StatusCode;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::ffi::OsStr;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[derive(Parser, Debug)]
    #[command(name = "fsd_mount")]
    struct Args {
        #[arg(long)]
        url: String,
        #[arg(long)]
        fragment: String,
        #[arg(long)]
        mount_point: PathBuf,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        secret: Option<String>,
        #[arg(long, default_value_t = true)]
        insecure: bool,
        #[arg(long, default_value_t = false)]
        read_only: bool,
        #[arg(long, default_value_t = false)]
        allow_other: bool,
        #[arg(long, default_value_t = false)]
        auto_unmount: bool,
        #[arg(long, default_value_t = 256 * 1024)]
        max_read_bytes: usize,
        #[arg(long, default_value_t = false)]
        no_direct_io: bool,
    }

    #[derive(Serialize)]
    struct AuthRequest {
        fragment: String,
        secret: String,
        read: bool,
        write: bool,
        modify: bool,
        delete: bool,
    }

    #[derive(Deserialize)]
    struct AuthResponse {
        access_token: String,
    }

    #[derive(Clone)]
    struct FsdHttpClient {
        base_url: String,
        fragment: String,
        token: String,
        http: Client,
        max_read_bytes: usize,
    }

    #[derive(Deserialize)]
    struct DirEntryResponse {
        name: String,
        path: String,
        kind: String,
        size_bytes: u64,
        modified_at_unix: i64,
    }

    #[derive(Deserialize)]
    struct StatResponse {
        path: String,
        kind: String,
        size_bytes: u64,
        modified_at_unix: i64,
    }

    #[derive(Serialize)]
    struct BeginWriteRequest {
        expected_size_bytes: Option<u64>,
    }

    #[derive(Deserialize)]
    struct BeginWriteResponse {
        session_id: String,
        version_id: String,
    }

    #[derive(Serialize)]
    struct RenameRequest {
        from: String,
        to: String,
    }

    impl FsdHttpClient {
        fn new(
            base_url: String,
            fragment: String,
            token: String,
            insecure: bool,
            max_read_bytes: usize,
        ) -> Result<Self, String> {
            let http = Client::builder()
                .danger_accept_invalid_certs(insecure)
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| format!("failed to build HTTP client: {}", e))?;

            Ok(Self {
                base_url,
                fragment,
                token,
                http,
                max_read_bytes: max_read_bytes.clamp(4 * 1024, 1024 * 1024),
            })
        }

        fn authenticate(
            base_url: &str,
            fragment: &str,
            secret: &str,
            insecure: bool,
        ) -> Result<String, String> {
            let http = Client::builder()
                .danger_accept_invalid_certs(insecure)
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| format!("failed to build auth client: {}", e))?;

            let resp = http
                .post(format!("{}/authenticate/fragment", base_url))
                .json(&AuthRequest {
                    fragment: fragment.to_string(),
                    secret: secret.to_string(),
                    read: true,
                    write: true,
                    modify: true,
                    delete: true,
                })
                .send()
                .map_err(|e| format!("authentication request failed: {}", e))?;

            if !resp.status().is_success() {
                let code = resp.status();
                let body = resp.text().unwrap_or_default();
                return Err(format!("authentication failed [{}]: {}", code, body));
            }

            resp.json::<AuthResponse>()
                .map(|v| v.access_token)
                .map_err(|e| format!("failed to decode auth response: {}", e))
        }

        fn encode_path(path: &str) -> String {
            path.split('/')
                .map(|segment| utf8_percent_encode(segment, NON_ALPHANUMERIC).to_string())
                .collect::<Vec<_>>()
                .join("/")
        }

        fn backend_mount_path(&self, rel_path: &str) -> String {
            if rel_path.is_empty() {
                self.fragment.clone()
            } else {
                format!("{}/{}", self.fragment, rel_path)
            }
        }

        fn map_http_error(status: StatusCode) -> i32 {
            match status {
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => EACCES,
                StatusCode::NOT_FOUND => ENOENT,
                StatusCode::CONFLICT => EEXIST,
                StatusCode::PAYLOAD_TOO_LARGE => EFBIG,
                StatusCode::BAD_REQUEST => EINVAL,
                _ => EIO,
            }
        }

        fn list_dir(&self, rel_path: &str) -> Result<Vec<DirEntryResponse>, i32> {
            let mount_path = self.backend_mount_path(rel_path);
            let url = format!(
                "{}/v4/fsd/list/{}",
                self.base_url,
                Self::encode_path(&mount_path)
            );

            let resp = self
                .http
                .get(url)
                .bearer_auth(&self.token)
                .send()
                .map_err(|_| EIO)?;

            if !resp.status().is_success() {
                return Err(Self::map_http_error(resp.status()));
            }

            resp.json::<Vec<DirEntryResponse>>().map_err(|_| EIO)
        }

        fn stat(&self, rel_path: &str) -> Result<StatResponse, i32> {
            let mount_path = self.backend_mount_path(rel_path);
            let url = format!(
                "{}/v4/fsd/stat/{}",
                self.base_url,
                Self::encode_path(&mount_path)
            );

            let resp = self
                .http
                .get(url)
                .bearer_auth(&self.token)
                .send()
                .map_err(|_| EIO)?;

            if !resp.status().is_success() {
                return Err(Self::map_http_error(resp.status()));
            }

            resp.json::<StatResponse>().map_err(|_| EIO)
        }

        fn read(&self, rel_path: &str, offset: u64, length: usize) -> Result<Vec<u8>, i32> {
            let mount_path = self.backend_mount_path(rel_path);
            let url = format!(
                "{}/v4/fsd/read/{}",
                self.base_url,
                Self::encode_path(&mount_path)
            );
            let requested = length.clamp(1, self.max_read_bytes);

            let resp = self
                .http
                .get(url)
                .bearer_auth(&self.token)
                .query(&[("offset", offset), ("length", requested as u64)])
                .send()
                .map_err(|_| EIO)?;

            if !resp.status().is_success() {
                return Err(Self::map_http_error(resp.status()));
            }

            resp.bytes().map(|b| b.to_vec()).map_err(|_| EIO)
        }

        fn begin_write(
            &self,
            rel_path: &str,
            expected_size_bytes: Option<u64>,
        ) -> Result<String, i32> {
            let mount_path = self.backend_mount_path(rel_path);
            let url = format!(
                "{}/v4/fsd/write/begin/{}",
                self.base_url,
                Self::encode_path(&mount_path)
            );

            let resp = self
                .http
                .post(url)
                .bearer_auth(&self.token)
                .json(&BeginWriteRequest {
                    expected_size_bytes,
                })
                .send()
                .map_err(|_| EIO)?;

            if !resp.status().is_success() {
                let status = resp.status();
                return Err(Self::map_http_error(status));
            }

            let value = resp.json::<BeginWriteResponse>().map_err(|_| EIO)?;
            let _ = value.version_id;
            Ok(value.session_id)
        }

        fn write_chunk(&self, session_id: &str, offset: u64, data: &[u8]) -> Result<(), i32> {
            let url = format!("{}/v4/fsd/write/chunk/{}", self.base_url, session_id);

            let resp = self
                .http
                .put(url)
                .bearer_auth(&self.token)
                .query(&[("offset", offset)])
                .body(data.to_vec())
                .send()
                .map_err(|_| EIO)?;

            if !resp.status().is_success() {
                return Err(Self::map_http_error(resp.status()));
            }

            Ok(())
        }

        fn commit_write(&self, session_id: &str) -> Result<(), i32> {
            let url = format!("{}/v4/fsd/write/commit/{}", self.base_url, session_id);

            let resp = self
                .http
                .post(url)
                .bearer_auth(&self.token)
                .send()
                .map_err(|_| EIO)?;

            if !resp.status().is_success() {
                return Err(Self::map_http_error(resp.status()));
            }

            Ok(())
        }

        fn abort_write(&self, session_id: &str) -> Result<(), i32> {
            let url = format!("{}/v4/fsd/write/abort/{}", self.base_url, session_id);

            let resp = self
                .http
                .post(url)
                .bearer_auth(&self.token)
                .send()
                .map_err(|_| EIO)?;

            if !resp.status().is_success() {
                return Err(Self::map_http_error(resp.status()));
            }

            Ok(())
        }

        fn delete(&self, rel_path: &str) -> Result<(), i32> {
            let mount_path = self.backend_mount_path(rel_path);
            let url = format!(
                "{}/v4/fsd/delete/{}",
                self.base_url,
                Self::encode_path(&mount_path)
            );

            let resp = self
                .http
                .delete(url)
                .bearer_auth(&self.token)
                .send()
                .map_err(|_| EIO)?;

            if !resp.status().is_success() {
                return Err(Self::map_http_error(resp.status()));
            }

            Ok(())
        }

        fn mkdir(&self, rel_path: &str) -> Result<(), i32> {
            let mount_path = self.backend_mount_path(rel_path);
            let url = format!(
                "{}/v4/fsd/mkdir/{}",
                self.base_url,
                Self::encode_path(&mount_path)
            );

            let resp = self
                .http
                .post(url)
                .bearer_auth(&self.token)
                .send()
                .map_err(|_| EIO)?;

            if !resp.status().is_success() {
                return Err(Self::map_http_error(resp.status()));
            }

            Ok(())
        }

        fn rmdir(&self, rel_path: &str) -> Result<(), i32> {
            let mount_path = self.backend_mount_path(rel_path);
            let url = format!(
                "{}/v4/fsd/rmdir/{}",
                self.base_url,
                Self::encode_path(&mount_path)
            );

            let resp = self
                .http
                .delete(url)
                .bearer_auth(&self.token)
                .send()
                .map_err(|_| EIO)?;

            if !resp.status().is_success() {
                let status = resp.status();
                if status == StatusCode::CONFLICT {
                    let body = resp.text().unwrap_or_default().to_lowercase();
                    if body.contains("not empty") {
                        return Err(ENOTEMPTY);
                    }
                }
                return Err(Self::map_http_error(status));
            }

            Ok(())
        }

        fn rename(&self, from_rel: &str, to_rel: &str) -> Result<(), i32> {
            let from = self.backend_mount_path(from_rel);
            let to = self.backend_mount_path(to_rel);
            let url = format!("{}/v4/fsd/rename", self.base_url);

            let resp = self
                .http
                .patch(url)
                .bearer_auth(&self.token)
                .json(&RenameRequest { from, to })
                .send()
                .map_err(|_| EIO)?;

            if !resp.status().is_success() {
                return Err(Self::map_http_error(resp.status()));
            }

            Ok(())
        }
    }

    #[derive(Default)]
    struct InodeTable {
        next_inode: u64,
        path_to_inode: HashMap<String, u64>,
        inode_to_path: HashMap<u64, String>,
    }

    impl InodeTable {
        fn new() -> Self {
            let mut table = Self {
                next_inode: 2,
                path_to_inode: HashMap::new(),
                inode_to_path: HashMap::new(),
            };
            table.path_to_inode.insert(String::new(), 1);
            table.inode_to_path.insert(1, String::new());
            table
        }

        fn ensure_inode(&mut self, rel_path: &str) -> u64 {
            if let Some(ino) = self.path_to_inode.get(rel_path) {
                return *ino;
            }

            let ino = self.next_inode;
            self.next_inode = self.next_inode.saturating_add(1);
            self.path_to_inode.insert(rel_path.to_string(), ino);
            self.inode_to_path.insert(ino, rel_path.to_string());
            ino
        }

        fn rel_path(&self, ino: u64) -> Option<String> {
            self.inode_to_path.get(&ino).cloned()
        }

        fn parent_rel(rel_path: &str) -> String {
            if rel_path.is_empty() {
                return String::new();
            }
            if let Some((parent, _)) = rel_path.rsplit_once('/') {
                parent.to_string()
            } else {
                String::new()
            }
        }

        fn parent_inode(&mut self, rel_path: &str) -> u64 {
            let parent = Self::parent_rel(rel_path);
            self.ensure_inode(&parent)
        }

        fn remove_path(&mut self, rel_path: &str) {
            if let Some(ino) = self.path_to_inode.remove(rel_path) {
                self.inode_to_path.remove(&ino);
            }
        }

        fn rewrite_prefix(&mut self, from_prefix: &str, to_prefix: &str) {
            let keys: Vec<String> = self
                .path_to_inode
                .keys()
                .filter(|k| {
                    k.as_str() == from_prefix || k.starts_with(&format!("{}/", from_prefix))
                })
                .cloned()
                .collect();

            for old_key in keys {
                if let Some(ino) = self.path_to_inode.remove(&old_key) {
                    let suffix = old_key
                        .strip_prefix(from_prefix)
                        .unwrap_or_default()
                        .trim_start_matches('/');
                    let new_key = if suffix.is_empty() {
                        to_prefix.to_string()
                    } else if to_prefix.is_empty() {
                        suffix.to_string()
                    } else {
                        format!("{}/{}", to_prefix, suffix)
                    };
                    self.path_to_inode.insert(new_key.clone(), ino);
                    self.inode_to_path.insert(ino, new_key);
                }
            }
        }
    }

    struct HandleState {
        rel_path: String,
        session_id: Option<String>,
        writable: bool,
        dirty: bool,
        size_hint: u64,
    }

    struct FsState {
        inodes: InodeTable,
        next_fh: u64,
        handles: HashMap<u64, HandleState>,
    }

    impl FsState {
        fn new() -> Self {
            Self {
                inodes: InodeTable::new(),
                next_fh: 1,
                handles: HashMap::new(),
            }
        }

        fn alloc_fh(&mut self) -> u64 {
            let fh = self.next_fh;
            self.next_fh = self.next_fh.saturating_add(1);
            fh
        }
    }

    struct FsdFilesystem {
        client: FsdHttpClient,
        state: Mutex<FsState>,
        ttl: Duration,
        uid: u32,
        gid: u32,
    }

    impl FsdFilesystem {
        fn new(client: FsdHttpClient) -> Self {
            Self {
                client,
                state: Mutex::new(FsState::new()),
                ttl: Duration::from_secs(1),
                uid: unsafe { libc::getuid() },
                gid: unsafe { libc::getgid() },
            }
        }

        fn join_rel(parent: &str, name: &OsStr) -> Option<String> {
            let name = name.to_str()?;
            if parent.is_empty() {
                Some(name.to_string())
            } else {
                Some(format!("{}/{}", parent, name))
            }
        }

        fn system_time(ts: i64) -> SystemTime {
            UNIX_EPOCH + Duration::from_secs(ts.max(0) as u64)
        }

        fn to_filetype(kind: &str) -> Option<FileType> {
            match kind {
                "file" => Some(FileType::RegularFile),
                "directory" => Some(FileType::Directory),
                _ => None,
            }
        }

        fn attr_for(&self, ino: u64, kind: FileType, size: u64, modified_at_unix: i64) -> FileAttr {
            let perm = match kind {
                FileType::Directory => 0o755,
                _ => 0o644,
            };
            let nlink = match kind {
                FileType::Directory => 2,
                _ => 1,
            };
            let ts = Self::system_time(modified_at_unix);

            FileAttr {
                ino,
                size,
                blocks: size.div_ceil(512),
                atime: ts,
                mtime: ts,
                ctime: ts,
                crtime: ts,
                kind,
                perm,
                nlink,
                uid: self.uid,
                gid: self.gid,
                rdev: 0,
                flags: 0,
                blksize: 4096,
            }
        }

        fn root_attr(&self) -> FileAttr {
            self.attr_for(1, FileType::Directory, 0, 0)
        }

        fn rel_path_for_ino(&self, ino: u64) -> Result<String, i32> {
            let state = self.state.lock().map_err(|_| EIO)?;
            state.inodes.rel_path(ino).ok_or(ENOENT)
        }

        fn backend_stat_to_attr(&self, ino: u64, stat: &StatResponse) -> Result<FileAttr, i32> {
            let _ = &stat.path;
            let kind = Self::to_filetype(&stat.kind).ok_or(EIO)?;
            Ok(self.attr_for(ino, kind, stat.size_bytes, stat.modified_at_unix))
        }

        fn register_path_inode(&self, rel_path: &str) -> Result<u64, i32> {
            let mut state = self.state.lock().map_err(|_| EIO)?;
            Ok(state.inodes.ensure_inode(rel_path))
        }
    }

    impl Filesystem for FsdFilesystem {
        fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
            let parent_rel = match self.rel_path_for_ino(parent) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let rel_path = match Self::join_rel(&parent_rel, name) {
                Some(v) => v,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            match self.client.stat(&rel_path) {
                Ok(stat) => {
                    let ino = match self.register_path_inode(&rel_path) {
                        Ok(v) => v,
                        Err(e) => {
                            reply.error(e);
                            return;
                        }
                    };
                    match self.backend_stat_to_attr(ino, &stat) {
                        Ok(attr) => reply.entry(&self.ttl, &attr, 0),
                        Err(e) => reply.error(e),
                    }
                }
                Err(e) => reply.error(e),
            }
        }

        fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
            if ino == 1 {
                reply.attr(&self.ttl, &self.root_attr());
                return;
            }

            let rel_path = match self.rel_path_for_ino(ino) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            match self.client.stat(&rel_path) {
                Ok(stat) => match self.backend_stat_to_attr(ino, &stat) {
                    Ok(attr) => reply.attr(&self.ttl, &attr),
                    Err(e) => reply.error(e),
                },
                Err(e) => reply.error(e),
            }
        }

        fn readdir(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _fh: u64,
            offset: i64,
            mut reply: ReplyDirectory,
        ) {
            let rel_path = match self.rel_path_for_ino(ino) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let entries = match self.client.list_dir(&rel_path) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let mut state = match self.state.lock() {
                Ok(v) => v,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };

            let parent_ino = if ino == 1 {
                1
            } else {
                state.inodes.parent_inode(&rel_path)
            };

            let mut rows: Vec<(u64, FileType, String)> = Vec::new();
            rows.push((ino, FileType::Directory, ".".to_string()));
            rows.push((parent_ino, FileType::Directory, "..".to_string()));

            for entry in entries {
                let _ = entry.path;
                let _ = entry.size_bytes;
                let _ = entry.modified_at_unix;
                let Some(kind) = Self::to_filetype(&entry.kind) else {
                    continue;
                };
                let child_rel = if rel_path.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{}/{}", rel_path, entry.name)
                };
                let child_ino = state.inodes.ensure_inode(&child_rel);
                rows.push((child_ino, kind, entry.name));
            }

            let start = offset.max(0) as usize;
            for (idx, (entry_ino, file_type, name)) in rows.into_iter().enumerate().skip(start) {
                let next_offset = (idx + 1) as i64;
                if reply.add(entry_ino, next_offset, file_type, name) {
                    break;
                }
            }

            reply.ok();
        }

        fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
            let rel_path = match self.rel_path_for_ino(ino) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let writable = (flags & libc::O_ACCMODE) != libc::O_RDONLY;
            let session_id = if writable {
                match self.client.begin_write(&rel_path, None) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        reply.error(e);
                        return;
                    }
                }
            } else {
                None
            };

            let mut state = match self.state.lock() {
                Ok(v) => v,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };

            let fh = state.alloc_fh();
            state.handles.insert(
                fh,
                HandleState {
                    rel_path,
                    session_id,
                    writable,
                    dirty: false,
                    size_hint: 0,
                },
            );

            reply.opened(fh, 0);
        }

        fn mknod(
            &mut self,
            _req: &Request<'_>,
            parent: u64,
            name: &OsStr,
            mode: u32,
            _umask: u32,
            _rdev: u32,
            reply: ReplyEntry,
        ) {
            let file_type = mode & libc::S_IFMT;
            if file_type != libc::S_IFREG {
                reply.error(EINVAL);
                return;
            }

            let parent_rel = match self.rel_path_for_ino(parent) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let rel_path = match Self::join_rel(&parent_rel, name) {
                Some(v) => v,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            match self.client.stat(&rel_path) {
                Ok(_) => {
                    reply.error(EEXIST);
                    return;
                }
                Err(ENOENT) => {}
                Err(e) => {
                    reply.error(e);
                    return;
                }
            }

            let session_id = match self.client.begin_write(&rel_path, Some(0)) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            if let Err(e) = self.client.commit_write(&session_id) {
                let _ = self.client.abort_write(&session_id);
                reply.error(e);
                return;
            }

            let ino = match self.register_path_inode(&rel_path) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let attr = self.attr_for(ino, FileType::RegularFile, 0, 0);
            reply.entry(&self.ttl, &attr, 0);
        }

        fn create(
            &mut self,
            _req: &Request<'_>,
            parent: u64,
            name: &OsStr,
            _mode: u32,
            _umask: u32,
            _flags: i32,
            reply: ReplyCreate,
        ) {
            let parent_rel = match self.rel_path_for_ino(parent) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let rel_path = match Self::join_rel(&parent_rel, name) {
                Some(v) => v,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let session_id = match self.client.begin_write(&rel_path, None) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let mut state = match self.state.lock() {
                Ok(v) => v,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };

            let ino = state.inodes.ensure_inode(&rel_path);
            let fh = state.alloc_fh();
            state.handles.insert(
                fh,
                HandleState {
                    rel_path,
                    session_id: Some(session_id),
                    writable: true,
                    dirty: false,
                    size_hint: 0,
                },
            );

            let attr = self.attr_for(ino, FileType::RegularFile, 0, 0);
            reply.created(&self.ttl, &attr, 0, fh, 0);
        }

        fn mkdir(
            &mut self,
            _req: &Request<'_>,
            parent: u64,
            name: &OsStr,
            _mode: u32,
            _umask: u32,
            reply: ReplyEntry,
        ) {
            let parent_rel = match self.rel_path_for_ino(parent) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let rel_path = match Self::join_rel(&parent_rel, name) {
                Some(v) => v,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            match self.client.mkdir(&rel_path) {
                Ok(()) => {
                    let ino = match self.register_path_inode(&rel_path) {
                        Ok(v) => v,
                        Err(e) => {
                            reply.error(e);
                            return;
                        }
                    };
                    let attr = self.attr_for(ino, FileType::Directory, 0, 0);
                    reply.entry(&self.ttl, &attr, 0);
                }
                Err(e) => reply.error(e),
            }
        }

        fn read(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _fh: u64,
            offset: i64,
            size: u32,
            _flags: i32,
            _lock_owner: Option<u64>,
            reply: ReplyData,
        ) {
            if offset < 0 {
                reply.error(EINVAL);
                return;
            }

            let rel_path = match self.rel_path_for_ino(ino) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            match self.client.read(&rel_path, offset as u64, size as usize) {
                Ok(bytes) => reply.data(&bytes),
                Err(e) => reply.error(e),
            }
        }

        fn write(
            &mut self,
            _req: &Request<'_>,
            _ino: u64,
            fh: u64,
            offset: i64,
            data: &[u8],
            _write_flags: u32,
            _flags: i32,
            _lock_owner: Option<u64>,
            reply: ReplyWrite,
        ) {
            if offset < 0 {
                reply.error(EINVAL);
                return;
            }

            let mut state = match self.state.lock() {
                Ok(v) => v,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };

            let Some(handle) = state.handles.get_mut(&fh) else {
                reply.error(ENOENT);
                return;
            };

            if !handle.writable {
                reply.error(EACCES);
                return;
            }

            let Some(session_id) = handle.session_id.clone() else {
                reply.error(EIO);
                return;
            };

            match self.client.write_chunk(&session_id, offset as u64, data) {
                Ok(()) => {
                    handle.dirty = true;
                    let end = (offset as u64).saturating_add(data.len() as u64);
                    handle.size_hint = handle.size_hint.max(end);
                    reply.written(data.len() as u32);
                }
                Err(e) => reply.error(e),
            }
        }

        fn setattr(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _mode: Option<u32>,
            _uid: Option<u32>,
            _gid: Option<u32>,
            size: Option<u64>,
            _atime: Option<TimeOrNow>,
            _mtime: Option<TimeOrNow>,
            _ctime: Option<SystemTime>,
            fh: Option<u64>,
            _crtime: Option<SystemTime>,
            _chgtime: Option<SystemTime>,
            _bkuptime: Option<SystemTime>,
            _flags: Option<u32>,
            reply: ReplyAttr,
        ) {
            if ino == 1 {
                reply.attr(&self.ttl, &self.root_attr());
                return;
            }

            let rel_path = match self.rel_path_for_ino(ino) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let mut open_size_hint = None;
            if let Ok(mut state) = self.state.lock() {
                if let Some(fh_id) = fh {
                    if let Some(handle) = state.handles.get_mut(&fh_id) {
                        if let Some(s) = size {
                            handle.size_hint = s;
                            if s == 0 {
                                handle.dirty = true;
                            }
                        }
                    }
                }

                open_size_hint = state
                    .handles
                    .values()
                    .filter(|h| h.rel_path == rel_path && h.writable)
                    .map(|h| h.size_hint)
                    .max();
            }

            match self.client.stat(&rel_path) {
                Ok(stat) => match self.backend_stat_to_attr(ino, &stat) {
                    Ok(attr) => reply.attr(&self.ttl, &attr),
                    Err(e) => reply.error(e),
                },
                Err(_) => {
                    if let Some(size_bytes) = open_size_hint {
                        let attr = self.attr_for(ino, FileType::RegularFile, size_bytes, 0);
                        reply.attr(&self.ttl, &attr);
                    } else {
                        reply.error(ENOENT);
                    }
                }
            }
        }

        fn flush(
            &mut self,
            _req: &Request<'_>,
            _ino: u64,
            _fh: u64,
            _lock_owner: u64,
            reply: ReplyEmpty,
        ) {
            reply.ok();
        }

        fn fsync(
            &mut self,
            _req: &Request<'_>,
            _ino: u64,
            _fh: u64,
            _datasync: bool,
            reply: ReplyEmpty,
        ) {
            reply.ok();
        }

        fn release(
            &mut self,
            _req: &Request<'_>,
            _ino: u64,
            fh: u64,
            _flags: i32,
            _lock_owner: Option<u64>,
            _flush: bool,
            reply: ReplyEmpty,
        ) {
            let handle = {
                let mut state = match self.state.lock() {
                    Ok(v) => v,
                    Err(_) => {
                        reply.error(EIO);
                        return;
                    }
                };
                state.handles.remove(&fh)
            };

            let Some(handle) = handle else {
                reply.ok();
                return;
            };

            let _ = &handle.rel_path;
            if handle.writable {
                if let Some(session_id) = handle.session_id.as_deref() {
                    let result = if handle.dirty {
                        self.client.commit_write(session_id)
                    } else {
                        self.client.abort_write(session_id)
                    };

                    if let Err(e) = result {
                        reply.error(e);
                        return;
                    }
                }
            }

            reply.ok();
        }

        fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
            let parent_rel = match self.rel_path_for_ino(parent) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let rel_path = match Self::join_rel(&parent_rel, name) {
                Some(v) => v,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            match self.client.delete(&rel_path) {
                Ok(()) => {
                    if let Ok(mut state) = self.state.lock() {
                        state.inodes.remove_path(&rel_path);
                    }
                    reply.ok();
                }
                Err(e) => reply.error(e),
            }
        }

        fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
            let parent_rel = match self.rel_path_for_ino(parent) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let rel_path = match Self::join_rel(&parent_rel, name) {
                Some(v) => v,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            match self.client.rmdir(&rel_path) {
                Ok(()) => {
                    if let Ok(mut state) = self.state.lock() {
                        state.inodes.remove_path(&rel_path);
                    }
                    reply.ok();
                }
                Err(e) => reply.error(e),
            }
        }

        fn rename(
            &mut self,
            _req: &Request<'_>,
            parent: u64,
            name: &OsStr,
            newparent: u64,
            newname: &OsStr,
            _flags: u32,
            reply: ReplyEmpty,
        ) {
            let from_parent_rel = match self.rel_path_for_ino(parent) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let to_parent_rel = match self.rel_path_for_ino(newparent) {
                Ok(v) => v,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };

            let from_rel = match Self::join_rel(&from_parent_rel, name) {
                Some(v) => v,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            let to_rel = match Self::join_rel(&to_parent_rel, newname) {
                Some(v) => v,
                None => {
                    reply.error(EINVAL);
                    return;
                }
            };

            match self.client.rename(&from_rel, &to_rel) {
                Ok(()) => {
                    if let Ok(mut state) = self.state.lock() {
                        state.inodes.rewrite_prefix(&from_rel, &to_rel);
                    }
                    reply.ok();
                }
                Err(e) => reply.error(e),
            }
        }
    }

    pub fn run() -> Result<(), String> {
        let args = Args::parse();

        let token = if let Some(token) = args.token.clone() {
            token
        } else {
            let secret = args
                .secret
                .clone()
                .ok_or_else(|| "either --token or --secret must be provided".to_string())?;
            FsdHttpClient::authenticate(&args.url, &args.fragment, &secret, args.insecure)?
        };

        let client = FsdHttpClient::new(
            args.url.clone(),
            args.fragment.clone(),
            token,
            args.insecure,
            args.max_read_bytes,
        )?;

        let max_read_bytes = args.max_read_bytes.clamp(4 * 1024, 1024 * 1024);
        let options = vec![
            MountOption::FSName(format!("n1-fsd-{}", args.fragment)),
            MountOption::DefaultPermissions,
            MountOption::CUSTOM(format!("max_read={}", max_read_bytes)),
        ];

        let mut mount_options = options.clone();

        if args.read_only {
            mount_options.push(MountOption::RO);
        } else {
            mount_options.push(MountOption::RW);
        }

        if args.allow_other {
            mount_options.push(MountOption::AllowOther);
        }

        if args.auto_unmount {
            mount_options.push(MountOption::AutoUnmount);
        }

        let mount_point = args.mount_point.clone();

        if !args.no_direct_io {
            let mut direct_options = mount_options.clone();
            direct_options.push(MountOption::CUSTOM("direct_io".to_string()));
            let direct_fs = FsdFilesystem::new(client.clone());
            match fuser::mount2(direct_fs, mount_point.clone(), &direct_options) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let msg = e.to_string();
                    if !(msg.contains("unknown option") && msg.contains("direct_io")) {
                        return Err(format!("failed to mount FUSE filesystem: {}", e));
                    }
                    eprintln!("direct_io is unsupported on this host, retrying without it");
                }
            }
        }

        let fs = FsdFilesystem::new(client);

        fuser::mount2(fs, mount_point, &mount_options)
            .map_err(|e| format!("failed to mount FUSE filesystem: {}", e))
    }
}

#[cfg(all(target_os = "linux", feature = "fsd-mount"))]
fn main() {
    if let Err(e) = linux_mount::run() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
