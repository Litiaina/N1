use crate::enums::app_error_enum::AppError;
use crate::fuse::model::{FsdPath, NodeKind, NodeMetadata, WriteHandle};
use crate::routes::storage_routes::global_app_state;
use futures_util::future::BoxFuture;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub path: FsdPath,
    pub metadata: NodeMetadata,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirListPage {
    pub entries: Vec<DirEntry>,
    pub continuation_token: Option<String>,
}

pub trait FsdBackend: Send + Sync + 'static {
    fn list_dir_page<'a>(
        &'a self,
        path: &'a FsdPath,
        continuation_token: Option<String>,
        page_limit: usize,
    ) -> BoxFuture<'a, Result<DirListPage, AppError>>;
    fn stat<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<NodeMetadata, AppError>>;
    fn read_range<'a>(
        &'a self,
        path: &'a FsdPath,
        offset: u64,
        length: usize,
    ) -> BoxFuture<'a, Result<Vec<u8>, AppError>>;
    fn begin_write<'a>(
        &'a self,
        path: &'a FsdPath,
        expected_size_bytes: Option<u64>,
    ) -> BoxFuture<'a, Result<WriteHandle, AppError>>;
    fn write_chunk<'a>(
        &'a self,
        handle: &'a WriteHandle,
        offset: u64,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), AppError>>;
    fn commit_write<'a>(&'a self, handle: &'a WriteHandle) -> BoxFuture<'a, Result<(), AppError>>;
    fn abort_write<'a>(&'a self, handle: &'a WriteHandle) -> BoxFuture<'a, Result<(), AppError>>;
    fn delete<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>>;
    fn mkdir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>>;
    fn rmdir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>>;
    fn rename<'a>(
        &'a self,
        from: &'a FsdPath,
        to: &'a FsdPath,
    ) -> BoxFuture<'a, Result<(), AppError>>;
}

#[derive(Debug, Clone)]
struct PendingWrite {
    path: FsdPath,
    target_path: PathBuf,
    expected_size_bytes: Option<u64>,
}

#[derive(Clone)]
pub struct InProcessBackend {
    _staging_root: PathBuf,
    pending_writes: Arc<Mutex<HashMap<String, PendingWrite>>>,
}

impl InProcessBackend {
    pub fn new(staging_root: impl Into<PathBuf>) -> Self {
        Self {
            _staging_root: staging_root.into(),
            pending_writes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn validate_object_key(object_key: &str) -> Result<(), AppError> {
        for segment in object_key.split('/') {
            if segment.is_empty() {
                continue;
            }
            if segment == "." || segment == ".." {
                return Err(AppError::BadRequest(
                    "invalid share path segment".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn share_root(share_name: &str) -> Result<PathBuf, AppError> {
        let Some(share) = global_app_state().metadata.share(share_name)? else {
            return Err(AppError::NotFound);
        };
        Ok(PathBuf::from(share.root_path))
    }

    fn share_fs_path(path: &FsdPath) -> Result<PathBuf, AppError> {
        Self::validate_object_key(path.object_key.as_str())?;
        let mut out = Self::share_root(path.fragment.as_str())?;
        if !path.object_key.is_empty() {
            for segment in path
                .object_key
                .split('/')
                .filter(|segment| !segment.is_empty())
            {
                out.push(segment);
            }
        }
        Ok(out)
    }

    fn relative_entry_path(parent: &FsdPath, name: &str) -> FsdPath {
        let object_key = if parent.object_key.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", parent.object_key, name)
        };
        FsdPath {
            fragment: parent.fragment.clone(),
            object_key,
        }
    }

    async fn ensure_share_writeable(&self, share_name: &str) -> Result<(), AppError> {
        let Some(share) = global_app_state().metadata.share(share_name)? else {
            return Err(AppError::NotFound);
        };
        if share.read_only {
            return Err(AppError::Forbidden(format!(
                "share '{}' is read-only",
                share_name
            )));
        }
        Ok(())
    }
}

impl FsdBackend for InProcessBackend {
    fn list_dir_page<'a>(
        &'a self,
        path: &'a FsdPath,
        continuation_token: Option<String>,
        page_limit: usize,
    ) -> BoxFuture<'a, Result<DirListPage, AppError>> {
        Box::pin(async move {
            let dir_path = Self::share_fs_path(path)?;
            let offset = continuation_token
                .as_deref()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            let page_limit = page_limit.max(1);
            let path_clone = path.clone();
            tokio::task::spawn_blocking(move || {
                let mut entries = Vec::new();
                if !dir_path.exists() {
                    return Err(AppError::NotFound);
                }
                let metadata = std::fs::metadata(&dir_path)
                    .map_err(|err| AppError::Io(err, format!("stat {}", dir_path.display())))?;
                if !metadata.is_dir() {
                    return Err(AppError::NotFound);
                }

                for entry in std::fs::read_dir(&dir_path)
                    .map_err(|err| AppError::Io(err, format!("readdir {}", dir_path.display())))?
                {
                    let entry = entry.map_err(|err| {
                        AppError::Io(err, format!("readdir {}", dir_path.display()))
                    })?;
                    let entry_path = entry.path();
                    let metadata = entry.metadata().map_err(|err| {
                        AppError::Io(err, format!("stat {}", entry_path.display()))
                    })?;
                    let modified_at_unix = metadata
                        .modified()
                        .ok()
                        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|duration| duration.as_secs() as i64)
                        .unwrap_or(0);
                    let name = entry.file_name().to_string_lossy().to_string();
                    entries.push(DirEntry {
                        name: name.clone(),
                        path: InProcessBackend::relative_entry_path(&path_clone, &name),
                        metadata: NodeMetadata {
                            kind: if metadata.is_dir() {
                                NodeKind::Directory
                            } else {
                                NodeKind::File
                            },
                            size_bytes: if metadata.is_file() {
                                metadata.len()
                            } else {
                                0
                            },
                            modified_at_unix,
                        },
                    });
                }

                entries.sort_by(|a, b| {
                    a.name
                        .to_ascii_lowercase()
                        .cmp(&b.name.to_ascii_lowercase())
                });
                let page = entries
                    .iter()
                    .skip(offset)
                    .take(page_limit)
                    .cloned()
                    .collect::<Vec<_>>();
                let next_offset = offset.saturating_add(page.len());
                let continuation_token = if next_offset < entries.len() {
                    Some(next_offset.to_string())
                } else {
                    None
                };
                Ok(DirListPage {
                    entries: page,
                    continuation_token,
                })
            })
            .await
            .map_err(|err| {
                AppError::InternalServerError(format!("list_dir_page join error: {}", err))
            })?
        })
    }

    fn stat<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<NodeMetadata, AppError>> {
        Box::pin(async move {
            let fs_path = Self::share_fs_path(path)?;
            let metadata = fs::metadata(&fs_path)
                .await
                .map_err(|err| match err.kind() {
                    std::io::ErrorKind::NotFound => AppError::NotFound,
                    _ => AppError::Io(err, format!("stat {}", fs_path.display())),
                })?;
            let modified_at_unix = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs() as i64)
                .unwrap_or(0);
            Ok(NodeMetadata {
                kind: if metadata.is_dir() {
                    NodeKind::Directory
                } else {
                    NodeKind::File
                },
                size_bytes: if metadata.is_file() {
                    metadata.len()
                } else {
                    0
                },
                modified_at_unix,
            })
        })
    }

    fn read_range<'a>(
        &'a self,
        path: &'a FsdPath,
        offset: u64,
        length: usize,
    ) -> BoxFuture<'a, Result<Vec<u8>, AppError>> {
        Box::pin(async move {
            let fs_path = Self::share_fs_path(path)?;
            let mut file = OpenOptions::new()
                .read(true)
                .open(&fs_path)
                .await
                .map_err(|err| match err.kind() {
                    std::io::ErrorKind::NotFound => AppError::NotFound,
                    _ => AppError::Io(err, format!("open {}", fs_path.display())),
                })?;
            file.seek(SeekFrom::Start(offset))
                .await
                .map_err(|err| AppError::Io(err, format!("seek {}", fs_path.display())))?;
            let mut out = vec![0u8; length];
            let read = file
                .read(&mut out)
                .await
                .map_err(|err| AppError::Io(err, format!("read {}", fs_path.display())))?;
            out.truncate(read);
            Ok(out)
        })
    }

    fn begin_write<'a>(
        &'a self,
        path: &'a FsdPath,
        expected_size_bytes: Option<u64>,
    ) -> BoxFuture<'a, Result<WriteHandle, AppError>> {
        Box::pin(async move {
            self.ensure_share_writeable(path.fragment.as_str()).await?;

            let session_id = format!(
                "{}-{}",
                std::process::id(),
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
            );
            let version_id = session_id.clone();
            let target_path = Self::share_fs_path(path)?;
            if let Some(parent) = target_path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|err| AppError::Io(err, format!("mkdir {}", parent.display())))?;
            }
            OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(&target_path)
                .await
                .map_err(|err| AppError::Io(err, format!("open {}", target_path.display())))?;

            let handle = WriteHandle {
                session_id: session_id.clone(),
                version_id,
                expected_size_bytes,
                path: path.clone(),
            };
            self.pending_writes.lock().await.insert(
                session_id,
                PendingWrite {
                    path: path.clone(),
                    target_path,
                    expected_size_bytes,
                },
            );
            Ok(handle)
        })
    }

    fn write_chunk<'a>(
        &'a self,
        handle: &'a WriteHandle,
        offset: u64,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            let pending = {
                let guard = self.pending_writes.lock().await;
                guard
                    .get(handle.session_id.as_str())
                    .cloned()
                    .ok_or(AppError::NotFound)?
            };
            if let Some(expected) = pending.expected_size_bytes {
                let end = offset.saturating_add(bytes.len() as u64);
                if end > expected {
                    return Err(AppError::BadRequest(
                        "write exceeds expected file size".to_string(),
                    ));
                }
            }
            let mut file = OpenOptions::new()
                .write(true)
                .read(true)
                .open(&pending.target_path)
                .await
                .map_err(|err| {
                    AppError::Io(err, format!("open {}", pending.target_path.display()))
                })?;
            file.seek(SeekFrom::Start(offset)).await.map_err(|err| {
                AppError::Io(err, format!("seek {}", pending.target_path.display()))
            })?;
            file.write_all(&bytes).await.map_err(|err| {
                AppError::Io(err, format!("write {}", pending.target_path.display()))
            })?;
            file.flush().await.map_err(|err| {
                AppError::Io(err, format!("flush {}", pending.target_path.display()))
            })?;
            Ok(())
        })
    }

    fn commit_write<'a>(&'a self, handle: &'a WriteHandle) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            let pending = self
                .pending_writes
                .lock()
                .await
                .remove(handle.session_id.as_str())
                .ok_or(AppError::NotFound)?;
            self.ensure_share_writeable(pending.path.fragment.as_str())
                .await?;
            if let Some(expected_size_bytes) = pending.expected_size_bytes {
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&pending.target_path)
                    .await
                    .map_err(|err| {
                        AppError::Io(err, format!("open {}", pending.target_path.display()))
                    })?;
                file.set_len(expected_size_bytes).await.map_err(|err| {
                    AppError::Io(err, format!("truncate {}", pending.target_path.display()))
                })?;
            }
            Ok(())
        })
    }

    fn abort_write<'a>(&'a self, handle: &'a WriteHandle) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            let pending = self
                .pending_writes
                .lock()
                .await
                .remove(handle.session_id.as_str());
            let _ = pending;
            Ok(())
        })
    }

    fn delete<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            self.ensure_share_writeable(path.fragment.as_str()).await?;
            let target_path = Self::share_fs_path(path)?;
            fs::remove_file(&target_path)
                .await
                .map_err(|err| match err.kind() {
                    std::io::ErrorKind::NotFound => AppError::NotFound,
                    _ => AppError::Io(err, format!("remove {}", target_path.display())),
                })?;
            Ok(())
        })
    }

    fn mkdir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            self.ensure_share_writeable(path.fragment.as_str()).await?;
            let target_path = Self::share_fs_path(path)?;
            fs::create_dir_all(&target_path)
                .await
                .map_err(|err| AppError::Io(err, format!("mkdir {}", target_path.display())))?;
            Ok(())
        })
    }

    fn rmdir<'a>(&'a self, path: &'a FsdPath) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            self.ensure_share_writeable(path.fragment.as_str()).await?;
            if path.object_key.trim_matches('/').is_empty() {
                return Err(AppError::Forbidden(
                    "cannot remove share root directory".to_string(),
                ));
            }
            let target_path = Self::share_fs_path(path)?;
            match fs::remove_dir(&target_path).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Err(AppError::NotFound);
                }
                Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                    fs::remove_dir_all(&target_path).await.map_err(|err| {
                        AppError::Io(err, format!("rmdir recursive {}", target_path.display()))
                    })?;
                }
                Err(err) => {
                    return Err(AppError::Io(
                        err,
                        format!("rmdir {}", target_path.display()),
                    ));
                }
            }
            Ok(())
        })
    }

    fn rename<'a>(
        &'a self,
        from: &'a FsdPath,
        to: &'a FsdPath,
    ) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            self.ensure_share_writeable(from.fragment.as_str()).await?;
            let from_path = Self::share_fs_path(from)?;
            let to_path = Self::share_fs_path(to)?;
            if let Some(parent) = to_path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|err| AppError::Io(err, format!("mkdir {}", parent.display())))?;
            }
            fs::rename(&from_path, &to_path)
                .await
                .map_err(|err| match err.kind() {
                    std::io::ErrorKind::NotFound => AppError::NotFound,
                    _ => AppError::Io(
                        err,
                        format!("rename {} -> {}", from_path.display(), to_path.display()),
                    ),
                })?;
            Ok(())
        })
    }
}
