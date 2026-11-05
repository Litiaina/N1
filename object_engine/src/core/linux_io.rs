/// Positioned owned bytes used by LINK's inline shard records. The driver
/// retains the file and buffer through CQE even when the requester disappears.
pub async fn write_at_async(file: Arc<File>, buffer: Vec<u8>, offset: u64) -> io::Result<()> {
    write_owned_at_async(file, buffer, offset).await.map(|_| ())
}

/// Return storage only after CQE, allowing a stream to reuse bounded buffers.
pub async fn write_owned_at_async(
    file: Arc<File>,
    buffer: Vec<u8>,
    offset: u64,
) -> io::Result<Vec<u8>> {
    if let Some(native) = &selected_backend()?.native {
        return match native
            .write_all(file, uring::Buffer::Bytes(buffer), offset)
            .await?
        {
            uring::Buffer::Bytes(bytes) => Ok(bytes),
            uring::Buffer::Aligned(_) => unreachable!("write changed buffer type"),
        };
    }
    tokio::task::spawn_blocking(move || {
        pwrite_all_at(&*file, &buffer, offset)?;
        Ok(buffer)
    })
    .await
    .map_err(|error| io::Error::other(error.to_string()))?
}
/// Shared descriptor variant for immutable LINK frame reads. Ownership stays
/// with the driver until completion without duplicating a descriptor per frame.
pub async fn read_exact_at_async(file: Arc<File>, len: usize, offset: u64) -> io::Result<Vec<u8>> {
    if let Some(native) = &selected_backend()?.native {
        return match native
            .read_exact(file, uring::Buffer::Bytes(vec![0; len]), offset)
            .await?
        {
            uring::Buffer::Bytes(bytes) => Ok(bytes),
            uring::Buffer::Aligned(_) => unreachable!("read changed buffer type"),
        };
    }
    tokio::task::spawn_blocking(move || {
        let mut bytes = vec![0; len];
        pread_exact_at(&*file, &mut bytes, offset)?;
        Ok(bytes)
    })
    .await
    .map_err(|e| io::Error::other(e.to_string()))?
}
use std::fs::File;
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use rustix::fs::{self, AtFlags, CWD, Mode, OFlags, RenameFlags};
use rustix::io as rio;
use serde::Serialize;

use crate::core::ec::AlignedBuffer;
use crate::enums::app_error_enum::AppError;

pub(crate) mod group;
mod uring;

/// Selection is process-local, performed before storage startup, and never
/// changes after an operation has been submitted. Storage errors are not a
/// reason to replay writes through another backend.
struct SelectedBackend {
    requested: String,
    native: Option<uring::Backend>,
    fallback_reason: Option<String>,
}

#[derive(Clone)]
struct InitializationError {
    kind: io::ErrorKind,
    message: String,
}

fn initialization_allows_fallback(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::Unsupported
        || matches!(
            error.raw_os_error(),
            Some(libc::ENOSYS | libc::EOPNOTSUPP | libc::EPERM | libc::EACCES | libc::EINVAL)
        )
}

impl InitializationError {
    fn error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

static BACKEND: LazyLock<Result<SelectedBackend, InitializationError>> = LazyLock::new(|| {
    let requested = std::env::var("N1_IO_BACKEND").unwrap_or_else(|_| "auto".into());
    if !matches!(requested.as_str(), "auto" | "uring" | "blocking") {
        return Err(InitializationError {
            kind: io::ErrorKind::InvalidInput,
            message: "N1_IO_BACKEND must be auto, uring, or blocking".into(),
        });
    }
    if requested == "blocking" {
        return Ok(SelectedBackend {
            requested,
            native: None,
            fallback_reason: None,
        });
    }
    match uring::Backend::new() {
        Ok(native) => Ok(SelectedBackend {
            requested,
            native: Some(native),
            fallback_reason: None,
        }),
        Err(error) if requested == "auto" && initialization_allows_fallback(&error) => {
            Ok(SelectedBackend {
                requested,
                native: None,
                fallback_reason: Some(format!("io_uring initialization failed: {error}")),
            })
        }
        Err(error) => Err(InitializationError {
            kind: error.kind(),
            message: format!("io_uring backend could not initialize ({requested}): {error}"),
        }),
    }
});

#[derive(Clone, Debug, Serialize)]
pub struct BackendSnapshot {
    pub backend: String,
    /// Fixed executor policy for synchronous metadata/publication helpers.
    pub publication_backend: String,
    pub requested: String,
    pub fallback_reason: Option<String>,
    pub queue_capacity: usize,
    pub queued: usize,
    pub in_flight: usize,
    pub submitted: u64,
    pub completed: u64,
}

fn selected_backend() -> io::Result<&'static SelectedBackend> {
    BACKEND.as_ref().map_err(InitializationError::error)
}

pub fn initialize_backend() -> io::Result<BackendSnapshot> {
    selected_backend()?;
    Ok(backend_snapshot())
}

pub fn backend_snapshot() -> BackendSnapshot {
    match selected_backend() {
        Ok(selected) => {
            let counters = selected.native.as_ref().map(uring::Backend::snapshot);
            BackendSnapshot {
                backend: if selected.native.is_some() {
                    "io_uring"
                } else {
                    "blocking"
                }
                .into(),
                publication_backend: "posix".into(),
                requested: selected.requested.clone(),
                fallback_reason: selected.fallback_reason.clone(),
                queue_capacity: if selected.native.is_some() {
                    uring::CAPACITY
                } else {
                    0
                },
                queued: counters.as_ref().map_or(0, |value| value.queue_depth),
                in_flight: counters.as_ref().map_or(0, |value| value.inflight),
                submitted: counters.as_ref().map_or(0, |value| value.submitted),
                completed: counters.as_ref().map_or(0, |value| value.completed),
            }
        }
        Err(error) => BackendSnapshot {
            backend: "unavailable".into(),
            publication_backend: "posix".into(),
            requested: std::env::var("N1_IO_BACKEND").unwrap_or_else(|_| "auto".into()),
            fallback_reason: Some(error.to_string()),
            queue_capacity: 0,
            queued: 0,
            in_flight: 0,
            submitted: 0,
            completed: 0,
        },
    }
}

/// Ownership, including O_DIRECT alignment, stays with the driver until CQE.
pub async fn write_aligned_at_async(
    file: Arc<File>,
    buffer: AlignedBuffer,
    offset: u64,
) -> io::Result<AlignedBuffer> {
    let alignment = crate::core::ec::ALIGNMENT;
    if !buffer.is_empty()
        && (!(buffer.as_slice().as_ptr() as usize).is_multiple_of(alignment)
            || !buffer.len().is_multiple_of(alignment)
            || !offset.is_multiple_of(alignment as u64))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "direct I/O requires aligned buffer, offset and length",
        ));
    }
    if let Some(native) = &selected_backend()?.native {
        return match native
            .write_all(file, uring::Buffer::Aligned(buffer), offset)
            .await?
        {
            uring::Buffer::Aligned(buffer) => Ok(buffer),
            uring::Buffer::Bytes(_) => {
                unreachable!("aligned write changed buffer type")
            }
        };
    }
    tokio::task::spawn_blocking(move || {
        pwrite_all_at(&*file, buffer.as_slice(), offset)?;
        Ok(buffer)
    })
    .await
    .map_err(|error| io::Error::other(error.to_string()))?
}

/// Synchronous publication workers use direct POSIX I/O. This fixed policy
/// avoids routing a blocking caller through the async driver's queue and reply.
/// Backend initialization still fails closed when native I/O is required.
pub fn write_bytes_at_blocking<Fd: AsFd>(fd: Fd, bytes: &[u8], offset: u64) -> io::Result<()> {
    selected_backend()?;
    pwrite_all_at(fd, bytes, offset)
}

/// Large encoded NDX is shared by its holders, not copied per submission.
fn write_shared_at_blocking<Fd: AsFd>(fd: Fd, bytes: Arc<Vec<u8>>, offset: u64) -> io::Result<()> {
    selected_backend()?;
    pwrite_all_at(fd, bytes.as_slice(), offset)
}

pub async fn fdatasync_async(file: Arc<File>) -> io::Result<()> {
    sync_file_async(file, true).await
}

pub async fn fsync_async(file: Arc<File>) -> io::Result<()> {
    sync_file_async(file, false).await
}

async fn sync_file_async(file: Arc<File>, data_only: bool) -> io::Result<()> {
    if let Some(native) = &selected_backend()?.native {
        return native.sync(file, data_only).await;
    }
    tokio::task::spawn_blocking(move || {
        if data_only {
            fs::fdatasync(&*file)
        } else {
            fs::fsync(&*file)
        }
        .map_err(into_io_error)
    })
    .await
    .map_err(|error| io::Error::other(error.to_string()))?
}

fn into_io_error(err: rustix::io::Errno) -> io::Error {
    err.into()
}

fn owned_fd_to_file(fd: OwnedFd) -> File {
    File::from(fd)
}

fn create_mode() -> Mode {
    Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::ROTH
}

#[cfg(test)]
std::thread_local! {
    static ENSURE_DIR_MKDIR_ATTEMPTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub fn ensure_dir_all_blocking(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }

    // Existing NDX parents take one open. New object/version directories walk
    // upward only to their nearest existing parent, not from the filesystem
    // root through every already-established mount and fanout component.
    match fs::openat(
        CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(directory) => {
            drop(directory);
            return Ok(());
        }
        Err(rustix::io::Errno::NOENT) => {}
        Err(error) => return Err(into_io_error(error)),
    }
    // Components remove redundant trailing/interior `.` while preserving `..`.
    // In particular, mkdir("missing/.") cannot itself create "missing".
    let normalized = path.components().collect::<PathBuf>();
    let mut missing = vec![normalized.as_path()];
    let mut ancestor = normalized.parent().unwrap_or_else(|| Path::new(""));
    while !ancestor.as_os_str().is_empty() {
        match fs::openat(
            CWD,
            ancestor,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(directory) => {
                drop(directory);
                break;
            }
            Err(rustix::io::Errno::NOENT) => {
                missing.push(ancestor);
                ancestor = ancestor.parent().unwrap_or_else(|| Path::new(""));
            }
            Err(error) => return Err(into_io_error(error)),
        }
    }

    for directory in missing.into_iter().rev() {
        #[cfg(test)]
        ENSURE_DIR_MKDIR_ATTEMPTS.with(|attempts| attempts.set(attempts.get() + 1));

        match fs::mkdirat(CWD, directory, Mode::from_raw_mode(0o755)) {
            Ok(()) => {}
            Err(rustix::io::Errno::EXIST) => {
                // A concurrent creator may have supplied this component. As
                // before, follow directory symlinks but reject files/errors.
                fs::openat(
                    CWD,
                    directory,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map(drop)
                .map_err(into_io_error)?;
            }
            Err(error) => return Err(into_io_error(error)),
        }
    }
    Ok(())
}

pub async fn ensure_dir_all(path: &Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    let task_path = path.clone();
    tokio::task::spawn_blocking(move || ensure_dir_all_blocking(&task_path))
        .await
        .map_err(|e| AppError::InternalServerError(format!("mkdir task failed: {}", e)))?
        .map_err(|e| AppError::Io(e, format!("mkdir {}", path.display())))
}

pub fn open_direct_write_blocking(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        ensure_dir_all_blocking(parent)?;
    }

    let fd = fs::openat(
        CWD,
        path,
        OFlags::WRONLY | OFlags::CREATE | OFlags::TRUNC | OFlags::CLOEXEC | OFlags::DIRECT,
        create_mode(),
    )
    .map_err(into_io_error)?;
    Ok(owned_fd_to_file(fd))
}

pub fn open_buffered_write_blocking(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        ensure_dir_all_blocking(parent)?;
    }

    let fd = fs::openat(
        CWD,
        path,
        OFlags::WRONLY | OFlags::CREATE | OFlags::TRUNC | OFlags::CLOEXEC,
        create_mode(),
    )
    .map_err(into_io_error)?;
    Ok(owned_fd_to_file(fd))
}

pub async fn open_buffered_write(path: &Path) -> Result<File, AppError> {
    let path = path.to_path_buf();
    let task_path = path.clone();
    tokio::task::spawn_blocking(move || open_buffered_write_blocking(&task_path))
        .await
        .map_err(|e| AppError::InternalServerError(format!("open buffered task failed: {}", e)))?
        .map_err(|e| AppError::Io(e, format!("create {}", path.display())))
}

pub async fn open_direct_write(path: &Path) -> Result<File, AppError> {
    let path = path.to_path_buf();
    let task_path = path.clone();
    tokio::task::spawn_blocking(move || open_direct_write_blocking(&task_path))
        .await
        .map_err(|e| AppError::InternalServerError(format!("open direct task failed: {}", e)))?
        .map_err(|e| AppError::Io(e, format!("create {}", path.display())))
}

pub fn open_read_blocking(path: &Path) -> io::Result<File> {
    fs::openat(CWD, path, OFlags::RDONLY | OFlags::CLOEXEC, Mode::empty())
        .map(owned_fd_to_file)
        .map_err(into_io_error)
}

pub async fn open_read(path: &Path) -> Result<File, AppError> {
    let path = path.to_path_buf();
    let task_path = path.clone();
    tokio::task::spawn_blocking(move || open_read_blocking(&task_path))
        .await
        .map_err(|e| AppError::InternalServerError(format!("open read task failed: {}", e)))?
        .map_err(|e| AppError::Io(e, format!("open {}", path.display())))
}

pub fn pwrite_all_at<Fd: AsFd>(fd: Fd, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        let written = rio::pwrite(fd.as_fd(), buf, offset).map_err(into_io_error)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "pwrite returned 0",
            ));
        }
        offset = offset.saturating_add(written as u64);
        buf = &buf[written..];
    }
    Ok(())
}

pub fn pread_exact_at<Fd: AsFd>(fd: Fd, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        let read = rio::pread(fd.as_fd(), &mut *buf, offset).map_err(into_io_error)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "pread returned EOF",
            ));
        }
        offset = offset.saturating_add(read as u64);
        let (_, rest) = buf.split_at_mut(read);
        buf = rest;
    }
    Ok(())
}

pub fn pread_at<Fd: AsFd>(fd: Fd, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    rio::pread(fd.as_fd(), buf, offset).map_err(into_io_error)
}

/// Read into an owned, reusable chunk; a short read and EOF are not errors.
/// Accepted buffers remain owned through completion if the caller disappears.
pub async fn read_at_async(
    file: Arc<File>,
    mut buffer: Vec<u8>,
    offset: u64,
) -> io::Result<Vec<u8>> {
    if let Some(native) = &selected_backend()?.native {
        return match native
            .read_some(file, uring::Buffer::Bytes(buffer), offset)
            .await?
        {
            uring::Buffer::Bytes(buffer) => Ok(buffer),
            uring::Buffer::Aligned(_) => {
                unreachable!("read changed buffer type")
            }
        };
    }
    tokio::task::spawn_blocking(move || {
        let read = pread_at(&*file, &mut buffer, offset)?;
        buffer.truncate(read);
        Ok(buffer)
    })
    .await
    .map_err(|error| io::Error::other(error.to_string()))?
}

pub async fn pread_exact_at_async(
    file: File,
    len: usize,
    offset: u64,
) -> io::Result<(File, Vec<u8>)> {
    if let Some(native) = &selected_backend()?.native {
        let owned_file = Arc::new(file.try_clone()?);
        let buffer = native
            .read_exact(owned_file, uring::Buffer::Bytes(vec![0; len]), offset)
            .await?;
        return match buffer {
            uring::Buffer::Bytes(bytes) => Ok((file, bytes)),
            uring::Buffer::Aligned(_) => {
                unreachable!("read changed buffer type")
            }
        };
    }
    tokio::task::spawn_blocking(move || {
        let mut buf = vec![0u8; len];
        pread_exact_at(&file, &mut buf, offset)?;
        Ok::<_, io::Error>((file, buf))
    })
    .await
    .map_err(io::Error::other)?
}

pub fn fdatasync_blocking<Fd: AsFd>(fd: Fd) -> io::Result<()> {
    selected_backend()?;
    fs::fdatasync(fd).map_err(into_io_error)
}

pub fn fsync_blocking<Fd: AsFd>(fd: Fd) -> io::Result<()> {
    selected_backend()?;
    fs::fsync(fd).map_err(into_io_error)
}

pub fn sync_dir_blocking(path: &Path) -> io::Result<()> {
    selected_backend()?;
    let fd = fs::openat(
        CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(into_io_error)?;
    fsync_blocking(&fd)
}

pub async fn sync_dir(path: &Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    let task_path = path.clone();
    let file = tokio::task::spawn_blocking(move || {
        fs::openat(
            CWD,
            &task_path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map(owned_fd_to_file)
        .map_err(into_io_error)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task failed: {}", e)))?
    .map_err(|e| AppError::Io(e, format!("open sync dir {}", path.display())))?;
    fsync_async(Arc::new(file))
        .await
        .map_err(|e| AppError::Io(e, format!("sync dir {}", path.display())))
}

pub fn rename_blocking(source: &Path, destination: &Path) -> io::Result<()> {
    fs::renameat(CWD, source, CWD, destination).map_err(into_io_error)
}

pub async fn rename(source: &Path, destination: &Path) -> Result<(), AppError> {
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    let task_source = source.clone();
    let task_destination = destination.clone();
    tokio::task::spawn_blocking(move || rename_blocking(&task_source, &task_destination))
        .await
        .map_err(|e| AppError::InternalServerError(format!("rename task failed: {}", e)))?
        .map_err(|e| {
            AppError::Io(
                e,
                format!("rename {} -> {}", source.display(), destination.display()),
            )
        })
}

pub fn rename_exchange_blocking(left: &Path, right: &Path) -> io::Result<()> {
    fs::renameat_with(CWD, left, CWD, right, RenameFlags::EXCHANGE).map_err(into_io_error)
}

pub async fn remove_file_if_exists(path: &Path) -> Result<bool, AppError> {
    let path = path.to_path_buf();
    let task_path = path.clone();
    tokio::task::spawn_blocking(
        move || match fs::unlinkat(CWD, &task_path, AtFlags::empty()) {
            Ok(()) => Ok(true),
            Err(rustix::io::Errno::NOENT) => Ok(false),
            Err(err) => Err(into_io_error(err)),
        },
    )
    .await
    .map_err(|e| AppError::InternalServerError(format!("unlink task failed: {}", e)))?
    .map_err(|e| AppError::Io(e, format!("unlink {}", path.display())))
}

pub fn set_len_blocking(file: &File, len: u64) -> io::Result<()> {
    file.set_len(len)
}

pub fn atomic_write_blocking_with_durability(
    path: &Path,
    data: &[u8],
    strict_durability: bool,
) -> io::Result<()> {
    atomic_write_using(path, strict_durability, |fd| {
        write_bytes_at_blocking(fd, data, 0)
    })
}

pub(crate) fn atomic_write_shared_blocking_with_durability(
    path: &Path,
    data: Arc<Vec<u8>>,
    strict_durability: bool,
) -> io::Result<()> {
    atomic_write_using(path, strict_durability, |fd| {
        write_shared_at_blocking(fd, data, 0)
    })
}

fn atomic_write_using(
    path: &Path,
    strict_durability: bool,
    write: impl FnOnce(&OwnedFd) -> io::Result<()>,
) -> io::Result<()> {
    selected_backend()?;
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no parent directory for {}", path.display()),
        )
    })?;
    ensure_dir_all_blocking(parent)?;

    let tmp = temp_sibling_path(path);
    let fd = fs::openat(
        CWD,
        &tmp,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
        create_mode(),
    )
    .map_err(into_io_error)?;

    write(&fd)?;
    if strict_durability {
        fsync_blocking(&fd)?;
    }
    drop(fd);

    rename_blocking(&tmp, path).inspect_err(|_| {
        let _ = fs::unlinkat(CWD, &tmp, AtFlags::empty());
    })?;
    if strict_durability {
        sync_dir_blocking(parent)?;
    }
    Ok(())
}

pub fn atomic_write_blocking(path: &Path, data: &[u8]) -> io::Result<()> {
    atomic_write_blocking_with_durability(path, data, true)
}

pub async fn atomic_write(path: &Path, data: Vec<u8>) -> Result<(), AppError> {
    let path = path.to_path_buf();
    let task_path = path.clone();
    tokio::task::spawn_blocking(move || atomic_write_blocking(&task_path, &data))
        .await
        .map_err(|e| AppError::InternalServerError(format!("atomic write task failed: {}", e)))?
        .map_err(|e| AppError::Io(e, format!("atomic write {}", path.display())))
}

pub async fn atomic_write_with_durability(
    path: &Path,
    data: Vec<u8>,
    strict_durability: bool,
) -> Result<(), AppError> {
    let path = path.to_path_buf();
    let task_path = path.clone();
    tokio::task::spawn_blocking(move || {
        atomic_write_shared_blocking_with_durability(&task_path, Arc::new(data), strict_durability)
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("atomic write task failed: {}", e)))?
    .map_err(|e| AppError::Io(e, format!("atomic write {}", path.display())))
}

fn temp_sibling_path(path: &Path) -> PathBuf {
    let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
    let nonce = ulid::Ulid::new().to_string();
    path.with_file_name(format!(".{}.tmp.{}", file_name, nonce))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ec::{ALIGNMENT, AlignedBuffer};

    #[test]
    fn fallback_is_limited_to_backend_initialization_availability_errors() {
        for code in [
            libc::ENOSYS,
            libc::EOPNOTSUPP,
            libc::EPERM,
            libc::EACCES,
            libc::EINVAL,
        ] {
            assert!(initialization_allows_fallback(
                &io::Error::from_raw_os_error(code)
            ));
        }
        for code in [
            libc::ENOSPC,
            libc::EDQUOT,
            libc::EIO,
            libc::ENOMEM,
            libc::EMFILE,
        ] {
            assert!(!initialization_allows_fallback(
                &io::Error::from_raw_os_error(code)
            ));
        }
    }

    #[test]
    fn directory_fast_path_preserves_missing_parent_and_non_directory_errors() {
        let root = std::env::temp_dir().join(format!("n1-directory-fast-{}", ulid::Ulid::new()));
        let directory = root.join("parent/child");
        ensure_dir_all_blocking(&directory).unwrap();
        ensure_dir_all_blocking(&directory).unwrap();
        let file = directory.join("not-a-directory");
        std::fs::write(&file, b"unchanged").unwrap();
        assert!(ensure_dir_all_blocking(&file).is_err());
        assert!(ensure_dir_all_blocking(&file.join("child")).is_err());
        assert_eq!(std::fs::read(&file).unwrap(), b"unchanged");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn directory_creation_only_attempts_missing_suffix_and_preserves_ancestors() {
        use std::os::unix::fs::MetadataExt;

        let root = std::env::temp_dir().join(format!("n1-directory-suffix-{}", ulid::Ulid::new()));
        let fanout = root.join("existing/fanout/00");
        std::fs::create_dir_all(&fanout).unwrap();
        let ancestors = [
            root.clone(),
            root.join("existing"),
            root.join("existing/fanout"),
        ];
        let before = ancestors
            .iter()
            .map(|path| {
                let metadata = std::fs::metadata(path).unwrap();
                (metadata.ino(), metadata.mtime(), metadata.mtime_nsec())
            })
            .collect::<Vec<_>>();
        let fanout_inode = std::fs::metadata(&fanout).unwrap().ino();
        let target = fanout.join("object/version");

        ENSURE_DIR_MKDIR_ATTEMPTS.with(|attempts| attempts.set(0));
        ensure_dir_all_blocking(&target).unwrap();
        assert_eq!(ENSURE_DIR_MKDIR_ATTEMPTS.with(|attempts| attempts.get()), 2);
        assert!(target.is_dir());
        assert_eq!(std::fs::metadata(&fanout).unwrap().ino(), fanout_inode);
        for (path, expected) in ancestors.iter().zip(before) {
            let metadata = std::fs::metadata(path).unwrap();
            assert_eq!(
                (metadata.ino(), metadata.mtime(), metadata.mtime_nsec()),
                expected,
                "an established ancestor was changed: {}",
                path.display()
            );
        }

        ENSURE_DIR_MKDIR_ATTEMPTS.with(|attempts| attempts.set(0));
        ensure_dir_all_blocking(&target).unwrap();
        assert_eq!(ENSURE_DIR_MKDIR_ATTEMPTS.with(|attempts| attempts.get()), 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn directory_creation_accepts_concurrently_created_common_prefixes() {
        let root = std::env::temp_dir().join(format!("n1-directory-race-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        let start = std::sync::Barrier::new(16);
        std::thread::scope(|scope| {
            let workers = (0..16)
                .map(|worker| {
                    let root = &root;
                    let start = &start;
                    scope.spawn(move || {
                        start.wait();
                        ensure_dir_all_blocking(&root.join("shared/common/object/version"))?;
                        ensure_dir_all_blocking(
                            &root.join(format!("shared/member-{worker}/object/version")),
                        )
                    })
                })
                .collect::<Vec<_>>();
            for worker in workers {
                worker.join().unwrap().unwrap();
            }
        });
        assert!(root.join("shared/common/object/version").is_dir());
        for worker in 0..16 {
            assert!(
                root.join(format!("shared/member-{worker}/object/version"))
                    .is_dir()
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn directory_creation_preserves_relative_and_parent_component_semantics() {
        let root =
            std::env::temp_dir().join(format!("n1-directory-relative-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        let absolute_root = std::fs::canonicalize(&root).unwrap();
        // Build a relative route without changing the process-wide working
        // directory while other tests may be performing filesystem operations.
        let depth = std::env::current_dir()
            .unwrap()
            .components()
            .filter(|component| matches!(component, std::path::Component::Normal(_)))
            .count();
        let mut relative_root = PathBuf::from(".");
        for _ in 0..depth {
            relative_root.push("..");
        }
        relative_root.push(absolute_root.strip_prefix("/").unwrap());

        ensure_dir_all_blocking(&relative_root.join("direct/child")).unwrap();
        ensure_dir_all_blocking(&relative_root.join("trailing-dot/.")).unwrap();
        ensure_dir_all_blocking(&relative_root.join("needed/../sibling/child")).unwrap();
        assert!(root.join("direct/child").is_dir());
        assert!(root.join("trailing-dot").is_dir());
        assert!(root.join("needed").is_dir());
        assert!(root.join("sibling/child").is_dir());
        ensure_dir_all_blocking(Path::new("")).unwrap();
        ensure_dir_all_blocking(Path::new(".")).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn directory_creation_preserves_directory_symlinks_and_rejects_invalid_targets() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!("n1-directory-links-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(root.join("target")).unwrap();
        symlink("target", root.join("directory-link")).unwrap();
        ensure_dir_all_blocking(&root.join("directory-link/object/version")).unwrap();
        assert!(root.join("target/object/version").is_dir());
        assert_eq!(
            std::fs::read_link(root.join("directory-link")).unwrap(),
            Path::new("target")
        );

        std::fs::write(root.join("file"), b"unchanged").unwrap();
        symlink("file", root.join("file-link")).unwrap();
        assert!(ensure_dir_all_blocking(&root.join("file-link/child")).is_err());
        assert_eq!(std::fs::read(root.join("file")).unwrap(), b"unchanged");

        symlink("absent", root.join("broken-link")).unwrap();
        assert!(ensure_dir_all_blocking(&root.join("broken-link/child")).is_err());
        assert!(!root.join("absent").exists());
        assert_eq!(
            std::fs::read_link(root.join("broken-link")).unwrap(),
            Path::new("absent")
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    fn direct_io_block(value: u8) -> AlignedBuffer {
        let mut block = AlignedBuffer::new(ALIGNMENT);
        block.extend_from_slice(&vec![value; ALIGNMENT]);
        assert_eq!(block.as_slice().as_ptr() as usize % ALIGNMENT, 0);
        block
    }

    #[test]
    fn pwrite_and_pread_are_positioned() {
        let root = std::env::temp_dir().join(format!("n1-linux-io-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).expect("create temp root");
        let path = root.join("positioned.bin");

        let file = open_direct_write_blocking(&path).expect("open direct write");
        file.set_len((ALIGNMENT * 2) as u64).expect("preallocate");
        let first_block = direct_io_block(1);
        let second_block = direct_io_block(2);
        pwrite_all_at(&file, first_block.as_slice(), 0).expect("write first block");
        pwrite_all_at(&file, second_block.as_slice(), ALIGNMENT as u64)
            .expect("write second block");
        fdatasync_blocking(&file).expect("sync data");
        drop(file);

        let file = open_read_blocking(&path).expect("open read");
        let mut first = vec![0u8; ALIGNMENT];
        let mut second = vec![0u8; ALIGNMENT];
        pread_exact_at(&file, &mut second, ALIGNMENT as u64).expect("read second");
        pread_exact_at(&file, &mut first, 0).expect("read first");
        assert_eq!(first, vec![1u8; ALIGNMENT]);
        assert_eq!(second, vec![2u8; ALIGNMENT]);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn atomic_write_publishes_and_syncs_parent() {
        let root = std::env::temp_dir().join(format!("n1-linux-io-{}", ulid::Ulid::new()));
        let path = root.join("meta.json");
        atomic_write_blocking(&path, b"{\"ok\":true}").expect("atomic write");
        assert_eq!(
            std::fs::read_to_string(&path).expect("read metadata"),
            "{\"ok\":true}"
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
