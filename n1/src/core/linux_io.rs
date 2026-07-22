use std::fs::File;
use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::path::{Component, Path, PathBuf};

use rustix::fs::{self, AtFlags, CWD, Mode, OFlags, RenameFlags};
use rustix::io as rio;

use crate::enums::app_error_enum::AppError;

fn into_io_error(err: rustix::io::Errno) -> io::Error {
    err.into()
}

fn owned_fd_to_file(fd: OwnedFd) -> File {
    File::from(fd)
}

fn create_mode() -> Mode {
    Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::ROTH
}

pub fn ensure_dir_all_blocking(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }

    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => {
                current.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                current.push(component.as_os_str());
            }
            Component::Normal(part) => {
                current.push(part);
                match fs::mkdirat(CWD, &current, Mode::from_raw_mode(0o755)) {
                    Ok(()) => {}
                    Err(rustix::io::Errno::EXIST) => {
                        fs::openat(
                            CWD,
                            &current,
                            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                            Mode::empty(),
                        )
                        .map(drop)
                        .map_err(into_io_error)?;
                    }
                    Err(err) => return Err(into_io_error(err)),
                }
            }
            Component::Prefix(_) => unreachable!("N1 is Linux-only"),
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

pub async fn pread_exact_at_async(
    file: File,
    len: usize,
    offset: u64,
) -> io::Result<(File, Vec<u8>)> {
    tokio::task::spawn_blocking(move || {
        let mut buf = vec![0u8; len];
        pread_exact_at(&file, &mut buf, offset)?;
        Ok::<_, io::Error>((file, buf))
    })
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
}

pub fn fdatasync_blocking<Fd: AsFd>(fd: Fd) -> io::Result<()> {
    fs::fdatasync(fd).map_err(into_io_error)
}

pub fn fsync_blocking<Fd: AsFd>(fd: Fd) -> io::Result<()> {
    fs::fsync(fd).map_err(into_io_error)
}

pub fn sync_dir_blocking(path: &Path) -> io::Result<()> {
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
    tokio::task::spawn_blocking(move || sync_dir_blocking(&task_path))
        .await
        .map_err(|e| AppError::InternalServerError(format!("sync dir task failed: {}", e)))?
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

    pwrite_all_at(&fd, data, 0)?;
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
        atomic_write_blocking_with_durability(&task_path, &data, strict_durability)
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
