use super::*;

pub struct DirectWriter {
    file: Arc<fs::File>,
    write_buffer: AlignedBuffer,
    next_offset: u64,
    write_task: Option<tokio::task::JoinHandle<Result<AlignedBuffer, std::io::Error>>>,
}

pub(super) enum UploadShardWriter {
    Direct(DirectWriter),
    Buffered(tokio_fs::File),
}

impl UploadShardWriter {
    pub(super) async fn sync_data(&self) -> Result<(), std::io::Error> {
        match self {
            Self::Direct(writer) => writer.sync_data().await,
            Self::Buffered(file) => file.sync_data().await,
        }
    }
}

impl AsyncWrite for UploadShardWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match &mut *self {
            Self::Direct(writer) => Pin::new(writer).poll_write(cx, buf),
            Self::Buffered(file) => Pin::new(file).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            Self::Direct(writer) => Pin::new(writer).poll_flush(cx),
            Self::Buffered(file) => Pin::new(file).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            Self::Direct(writer) => Pin::new(writer).poll_shutdown(cx),
            Self::Buffered(file) => Pin::new(file).poll_shutdown(cx),
        }
    }
}

#[cfg(target_os = "linux")]
fn write_all_at_blocking(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    let mut written = 0usize;
    while written < buf.len() {
        let n = file.write_at(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "write_at returned 0",
            ));
        }
        written += n;
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn write_all_at_blocking(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let mut cloned = file.try_clone()?;
    cloned.seek(SeekFrom::Start(offset))?;
    cloned.write_all(buf)
}

#[cfg(target_os = "linux")]
fn write_all_at_io_uring(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    let mut ring = IoUring::new(8)?;
    let fd = types::Fd(file.as_raw_fd());
    let mut written = 0usize;

    while written < buf.len() {
        let ptr = unsafe { buf.as_ptr().add(written) };
        let len = (buf.len() - written) as u32;
        let entry = opcode::Write::new(fd, ptr, len)
            .offset(offset + written as u64)
            .build()
            .user_data(1);

        unsafe {
            ring.submission()
                .push(&entry)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "io_uring submission full"))?;
        }

        ring.submit_and_wait(1)?;
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "io_uring completion missing"))?;
        let result = cqe.result();
        if result < 0 {
            return Err(io::Error::from_raw_os_error(-result));
        }
        if result == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "io_uring write returned 0",
            ));
        }
        written += result as usize;
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn write_all_direct(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    if *IO_URING_ENABLED {
        write_all_at_io_uring(file, buf, offset)
    } else {
        write_all_at_blocking(file, buf, offset)
    }
}

#[cfg(not(target_os = "linux"))]
fn write_all_direct(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    write_all_at_blocking(file, buf, offset)
}

#[cfg(target_os = "linux")]
fn sync_file_direct(file: &fs::File) -> io::Result<()> {
    if *IO_URING_ENABLED {
        let mut ring = IoUring::new(8)?;
        let entry = opcode::Fsync::new(types::Fd(file.as_raw_fd()))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(1);
        unsafe {
            ring.submission()
                .push(&entry)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "io_uring submission full"))?;
        }
        ring.submit_and_wait(1)?;
        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "io_uring completion missing"))?;
        let result = cqe.result();
        if result < 0 {
            return Err(io::Error::from_raw_os_error(-result));
        }
        Ok(())
    } else {
        file.sync_data()
    }
}

#[cfg(not(target_os = "linux"))]
fn sync_file_direct(file: &fs::File) -> io::Result<()> {
    file.sync_data()
}

impl DirectWriter {
    pub fn new(file: fs::File) -> Self {
        let configured = WRITER_BUFFER_SIZE_ATOMIC
            .get()
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(256 * 1024);
        let aligned = ((configured.max(4096) + 4095) / 4096) * 4096;
        Self {
            file: Arc::new(file),
            write_buffer: AlignedBuffer::new(aligned),
            next_offset: 0,
            write_task: None,
        }
    }

    fn poll_write_task(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if let Some(task) = &mut self.write_task {
            match Pin::new(task).poll(cx) {
                Poll::Ready(Ok(Ok(returned_buffer))) => {
                    self.write_buffer = returned_buffer;
                    self.write_task = None;
                }
                Poll::Ready(Ok(Err(e))) => {
                    self.write_task = None;
                    return Poll::Ready(Err(e));
                }
                Poll::Ready(Err(e)) => {
                    self.write_task = None;
                    return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }

    fn spawn_buffer_flush(&mut self) {
        if self.write_buffer.len() == 0 || self.write_task.is_some() {
            return;
        }

        let file = self.file.clone();
        let offset = self.next_offset;
        let mut to_write = std::mem::replace(&mut self.write_buffer, AlignedBuffer::new(0));
        self.next_offset = self.next_offset.saturating_add(to_write.len() as u64);

        let task = tokio::task::spawn_blocking(move || {
            write_all_direct(&file, to_write.as_slice(), offset)?;
            to_write.clear();
            Ok::<AlignedBuffer, std::io::Error>(to_write)
        });

        self.write_task = Some(task);
    }

    pub async fn sync_data(&self) -> Result<(), std::io::Error> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || sync_file_direct(&file)).await?
    }
}

impl AsyncWrite for DirectWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if matches!(self.poll_write_task(cx), Poll::Pending) {
            return Poll::Pending;
        }

        if self.write_buffer.len() == self.write_buffer.capacity() {
            self.spawn_buffer_flush();
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let remaining = self.write_buffer.capacity() - self.write_buffer.len();
        let to_copy = std::cmp::min(remaining, buf.len());
        self.write_buffer.extend_from_slice(&buf[..to_copy]);

        if self.write_buffer.len() == self.write_buffer.capacity() {
            self.spawn_buffer_flush();
        }

        Poll::Ready(Ok(to_copy))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if self.write_buffer.len() > 0 && self.write_task.is_none() {
            self.spawn_buffer_flush();
        }

        if matches!(self.poll_write_task(cx), Poll::Ready(Ok(()))) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.as_mut().poll_flush(cx)
    }
}

pub async fn create_direct_file(path: &PathBuf) -> Result<fs::File, AppError> {
    let path_clone = path.clone();

    if let Some(parent) = path.parent() {
        let _ = tokio_fs::create_dir_all(parent).await;
    }

    tokio::task::spawn_blocking(move || {
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);

        #[cfg(target_os = "linux")]
        {
            options.custom_flags(libc::O_DIRECT);
        }

        #[cfg(windows)]
        {
            const FILE_FLAG_NO_BUFFERING: u32 = 0x20000000;
            options.custom_flags(FILE_FLAG_NO_BUFFERING);
        }

        options
            .open(&path_clone)
            .map_err(|e| AppError::Io(e, format!("create {}", path_clone.display())))
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("Spawn blocking failed: {}", e)))?
}

pub(super) async fn create_buffered_file(path: &PathBuf) -> Result<tokio_fs::File, AppError> {
    if let Some(parent) = path.parent() {
        tokio_fs::create_dir_all(parent)
            .await
            .map_err(|e| AppError::Io(e, format!("create {}", parent.display())))?;
    }

    tokio_fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("create {}", path.display())))
}

#[cfg(unix)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let dir = tokio_fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open dir for sync: {:?}", path)))?;

    dir.sync_all()
        .await
        .map_err(|e| AppError::Io(e, format!("sync dir: {:?}", path)))?;
    Ok(())
}

#[cfg(windows)]
pub async fn sync_dir(path: &std::path::Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;

        let result = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&path);

        match result {
            Ok(file) => {
                let _ = file.sync_all();
            }
            Err(e) => {
                warn!(
                    "Windows: Failed to open directory for sync (non-fatal): {} - {}",
                    path.display(),
                    e
                );
            }
        }

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::InternalServerError(format!("sync dir task: {}", e)))?
}

#[cfg(not(any(unix, windows)))]
pub async fn sync_dir(_path: &std::path::Path) -> Result<(), AppError> {
    Ok(())
}

pub async fn write_atomic(path: &std::path::Path, data: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| {
        AppError::InternalServerError(format!("No parent dir for {}", path.display()))
    })?;

    tokio_fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, "create parent dir".into()))?;

    let tmp = parent.join(format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        Ulid::new().to_string()
    ));

    let mut f = tokio_fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|e| AppError::Io(e, format!("open tmp {}", tmp.display())))?;

    f.write_all(data)
        .await
        .map_err(|e| AppError::Io(e, "write tmp".into()))?;

    f.sync_all()
        .await
        .map_err(|e| AppError::Io(e, "sync tmp".into()))?;
    drop(f);

    tokio_fs::rename(&tmp, path)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {} -> {}", tmp.display(), path.display())))?;

    sync_dir(parent).await?;
    Ok(())
}
