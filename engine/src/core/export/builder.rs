use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use async_zip::{Compression, ZipEntryBuilder, tokio::write::ZipFileWriter};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::fs::{self, File};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_stream::wrappers::ReceiverStream;
use tokio_tar::{Builder as TarBuilder, Header as TarHeader};
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use tokio_util::io::StreamReader;

use crate::core::archive::create_multipart_range_reader;
use crate::core::index;
use crate::core::io_scheduler::IoProgress;
use crate::enums::app_error_enum::AppError;

use super::models::{ArtifactChunk, ExportConfig, ExportManifestEntry};
use super::store::{artifact_job_directory, cleanup_artifact_chunks, manifest_path};

const ARCHIVE_PIPE_BYTES: usize = 4 * 1024 * 1024;
const OBJECT_PIPE_CAPACITY: usize = 4;
const OBJECT_READ_BYTES: usize = 256 * 1024;

pub(super) struct ActiveProgress {
    pub cancel: Arc<AtomicBool>,
    pub objects_completed: AtomicU64,
    pub source_bytes_completed: AtomicU64,
    scheduler_progress: OnceLock<IoProgress>,
}

impl ActiveProgress {
    pub(super) fn new() -> Self {
        Self {
            cancel: Arc::new(AtomicBool::new(false)),
            objects_completed: AtomicU64::new(0),
            source_bytes_completed: AtomicU64::new(0),
            scheduler_progress: OnceLock::new(),
        }
    }

    pub(super) fn attach_scheduler_progress(&self, progress: IoProgress) {
        let _ = self.scheduler_progress.set(progress);
    }

    fn record_source_bytes(&self, bytes: u64) {
        self.source_bytes_completed
            .fetch_add(bytes, Ordering::Relaxed);
        if let Some(progress) = self.scheduler_progress.get() {
            progress.record(bytes);
        }
    }

    pub(super) fn ensure_running(&self) -> Result<(), AppError> {
        if let Some(progress) = self.scheduler_progress.get() {
            progress.heartbeat();
        }
        if self.cancel.load(Ordering::Acquire) {
            Err(AppError::Conflict("export cancelled".to_string()))
        } else {
            Ok(())
        }
    }
}

pub(super) struct BuiltArtifact {
    pub chunks: Vec<ArtifactChunk>,
    pub size_bytes: u64,
    pub sha256: String,
}

struct PreparedEntry {
    manifest: ExportManifestEntry,
    receiver: mpsc::Receiver<Result<Bytes, std::io::Error>>,
    task: JoinHandle<()>,
}

struct ManifestPrefetcher {
    lines: tokio::io::Lines<BufReader<File>>,
    queue: VecDeque<PreparedEntry>,
    fragment: String,
    progress: Arc<ActiveProgress>,
    concurrency: usize,
    exhausted: bool,
}

impl ManifestPrefetcher {
    async fn open(
        manifest_path: &Path,
        fragment: String,
        progress: Arc<ActiveProgress>,
        concurrency: usize,
    ) -> Result<Self, AppError> {
        let file = File::open(manifest_path)
            .await
            .map_err(|error| AppError::Io(error, "open N1 export manifest".to_string()))?;
        Ok(Self {
            lines: BufReader::new(file).lines(),
            queue: VecDeque::new(),
            fragment,
            progress,
            concurrency: concurrency.max(1),
            exhausted: false,
        })
    }

    async fn fill(&mut self) -> Result<(), AppError> {
        while !self.exhausted && self.queue.len() < self.concurrency {
            self.progress.ensure_running()?;
            let line = self
                .lines
                .next_line()
                .await
                .map_err(|error| AppError::Io(error, "read N1 export manifest".to_string()))?;
            let Some(line) = line else {
                self.exhausted = true;
                break;
            };
            let manifest: ExportManifestEntry = serde_json::from_str(&line).map_err(|error| {
                AppError::InternalServerError(format!("decode N1 export manifest entry: {error}"))
            })?;
            self.queue.push_back(spawn_object_reader(
                self.fragment.clone(),
                manifest,
                Arc::clone(&self.progress),
            ));
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<PreparedEntry>, AppError> {
        self.fill().await?;
        let entry = self.queue.pop_front();
        self.fill().await?;
        Ok(entry)
    }

    fn abort_remaining(&mut self) {
        for entry in self.queue.drain(..) {
            entry.task.abort();
        }
    }
}

fn io_error(context: &str, error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(format!("{context}: {error}"))
}

fn spawn_object_reader(
    fragment: String,
    manifest: ExportManifestEntry,
    progress: Arc<ActiveProgress>,
) -> PreparedEntry {
    let (sender, receiver) = mpsc::channel(OBJECT_PIPE_CAPACITY);
    let task_manifest = manifest.clone();
    let task = tokio::spawn(async move {
        let result = async {
            progress
                .ensure_running()
                .map_err(|error| io_error("export cancelled", error))?;
            let metadata = index::read_object_version(
                &fragment,
                &task_manifest.object_id,
                &task_manifest.version_id,
            )
            .await
            .map_err(|error| io_error("read immutable object metadata", error))?;
            if metadata.is_delete_marker
                || metadata.version_id != task_manifest.version_id
                || task_manifest
                    .source_offset_bytes
                    .checked_add(task_manifest.size_bytes)
                    .is_none_or(|end| end > metadata.total_size_bytes)
            {
                return Err(io_error(
                    "validate immutable object version",
                    format!(
                        "object '{}' changed or is unavailable",
                        task_manifest.archive_path
                    ),
                ));
            }

            let mut reader = create_multipart_range_reader(
                &metadata,
                task_manifest.source_offset_bytes,
                task_manifest.size_bytes,
            );
            let mut buffer = vec![0_u8; OBJECT_READ_BYTES];
            loop {
                progress
                    .ensure_running()
                    .map_err(|error| io_error("export cancelled", error))?;
                let read = reader
                    .read(&mut buffer)
                    .await
                    .map_err(|error| io_error("read immutable object bytes", error))?;
                if read == 0 {
                    break;
                }
                let chunk = Bytes::copy_from_slice(&buffer[..read]);
                if sender.send(Ok(chunk)).await.is_err() {
                    return Ok(());
                }
                progress.record_source_bytes(read as u64);
            }
            Ok(())
        }
        .await;

        if let Err(error) = result {
            let _ = sender.send(Err(error)).await;
        }
    });
    PreparedEntry {
        manifest,
        receiver,
        task,
    }
}

async fn finish_prepared_entry(task: JoinHandle<()>) -> Result<(), AppError> {
    task.await.map_err(|error| {
        AppError::InternalServerError(format!("N1 export object reader task failed: {error}"))
    })
}

async fn build_zip(
    manifest_path: &Path,
    fragment: String,
    writer: tokio::io::DuplexStream,
    progress: Arc<ActiveProgress>,
    concurrency: usize,
) -> Result<(), AppError> {
    let mut zip = ZipFileWriter::with_tokio(writer).force_zip64();
    let mut prefetcher =
        ManifestPrefetcher::open(manifest_path, fragment, Arc::clone(&progress), concurrency)
            .await?;
    let result = async {
        while let Some(prepared) = prefetcher.next().await? {
            progress.ensure_running()?;
            let PreparedEntry {
                manifest,
                receiver,
                task,
            } = prepared;
            let builder = ZipEntryBuilder::new(manifest.archive_path.into(), Compression::Stored);
            let mut entry_writer = zip.write_entry_stream(builder).await.map_err(|error| {
                AppError::Io(
                    io_error("create ZIP64 export entry", error),
                    "create ZIP64 export entry".to_string(),
                )
            })?;
            let stream = ReceiverStream::new(receiver);
            let mut reader = StreamReader::new(stream);
            {
                let mut compat_writer = (&mut entry_writer).compat_write();
                tokio::io::copy(&mut reader, &mut compat_writer)
                    .await
                    .map_err(|error| AppError::Io(error, "write ZIP64 export entry".to_string()))?;
            }
            entry_writer.close().await.map_err(|error| {
                AppError::Io(
                    io_error("close ZIP64 export entry", error),
                    "close ZIP64 export entry".to_string(),
                )
            })?;
            finish_prepared_entry(task).await?;
            progress.objects_completed.fetch_add(1, Ordering::Relaxed);
        }
        zip.close().await.map_err(|error| {
            AppError::Io(
                io_error("finalize ZIP64 export", error),
                "finalize ZIP64 export".to_string(),
            )
        })?;
        Ok(())
    }
    .await;
    if result.is_err() {
        prefetcher.abort_remaining();
    }
    result
}

async fn build_tar(
    manifest_path: &Path,
    fragment: String,
    writer: tokio::io::DuplexStream,
    progress: Arc<ActiveProgress>,
    concurrency: usize,
) -> Result<(), AppError> {
    let mut tar = TarBuilder::new(writer);
    let mut prefetcher =
        ManifestPrefetcher::open(manifest_path, fragment, Arc::clone(&progress), concurrency)
            .await?;
    let result = async {
        while let Some(prepared) = prefetcher.next().await? {
            progress.ensure_running()?;
            let PreparedEntry {
                manifest,
                receiver,
                task,
            } = prepared;
            let mut header = TarHeader::new_gnu();
            header.set_size(manifest.size_bytes);
            header.set_mode(0o644);
            header.set_cksum();
            let stream = ReceiverStream::new(receiver);
            let reader = StreamReader::new(stream);
            tar.append_data(&mut header, &manifest.archive_path, reader)
                .await
                .map_err(|error| AppError::Io(error, "write TAR export entry".to_string()))?;
            finish_prepared_entry(task).await?;
            progress.objects_completed.fetch_add(1, Ordering::Relaxed);
        }
        tar.finish()
            .await
            .map_err(|error| AppError::Io(error, "finalize TAR export".to_string()))?;
        Ok(())
    }
    .await;
    if result.is_err() {
        prefetcher.abort_remaining();
    }
    result
}

async fn sync_parent(path: &Path) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("artifact chunk has no parent"))?
        .to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::File::open(parent).and_then(|directory| directory.sync_all())
    })
    .await
    .map_err(|error| std::io::Error::other(format!("artifact sync task: {error}")))?
}

async fn write_chunk_with_failover(
    id: String,
    index: u64,
    data: Vec<u8>,
    roots: Arc<Vec<PathBuf>>,
    first_root: usize,
) -> Result<(u64, PathBuf, u64), AppError> {
    let mut last_error = None;
    for attempt in 0..roots.len() {
        let root = &roots[(first_root + attempt) % roots.len()];
        let directory = artifact_job_directory(root, &id);
        if let Err(error) = fs::create_dir_all(&directory).await {
            last_error = Some(error);
            continue;
        }
        let final_path = directory.join(format!("chunk-{index:020}.bin"));
        let partial_path = directory.join(format!("chunk-{index:020}.partial"));
        let _ = fs::remove_file(&partial_path).await;
        let operation = async {
            let mut file = File::create(&partial_path).await?;
            file.write_all(&data).await?;
            file.flush().await?;
            file.sync_all().await?;
            drop(file);
            fs::rename(&partial_path, &final_path).await?;
            sync_parent(&final_path).await?;
            Ok::<(), std::io::Error>(())
        }
        .await;
        match operation {
            Ok(()) => return Ok((index, final_path, data.len() as u64)),
            Err(error) => {
                let _ = fs::remove_file(&partial_path).await;
                last_error = Some(error);
            }
        }
    }
    Err(AppError::Io(
        last_error.unwrap_or_else(|| std::io::Error::other("no export artifact roots")),
        format!("write N1 export artifact chunk {index}"),
    ))
}

async fn collect_chunk_result(
    tasks: &mut JoinSet<Result<(u64, PathBuf, u64), AppError>>,
    chunks: &mut Vec<(u64, PathBuf, u64)>,
) -> Result<(), AppError> {
    let result = tasks
        .join_next()
        .await
        .ok_or_else(|| AppError::InternalServerError("export chunk task disappeared".to_string()))?
        .map_err(|error| {
            AppError::InternalServerError(format!("export chunk task failed: {error}"))
        })??;
    chunks.push(result);
    Ok(())
}

pub(super) async fn persist_archive_chunks<R: AsyncRead + Unpin>(
    mut reader: R,
    id: String,
    roots: Vec<PathBuf>,
    config: ExportConfig,
    progress: Arc<ActiveProgress>,
) -> Result<BuiltArtifact, AppError> {
    let roots = Arc::new(roots);
    let start_root = id.bytes().fold(0usize, |value, byte| {
        value.wrapping_mul(31).wrapping_add(byte as usize)
    }) % roots.len();
    let mut hasher = Sha256::new();
    let mut tasks = JoinSet::new();
    let mut completed_chunks = Vec::new();
    let mut index = 0_u64;
    let mut total_size = 0_u64;

    loop {
        progress.ensure_running()?;
        let mut chunk = Vec::with_capacity(config.artifact_chunk_size_bytes);
        while chunk.len() < config.artifact_chunk_size_bytes {
            let remaining = config.artifact_chunk_size_bytes - chunk.len();
            let mut buffer = vec![0_u8; remaining.min(1024 * 1024)];
            let read = reader.read(&mut buffer).await.map_err(|error| {
                AppError::Io(error, "read generated export archive".to_string())
            })?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            chunk.extend_from_slice(&buffer[..read]);
            total_size = total_size.saturating_add(read as u64);
        }
        if chunk.is_empty() {
            break;
        }
        while tasks.len() >= config.artifact_write_concurrency.min(roots.len()).max(1) {
            collect_chunk_result(&mut tasks, &mut completed_chunks).await?;
        }
        let task_roots = Arc::clone(&roots);
        let task_id = id.clone();
        let root_index = (start_root + index as usize) % roots.len();
        tasks.spawn(write_chunk_with_failover(
            task_id, index, chunk, task_roots, root_index,
        ));
        index = index.saturating_add(1);
    }

    while !tasks.is_empty() {
        collect_chunk_result(&mut tasks, &mut completed_chunks).await?;
    }
    completed_chunks.sort_by_key(|(index, _, _)| *index);
    let mut logical_offset = 0_u64;
    let chunks = completed_chunks
        .into_iter()
        .map(|(index, path, size_bytes)| {
            let chunk = ArtifactChunk {
                index,
                path: path.to_string_lossy().to_string(),
                logical_offset,
                size_bytes,
            };
            logical_offset = logical_offset.saturating_add(size_bytes);
            chunk
        })
        .collect::<Vec<_>>();
    if logical_offset != total_size {
        return Err(AppError::InternalServerError(
            "N1 export artifact chunk accounting mismatch".to_string(),
        ));
    }
    Ok(BuiltArtifact {
        chunks,
        size_bytes: total_size,
        sha256: hex::encode(hasher.finalize()),
    })
}

pub(super) async fn build_artifact(
    id: &str,
    fragment: &str,
    format: &str,
    roots: Vec<PathBuf>,
    config: &ExportConfig,
    progress: Arc<ActiveProgress>,
) -> Result<BuiltArtifact, AppError> {
    cleanup_artifact_chunks(id).await;
    let manifest_path = manifest_path(id)?;
    let (reader, writer) = tokio::io::duplex(ARCHIVE_PIPE_BYTES);
    let sink_id = id.to_string();
    let sink_config = config.clone();
    let sink_progress = Arc::clone(&progress);
    let sink = tokio::spawn(async move {
        persist_archive_chunks(reader, sink_id, roots, sink_config, sink_progress).await
    });

    let build_result = match format {
        "tar" => {
            build_tar(
                &manifest_path,
                fragment.to_string(),
                writer,
                Arc::clone(&progress),
                config.object_read_concurrency,
            )
            .await
        }
        _ => {
            build_zip(
                &manifest_path,
                fragment.to_string(),
                writer,
                Arc::clone(&progress),
                config.object_read_concurrency,
            )
            .await
        }
    };
    let sink_result = sink.await.map_err(|error| {
        AppError::InternalServerError(format!("N1 export artifact task failed: {error}"))
    })?;
    match (build_result, sink_result) {
        (Ok(()), Ok(artifact)) => Ok(artifact),
        (Err(error), _) => {
            cleanup_artifact_chunks(id).await;
            Err(error)
        }
        (_, Err(error)) => {
            cleanup_artifact_chunks(id).await;
            Err(error)
        }
    }
}
