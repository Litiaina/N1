//! One request-owned small-object wave, using the ordinary object layout.
//!
//! A disconnected HTTP caller cannot cancel an admitted publisher. Exact-key,
//! fragment, upload-activity and disk-admission guards remain owned until all
//! admitted payloads have drained and the last shared persistence barrier has
//! either completed or failed. Independent groups may share a sealed syncfs
//! round on the same filesystem, but no background publisher or commit queue.

use super::*;
use crate::core::disk_manager::ObjectMetadata;
use crate::core::index_store::batch::StagedMetadata;
use crate::core::io_scheduler::IoProgress;
use crate::core::linux_io::group::FilesystemGroup;
use crate::core::upload::small_batch::Plan as PayloadPlan;
use crate::namespace::flat::batch::StagedNamespace;
use crate::namespace::flat::{self, Pointer};
use crate::namespace::runtime::finalize_guard::FinalizeGuard;
use futures_util::{FutureExt, StreamExt, stream};
use std::path::PathBuf;

const MAX_GROUP_OBJECTS: usize = 64;
const MAX_GROUP_BYTES: usize = 8 * 1024 * 1024;

#[cfg(test)]
static ADMISSION_WAIT_FOR_TEST: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>> =
    std::sync::Mutex::new(None);
#[cfg(test)]
static SNAPSHOT_WAIT_FOR_TEST: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<()>>> =
    std::sync::Mutex::new(None);

pub(super) struct Input {
    pub index: usize,
    pub logical_path: String,
    pub response_key: String,
    pub content_type: String,
    pub bytes: Bytes,
}

#[derive(Clone)]
struct Original {
    index: usize,
    response_key: String,
    size_bytes: u64,
}

impl From<&Input> for Original {
    fn from(input: &Input) -> Self {
        Self {
            index: input.index,
            response_key: input.response_key.clone(),
            size_bytes: input.bytes.len() as u64,
        }
    }
}

#[derive(Clone)]
struct Failure {
    status: u16,
    message: String,
}

impl From<&AppError> for Failure {
    fn from(error: &AppError) -> Self {
        Self {
            status: batch_error_status(error).as_u16(),
            message: batch_error_message(error),
        }
    }
}

impl Original {
    fn failure(&self, failure: &Failure) -> IndexedStandardBatchUploadResult {
        IndexedStandardBatchUploadResult {
            index: self.index,
            result: StandardBatchUploadEntryResult {
                object_key: self.response_key.clone(),
                status: failure.status,
                version_id: None,
                size_bytes: Some(self.size_bytes),
                error: Some(failure.message.clone()),
            },
        }
    }

    fn success(&self, metadata: &ObjectMetadata) -> IndexedStandardBatchUploadResult {
        IndexedStandardBatchUploadResult {
            index: self.index,
            result: StandardBatchUploadEntryResult {
                object_key: self.response_key.clone(),
                status: StatusCode::CREATED.as_u16(),
                version_id: Some(metadata.version_id.clone()),
                size_bytes: Some(self.size_bytes),
                error: None,
            },
        }
    }
}

struct Resolved {
    input: Input,
    physical: String,
    existing: Option<Pointer>,
}

struct Admitted {
    entries: Vec<Resolved>,
    results: Vec<IndexedStandardBatchUploadResult>,
    _operation_guards: Vec<crate::core::index::ObjectOperationGuard>,
    _activities: Vec<crate::core::upload::UploadActivityGuard>,
}

struct Prepared {
    original: Original,
    logical_path: String,
    physical: String,
    payload: PayloadPlan,
    existing: Option<Pointer>,
    pool: Option<crate::core::storage_pool::StoragePool>,
}

struct PreparedGroup {
    entries: Vec<Prepared>,
    results: Vec<IndexedStandardBatchUploadResult>,
    roots: Vec<PathBuf>,
    filesystems: Option<FilesystemGroup>,
}

struct Staged {
    original: Original,
    logical_path: String,
    physical: String,
    metadata: ObjectMetadata,
    ndx: StagedMetadata,
    namespace: StagedNamespace,
}

/// The outer task is deliberately detached if its HTTP owner disappears. The
/// inner blocking owner additionally outlives cancellation of this async task.
pub(super) async fn commit(
    state: Arc<AppState>,
    claims: Claims,
    inputs: Vec<Input>,
    io_guard: Option<AdaptiveIoGuard>,
    concurrency: usize,
) -> Vec<IndexedStandardBatchUploadResult> {
    let originals = inputs.iter().map(Original::from).collect::<Vec<_>>();
    let outcome = tokio::spawn(async move {
        if !claims.write {
            return Err(AppError::Forbidden("Write permission required".into()));
        }
        validate_wave(&inputs)?;
        let fragment = index_store::stable_fragment_id_for_fragment(&claims.fragment);
        let lifecycle = crate::core::fragment_lifecycle::acquire_live(&fragment).await?;
        let keys = inputs
            .iter()
            .map(|input| input.logical_path.clone())
            .collect::<BTreeSet<_>>();
        let mut finalize_guards = Vec::with_capacity(keys.len());
        for key in &keys {
            finalize_guards.push(FinalizeGuard::acquire(format!("{fragment}\0{key}")).await?);
        }
        let resolve_fragment = fragment.clone();
        let (resolved, results) =
            tokio::task::spawn_blocking(move || resolve(&resolve_fragment, inputs))
                .await
                .map_err(|error| {
                    AppError::InternalServerError(format!("batch identity worker: {error}"))
                })?;
        // Waiting for physical locks must not occupy the Tokio blocking pool:
        // their owners and the payload opens below also need that executor.
        let Admitted {
            entries,
            results,
            _operation_guards,
            _activities,
        } = admit(&fragment, resolved, results).await?;
        let prepare_fragment = fragment.clone();
        let PreparedGroup {
            entries,
            results,
            roots,
            filesystems,
        } = tokio::task::spawn_blocking(move || prepare(&prepare_fragment, entries, results))
            .await
            .map_err(|error| {
                AppError::InternalServerError(format!("batch planning worker: {error}"))
            })??;
        let progress = io_guard.as_ref().map(AdaptiveIoGuard::progress_handle);
        let payload_started = Instant::now();
        let payload_bytes = entries.iter().map(|entry| entry.original.size_bytes).sum();
        // No blocking worker, namespace mutex or backup boundary is held while
        // this asynchronous stage schedules file opens/writes on Tokio's pool.
        // Join all entries, even if a neighbour fails or panics.
        let written = stream::iter(entries.into_iter().map(|entry| {
            let progress = progress.clone();
            async move {
                let outcome = std::panic::AssertUnwindSafe(entry.payload.write(progress))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| {
                        Err(AppError::InternalServerError(
                            "grouped payload worker panicked".into(),
                        ))
                    });
                (entry, outcome)
            }
        }))
        .buffer_unordered(concurrency.clamp(1, 16))
        .collect::<Vec<_>>()
        .await;
        let payload_elapsed = payload_started.elapsed();
        // Queue behind an exclusive backup asynchronously, before consuming a
        // blocking worker. Existing read owners can still schedule filesystem
        // work needed to finish and release the backup boundary.
        #[cfg(test)]
        if let Some(ready) = SNAPSHOT_WAIT_FOR_TEST.lock().unwrap().as_ref() {
            let _ = ready.send(());
        }
        let snapshot_guard = crate::core::backup::barrier::mutation_async().await;
        tokio::task::spawn_blocking(move || {
            let _lifecycle = lifecycle;
            let _finalize_guards = finalize_guards;
            let _operation_guards = _operation_guards;
            let _activities = _activities;
            let _boundary = crate::core::backup::barrier::Mutation::from_owned(snapshot_guard);
            let key_refs = keys.iter().map(String::as_str).collect::<Vec<_>>();
            // The owned synchronous publisher has no asynchronous dependency
            // on this same executor once it enters the namespace boundary.
            let result = flat::with_keys(&fragment, &key_refs, || {
                publish_group(
                    &state,
                    &fragment,
                    written,
                    results,
                    roots,
                    filesystems,
                    progress,
                    payload_elapsed,
                    payload_bytes,
                )
            });
            // An unsuccessful wave still retains its permit through all
            // completed work; Drop records failure rather than success.
            if result.as_ref().is_ok_and(|results| {
                results
                    .iter()
                    .all(|result| result.result.status == StatusCode::CREATED.as_u16())
            }) {
                if let Some(guard) = io_guard {
                    guard.complete();
                }
            }
            result
        })
        .await
        .map_err(|error| {
            AppError::InternalServerError(format!("batch publication worker: {error}"))
        })?
    })
    .await
    .unwrap_or_else(|error| {
        Err(AppError::InternalServerError(format!(
            "batch publication owner: {error}"
        )))
    });
    match outcome {
        Ok(results) => results,
        Err(error) => {
            let failure = Failure::from(&error);
            originals
                .iter()
                .map(|input| input.failure(&failure))
                .collect()
        }
    }
}

fn validate_wave(inputs: &[Input]) -> Result<(), AppError> {
    let total_bytes = inputs
        .iter()
        .try_fold(0usize, |sum, input| sum.checked_add(input.bytes.len()));
    if inputs.is_empty()
        || inputs.len() > MAX_GROUP_OBJECTS
        || total_bytes.is_none_or(|bytes| bytes > MAX_GROUP_BYTES)
    {
        return Err(AppError::BadRequest(
            "invalid small-object wave bounds".into(),
        ));
    }
    let mut keys = BTreeSet::new();
    let mut indices = BTreeSet::new();
    for input in inputs {
        if input.logical_path.is_empty()
            || !keys.insert(&input.logical_path)
            || !indices.insert(input.index)
        {
            return Err(AppError::BadRequest(
                "duplicate or empty small-object wave identity".into(),
            ));
        }
    }
    Ok(())
}

fn require_roots(roots: &[PathBuf]) -> Result<(), AppError> {
    if roots.is_empty()
        || roots
            .iter()
            .any(|root| !crate::core::storage_pool::root_available(root))
    {
        return Err(AppError::ServiceUnavailable(
            "a grouped upload holder is unavailable".into(),
        ));
    }
    Ok(())
}

fn barrier(
    filesystems: &FilesystemGroup,
    roots: &[PathBuf],
    progress: Option<&IoProgress>,
    label: &str,
) -> Result<(), AppError> {
    if let Some(progress) = progress {
        progress.heartbeat();
    }
    let started = Instant::now();
    let result = (|| {
        require_roots(roots)?;
        filesystems
            .sync_all()
            .map_err(|error| AppError::Io(error, "sync grouped upload filesystems".into()))?;
        require_roots(roots)
    })();
    record_io_latency(label, started.elapsed(), 0, result.is_ok());
    if let Some(progress) = progress {
        progress.heartbeat();
    }
    result
}

/// Read-only lookup uses a short filesystem worker, never lock waits or I/O
/// futures. Its result has no publication authority until the final recheck.
fn resolve(
    fragment: &str,
    inputs: Vec<Input>,
) -> (Vec<Resolved>, Vec<IndexedStandardBatchUploadResult>) {
    let mut results = Vec::with_capacity(inputs.len());
    let mut resolved = Vec::with_capacity(inputs.len());
    for input in inputs {
        match flat::lookup(fragment, &input.logical_path) {
            Ok(existing) => {
                let physical = existing
                    .as_ref()
                    .map(|pointer| pointer.object_id.clone())
                    .unwrap_or_else(|| {
                        if looks_like_flat_storage_object_id(&input.logical_path) {
                            input.logical_path.clone()
                        } else {
                            index_store::new_object_id()
                        }
                    });
                resolved.push(Resolved {
                    input,
                    physical,
                    existing,
                });
            }
            Err(error) => results.push(Original::from(&input).failure(&Failure::from(&error))),
        }
    }
    (resolved, results)
}

/// Lock waiting is asynchronous and outside the backup/key publication
/// boundary. Old lock owners may need both the pool and boundary to finish.
async fn admit(
    fragment: &str,
    resolved: Vec<Resolved>,
    mut results: Vec<IndexedStandardBatchUploadResult>,
) -> Result<Admitted, AppError> {
    let physical_ids = resolved
        .iter()
        .map(|entry| entry.physical.clone())
        .collect::<BTreeSet<_>>();
    let mut operation_guards = Vec::with_capacity(physical_ids.len());
    let mut activities = Vec::with_capacity(physical_ids.len());
    let mut activity_failures = HashMap::new();
    for physical in physical_ids {
        #[cfg(test)]
        if let Some(ready) = ADMISSION_WAIT_FOR_TEST.lock().unwrap().take() {
            let _ = ready.send(());
        }
        operation_guards
            .push(crate::core::index::acquire_object_operation_lock(fragment, &physical).await?);
        match crate::core::upload::UploadActivityGuard::begin(fragment, &physical) {
            Ok(activity) => activities.push(activity),
            Err(error) => {
                activity_failures.insert(physical, Failure::from(&error));
            }
        }
    }
    let mut entries = Vec::with_capacity(resolved.len());
    for entry in resolved {
        match activity_failures.get(&entry.physical) {
            Some(failure) => results.push(Original::from(&entry.input).failure(failure)),
            None => entries.push(entry),
        }
    }
    Ok(Admitted {
        entries,
        results,
        _operation_guards: operation_guards,
        _activities: activities,
    })
}

/// Plan and pin every participating filesystem before payload writes begin.
/// This short worker never acquires a mutex that waits on another I/O worker.
fn prepare(
    fragment: &str,
    resolved: Vec<Resolved>,
    mut results: Vec<IndexedStandardBatchUploadResult>,
) -> Result<PreparedGroup, AppError> {
    crate::core::operation_gate::ensure_user_mutations_allowed()?;
    let prepare_started = Instant::now();
    let mut prepared = Vec::with_capacity(resolved.len());
    let mut roots = BTreeSet::new();
    for resolved in resolved {
        let original = Original::from(&resolved.input);
        let preparation = (|| {
            let payload = PayloadPlan::new(
                fragment,
                &resolved.physical,
                &resolved.input.content_type,
                resolved.input.bytes,
            )?;
            let payload_roots = payload.roots()?;
            let pool = payload_roots
                .first()
                .and_then(|root| crate::core::storage_pool::pool_for_root(root));
            let namespace = flat::batch::plan(
                fragment,
                &resolved.input.logical_path,
                &resolved.physical,
                pool,
                resolved.existing.clone(),
            )?;
            roots.extend(payload_roots);
            roots.extend(namespace.roots().iter().cloned());
            Ok::<_, AppError>(Prepared {
                original: original.clone(),
                logical_path: resolved.input.logical_path,
                physical: resolved.physical,
                payload,
                existing: resolved.existing,
                pool: pool.cloned(),
            })
        })();
        match preparation {
            Ok(entry) => prepared.push(entry),
            Err(error) => results.push(original.failure(&Failure::from(&error))),
        }
    }
    if prepared.is_empty() {
        record_io_latency(
            "upload.batch.group_prepare",
            prepare_started.elapsed(),
            0,
            false,
        );
        return Ok(PreparedGroup {
            entries: prepared,
            results,
            roots: Vec::new(),
            filesystems: None,
        });
    }
    let roots = roots.into_iter().collect::<Vec<_>>();
    require_roots(&roots)?;
    let filesystems = FilesystemGroup::open(&roots)
        .map_err(|error| AppError::Io(error, "open grouped upload filesystems".into()))?;
    record_io_latency(
        "upload.batch.group_prepare",
        prepare_started.elapsed(),
        0,
        true,
    );

    Ok(PreparedGroup {
        entries: prepared,
        results,
        roots,
        filesystems: Some(filesystems),
    })
}

/// Only synchronous metadata work executes under the existing backup boundary
/// and sorted key mutexes. No file-open future is queued behind this worker.
#[allow(clippy::too_many_arguments)]
fn publish_group(
    state: &Arc<AppState>,
    fragment: &str,
    written: Vec<(Prepared, Result<ObjectMetadata, AppError>)>,
    mut results: Vec<IndexedStandardBatchUploadResult>,
    roots: Vec<PathBuf>,
    filesystems: Option<FilesystemGroup>,
    progress: Option<IoProgress>,
    payload_elapsed: Duration,
    payload_bytes: u64,
) -> Result<Vec<IndexedStandardBatchUploadResult>, AppError> {
    crate::core::operation_gate::ensure_user_mutations_allowed()?;
    let stage_started = Instant::now();
    let mut staged = Vec::with_capacity(written.len());
    for (entry, outcome) in written {
        let original = entry.original.clone();
        let preparation = outcome.and_then(|metadata| {
            let existing = flat::lookup(fragment, &entry.logical_path)?;
            let unchanged = match (&entry.existing, &existing) {
                (None, None) => true,
                (_, Some(pointer)) => pointer.object_id == entry.physical,
                (Some(_), None) => false,
            };
            if !unchanged {
                return Err(AppError::Conflict(
                    "Object identity changed while the upload was admitted; retry this object"
                        .into(),
                ));
            }
            let namespace = flat::batch::plan(
                fragment,
                &entry.logical_path,
                &entry.physical,
                entry.pool.as_ref(),
                existing,
            )?;
            if namespace.roots().iter().any(|root| !roots.contains(root)) {
                return Err(AppError::Conflict(
                    "Namespace placement changed while the upload was admitted; retry this object"
                        .into(),
                ));
            }
            let ndx = index_store::batch::stage(&metadata)?;
            let namespace_started = Instant::now();
            let namespace = namespace.stage(&metadata.version_id);
            record_io_latency(
                "upload.batch.group_ncx_prepare",
                namespace_started.elapsed(),
                0,
                namespace.is_ok(),
            );
            Ok(Staged {
                original: entry.original,
                logical_path: entry.logical_path,
                physical: entry.physical,
                metadata,
                ndx,
                namespace: namespace?,
            })
        });
        match preparation {
            Ok(entry) => staged.push(entry),
            Err(error) => results.push(original.failure(&Failure::from(&error))),
        }
    }
    record_io_latency(
        "upload.batch.group_payload_ndx",
        payload_elapsed + stage_started.elapsed(),
        payload_bytes,
        !staged.is_empty(),
    );
    if staged.is_empty() {
        return Ok(results);
    }
    let filesystems = filesystems.ok_or_else(|| {
        AppError::InternalServerError("grouped payload has no pinned filesystem boundary".into())
    })?;
    barrier(
        &filesystems,
        &roots,
        progress.as_ref(),
        "upload.batch.group_sync_payload",
    )?;
    let mut ndx_published = Vec::with_capacity(staged.len());
    for mut entry in staged {
        match crate::core::upload::staging::promote(&entry.metadata, false)
            .and_then(|()| entry.ndx.publish())
        {
            Ok(()) => ndx_published.push(entry),
            Err(error) => results.push(entry.original.failure(&Failure::from(&error))),
        }
    }
    barrier(
        &filesystems,
        &roots,
        progress.as_ref(),
        "upload.batch.group_sync_ndx",
    )?;
    let namespace_started = Instant::now();
    let mut published = Vec::with_capacity(ndx_published.len());
    for mut entry in ndx_published {
        match entry.namespace.publish() {
            Ok(()) => published.push(entry),
            Err(error) => results.push(entry.original.failure(&Failure::from(&error))),
        }
    }
    record_io_latency(
        "upload.batch.group_ncx",
        namespace_started.elapsed(),
        0,
        !published.is_empty(),
    );
    barrier(
        &filesystems,
        &roots,
        progress.as_ref(),
        "upload.batch.group_sync_ncx",
    )?;
    for entry in published {
        crate::core::upload::staging::retire_empty(&entry.metadata);
        emit_logical_file_uploaded(
            state,
            fragment,
            &entry.logical_path,
            &entry.physical,
            &entry.metadata,
        );
        results.push(entry.original.success(&entry.metadata));
    }
    results.sort_by_key(|result| result.index);
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(index: usize, key: &str) -> Input {
        Input {
            index,
            logical_path: key.into(),
            response_key: format!("/{key}"),
            content_type: "application/octet-stream".into(),
            bytes: Bytes::from_static(b"payload"),
        }
    }

    #[test]
    fn waves_reject_duplicate_keys_indices_and_unbounded_memory_before_storage() {
        assert!(validate_wave(&[]).is_err());
        assert!(validate_wave(&[input(0, "a"), input(1, "a")]).is_err());
        assert!(validate_wave(&[input(0, "a"), input(0, "b")]).is_err());
        assert!(
            validate_wave(
                &(0..65)
                    .map(|i| input(i, &format!("k{i}")))
                    .collect::<Vec<_>>()
            )
            .is_err()
        );
        let mut too_large = input(0, "a");
        too_large.bytes = Bytes::from(vec![0; MAX_GROUP_BYTES + 1]);
        assert!(validate_wave(&[too_large]).is_err());
        assert!(validate_wave(&[input(0, "a"), input(1, "b")]).is_ok());
    }

    #[tokio::test]
    async fn early_failure_keeps_each_original_identity_and_never_claims_a_version() {
        let claims = Claims {
            fragment: "unmounted-no-storage-access".into(),
            fragment_alias: None,
            operator: false,
            read: true,
            write: false,
            modify: false,
            delete: false,
            exp: usize::MAX,
        };
        let state = Arc::new(AppState {
            ws_manager: WsConnectionManager::new(),
        });
        let results = commit(state, claims, vec![input(7, "a"), input(9, "b")], None, 16).await;
        assert_eq!(results.len(), 2);
        for (result, (index, key)) in results.iter().zip([(7, "/a"), (9, "/b")]) {
            assert_eq!(result.index, index);
            assert_eq!(result.result.object_key, key);
            assert_eq!(result.result.status, StatusCode::FORBIDDEN.as_u16());
            assert_eq!(result.result.size_bytes, Some(7));
            assert!(result.result.version_id.is_none());
        }
    }

    /// Pool enrollment is process-immutable. Run the complete pool fixture in
    /// an isolated copy of this test binary rather than alter any other test's
    /// OnceLock roots, geometry, root identities or backend selection.
    #[test]
    fn configured_group_create_version_partial_error_cancellation_and_lock_order() {
        const CHILD: &str = "N1_GROUP_COMMIT_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let base = std::env::temp_dir().join(format!("n1-group-commit-{}", ulid::Ulid::new()));
            std::fs::create_dir(&base).unwrap();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "routes::storage_routes::small_batch::tests::configured_group_create_version_partial_error_cancellation_and_lock_order",
                    "--nocapture",
                ])
                .env(CHILD, &base)
                .env("N1_IO_BACKEND", "blocking")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(90);
            let mut timed_out = false;
            while child.try_wait().unwrap().is_none() {
                if Instant::now() >= deadline {
                    timed_out = true;
                    child.kill().unwrap();
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let output = child.wait_with_output().unwrap();
            let _ = std::fs::remove_dir_all(&base);
            assert!(
                !timed_out
                    && output.status.success()
                    && String::from_utf8_lossy(&output.stderr)
                        .contains("ISOLATED_SMALL_BATCH_COMPLETE"),
                "isolated group fixture failed (timeout={timed_out}):\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            return;
        }
        let base = PathBuf::from(std::env::var_os(CHILD).unwrap());
        let roots = (0..4)
            .map(|slot| base.join(format!("member-{slot}")))
            .collect::<Vec<_>>();
        for root in &roots {
            std::fs::create_dir(root).unwrap();
        }
        let root_strings = roots
            .iter()
            .map(|root| root.display().to_string())
            .collect::<Vec<_>>();
        crate::core::disk_manager::INDEX_STORE_DISKS
            .set(root_strings.clone())
            .unwrap();
        crate::core::disk_manager::BLOB_STORE_DISKS
            .set(root_strings)
            .unwrap();
        crate::core::disk_manager::FS_STORAGE_ROOT
            .set(base.join("control").display().to_string())
            .unwrap();
        crate::core::storage_pool::STORAGE_POOLS
            .set(vec![crate::core::storage_pool::StoragePool {
                id: "group-fixture".into(),
                generation: 1,
                data_shards: 3,
                parity_shards: 1,
                roots: roots.clone(),
                members: Vec::new(),
                legacy_roots: Vec::new(),
                sealed: false,
            }])
            .unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .max_blocking_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use std::os::unix::fs::MetadataExt;
            let _global = crate::test_support::global_test_lock().lock().await;
            let (fragment, claims, state) = super::super::tests::upload_test_fixture();
            let first_payload = Bytes::from(
                (0..65537)
                    .map(|offset| (offset % 251) as u8)
                    .collect::<Vec<_>>(),
            );
            let mut first = input(0, "folder/first.bin");
            first.bytes = first_payload.clone();
            let mut invalid = input(2, "folder/too-big.bin");
            invalid.bytes = Bytes::from(vec![
                0;
                crate::core::upload::small_batch::MAX_OBJECT_BYTES + 1
            ]);
            let initial = commit(
                state.clone(),
                claims.clone(),
                vec![first, input(1, "folder/second.bin"), invalid],
                None,
                16,
            )
            .await;
            assert_eq!(initial.len(), 3);
            assert_eq!(initial[0].result.status, 201);
            assert_eq!(initial[1].result.status, 201);
            assert_eq!(initial[2].result.status, 413);
            assert!(
                flat::lookup(&fragment, "folder/too-big.bin")
                    .unwrap()
                    .is_none()
            );
            let pointer = flat::lookup(&fragment, "folder/first.bin")
                .unwrap()
                .unwrap();
            let first_version = initial[0].result.version_id.as_ref().unwrap();
            let metadata =
                index_store::read_live_version(&fragment, &pointer.object_id, first_version)
                    .unwrap()
                    .unwrap();
            assert_eq!(read_fixture_payload(&metadata), first_payload);
            assert_eq!(metadata.data_shards, 3);
            assert_eq!(metadata.parity_shards, 1);
            let namespace_files = roots
                .iter()
                .filter_map(|root| {
                    let path = flat::path(root, &fragment, "folder/first.bin");
                    path.exists().then(|| {
                        let stat = std::fs::metadata(&path).unwrap();
                        (path.clone(), stat.ino(), std::fs::read(path).unwrap())
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(namespace_files.len(), 2);
            let immutable = roots
                .iter()
                .flat_map(|root| {
                    let object =
                        index_store::object_index_dir(root, &fragment, &pointer.object_id).unwrap();
                    walkdir::WalkDir::new(object)
                        .into_iter()
                        .filter_map(Result::ok)
                        .filter(|entry| entry.file_type().is_file())
                        .map(|entry| {
                            let path = entry.into_path();
                            let stat = std::fs::metadata(&path).unwrap();
                            (
                                path.clone(),
                                stat.ino(),
                                stat.modified().unwrap(),
                                std::fs::read(path).unwrap(),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert_eq!(immutable.len(), 8, "four pieces and four NDX replicas");
            let next_payload = Bytes::from_static(b"new immutable version");
            let mut next = input(3, "folder/first.bin");
            next.bytes = next_payload.clone();
            let mut empty = input(4, "folder/empty.bin");
            empty.bytes = Bytes::new();
            let updated = commit(state.clone(), claims.clone(), vec![next, empty], None, 4).await;
            assert!(updated.iter().all(|entry| entry.result.status == 201));
            assert_ne!(updated[0].result.version_id.as_ref(), Some(first_version));
            let newest = index_store::read_live_version(
                &fragment,
                &pointer.object_id,
                updated[0].result.version_id.as_ref().unwrap(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(read_fixture_payload(&newest), next_payload);
            assert_eq!(
                index_store::list_live_versions(&fragment, &pointer.object_id)
                    .unwrap()
                    .len(),
                2
            );
            for (path, inode, bytes) in namespace_files {
                assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
                assert_eq!(std::fs::read(path).unwrap(), bytes);
            }
            for (path, inode, modified, bytes) in immutable {
                let stat = std::fs::metadata(&path).unwrap();
                assert_eq!(stat.ino(), inode);
                assert_eq!(stat.modified().unwrap(), modified);
                assert_eq!(std::fs::read(path).unwrap(), bytes);
            }
            assert_eq!(
                flat::lookup(&fragment, "folder/first.bin")
                    .unwrap()
                    .unwrap(),
                pointer
            );

            // More independent groups than blocking workers must still drain
            // every payload open, filesystem barrier and acknowledgement.
            // The old blocking-owner -> block_on(payload) form starved this
            // same two-worker executor as soon as two groups were admitted.
            let parallel = (0..8).map(|group| {
                let state = state.clone();
                let claims = claims.clone();
                async move {
                    let inputs = (0..4)
                        .map(|object| {
                            let mut entry =
                                input(object, &format!("parallel/{group}/{object}.bin"));
                            entry.bytes = Bytes::from(vec![group as u8; 4097 + object]);
                            entry
                        })
                        .collect();
                    commit(state, claims, inputs, None, 4).await
                }
            });
            let parallel = tokio::time::timeout(
                Duration::from_secs(30),
                futures_util::future::join_all(parallel),
            )
            .await
            .expect("eight groups must not exhaust the two-worker blocking pool");
            for (group, results) in parallel.into_iter().enumerate() {
                assert_eq!(results.len(), 4);
                for result in results {
                    assert_eq!(result.result.status, 201, "{result:?}");
                    let key = format!("parallel/{group}/{}.bin", result.index);
                    let pointer = flat::lookup(&fragment, &key).unwrap().unwrap();
                    let metadata = index_store::read_live_version(
                        &fragment,
                        &pointer.object_id,
                        result.result.version_id.as_ref().unwrap(),
                    )
                    .unwrap()
                    .unwrap();
                    assert_eq!(
                        read_fixture_payload(&metadata),
                        vec![group as u8; 4097 + result.index]
                    );
                }
            }

            // An old async mutation can hold shared backup admission while
            // awaiting filesystem IO. A queued exclusive capture must not
            // cause new batch publishers to consume the remaining worker and
            // starve that old owner's IO (only two blocking workers exist).
            let old_mutation = crate::core::backup::barrier::mutation_async().await;
            let freeze = tokio::task::spawn_blocking(|| crate::core::backup::barrier::freeze(30));
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let reader =
                        tokio::task::unconstrained(crate::core::backup::barrier::mutation_async());
                    tokio::pin!(reader);
                    if futures_util::poll!(reader.as_mut()).is_pending() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("exclusive backup capture must actually be queued");
            let (snapshot_tx, mut snapshot_rx) = tokio::sync::mpsc::unbounded_channel();
            *SNAPSHOT_WAIT_FOR_TEST.lock().unwrap() = Some(snapshot_tx);
            let mut pending_groups = JoinSet::new();
            for group in 0..4 {
                pending_groups.spawn(commit(
                    state.clone(),
                    claims.clone(),
                    vec![input(group, &format!("backup-queued/group-{group}.bin"))],
                    None,
                    4,
                ));
            }
            for _ in 0..4 {
                tokio::time::timeout(Duration::from_secs(10), snapshot_rx.recv())
                    .await
                    .expect("all batch payloads must drain while snapshot publication waits")
                    .expect("snapshot test notification channel closed");
            }
            *SNAPSHOT_WAIT_FOR_TEST.lock().unwrap() = None;
            tokio::time::timeout(
                Duration::from_secs(5),
                tokio::fs::write(base.join("old-mutation-io"), b"old mutation can finish"),
            )
            .await
            .expect("backup-waiting publishers must not starve old mutation filesystem work")
            .unwrap();
            drop(old_mutation);
            let frozen = tokio::time::timeout(Duration::from_secs(5), freeze)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(
                pending_groups.try_join_next().is_none(),
                "no batch can acknowledge while the capture owns its boundary"
            );
            drop(frozen);
            tokio::time::timeout(Duration::from_secs(30), async {
                while let Some(result) = pending_groups.join_next().await {
                    let result = result.unwrap();
                    assert_eq!(result.len(), 1);
                    assert_eq!(result[0].result.status, 201);
                }
            })
            .await
            .expect("all queued publishers must complete after backup admission releases");

            // A raw object key is also its physical lock key. While that lock
            // is occupied, neither namespace nor backup admission may be held.
            let raw = "a".repeat(64);
            let held = crate::core::index::acquire_object_operation_lock(&fragment, &raw)
                .await
                .unwrap();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            *ADMISSION_WAIT_FOR_TEST.lock().unwrap() = Some(ready_tx);
            let context = format!("group-cancel-{fragment}");
            let io_guard = acquire_upload_io_guard_for_query(Some(true), &fragment, &context)
                .await
                .unwrap();
            let caller = tokio::spawn(commit(
                state.clone(),
                claims.clone(),
                vec![input(5, &raw)],
                io_guard,
                4,
            ));
            tokio::time::timeout(Duration::from_secs(10), ready_rx)
                .await
                .unwrap()
                .unwrap();
            // Occupy another physical identity as well, then schedule a file
            // operation which requires this very same bounded blocking pool.
            // Physical-lock waits must remain async so the old owner can do IO.
            let other_raw = "b".repeat(64);
            let other_held =
                crate::core::index::acquire_object_operation_lock(&fragment, &other_raw)
                    .await
                    .unwrap();
            let (other_ready_tx, other_ready_rx) = tokio::sync::oneshot::channel();
            *ADMISSION_WAIT_FOR_TEST.lock().unwrap() = Some(other_ready_tx);
            let other_caller =
                tokio::spawn(commit(state, claims, vec![input(6, &other_raw)], None, 4));
            tokio::time::timeout(Duration::from_secs(10), other_ready_rx)
                .await
                .unwrap()
                .unwrap();
            let queued_file = base.join("queued-owner-io");
            tokio::time::timeout(
                Duration::from_secs(5),
                tokio::fs::write(&queued_file, b"old lock owner can still progress"),
            )
            .await
            .expect("two physical-lock waiters must not consume both blocking workers")
            .unwrap();
            assert_eq!(
                std::fs::read(queued_file).unwrap(),
                b"old lock owner can still progress"
            );
            let (probe_fragment, probe_raw) = (fragment.clone(), raw.clone());
            tokio::time::timeout(
                Duration::from_secs(5),
                tokio::task::spawn_blocking(move || {
                    flat::with_keys(&probe_fragment, &[&probe_raw], || Ok(()))
                }),
            )
            .await
            .expect("physical lock waits must not own namespace keys")
            .unwrap()
            .unwrap();
            let frozen = tokio::time::timeout(
                Duration::from_secs(5),
                tokio::task::spawn_blocking(|| crate::core::backup::barrier::freeze(4)),
            )
            .await
            .expect("physical lock waits must not hold snapshot mutation admission")
            .unwrap()
            .unwrap();
            drop(frozen);
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
            assert!(
                crate::core::io_scheduler::disk_io_scheduler_snapshot()
                    .active_operations
                    .iter()
                    .any(|operation| operation.context == context)
            );
            drop(held);
            drop(other_held);
            let other_result = tokio::time::timeout(Duration::from_secs(30), other_caller)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(other_result[0].result.status, 201);
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let committed = flat::lookup(&fragment, &raw).unwrap().is_some();
                    let admitted = crate::core::io_scheduler::disk_io_scheduler_snapshot()
                        .active_operations
                        .iter()
                        .any(|operation| operation.context == context);
                    if committed && !admitted {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("detached grouped publication must finish and release admission");
            let raw_pointer = flat::lookup(&fragment, &raw).unwrap().unwrap();
            assert_eq!(raw_pointer.object_id, raw);
            let cancelled_metadata = index_store::read_current_live_head(&fragment, &raw)
                .unwrap()
                .unwrap();
            assert_eq!(
                read_fixture_payload(&cancelled_metadata),
                b"payload".as_slice()
            );
            super::super::tests::remove_upload_test_fixture(&fragment);
        });
        eprintln!("ISOLATED_SMALL_BATCH_COMPLETE");
    }

    fn read_fixture_payload(metadata: &ObjectMetadata) -> Vec<u8> {
        let mut payload = Vec::new();
        for part in &metadata.parts {
            let pieces = part
                .shards
                .iter()
                .map(|shard| {
                    let path =
                        crate::core::storage_pool::resolve_address(&shard.absolute_path).unwrap();
                    let bytes = std::fs::read(path).unwrap();
                    assert_eq!(format!("{:08x}", crc32fast::hash(&bytes)), shard.checksum);
                    bytes
                })
                .collect::<Vec<_>>();
            assert_eq!(
                pieces.len(),
                usize::from(metadata.data_shards) + usize::from(metadata.parity_shards)
            );
            let data = &pieces[..usize::from(metadata.data_shards)];
            for offset in (0..data[0].len()).step_by(metadata.stripe_size_bytes) {
                for shard in data {
                    payload.extend_from_slice(&shard[offset..offset + metadata.stripe_size_bytes]);
                }
            }
        }
        payload.truncate(metadata.total_size_bytes as usize);
        payload
    }
}
