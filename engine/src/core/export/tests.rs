use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use ini::Ini;
use sha2::{Digest, Sha256};

use crate::core::disk_manager::{DownloadArchiveEntry, DownloadArchiveRequest};

use super::builder::{
    ActiveProgress, export_buffer_plan, export_buffer_plan_with_overhead, persist_archive_chunks,
};
use super::manifest::validate_archive_path;
use super::models::ExportConfig;
use super::store::load_export_config_from;
use super::{
    ACTIVE_DOWNLOADS, ActiveDownloadLease, export_has_active_downloads, validate_create_request,
};

fn pending_export(fragment: &str) -> super::models::PersistedExport {
    super::models::PersistedExport {
        id: ulid::Ulid::new().to_string(),
        fragment: fragment.to_string(),
        request: DownloadArchiveRequest {
            keys: vec!["source.bin".to_string()],
            entries: Vec::new(),
            prefix: None,
            prefixes: Vec::new(),
            format: "tar".to_string(),
            filename: None,
            export_all: false,
            include_internal: false,
        },
        phase: super::ExportPhase::Queued,
        format: "tar".to_string(),
        filename: "export.tar".to_string(),
        created_at_unix: super::now_unix(),
        updated_at_unix: super::now_unix(),
        expires_at_unix: super::now_unix() + 3600,
        manifest_ready: false,
        objects_total: 1,
        objects_completed: 0,
        source_bytes_total: 16,
        source_bytes_completed: 0,
        artifact_bytes_completed: 0,
        reserved_bytes: 0,
        artifact_size_bytes: None,
        etag_sha256: None,
        chunks: Vec::new(),
        error: None,
    }
}

fn test_root(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("n1-export-{name}-{}-{unique}", std::process::id()))
}

#[test]
fn archive_paths_reject_cross_platform_traversal_and_controls() {
    assert!(validate_archive_path("photos/image.jpg").is_ok());
    assert!(validate_archive_path("../secret").is_err());
    assert!(validate_archive_path("photos\\image.jpg").is_err());
    assert!(validate_archive_path("photos\nimage.jpg").is_err());
    assert!(validate_archive_path("/absolute/path").is_err());
}

#[test]
fn bulk_export_rejects_internal_namespace_selection() {
    let request = DownloadArchiveRequest {
        keys: Vec::new(),
        entries: Vec::new(),
        prefix: None,
        prefixes: Vec::new(),
        format: "zip".to_string(),
        filename: None,
        export_all: true,
        include_internal: true,
    };
    assert!(validate_create_request(&request).is_err());
}

#[test]
fn bulk_export_bounds_explicit_selection_state() {
    let request = DownloadArchiveRequest {
        keys: vec!["object".to_string(); 10_001],
        entries: Vec::new(),
        prefix: None,
        prefixes: Vec::new(),
        format: "zip".to_string(),
        filename: None,
        export_all: false,
        include_internal: false,
    };
    assert!(validate_create_request(&request).is_err());
}

#[test]
fn mapped_export_entries_are_bounded_and_exclusive() {
    let mapped = DownloadArchiveEntry {
        object_key: "01JPACK00000000000000000000.pack".to_string(),
        archive_path: "records/patient.dcm".to_string(),
        version_id: Some("01JVERSION000000000000000000".to_string()),
        offset_bytes: Some(4096),
        length_bytes: Some(8192),
    };
    let request = DownloadArchiveRequest {
        keys: vec!["another-object".to_string()],
        entries: vec![mapped.clone()],
        prefix: None,
        prefixes: Vec::new(),
        format: "zip".to_string(),
        filename: None,
        export_all: false,
        include_internal: false,
    };
    assert!(validate_create_request(&request).is_err());

    let request = DownloadArchiveRequest {
        keys: Vec::new(),
        entries: vec![mapped; 10_001],
        prefix: None,
        prefixes: Vec::new(),
        format: "tar".to_string(),
        filename: None,
        export_all: false,
        include_internal: false,
    };
    assert!(validate_create_request(&request).is_err());
}

#[test]
fn export_configuration_is_bounded_without_requiring_new_config_keys() {
    let empty = Ini::new();
    let defaults = load_export_config_from(&empty);
    assert!(defaults.object_read_concurrency >= 1);
    assert!(defaults.artifact_chunk_size_bytes >= 1024 * 1024);

    let mut configured = Ini::new();
    configured
        .with_section(Some("export"))
        .set("retention_seconds", "1")
        .set("max_concurrent_jobs", "999")
        .set("max_queued_jobs", "99999")
        .set("manifest_validation_concurrency", "0")
        .set("object_read_concurrency", "0")
        .set("artifact_chunk_size_bytes", "32")
        .set("artifact_write_concurrency", "999")
        .set("max_buffered_bytes_per_job", "1")
        .set("max_buffered_bytes_global", "1")
        .set("max_concurrent_downloads", "0");
    let bounded = load_export_config_from(&configured);
    assert_eq!(bounded.retention_seconds, 60);
    assert_eq!(bounded.max_concurrent_jobs, 32);
    assert_eq!(bounded.max_queued_jobs, 4096);
    assert_eq!(bounded.manifest_validation_concurrency, 1);
    assert_eq!(bounded.object_read_concurrency, 1);
    assert_eq!(bounded.artifact_chunk_size_bytes, 1024 * 1024);
    assert_eq!(bounded.artifact_write_concurrency, 32);
    assert_eq!(bounded.max_buffered_bytes_per_job, 16 * 1024 * 1024);
    assert_eq!(bounded.max_buffered_bytes_global, 16 * 1024 * 1024);
    assert_eq!(bounded.max_concurrent_downloads, 1);
}

#[test]
fn export_buffer_plan_contains_hostile_concurrency_configuration() {
    let config = ExportConfig {
        object_read_concurrency: 128,
        artifact_chunk_size_bytes: 1024 * 1024 * 1024,
        artifact_write_concurrency: 32,
        max_buffered_bytes_per_job: 16 * 1024 * 1024,
        max_buffered_bytes_global: 16 * 1024 * 1024,
        ..ExportConfig::default()
    };
    let plan = export_buffer_plan(&config, 200);
    assert!(plan.admitted_bytes <= 16 * 1024 * 1024);
    assert!(plan.artifact_chunk_size_bytes >= 1024 * 1024);
    assert!(plan.artifact_chunk_size_bytes < config.artifact_chunk_size_bytes);
    assert!(plan.object_read_concurrency >= 1);
    assert!(plan.artifact_write_concurrency >= 1);
    assert!(plan.object_read_concurrency < config.object_read_concurrency);
    assert!(plan.artifact_write_concurrency < config.artifact_write_concurrency);
}

#[test]
fn export_buffer_plan_never_schedules_more_writers_than_roots() {
    let config = ExportConfig {
        artifact_write_concurrency: 32,
        max_buffered_bytes_per_job: 1024 * 1024 * 1024,
        max_buffered_bytes_global: 1024 * 1024 * 1024,
        ..ExportConfig::default()
    };
    let plan = export_buffer_plan(&config, 3);
    assert_eq!(plan.artifact_write_concurrency, 3);
}

#[test]
fn zip_directory_memory_must_fit_the_per_job_budget() {
    let config = ExportConfig {
        max_buffered_bytes_per_job: 16 * 1024 * 1024,
        max_buffered_bytes_global: 16 * 1024 * 1024,
        ..ExportConfig::default()
    };
    assert!(export_buffer_plan_with_overhead(&config, 4, 8 * 1024 * 1024).is_ok());
    assert!(export_buffer_plan_with_overhead(&config, 4, 15 * 1024 * 1024).is_err());
}

#[tokio::test]
async fn active_download_lease_releases_retention_fence_on_drop() {
    let id = format!("test-download-{}", std::process::id());
    let permit = Arc::new(tokio::sync::Semaphore::new(1))
        .acquire_owned()
        .await
        .expect("download permit");
    ACTIVE_DOWNLOADS.insert(id.clone(), 1);
    let lease = ActiveDownloadLease {
        export_id: id.clone(),
        _permit: permit,
    };
    assert!(export_has_active_downloads(&id));
    drop(lease);
    assert!(!export_has_active_downloads(&id));
}

#[tokio::test]
async fn artifact_chunks_round_robin_across_roots_and_reconstruct_exactly() {
    let base = test_root("chunks");
    let roots = (0..3)
        .map(|index| base.join(format!("disk-{index}")))
        .collect::<Vec<_>>();
    for root in &roots {
        tokio::fs::create_dir_all(root)
            .await
            .expect("create test root");
    }
    let data = (0..(3 * 1024 * 1024 + 333_333))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let expected_sha = hex::encode(Sha256::digest(&data));
    let progress = Arc::new(ActiveProgress::new());
    let config = ExportConfig {
        artifact_chunk_size_bytes: 1024 * 1024,
        artifact_write_concurrency: 3,
        ..ExportConfig::default()
    };
    let result = persist_archive_chunks(
        Cursor::new(data.clone()),
        "01J00000000000000000000000".to_string(),
        roots.clone(),
        config,
        progress,
    )
    .await
    .expect("persist artifact chunks");

    assert_eq!(result.size_bytes, data.len() as u64);
    assert_eq!(result.sha256, expected_sha);
    assert_eq!(result.chunks.len(), 4);
    for chunk in &result.chunks {
        let bytes = tokio::fs::read(&chunk.path).await.expect("read chunk");
        assert_eq!(chunk.sha256, hex::encode(Sha256::digest(&bytes)));
    }
    let used_roots = result
        .chunks
        .iter()
        .filter_map(|chunk| {
            roots
                .iter()
                .position(|root| PathBuf::from(&chunk.path).starts_with(root))
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(used_roots.len(), roots.len());

    let mut reconstructed = Vec::new();
    for chunk in &result.chunks {
        reconstructed.extend(tokio::fs::read(&chunk.path).await.expect("read chunk"));
    }
    assert_eq!(reconstructed, data);
    let _ = tokio::fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn cancelled_artifact_does_not_begin_writing() {
    let base = test_root("cancel");
    tokio::fs::create_dir_all(&base)
        .await
        .expect("create test root");
    let progress = Arc::new(ActiveProgress::new());
    progress.cancel.store(true, Ordering::Release);
    let result = persist_archive_chunks(
        Cursor::new(vec![7_u8; 2 * 1024 * 1024]),
        "01J00000000000000000000001".to_string(),
        vec![base.clone()],
        ExportConfig {
            artifact_chunk_size_bytes: 1024 * 1024,
            ..ExportConfig::default()
        },
        progress,
    )
    .await;
    assert!(result.is_err());
    let entries = std::fs::read_dir(&base).expect("scan test root").count();
    assert_eq!(entries, 0);
    let _ = tokio::fs::remove_dir_all(base).await;
}

#[tokio::test]
async fn late_export_admission_cannot_cross_gc_unlink_or_a_later_reclaim_batch() {
    let _global = crate::test_support::global_test_lock().lock().await;
    crate::core::disk_manager::FS_STORAGE_ROOT.get_or_init(|| {
        std::env::temp_dir()
            .join(format!("n1-matrix-{}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    });
    let fragment = format!("late-export-gc-{}", ulid::Ulid::new());
    let state = pending_export(&fragment);
    let directory = super::store::job_directory(&state.id).unwrap();
    let fixture = test_root("late-gc-admission");
    tokio::fs::create_dir_all(&fixture).await.unwrap();
    let first = fixture.join("first-unreferenced-version");
    let second = fixture.join("later-export-source");
    tokio::fs::write(&first, b"first bounded GC batch")
        .await
        .unwrap();
    tokio::fs::write(&second, b"must survive export admission")
        .await
        .unwrap();

    let guard = super::gc_reclaim_guard(&fragment)
        .await
        .unwrap()
        .expect("no export was admitted before this GC batch");
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let admitted_state = state.clone();
    let admission = tokio::spawn(async move {
        entered_tx.send(()).unwrap();
        super::admit_new_export(&admitted_state).await
    });
    entered_rx.await.unwrap();
    tokio::task::yield_now().await;
    assert!(
        !admission.is_finished(),
        "export admission must wait until the current destructive GC batch ends"
    );
    assert!(
        !directory.join("state.json").exists(),
        "an export cannot publish protection through the held GC guard"
    );
    // Only the test-owned first candidate is unlinked. The real guard remains
    // held across this destructive action, exactly as required in GC.
    tokio::fs::remove_file(&first).await.unwrap();
    assert!(!admission.is_finished());
    assert!(second.exists());
    drop(guard);
    tokio::time::timeout(std::time::Duration::from_secs(5), admission)
        .await
        .expect("export admission progresses after the GC batch releases its guard")
        .unwrap()
        .unwrap();

    assert!(directory.join("state.json").is_file());
    assert!(
        super::gc_reclaim_guard(&fragment).await.unwrap().is_none(),
        "the next GC batch must recheck late export admission, not trust an earlier empty snapshot"
    );
    assert_eq!(
        tokio::fs::read(&second).await.unwrap(),
        b"must survive export admission"
    );
    // Protection is fragment-scoped rather than a permanent node-wide GC ban.
    let unrelated = format!("unrelated-export-gc-{}", ulid::Ulid::new());
    assert!(super::gc_reclaim_guard(&unrelated).await.unwrap().is_some());
    tokio::fs::remove_dir_all(directory).await.unwrap();
    tokio::fs::remove_dir_all(fixture).await.unwrap();
}

#[tokio::test]
async fn gc_protects_every_nonterminal_export_phase_and_releases_terminal_jobs() {
    let _global = crate::test_support::global_test_lock().lock().await;
    crate::core::disk_manager::FS_STORAGE_ROOT.get_or_init(|| {
        std::env::temp_dir()
            .join(format!("n1-matrix-{}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    });
    let fragment = format!("export-gc-phases-{}", ulid::Ulid::new());
    let mut state = pending_export(&fragment);
    let directory = super::store::job_directory(&state.id).unwrap();
    for phase in [
        super::ExportPhase::Queued,
        super::ExportPhase::CapturingManifest,
        super::ExportPhase::Building,
    ] {
        state.phase = phase;
        super::save_state(&state).await.unwrap();
        assert!(
            super::gc_reclaim_guard(&fragment).await.unwrap().is_none(),
            "nonterminal export {phase:?} must protect its source fragment"
        );
    }
    for phase in [
        super::ExportPhase::Ready,
        super::ExportPhase::Failed,
        super::ExportPhase::Cancelled,
    ] {
        state.phase = phase;
        super::save_state(&state).await.unwrap();
        assert!(
            super::gc_reclaim_guard(&fragment).await.unwrap().is_some(),
            "terminal export {phase:?} must not retain original source data forever"
        );
    }
    tokio::fs::remove_dir_all(directory).await.unwrap();
}

#[tokio::test]
async fn gc_export_protection_enumeration_fails_closed_on_corrupt_or_unreadable_state() {
    let base = test_root("gc-strict-state-errors");
    let state = pending_export("protected-fragment");
    let directory = base.join(&state.id);
    tokio::fs::create_dir_all(&directory).await.unwrap();
    let path = directory.join("state.json");
    tokio::fs::write(&path, b"{not a decodable durable export}")
        .await
        .unwrap();
    assert!(
        super::store::list_states_for_gc_at(&base).await.is_err(),
        "corrupt export authority must not be logged and silently skipped by GC"
    );
    tokio::fs::remove_file(&path).await.unwrap();
    // EISDIR is a real read error even for a root test runner; a chmod-only
    // unreadable-file test would incorrectly pass when run with CAP_DAC_OVERRIDE.
    tokio::fs::create_dir(&path).await.unwrap();
    assert!(
        super::store::list_states_for_gc_at(&base).await.is_err(),
        "an unreadable export state must stop destructive reclamation"
    );
    tokio::fs::remove_dir(&path).await.unwrap();
    tokio::fs::write(&path, serde_json::to_vec(&state).unwrap())
        .await
        .unwrap();
    let states = super::store::list_states_for_gc_at(&base).await.unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].id, state.id);
    assert_eq!(states[0].fragment, state.fragment);
    tokio::fs::remove_dir_all(base).await.unwrap();
}

#[tokio::test]
async fn gc_export_protection_rejects_mismatched_job_identity_but_tolerates_removed_jobs() {
    let base = test_root("gc-strict-state-identity");
    let mut state = pending_export("protected-fragment");
    let directory = base.join(&state.id);
    tokio::fs::create_dir_all(&directory).await.unwrap();
    let path = directory.join("state.json");
    // A missing state file can be the ordinary atomic-publication/removal
    // window; it is not evidence that a different job owns this directory.
    assert!(
        super::store::list_states_for_gc_at(&base)
            .await
            .unwrap()
            .is_empty()
    );
    state.id = ulid::Ulid::new().to_string();
    tokio::fs::write(&path, serde_json::to_vec(&state).unwrap())
        .await
        .unwrap();
    assert!(
        super::store::list_states_for_gc_at(&base).await.is_err(),
        "a decoded state from another job must not grant GC permission to unlink"
    );
    tokio::fs::remove_file(&path).await.unwrap();
    assert!(
        super::store::list_states_for_gc_at(&base)
            .await
            .unwrap()
            .is_empty()
    );
    tokio::fs::remove_dir_all(&base).await.unwrap();
    assert!(
        super::store::list_states_for_gc_at(&base)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn gc_export_protection_rejects_redirected_job_or_state() {
    let base = test_root("gc-strict-state-redirect");
    let state = pending_export("protected-fragment");
    let external = base.join("external");
    tokio::fs::create_dir_all(&external).await.unwrap();
    let state_path = external.join("state.json");
    tokio::fs::write(&state_path, serde_json::to_vec(&state).unwrap())
        .await
        .unwrap();
    let job = base.join(&state.id);
    std::os::unix::fs::symlink(&external, &job).unwrap();
    assert!(super::store::list_states_for_gc_at(&base).await.is_err());
    tokio::fs::remove_file(&job).await.unwrap();
    tokio::fs::create_dir(&job).await.unwrap();
    std::os::unix::fs::symlink(&state_path, job.join("state.json")).unwrap();
    assert!(super::store::list_states_for_gc_at(&base).await.is_err());
    assert_eq!(
        tokio::fs::read(&state_path).await.unwrap(),
        serde_json::to_vec(&state).unwrap()
    );
    tokio::fs::remove_dir_all(base).await.unwrap();
}
