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

fn test_root(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("n2-export-{name}-{}-{unique}", std::process::id()))
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
fn expired_build_lease_prevents_publication() {
    let progress = ActiveProgress::new();
    progress.deadline.store(1, Ordering::Release);
    assert!(progress.ensure_running().is_err());
}

#[test]
fn zip_selection_is_not_silently_downgraded_for_folders_or_large_key_sets() {
    let request: DownloadArchiveRequest = serde_json::from_value(serde_json::json!({
        "keys": vec!["file"; 1500], "prefixes": ["folder"], "format": "zip"
    }))
    .unwrap();
    assert_eq!(validate_create_request(&request).unwrap().0, "zip");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_download_leases_keep_the_new_reader_pinned() {
    let id = format!("test-overlapping-download-{}", std::process::id());
    let semaphore = Arc::new(tokio::sync::Semaphore::new(2));
    for _ in 0..1000 {
        ACTIVE_DOWNLOADS.insert(id.clone(), 1);
        let old = ActiveDownloadLease {
            export_id: id.clone(),
            _permit: Arc::clone(&semaphore).acquire_owned().await.unwrap(),
        };
        let next_id = id.clone();
        let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
        let acquire = tokio::spawn(async move {
            ACTIVE_DOWNLOADS
                .entry(next_id.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1);
            ActiveDownloadLease {
                export_id: next_id,
                _permit: permit,
            }
        });
        let release = tokio::spawn(async move {
            drop(old);
        });
        let next = acquire.await.unwrap();
        release.await.unwrap();
        assert_eq!(*ACTIVE_DOWNLOADS.get(&id).expect("new reader is pinned"), 1);
        drop(next);
        assert!(!export_has_active_downloads(&id));
    }
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
