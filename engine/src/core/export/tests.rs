use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use ini::Ini;
use sha2::{Digest, Sha256};

use crate::core::disk_manager::{DownloadArchiveEntry, DownloadArchiveRequest};

use super::builder::{ActiveProgress, persist_archive_chunks};
use super::manifest::validate_archive_path;
use super::models::ExportConfig;
use super::store::load_export_config_from;
use super::validate_create_request;

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
        .set("object_read_concurrency", "0")
        .set("artifact_chunk_size_bytes", "32")
        .set("artifact_write_concurrency", "999");
    let bounded = load_export_config_from(&configured);
    assert_eq!(bounded.retention_seconds, 60);
    assert_eq!(bounded.max_concurrent_jobs, 32);
    assert_eq!(bounded.max_queued_jobs, 4096);
    assert_eq!(bounded.object_read_concurrency, 1);
    assert_eq!(bounded.artifact_chunk_size_bytes, 1024 * 1024);
    assert_eq!(bounded.artifact_write_concurrency, 32);
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
