// Retained original export memory-budget regression cases from 061a40a.
use super::builder::{export_buffer_plan, export_buffer_plan_with_overhead};
use super::models::ExportConfig;
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
