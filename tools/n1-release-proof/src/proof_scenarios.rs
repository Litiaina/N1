impl Proof {
    async fn run(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("before").await?;
        self.create_fragment().await?;
        self.auth().await?;

        let object_key = self.object_key();
        let source_len = self.args.source_file.metadata()?.len();
        if source_len <= self.args.max_part_size {
            return Err(format!(
                "source file is too small for large multipart proof: {} <= max_part_size {}",
                source_len, self.args.max_part_size
            )
            .into());
        }

        let mut live_expected = BTreeSet::new();
        let mut evidence = Vec::new();
        let mut rss_samples = Vec::new();
        let local_sha = self.sha256_file(&self.args.source_file).await?;
        rss_samples.push(self.rss_sample("server_started"));
        let upload = self.multipart_restart_resume(&object_key).await?;
        live_expected.insert(object_key.clone());
        self.assert_list_contains(&object_key).await?;
        let downloaded_sha = self.download_sha256(&object_key).await?;
        if downloaded_sha != local_sha {
            return Err(format!(
                "download hash mismatch for {object_key}: local={local_sha} remote={downloaded_sha}"
            )
            .into());
        }
        self.range_probe(&object_key).await?;
        self.assert_object_status_clean(&object_key, "after_large_multipart")
            .await?;
        self.assert_health_clean("after_large_multipart").await?;
        let native_protocol = self.native_protocol_proof(&object_key).await?;
        if let Some(keys) = native_protocol
            .get("live_object_keys")
            .and_then(Value::as_array)
        {
            for key in keys.iter().filter_map(Value::as_str) {
                live_expected.insert(key.to_string());
            }
        }
        evidence.push(json!({"gate": "native_n1_protocol", "status": "PASS"}));
        rss_samples.push(self.rss_sample("after_large_multipart"));
        evidence.push(json!({"gate": "large_multipart_restart_resume", "status": "PASS"}));

        self.stop_server()?;
        self.start_server("after_restart").await?;
        self.auth().await?;
        self.assert_expected_visible(&live_expected).await?;
        self.assert_download_sha256(&object_key, &local_sha).await?;
        rss_samples.push(self.rss_sample("after_restart_persistence"));
        evidence.push(json!({"gate": "restart_persistence", "status": "PASS"}));

        let permissions = self.permission_boundary_proof(&mut live_expected).await?;
        evidence.push(json!({"gate": "auth_permission_boundaries", "status": "PASS"}));

        let fragment_usage = self.fragment_usage_api_proof().await?;
        evidence.push(json!({"gate": "fragment_usage_api", "status": "PASS"}));

        let rename_lineage = self
            .rename_version_lineage_proof(&mut live_expected)
            .await?;
        rss_samples.push(self.rss_sample("after_rename_version_lineage"));
        evidence.push(json!({"gate": "rename_preserves_version_lineage", "status": "PASS"}));

        let websocket = self
            .websocket_namespace_delta_proof(&mut live_expected)
            .await?;
        rss_samples.push(self.rss_sample("after_websocket_namespace_delta"));
        evidence.push(json!({"gate": "websocket_namespace_delta", "status": "PASS"}));

        let additional_large = self
            .multiple_large_multipart_proof(&mut live_expected, &local_sha)
            .await?;
        rss_samples.push(self.rss_sample("after_multiple_large_multipart"));
        evidence.push(json!({"gate": "multiple_large_multipart", "status": "PASS"}));

        let object_storm = self
            .concurrent_object_storm(&mut live_expected, "main-storm", self.args.storm_objects)
            .await?;
        rss_samples.push(self.rss_sample("after_10k_concurrent_object_storm"));
        evidence.push(json!({"gate": "large_object_count_concurrency", "status": "PASS"}));

        let same_fragment_mixed = self
            .same_fragment_mixed_user_proof(&mut live_expected)
            .await?;
        rss_samples.push(self.rss_sample("after_same_fragment_mixed_users"));
        evidence.push(json!({"gate": "same_fragment_mixed_users", "status": "PASS"}));

        let parallel_multipart = self
            .parallel_multipart_user_proof(&mut live_expected)
            .await?;
        rss_samples.push(self.rss_sample("after_parallel_multipart_users"));
        evidence.push(json!({"gate": "parallel_multipart_users", "status": "PASS"}));

        let version_contention = self
            .same_key_version_contention_proof(&mut live_expected)
            .await?;
        rss_samples.push(self.rss_sample("after_same_key_version_contention"));
        evidence.push(json!({"gate": "same_key_version_contention", "status": "PASS"}));

        let multi_fragment = self.multi_fragment_isolation_proof().await?;
        rss_samples.push(self.rss_sample("after_multi_fragment_users"));
        evidence.push(json!({"gate": "multi_fragment_user_isolation", "status": "PASS"}));

        let crud = self.posix_crud_gc_proof(&mut live_expected).await?;
        rss_samples.push(self.rss_sample("after_posix_crud_gc"));
        evidence.push(json!({"gate": "posix_crud_trash_recover_gc", "status": "PASS"}));

        let small_multipart = self.small_multipart_proof(&mut live_expected).await?;
        rss_samples.push(self.rss_sample("after_small_multipart"));
        evidence.push(json!({"gate": "small_multipart", "status": "PASS"}));

        let archive = self.archive_proof(&mut live_expected).await?;
        evidence.push(json!({"gate": "archive_tar", "status": "PASS"}));

        let recover_on_read = self
            .simulate_recover_on_read(&object_key, &local_sha)
            .await?;
        rss_samples.push(self.rss_sample("after_recover_on_read"));
        evidence.push(json!({"gate": "recover_on_read", "status": "PASS"}));

        let object_repair = self.object_repair_status_proof(&object_key).await?;
        rss_samples.push(self.rss_sample("after_object_repair"));
        evidence.push(json!({"gate": "object_status_repair", "status": "PASS"}));

        let disk_repair = self
            .simulate_disk_format_and_repair(&live_expected, &object_key, &local_sha)
            .await?;
        rss_samples.push(self.rss_sample("after_disk_format_repair"));
        evidence.push(json!({"gate": "disk_format_repair", "status": "PASS"}));

        let redistribution = self.redistribution_proof(&mut live_expected).await?;
        rss_samples.push(self.rss_sample("after_redistribution"));
        evidence.push(json!({"gate": "redistribution_2_1_2", "status": "PASS"}));

        let chaos = if self.args.chaos || self.args.chaos_seconds > 0 || self.args.chaos_cycles > 0
        {
            let report = self
                .chaos_proof(&mut live_expected, &local_sha, &mut rss_samples)
                .await?;
            evidence.push(json!({"gate": "chaos", "status": "PASS"}));
            report
        } else {
            json!({"enabled": false})
        };

        self.assert_expected_visible(&live_expected).await?;
        self.assert_health_clean("final").await?;
        rss_samples.push(self.rss_sample("final_health_clean"));
        let inspection = self.inspect_index(live_expected.len())?;
        self.stop_server()?;
        let logs = self.scan_logs()?;

        self.summary = json!({
            "status": "PASS",
            "engine": "n1",
            "proof": "n1_release_matrix",
            "source_file": self.args.source_file,
            "object_key": object_key,
            "source_bytes": source_len,
            "source_sha256": local_sha,
            "download_sha256": downloaded_sha,
            "max_part_size": self.args.max_part_size,
            "trash_retention_seconds": self.args.trash_retention_seconds,
            "rate_limiter_enabled": self.args.rate_limiter_enabled,
            "native_protocol": native_protocol,
            "upload": upload,
            "additional_large_multipart": additional_large,
            "object_storm": object_storm,
            "same_fragment_mixed_users": same_fragment_mixed,
            "parallel_multipart_users": parallel_multipart,
            "same_key_version_contention": version_contention,
            "multi_fragment_user_isolation": multi_fragment,
            "permissions": permissions,
            "fragment_usage": fragment_usage,
            "rename_version_lineage": rename_lineage,
            "websocket": websocket,
            "crud": crud,
            "small_multipart": small_multipart,
            "archive": archive,
            "recover_on_read": recover_on_read,
            "object_repair": object_repair,
            "disk_format_repair": disk_repair,
            "redistribution": redistribution,
            "chaos": chaos,
            "live_object_count": live_expected.len(),
            "evidence": evidence,
            "rss_samples": rss_samples,
            "log_scan": logs,
            "index_inspection": inspection,
            "fragment_hash": self.fragment,
            "index_roots": self.index_roots,
            "blob_roots": self.blob_roots,
        });
        self.write_summary()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }

    async fn run_maintenance_interrupt_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("maintenance-before").await?;
        self.create_fragment().await?;
        self.auth().await?;

        let object_key = self.object_key();
        let source_len = self.args.source_file.metadata()?.len();
        if source_len <= self.args.max_part_size {
            return Err(format!(
                "source file is too small for large multipart proof: {} <= max_part_size {}",
                source_len, self.args.max_part_size
            )
            .into());
        }

        let mut expected = BTreeSet::new();
        let mut evidence = Vec::new();
        let mut rss_samples = Vec::new();
        let source_sha = self.sha256_file(&self.args.source_file).await?;
        rss_samples.push(self.rss_sample("maintenance_started"));

        let upload = self.multipart_restart_resume(&object_key).await?;
        expected.insert(object_key.clone());
        self.assert_download_sha256(&object_key, &source_sha)
            .await?;
        self.assert_object_status_clean(&object_key, "maintenance_large_multipart")
            .await?;
        evidence.push(json!({"gate": "large_multipart_restart_resume", "status": "PASS"}));
        rss_samples.push(self.rss_sample("after_large_multipart"));

        let storm = self
            .concurrent_object_storm(&mut expected, "maintenance-1000", self.args.storm_objects)
            .await?;
        evidence.push(json!({"gate": "thousand_object_concurrency", "status": "PASS"}));
        rss_samples.push(self.rss_sample("after_thousand_object_concurrency"));

        self.assert_expected_visible(&expected).await?;
        self.assert_health_clean("before_maintenance_interrupt")
            .await?;

        let disk_repair = self
            .simulate_disk_format_and_repair(&expected, &object_key, &source_sha)
            .await?;
        evidence.push(json!({"gate": "disk_format_repair_on_large_plus_1000", "status": "PASS"}));
        rss_samples.push(self.rss_sample("after_disk_format_repair"));

        let redistribution = self
            .redistribution_interrupt_resume_proof(&mut expected, &object_key, &source_sha)
            .await?;
        evidence.push(json!({"gate": "redistribution_interrupt_resume_2_1_2", "status": "PASS"}));
        rss_samples.push(self.rss_sample("after_redistribution_interrupt_resume"));

        self.assert_expected_visible(&expected).await?;
        self.assert_download_sha256(&object_key, &source_sha)
            .await?;
        self.assert_health_clean("maintenance_interrupt_final")
            .await?;
        let inspection = self.inspect_index(expected.len())?;
        self.stop_server()?;
        let logs = self.scan_logs()?;

        self.summary = json!({
            "status": "PASS",
            "engine": "n1",
            "proof": "n1_maintenance_interrupt_resume",
            "source_file": self.args.source_file,
            "source_bytes": source_len,
            "source_sha256": source_sha,
            "large_object_key": object_key,
            "max_part_size": self.args.max_part_size,
            "large_upload": upload,
            "object_storm": storm,
            "disk_format_repair": disk_repair,
            "redistribution_interrupt_resume": redistribution,
            "live_object_count": expected.len(),
            "evidence": evidence,
            "rss_samples": rss_samples,
            "log_scan": logs,
            "index_inspection": inspection,
            "fragment_hash": self.fragment,
            "index_roots": self.index_roots,
            "blob_roots": self.blob_roots,
        });
        self.write_summary()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }

    async fn run_object_maintenance_gate_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("object-maintenance-before").await?;
        self.create_fragment().await?;
        self.auth().await?;

        let object_key = self.object_key();
        let source_len = self.args.source_file.metadata()?.len();
        if source_len <= self.args.max_part_size {
            return Err(format!(
                "source file is too small for object maintenance gate proof: {} <= max_part_size {}",
                source_len, self.args.max_part_size
            )
            .into());
        }

        let mut expected = BTreeSet::new();
        let mut evidence = Vec::new();
        let mut rss_samples = Vec::new();
        let source_sha = self.sha256_file(&self.args.source_file).await?;
        rss_samples.push(self.rss_sample("object_maintenance_started"));

        let upload = self.multipart_restart_resume(&object_key).await?;
        expected.insert(object_key.clone());
        self.assert_download_sha256(&object_key, &source_sha)
            .await?;
        self.assert_object_status_clean(&object_key, "object_maintenance_baseline")
            .await?;
        evidence.push(json!({"gate": "baseline_large_multipart", "status": "PASS"}));

        let same_object_contention = self
            .same_object_repair_contention_proof(&object_key)
            .await?;
        rss_samples.push(self.rss_sample("after_same_object_repair_contention"));
        evidence.push(json!({"gate": "same_object_repair_contention", "status": "PASS"}));

        let object_then_fragment_repair = self
            .object_repair_then_fragment_repair_overlap_proof(&object_key)
            .await?;
        rss_samples.push(self.rss_sample("after_object_then_fragment_repair_overlap"));
        evidence
            .push(json!({"gate": "object_repair_then_fragment_repair_overlap", "status": "PASS"}));

        let storm_count = self.args.storm_objects.clamp(256, 1_000);
        let storm = self
            .concurrent_object_storm(&mut expected, "object-maint-gate", storm_count)
            .await?;
        rss_samples.push(self.rss_sample("after_object_maintenance_storm"));
        evidence.push(json!({"gate": "fragment_repair_seed_objects", "status": "PASS"}));

        let fragment_scrub_blocking = self
            .fragment_scrub_blocks_object_maintenance_proof(&expected, &object_key, &source_sha)
            .await?;
        rss_samples.push(self.rss_sample("after_fragment_scrub_blocking"));
        evidence
            .push(json!({"gate": "fragment_scrub_blocks_object_maintenance", "status": "PASS"}));

        self.assert_expected_visible(&expected).await?;
        self.assert_download_sha256(&object_key, &source_sha)
            .await?;
        self.assert_health_clean("object_maintenance_final").await?;
        let inspection = self.inspect_index(expected.len())?;
        self.stop_server()?;
        let logs = self.scan_logs()?;

        self.summary = json!({
            "status": "PASS",
            "engine": "n1",
            "proof": "n1_object_maintenance_gate",
            "source_file": self.args.source_file,
            "source_bytes": source_len,
            "source_sha256": source_sha,
            "large_object_key": object_key,
            "large_upload": upload,
            "same_object_repair_contention": same_object_contention,
            "object_repair_then_fragment_repair_overlap": object_then_fragment_repair,
            "fragment_repair_seed_storm": storm,
            "fragment_scrub_blocking": fragment_scrub_blocking,
            "live_object_count": expected.len(),
            "evidence": evidence,
            "rss_samples": rss_samples,
            "log_scan": logs,
            "index_inspection": inspection,
            "fragment_hash": self.fragment,
            "index_roots": self.index_roots,
            "blob_roots": self.blob_roots,
        });
        self.write_summary()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }
}
