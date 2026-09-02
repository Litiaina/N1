impl Proof {
    async fn run_concurrent_upload_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("concurrent-upload-before").await?;
        self.create_fragment().await?;
        self.auth().await?;

        let mut expected = BTreeSet::new();
        let multipart = self.parallel_multipart_user_proof(&mut expected).await?;
        let batches = self
            .concurrent_standard_batch_upload_proof(&mut expected)
            .await?;
        let mixed_operations = tokio::time::timeout(
            Duration::from_secs(180),
            self.same_fragment_mixed_user_proof(&mut expected),
        )
        .await
        .map_err(|_| "concurrent mixed read/write/namespace workload timed out")??;
        self.assert_health_clean("after concurrent upload proof")
            .await?;
        let before_restart_rss = self.rss_sample("concurrent_upload_before_restart");

        self.stop_server()?;
        self.start_server("concurrent-upload-after").await?;
        self.auth().await?;
        self.assert_expected_visible(&expected).await?;
        self.assert_health_clean("concurrent upload proof after restart")
            .await?;
        let after_restart_rss = self.rss_sample("concurrent_upload_after_restart");
        self.stop_server()?;
        let logs = self.scan_logs()?;

        self.summary = json!({
            "status": "PASS",
            "engine": "n1",
            "proof": "concurrent_standard_multipart_and_batch",
            "topology": {
                "data_shards": self.data_shards,
                "parity_shards": self.parity_shards,
                "roots": self.active_disk_count,
            },
            "multipart": multipart,
            "standard_batches": batches,
            "mixed_operations": mixed_operations,
            "restart_visibility": "PASS",
            "expected_live_objects": expected.len(),
            "rss_samples": [before_restart_rss, after_restart_rss],
            "log_scan": logs,
        });
        self.write_summary()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }
}
