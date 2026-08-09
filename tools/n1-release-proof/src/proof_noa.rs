impl Proof {
    async fn run_noa_api_smoke(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("noa-v1-before").await?;
        self.create_fragment().await?;
        self.auth().await?;

        let legacy_status_before = self.assert_legacy_http_namespace_unavailable().await?;
        let object_key = format!("{}noa-api/object.bin", self.args.prefix);
        let payload = deterministic_payload(2 * 1024 * 1024, b"noa-v1-api-proof");
        let payload_sha256 = sha256_bytes(&payload);
        let version_id = self
            .upload_bytes(&object_key, payload, "application/octet-stream")
            .await?;

        self.assert_list_contains(&object_key).await?;
        self.posix_stat(&object_key, false).await?;
        self.assert_download_sha256(&object_key, &payload_sha256)
            .await?;
        self.range_probe(&object_key).await?;
        self.assert_object_status_clean(&object_key, "NOA initial object status")
            .await?;

        let mut expected = BTreeSet::from([object_key.clone()]);
        let individual_small_objects = self
            .concurrent_object_storm(&mut expected, "noa-individual-small", 256)
            .await?;
        let standard_batch = self.standard_batch_upload_proof(&mut expected).await?;
        let websocket = self.websocket_namespace_delta_proof(&mut expected).await?;
        let crud = self.posix_crud_gc_proof(&mut expected).await?;
        let maintenance_status = self.noa_maintenance_status_smoke().await?;
        self.assert_health_clean("NOA before restart").await?;
        let before_restart_rss = self.rss_sample("noa_before_restart");

        self.stop_server()?;
        self.start_server("noa-v1-after-restart").await?;
        self.auth().await?;
        self.assert_expected_visible(&expected).await?;
        self.assert_download_sha256(&object_key, &payload_sha256)
            .await?;
        self.assert_object_status_clean(&object_key, "NOA after restart object status")
            .await?;
        self.assert_health_clean("NOA after restart").await?;
        let legacy_status_after = self.assert_legacy_http_namespace_unavailable().await?;
        let after_restart_rss = self.rss_sample("noa_after_restart");

        let binary_sha256 = self.sha256_file(&self.args.binary).await?;
        let binary_version_output = self.noa_binary_version_output()?;
        let inspection = self.inspect_index(expected.len())?;
        self.stop_server()?;
        let logs = self.scan_logs()?;

        self.summary = json!({
            "status": "PASS",
            "engine": "n1",
            "engine_version_output": binary_version_output,
            "api": "NOA",
            "api_version": "v1",
            "proof": "noa_v1_api_smoke",
            "binary": self.args.binary,
            "binary_sha256": binary_sha256,
            "fragment_hash": self.fragment,
            "object_key": object_key,
            "object_version_id": version_id,
            "payload_sha256": payload_sha256,
            "individual_small_objects": individual_small_objects,
            "standard_batch": standard_batch,
            "legacy_http_status_before_restart": legacy_status_before,
            "legacy_http_status_after_restart": legacy_status_after,
            "websocket": websocket,
            "posix_crud_lifecycle": crud,
            "maintenance_status": maintenance_status,
            "restart_persistence": "PASS",
            "expected_live_objects": expected,
            "rss_samples": [before_restart_rss, after_restart_rss],
            "index_inspection": inspection,
            "log_scan": logs,
            "index_roots": self.index_roots,
            "blob_roots": self.blob_roots,
            "topology_evidence": "functional D+P proof; physical durability classification requires separately recorded mount and device evidence",
        });
        self.write_summary()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }

    async fn assert_legacy_http_namespace_unavailable(&self) -> ProofResult<u16> {
        let response = self
            .client
            .get(format!("{}/n1/ping", self.base_url))
            .bearer_auth(&self.token)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() || status.is_redirection() {
            return Err(format!(
                "legacy /n1 HTTP namespace is still active: status={} body={}",
                status,
                response.text().await?
            )
            .into());
        }
        Ok(status.as_u16())
    }

    fn noa_binary_version_output(&self) -> ProofResult<String> {
        let output = Command::new(&self.args.binary).arg("--version").output()?;
        if !output.status.success() {
            return Err(format!(
                "N1 --version failed with status {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(String::from_utf8(output.stdout)?.trim().to_string())
    }

    async fn noa_maintenance_status_smoke(&self) -> ProofResult<Value> {
        let mut statuses = serde_json::Map::new();
        for (name, route) in [
            ("repair", "/noa/v1/maintenance/repair/status"),
            (
                "redistribution",
                "/noa/v1/maintenance/redistribution/status",
            ),
            ("gc", "/noa/v1/maintenance/gc/status"),
        ] {
            let response = self
                .client
                .get(format!("{}{}", self.base_url, route))
                .bearer_auth(&self.token)
                .send()
                .await?;
            if response.status() != StatusCode::OK {
                return Err(format!(
                    "NOA {name} status failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
            statuses.insert(name.to_string(), response.json().await?);
        }
        Ok(Value::Object(statuses))
    }
}
