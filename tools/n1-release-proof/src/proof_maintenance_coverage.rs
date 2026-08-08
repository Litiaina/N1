impl Proof {
    async fn maintenance_health_snapshot(
        &self,
        deep_scan: bool,
        refresh: bool,
    ) -> ProofResult<Value> {
        let response = self
            .client
            .get(format!(
                "{}/noa/v1/maintenance/health?deep_scan={deep_scan}&refresh={refresh}",
                self.base_url
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "maintenance health failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json().await?)
    }

    fn assert_maintenance_access(
        snapshot: &Value,
        expected: &str,
        label: &str,
    ) -> ProofResult<()> {
        let actual = snapshot
            .pointer("/maintenance/access_mode")
            .and_then(Value::as_str)
            .unwrap_or("missing");
        if actual != expected {
            return Err(format!(
                "{label} maintenance access mismatch: expected={expected} actual={actual} snapshot={snapshot}"
            )
            .into());
        }
        Ok(())
    }

    fn assert_no_persistent_healer_fault(snapshot: &Value, label: &str) -> ProofResult<()> {
        let healer = snapshot
            .pointer("/maintenance/auto_healing")
            .ok_or_else(|| format!("{label} health has no auto_healing object: {snapshot}"))?;
        if healer.get("write_safe").and_then(Value::as_bool) != Some(true)
            || healer.get("read_only_reason").is_some_and(|value| !value.is_null())
            || healer.get("last_error").is_some_and(|value| !value.is_null())
        {
            return Err(format!("{label} retained a healer fault: {healer}").into());
        }
        Ok(())
    }

    async fn start_manual_repair_mode(&self, mode: &str) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/maintenance/fragments/{}/repair/start?mode={mode}&fast_mode=false&meta_concurrency=1&repair_concurrency=1&batch=32",
                self.base_url, self.fragment
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "manual {mode} repair start failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(json!({"mode": mode, "accepted": true}))
    }

    async fn wait_for_maintenance_access(
        &self,
        expected: &str,
        timeout: Duration,
    ) -> ProofResult<Value> {
        let deadline = Instant::now() + timeout;
        let mut last = json!(null);
        while Instant::now() < deadline {
            last = self.maintenance_health_snapshot(false, true).await?;
            if last
                .pointer("/maintenance/access_mode")
                .and_then(Value::as_str)
                == Some(expected)
            {
                return Ok(last);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Err(format!(
            "maintenance access did not become {expected} within {timeout:?}; last={last}"
        )
        .into())
    }

    async fn raw_one_shot_mutation(&self, object_key: &str) -> ProofResult<(StatusCode, String)> {
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/upload/one-shot/{}",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(&self.token)
            .header("content-type", "text/plain")
            .body(b"maintenance access probe\n".to_vec())
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Ok((status, body))
    }

    async fn raw_download(&self, object_key: &str) -> ProofResult<(StatusCode, usize)> {
        let response = self
            .client
            .get(format!(
                "{}/noa/v1/objects/download?object_key={}",
                self.base_url,
                encode_query_value(object_key)
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        Ok((status, bytes.len()))
    }

    async fn upload_complete_multipart_payload(
        &self,
        object_key: &str,
        payload: &[u8],
        part_size: usize,
    ) -> ProofResult<String> {
        if payload.is_empty() || part_size == 0 || !payload.len().is_multiple_of(part_size) {
            return Err("maintenance baseline multipart shape is invalid".into());
        }
        let expected_parts = payload.len() / part_size;
        let params = json!({
            "content_type": "application/octet-stream",
            "total_size_bytes": payload.len(),
            "expected_parts": expected_parts,
            "part_size_bytes": part_size,
        });
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/upload/multipart/durable/init/{}",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "maintenance baseline multipart init failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let init: MultipartInit = response.json().await?;
        for index in 0..expected_parts {
            let begin = index * part_size;
            let end = begin + part_size;
            let response = self
                .client
                .put(format!(
                    "{}/noa/v1/upload/multipart/durable/{}/{}?durability=batched",
                    self.base_url, init.version_id, index
                ))
                .bearer_auth(&self.token)
                .header("content-type", "application/octet-stream")
                .body(payload[begin..end].to_vec())
                .send()
                .await?;
            if !matches!(response.status(), StatusCode::OK | StatusCode::CREATED) {
                return Err(format!(
                    "maintenance baseline multipart part {index} failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
        }
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/upload/multipart/durable/{}/{}/finalize?object_key={}",
                self.base_url,
                init.version_id,
                init.session_id,
                encode_query_value(object_key)
            ))
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "maintenance baseline multipart finalize failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(init.version_id)
    }

    async fn upload_incomplete_multipart_then_restart(
        &mut self,
        object_key: &str,
    ) -> ProofResult<Value> {
        let payload = deterministic_payload(8 * 1024 * 1024, b"n1-maintenance-restart");
        let expected_sha = sha256_bytes(&payload);
        let part_size = 2 * 1024 * 1024usize;
        let expected_parts = payload.len() / part_size;
        let params = json!({
            "content_type": "application/octet-stream",
            "total_size_bytes": payload.len(),
            "expected_parts": expected_parts,
            "part_size_bytes": part_size,
        });
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/upload/multipart/durable/init/{}",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "maintenance multipart init failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let init: MultipartInit = response.json().await?;

        let first = self
            .client
            .put(format!(
                "{}/noa/v1/upload/multipart/durable/{}/0?durability=batched",
                self.base_url, init.version_id
            ))
            .bearer_auth(&self.token)
            .header("content-type", "application/octet-stream")
            .body(payload[..part_size].to_vec())
            .send()
            .await?;
        if !matches!(first.status(), StatusCode::OK | StatusCode::CREATED) {
            return Err(format!(
                "maintenance multipart first part failed: {} {}",
                first.status(),
                first.text().await?
            )
            .into());
        }

        self.stop_server()?;
        self.start_server("maintenance-coverage-multipart-restart")
            .await?;
        self.auth().await?;
        let startup = self
            .wait_for_write_safe("incomplete multipart restart topology")
            .await?;
        let deep_after_restart = self.maintenance_health_snapshot(true, true).await?;
        Self::assert_maintenance_access(
            &deep_after_restart,
            "availability",
            "deep scan after incomplete multipart restart",
        )?;
        Self::assert_no_persistent_healer_fault(
            &deep_after_restart,
            "deep scan after incomplete multipart restart",
        )?;

        for index in 1..expected_parts {
            let begin = index * part_size;
            let end = begin + part_size;
            let response = self
                .client
                .put(format!(
                    "{}/noa/v1/upload/multipart/durable/{}/{}?durability=batched",
                    self.base_url, init.version_id, index
                ))
                .bearer_auth(&self.token)
                .header("content-type", "application/octet-stream")
                .body(payload[begin..end].to_vec())
                .send()
                .await?;
            if !matches!(response.status(), StatusCode::OK | StatusCode::CREATED) {
                return Err(format!(
                    "maintenance multipart resumed part {index} failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
        }
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/upload/multipart/durable/{}/{}/finalize?object_key={}",
                self.base_url,
                init.version_id,
                init.session_id,
                encode_query_value(object_key)
            ))
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "maintenance multipart finalize failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        self.assert_download_sha256(object_key, &expected_sha).await?;
        Ok(json!({
            "version_id": init.version_id,
            "session_id": init.session_id,
            "parts": expected_parts,
            "bytes": payload.len(),
            "startup": startup,
            "deep_scan_after_restart": deep_after_restart,
            "sha256_verified": true,
        }))
    }

    async fn run_maintenance_coverage_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        if self.data_shards == 0 || self.parity_shards == 0 {
            return Err(
                "maintenance coverage proof requires non-zero data and parity shards".into(),
            );
        }
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("maintenance-coverage-startup").await?;
        self.wait_for_log_count(
            "maintenance-coverage-startup",
            "leaving automatic read-only mode",
            1,
            Duration::from_secs(30),
        )
        .await?;
        self.create_fragment().await?;
        self.auth().await?;
        let startup = self.wait_for_write_safe("maintenance coverage startup").await?;

        let baseline_key = format!("{}maintenance-coverage/baseline.bin", self.args.prefix);
        let baseline_payload =
            deterministic_payload(128 * 1024 * 1024, b"n1-maintenance-coverage-baseline");
        let baseline_sha = sha256_bytes(&baseline_payload);
        let baseline_version = self
            .upload_complete_multipart_payload(
                &baseline_key,
                &baseline_payload,
                32 * 1024 * 1024,
            )
            .await?;
        self.assert_download_sha256(&baseline_key, &baseline_sha)
            .await?;

        let deep_fresh = self.maintenance_health_snapshot(true, true).await?;
        Self::assert_maintenance_access(
            &deep_fresh,
            "availability",
            "deep scan plus fresh snapshot",
        )?;
        Self::assert_no_persistent_healer_fault(&deep_fresh, "deep scan plus fresh snapshot")?;
        let post_deep_key = format!("{}maintenance-coverage/post-deep.txt", self.args.prefix);
        self.upload_bytes(
            &post_deep_key,
            b"deep scan did not gate writes\n".to_vec(),
            "text/plain",
        )
        .await?;

        let restart = self
            .upload_incomplete_multipart_then_restart(&format!(
                "{}maintenance-coverage/restarted-multipart.bin",
                self.args.prefix
            ))
            .await?;

        let removed = self.remove_one_shard_matching(&baseline_version, |path| {
            path.extension().and_then(|ext| ext.to_str()) == Some("parity")
        })?;
        let availability_start = self.start_manual_repair_mode("availability").await?;
        let availability_running = self.wait_repair_running(Duration::from_secs(30)).await?;
        let availability_probe_key =
            format!("{}maintenance-coverage/availability-write.txt", self.args.prefix);
        let (availability_write_status, availability_write_body) =
            self.raw_one_shot_mutation(&availability_probe_key).await?;
        if availability_write_status != StatusCode::CREATED {
            return Err(format!(
                "availability repair blocked a foreground write: status={availability_write_status} body={availability_write_body}"
            )
            .into());
        }
        let availability_result = self
            .wait_job("/noa/v1/maintenance/repair/status", "availability repair")
            .await?;
        self.wait_for_path(&removed, true, Duration::from_secs(30))
            .await?;
        let availability_final = self.maintenance_health_snapshot(true, true).await?;
        Self::assert_maintenance_access(
            &availability_final,
            "availability",
            "availability repair completion",
        )?;
        Self::assert_no_persistent_healer_fault(
            &availability_final,
            "availability repair completion",
        )?;

        let checksum_start = self.start_manual_repair_mode("checksum").await?;
        let checksum_running = self.wait_repair_running(Duration::from_secs(30)).await?;
        let checksum_blocked_key = format!(
            "{}maintenance-coverage/checksum-blocked.txt",
            self.args.prefix
        );
        let (checksum_gate, checksum_read, checksum_write) = tokio::join!(
            self.wait_for_maintenance_access("read_only", Duration::from_secs(30)),
            self.raw_download(&baseline_key),
            self.raw_one_shot_mutation(&checksum_blocked_key),
        );
        let checksum_gate = checksum_gate?;
        let (checksum_read_status, checksum_read_bytes) = checksum_read?;
        if checksum_read_status != StatusCode::OK || checksum_read_bytes != 128 * 1024 * 1024 {
            return Err(format!(
                "checksum read-only mode did not preserve reads: status={checksum_read_status} bytes={checksum_read_bytes}"
            )
            .into());
        }
        let (checksum_write_status, checksum_write_body) = checksum_write?;
        if checksum_write_status != StatusCode::FORBIDDEN {
            return Err(format!(
                "checksum mode did not temporarily reject mutation with 403: status={checksum_write_status} body={checksum_write_body}"
            )
            .into());
        }
        let checksum_result = self
            .wait_job("/noa/v1/maintenance/repair/status", "checksum repair")
            .await?;
        let checksum_final = self.maintenance_health_snapshot(true, true).await?;
        Self::assert_maintenance_access(
            &checksum_final,
            "availability",
            "checksum completion",
        )?;
        Self::assert_no_persistent_healer_fault(&checksum_final, "checksum completion")?;

        let strict_start = self.start_manual_repair_mode("strict").await?;
        let strict_running = self.wait_repair_running(Duration::from_secs(30)).await?;
        let strict_blocked_key = format!(
            "{}maintenance-coverage/strict-blocked.txt",
            self.args.prefix
        );
        let (strict_gate, strict_write) = tokio::join!(
            self.wait_for_maintenance_access("read_only", Duration::from_secs(30)),
            self.raw_one_shot_mutation(&strict_blocked_key),
        );
        let strict_gate = strict_gate?;
        let (strict_write_status, strict_write_body) = strict_write?;
        if strict_write_status != StatusCode::FORBIDDEN {
            return Err(format!(
                "strict mode did not temporarily reject mutation with 403: status={strict_write_status} body={strict_write_body}"
            )
            .into());
        }
        let strict_result = self
            .wait_job("/noa/v1/maintenance/repair/status", "strict repair")
            .await?;
        let strict_final = self.maintenance_health_snapshot(true, true).await?;
        Self::assert_maintenance_access(&strict_final, "availability", "strict completion")?;
        Self::assert_no_persistent_healer_fault(&strict_final, "strict completion")?;

        let scrub_start = self.start_manual_repair_mode("scrub").await?;
        let scrub_running = self.wait_repair_running(Duration::from_secs(30)).await?;
        let scrub_blocked_key = format!(
            "{}maintenance-coverage/scrub-blocked.txt",
            self.args.prefix
        );
        let (scrub_gate, scrub_read, scrub_write) = tokio::join!(
            self.wait_for_maintenance_access("quiesced", Duration::from_secs(30)),
            self.raw_download(&baseline_key),
            self.raw_one_shot_mutation(&scrub_blocked_key),
        );
        let scrub_gate = scrub_gate?;
        let (scrub_read_status, _) = scrub_read?;
        if scrub_read_status != StatusCode::SERVICE_UNAVAILABLE {
            return Err(format!(
                "scrub mode did not quiesce reads with 503: status={scrub_read_status}"
            )
            .into());
        }
        let (scrub_write_status, scrub_write_body) = scrub_write?;
        if scrub_write_status != StatusCode::SERVICE_UNAVAILABLE {
            return Err(format!(
                "scrub mode did not quiesce mutation with 503: status={scrub_write_status} body={scrub_write_body}"
            )
            .into());
        }
        let scrub_result = self
            .wait_job("/noa/v1/maintenance/repair/status", "scrub repair")
            .await?;
        let scrub_final = self.maintenance_health_snapshot(true, true).await?;
        Self::assert_maintenance_access(&scrub_final, "availability", "scrub completion")?;
        Self::assert_no_persistent_healer_fault(&scrub_final, "scrub completion")?;

        let final_probe_key = format!("{}maintenance-coverage/final-write.txt", self.args.prefix);
        self.upload_bytes(
            &final_probe_key,
            b"maintenance returned to availability\n".to_vec(),
            "text/plain",
        )
        .await?;
        self.assert_download_sha256(&baseline_key, &baseline_sha)
            .await?;
        self.assert_object_status_clean(&baseline_key, "maintenance coverage final")
            .await?;
        let final_health = self.maintenance_health_snapshot(true, true).await?;
        Self::assert_maintenance_access(&final_health, "availability", "final health")?;
        Self::assert_no_persistent_healer_fault(&final_health, "final health")?;

        self.summary = json!({
            "status": "PASS",
            "proof": "n1_maintenance_mode_coverage",
            "physical_topology": {
                "disk_roots": self.disk_roots()?,
                "data_shards": self.data_shards,
                "parity_shards": self.parity_shards,
                "colocated_roots": self.args.colocated_roots,
            },
            "startup": startup,
            "deep_scan_plus_fresh_snapshot": {
                "health": deep_fresh,
                "post_scan_mutation_succeeded": true,
            },
            "incomplete_multipart_restart": restart,
            "manual_modes": {
                "availability": {
                    "start": availability_start,
                    "running": availability_running,
                    "foreground_write_status": availability_write_status.as_u16(),
                    "result": availability_result,
                    "final_health": availability_final,
                },
                "checksum": {
                    "start": checksum_start,
                    "running": checksum_running,
                    "gate_health": checksum_gate,
                    "read_status": checksum_read_status.as_u16(),
                    "blocked_write_status": checksum_write_status.as_u16(),
                    "result": checksum_result,
                    "final_health": checksum_final,
                },
                "strict": {
                    "start": strict_start,
                    "running": strict_running,
                    "gate_health": strict_gate,
                    "blocked_write_status": strict_write_status.as_u16(),
                    "result": strict_result,
                    "final_health": strict_final,
                },
                "scrub": {
                    "start": scrub_start,
                    "running": scrub_running,
                    "gate_health": scrub_gate,
                    "blocked_read_status": scrub_read_status.as_u16(),
                    "blocked_write_status": scrub_write_status.as_u16(),
                    "result": scrub_result,
                    "final_health": scrub_final,
                },
            },
            "final_health": final_health,
            "final_object_sha256_verified": true,
            "persistent_read_only_after_clean_completion": false,
        });
        self.write_summary()?;
        self.stop_server()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }
}
