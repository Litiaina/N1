impl Proof {
    async fn posix_crud_gc_proof(&self, expected: &mut BTreeSet<String>) -> ProofResult<Value> {
        let start = Instant::now();
        let base = format!("{}n1-crud", self.args.prefix);
        self.posix_post("/n1/posix/mkdir", json!({"path": format!("{base}/folder")}))
            .await?;

        let old_file = format!("{base}/rename-me.txt");
        let new_file = format!("{base}/renamed.txt");
        let rename_payload = b"n1 rename payload\n".to_vec();
        let rename_sha = sha256_bytes(&rename_payload);
        self.upload_bytes(&old_file, rename_payload, "text/plain")
            .await?;
        self.posix_post(
            "/n1/posix/rename",
            json!({"source_path": old_file, "destination_path": new_file}),
        )
        .await?;
        self.assert_download_sha256(&new_file, &rename_sha).await?;
        expected.insert(new_file.clone());

        let child = format!("{base}/folder/child.txt");
        let moved_child = format!("{base}/folder-renamed/child.txt");
        let child_payload = b"n1 folder rename payload\n".to_vec();
        let child_sha = sha256_bytes(&child_payload);
        self.upload_bytes(&child, child_payload, "text/plain")
            .await?;
        self.posix_post(
            "/n1/posix/rename",
            json!({"source_path": format!("{base}/folder"), "destination_path": format!("{base}/folder-renamed")}),
        )
        .await?;
        self.assert_download_sha256(&moved_child, &child_sha)
            .await?;

        let trash = self
            .posix_post(
                "/n1/posix/trash",
                json!({"path": format!("{base}/folder-renamed")}),
            )
            .await?;
        let trash_entry_id = trash
            .get("trash_entry_id")
            .and_then(Value::as_str)
            .ok_or("trash did not return trash_entry_id")?
            .to_string();
        self.posix_stat(&format!("{base}/folder-renamed"), true)
            .await?;
        self.posix_post(
            "/n1/posix/recover",
            json!({"path": format!("{base}/folder-renamed"), "trash_entry_id": trash_entry_id}),
        )
        .await?;
        self.assert_download_sha256(&moved_child, &child_sha)
            .await?;
        expected.insert(moved_child.clone());

        let delete_me = format!("{base}/delete-me.bin");
        let delete_payload = deterministic_payload(256 * 1024, b"n1-gc-purge");
        let version_id = self
            .upload_bytes(&delete_me, delete_payload, "application/octet-stream")
            .await?;
        let trash = self
            .posix_post("/n1/posix/trash", json!({"path": delete_me}))
            .await?;
        self.posix_post(
            "/n1/posix/purge",
            json!({"path": delete_me, "trash_entry_id": trash.get("trash_entry_id").and_then(Value::as_str).unwrap_or("")}),
        )
        .await?;
        self.run_gc_and_wait().await?;
        if self.blob_version_exists(&version_id) {
            return Err(format!("GC did not reclaim purged version {version_id}").into());
        }
        Ok(json!({
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "renamed_file": new_file,
            "recovered_folder_child": moved_child,
            "gc_reclaimed_version": version_id,
        }))
    }

    async fn small_multipart_proof(&self, expected: &mut BTreeSet<String>) -> ProofResult<Value> {
        let logical = format!("{}n1-multipart/video-parted.bin", self.args.prefix);
        let payload = deterministic_payload(8 * 1024 * 1024, b"n1-multipart-proof");
        let payload_sha = sha256_bytes(&payload);
        let part_size = 2 * 1024 * 1024_u64;
        let expected_parts = 4_u64;
        let params = json!({
            "content_type": "application/octet-stream",
            "total_size_bytes": payload.len(),
            "expected_parts": expected_parts,
            "part_size_bytes": part_size,
        });
        let start = Instant::now();
        let response = self
            .client
            .post(format!(
                "{}/n1/upload/multipart/init/{}",
                self.base_url,
                encode_path(&logical)
            ))
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "small multipart init failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let init: MultipartInit = response.json().await?;
        for idx in 0..expected_parts {
            let begin = idx as usize * part_size as usize;
            let end = begin + part_size as usize;
            let response = self
                .client
                .put(format!(
                    "{}/n1/upload/multipart/{}/{}?durability=batched",
                    self.base_url, init.version_id, idx
                ))
                .bearer_auth(&self.token)
                .header("content-type", "application/octet-stream")
                .body(payload[begin..end].to_vec())
                .send()
                .await?;
            if response.status() != StatusCode::OK && response.status() != StatusCode::CREATED {
                return Err(format!(
                    "small multipart part {idx} failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
        }
        let response = self
            .client
            .post(format!(
                "{}/n1/upload/multipart/{}/{}/finalize?object_key={}",
                self.base_url,
                init.version_id,
                init.session_id,
                encode_query_value(&logical)
            ))
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "small multipart finalize failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        self.assert_download_sha256(&logical, &payload_sha).await?;
        expected.insert(logical);
        Ok(json!({
            "bytes": payload.len(),
            "parts": expected_parts,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "version_id": init.version_id,
        }))
    }

    async fn archive_proof(&self, expected: &mut BTreeSet<String>) -> ProofResult<Value> {
        let archive_prefix = format!("{}archive-proof", self.args.prefix);
        let mut archive_expected = BTreeSet::new();
        for idx in 0..3 {
            let key = format!("{archive_prefix}/file-{idx:02}.txt");
            let payload = deterministic_payload(32 * 1024, key.as_bytes());
            let sha = sha256_bytes(&payload);
            self.upload_bytes(&key, payload, "text/plain").await?;
            self.assert_download_sha256(&key, &sha).await?;
            expected.insert(key.clone());
            archive_expected.insert(key);
        }

        let response = self
            .client
            .post(format!("{}/n1/objects/archive", self.base_url))
            .bearer_auth(&self.token)
            .json(
                &json!({"prefixes": [archive_prefix], "format": "tar", "filename": "n1-proof.tar"}),
            )
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "archive failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let raw = response_bytes_limited(response, 64 * 1024 * 1024, "archive").await?;
        let names = tar_names(&raw)?;
        for sample in &archive_expected {
            if !names.contains(sample) {
                return Err(format!("archive missing sample {sample}; names={names:?}").into());
            }
        }
        Ok(json!({
            "bytes": raw.len(),
            "members": names.len(),
            "samples": archive_expected,
        }))
    }

    async fn simulate_recover_on_read(
        &self,
        object_key: &str,
        expected_sha: &str,
    ) -> ProofResult<Value> {
        let version_id = self.latest_version_id(object_key).await?;
        let removed = self.remove_one_shard(&version_id)?;
        self.assert_download_sha256(object_key, expected_sha)
            .await?;
        self.wait_for_path(&removed, true, Duration::from_secs(30))
            .await?;
        Ok(json!({"object_key": object_key, "version_id": version_id, "restored": removed}))
    }

    async fn object_repair_status_proof(&self, object_key: &str) -> ProofResult<Value> {
        let version_id = self.latest_version_id(object_key).await?;
        let removed = self.remove_one_shard(&version_id)?;
        let degraded = self.object_status(object_key, "strict").await?;
        let status = degraded
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if status == "healthy" {
            return Err(
                format!("object status stayed healthy after shard removal: {degraded}").into(),
            );
        }
        let response = self
            .client
            .post(format!(
                "{}/n1/objects/repair/{}?mode=strict",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|err| format!("object repair request failed for {object_key}: {err:?}"))?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "object repair failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        self.wait_for_path(&removed, true, Duration::from_secs(30))
            .await?;
        self.assert_object_status_clean(object_key, "after_object_repair")
            .await?;
        Ok(
            json!({"object_key": object_key, "version_id": version_id, "removed": removed, "pre_repair_status": degraded}),
        )
    }

    async fn same_object_repair_contention_proof(&self, object_key: &str) -> ProofResult<Value> {
        let version_id = self.latest_version_id(object_key).await?;
        let removed = self.remove_one_shard(&version_id)?;
        let degraded = self.object_status(object_key, "strict").await?;
        let status = degraded
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if status == "healthy" {
            return Err(format!(
                "object status stayed healthy before contention repair: {degraded}"
            )
            .into());
        }

        let attempts = 8usize;
        let responses =
            join_all((0..attempts).map(|_| self.object_repair_raw(object_key, "strict"))).await;
        let mut ok = 0usize;
        let mut busy = 0usize;
        let mut details = Vec::new();
        for response in responses {
            let (status, body) = response?;
            if status == StatusCode::OK {
                ok += 1;
            } else if status == StatusCode::SERVICE_UNAVAILABLE {
                busy += 1;
            }
            details.push(json!({
                "status": status.as_u16(),
                "body": body,
            }));
        }
        if ok == 0 {
            return Err(format!(
                "same-object repair contention had no successful repair: {details:?}"
            )
            .into());
        }
        if busy == 0 {
            return Err(format!(
                "same-object repair contention did not reject any concurrent repair: {details:?}"
            )
            .into());
        }

        self.wait_for_path(&removed, true, Duration::from_secs(60))
            .await?;
        self.assert_object_status_clean(object_key, "after_same_object_repair_contention")
            .await?;
        Ok(json!({
            "object_key": object_key,
            "version_id": version_id,
            "removed": removed,
            "pre_repair_status": degraded,
            "attempts": attempts,
            "ok": ok,
            "service_unavailable": busy,
            "responses": details,
        }))
    }

    async fn object_repair_then_fragment_repair_overlap_proof(
        &self,
        object_key: &str,
    ) -> ProofResult<Value> {
        let version_id = self.latest_version_id(object_key).await?;
        let removed = self.remove_one_shard(&version_id)?;
        let degraded = self.object_status(object_key, "strict").await?;
        let status = degraded
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if status == "healthy" {
            return Err(
                format!("object status stayed healthy before overlap repair: {degraded}").into(),
            );
        }

        let client = self.client.clone();
        let token = self.token.clone();
        let url = format!(
            "{}/n1/objects/repair/{}?mode=strict",
            self.base_url,
            encode_path(object_key)
        );
        let object_repair = tokio::spawn(async move {
            let response = client.post(url).bearer_auth(token).send().await?;
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Ok::<_, reqwest::Error>((status, body))
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let fragment_start = self.start_repair_job().await?;
        let (object_repair_code, object_repair_body) = object_repair.await??;
        if object_repair_code != StatusCode::OK {
            return Err(format!(
                "object repair did not complete successfully before/with fragment repair: status={} body={}",
                object_repair_code, object_repair_body
            )
            .into());
        }
        let fragment_repair = self
            .wait_job("/n1/maintenance/repair/status", "repair")
            .await?;
        self.wait_for_path(&removed, true, Duration::from_secs(60))
            .await?;
        self.assert_object_status_clean(object_key, "after_object_fragment_repair_overlap")
            .await?;

        Ok(json!({
            "object_key": object_key,
            "version_id": version_id,
            "removed": removed,
            "pre_repair_status": degraded,
            "object_repair": {
                "status": object_repair_code.as_u16(),
                "body": object_repair_body,
            },
            "fragment_repair_start": fragment_start,
            "fragment_repair": fragment_repair,
        }))
    }

    async fn fragment_scrub_blocks_object_maintenance_proof(
        &mut self,
        expected: &BTreeSet<String>,
        sample_key: &str,
        sample_sha: &str,
    ) -> ProofResult<Value> {
        self.stop_server()?;
        let removed_index = self.index_roots[0].join(&self.fragment);
        let removed_blob = self.blob_roots[0].join(&self.fragment);
        let _ = fs::remove_dir_all(&removed_index);
        let _ = fs::remove_dir_all(&removed_blob);
        self.start_server("fragment-scrub-blocking").await?;
        self.auth().await?;

        let start = self.start_repair_job().await?;
        let running = self.wait_repair_running(Duration::from_secs(30)).await?;

        let (status_code, status_body) = self.object_status_raw(sample_key, "strict").await?;
        if status_code != StatusCode::SERVICE_UNAVAILABLE {
            return Err(format!(
                "object status was not blocked during fragment scrub repair: status={} body={}",
                status_code, status_body
            )
            .into());
        }

        let (repair_code, repair_body) = self.object_repair_raw(sample_key, "strict").await?;
        if repair_code != StatusCode::SERVICE_UNAVAILABLE {
            return Err(format!(
                "object repair was not blocked during fragment scrub repair: status={} body={}",
                repair_code, repair_body
            )
            .into());
        }

        let repair = self
            .wait_job("/n1/maintenance/repair/status", "repair")
            .await?;
        self.assert_expected_visible(expected).await?;
        self.assert_download_sha256(sample_key, sample_sha).await?;
        self.assert_object_status_clean(sample_key, "after_fragment_scrub_blocking")
            .await?;
        self.assert_health_clean("after_fragment_scrub_blocking")
            .await?;

        Ok(json!({
            "removed_index": removed_index,
            "removed_blob": removed_blob,
            "start": start,
            "running": running,
            "blocked_object_status": {
                "status": status_code.as_u16(),
                "body": status_body,
            },
            "blocked_object_repair": {
                "status": repair_code.as_u16(),
                "body": repair_body,
            },
            "repair": repair,
        }))
    }

    async fn simulate_disk_format_and_repair(
        &mut self,
        expected: &BTreeSet<String>,
        sample_key: &str,
        sample_sha: &str,
    ) -> ProofResult<Value> {
        self.stop_server()?;
        let removed_index = self.index_roots[0].join(&self.fragment);
        let removed_blob = self.blob_roots[0].join(&self.fragment);
        let _ = fs::remove_dir_all(&removed_index);
        let _ = fs::remove_dir_all(&removed_blob);
        self.start_server("disk-format").await?;
        self.auth().await?;
        self.run_repair_and_wait().await?;
        self.assert_expected_visible(expected).await?;
        self.assert_download_sha256(sample_key, sample_sha).await?;
        self.assert_health_clean("after_disk_format_repair").await?;
        Ok(json!({"removed_index": removed_index, "removed_blob": removed_blob}))
    }

    async fn redistribution_proof(
        &mut self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        self.stop_server()?;
        self.active_disk_count = 1;
        self.parity_shards = 0;
        self.write_config()?;
        self.start_server("one-disk").await?;
        self.auth().await?;
        self.assert_expected_visible(expected).await?;
        self.run_redistribution_and_wait().await?;

        let one_disk_key = format!("{}n1-redistribution/one-disk.txt", self.args.prefix);
        let payload = b"n1 one disk redistribution payload\n".to_vec();
        let sha = sha256_bytes(&payload);
        self.upload_bytes(&one_disk_key, payload, "text/plain")
            .await?;
        self.assert_download_sha256(&one_disk_key, &sha).await?;
        expected.insert(one_disk_key.clone());

        self.stop_server()?;
        self.active_disk_count = self.index_roots.len();
        self.parity_shards = 1;
        self.write_config()?;
        self.start_server("two-disk-again").await?;
        self.auth().await?;
        self.run_redistribution_and_wait().await?;
        self.run_repair_and_wait().await?;
        self.assert_expected_visible(expected).await?;
        self.assert_download_sha256(&one_disk_key, &sha).await?;
        self.assert_health_clean("after_redistribution").await?;
        Ok(json!({"one_disk_object": one_disk_key, "active_disk_count": self.active_disk_count}))
    }

    async fn redistribution_interrupt_resume_proof(
        &mut self,
        expected: &mut BTreeSet<String>,
        sample_key: &str,
        sample_sha: &str,
    ) -> ProofResult<Value> {
        self.stop_server()?;
        self.active_disk_count = 1;
        self.parity_shards = 0;
        self.write_config()?;
        self.start_server("redistribution-interrupt-one-disk")
            .await?;
        self.auth().await?;
        self.assert_expected_visible(expected).await?;

        let one_disk_interrupt = self
            .start_redistribution_then_interrupt("redistribution-interrupt-one-disk", 8)
            .await?;

        self.start_server("redistribution-resume-one-disk").await?;
        self.auth().await?;
        let one_disk_resume = self.run_redistribution_and_wait().await?;
        self.assert_expected_visible(expected).await?;
        self.assert_download_sha256(sample_key, sample_sha).await?;

        let one_disk_key = format!(
            "{}n1-redistribution/one-disk-after-resume.txt",
            self.args.prefix
        );
        let payload = b"n1 one disk write after interrupted redistribution resume\n".to_vec();
        let one_disk_sha = sha256_bytes(&payload);
        self.upload_bytes(&one_disk_key, payload, "text/plain")
            .await?;
        self.assert_download_sha256(&one_disk_key, &one_disk_sha)
            .await?;
        expected.insert(one_disk_key.clone());

        self.stop_server()?;
        self.active_disk_count = self.index_roots.len();
        self.parity_shards = 1;
        self.write_config()?;
        self.start_server("redistribution-interrupt-two-disk")
            .await?;
        self.auth().await?;
        self.assert_expected_visible(expected).await?;

        let two_disk_interrupt = self
            .start_redistribution_then_interrupt("redistribution-interrupt-two-disk", 8)
            .await?;

        self.start_server("redistribution-resume-two-disk").await?;
        self.auth().await?;
        let two_disk_resume = self.run_redistribution_and_wait().await?;
        let repair_after_resume = self.run_repair_and_wait().await?;
        self.assert_expected_visible(expected).await?;
        self.assert_download_sha256(sample_key, sample_sha).await?;
        self.assert_download_sha256(&one_disk_key, &one_disk_sha)
            .await?;
        self.assert_health_clean("after_redistribution_interrupt_resume")
            .await?;

        Ok(json!({
            "one_disk_interrupt": one_disk_interrupt,
            "one_disk_resume": one_disk_resume,
            "one_disk_object": one_disk_key,
            "two_disk_interrupt": two_disk_interrupt,
            "two_disk_resume": two_disk_resume,
            "repair_after_resume": repair_after_resume,
            "active_disk_count": self.active_disk_count,
        }))
    }

    async fn start_redistribution_then_interrupt(
        &mut self,
        log_label: &str,
        min_redistributed_lines: usize,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let started = self.start_redistribution_job().await?;
        self.wait_for_log_count(
            log_label,
            "Redistributed ",
            min_redistributed_lines,
            Duration::from_secs(180),
        )
        .await?;
        let status_before_stop = self
            .redistribution_status()
            .await
            .unwrap_or_else(|err| json!({"status_read_error": err.to_string()}));
        self.stop_server()?;
        let log_count = self.count_log_matches(log_label, "Redistributed ")?;
        Ok(json!({
            "started": started,
            "interrupted_after_redistributed_log_lines": log_count,
            "status_before_stop": status_before_stop,
            "elapsed_before_stop_s": round3(start.elapsed().as_secs_f64()),
        }))
    }
}
