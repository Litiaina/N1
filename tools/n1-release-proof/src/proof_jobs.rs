impl Proof {
    async fn chaos_proof(
        &mut self,
        expected: &mut BTreeSet<String>,
        large_sha: &str,
        rss_samples: &mut Vec<Value>,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let min_cycles = if self.args.chaos_cycles == 0 {
            1
        } else {
            self.args.chaos_cycles
        };
        let deadline = if self.args.chaos_seconds == 0 {
            None
        } else {
            Some(start + Duration::from_secs(self.args.chaos_seconds))
        };
        let file_name = self
            .args
            .source_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("large-object.bin")
            .to_string();
        let mut cycles = Vec::new();
        let mut cycle_idx = 0usize;
        loop {
            let cycle_start = Instant::now();
            let storm = self
                .concurrent_object_storm(
                    expected,
                    &format!("chaos-cycle-{cycle_idx:03}"),
                    self.args.chaos_storm_objects,
                )
                .await?;
            rss_samples.push(self.rss_sample(&format!("chaos_{cycle_idx:03}_after_storm")));

            let large_key = format!(
                "{}chaos/cycle-{cycle_idx:03}/multipart-{file_name}",
                self.args.prefix
            );
            let upload = self.multipart_restart_resume(&large_key).await?;
            expected.insert(large_key.clone());
            self.assert_download_sha256(&large_key, large_sha).await?;
            rss_samples.push(self.rss_sample(&format!("chaos_{cycle_idx:03}_after_large")));

            let version_id = self.latest_version_id(&large_key).await?;
            let removed = self.remove_one_shard(&version_id)?;
            self.assert_download_sha256(&large_key, large_sha).await?;
            self.wait_for_path(&removed, true, Duration::from_secs(60))
                .await?;
            self.assert_object_status_clean(&large_key, "chaos_recover_on_read")
                .await?;

            let trash_root = format!("{}chaos-trash/cycle-{cycle_idx:03}", self.args.prefix);
            let trash_child = format!("{trash_root}/payload.txt");
            let payload = deterministic_payload(128 * 1024, trash_child.as_bytes());
            let trash_sha = sha256_bytes(&payload);
            self.posix_post("/n1/posix/mkdir", json!({"path": trash_root}))
                .await?;
            self.upload_bytes(&trash_child, payload, "text/plain")
                .await?;
            expected.insert(trash_child.clone());
            let trash = self
                .posix_post("/n1/posix/trash", json!({"path": trash_root}))
                .await?;
            expected.remove(&trash_child);
            let trash_entry_id = trash
                .get("trash_entry_id")
                .and_then(Value::as_str)
                .ok_or("chaos trash did not return trash_entry_id")?
                .to_string();
            self.posix_post(
                "/n1/posix/recover",
                json!({"path": trash_root, "trash_entry_id": trash_entry_id}),
            )
            .await?;
            self.assert_download_sha256(&trash_child, &trash_sha)
                .await?;
            expected.insert(trash_child.clone());

            if cycle_idx == 0 {
                let repair = self
                    .simulate_disk_format_and_repair(expected, &large_key, large_sha)
                    .await?;
                rss_samples.push(self.rss_sample("chaos_after_disk_wipe_repair"));
                cycles.push(json!({
                    "cycle": cycle_idx,
                    "storm": storm,
                    "large_multipart": upload,
                    "recover_on_read_version_id": version_id,
                    "disk_wipe_repair": repair,
                    "trash_recover_object": trash_child,
                    "elapsed_s": round3(cycle_start.elapsed().as_secs_f64()),
                }));
            } else {
                cycles.push(json!({
                    "cycle": cycle_idx,
                    "storm": storm,
                    "large_multipart": upload,
                    "recover_on_read_version_id": version_id,
                    "trash_recover_object": trash_child,
                    "elapsed_s": round3(cycle_start.elapsed().as_secs_f64()),
                }));
            }

            self.run_gc_and_wait().await?;
            self.assert_health_clean(&format!("chaos_cycle_{cycle_idx}"))
                .await?;
            cycle_idx += 1;
            let reached_min_cycles = cycle_idx >= min_cycles;
            let reached_deadline = deadline.is_none_or(|limit| Instant::now() >= limit);
            if reached_min_cycles && reached_deadline {
                break;
            }
        }
        Ok(json!({
            "enabled": true,
            "cycles": cycles.len(),
            "chaos_seconds_requested": self.args.chaos_seconds,
            "chaos_cycles_requested": self.args.chaos_cycles,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "cycle_reports": cycles,
        }))
    }

    async fn object_status(&self, object_key: &str, mode: &str) -> ProofResult<Value> {
        let response = self
            .client
            .get(format!(
                "{}/n1/objects/status/{}?mode={}",
                self.base_url,
                encode_path(object_key),
                mode
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "object status failed for {object_key}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json().await?)
    }

    async fn object_status_raw(
        &self,
        object_key: &str,
        mode: &str,
    ) -> ProofResult<(StatusCode, String)> {
        let response = self
            .client
            .get(format!(
                "{}/n1/objects/status/{}?mode={}",
                self.base_url,
                encode_path(object_key),
                mode
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Ok((status, body))
    }

    async fn object_repair_raw(
        &self,
        object_key: &str,
        mode: &str,
    ) -> ProofResult<(StatusCode, String)> {
        let response = self
            .client
            .post(format!(
                "{}/n1/objects/repair/{}?mode={}",
                self.base_url,
                encode_path(object_key),
                mode
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Ok((status, body))
    }

    async fn assert_object_status_clean(&self, object_key: &str, phase: &str) -> ProofResult<()> {
        let body = self.object_status(object_key, "strict").await?;
        let status = body
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if status != "healthy" {
            return Err(format!("{phase} object status is not healthy: {body}").into());
        }
        Ok(())
    }

    async fn assert_health_clean(&self, phase: &str) -> ProofResult<()> {
        let response = self
            .client
            .get(format!(
                "{}/n1/maintenance/health?deep_scan=true",
                self.base_url
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "{phase} health failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let body: Value = response.json().await?;
        let mut bad = Vec::new();
        collect_unhealthy_strings("", &body, &mut bad);
        if !bad.is_empty() {
            return Err(format!("{phase} health reported unhealthy state: {:?}", bad).into());
        }
        Ok(())
    }

    async fn run_gc_and_wait(&self) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/n1/maintenance/fragments/{}/gc/start?mode=scrub&fast_mode=true",
                self.base_url, self.fragment
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "GC start failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        self.wait_job("/n1/maintenance/gc/status", "GC").await
    }

    async fn run_repair_and_wait(&self) -> ProofResult<Value> {
        self.start_repair_job().await?;
        self.wait_job("/n1/maintenance/repair/status", "repair")
            .await
    }

    async fn start_repair_job(&self) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/n1/maintenance/fragments/{}/repair/start?mode=scrub&fast_mode=true&meta_concurrency=4&repair_concurrency=2&batch=128",
                self.base_url, self.fragment
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "repair start failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response
            .json()
            .await
            .unwrap_or_else(|_| json!({"accepted": true})))
    }

    async fn wait_repair_running(&self, timeout: Duration) -> ProofResult<Value> {
        let deadline = Instant::now() + timeout;
        let mut last = json!(null);
        while Instant::now() < deadline {
            let response = self
                .client
                .get(format!("{}/n1/maintenance/repair/status", self.base_url))
                .bearer_auth(&self.token)
                .send()
                .await?;
            if response.status() != StatusCode::OK {
                return Err(format!(
                    "repair running status failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
            last = response.json().await?;
            if last
                .get("is_running")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Ok(last);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!("repair did not enter running state; last={last}").into())
    }

    async fn run_redistribution_and_wait(&self) -> ProofResult<Value> {
        self.start_redistribution_job().await?;
        self.wait_job("/n1/maintenance/redistribution/status", "redistribution")
            .await
    }

    async fn start_redistribution_job(&self) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/n1/maintenance/fragments/{}/redistribution/start?mode=scrub&fast_mode=true&durability=batched&object_concurrency=8&status_batch=512",
                self.base_url, self.fragment
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "redistribution start failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response
            .json()
            .await
            .unwrap_or_else(|_| json!({"accepted": true})))
    }

    async fn redistribution_status(&self) -> ProofResult<Value> {
        let response = self
            .client
            .get(format!(
                "{}/n1/maintenance/redistribution/status",
                self.base_url
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "redistribution status failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json().await?)
    }

    async fn wait_job(&self, path: &str, label: &str) -> ProofResult<Value> {
        let deadline = Instant::now() + Duration::from_secs(self.args.job_timeout_seconds);
        let mut last = json!(null);
        while Instant::now() < deadline {
            let response = self
                .client
                .get(format!("{}{}", self.base_url, path))
                .bearer_auth(&self.token)
                .send()
                .await?;
            if response.status() != StatusCode::OK {
                return Err(format!(
                    "{label} status failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
            last = response.json().await?;
            if !last
                .get("is_running")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                let phase = last
                    .get("phase")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let last_error = last.get("last_error").filter(|value| !value.is_null());
                if phase == "failed" || last_error.is_some() {
                    return Err(format!("{label} failed: {last}").into());
                }
                return Ok(last);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(format!("{label} timed out; last={last}").into())
    }

    async fn list_version_ids(&self, object_key: &str) -> ProofResult<Vec<String>> {
        let response = self
            .client
            .get(format!(
                "{}/n1/objects/versions/{}",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "versions failed for {object_key}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let versions: Value = response.json().await?;
        let ids = versions
            .as_array()
            .ok_or_else(|| {
                format!("versions response was not an array for {object_key}: {versions}")
            })?
            .iter()
            .filter_map(|item| item.get("version_id").and_then(Value::as_str))
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Err(format!("no versions for {object_key}: {versions}").into());
        }
        Ok(ids)
    }

    async fn latest_version_id(&self, object_key: &str) -> ProofResult<String> {
        self.list_version_ids(object_key)
            .await?
            .into_iter()
            .max()
            .ok_or_else(|| format!("no versions for {object_key}").into())
    }

    fn remove_one_shard(&self, version_id: &str) -> ProofResult<PathBuf> {
        let version_dir = self.blob_roots[0]
            .join(&self.fragment)
            .join(".blobs")
            .join(version_id);
        let mut shard_files = fs::read_dir(&version_dir)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect::<Vec<_>>();
        shard_files.sort();
        let removed = shard_files
            .into_iter()
            .next()
            .ok_or_else(|| format!("no shard files under {}", version_dir.display()))?;
        fs::remove_file(&removed)?;
        Ok(removed)
    }

    async fn wait_for_path(
        &self,
        path: &Path,
        should_exist: bool,
        timeout: Duration,
    ) -> ProofResult<()> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if path.exists() == should_exist {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Err(format!(
            "path {} did not reach existence={} within {:?}",
            path.display(),
            should_exist,
            timeout
        )
        .into())
    }

    fn blob_version_exists(&self, version_id: &str) -> bool {
        self.blob_roots.iter().any(|root| {
            root.join(&self.fragment)
                .join(".blobs")
                .join(version_id)
                .exists()
        })
    }

    fn inspect_index(&self, expected_live_objects: usize) -> ProofResult<Value> {
        let mut inspections = Vec::new();
        for root in &self.index_roots {
            let fragment_root = root.join(&self.fragment);
            if fragment_root.join("indexdb").exists() {
                return Err(format!(
                    "N1 fragment must not contain indexdb: {}",
                    fragment_root.display()
                )
                .into());
            }
            let object_root = fragment_root.join(".objects");
            let live_ndx = count_files_with_suffix(&object_root, ".ndx")?;
            let object_record_dirs = count_object_record_dirs(&object_root)?;
            let namespace_symlinks = count_symlinks_excluding(&fragment_root, ".trash")?;
            let blob_root = self
                .blob_roots
                .get(
                    self.index_roots
                        .iter()
                        .position(|value| value == root)
                        .unwrap_or(0),
                )
                .unwrap_or(&self.blob_roots[0]);
            let blob_version_dirs =
                count_dirs(blob_root.join(&self.fragment).join(".blobs").as_path())?;
            if object_record_dirs != expected_live_objects {
                return Err(format!(
                    "index inspection failed under {}: object_record_dirs={} expected={}",
                    fragment_root.display(),
                    object_record_dirs,
                    expected_live_objects
                )
                .into());
            }
            if namespace_symlinks != expected_live_objects {
                return Err(format!(
                    "index inspection failed under {}: namespace_symlinks={} expected={}",
                    fragment_root.display(),
                    namespace_symlinks,
                    expected_live_objects
                )
                .into());
            }
            if live_ndx < expected_live_objects {
                return Err(format!(
                    "index inspection failed under {}: live_ndx={} expected_at_least={}",
                    fragment_root.display(),
                    live_ndx,
                    expected_live_objects
                )
                .into());
            }
            if blob_version_dirs < expected_live_objects {
                return Err(format!(
                    "blob inspection failed under {}: blob_version_dirs={} expected_at_least={}",
                    blob_root.display(),
                    blob_version_dirs,
                    expected_live_objects
                )
                .into());
            }
            inspections.push(json!({
                "index_root": root,
                "blob_root": blob_root,
                "object_record_dirs": object_record_dirs,
                "live_ndx": live_ndx,
                "namespace_symlinks": namespace_symlinks,
                "blob_version_dirs": blob_version_dirs,
            }));
        }
        Ok(json!(inspections))
    }

    async fn sha256_file(&self, path: &Path) -> ProofResult<String> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let read = file.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(format!("{:x}", hasher.finalize()))
    }

    fn object_key(&self) -> String {
        let name = self
            .args
            .source_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("large-object.bin");
        format!("{}{}", self.args.prefix, name)
    }

    fn rss_sample(&self, label: &str) -> Value {
        let pid = self.server.as_ref().map(Child::id);
        let rss_kib = pid.and_then(|pid| read_rss_kib(pid).ok().flatten());
        json!({
            "label": label,
            "pid": pid,
            "rss_kib": rss_kib,
        })
    }

    fn scan_logs(&self) -> ProofResult<Value> {
        let mut files = Vec::new();
        let mut bad_lines = Vec::new();
        for entry in fs::read_dir(&self.args.out_dir)? {
            let entry = entry?;
            let path = entry.path();
            let is_log = path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("server-") && name.ends_with(".log"));
            if !is_log {
                continue;
            }
            let text = fs::read_to_string(&path).unwrap_or_default();
            let mut line_count = 0usize;
            for (idx, line) in text.lines().enumerate() {
                line_count += 1;
                let lower = line.to_ascii_lowercase();
                let bad = lower.contains(" warn ")
                    || lower.contains(" error ")
                    || lower.contains("panic")
                    || lower.contains("critical")
                    || lower.contains("degraded")
                    || lower.contains("failed");
                if bad {
                    bad_lines.push(json!({
                        "file": path,
                        "line": idx + 1,
                        "text": line,
                    }));
                }
            }
            files.push(json!({
                "file": path,
                "lines": line_count,
            }));
        }
        if !bad_lines.is_empty() {
            return Err(format!("server logs contain warnings/errors: {bad_lines:?}").into());
        }
        Ok(json!({
            "status": "PASS",
            "files": files,
            "bad_lines": bad_lines,
        }))
    }

    fn count_log_matches(&self, label: &str, needle: &str) -> ProofResult<usize> {
        let path = self.args.out_dir.join(format!("server-{label}.log"));
        let text = fs::read_to_string(&path).unwrap_or_default();
        Ok(text.lines().filter(|line| line.contains(needle)).count())
    }

    async fn wait_for_log_count(
        &self,
        label: &str,
        needle: &str,
        min_count: usize,
        timeout: Duration,
    ) -> ProofResult<()> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let count = self.count_log_matches(label, needle)?;
            if count >= min_count {
                return Ok(());
            }
            if let Some(child) = self.server.as_ref() {
                let pid = child.id();
                if !Path::new(&format!("/proc/{pid}")).exists() {
                    return Err(format!(
                        "server exited while waiting for {min_count} log matches of {needle:?} in {label}; observed {count}"
                    )
                    .into());
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let count = self.count_log_matches(label, needle)?;
        Err(format!(
            "timed out waiting for {min_count} log matches of {needle:?} in {label}; observed {count}"
        )
        .into())
    }

    fn write_summary(&self) -> ProofResult<()> {
        fs::write(
            self.args.out_dir.join("summary.json"),
            serde_json::to_vec_pretty(&self.summary)?,
        )?;
        Ok(())
    }

    fn cleanup_roots(&self) -> ProofResult<()> {
        for root in self.index_roots.iter().chain(self.blob_roots.iter()) {
            if let Some(parent) = root.parent() {
                let _ = fs::remove_dir_all(parent);
            }
        }
        Ok(())
    }
}
