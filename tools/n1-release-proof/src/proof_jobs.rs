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
            let removed = self.remove_recover_on_read_target(&version_id)?;
            self.assert_download_sha256(&large_key, large_sha).await?;
            self.wait_for_path(&removed, true, Duration::from_secs(60))
                .await?;
            self.assert_object_status_clean(&large_key, "chaos_recover_on_read")
                .await?;

            let trash_root = format!("{}chaos-trash/cycle-{cycle_idx:03}", self.args.prefix);
            let trash_child = format!("{trash_root}/payload.txt");
            let payload = deterministic_payload(128 * 1024, trash_child.as_bytes());
            let trash_sha = sha256_bytes(&payload);
            self.posix_post("/noa/v1/posix/mkdir", json!({"path": trash_root}))
                .await?;
            self.upload_bytes(&trash_child, payload, "text/plain")
                .await?;
            expected.insert(trash_child.clone());
            self.posix_post("/noa/v1/posix/trash", json!({"path": trash_root}))
                .await?;
            expected.remove(&trash_child);
            self.posix_post("/noa/v1/posix/recover", json!({"path": trash_root}))
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
                "{}/noa/v1/objects/status/{}?mode={}",
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
                "{}/noa/v1/objects/status/{}?mode={}",
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
                "{}/noa/v1/objects/repair/{}?mode={}",
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
                "{}/noa/v1/maintenance/health?deep_scan=true",
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
                "{}/noa/v1/maintenance/fragments/{}/gc/start?mode=scrub&fast_mode=true",
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
        self.wait_job("/noa/v1/maintenance/gc/status", "GC").await
    }

    async fn run_repair_and_wait(&self) -> ProofResult<Value> {
        self.start_repair_job().await?;
        self.wait_job("/noa/v1/maintenance/repair/status", "repair")
            .await
    }

    async fn start_repair_job(&self) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/maintenance/fragments/{}/repair/start?mode=scrub&fast_mode=true&meta_concurrency=4&repair_concurrency=2&batch=128",
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
                .get(format!("{}/noa/v1/maintenance/repair/status", self.base_url))
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
        self.wait_job("/noa/v1/maintenance/redistribution/status", "redistribution")
            .await
    }

    async fn run_object_redistribution_and_wait(
        &self,
        object_key: &str,
    ) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/maintenance/objects/redistribution/start/{}?mode=scrub&fast_mode=true&durability=batched",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "object redistribution start failed for {object_key}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        self.wait_job("/noa/v1/maintenance/redistribution/status", "object redistribution")
            .await
    }

    async fn start_redistribution_job(&self) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/maintenance/fragments/{}/redistribution/start?mode=scrub&fast_mode=true&durability=batched&object_concurrency=8&status_batch=512",
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
                "{}/noa/v1/maintenance/redistribution/status",
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
                "{}/noa/v1/objects/versions/{}",
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

    fn resolve_stable_fragment_id(&self) -> ProofResult<String> {
        let mut resolved = None::<String>;
        for root in self.index_roots.iter().take(self.active_disk_count) {
            let stable = fragment_alias_stable_id(root, &self.fragment)?;
            if resolved.as_ref().is_some_and(|current| current != &stable) {
                return Err(format!(
                    "fragment alias resolves inconsistently: expected={resolved:?} root={} actual={stable}",
                    root.display()
                )
                .into());
            }
            resolved = Some(stable);
        }
        resolved.ok_or_else(|| "no active index root resolved the stable fragment identity".into())
    }

    fn object_version_dirs(&self, version_id: &str) -> ProofResult<Vec<PathBuf>> {
        let mut out = Vec::new();
        for root in &self.blob_roots {
            let objects =
                physical_fragment_root(root, &self.stable_fragment_id).join(".objects");
            for object_dir in collect_object_dirs(&objects)? {
                let version_dir = object_dir.join(version_id);
                if version_dir.is_dir() {
                    out.push(version_dir);
                }
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    fn assert_object_version_placement(&self, version_id: &str) -> ProofResult<Value> {
        let mut placements = Vec::new();
        for root in self.blob_roots.iter().take(self.active_disk_count) {
            let objects =
                physical_fragment_root(root, &self.stable_fragment_id).join(".objects");
            let mut payload = None;
            let mut metadata = None;
            for object_dir in collect_object_dirs(&objects)? {
                let version_dir = object_dir.join(version_id);
                if !version_dir.is_dir() {
                    continue;
                }
                payload = Some(version_dir);
                metadata = fs::read_dir(&object_dir)?
                    .filter_map(Result::ok)
                    .map(|entry| entry.path())
                    .find(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| {
                                name.starts_with(version_id) && name.ends_with(".ndx")
                            })
                    });
                break;
            }
            if let Some(payload) = payload {
                let metadata = metadata.ok_or_else(|| {
                    format!(
                        "payload placement {} has no colocated .ndx for version {version_id}",
                        payload.display()
                    )
                })?;
                placements.push(json!({
                    "blob_root": root,
                    "payload": payload,
                    "metadata": metadata,
                }));
            }
        }

        let expected_roots = usize::from(self.data_shards + self.parity_shards)
            .min(self.active_disk_count)
            .max(1);
        if placements.len() != expected_roots {
            return Err(format!(
                "version {version_id} placement mismatch: expected {expected_roots} shard-bearing roots, found {}: {placements:?}",
                placements.len()
            )
            .into());
        }
        Ok(json!({
            "version_id": version_id,
            "expected_shard_bearing_roots": expected_roots,
            "placements": placements,
        }))
    }

    fn physical_fragment_roots_for_disk(&self, disk_index: usize) -> BTreeSet<PathBuf> {
        let mut roots = BTreeSet::new();
        if let Some(root) = self.index_roots.get(disk_index) {
            roots.insert(physical_fragment_root(root, &self.stable_fragment_id));
        }
        if let Some(root) = self.blob_roots.get(disk_index) {
            roots.insert(physical_fragment_root(root, &self.stable_fragment_id));
        }
        roots
    }

    fn remove_one_shard_matching<F>(&self, version_id: &str, keep: F) -> ProofResult<PathBuf>
    where
        F: Fn(&Path) -> bool,
    {
        let mut shard_files = Vec::new();
        for version_dir in self.object_version_dirs(version_id)? {
            shard_files.extend(
                fs::read_dir(&version_dir)?
                    .filter_map(Result::ok)
                    .map(|entry| entry.path())
                    .filter(|path| path.is_file() && is_erasure_shard_path(path) && keep(path)),
            );
        }
        shard_files.sort();
        let removed = shard_files
            .into_iter()
            .next()
            .ok_or_else(|| format!("no matching shard files for version {version_id}"))?;
        fs::remove_file(&removed)?;
        Ok(removed)
    }

    fn remove_one_shard(&self, version_id: &str) -> ProofResult<PathBuf> {
        self.remove_one_shard_matching(version_id, |_| true)
    }

    fn remove_one_data_shard(&self, version_id: &str) -> ProofResult<PathBuf> {
        self.remove_one_shard_matching(version_id, is_data_shard_path)
    }

    fn remove_recover_on_read_target(&self, version_id: &str) -> ProofResult<PathBuf> {
        // A healthy read consumes data shards only. Removing parity here would
        // exercise explicit object/fragment repair, not recover-on-read.
        self.remove_one_data_shard(version_id)
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
        self.object_version_dirs(version_id)
            .is_ok_and(|dirs| !dirs.is_empty())
            || self.blob_roots.iter().any(|root| {
                let objects = physical_fragment_root(root, &self.stable_fragment_id).join(".objects");
                count_files_with_suffix(&objects, ".ndx").is_ok_and(|count| {
                    count > 0
                        && collect_object_dirs(&objects).is_ok_and(|dirs| {
                            dirs.into_iter().any(|dir| {
                                fs::read_dir(dir).is_ok_and(|entries| {
                                    entries.filter_map(Result::ok).any(|entry| {
                                        entry
                                            .file_name()
                                            .to_str()
                                            .is_some_and(|name| {
                                                name.starts_with(version_id)
                                                    && name.ends_with(".ndx")
                                            })
                                    })
                                })
                            })
                        })
                })
            })
    }

    fn inspect_index(&self, expected_live_objects: usize) -> ProofResult<Value> {
        let mut index_inspections = Vec::new();
        let mut blob_inspections = Vec::new();
        let mut unique_objects = BTreeSet::new();
        for root in &self.index_roots {
            let fragment_root = physical_fragment_root(root, &self.stable_fragment_id);
            if !fragment_root.join(".fragment.ndx").is_file() {
                return Err(format!(
                    "index root is missing .fragment.ndx: {}",
                    fragment_root.display()
                )
                .into());
            }
            if fragment_root.join("indexdb").exists() {
                return Err(format!(
                    "N1 fragment must not contain indexdb: {}",
                    fragment_root.display()
                )
                .into());
            }
            let namespace_symlinks = count_live_namespace_links(&fragment_root)?;
            if namespace_symlinks != expected_live_objects {
                return Err(format!(
                    "index inspection failed under {}: namespace_symlinks={} expected={}",
                    fragment_root.display(),
                    namespace_symlinks,
                    expected_live_objects
                )
                .into());
            }
            let root_is_blob = self.blob_roots.iter().any(|blob| blob == root);
            if !root_is_blob && fragment_root.join(".objects").exists() {
                return Err(format!(
                    "index-only root contains immutable object plane: {}",
                    fragment_root.join(".objects").display()
                )
                .into());
            }
            index_inspections.push(json!({
                "index_root": root,
                "physical_fragment_root": fragment_root,
                "namespace_symlinks": namespace_symlinks,
                "colocated_blob_role": root_is_blob,
            }));
        }
        for root in &self.blob_roots {
            let fragment_root = physical_fragment_root(root, &self.stable_fragment_id);
            let objects_root = fragment_root.join(".objects");
            let object_dirs = collect_object_dirs(&objects_root)?;
            let mut metadata_versions = 0usize;
            let mut payload_versions = 0usize;
            for object_dir in &object_dirs {
                let name = object_dir
                    .file_name()
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| format!("invalid object directory {}", object_dir.display()))?;
                unique_objects.insert(name.to_string());
                let (metadata, payload) = verify_colocated_object_dir(object_dir)?;
                metadata_versions += metadata;
                payload_versions += payload;
            }
            let root_is_index = self.index_roots.iter().any(|index| index == root);
            if !root_is_index
                && (fragment_root.join(".fragment.ndx").exists()
                    || fragment_root.join(".uploads").exists()
                    || fragment_root.join(".trash").exists()
                    || root.join(".fragment").exists())
            {
                return Err(format!(
                    "blob-only root contains namespace/control state: {}",
                    root.display()
                )
                .into());
            }
            blob_inspections.push(json!({
                "blob_root": root,
                "physical_fragment_root": fragment_root,
                "object_record_dirs": object_dirs.len(),
                "metadata_versions": metadata_versions,
                "payload_versions": payload_versions,
                "colocated_index_role": root_is_index,
            }));
        }
        if unique_objects.len() != expected_live_objects {
            return Err(format!(
                "blob inspection found {} unique objects; expected {}",
                unique_objects.len(),
                expected_live_objects
            )
            .into());
        }
        Ok(json!({
            "stable_fragment_id": self.stable_fragment_id,
            "colocated_roots": self.args.colocated_roots,
            "unique_objects": unique_objects.len(),
            "index_roots": index_inspections,
            "blob_roots": blob_inspections,
        }))
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
            let report_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("server.log")
                .to_string();
            let mut line_count = 0usize;
            for (idx, line) in text.lines().enumerate() {
                line_count += 1;
                if log_line_is_failure(line) {
                    bad_lines.push(json!({
                        "file": report_name.clone(),
                        "line": idx + 1,
                        "text": line,
                    }));
                }
            }
            files.push(json!({
                "file": report_name,
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
        let mut parents = BTreeSet::new();
        for root in self.index_roots.iter().chain(self.blob_roots.iter()) {
            if let Some(parent) = root.parent() {
                parents.insert(parent.to_path_buf());
            }
        }
        for parent in parents {
            let _ = fs::remove_dir_all(parent);
        }
        Ok(())
    }
}

fn strip_ansi_control_sequences(line: &str) -> String {
    let mut cleaned = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for control in chars.by_ref() {
                if ('@'..='~').contains(&control) {
                    break;
                }
            }
        } else {
            cleaned.push(ch);
        }
    }
    cleaned
}

fn log_line_is_failure(line: &str) -> bool {
    let cleaned = strip_ansi_control_sequences(line);
    let has_failure_level = cleaned
        .split_whitespace()
        .any(|token| matches!(token, "WARN" | "ERROR"));
    if has_failure_level {
        return true;
    }

    let lower = cleaned.to_ascii_lowercase();
    let zero_failed_counter = lower.contains("failed: 0") || lower.contains("failed=0");
    lower.contains("panic")
        || lower.contains("critical")
        || lower.contains("degraded")
        || (lower.contains("failed") && !zero_failed_counter)
}

fn is_erasure_shard_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| matches!(ext, "shard" | "parity"))
}

fn is_data_shard_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext == "shard")
}

#[cfg(test)]
mod proof_job_tests {
    use super::{is_data_shard_path, is_erasure_shard_path, log_line_is_failure};
    use std::path::Path;

    #[test]
    fn recover_on_read_fault_targets_data_and_never_parity() {
        let data = Path::new("part_0_shard_0.shard");
        let parity = Path::new("part_0_shard_1.parity");
        let metadata = Path::new("version.ndx");

        assert!(is_erasure_shard_path(data));
        assert!(is_erasure_shard_path(parity));
        assert!(!is_erasure_shard_path(metadata));
        assert!(is_data_shard_path(data));
        assert!(!is_data_shard_path(parity));
        assert!(!is_data_shard_path(metadata));
    }

    #[test]
    fn log_scan_accepts_zero_failed_counter_and_detects_real_levels() {
        let healthy = "\u{1b}[32m INFO\u{1b}[0m Repair Job Ended. Failed: 0";
        let warning = "\u{1b}[33m WARN\u{1b}[0m repair retry required";
        let failed = "\u{1b}[32m INFO\u{1b}[0m Repair Job Ended. Failed: 2";
        assert!(!log_line_is_failure(healthy));
        assert!(log_line_is_failure(warning));
        assert!(log_line_is_failure(failed));
    }
}
