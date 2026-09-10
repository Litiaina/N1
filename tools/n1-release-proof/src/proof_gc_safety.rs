// Physical-disk regression for conservative GC and concurrent foreground work.

impl Proof {
    fn gc_fixture_dir(&self, root: &Path, object_id: &str) -> PathBuf {
        let hash = sha256_bytes(object_id.as_bytes());
        root.join(format!("@f_{}", self.stable_fragment_id))
            .join(".objects")
            .join(&hash[..2])
            .join(&hash[2..4])
            .join(format!("@o_{object_id}"))
    }

    async fn gc_safety_run(&self) -> ProofResult<Value> {
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/maintenance/fragments/{}/gc/start?mode=availability&fast_mode=false",
                self.base_url, self.fragment,
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "GC admission failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let status: Value = self
                .client
                .get(format!("{}/noa/v1/maintenance/gc/status", self.base_url))
                .bearer_auth(&self.token)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            if status.get("is_running").and_then(Value::as_bool) == Some(false) {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(format!("GC did not finish: {status}").into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn run_gc_safety_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("gc-safety").await?;
        self.create_fragment().await?;
        self.auth().await?;
        let (paused_key, paused_hash, paused_upload) = self.paused_upload_gc_proof(false).await?;
        let (ttl_key, ttl_hash, ttl_upload) = self.paused_upload_gc_proof(true).await?;
        let baseline_key = format!("{}gc-safety/live.bin", self.args.prefix);
        let payload = deterministic_payload(128 * 1024, b"gc-live-integrity");
        let digest = sha256_bytes(&payload);
        let version = self
            .upload_bytes(&baseline_key, payload, "application/octet-stream")
            .await?;
        let now = SystemTime::now();
        let old = now - Duration::from_secs(3600);
        let mut protected = Vec::new();
        let mut expired = Vec::new();
        // Future child mtimes model clock rollback without changing host time.
        // Parent mtimes remain old: editing a child never refreshes its parent.
        for root in &self.blob_roots {
            let young_parent = self.gc_fixture_dir(root, "gc-young-child");
            let child = young_parent.join(&version);
            fs::create_dir_all(&child)?;
            let file = child.join("part_0_shard_0.shard");
            fs::write(&file, b"young upload must survive")?;
            fs::File::open(&child)?
                .set_times(fs::FileTimes::new().set_modified(now + Duration::from_secs(3600)))?;
            fs::File::open(&young_parent)?.set_times(fs::FileTimes::new().set_modified(old))?;
            protected.push(file);

            let unknown = self.gc_fixture_dir(root, "gc-unknown-file");
            fs::create_dir_all(&unknown)?;
            let file = unknown.join("operator-note");
            fs::write(&file, b"unknown content must survive")?;
            fs::File::open(&unknown)?.set_times(fs::FileTimes::new().set_modified(old))?;
            protected.push(file);

            let orphan = self
                .gc_fixture_dir(root, "gc-expired-orphan")
                .join(&version);
            fs::create_dir_all(&orphan)?;
            fs::write(orphan.join("part_0_shard_0.shard"), b"abandoned upload")?;
            fs::File::open(&orphan)?.set_times(fs::FileTimes::new().set_modified(old))?;
            expired.push(orphan);
        }
        let initial_gc = self.gc_safety_run().await?;
        if initial_gc.get("total_errors").and_then(Value::as_u64) != Some(0) {
            return Err(format!("clean GC reported errors: {initial_gc}").into());
        }
        for path in &protected {
            if !path.is_file() {
                return Err(
                    "GC removed a protected child or unknown file through its parent".into(),
                );
            }
        }
        for path in &expired {
            if path.exists() {
                return Err("GC failed to reclaim a valid expired orphan version".into());
            }
        }
        self.assert_download_sha256(&baseline_key, &digest).await?;

        let fragment_cleanup: Value = self
            .client
            .post(format!(
                "{}/noa/v1/maintenance/fragment/cleanup",
                self.base_url
            ))
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        for path in &protected {
            if !path.is_file() {
                return Err("fragment cleanup removed a protected child or unknown file".into());
            }
        }

        // Every task runs against this server's localhost, with real EC writes.
        // Immutable export sources are separate from keys undergoing mutations.
        let export_prefix = format!("{}gc-export", self.args.prefix);
        let export_expected = self.upload_export_fixture(&export_prefix).await?;
        let mut standard_keys = BTreeSet::new();
        let mut mixed_keys = BTreeSet::new();
        let standard = self.parallel_multipart_user_proof(&mut standard_keys);
        let mixed = self.same_fragment_mixed_user_proof(&mut mixed_keys);
        let proof = &*self;
        let durable = async {
            let results = join_all((0..self.args.mixed_users).map(|user| async move {
                let key = format!("{}gc-durable/user-{user}.bin", proof.args.prefix);
                let data = deterministic_payload(8 * 1024 * 1024, key.as_bytes());
                let hash = sha256_bytes(&data);
                let started = Instant::now();
                proof
                    .upload_complete_multipart_payload(&key, &data, 2 * 1024 * 1024)
                    .await?;
                proof.assert_download_sha256(&key, &hash).await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    json!({"key":key,"elapsed_seconds":round3(started.elapsed().as_secs_f64())}),
                )
            }))
            .await;
            results.into_iter().collect::<ProofResult<Vec<_>>>()
        };
        let background = async {
            // Exercise actual orphan scanning while uploads are active, before
            // an export's fragment-protection snapshot can defer collection.
            let unprotected_gc = self.gc_safety_run().await?;
            if unprotected_gc.get("total_errors").and_then(Value::as_u64) != Some(0) {
                return Err(format!("foreground-overlap GC failed: {unprotected_gc}").into());
            }
            let id = self
                .create_export_job(&export_prefix, "zip", "gc-mixed.zip")
                .await?;
            let mut gc_runs = Vec::new();
            for _ in 0..3 {
                let status = self.gc_safety_run().await?;
                if status.get("total_errors").and_then(Value::as_u64) != Some(0) {
                    return Err(format!("mixed GC reported errors: {status}").into());
                }
                gc_runs.push(status);
            }
            self.wait_for_export_phase(&id, "ready", Duration::from_secs(300))
                .await?;
            let archive = self.download_export_full(&id).await?;
            self.validate_zip_export(&archive.bytes, &export_expected)
                .await?;
            self.cancel_export_job(&id).await?;
            self.wait_for_export_cancellation_cleanup(&id, Duration::from_secs(30))
                .await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                json!({"foreground_overlap_gc":unprotected_gc,"gc_runs":gc_runs,"zip_integrity":"PASS"}),
            )
        };
        let (standard, mixed, durable, background) =
            tokio::time::timeout(Duration::from_secs(300), async {
                tokio::try_join!(standard, mixed, durable, background)
            })
            .await
            .map_err(|_| "mixed standard/durable/export/GC workload timed out")??;
        self.assert_expected_visible(&standard_keys).await?;
        self.assert_expected_visible(&mixed_keys).await?;
        self.assert_download_sha256(&paused_key, &paused_hash)
            .await?;
        self.assert_download_sha256(&ttl_key, &ttl_hash).await?;

        // A corrupt durable manifest must prevent orphan reclamation, report
        // incomplete cleanup, and recover cleanly once the fault is removed.
        let mut bad_manifests = Vec::new();
        for root in &self.index_roots {
            let dir = root
                .join(format!("@f_{}", self.stable_fragment_id))
                .join(".uploads")
                .join(&version);
            fs::create_dir_all(&dir)?;
            fs::write(dir.join("manifest.ndx"), b"invalid session metadata")?;
            bad_manifests.push(dir);
        }
        let mut deferred = Vec::new();
        for root in &self.blob_roots {
            let dir = self
                .gc_fixture_dir(root, "gc-unreadable-session")
                .join(&version);
            fs::create_dir_all(&dir)?;
            fs::write(
                dir.join("part_0_shard_0.shard"),
                b"defer until protection is known",
            )?;
            fs::File::open(&dir)?.set_times(fs::FileTimes::new().set_modified(old))?;
            deferred.push(dir);
        }
        let fault = self.gc_safety_run().await?;
        if fault
            .get("total_errors")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            == 0
            || fault.get("phase").and_then(Value::as_str) != Some("Completed with errors")
            || deferred.iter().any(|dir| !dir.exists())
        {
            return Err(format!(
                "GC failed to preserve/report uncertain session protection: {fault}"
            )
            .into());
        }
        for dir in bad_manifests {
            fs::remove_dir_all(dir)?;
        }
        let retry = self.gc_safety_run().await?;
        if retry.get("total_errors").and_then(Value::as_u64) != Some(0)
            || retry
                .get("last_error")
                .is_some_and(|value| !value.is_null())
            || deferred.iter().any(|dir| dir.exists())
        {
            return Err(format!(
                "GC did not recover after removing injected session corruption: {retry}"
            )
            .into());
        }
        self.assert_download_sha256(&baseline_key, &digest).await?;
        let rss = self.rss_sample("gc_safety_complete");
        self.stop_server()?;
        self.start_server("gc-safety-after-restart").await?;
        self.auth().await?;
        self.assert_download_sha256(&baseline_key, &digest).await?;
        self.assert_expected_visible(&standard_keys).await?;
        self.assert_expected_visible(&mixed_keys).await?;
        self.stop_server()?;
        let expected_fault = format!(
            "GC preserving fragment {}: cannot establish durable upload protection:",
            self.stable_fragment_id
        );
        let logs = self.scan_logs_with_expected(&[&expected_fault])?;
        self.summary = json!({
            "status":"PASS", "proof":"gc_safety_and_mixed_workload",
            "topology":{"data_shards":self.data_shards,"parity_shards":self.parity_shards,"roots":self.active_disk_count},
            "protected_files":protected.len(), "expired_orphans":expired.len(),
            "initial_gc":initial_gc, "fragment_cleanup":fragment_cleanup,
            "paused_one_shot":paused_upload,
            "paused_upload_during_trash_expiry":ttl_upload,
            "standard":standard,"mixed":mixed,"durable":durable,
            "background":background, "injected_session_corruption":fault,"recovered_gc":retry,
            "restart_visibility":"PASS","rss":rss,"log_scan":logs,
        });
        self.write_summary()?;
        Ok(())
    }
}
