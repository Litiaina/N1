// Operates only on prepare_dirs() fixtures. No formatting, mount changes,
// production process control or production configuration is permitted here.
impl Proof {
    fn relocation_config(&self, reversed: bool) -> ProofResult<()> {
        self.write_config()?;
        let path = self.args.out_dir.join("n1_config.ini");
        let mut config = fs::read_to_string(&path)?.replace("file_ttl_seconds=30", "file_ttl_seconds=86400");
        if reversed {
            let original = self.index_roots.iter().map(|p| p.display().to_string()).collect::<Vec<_>>().join(",");
            let reordered = self.index_roots.iter().rev().map(|p| p.display().to_string()).collect::<Vec<_>>().join(",");
            config = config.replace(&format!("roots={original}"), &format!("roots={reordered}"));
        }
        fs::write(path, config)?;
        Ok(())
    }

    async fn run_pool_relocation_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.relocation_config(false)?;
        let candidate_binary = self.args.binary.clone();
        if let Some(baseline) = &self.args.baseline_binary {
            self.args.binary = fs::canonicalize(baseline)?;
        }
        self.start_server("relocation-seed").await?;
        self.create_fragment().await?;
        self.auth().await?;
        let mut expected = BTreeSet::new();
        self.upload_bytes("empty.bin", Vec::new(), "application/octet-stream").await?;
        expected.insert("empty.bin".into());
        let lineage = self.rename_version_lineage_proof(&mut expected).await?;
        self.concurrent_object_storm(&mut expected, "relocation", 64).await?;
        let mut hashes = Vec::new();
        for key in &expected { hashes.push((key.clone(), self.download_sha256(key).await?)); }
        let trash = deterministic_payload(65536, b"relocation-trash");
        let trash_hash = sha256_bytes(&trash);
        self.upload_bytes("relocation-trash/file.bin", trash, "application/octet-stream").await?;
        self.posix_post("/noa/v1/posix/trash", json!({"path":"relocation-trash"})).await?;
        let total = self.args.source_file.metadata()?.len();
        let part_size = self.args.max_part_size.min(total.div_ceil(2)).max(1);
        let count = total.div_ceil(part_size);
        let response = self.client.post(format!("{}/noa/v1/upload/multipart/durable/init/relocation-durable.bin",self.base_url))
            .bearer_auth(&self.token).json(&json!({
                "content_type": "application/octet-stream",
                "total_size_bytes": total,
                "expected_parts": count,
                "part_size_bytes": part_size
            })).send().await?;
        if response.status() != StatusCode::CREATED { return Err(response.text().await?.into()); }
        let init: MultipartInit = response.json().await?;
        self.upload_durable_part_range(&init.version_id, 0, 1, part_size).await?;
        self.stop_server()?;
        let before = self.flat_immutable_digest(&self.index_roots)?;
        self.args.binary = candidate_binary;
        // Upgrade only topology records while the original mount paths exist.
        self.start_server("relocation-descriptor-upgrade").await?;
        self.auth().await?;
        self.stop_server()?;
        if before != self.flat_immutable_digest(&self.index_roots)? { return Err("descriptor upgrade changed committed NDX/payload".into()); }
        let original_roots = self.index_roots.clone();
        let mut measures = Vec::new();
        let copied = if let Some(spare) = &self.args.spare_root {
            let target = spare.join(format!("verified-clone-{}", self.stamp));
            fs::create_dir(&target)?;
            self.pool_cli("clone", &["--copy-root", target.to_str().ok_or("invalid copy path")?], true, &mut measures)?;
            Some(target)
        } else { None };
        for (slot, root) in self.index_roots.iter_mut().enumerate() {
            let moved = root.with_file_name("relocated-store");
            fs::rename(&*root, &moved)?;
            *root = if slot == self.args.pool_member { copied.clone().unwrap_or(moved) } else { moved };
        }
        self.blob_roots = self.index_roots.clone();
        if original_roots.iter().any(|root| root.exists()) { return Err("original locations remain reachable".into()); }
        self.relocation_config(true)?;
        self.pool_cli("verify", &[], true, &mut measures)?;
        self.start_server("relocation-new-paths-reversed-discovery").await?;
        self.auth().await?;
        for (key, hash) in &hashes { self.assert_download_sha256(key, hash).await?; }
        self.assert_expected_listing(&expected).await?;
        self.stop_server()?;
        if before != self.flat_immutable_digest(&self.index_roots)? { return Err("relocation/readback rewrote committed NDX/payload".into()); }
        if self.parity_shards > 0 {
            let root = self.index_roots[0].clone();
            let hidden = root.with_file_name("temporarily-offline-relocated-member");
            fs::rename(&root, &hidden)?;
            self.start_server("relocation-degraded-cold-start").await?;
            self.auth().await?;
            for (key, hash) in &hashes { self.assert_download_sha256(key, hash).await?; }
            self.assert_expected_listing(&expected).await?;
            self.stop_server()?;
            if root.exists() { return Err("cold start recreated missing member directory".into()); }
            fs::rename(hidden, root)?;
        }
        // A descriptor cannot override accepted membership or activation fences.
        let marker = self.index_roots[0].join(".n1-pool.json");
        let descriptor = fs::read(&marker)?;
        let mut invalid: Value = serde_json::from_slice(&descriptor)?;
        invalid["member_id"] = json!("unaccepted-member");
        fs::write(&marker, serde_json::to_vec(&invalid)?)?;
        if self.start_server("relocation-wrong-identity").await.is_ok() { return Err("unaccepted member admitted".into()); }
        self.stop_server()?;
        fs::write(&marker, &descriptor)?;
        let fence = self.index_roots[0].join(".n1-restore-pending");
        fs::write(&fence, b"injected incomplete copy")?;
        if self.start_server("relocation-incomplete-copy").await.is_ok() { return Err("unfinished member admitted".into()); }
        self.stop_server()?;
        fs::remove_file(&fence)?; // Remove only this proof's deliberately injected marker.
        self.start_server("relocation-operational").await?;
        self.auth().await?;
        self.upload_durable_part_range(&init.version_id, 1, count, part_size).await?;
        let response = self.client.post(format!("{}/noa/v1/upload/multipart/durable/{}/{}/finalize?object_key=relocation-durable.bin",self.base_url,init.version_id,init.session_id))
            .bearer_auth(&self.token).json(&json!({
                "content_type": "application/octet-stream",
                "total_size_bytes": total,
                "expected_parts": count,
                "part_size_bytes": part_size
            })).send().await?;
        if !response.status().is_success() { return Err(response.text().await?.into()); }
        self.assert_download_sha256("relocation-durable.bin", &self.sha256_file(&self.args.source_file).await?).await?;
        expected.insert("relocation-durable.bin".into());
        self.posix_post("/noa/v1/posix/recover", json!({"path":"relocation-trash"})).await?;
        self.assert_download_sha256("relocation-trash/file.bin", &trash_hash).await?;
        expected.insert("relocation-trash/file.bin".into());
        let batch = self.concurrent_standard_batch_upload_proof(&mut expected).await?;
        let direct = self.flat_direct_upload_regression().await?;
        // These direct-path fixtures are intentionally separate from exact LIST.
        self.stop_server()?;
        self.pool_cli("verify", &[], true, &mut measures)?;
        let before_replacement = self.flat_immutable_digest(&self.index_roots)?;
        self.relocation_config(false)?;
        let slot = self.args.pool_member;
        if slot >= self.index_roots.len() { return Err("replacement slot outside pool".into()); }
        let previous = self.index_roots[slot].clone();
        let replacement = previous.with_file_name(format!("replacement-at-new-path-{}", self.stamp));
        fs::create_dir(&replacement)?;
        self.index_roots[slot] = replacement.clone();
        self.blob_roots = self.index_roots.clone();
        self.relocation_config(false)?;
        let mut interrupted_files = None;
        if self.parity_shards == 0 {
            self.pool_cli("replace", &["--authorize"], false, &mut measures)?;
            self.index_roots[slot] = previous;
            self.blob_roots = self.index_roots.clone();
            self.relocation_config(false)?;
        } else {
            interrupted_files = Some(self.interrupted_pool_cli("replace", &["--authorize"], &replacement.join(".n1-restore-pending"))?);
            self.pool_cli("verify", &[], false, &mut measures)?;
            if self.start_server("relocation-incomplete-replacement").await.is_ok() { return Err("unfinished replacement admitted".into()); }
            self.stop_server()?;
            self.pool_cli("replace", &["--authorize", "--resume"], true, &mut measures)?;
            self.pool_cli("verify", &[], true, &mut measures)?;
            if before_replacement != self.flat_immutable_digest(&self.index_roots)? { return Err("replacement changed NDX/reconstructed payload bytes".into()); }
        }
        let config_path = self.args.out_dir.join("n1_config.ini");
        let config = fs::read_to_string(&config_path)?
            .replace("enabled=false\ntopology_check_interval_seconds=1", "enabled=true\ntopology_check_interval_seconds=1")
            .replace("initial_scan_delay_seconds=60", "initial_scan_delay_seconds=1")
            .replace("availability_scan_interval_seconds=3600", "availability_scan_interval_seconds=2");
        fs::write(&config_path, config)?;
        self.start_server("relocation-rebuilt").await?;
        self.auth().await?;
        for (key, hash) in &hashes { self.assert_download_sha256(key, hash).await?; }
        self.assert_download_sha256("relocation-durable.bin", &self.sha256_file(&self.args.source_file).await?).await?;
        if self.parity_shards > 0 {
            let removed = self.remove_one_data_shard(&init.version_id)?;
            self.assert_download_sha256("relocation-durable.bin", &self.sha256_file(&self.args.source_file).await?).await?;
            self.wait_for_path(&removed, true, Duration::from_secs(60)).await?;
            self.assert_object_status_clean("relocation-durable.bin", "relocated_recover_on_read").await?;
            let removed = self.remove_one_shard_matching(&init.version_id, |path| path.extension().is_some_and(|ext| ext == "parity"))?;
            self.wait_for_path(&removed, true, Duration::from_secs(90)).await?;
            self.assert_object_status_clean("relocation-durable.bin", "relocated_automatic_healing").await?;
        }
        self.upload_bytes("post-relocation.bin", b"logical member upload".to_vec(), "application/octet-stream").await?;
        self.posix_post("/noa/v1/posix/rename", json!({"source_path":"post-relocation.bin","destination_path":"post-relocation-renamed.bin"})).await?;
        self.assert_download_sha256("post-relocation-renamed.bin", &sha256_bytes(b"logical member upload")).await?;
        self.stop_server()?;
        self.pool_cli("verify", &[], true, &mut measures)?;
        self.summary = json!({
            "status": "PASS",
            "proof": "pool_relocation",
            "ec": [self.data_shards,self.parity_shards],
            "member": slot,
            "legacy_seed": self.args.baseline_binary.is_some(),
            "cross_device_clone": copied.is_some(),
            "original_paths_unavailable": true,
            "committed_bytes_unchanged": true,
            "checks": ["descriptor_upgrade","reversed_discovery","unchanged_ndx_payload","listing_readback","wrong_identity_refusal","incomplete_copy_refusal","durable_continuation","trash_recovery","batch_and_native_upload","new_path_replacement_resume","recover_on_read","automatic_healing","post_relocation_rename"],
            "lineage": lineage,
            "batch": batch,
            "direct": direct,
            "measurements": measures,
            "interrupted_replacement_files": interrupted_files,
            "physical_power_cut": false,
            "production_data_touched": false
        });
        self.write_summary()?;
        Ok(())
    }
}
