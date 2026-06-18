impl Proof {
    async fn permission_boundary_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let object_key = format!("{}auth-boundary/readable.txt", self.args.prefix);
        let payload = b"n1 permission boundary\n".to_vec();
        let sha = sha256_bytes(&payload);
        self.upload_bytes(&object_key, payload, "text/plain")
            .await?;
        expected.insert(object_key.clone());

        let read_only = self
            .auth_fragment_permissions(
                &self.fragment,
                &self.args.fragment_key,
                true,
                false,
                false,
                false,
            )
            .await?;
        self.assert_list_contains_with_token(&read_only, &object_key)
            .await?;
        self.assert_download_sha256_with_token(&read_only, &object_key, &sha)
            .await?;

        let denied_upload = self
            .client
            .post(format!(
                "{}/n1/upload/one-shot/{}",
                self.base_url,
                encode_path(&format!(
                    "{}auth-boundary/should-deny.txt",
                    self.args.prefix
                ))
            ))
            .bearer_auth(&read_only)
            .header("content-type", "text/plain")
            .body("must not write")
            .send()
            .await?;
        if denied_upload.status() != StatusCode::FORBIDDEN {
            return Err(format!(
                "read-only token was allowed to upload or returned wrong status: {} {}",
                denied_upload.status(),
                denied_upload.text().await?
            )
            .into());
        }

        let denied_mkdir = self
            .client
            .post(format!("{}/n1/posix/mkdir", self.base_url))
            .bearer_auth(&read_only)
            .json(&json!({"path": format!("{}auth-boundary/should-deny-dir", self.args.prefix)}))
            .send()
            .await?;
        if denied_mkdir.status() != StatusCode::FORBIDDEN {
            return Err(format!(
                "read-only token was allowed to mkdir or returned wrong status: {} {}",
                denied_mkdir.status(),
                denied_mkdir.text().await?
            )
            .into());
        }

        Ok(json!({
            "read_only_list_download": "PASS",
            "write_denied_status": StatusCode::FORBIDDEN.as_u16(),
            "mkdir_denied_status": StatusCode::FORBIDDEN.as_u16(),
            "sample_object": object_key,
        }))
    }

    async fn fragment_usage_api_proof(&self) -> ProofResult<Value> {
        let usage = self
            .client
            .get(format!("{}/n1/capacity/fragment", self.base_url))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if usage.status() != StatusCode::OK {
            return Err(format!(
                "fragment usage failed: {} {}",
                usage.status(),
                usage.text().await?
            )
            .into());
        }
        let usage_body: Value = usage.json().await?;
        let used_bytes = usage_body
            .get("used_bytes")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("fragment usage missing used_bytes: {usage_body}"))?;
        if used_bytes == 0 {
            return Err(
                format!("fragment usage stayed at zero after large upload: {usage_body}").into(),
            );
        }

        Ok(json!({
            "usage": usage_body,
        }))
    }

    async fn rename_version_lineage_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let old_key = format!("{}rename-lineage/original-name.bin", self.args.prefix);
        let new_key = format!("{}rename-lineage/renamed-name.bin", self.args.prefix);
        let first_payload = deterministic_payload(256 * 1024, b"rename-lineage-v1");
        let second_payload = deterministic_payload(384 * 1024, b"rename-lineage-v2");
        let first_sha = sha256_bytes(&first_payload);
        let second_sha = sha256_bytes(&second_payload);

        let first_version = self
            .upload_bytes(&old_key, first_payload, "application/octet-stream")
            .await?;
        self.assert_download_sha256(&old_key, &first_sha).await?;

        self.posix_post(
            "/n1/posix/rename",
            json!({"source_path": &old_key, "destination_path": &new_key}),
        )
        .await?;
        self.assert_list_contains(&new_key).await?;
        self.assert_download_sha256(&new_key, &first_sha).await?;
        self.expect_download_status(&old_key, StatusCode::NOT_FOUND)
            .await?;

        let second_version = self
            .upload_bytes(&new_key, second_payload, "application/octet-stream")
            .await?;
        self.assert_download_sha256(&new_key, &second_sha).await?;

        let versions = self.list_version_ids(&new_key).await?;
        if !versions.contains(&first_version) || !versions.contains(&second_version) {
            return Err(format!(
                "rename lineage lost version history for {new_key}: first={first_version} second={second_version} versions={versions:?}"
            )
            .into());
        }
        if versions.len() < 2 {
            return Err(format!(
                "rename lineage expected at least two versions for {new_key}: {versions:?}"
            )
            .into());
        }

        expected.insert(new_key.clone());
        Ok(json!({
            "old_key": old_key,
            "new_key": new_key,
            "first_version": first_version,
            "second_version": second_version,
            "version_count": versions.len(),
        }))
    }

    async fn websocket_namespace_delta_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let token = encode_query_value(&self.token);
        let ws_url = format!("wss://127.0.0.1:{}/ws?token={}", self.args.port, token);
        let connector = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()?;
        let (mut ws, _) = connect_async_tls_with_config(
            ws_url,
            None,
            true,
            Some(Connector::NativeTls(connector)),
        )
        .await?;

        let hello = self.wait_ws_control(&mut ws, "HELLO").await?;
        ws.send(Message::Text("{\"op\":\"PING\"}".into())).await?;
        let pong = self.wait_ws_control(&mut ws, "PONG").await?;

        let base = format!("{}websocket-delta", self.args.prefix);
        let dir = format!("{base}/dir");
        let file = format!("{base}/file.txt");
        let renamed = format!("{base}/renamed.txt");
        let payload = b"n1 websocket namespace delta\n".to_vec();
        let sha = sha256_bytes(&payload);

        self.posix_post("/n1/posix/mkdir", json!({"path": &dir}))
            .await?;
        let mkdir_event = self
            .wait_ws_namespace_event(&mut ws, "mkdir", Some(&dir), None)
            .await?;

        self.upload_bytes(&file, payload, "text/plain").await?;
        let upsert_event = self
            .wait_ws_namespace_event(&mut ws, "upsert_file", Some(&file), None)
            .await?;

        self.posix_post(
            "/n1/posix/rename",
            json!({"source_path": &file, "destination_path": &renamed}),
        )
        .await?;
        let rename_event = self
            .wait_ws_namespace_event(&mut ws, "rename", Some(&renamed), Some(&file))
            .await?;

        self.assert_download_sha256(&renamed, &sha).await?;
        expected.insert(renamed.clone());

        self.posix_post("/n1/posix/trash", json!({"path": &dir}))
            .await?;
        let trash_event = self
            .wait_ws_namespace_event(&mut ws, "trash", Some(&dir), None)
            .await?;
        self.posix_post("/n1/posix/recover", json!({"path": &dir}))
            .await?;
        let recover_event = self
            .wait_ws_namespace_event(&mut ws, "recover", Some(&dir), None)
            .await?;

        Ok(json!({
            "hello": hello,
            "pong": pong,
            "mkdir": mkdir_event,
            "upsert": upsert_event,
            "rename": rename_event,
            "trash": trash_event,
            "recover": recover_event,
        }))
    }

    async fn multipart_restart_resume(&mut self, object_key: &str) -> ProofResult<Value> {
        let total_size = self.args.source_file.metadata()?.len();
        let part_size = self.args.max_part_size;
        let expected_parts = total_size.div_ceil(part_size);
        let params = json!({
            "content_type": "application/octet-stream",
            "total_size_bytes": total_size,
            "expected_parts": expected_parts,
            "part_size_bytes": part_size,
        });
        let start = Instant::now();
        let init_url = format!(
            "{}/n1/upload/multipart/init/{}",
            self.base_url,
            encode_path(object_key)
        );
        let response = self
            .client
            .post(init_url)
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "multipart init failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let init: MultipartInit = response.json().await?;
        let restart_after_parts = (expected_parts / 2).max(1);
        self.upload_part_range(&init.version_id, 0, restart_after_parts, part_size)
            .await?;
        self.stop_server()?;
        self.start_server("resume").await?;
        self.auth().await?;
        self.upload_part_range(
            &init.version_id,
            restart_after_parts,
            expected_parts,
            part_size,
        )
        .await?;
        let finalize_url = format!(
            "{}/n1/upload/multipart/{}/{}/finalize?object_key={}",
            self.base_url,
            init.version_id,
            init.session_id,
            encode_query_value(object_key)
        );
        let response = self
            .client
            .post(finalize_url)
            .bearer_auth(&self.token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "multipart finalize failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let elapsed = start.elapsed().as_secs_f64();
        Ok(json!({
            "mode": "multipart",
            "bytes": total_size,
            "parts": expected_parts,
            "part_size_bytes": part_size,
            "restart_after_parts": restart_after_parts,
            "elapsed_s": round3(elapsed),
            "mib_s": round3((total_size as f64 / 1024.0 / 1024.0) / elapsed.max(0.001)),
            "version_id": init.version_id,
            "session_id": init.session_id,
        }))
    }

    async fn multiple_large_multipart_proof(
        &mut self,
        expected: &mut BTreeSet<String>,
        expected_sha: &str,
    ) -> ProofResult<Value> {
        let file_name = self
            .args
            .source_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("large-object.bin")
            .to_string();
        let mut uploads = Vec::new();
        for idx in 1..self.args.large_copies {
            let object_key = format!(
                "{}large-multipart/copy-{idx:03}-{file_name}",
                self.args.prefix
            );
            let upload = self.multipart_restart_resume(&object_key).await?;
            expected.insert(object_key.clone());
            self.assert_list_contains(&object_key).await?;
            self.assert_download_sha256(&object_key, expected_sha)
                .await?;
            self.assert_object_status_clean(&object_key, "after_large_copy")
                .await?;
            uploads.push(json!({
                "object_key": object_key,
                "upload": upload,
            }));
        }
        Ok(json!({
            "requested_large_copies": self.args.large_copies,
            "additional_large_copies": uploads.len(),
            "uploads": uploads,
        }))
    }

    async fn concurrent_object_storm(
        &self,
        expected: &mut BTreeSet<String>,
        label: &str,
        count: usize,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let object_size = self.args.storm_object_size;
        let concurrency = self.args.storm_concurrency.max(1);
        let sample_stride = (count / 16).max(1);
        let keys = (0..count)
            .map(|idx| {
                format!(
                    "{}object-storm/{label}/tenant-{}/deep-{}/file-{idx:08}.bin",
                    self.args.prefix,
                    idx % 64,
                    idx % 257
                )
            })
            .collect::<Vec<_>>();
        let mut samples = Vec::new();
        let mut uploaded = 0usize;
        for chunk in keys.chunks(concurrency) {
            let uploads = chunk.iter().enumerate().map(|(local_idx, key)| {
                let idx = uploaded + local_idx;
                let key = key.clone();
                async move {
                    let payload = deterministic_payload(object_size, key.as_bytes());
                    let sha = sha256_bytes(&payload);
                    let version_id = self
                        .upload_bytes(&key, payload, "application/octet-stream")
                        .await?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((idx, key, sha, version_id))
                }
            });
            let results = join_all(uploads).await;
            for result in results {
                let (idx, key, sha, version_id) = result?;
                expected.insert(key.clone());
                if idx == 0 || idx + 1 == count || idx % sample_stride == 0 {
                    samples.push(json!({
                        "index": idx,
                        "object_key": key,
                        "sha256": sha,
                        "version_id": version_id,
                    }));
                }
            }
            uploaded += chunk.len();
        }
        for sample in &samples {
            let object_key = sample
                .get("object_key")
                .and_then(Value::as_str)
                .ok_or("storm sample missing object_key")?;
            let sha = sample
                .get("sha256")
                .and_then(Value::as_str)
                .ok_or("storm sample missing sha256")?;
            self.assert_list_contains(object_key).await?;
            self.assert_download_sha256(object_key, sha).await?;
            self.assert_object_status_clean(object_key, "after_object_storm")
                .await?;
        }
        let elapsed = start.elapsed().as_secs_f64();
        Ok(json!({
            "label": label,
            "objects": count,
            "object_size_bytes": object_size,
            "concurrency": concurrency,
            "elapsed_s": round3(elapsed),
            "objects_per_s": round3(count as f64 / elapsed.max(0.001)),
            "mib_s": round3(((count * object_size) as f64 / 1024.0 / 1024.0) / elapsed.max(0.001)),
            "verified_samples": samples,
        }))
    }

    async fn same_fragment_mixed_user_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let users = self.args.mixed_users;
        let files_per_user = self.args.mixed_files_per_user;
        let base = format!("{}mixed-same-fragment", self.args.prefix);
        let reader = self.namespace_reader_probe(
            &self.token,
            (0..users)
                .map(|user| format!("{base}/user-{user:02}"))
                .collect(),
            96,
        );
        let actors = async {
            let futures = (0..users).map(|user| {
                let actor_base = format!("{base}/user-{user:02}");
                async move {
                    self.mixed_user_actor(&self.token, user, &actor_base, files_per_user)
                        .await
                }
            });
            let results = join_all(futures).await;
            let mut reports = Vec::new();
            let mut final_keys = Vec::new();
            for result in results {
                let actor = result?;
                final_keys.extend(actor.final_keys);
                reports.push(actor.report);
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((reports, final_keys))
        };
        let background = self.upload_one_shot_batch(
            &self.token,
            "same-fragment-background-upload",
            self.args.mixed_background_uploads,
            self.args.storm_object_size,
        );
        let (reader_report, actor_result, background_report) =
            tokio::try_join!(reader, actors, background)?;
        for key in actor_result.1 {
            expected.insert(key);
        }
        for key in background_report.final_keys {
            expected.insert(key);
        }
        Ok(json!({
            "users": users,
            "files_per_user": files_per_user,
            "background_uploads": self.args.mixed_background_uploads,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "reader": reader_report,
            "actors": actor_result.0,
            "background": background_report.report,
        }))
    }

    async fn parallel_multipart_user_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let users = self.args.mixed_users;
        let futures = (0..users).map(|user| async move {
            let object_key = format!(
                "{}parallel-multipart/user-{user:02}/object.bin",
                self.args.prefix
            );
            let payload = deterministic_payload(16 * 1024 * 1024, object_key.as_bytes());
            let sha = sha256_bytes(&payload);
            let report = self
                .multipart_bytes_with_token(&self.token, &object_key, payload, 2 * 1024 * 1024)
                .await?;
            self.assert_download_sha256(&object_key, &sha).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(json!({
                "user": user,
                "object_key": object_key,
                "sha256": sha,
                "upload": report,
            }))
        });
        let results = join_all(futures).await;
        let mut reports = Vec::new();
        for result in results {
            let report = result?;
            if let Some(key) = report.get("object_key").and_then(Value::as_str) {
                expected.insert(key.to_string());
            }
            reports.push(report);
        }
        Ok(json!({
            "users": users,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "reports": reports,
        }))
    }

    async fn same_key_version_contention_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let object_key = format!("{}version-contention/shared-key.bin", self.args.prefix);
        let writes = self.args.mixed_users * 16;
        let concurrency = self.args.mixed_users.max(1);
        let mut known = BTreeSet::new();
        let mut version_ids = Vec::new();
        for chunk_start in (0..writes).step_by(concurrency) {
            let chunk_end = (chunk_start + concurrency).min(writes);
            let futures = (chunk_start..chunk_end).map(|idx| {
                let object_key = object_key.clone();
                let payload =
                    deterministic_payload(256 * 1024, format!("{object_key}:{idx}").as_bytes());
                let sha = sha256_bytes(&payload);
                async move {
                    let version_id = self
                        .upload_bytes(&object_key, payload, "application/octet-stream")
                        .await?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((sha, version_id))
                }
            });
            for result in join_all(futures).await {
                let (sha, version_id) = result?;
                known.insert(sha);
                version_ids.push(version_id);
            }
        }
        self.assert_list_contains(&object_key).await?;
        let latest_sha = self.download_sha256(&object_key).await?;
        if !known.contains(&latest_sha) {
            return Err(format!(
                "same-key contention latest hash is not one of the committed versions: {latest_sha}"
            )
            .into());
        }
        expected.insert(object_key.clone());
        Ok(json!({
            "object_key": object_key,
            "writes": writes,
            "unique_payloads": known.len(),
            "version_ids": version_ids,
            "latest_sha256": latest_sha,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
        }))
    }

    async fn multi_fragment_isolation_proof(&self) -> ProofResult<Value> {
        let start = Instant::now();
        let users = self.args.multi_fragment_users;
        let files_per_user = self.args.multi_fragment_files_per_user;
        let mut fragments = Vec::new();
        for user in 0..users {
            let key = format!("{}-mf-{user:02}", self.args.fragment_key);
            let name = format!("release-proof-mf-{}-{user:02}", self.stamp);
            let fragment = self.create_named_fragment(&name, &key).await?;
            let token = self.auth_fragment(&fragment, &key).await?;
            fragments.push((user, fragment, token));
        }
        let futures = fragments.iter().map(|(user, fragment, token)| {
            let base = format!("{}multi-fragment/user-{user:02}", self.args.prefix);
            async move {
                let actor = self
                    .mixed_user_actor(token, *user, &base, files_per_user)
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(json!({
                    "user": user,
                    "fragment": fragment,
                    "actor": actor.report,
                }))
            }
        });
        let mut reports = Vec::new();
        for result in join_all(futures).await {
            reports.push(result?);
        }
        Ok(json!({
            "users": users,
            "files_per_user": files_per_user,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
            "reports": reports,
        }))
    }

    async fn mixed_user_actor(
        &self,
        token: &str,
        user: usize,
        base: &str,
        files: usize,
    ) -> ProofResult<ActorReport> {
        let start = Instant::now();
        self.posix_post_with_token(
            token,
            "/n1/posix/mkdir",
            json!({"path": format!("{base}/inbox")}),
        )
        .await?;
        self.posix_post_with_token(
            token,
            "/n1/posix/mkdir",
            json!({"path": format!("{base}/work")}),
        )
        .await?;
        let sample_stride = (files / 8).max(1);
        let mut final_keys = Vec::with_capacity(files + 32);
        let mut samples = Vec::new();
        for idx in 0..files {
            let source = format!("{base}/inbox/file-{idx:08}.bin");
            let dest = format!("{base}/work/renamed-{idx:08}.bin");
            let payload = deterministic_payload(8 * 1024, format!("{base}:{idx}").as_bytes());
            let sha = sha256_bytes(&payload);
            self.upload_bytes_with_token(token, &source, payload, "application/octet-stream")
                .await?;
            self.posix_post_with_token(
                token,
                "/n1/posix/rename",
                json!({"source_path": source, "destination_path": dest}),
            )
            .await?;
            if idx == 0 || idx + 1 == files || idx % sample_stride == 0 {
                self.assert_list_contains_with_token(token, &dest).await?;
                self.assert_download_sha256_with_token(token, &dest, &sha)
                    .await?;
                samples.push(json!({"index": idx, "object_key": dest, "sha256": sha}));
            }
            final_keys.push(dest);
        }

        let folder_source = format!("{base}/folder-source");
        let folder_dest = format!("{base}/folder-dest");
        self.posix_post_with_token(token, "/n1/posix/mkdir", json!({"path": &folder_source}))
            .await?;
        for idx in 0..16usize {
            let child = format!("{folder_source}/child-{idx:04}.bin");
            let payload = deterministic_payload(32 * 1024, child.as_bytes());
            self.upload_bytes_with_token(token, &child, payload, "application/octet-stream")
                .await?;
        }
        self.posix_post_with_token(
            token,
            "/n1/posix/rename",
            json!({"source_path": folder_source, "destination_path": folder_dest}),
        )
        .await?;
        let moved_sample = format!("{folder_dest}/child-0000.bin");
        self.assert_list_contains_with_token(token, &moved_sample)
            .await?;
        for idx in 0..16usize {
            final_keys.push(format!("{folder_dest}/child-{idx:04}.bin"));
        }

        let recover_dir = format!("{base}/recover-dir");
        let recover_child = format!("{recover_dir}/payload.bin");
        let recover_sha = sha256_bytes(&deterministic_payload(64 * 1024, recover_child.as_bytes()));
        self.posix_post_with_token(token, "/n1/posix/mkdir", json!({"path": &recover_dir}))
            .await?;
        self.upload_bytes_with_token(
            token,
            &recover_child,
            deterministic_payload(64 * 1024, recover_child.as_bytes()),
            "application/octet-stream",
        )
        .await?;
        self.posix_post_with_token(token, "/n1/posix/trash", json!({"path": &recover_dir}))
            .await?;
        self.posix_post_with_token(
            token,
            "/n1/posix/recover",
            json!({"path": recover_dir}),
        )
        .await?;
        self.assert_download_sha256_with_token(token, &recover_child, &recover_sha)
            .await?;
        final_keys.push(recover_child.clone());

        let purge_dir = format!("{base}/purge-dir");
        let purge_child = format!("{purge_dir}/payload.bin");
        self.posix_post_with_token(token, "/n1/posix/mkdir", json!({"path": &purge_dir}))
            .await?;
        self.upload_bytes_with_token(
            token,
            &purge_child,
            deterministic_payload(32 * 1024, purge_child.as_bytes()),
            "application/octet-stream",
        )
        .await?;
        self.posix_post_with_token(token, "/n1/posix/trash", json!({"path": &purge_dir}))
            .await?;
        self.posix_post_with_token(
            token,
            "/n1/posix/purge",
            json!({"path": purge_dir}),
        )
        .await?;

        Ok(ActorReport {
            final_keys,
            report: json!({
                "user": user,
                "base": base,
                "renamed_files": files,
                "folder_renamed_children": 16,
                "trash_recover_child": recover_child,
                "purged_folder": purge_dir,
                "verified_samples": samples,
                "elapsed_s": round3(start.elapsed().as_secs_f64()),
            }),
        })
    }

    async fn upload_one_shot_batch(
        &self,
        token: &str,
        label: &str,
        count: usize,
        object_size: usize,
    ) -> ProofResult<ActorReport> {
        let start = Instant::now();
        let concurrency = self.args.storm_concurrency.max(1);
        let mut final_keys = Vec::with_capacity(count);
        let mut samples = Vec::new();
        let sample_stride = (count / 8).max(1);
        for chunk_start in (0..count).step_by(concurrency) {
            let chunk_end = (chunk_start + concurrency).min(count);
            let futures = (chunk_start..chunk_end).map(|idx| {
                let key = format!("{}batch/{label}/object-{idx:08}.bin", self.args.prefix);
                let payload = deterministic_payload(object_size, key.as_bytes());
                let sha = sha256_bytes(&payload);
                async move {
                    let version_id = self
                        .upload_bytes_with_token(token, &key, payload, "application/octet-stream")
                        .await?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((idx, key, sha, version_id))
                }
            });
            for result in join_all(futures).await {
                let (idx, key, sha, version_id) = result?;
                if idx == 0 || idx + 1 == count || idx % sample_stride == 0 {
                    self.assert_download_sha256_with_token(token, &key, &sha)
                        .await?;
                    samples.push(json!({
                        "index": idx,
                        "object_key": key,
                        "sha256": sha,
                        "version_id": version_id,
                    }));
                }
                final_keys.push(key);
            }
        }
        Ok(ActorReport {
            final_keys,
            report: json!({
                "label": label,
                "objects": count,
                "object_size_bytes": object_size,
                "concurrency": concurrency,
                "elapsed_s": round3(start.elapsed().as_secs_f64()),
                "verified_samples": samples,
            }),
        })
    }

    async fn namespace_reader_probe(
        &self,
        token: &str,
        bases: Vec<String>,
        rounds: usize,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let mut reads = 0usize;
        for _ in 0..rounds {
            for base in &bases {
                self.list_path_with_token(token, base, 200).await?;
                self.list_path_with_token(token, &format!("{base}/work"), 200)
                    .await?;
                reads += 2;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(json!({
            "bases": bases.len(),
            "rounds": rounds,
            "list_reads": reads,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
        }))
    }

    async fn multipart_bytes_with_token(
        &self,
        token: &str,
        object_key: &str,
        payload: Vec<u8>,
        part_size: usize,
    ) -> ProofResult<Value> {
        let start = Instant::now();
        let expected_parts = payload.len().div_ceil(part_size);
        let params = json!({
            "content_type": "application/octet-stream",
            "total_size_bytes": payload.len(),
            "expected_parts": expected_parts,
            "part_size_bytes": part_size,
        });
        let response = self
            .client
            .post(format!(
                "{}/n1/upload/multipart/init/{}",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "parallel multipart init failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let init: MultipartInit = response.json().await?;
        for (idx, chunk) in payload.chunks(part_size).enumerate() {
            let response = self
                .client
                .put(format!(
                    "{}/n1/upload/multipart/{}/{}?durability=batched",
                    self.base_url, init.version_id, idx
                ))
                .bearer_auth(token)
                .header("content-type", "application/octet-stream")
                .body(chunk.to_vec())
                .send()
                .await?;
            if response.status() != StatusCode::OK && response.status() != StatusCode::CREATED {
                return Err(format!(
                    "parallel multipart part {idx} failed: {} {}",
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
                encode_query_value(object_key)
            ))
            .bearer_auth(token)
            .json(&params)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "parallel multipart finalize failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(json!({
            "object_key": object_key,
            "bytes": payload.len(),
            "parts": expected_parts,
            "part_size_bytes": part_size,
            "version_id": init.version_id,
            "session_id": init.session_id,
            "elapsed_s": round3(start.elapsed().as_secs_f64()),
        }))
    }

    async fn list_path_with_token(
        &self,
        token: &str,
        path: &str,
        limit: usize,
    ) -> ProofResult<Value> {
        let response = self
            .client
            .get(format!(
                "{}/n1/posix/list?path={}&limit={}",
                self.base_url,
                encode_query_value(path),
                limit
            ))
            .bearer_auth(token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "list probe failed for {path}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.json().await?)
    }

    async fn wait_ws_control(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        op: &str,
    ) -> ProofResult<Value> {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            let next = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
            let Some(message) = next.ok().flatten().transpose()? else {
                continue;
            };
            match message {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(text.as_ref())?;
                    seen.push(value.clone());
                    if value.get("op").and_then(Value::as_str) == Some(op) {
                        return Ok(value);
                    }
                }
                Message::Ping(bytes) => {
                    ws.send(Message::Pong(bytes)).await?;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        Err(format!("websocket did not emit control op={op}; seen={seen:?}").into())
    }

    async fn wait_ws_namespace_event(
        &self,
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        operation: &str,
        path: Option<&str>,
        source_path: Option<&str>,
    ) -> ProofResult<Value> {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            let next = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
            let Some(message) = next.ok().flatten().transpose()? else {
                continue;
            };
            match message {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(text.as_ref())?;
                    seen.push(value.clone());
                    let is_event = value.get("op").and_then(Value::as_str) == Some("EVENT");
                    let data = value
                        .get("d")
                        .and_then(|event| event.get("data"))
                        .cloned()
                        .unwrap_or_else(|| json!(null));
                    let event_operation = data.get("operation").and_then(Value::as_str);
                    let event_path = data.get("path").and_then(Value::as_str);
                    let event_source = data.get("source_path").and_then(Value::as_str);
                    let event_dest = data.get("destination_path").and_then(Value::as_str);
                    let matches_operation =
                        event_operation.is_some_and(|actual| actual.contains(operation));
                    let matches_path = path.is_none_or(|wanted| {
                        event_path == Some(wanted) || event_dest == Some(wanted)
                    });
                    let matches_source =
                        source_path.is_none_or(|wanted| event_source == Some(wanted));
                    if is_event && matches_operation && matches_path && matches_source {
                        return Ok(data);
                    }
                }
                Message::Ping(bytes) => {
                    ws.send(Message::Pong(bytes)).await?;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        Err(format!(
            "websocket did not emit operation={operation} path={path:?} source={source_path:?}; seen={seen:?}"
        )
        .into())
    }
}
