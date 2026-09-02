impl Proof {
    async fn concurrent_standard_batch_upload_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        const OBJECTS_PER_CLIENT: usize = 64;
        const OBJECT_SIZE: usize = 16 * 1024;
        let clients = self.args.mixed_users.clamp(2, 32);

        let started = Instant::now();
        let requests = (0..clients).map(|client_index| async move {
            let mut form = reqwest::multipart::Form::new();
            let mut keys = Vec::with_capacity(OBJECTS_PER_CLIENT);
            let mut samples = Vec::new();
            for object_index in 0..OBJECTS_PER_CLIENT {
                let key = format!(
                    "{}concurrent-standard-batch/client-{client_index:02}/file-{object_index:04}.bin",
                    self.args.prefix
                );
                let payload = deterministic_payload(OBJECT_SIZE, key.as_bytes());
                if object_index == 0 || object_index + 1 == OBJECTS_PER_CLIENT {
                    samples.push((key.clone(), sha256_bytes(&payload)));
                }
                form = form.part(
                    "object",
                    reqwest::multipart::Part::bytes(payload)
                        .file_name(encode_query_value(&key))
                        .mime_str("application/octet-stream")?,
                );
                keys.push(key);
            }

            let request_started = Instant::now();
            let response = tokio::time::timeout(
                Duration::from_secs(60),
                self.client
                    .post(format!("{}/noa/v1/upload/batch", self.base_url))
                    .bearer_auth(&self.token)
                    .multipart(form)
                    .send(),
            )
            .await
            .map_err(|_| format!("concurrent standard batch client {client_index} timed out"))??;
            let status = response.status();
            let body: Value = response.json().await?;
            if status != StatusCode::CREATED
                || body.get("succeeded").and_then(Value::as_u64)
                    != Some(OBJECTS_PER_CLIENT as u64)
                || body.get("failed").and_then(Value::as_u64) != Some(0)
            {
                return Err(format!(
                    "concurrent standard batch client {client_index} failed: {status} {body}"
                )
                .into());
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                keys,
                samples,
                request_started.elapsed().as_millis() as u64,
            ))
        });

        let mut request_elapsed_ms = Vec::with_capacity(clients);
        let mut verified_samples = Vec::new();
        for result in join_all(requests).await {
            let (keys, samples, elapsed_ms) = result?;
            request_elapsed_ms.push(elapsed_ms);
            expected.extend(keys);
            for (key, sha256) in samples {
                self.assert_download_sha256(&key, &sha256).await?;
                verified_samples.push(json!({ "object_key": key, "sha256": sha256 }));
            }
        }

        let elapsed = started.elapsed().as_secs_f64();
        let object_count = clients * OBJECTS_PER_CLIENT;
        Ok(json!({
            "status": "PASS",
            "concurrent_clients": clients,
            "objects_per_client": OBJECTS_PER_CLIENT,
            "objects": object_count,
            "object_size_bytes": OBJECT_SIZE,
            "elapsed_s": round3(elapsed),
            "objects_per_s": round3(object_count as f64 / elapsed.max(0.001)),
            "mib_s": round3(((object_count * OBJECT_SIZE) as f64 / 1024.0 / 1024.0) / elapsed.max(0.001)),
            "request_elapsed_ms": request_elapsed_ms,
            "verified_samples": verified_samples,
        }))
    }

    async fn standard_batch_upload_proof(
        &self,
        expected: &mut BTreeSet<String>,
    ) -> ProofResult<Value> {
        const OBJECT_COUNT: usize = 256;
        const OBJECT_SIZE: usize = 8 * 1024;

        let base = format!("{}standard-batch", self.args.prefix);
        let conflict_key = format!("{base}/directory-conflict");
        self.posix_post(
            "/noa/v1/posix/mkdir",
            json!({"path": &conflict_key}),
        )
        .await?;

        let mut form = reqwest::multipart::Form::new();
        let mut samples = Vec::<(String, String)>::new();
        let mut batch_keys = Vec::with_capacity(OBJECT_COUNT);
        for index in 0..OBJECT_COUNT {
            let key = format!("{base}/tenant-{}/file-{index:06}.bin", index % 32);
            let payload = deterministic_payload(OBJECT_SIZE, key.as_bytes());
            if index == 0 || index + 1 == OBJECT_COUNT || index % 64 == 0 {
                samples.push((key.clone(), sha256_bytes(&payload)));
            }
            let part = reqwest::multipart::Part::bytes(payload)
                .file_name(encode_query_value(&key))
                .mime_str("application/octet-stream")?;
            form = form.part("object", part);
            batch_keys.push(key);
        }

        form = form.part(
            "object",
            reqwest::multipart::Part::bytes(b"must-conflict".to_vec())
                .file_name(encode_query_value(&conflict_key))
                .mime_str("application/octet-stream")?,
        );

        let start = Instant::now();
        let response = self
            .client
            .post(format!(
                "{}/noa/v1/upload/batch?concurrency={}",
                self.base_url,
                self.args.storm_concurrency.clamp(1, 128)
            ))
            .bearer_auth(&self.token)
            .multipart(form)
            .send()
            .await?;
        let status = response.status();
        let body: Value = response.json().await?;
        if status != StatusCode::MULTI_STATUS {
            return Err(format!(
                "standard batch expected 207 for mixed results, got {status}: {body}"
            )
            .into());
        }

        let requested = body
            .get("requested")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize;
        let succeeded = body
            .get("succeeded")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize;
        let failed = body
            .get("failed")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize;
        if requested != OBJECT_COUNT + 1 || succeeded != OBJECT_COUNT || failed != 1 {
            return Err(format!(
                "standard batch count mismatch: requested={requested} succeeded={succeeded} failed={failed} body={body}"
            )
            .into());
        }

        let conflict = body
            .get("results")
            .and_then(Value::as_array)
            .and_then(|results| {
                results.iter().find(|entry| {
                    entry.get("object_key").and_then(Value::as_str)
                        == Some(conflict_key.as_str())
                })
            })
            .ok_or("standard batch response omitted conflict result")?;
        if conflict.get("status").and_then(Value::as_u64) != Some(409) {
            return Err(format!("standard batch conflict was not isolated as 409: {conflict}").into());
        }

        for key in batch_keys {
            expected.insert(key);
        }
        for (key, sha256) in &samples {
            self.assert_list_contains(key).await?;
            self.assert_download_sha256(key, sha256).await?;
            self.assert_object_status_clean(key, "after_standard_batch")
                .await?;
        }

        let elapsed = start.elapsed().as_secs_f64();
        Ok(json!({
            "status": "PASS",
            "objects": OBJECT_COUNT,
            "object_size_bytes": OBJECT_SIZE,
            "request_count": 1,
            "concurrency": self.args.storm_concurrency.clamp(1, 128),
            "elapsed_s": round3(elapsed),
            "objects_per_s": round3(OBJECT_COUNT as f64 / elapsed.max(0.001)),
            "mib_s": round3(((OBJECT_COUNT * OBJECT_SIZE) as f64 / 1024.0 / 1024.0) / elapsed.max(0.001)),
            "mixed_result_isolation": "256 created plus one deterministic directory conflict",
            "verified_samples": samples.iter().map(|(key, sha256)| json!({
                "object_key": key,
                "sha256": sha256,
            })).collect::<Vec<_>>(),
        }))
    }
}
