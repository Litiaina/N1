impl Proof {
    async fn run_multipart_resume_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("multipart-resume-start").await?;
        self.create_fragment().await?;
        self.auth().await?;

        let bytes = self.args.multipart_resume_bytes.max(1);
        let part_size = self.args.multipart_resume_part_bytes.max(1);
        let concurrency = self.args.multipart_resume_concurrency.clamp(1, 16);
        if part_size as u64 > self.args.max_part_size {
            return Err(format!(
                "multipart resume part size {} exceeds configured max_part_size {}",
                part_size, self.args.max_part_size
            )
            .into());
        }

        let source = self.args.out_dir.join("multipart-resume-source.bin");
        create_benchmark_file(&source, bytes, b"n1-multipart-resume-proof-source")?;
        let source_sha = self.sha256_file(&source).await?;
        let expected_parts = bytes.div_ceil(part_size);
        let restart_after_parts = (expected_parts / 2).max(1);
        let mut rss_samples = vec![self.rss_sample("resume_started")];

        let http_key = format!("{}resume/http-2g.bin", self.args.prefix);
        let http = self
            .http_multipart_resume_file(
                &http_key,
                &source,
                bytes,
                part_size,
                concurrency,
                restart_after_parts,
            )
            .await?;
        self.assert_list_contains(&http_key).await?;
        self.assert_download_sha256(&http_key, &source_sha).await?;
        rss_samples.push(self.rss_sample("after_http_resume"));

        let native_key = format!("{}resume/native-2g.bin", self.args.prefix);
        let native = self
            .native_multipart_resume_file(
                &native_key,
                &source,
                bytes,
                part_size,
                concurrency,
                restart_after_parts,
            )
            .await?;
        self.assert_list_contains(&native_key).await?;
        self.assert_download_sha256(&native_key, &source_sha).await?;
        rss_samples.push(self.rss_sample("after_native_resume"));

        self.assert_health_clean("multipart_resume_final").await?;
        let inspection = self.inspect_index(2)?;
        self.stop_server()?;
        let logs = self.scan_logs()?;

        self.summary = json!({
            "status": "PASS",
            "proof": "n1_multipart_resume_http_native",
            "bytes": bytes,
            "part_size_bytes": part_size,
            "expected_parts": expected_parts,
            "restart_after_parts": restart_after_parts,
            "concurrency": concurrency,
            "source_sha256": source_sha,
            "http": http,
            "native": native,
            "rss_samples": rss_samples,
            "index_inspection": inspection,
            "log_scan": logs,
            "fragment_hash": self.fragment,
        });
        self.write_summary()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }

    async fn http_multipart_resume_file(
        &mut self,
        object_key: &str,
        path: &Path,
        total_size: usize,
        part_size: usize,
        concurrency: usize,
        restart_after_parts: usize,
    ) -> ProofResult<Value> {
        let expected_parts = total_size.div_ceil(part_size);
        let params = json!({
            "content_type": "application/octet-stream",
            "total_size_bytes": total_size,
            "expected_parts": expected_parts,
            "part_size_bytes": part_size,
        });
        let started = Instant::now();
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
                "http multipart resume init failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let init: MultipartInit = response.json().await?;

        let mut part_samples = self
            .http_upload_part_batches(
                &init.version_id,
                path,
                total_size,
                part_size,
                0,
                restart_after_parts,
                concurrency,
            )
            .await?;
        let before_restart_elapsed = started.elapsed();
        self.stop_server()?;
        self.start_server("multipart-resume-http").await?;
        self.auth().await?;
        part_samples.extend(
            self.http_upload_part_batches(
                &init.version_id,
                path,
                total_size,
                part_size,
                restart_after_parts,
                expected_parts,
                concurrency,
            )
            .await?,
        );

        let finalize_started = Instant::now();
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
                "http multipart resume finalize failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let finalize_elapsed = finalize_started.elapsed();
        let elapsed = started.elapsed();

        Ok(json!({
            "mode": "http_multipart_resume",
            "object_key": object_key,
            "bytes": total_size,
            "parts": expected_parts,
            "part_size_bytes": part_size,
            "concurrency": concurrency,
            "restart_after_parts": restart_after_parts,
            "elapsed_s": round3(elapsed.as_secs_f64()),
            "mib_s": round3(mbps(total_size, elapsed)),
            "elapsed_before_restart_s": round3(before_restart_elapsed.as_secs_f64()),
            "finalize_elapsed_s": round3(finalize_elapsed.as_secs_f64()),
            "version_id": init.version_id,
            "session_id": init.session_id,
            "part_samples": part_samples,
        }))
    }

    async fn http_upload_part_batches(
        &self,
        version_id: &str,
        path: &Path,
        total_size: usize,
        part_size: usize,
        start_part: usize,
        end_part: usize,
        concurrency: usize,
    ) -> ProofResult<Vec<Value>> {
        let mut part_samples = Vec::new();
        for batch_start in (start_part..end_part).step_by(concurrency.max(1)) {
            let batch_end = (batch_start + concurrency.max(1)).min(end_part);
            let futures = (batch_start..batch_end).map(|idx| {
                let client = self.client.clone();
                let base_url = self.base_url.clone();
                let token = self.token.clone();
                let version_id = version_id.to_string();
                let path = path.to_path_buf();
                async move {
                    let offset = idx * part_size;
                    let len = total_size.saturating_sub(offset).min(part_size);
                    let mut file = tokio::fs::File::open(&path).await?;
                    file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
                    let reader = file.take(len as u64);
                    let stream = ReaderStream::with_capacity(reader, 1024 * 1024);
                    let started = Instant::now();
                    let response = client
                        .put(format!(
                            "{base_url}/noa/v1/upload/multipart/durable/{version_id}/{idx}?durability=batched"
                        ))
                        .bearer_auth(token)
                        .header("content-type", "application/octet-stream")
                        .header(CONTENT_LENGTH, len)
                        .body(reqwest::Body::wrap_stream(stream))
                        .send()
                        .await?;
                    if response.status() != StatusCode::OK
                        && response.status() != StatusCode::CREATED
                    {
                        return Err(format!(
                            "http multipart resume part {idx} failed: {} {}",
                            response.status(),
                            response.text().await?
                        )
                        .into());
                    }
                    let elapsed = started.elapsed();
                    Ok::<Value, Box<dyn std::error::Error + Send + Sync>>(json!({
                        "part": idx,
                        "bytes": len,
                        "elapsed_s": round3(elapsed.as_secs_f64()),
                        "mib_per_s": round3(mbps(len, elapsed)),
                    }))
                }
            });
            for result in join_all(futures).await {
                part_samples.push(result?);
            }
        }
        Ok(part_samples)
    }

    async fn native_multipart_resume_file(
        &mut self,
        object_key: &str,
        path: &Path,
        total_size: usize,
        part_size: usize,
        concurrency: usize,
        restart_after_parts: usize,
    ) -> ProofResult<Value> {
        let expected_parts = total_size.div_ceil(part_size);
        let params = json!({
            "object_key": object_key,
            "content_type": "application/octet-stream",
            "total_size_bytes": total_size,
            "expected_parts": expected_parts,
            "part_size_bytes": part_size,
        });
        let started = Instant::now();
        let mut control_stream = self
            .open_native_upload_benchmark_stream("native-resume-control-before")
            .await?;
        let init = native_round_trip(
            &mut control_stream,
            "native-resume-init",
            "multipart.init",
            params.clone(),
            None,
        )
        .await?;
        ensure_native_status(&init, 201, "native multipart resume init")?;
        let init_body = native_json_body(&init)?;
        let version_id = init_body
            .get("version_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("native resume init missing version_id: {init_body}"))?
            .to_string();
        let session_id = init_body
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("native resume init missing session_id: {init_body}"))?
            .to_string();

        let mut part_samples = self
            .native_upload_part_batches(
                &version_id,
                path,
                total_size,
                part_size,
                0,
                restart_after_parts,
                concurrency,
                "before",
            )
            .await?;
        let before_restart_elapsed = started.elapsed();
        self.stop_server()?;
        self.start_server("multipart-resume-native").await?;
        self.auth().await?;
        part_samples.extend(
            self.native_upload_part_batches(
                &version_id,
                path,
                total_size,
                part_size,
                restart_after_parts,
                expected_parts,
                concurrency,
                "after",
            )
            .await?,
        );

        let mut control_stream = self
            .open_native_upload_benchmark_stream("native-resume-control-after")
            .await?;
        let finalize_started = Instant::now();
        let finalize = native_round_trip(
            &mut control_stream,
            "native-resume-finalize",
            "multipart.finalize",
            json!({
                "object_key": object_key,
                "version_id": version_id,
                "session_id": session_id,
                "content_type": "application/octet-stream",
                "total_size_bytes": total_size,
                "expected_parts": expected_parts,
                "part_size_bytes": part_size,
            }),
            None,
        )
        .await?;
        ensure_native_status(&finalize, 200, "native multipart resume finalize")?;
        let finalize_elapsed = finalize_started.elapsed();
        let elapsed = started.elapsed();

        Ok(json!({
            "mode": "native_multipart_resume",
            "object_key": object_key,
            "bytes": total_size,
            "parts": expected_parts,
            "part_size_bytes": part_size,
            "concurrency": concurrency,
            "restart_after_parts": restart_after_parts,
            "elapsed_s": round3(elapsed.as_secs_f64()),
            "mib_s": round3(mbps(total_size, elapsed)),
            "elapsed_before_restart_s": round3(before_restart_elapsed.as_secs_f64()),
            "finalize_elapsed_s": round3(finalize_elapsed.as_secs_f64()),
            "version_id": version_id,
            "session_id": session_id,
            "part_samples": part_samples,
        }))
    }

    async fn native_upload_part_batches(
        &self,
        version_id: &str,
        path: &Path,
        total_size: usize,
        part_size: usize,
        start_part: usize,
        end_part: usize,
        concurrency: usize,
        label: &str,
    ) -> ProofResult<Vec<Value>> {
        let concurrency = concurrency.max(1);
        let mut streams = Vec::new();
        for slot in 0..concurrency {
            streams.push(Some(
                self.open_native_upload_benchmark_stream(&format!(
                    "native-resume-{label}-worker-{slot}"
                ))
                .await?,
            ));
        }

        let mut part_samples = Vec::new();
        for batch_start in (start_part..end_part).step_by(concurrency) {
            let batch_end = (batch_start + concurrency).min(end_part);
            let futures = (batch_start..batch_end).enumerate().map(|(slot, idx)| {
                let mut stream = streams[slot].take().expect("native resume stream present");
                let version_id = version_id.to_string();
                let path = path.to_path_buf();
                let label = label.to_string();
                async move {
                    let offset = idx * part_size;
                    let len = total_size.saturating_sub(offset).min(part_size);
                    let started = Instant::now();
                    let response = native_round_trip_file_range(
                        &mut stream,
                        &format!("native-resume-{label}-part-{idx}"),
                        "multipart.put_part",
                        json!({
                            "version_id": version_id,
                            "part_index": idx,
                            "durability": "batched",
                        }),
                        &path,
                        offset as u64,
                        len,
                    )
                    .await?;
                    ensure_native_status(&response, 200, "native multipart resume part")?;
                    let elapsed = started.elapsed();
                    Ok::<(usize, TcpStream, Value), Box<dyn std::error::Error + Send + Sync>>((
                        slot,
                        stream,
                        json!({
                            "part": idx,
                            "bytes": len,
                            "elapsed_s": round3(elapsed.as_secs_f64()),
                            "mib_per_s": round3(mbps(len, elapsed)),
                        }),
                    ))
                }
            });
            for result in join_all(futures).await {
                let (slot, stream, sample) = result?;
                streams[slot] = Some(stream);
                part_samples.push(sample);
            }
        }
        Ok(part_samples)
    }
}
