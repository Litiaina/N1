use std::io::Write as IoWrite;

impl Proof {
    async fn run_upload_benchmark(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;

        let repeats = self.args.upload_benchmark_repeats.max(1);
        let one_shot_bytes = self.args.one_shot_benchmark_bytes;
        let multipart_bytes = self.args.multipart_benchmark_bytes;
        let multipart_part_bytes = self.args.multipart_benchmark_part_bytes.max(1);
        let requested_multipart_concurrency = self.args.multipart_benchmark_concurrency;
        let multipart_concurrency = effective_upload_benchmark_concurrency(
            requested_multipart_concurrency,
            self.active_disk_count,
        );

        if one_shot_bytes as u64 > self.args.max_part_size {
            return Err(format!(
                "one-shot benchmark size {} exceeds configured max_part_size {}",
                one_shot_bytes, self.args.max_part_size
            )
            .into());
        }
        if multipart_part_bytes as u64 > self.args.max_part_size {
            return Err(format!(
                "multipart benchmark part size {} exceeds configured max_part_size {}",
                multipart_part_bytes, self.args.max_part_size
            )
            .into());
        }

        let one_shot_file = self.args.out_dir.join("one-shot-benchmark-source.bin");
        create_benchmark_file(
            &one_shot_file,
            one_shot_bytes,
            b"n1-one-shot-upload-benchmark-source",
        )?;

        let multipart_file = self.args.out_dir.join("multipart-benchmark-source.bin");
        create_benchmark_file(
            &multipart_file,
            multipart_bytes,
            b"n1-multipart-upload-benchmark-source",
        )?;

        let mut rss_samples = Vec::new();
        let mut disk_io_samples = Vec::new();

        self.start_server("upload-http").await?;
        self.create_fragment().await?;
        self.auth().await?;
        rss_samples.push(self.rss_sample("http_started"));

        let mut one_shot_samples = Vec::new();
        for idx in 0..repeats {
            let object_key = format!("{}bench/one-shot/object-{idx:04}.bin", self.args.prefix);
            let before_io = capture_disk_io(&self.blob_roots);
            let started = Instant::now();
            let version_id = self
                .upload_file_one_shot_benchmark(
                    &self.token,
                    &object_key,
                    &one_shot_file,
                    one_shot_bytes,
                    "application/octet-stream",
                )
                .await?;
            let elapsed = started.elapsed();
            disk_io_samples.push(disk_io_delta("http_one_shot", &before_io, &capture_disk_io(&self.blob_roots)));
            self.assert_list_contains(&object_key).await?;
            let sample = upload_bench_sample_json(
                &object_key,
                Some(&version_id),
                one_shot_bytes,
                elapsed,
                None,
            );
            eprintln!(
                "one-shot sample {}: {} bytes in {:.3}s ({:.2} MiB/s)",
                idx + 1,
                one_shot_bytes,
                elapsed.as_secs_f64(),
                mbps(one_shot_bytes, elapsed)
            );
            one_shot_samples.push(sample);
        }
        rss_samples.push(self.rss_sample("after_http_one_shot"));

        let mut multipart_samples = Vec::new();
        for idx in 0..repeats {
            let object_key = format!("{}bench/multipart/object-{idx:04}.bin", self.args.prefix);
            let before_io = capture_disk_io(&self.blob_roots);
            let started = Instant::now();
            let multipart = self
                .multipart_file_benchmark(
                    &self.token,
                    &object_key,
                    &multipart_file,
                    multipart_bytes,
                    multipart_part_bytes,
                )
                .await?;
            let elapsed = started.elapsed();
            disk_io_samples.push(disk_io_delta("http_multipart", &before_io, &capture_disk_io(&self.blob_roots)));
            self.assert_list_contains(&object_key).await?;
            let version_id = multipart
                .get("version_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let sample = upload_bench_sample_json(
                &object_key,
                version_id.as_deref(),
                multipart_bytes,
                elapsed,
                Some(multipart),
            );
            eprintln!(
                "multipart sample {}: {} bytes in {:.3}s ({:.2} MiB/s)",
                idx + 1,
                multipart_bytes,
                elapsed.as_secs_f64(),
                mbps(multipart_bytes, elapsed)
            );
            multipart_samples.push(sample);
        }
        rss_samples.push(self.rss_sample("after_http_multipart"));

        let mut multipart_pipelined_samples = Vec::new();
        for idx in 0..repeats {
            let object_key = format!(
                "{}bench/multipart-pipelined/object-{idx:04}.bin",
                self.args.prefix
            );
            let before_io = capture_disk_io(&self.blob_roots);
            let started = Instant::now();
            let multipart = self
                .multipart_file_benchmark_pipelined(
                    &self.token,
                    &object_key,
                    &multipart_file,
                    multipart_bytes,
                    multipart_part_bytes,
                    multipart_concurrency,
                )
                .await?;
            let elapsed = started.elapsed();
            disk_io_samples.push(disk_io_delta("http_multipart_pipelined", &before_io, &capture_disk_io(&self.blob_roots)));
            self.assert_list_contains(&object_key).await?;
            let version_id = multipart
                .get("version_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let sample = upload_bench_sample_json(
                &object_key,
                version_id.as_deref(),
                multipart_bytes,
                elapsed,
                Some(multipart),
            );
            eprintln!(
                "multipart pipelined sample {}: {} bytes in {:.3}s ({:.2} MiB/s, concurrency={})",
                idx + 1,
                multipart_bytes,
                elapsed.as_secs_f64(),
                mbps(multipart_bytes, elapsed),
                multipart_concurrency
            );
            multipart_pipelined_samples.push(sample);
        }
        rss_samples.push(self.rss_sample("after_http_multipart_pipelined"));
        self.stop_server()?;

        self.start_server("upload-native").await?;
        self.auth().await?;
        rss_samples.push(self.rss_sample("native_started"));
        let mut native_stream = TcpStream::connect(("127.0.0.1", self.native_port())).await?;
        native_stream.set_nodelay(true)?;
        ensure_native_ok(
            &native_round_trip(
                &mut native_stream,
                "upload-bench-native-auth",
                "auth.fragment",
                json!({
                    "fragment": self.fragment,
                    "secret": self.args.fragment_key,
                    "read": true,
                    "write": true,
                    "modify": true,
                    "delete": true,
                    "expires_in": 21600,
                }),
                None,
            )
            .await?,
            "native upload benchmark auth",
        )?;

        let mut native_one_shot_samples = Vec::new();
        for idx in 0..repeats {
            let object_key = format!(
                "{}bench/native-one-shot/object-{idx:04}.bin",
                self.args.prefix
            );
            let before_io = capture_disk_io(&self.blob_roots);
            let started = Instant::now();
            let response = native_round_trip_file_range(
                &mut native_stream,
                &format!("native-one-shot-{idx}"),
                "upload.one_shot",
                json!({
                    "object_key": object_key,
                    "content_type": "application/octet-stream",
                    "durability": "batched",
                }),
                &one_shot_file,
                0,
                one_shot_bytes,
            )
            .await?;
            ensure_native_status(&response, 201, "native one-shot upload benchmark")?;
            let elapsed = started.elapsed();
            disk_io_samples.push(disk_io_delta("native_one_shot", &before_io, &capture_disk_io(&self.blob_roots)));
            self.assert_list_contains(&object_key).await?;
            let body = native_json_body(&response)?;
            let version_id = body
                .get("version_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let sample = upload_bench_sample_json(
                &object_key,
                version_id.as_deref(),
                one_shot_bytes,
                elapsed,
                None,
            );
            eprintln!(
                "native one-shot sample {}: {} bytes in {:.3}s ({:.2} MiB/s)",
                idx + 1,
                one_shot_bytes,
                elapsed.as_secs_f64(),
                mbps(one_shot_bytes, elapsed)
            );
            native_one_shot_samples.push(sample);
        }
        rss_samples.push(self.rss_sample("after_native_one_shot"));

        let mut native_multipart_samples = Vec::new();
        for idx in 0..repeats {
            let object_key = format!(
                "{}bench/native-multipart/object-{idx:04}.bin",
                self.args.prefix
            );
            let before_io = capture_disk_io(&self.blob_roots);
            let started = Instant::now();
            let multipart = self
                .native_multipart_file_benchmark(
                    &mut native_stream,
                    &object_key,
                    &multipart_file,
                    multipart_bytes,
                    multipart_part_bytes,
                    idx,
                )
                .await?;
            let elapsed = started.elapsed();
            disk_io_samples.push(disk_io_delta("native_multipart", &before_io, &capture_disk_io(&self.blob_roots)));
            self.assert_list_contains(&object_key).await?;
            let version_id = multipart
                .get("version_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let sample = upload_bench_sample_json(
                &object_key,
                version_id.as_deref(),
                multipart_bytes,
                elapsed,
                Some(multipart),
            );
            eprintln!(
                "native multipart sample {}: {} bytes in {:.3}s ({:.2} MiB/s)",
                idx + 1,
                multipart_bytes,
                elapsed.as_secs_f64(),
                mbps(multipart_bytes, elapsed)
            );
            native_multipart_samples.push(sample);
        }
        rss_samples.push(self.rss_sample("after_native_multipart"));

        let mut native_multipart_pipelined_samples = Vec::new();
        for idx in 0..repeats {
            let object_key = format!(
                "{}bench/native-multipart-pipelined/object-{idx:04}.bin",
                self.args.prefix
            );
            let before_io = capture_disk_io(&self.blob_roots);
            let started = Instant::now();
            let multipart = self
                .native_multipart_file_benchmark_pipelined(
                    &object_key,
                    &multipart_file,
                    multipart_bytes,
                    multipart_part_bytes,
                    idx,
                    multipart_concurrency,
                )
                .await?;
            let elapsed = started.elapsed();
            disk_io_samples.push(disk_io_delta("native_multipart_pipelined", &before_io, &capture_disk_io(&self.blob_roots)));
            self.assert_list_contains(&object_key).await?;
            let version_id = multipart
                .get("version_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let sample = upload_bench_sample_json(
                &object_key,
                version_id.as_deref(),
                multipart_bytes,
                elapsed,
                Some(multipart),
            );
            eprintln!(
                "native multipart pipelined sample {}: {} bytes in {:.3}s ({:.2} MiB/s, concurrency={})",
                idx + 1,
                multipart_bytes,
                elapsed.as_secs_f64(),
                mbps(multipart_bytes, elapsed),
                multipart_concurrency
            );
            native_multipart_pipelined_samples.push(sample);
        }
        rss_samples.push(self.rss_sample("after_native_multipart_pipelined"));

        let one_shot_stats =
            upload_benchmark_stats("one_shot", one_shot_bytes, &one_shot_samples);
        let multipart_stats =
            upload_benchmark_stats("multipart", multipart_bytes, &multipart_samples);
        let native_one_shot_stats = upload_benchmark_stats(
            "native_one_shot",
            one_shot_bytes,
            &native_one_shot_samples,
        );
        let native_multipart_stats = upload_benchmark_stats(
            "native_multipart",
            multipart_bytes,
            &native_multipart_samples,
        );
        let multipart_pipelined_stats = upload_benchmark_stats(
            "multipart_pipelined",
            multipart_bytes,
            &multipart_pipelined_samples,
        );
        let native_multipart_pipelined_stats = upload_benchmark_stats(
            "native_multipart_pipelined",
            multipart_bytes,
            &native_multipart_pipelined_samples,
        );
        self.assert_health_clean("upload_benchmark_final").await?;
        let rss = self.rss_sample("after_upload_benchmark");
        let logs = self.scan_logs()?;

        self.summary = json!({
            "status": "PASS",
            "proof": "n1_upload_benchmark",
            "stamp": self.stamp,
            "fragment_hash": self.fragment,
            "base_url": self.base_url,
            "disk_roots": self.disk_roots()?.iter().map(|path| path.display().to_string()).collect::<Vec<_>>(),
            "active_disk_count": self.active_disk_count,
            "rate_limiter_enabled": self.args.rate_limiter_enabled,
            "max_part_size": self.args.max_part_size,
            "repeats": repeats,
            "requested_multipart_concurrency": requested_multipart_concurrency,
            "effective_multipart_concurrency": multipart_concurrency,
            "concurrency_policy": upload_benchmark_concurrency_policy(
                requested_multipart_concurrency,
                self.active_disk_count,
                multipart_concurrency,
            ),
            "one_shot": one_shot_stats,
            "multipart": {
                "part_size_bytes": multipart_part_bytes,
                "expected_parts": multipart_bytes.div_ceil(multipart_part_bytes),
                "stats": multipart_stats,
            },
            "multipart_pipelined": {
                "part_size_bytes": multipart_part_bytes,
                "expected_parts": multipart_bytes.div_ceil(multipart_part_bytes),
                "concurrency": multipart_concurrency,
                "stats": multipart_pipelined_stats,
            },
            "native_one_shot": native_one_shot_stats,
            "native_multipart": {
                "part_size_bytes": multipart_part_bytes,
                "expected_parts": multipart_bytes.div_ceil(multipart_part_bytes),
                "stats": native_multipart_stats,
            },
            "native_multipart_pipelined": {
                "part_size_bytes": multipart_part_bytes,
                "expected_parts": multipart_bytes.div_ceil(multipart_part_bytes),
                "concurrency": multipart_concurrency,
                "stats": native_multipart_pipelined_stats,
            },
            "rss_samples": rss_samples,
            "disk_io_samples": disk_io_samples,
            "rss": rss,
            "log_scan": logs,
        });

        self.write_summary()?;
        self.stop_server()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }

    async fn multipart_file_benchmark(
        &self,
        token: &str,
        object_key: &str,
        path: &Path,
        total_size: usize,
        part_size: usize,
    ) -> ProofResult<Value> {
        let expected_parts = total_size.div_ceil(part_size);
        let params = json!({
            "content_type": "application/octet-stream",
            "total_size_bytes": total_size,
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
                "multipart benchmark init failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let init: MultipartInit = response.json().await?;

        let mut part_samples = Vec::new();
        for idx in 0..expected_parts {
            let offset = idx * part_size;
            let len = total_size.saturating_sub(offset).min(part_size);
            let mut file = tokio::fs::File::open(path).await?;
            file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
            let reader = file.take(len as u64);
            let stream = ReaderStream::with_capacity(reader, 1024 * 1024);
            let started = Instant::now();
            let response = self
                .client
                .put(format!(
                    "{}/n1/upload/multipart/{}/{}?durability=batched",
                    self.base_url, init.version_id, idx
                ))
                .bearer_auth(token)
                .header("content-type", "application/octet-stream")
                .header(CONTENT_LENGTH, len)
                .body(reqwest::Body::wrap_stream(stream))
                .send()
                .await?;
            if response.status() != StatusCode::OK && response.status() != StatusCode::CREATED {
                return Err(format!(
                    "multipart benchmark part {idx} failed: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
            let elapsed = started.elapsed();
            part_samples.push(json!({
                "part": idx,
                "bytes": len,
                "elapsed_s": round3(elapsed.as_secs_f64()),
                "mib_per_s": round3(mbps(len, elapsed)),
            }));
        }

        let finalize_started = Instant::now();
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
                "multipart benchmark finalize failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let finalize_elapsed = finalize_started.elapsed();

        Ok(json!({
            "object_key": object_key,
            "bytes": total_size,
            "parts": expected_parts,
            "part_size_bytes": part_size,
            "version_id": init.version_id,
            "session_id": init.session_id,
            "part_samples": part_samples,
            "finalize_elapsed_s": round3(finalize_elapsed.as_secs_f64()),
        }))
    }

    async fn upload_file_one_shot_benchmark(
        &self,
        token: &str,
        object_key: &str,
        path: &Path,
        len: usize,
        content_type: &str,
    ) -> ProofResult<String> {
        let mut file = tokio::fs::File::open(path).await?;
        file.seek(std::io::SeekFrom::Start(0)).await?;
        let reader = file.take(len as u64);
        let stream = ReaderStream::with_capacity(reader, NATIVE_UPLOAD_COPY_BUFFER_BYTES);
        let response = self
            .client
            .post(format!(
                "{}/n1/upload/one-shot/{}",
                self.base_url,
                encode_path(object_key)
            ))
            .bearer_auth(token)
            .header("content-type", content_type)
            .header(CONTENT_LENGTH, len)
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "one-shot file benchmark upload failed for {object_key}: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let body: Value = response.json().await?;
        if body.get("object_key").and_then(Value::as_str) != Some(object_key) {
            return Err(format!("one-shot logical path mismatch for {object_key}: {body}").into());
        }
        Ok(body
            .get("version_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string())
    }

    async fn multipart_file_benchmark_pipelined(
        &self,
        token: &str,
        object_key: &str,
        path: &Path,
        total_size: usize,
        part_size: usize,
        concurrency: usize,
    ) -> ProofResult<Value> {
        let expected_parts = total_size.div_ceil(part_size);
        let params = json!({
            "content_type": "application/octet-stream",
            "total_size_bytes": total_size,
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
                "multipart pipelined benchmark init failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let init: MultipartInit = response.json().await?;

        let mut part_samples = Vec::new();
        let concurrency = concurrency.max(1);
        for batch_start in (0..expected_parts).step_by(concurrency) {
            let batch_end = (batch_start + concurrency).min(expected_parts);
            let futures = (batch_start..batch_end).map(|idx| {
                let client = self.client.clone();
                let base_url = self.base_url.clone();
                let token = token.to_string();
                let version_id = init.version_id.clone();
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
                            "{base_url}/n1/upload/multipart/{version_id}/{idx}?durability=batched"
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
                            "multipart pipelined benchmark part {idx} failed: {} {}",
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

        let finalize_started = Instant::now();
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
                "multipart pipelined benchmark finalize failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let finalize_elapsed = finalize_started.elapsed();

        Ok(json!({
            "object_key": object_key,
            "bytes": total_size,
            "parts": expected_parts,
            "part_size_bytes": part_size,
            "concurrency": concurrency,
            "version_id": init.version_id,
            "session_id": init.session_id,
            "part_samples": part_samples,
            "finalize_elapsed_s": round3(finalize_elapsed.as_secs_f64()),
        }))
    }

    async fn native_multipart_file_benchmark(
        &self,
        stream: &mut TcpStream,
        object_key: &str,
        path: &Path,
        total_size: usize,
        part_size: usize,
        sample_index: usize,
    ) -> ProofResult<Value> {
        let expected_parts = total_size.div_ceil(part_size);
        let init = native_round_trip(
            stream,
            &format!("native-multipart-init-{sample_index}"),
            "multipart.init",
            json!({
                "object_key": object_key,
                "content_type": "application/octet-stream",
                "total_size_bytes": total_size,
                "expected_parts": expected_parts,
                "part_size_bytes": part_size,
            }),
            None,
        )
        .await?;
        ensure_native_status(&init, 201, "native multipart init benchmark")?;
        let init_body = native_json_body(&init)?;
        let version_id = init_body
            .get("version_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("native multipart init missing version_id: {init_body}"))?
            .to_string();
        let session_id = init_body
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("native multipart init missing session_id: {init_body}"))?
            .to_string();

        let mut part_samples = Vec::new();
        for idx in 0..expected_parts {
            let offset = idx * part_size;
            let len = total_size.saturating_sub(offset).min(part_size);
            let started = Instant::now();
            let response = native_round_trip_file_range(
                stream,
                &format!("native-multipart-part-{sample_index}-{idx}"),
                "multipart.put_part",
                json!({
                    "version_id": version_id,
                    "part_index": idx,
                    "durability": "batched",
                }),
                path,
                offset as u64,
                len,
            )
            .await?;
            ensure_native_status(&response, 200, "native multipart part benchmark")?;
            let elapsed = started.elapsed();
            part_samples.push(json!({
                "part": idx,
                "bytes": len,
                "elapsed_s": round3(elapsed.as_secs_f64()),
                "mib_per_s": round3(mbps(len, elapsed)),
            }));
        }

        let finalize_started = Instant::now();
        let finalize = native_round_trip(
            stream,
            &format!("native-multipart-finalize-{sample_index}"),
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
        ensure_native_status(&finalize, 200, "native multipart finalize benchmark")?;
        let finalize_elapsed = finalize_started.elapsed();

        Ok(json!({
            "object_key": object_key,
            "bytes": total_size,
            "parts": expected_parts,
            "part_size_bytes": part_size,
            "version_id": version_id,
            "session_id": session_id,
            "part_samples": part_samples,
            "finalize_elapsed_s": round3(finalize_elapsed.as_secs_f64()),
        }))
    }

    async fn native_multipart_file_benchmark_pipelined(
        &self,
        object_key: &str,
        path: &Path,
        total_size: usize,
        part_size: usize,
        sample_index: usize,
        concurrency: usize,
    ) -> ProofResult<Value> {
        let expected_parts = total_size.div_ceil(part_size);
        let mut control_stream = self
            .open_native_upload_benchmark_stream(&format!(
                "native-multipart-pipelined-control-{sample_index}"
            ))
            .await?;
        let init = native_round_trip(
            &mut control_stream,
            &format!("native-multipart-pipelined-init-{sample_index}"),
            "multipart.init",
            json!({
                "object_key": object_key,
                "content_type": "application/octet-stream",
                "total_size_bytes": total_size,
                "expected_parts": expected_parts,
                "part_size_bytes": part_size,
            }),
            None,
        )
        .await?;
        ensure_native_status(&init, 201, "native multipart pipelined init benchmark")?;
        let init_body = native_json_body(&init)?;
        let version_id = init_body
            .get("version_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!("native multipart pipelined init missing version_id: {init_body}")
            })?
            .to_string();
        let session_id = init_body
            .get("session_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!("native multipart pipelined init missing session_id: {init_body}")
            })?
            .to_string();

        let concurrency = concurrency.max(1);
        let mut streams = Vec::new();
        for slot in 0..concurrency {
            streams.push(Some(
                self.open_native_upload_benchmark_stream(&format!(
                    "native-multipart-pipelined-worker-{sample_index}-{slot}"
                ))
                .await?,
            ));
        }

        let mut part_samples = Vec::new();
        for batch_start in (0..expected_parts).step_by(concurrency) {
            let batch_end = (batch_start + concurrency).min(expected_parts);
            let futures = (batch_start..batch_end).enumerate().map(|(slot, idx)| {
                let mut stream = streams[slot].take().expect("native worker stream present");
                let version_id = version_id.clone();
                let path = path.to_path_buf();
                async move {
                    let offset = idx * part_size;
                    let len = total_size.saturating_sub(offset).min(part_size);
                    let started = Instant::now();
                    let response = native_round_trip_file_range(
                        &mut stream,
                        &format!("native-multipart-pipelined-part-{sample_index}-{idx}"),
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
                    ensure_native_status(
                        &response,
                        200,
                        "native multipart pipelined part benchmark",
                    )?;
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

        let finalize_started = Instant::now();
        let finalize = native_round_trip(
            &mut control_stream,
            &format!("native-multipart-pipelined-finalize-{sample_index}"),
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
        ensure_native_status(&finalize, 200, "native multipart pipelined finalize benchmark")?;
        let finalize_elapsed = finalize_started.elapsed();

        Ok(json!({
            "object_key": object_key,
            "bytes": total_size,
            "parts": expected_parts,
            "part_size_bytes": part_size,
            "concurrency": concurrency,
            "version_id": version_id,
            "session_id": session_id,
            "part_samples": part_samples,
            "finalize_elapsed_s": round3(finalize_elapsed.as_secs_f64()),
        }))
    }

    async fn open_native_upload_benchmark_stream(&self, label: &str) -> ProofResult<TcpStream> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.native_port())).await?;
        stream.set_nodelay(true)?;
        ensure_native_ok(
            &native_round_trip(
                &mut stream,
                &format!("{label}-auth"),
                "auth.fragment",
                json!({
                    "fragment": self.fragment,
                    "secret": self.args.fragment_key,
                    "read": true,
                    "write": true,
                    "modify": true,
                    "delete": true,
                    "expires_in": 21600,
                }),
                None,
            )
            .await?,
            "native upload benchmark worker auth",
        )?;
        Ok(stream)
    }
}

fn create_benchmark_file(path: &Path, size: usize, seed: &[u8]) -> ProofResult<()> {
    let mut file = fs::File::create(path)?;
    let chunk = deterministic_payload(1024 * 1024, seed);
    let mut remaining = size;
    while remaining > 0 {
        let write_len = remaining.min(chunk.len());
        file.write_all(&chunk[..write_len])?;
        remaining -= write_len;
    }
    file.sync_all()?;
    Ok(())
}

fn upload_bench_sample_json(
    object_key: &str,
    version_id: Option<&str>,
    bytes: usize,
    elapsed: Duration,
    detail: Option<Value>,
) -> Value {
    json!({
        "object_key": object_key,
        "version_id": version_id,
        "bytes": bytes,
        "elapsed_s": round3(elapsed.as_secs_f64()),
        "mib_per_s": round3(mbps(bytes, elapsed)),
        "detail": detail,
    })
}

fn upload_benchmark_stats(kind: &str, bytes: usize, samples: &[Value]) -> Value {
    let mut elapsed = Vec::new();
    let mut speeds = Vec::new();
    for sample in samples {
        if let Some(value) = sample.get("elapsed_s").and_then(Value::as_f64) {
            elapsed.push(value);
        }
        if let Some(value) = sample.get("mib_per_s").and_then(Value::as_f64) {
            speeds.push(value);
        }
    }
    json!({
        "kind": kind,
        "bytes_per_sample": bytes,
        "sample_count": samples.len(),
        "elapsed_s": numeric_stats(&elapsed),
        "mib_per_s": numeric_stats(&speeds),
        "samples": samples,
    })
}

fn numeric_stats(values: &[f64]) -> Value {
    if values.is_empty() {
        return json!({"min": null, "avg": null, "max": null});
    }
    let min = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let avg = values.iter().copied().sum::<f64>() / values.len() as f64;
    json!({
        "min": round3(min),
        "avg": round3(avg),
        "max": round3(max),
    })
}

fn mbps(bytes: usize, elapsed: Duration) -> f64 {
    bytes as f64 / 1_048_576.0 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
}

#[derive(Clone, Debug)]
struct DiskIoSnapshot {
    entries: Vec<DiskIoEntry>,
}

#[derive(Clone, Debug)]
struct DiskIoEntry {
    root: String,
    device: Option<String>,
    stats: Option<DiskStats>,
}

#[derive(Clone, Copy, Debug)]
struct DiskStats {
    read_ios: u64,
    read_sectors: u64,
    read_ms: u64,
    write_ios: u64,
    write_sectors: u64,
    write_ms: u64,
    io_ms: u64,
    weighted_io_ms: u64,
}

fn effective_upload_benchmark_concurrency(requested: usize, active_disk_count: usize) -> usize {
    if requested > 0 {
        return requested.clamp(1, 16);
    }
    match active_disk_count {
        0..=2 => 1,
        3 | 4 => 2,
        _ => active_disk_count.min(4),
    }
}

fn upload_benchmark_concurrency_policy(
    requested: usize,
    active_disk_count: usize,
    effective: usize,
) -> Value {
    json!({
        "requested": if requested == 0 { Value::Null } else { json!(requested) },
        "effective": effective,
        "active_disk_count": active_disk_count,
        "policy": if requested == 0 {
            "auto: minimum hardware avoids client-side multipart fan-out; explicit values override"
        } else {
            "explicit: operator-requested benchmark concurrency"
        },
    })
}

fn capture_disk_io(roots: &[PathBuf]) -> DiskIoSnapshot {
    let proc_stats = read_proc_diskstats();
    let entries = roots
        .iter()
        .map(|root| {
            let root_display = root.display().to_string();
            let device = device_name_for_root(root);
            let stats = device.as_ref().and_then(|device| proc_stats.get(device).copied());
            DiskIoEntry {
                root: root_display,
                device,
                stats,
            }
        })
        .collect();
    DiskIoSnapshot { entries }
}

fn disk_io_delta(label: &str, before: &DiskIoSnapshot, after: &DiskIoSnapshot) -> Value {
    let roots = before
        .entries
        .iter()
        .zip(after.entries.iter())
        .map(|(before, after)| {
            let delta = before.stats.zip(after.stats).map(|(a, b)| {
                let write_sectors = b.write_sectors.saturating_sub(a.write_sectors);
                let read_sectors = b.read_sectors.saturating_sub(a.read_sectors);
                json!({
                    "read_ios": b.read_ios.saturating_sub(a.read_ios),
                    "read_bytes": read_sectors.saturating_mul(512),
                    "read_ms": b.read_ms.saturating_sub(a.read_ms),
                    "write_ios": b.write_ios.saturating_sub(a.write_ios),
                    "write_bytes": write_sectors.saturating_mul(512),
                    "write_mib": round3(write_sectors as f64 * 512.0 / 1_048_576.0),
                    "write_ms": b.write_ms.saturating_sub(a.write_ms),
                    "io_ms": b.io_ms.saturating_sub(a.io_ms),
                    "weighted_io_ms": b.weighted_io_ms.saturating_sub(a.weighted_io_ms),
                })
            });
            json!({
                "root": before.root,
                "device": before.device.as_ref().or(after.device.as_ref()),
                "delta": delta,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "label": label,
        "roots": roots,
    })
}

fn read_proc_diskstats() -> std::collections::BTreeMap<String, DiskStats> {
    let Ok(text) = fs::read_to_string("/proc/diskstats") else {
        return std::collections::BTreeMap::new();
    };
    text.lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 14 {
                return None;
            }
            let name = fields.get(2)?.to_string();
            let parse = |idx: usize| fields.get(idx)?.parse::<u64>().ok();
            Some((
                name,
                DiskStats {
                    read_ios: parse(3)?,
                    read_sectors: parse(5)?,
                    read_ms: parse(6)?,
                    write_ios: parse(7)?,
                    write_sectors: parse(9)?,
                    write_ms: parse(10)?,
                    io_ms: parse(12)?,
                    weighted_io_ms: parse(13)?,
                },
            ))
        })
        .collect()
}

fn device_name_for_root(root: &Path) -> Option<String> {
    let output = Command::new("df").arg("-P").arg(root).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let device = stdout.lines().nth(1)?.split_whitespace().next()?;
    let path = PathBuf::from(device);
    let canonical = fs::canonicalize(&path).unwrap_or(path);
    canonical
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
}
