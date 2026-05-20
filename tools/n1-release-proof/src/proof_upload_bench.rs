use std::io::Write as IoWrite;

impl Proof {
    async fn run_upload_benchmark(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("upload-benchmark").await?;
        self.create_fragment().await?;
        self.auth().await?;

        let repeats = self.args.upload_benchmark_repeats.max(1);
        let one_shot_bytes = self.args.one_shot_benchmark_bytes;
        let multipart_bytes = self.args.multipart_benchmark_bytes;
        let multipart_part_bytes = self.args.multipart_benchmark_part_bytes.max(1);

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

        let mut one_shot_samples = Vec::new();
        for idx in 0..repeats {
            let object_key = format!("{}bench/one-shot/object-{idx:04}.bin", self.args.prefix);
            let seed = format!("n1-one-shot-benchmark-{idx}");
            let payload = deterministic_payload(one_shot_bytes, seed.as_bytes());
            let started = Instant::now();
            let version_id = self
                .upload_bytes(&object_key, payload, "application/octet-stream")
                .await?;
            let elapsed = started.elapsed();
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

        let multipart_file = self.args.out_dir.join("multipart-benchmark-source.bin");
        create_benchmark_file(
            &multipart_file,
            multipart_bytes,
            b"n1-multipart-upload-benchmark-source",
        )?;

        let mut multipart_samples = Vec::new();
        for idx in 0..repeats {
            let object_key = format!("{}bench/multipart/object-{idx:04}.bin", self.args.prefix);
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
            let seed = format!("n1-native-one-shot-benchmark-{idx}");
            let payload = deterministic_payload(one_shot_bytes, seed.as_bytes());
            let started = Instant::now();
            let response = native_round_trip(
                &mut native_stream,
                &format!("native-one-shot-{idx}"),
                "upload.one_shot",
                json!({
                    "object_key": object_key,
                    "content_type": "application/octet-stream",
                    "durability": "batched",
                }),
                Some(payload),
            )
            .await?;
            ensure_native_status(&response, 201, "native one-shot upload benchmark")?;
            let elapsed = started.elapsed();
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

        let mut native_multipart_samples = Vec::new();
        for idx in 0..repeats {
            let object_key = format!(
                "{}bench/native-multipart/object-{idx:04}.bin",
                self.args.prefix
            );
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
        let rss = self.rss_sample("after_upload_benchmark");

        self.summary = json!({
            "status": "PASS",
            "proof": "n1_upload_benchmark",
            "stamp": self.stamp,
            "fragment_hash": self.fragment,
            "base_url": self.base_url,
            "disk_roots": self.disk_roots()?.iter().map(|path| path.display().to_string()).collect::<Vec<_>>(),
            "active_disk_count": self.active_disk_count,
            "quota_backend": self.args.quota_backend,
            "quota_bytes": self.args.quota_bytes,
            "max_part_size": self.args.max_part_size,
            "repeats": repeats,
            "one_shot": one_shot_stats,
            "multipart": {
                "part_size_bytes": multipart_part_bytes,
                "expected_parts": multipart_bytes.div_ceil(multipart_part_bytes),
                "stats": multipart_stats,
            },
            "native_one_shot": native_one_shot_stats,
            "native_multipart": {
                "part_size_bytes": multipart_part_bytes,
                "expected_parts": multipart_bytes.div_ceil(multipart_part_bytes),
                "stats": native_multipart_stats,
            },
            "rss": rss,
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
            let mut file = tokio::fs::File::open(path).await?;
            file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
            let mut body = vec![0u8; len];
            file.read_exact(&mut body).await?;
            let started = Instant::now();
            let response = native_round_trip(
                stream,
                &format!("native-multipart-part-{sample_index}-{idx}"),
                "multipart.put_part",
                json!({
                    "version_id": version_id,
                    "part_index": idx,
                    "durability": "batched",
                }),
                Some(body),
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
