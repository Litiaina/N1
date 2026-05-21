impl Proof {
    async fn run_native_transport_benchmark(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("native-bench").await?;
        self.create_fragment().await?;
        self.auth().await?;

        let bench_prefix = format!("{}native-bench", self.args.prefix);
        let bench_object_count = 64usize;
        for idx in 0..bench_object_count {
            let object_key = format!("{bench_prefix}/object-{idx:04}.txt");
            let payload = deterministic_payload(1024, object_key.as_bytes());
            self.upload_bytes(&object_key, payload, "text/plain").await?;
        }
        let object_key = format!("{bench_prefix}/object-0000.txt");
        self.assert_list_contains(&object_key).await?;

        let download_bytes = self.args.download_benchmark_bytes.max(1);
        let download_part_bytes = self.args.download_benchmark_part_bytes.max(1);
        if download_part_bytes as u64 > self.args.max_part_size {
            return Err(format!(
                "download benchmark part size {} exceeds configured max_part_size {}",
                download_part_bytes, self.args.max_part_size
            )
            .into());
        }
        let download_source = self.args.out_dir.join("native-transport-download-source.bin");
        create_benchmark_file(
            &download_source,
            download_bytes,
            b"n1-native-transport-download-source",
        )?;
        let download_sha = self.sha256_file(&download_source).await?;
        let download_key = format!("{bench_prefix}/download-stream.bin");
        let download_seed = self
            .multipart_file_benchmark(
                &self.token,
                &download_key,
                &download_source,
                download_bytes,
                download_part_bytes,
            )
            .await?;
        self.assert_download_sha256(&download_key, &download_sha).await?;

        let mut stream = TcpStream::connect(("127.0.0.1", self.native_port())).await?;
        stream.set_nodelay(true)?;
        ensure_native_ok(
            &native_round_trip(&mut stream, "bench-ping", "ping", json!({}), None).await?,
            "native benchmark ping",
        )?;
        ensure_native_ok(
            &native_round_trip(
                &mut stream,
                "bench-auth",
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
            "native benchmark auth",
        )?;

        let iterations = self.args.benchmark_iterations.max(1);
        let warmup = self.args.benchmark_warmup;
        let stat_path = format!(
            "/n1/posix/stat?path={}",
            encode_query_value(&object_key)
        );
        let list_path = format!(
            "/n1/posix/list?path={}&limit=1000",
            encode_query_value(&bench_prefix)
        );

        let http_ping = self
            .benchmark_http_get("http_ping", "/n1/ping", iterations, warmup)
            .await?;
        let native_ping = self
            .benchmark_native_op(
                &mut stream,
                "native_ping",
                "ping",
                json!({}),
                iterations,
                warmup,
            )
            .await?;
        let http_stat = self
            .benchmark_http_get("http_posix_stat", &stat_path, iterations, warmup)
            .await?;
        let native_typed_stat = self
            .benchmark_native_op(
                &mut stream,
                "native_typed_namespace_stat",
                "namespace.stat",
                json!({"path": object_key}),
                iterations,
                warmup,
            )
            .await?;
        let http_list = self
            .benchmark_http_get("http_posix_list", &list_path, iterations, warmup)
            .await?;
        let native_typed_list = self
            .benchmark_native_op(
                &mut stream,
                "native_typed_namespace_list",
                "namespace.list",
                json!({"path": bench_prefix, "limit": 1000}),
                iterations,
                warmup,
            )
            .await?;
        let download_iterations = self.args.download_benchmark_iterations.max(1);
        let download_warmup = self.args.download_benchmark_warmup;
        let http_download = self
            .benchmark_http_download(
                "http_object_download_stream",
                &download_key,
                &download_sha,
                download_iterations,
                download_warmup,
            )
            .await?;
        let native_download = self
            .benchmark_native_stream_download(
                &mut stream,
                "native_object_download_stream",
                &download_key,
                &download_sha,
                download_iterations,
                download_warmup,
            )
            .await?;

        let rss = self.rss_sample("after_native_transport_benchmark");
        self.summary = json!({
            "status": "PASS",
            "proof": "n1_native_transport_benchmark",
            "iterations": iterations,
            "warmup": warmup,
            "download_iterations": download_iterations,
            "download_warmup": download_warmup,
            "object_count": bench_object_count,
            "download_object": {
                "object_key": download_key,
                "bytes": download_bytes,
                "part_size_bytes": download_part_bytes,
                "sha256": download_sha,
                "seed_upload": download_seed,
            },
            "fragment_hash": self.fragment,
            "http_base_url": self.base_url,
            "native_url": format!("n1://127.0.0.1:{}", self.native_port()),
            "benchmarks": [
                http_ping,
                native_ping,
                http_stat,
                native_typed_stat,
                http_list,
                native_typed_list,
                http_download,
                native_download,
            ],
            "rss": rss,
        });
        self.write_summary()?;
        self.stop_server()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }

    async fn benchmark_http_get(
        &self,
        name: &str,
        path: &str,
        iterations: usize,
        warmup: usize,
    ) -> ProofResult<Value> {
        eprintln!("N1_NATIVE_BENCH start {name}");
        for _ in 0..warmup {
            let _ = self.http_get_ok(path).await?;
        }

        let mut samples = Vec::with_capacity(iterations);
        let mut total_body_bytes = 0usize;
        let total_start = Instant::now();
        for _ in 0..iterations {
            let start = Instant::now();
            total_body_bytes = total_body_bytes.saturating_add(self.http_get_ok(path).await?);
            samples.push(start.elapsed());
        }
        let stats = transport_benchmark_stats(
            name,
            iterations,
            total_start.elapsed(),
            samples,
            total_body_bytes,
        );
        eprintln!("N1_NATIVE_BENCH done {name}");
        Ok(stats)
    }

    async fn http_get_ok(&self, path: &str) -> ProofResult<usize> {
        let request = self.client.get(format!("{}{}", self.base_url, path));
        let request = if path == "/n1/ping" {
            request
        } else {
            request.bearer_auth(&self.token)
        };
        let response = request.send().await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "benchmark HTTP GET {path} failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response.bytes().await?.len())
    }

    async fn benchmark_native_op(
        &self,
        stream: &mut TcpStream,
        name: &str,
        op: &str,
        payload: Value,
        iterations: usize,
        warmup: usize,
    ) -> ProofResult<Value> {
        eprintln!("N1_NATIVE_BENCH start {name}");
        for idx in 0..warmup {
            let id = format!("{name}-warmup-{idx}");
            ensure_native_ok(
                &native_round_trip(stream, &id, op, payload.clone(), None).await?,
                name,
            )?;
        }

        let mut samples = Vec::with_capacity(iterations);
        let mut total_body_bytes = 0usize;
        let total_start = Instant::now();
        for idx in 0..iterations {
            let id = format!("{name}-{idx}");
            let start = Instant::now();
            let response = native_round_trip(stream, &id, op, payload.clone(), None).await?;
            samples.push(start.elapsed());
            ensure_native_ok(&response, name)?;
            total_body_bytes =
                total_body_bytes.saturating_add(response.body.as_ref().map_or(0, |body| body.len()));
        }
        let stats = transport_benchmark_stats(
            name,
            iterations,
            total_start.elapsed(),
            samples,
            total_body_bytes,
        );
        eprintln!("N1_NATIVE_BENCH done {name}");
        Ok(stats)
    }

    async fn benchmark_http_download(
        &self,
        name: &str,
        object_key: &str,
        expected_sha: &str,
        iterations: usize,
        warmup: usize,
    ) -> ProofResult<Value> {
        eprintln!("N1_NATIVE_BENCH start {name}");
        for _ in 0..warmup {
            let _ = self.http_download_sha(object_key).await?;
        }

        let mut samples = Vec::with_capacity(iterations);
        let mut total_body_bytes = 0usize;
        let total_start = Instant::now();
        for _ in 0..iterations {
            let start = Instant::now();
            let (bytes, sha) = self.http_download_sha(object_key).await?;
            if sha != expected_sha {
                return Err(format!(
                    "HTTP download benchmark sha mismatch for {object_key}: got {sha} expected {expected_sha}"
                )
                .into());
            }
            total_body_bytes = total_body_bytes.saturating_add(bytes);
            samples.push(start.elapsed());
        }
        let stats = transport_benchmark_stats(
            name,
            iterations,
            total_start.elapsed(),
            samples,
            total_body_bytes,
        );
        eprintln!("N1_NATIVE_BENCH done {name}");
        Ok(stats)
    }

    async fn http_download_sha(&self, object_key: &str) -> ProofResult<(usize, String)> {
        let response = self
            .client
            .get(format!(
                "{}/n1/objects/download?object_key={}",
                self.base_url,
                encode_query_value(object_key)
            ))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "HTTP download benchmark failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let mut stream = response.bytes_stream();
        let mut hasher = Sha256::new();
        let mut bytes = 0usize;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            bytes = bytes.saturating_add(chunk.len());
            hasher.update(&chunk);
        }
        Ok((bytes, format!("{:x}", hasher.finalize())))
    }

    async fn benchmark_native_stream_download(
        &self,
        stream: &mut TcpStream,
        name: &str,
        object_key: &str,
        expected_sha: &str,
        iterations: usize,
        warmup: usize,
    ) -> ProofResult<Value> {
        eprintln!("N1_NATIVE_BENCH start {name}");
        for idx in 0..warmup {
            let id = format!("{name}-warmup-{idx}");
            let _ = native_round_trip_stream_hash(
                stream,
                &id,
                "object.download_stream",
                json!({"object_key": object_key}),
            )
            .await?;
        }

        let mut samples = Vec::with_capacity(iterations);
        let mut total_body_bytes = 0usize;
        let total_start = Instant::now();
        for idx in 0..iterations {
            let id = format!("{name}-{idx}");
            let start = Instant::now();
            let response = native_round_trip_stream_hash(
                stream,
                &id,
                "object.download_stream",
                json!({"object_key": object_key}),
            )
            .await?;
            samples.push(start.elapsed());
            ensure_native_status(&response.response, 200, name)?;
            if response.sha256 != expected_sha {
                return Err(format!(
                    "native download benchmark sha mismatch for {object_key}: got {} expected {}",
                    response.sha256, expected_sha
                )
                .into());
            }
            total_body_bytes = total_body_bytes.saturating_add(response.bytes_read);
        }
        let stats = transport_benchmark_stats(
            name,
            iterations,
            total_start.elapsed(),
            samples,
            total_body_bytes,
        );
        eprintln!("N1_NATIVE_BENCH done {name}");
        Ok(stats)
    }

}

fn transport_benchmark_stats(
    name: &str,
    iterations: usize,
    elapsed: Duration,
    mut samples: Vec<Duration>,
    total_body_bytes: usize,
) -> Value {
    samples.sort_unstable();
    let total_us: u128 = samples.iter().map(Duration::as_micros).sum();
    let avg_us = total_us as f64 / iterations as f64;
    let elapsed_secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    json!({
        "name": name,
        "iterations": iterations,
        "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
        "ops_per_sec": iterations as f64 / elapsed_secs,
        "latency_us": {
            "min": percentile_us(&samples, 0.0),
            "avg": avg_us,
            "p50": percentile_us(&samples, 50.0),
            "p95": percentile_us(&samples, 95.0),
            "p99": percentile_us(&samples, 99.0),
            "max": percentile_us(&samples, 100.0),
        },
        "response_bytes_total": total_body_bytes,
        "response_bytes_avg": total_body_bytes as f64 / iterations as f64,
    })
}

fn percentile_us(samples: &[Duration], percentile: f64) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let max_idx = samples.len() - 1;
    let idx = ((percentile / 100.0) * max_idx as f64).round() as usize;
    samples[idx.min(max_idx)].as_micros()
}
