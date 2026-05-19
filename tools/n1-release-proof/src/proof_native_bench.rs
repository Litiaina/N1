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
        let native_api_stat = self
            .benchmark_native_api_get(
                &mut stream,
                "native_api_posix_stat",
                &stat_path,
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
        let native_api_list = self
            .benchmark_native_api_get(
                &mut stream,
                "native_api_posix_list",
                &list_path,
                iterations,
                warmup,
            )
            .await?;

        let rss = self.rss_sample("after_native_transport_benchmark");
        self.summary = json!({
            "status": "PASS",
            "proof": "n1_native_transport_benchmark",
            "iterations": iterations,
            "warmup": warmup,
            "object_count": bench_object_count,
            "fragment_hash": self.fragment,
            "http_base_url": self.base_url,
            "native_url": format!("n1://127.0.0.1:{}", self.native_port()),
            "benchmarks": [
                http_ping,
                native_ping,
                http_stat,
                native_typed_stat,
                native_api_stat,
                http_list,
                native_typed_list,
                native_api_list,
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
                total_body_bytes.saturating_add(response.body.as_ref().map_or(0, Vec::len));
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

    async fn benchmark_native_api_get(
        &self,
        stream: &mut TcpStream,
        name: &str,
        path: &str,
        iterations: usize,
        warmup: usize,
    ) -> ProofResult<Value> {
        self.benchmark_native_op(
            stream,
            name,
            "api.request",
            json!({
                "method": "GET",
                "path": path,
            }),
            iterations,
            warmup,
        )
        .await
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
