// N1 4 proofs operate only inside prepare_dirs()'s unique isolated roots.
impl Proof {
    async fn flat_direct_upload_regression(&self) -> ProofResult<Vec<(String, String)>> {
        use tokio::io::AsyncWriteExt;
        let mut stream = TcpStream::connect(("127.0.0.1", self.native_port())).await?;
        ensure_native_ok(&native_round_trip(&mut stream, "flat-auth", "auth.fragment",
            json!({"fragment":self.fragment,"secret":self.args.fragment_key,
                "read":true,"write":true,"modify":true,"delete":true,"expires_in":3600}), None).await?,
            "flat native auth")?;
        let mut samples = Vec::new();
        for embedded in [false, true] {
            let key = format!("direct/native-{}.bin", if embedded { "embedded" } else { "stream" });
            let bytes = deterministic_payload(8192, key.as_bytes());
            let hash = sha256_bytes(&bytes);
            let payload = json!({"object_key":key,"content_type":"application/octet-stream","durability":"batched"});
            let response = if embedded {
                let request = NativeProofRequest {
                    id: "flat-embedded".into(), op: "upload.one_shot".into(),
                    payload: serde_json::to_string(&payload)?,
                    body: Some(serde_bytes::ByteBuf::from(bytes)), body_len: None,
                };
                let encoded = rmp_serde::to_vec(&request)?;
                stream.write_all(&(encoded.len() as u32).to_be_bytes()).await?;
                stream.write_all(&encoded).await?;
                stream.flush().await?;
                read_native_response(&mut stream, "flat-embedded").await?
            } else {
                native_round_trip(&mut stream, "flat-stream", "upload.one_shot", payload, Some(bytes)).await?
            };
            ensure_native_status(&response, 201, "flat native one-shot publication")?;
            let metadata = native_json_body(&response)?;
            if metadata["parts"] != json!([]) {
                return Err("native upload exposed internal placement metadata".into());
            }
            self.assert_download_sha256(&key, &hash).await?;
            samples.push((key, hash));
        }
        Ok(samples)
    }

    fn flat_ncx_paths(&self, key: &str) -> Vec<PathBuf> {
        let mut parts = key.split('/').peekable();
        let mut relative = PathBuf::new();
        while let Some(part) = parts.next() {
            relative.push(format!(
                "{part}{}",
                if parts.peek().is_some() {
                    ".ncp"
                } else {
                    ".ncx"
                }
            ));
        }
        self.index_roots
            .iter()
            .map(|root| {
                physical_fragment_root(root, &self.stable_fragment_id)
                    .join(".namespace")
                    .join(&relative)
            })
            .filter(|p| p.is_file())
            .collect()
    }

    fn flat_immutable_digest(&self, roots: &[PathBuf]) -> ProofResult<String> {
        let mut records = std::collections::BTreeMap::new();
        for (slot, root) in roots.iter().enumerate() {
            let mut stack = vec![root.clone()];
            while let Some(dir) = stack.pop() {
                for entry in fs::read_dir(&dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if entry.file_type()?.is_dir() {
                        stack.push(path);
                        continue;
                    }
                    if !entry.file_type()?.is_file() {
                        continue;
                    }
                    let relative = path.strip_prefix(root)?;
                    if relative.components().any(|p| p.as_os_str() == ".objects")
                        && matches!(
                            path.extension().and_then(|e| e.to_str()),
                            Some("ndx" | "shard")
                        )
                    {
                        records.insert(
                            format!("{slot}/{}", relative.display()),
                            sha256_bytes(&fs::read(&path)?),
                        );
                    }
                }
            }
        }
        Ok(sha256_bytes(&serde_json::to_vec(&records)?))
    }

    async fn flat_list_all(
        &self,
        path: &str,
        limit: usize,
    ) -> ProofResult<(Vec<String>, Vec<f64>)> {
        let mut cursor: Option<String> = None;
        let mut keys = Vec::new();
        let mut latency = Vec::new();
        loop {
            let mut query = vec![("path", path.to_string()), ("limit", limit.to_string())];
            if let Some(ref token) = cursor {
                query.push(("continuation_token", token.clone()));
            }
            let started = Instant::now();
            let response = self
                .client
                .get(format!("{}/noa/v1/posix/list", self.base_url))
                .bearer_auth(&self.token)
                .query(&query)
                .send()
                .await?;
            if !response.status().is_success() {
                return Err(format!(
                    "flat LIST: {} {}",
                    response.status(),
                    response.text().await?
                )
                .into());
            }
            let body: Value = response.json().await?;
            latency.push(started.elapsed().as_secs_f64() * 1000.0);
            for entry in body["entries"].as_array().ok_or("LIST has no entries")? {
                keys.push(
                    entry["path"]
                        .as_str()
                        .ok_or("LIST entry has no path")?
                        .to_string(),
                );
            }
            let next = body["continuation_token"].as_str().map(str::to_string);
            if next == cursor && next.is_some() {
                return Err("LIST cursor did not advance".into());
            }
            cursor = next;
            if cursor.is_none() {
                break;
            }
        }
        if !keys.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err("LIST duplicate or out-of-order key".into());
        }
        Ok((keys, latency))
    }

    async fn run_flat_namespace_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        if self.data_shards != 3 || self.parity_shards != 1 || self.active_disk_count != 4 {
            return Err("flat namespace hardware proof requires four members, 3+1".into());
        }
        self.start_server("flat-initial").await?;
        self.create_fragment().await?;
        self.auth().await?;
        // Batch and native direct paths must preserve internal placement until
        // NCX publication, while public replies must still omit shard paths.
        let mut batch_keys = BTreeSet::new();
        let batch = self.concurrent_standard_batch_upload_proof(&mut batch_keys).await?;
        self.assert_expected_visible(&batch_keys).await?;
        let direct_samples = self.flat_direct_upload_regression().await?;
        let payload = deterministic_payload(1024 * 1024, b"n1-v4-flat-proof");
        let checksum = sha256_bytes(&payload);
        self.upload_bytes("flat/item", payload.clone(), "application/octet-stream")
            .await?;
        self.upload_bytes("flat/item/child", payload, "application/octet-stream")
            .await?;
        self.assert_download_sha256("flat/item", &checksum).await?;
        self.assert_download_sha256("flat/item/child", &checksum)
            .await?;
        self.posix_post("/noa/v1/posix/rename", json!({"source_path":"flat/item/", "destination_path":"flat/prefix-only/"})).await?;
        self.assert_download_sha256("flat/item", &checksum).await?;
        self.assert_download_sha256("flat/prefix-only/child", &checksum).await?;
        self.posix_post("/noa/v1/posix/rename", json!({"source_path":"flat/prefix-only/", "destination_path":"flat/item/"})).await?;
        self.posix_post("/noa/v1/posix/trash", json!({"path":"flat/item/"})).await?;
        self.assert_download_sha256("flat/item", &checksum).await?;
        self.posix_post("/noa/v1/posix/recover", json!({"path":"flat/item/"})).await?;
        self.assert_download_sha256("flat/item/child", &checksum).await?;
        for key in [
            "suffix/a",
            "suffix/a/child",
            "suffix/a.ncx/child",
            "suffix/a.ncp/child",
        ] {
            self.upload_bytes(
                key,
                deterministic_payload(4096, key.as_bytes()),
                "application/octet-stream",
            )
            .await?;
        }
        let (suffixes, _) = self.flat_list_all("suffix", 73).await?;
        if suffixes != ["suffix/a", "suffix/a.ncp/", "suffix/a.ncx/", "suffix/a/"] {
            return Err(format!("suffix/prefix collision in flat LIST: {suffixes:?}").into());
        }
        for key in ["partial/a", "partial/b", "partial/c", "partial-moved/b"] {
            self.upload_bytes(
                key,
                deterministic_payload(4096, key.as_bytes()),
                "application/octet-stream",
            )
            .await?;
        }
        let rename = self
            .posix_post(
                "/noa/v1/posix/rename",
                json!({"source_path":"partial", "destination_path":"partial-moved"}),
            )
            .await?;
        if rename["conflicted_paths"] != json!(["partial/b"]) {
            return Err(format!("prefix conflict was not reported: {rename}").into());
        }
        for (destination, source) in [
            ("partial-moved/a", "partial/a"),
            ("partial-moved/c", "partial/c"),
            ("partial/b", "partial/b"),
            ("partial-moved/b", "partial-moved/b"),
        ] {
            self.assert_download_sha256(
                destination,
                &sha256_bytes(&deterministic_payload(4096, source.as_bytes())),
            )
            .await?;
        }
        self.posix_post("/noa/v1/posix/trash", json!({"path":"partial-moved"}))
            .await?;
        self.posix_post("/noa/v1/posix/recover", json!({"path":"partial-moved"}))
            .await?;
        self.assert_download_sha256(
            "partial-moved/a",
            &sha256_bytes(&deterministic_payload(4096, b"partial/a")),
        )
        .await?;
        let response = self
            .client
            .post(format!("{}/noa/v1/posix/mkdir", self.base_url))
            .bearer_auth(&self.token)
            .json(&json!({"path":"empty"}))
            .send()
            .await?;
        if response.status() != StatusCode::BAD_REQUEST {
            return Err("empty directory was accepted as an object".into());
        }
        let before = self.flat_immutable_digest(&self.blob_roots)?;
        self.posix_post(
            "/noa/v1/posix/rename",
            json!({"source_path":"flat/item", "destination_path":"flat/renamed"}),
        )
        .await?;
        self.assert_download_sha256("flat/renamed", &checksum)
            .await?;
        self.assert_download_sha256("flat/item/child", &checksum)
            .await?;
        self.posix_post("/noa/v1/posix/trash", json!({"path":"flat/renamed"}))
            .await?;
        self.posix_post("/noa/v1/posix/recover", json!({"path":"flat/renamed"}))
            .await?;
        if before != self.flat_immutable_digest(&self.blob_roots)? {
            return Err("copy/delete rewrote immutable payload or NDX".into());
        }
        if self.flat_ncx_paths("flat/renamed").len() != 2 {
            return Err("3+1 namespace must have exactly P+1=2 holders".into());
        }

        let count = self.args.storm_objects;
        let directory = "flat-wide".to_string();
        let expected = (0..count)
            .map(|i| format!("{directory}/file-{i:08}.bin"))
            .collect::<BTreeSet<_>>();
        let proof = &*self;
        let uploads = futures_util::stream::iter(expected.iter())
            .map(|key| async move {
                proof
                    .upload_bytes(
                        key,
                        deterministic_payload(proof.args.storm_object_size, key.as_bytes()),
                        "application/octet-stream",
                    )
                    .await
            })
            .buffer_unordered(self.args.storm_concurrency)
            .collect::<Vec<_>>()
            .await;
        for upload in uploads {
            upload?;
        }
        let (listed, latency) = self.flat_list_all(&directory, 73).await?;
        if listed.len() != count
            || listed.windows(2).any(|pair| pair[0] >= pair[1])
            || listed.into_iter().collect::<BTreeSet<_>>() != expected
        {
            return Err("wide pagination skipped objects".into());
        }

        // Competing first publishers must converge on one object lineage.
        let proof = &*self;
        let outcomes = futures_util::stream::iter(0..32usize)
            .map(|i| async move {
                proof
                    .upload_bytes(
                        "same-key/race",
                        deterministic_payload(4096, &i.to_le_bytes()),
                        "application/octet-stream",
                    )
                    .await
            })
            .buffer_unordered(16)
            .collect::<Vec<_>>()
            .await;
        for outcome in outcomes {
            outcome?;
        }
        if self.list_version_ids("same-key/race").await?.len() != 32 {
            return Err("concurrent publication lost version lineage".into());
        }

        // A corrupt NCX is repaired from its other checksummed holder.
        let copies = self.flat_ncx_paths("flat/renamed");
        let original = fs::read(&copies[0])?;
        for copy in &copies {
            fs::write(copy, b"LIST must not decode NCX")?;
        }
        self.flat_list_all("flat", 73).await?;
        for copy in &copies {
            fs::write(copy, &original)?;
        }
        fs::write(&copies[0], b"injected NCX corruption")?;
        self.assert_download_sha256("flat/renamed", &checksum)
            .await?;
        self.run_repair_and_wait().await?;
        if fs::read(&copies[0])? != original {
            return Err("NCX checksum repair did not restore the pointer".into());
        }
        self.stop_server()?;

        // Cold-start degraded reads, without unmounting any live server device.
        let root = self.blob_roots[0].clone();
        let hidden = root.with_file_name("store-offline-proof");
        fs::rename(&root, &hidden)?;
        let degraded = async {
            self.start_server("flat-one-member-offline").await?;
            self.auth().await?;
            self.assert_download_sha256("flat/renamed", &checksum)
                .await?;
            self.flat_list_all(&directory, 73).await?;
            let response = self
                .client
                .post(format!("{}/noa/v1/posix/trash", self.base_url))
                .bearer_auth(&self.token)
                .json(&json!({"path":"flat/renamed"}))
                .send()
                .await?;
            if response.status() != StatusCode::SERVICE_UNAVAILABLE {
                return Err("incomplete pool accepted a mutation".into());
            }
            self.stop_server()?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }
        .await;
        let _ = self.stop_server();
        fs::rename(&hidden, &root)?;
        degraded?;
        self.start_server("flat-restored").await?;
        self.auth().await?;
        self.assert_expected_visible(&batch_keys).await?;
        for (key, hash) in &direct_samples {
            self.assert_download_sha256(key, hash).await?;
        }
        self.assert_download_sha256("flat/renamed", &checksum)
            .await?;
        self.assert_health_clean("flat final").await?;
        self.stop_server()?;
        self.summary = json!({"status":"PASS", "proof":"n1_v4_flat_namespace", "topology":"3+1",
            "namespace_replicas":2, "wide_objects":count, "listing_page_ms":latency,
            "same_key_versions":32, "file_prefix_coexistence":"PASS", "copy_delete_payload_immutability":"PASS",
            "prefix_partial_conflicts":"PASS", "prefix_trash_recover":"PASS", "internal_suffix_keys":"PASS",
            "ncx_corruption_repair":"PASS", "degraded_cold_restart":"PASS", "degraded_mutation_fence":"PASS",
            "concurrent_standard_batch":batch,"native_direct_publication":"PASS","public_upload_layout_redacted":"PASS"});
        self.write_summary()?;
        Ok(())
    }

    async fn run_pool_expansion_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        if self.index_roots.len() != 4 {
            return Err("expansion proof needs four physical devices".into());
        }
        self.active_disk_count = 1;
        self.data_shards = 1;
        self.parity_shards = 0;
        self.write_config()?;
        self.start_server("pool-A-1+0").await?;
        self.create_fragment().await?;
        self.auth().await?;
        let payload = deterministic_payload(1024 * 1024, b"pool-expansion-payload");
        let checksum = sha256_bytes(&payload);
        self.upload_bytes(
            "old/object.bin",
            payload.clone(),
            "application/octet-stream",
        )
        .await?;
        let old_digest = self.flat_immutable_digest(&self.blob_roots[..1])?;
        let old_pointer = fs::read(
            self.flat_ncx_paths("old/object.bin")
                .first()
                .ok_or("old NCX missing")?,
        )?;
        self.stop_server()?;
        let config_path = self.args.out_dir.join("n1_config.ini");
        let initial = fs::read_to_string(&config_path)?;
        let roots = self.blob_roots[1..]
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(",");
        let expanded = format!(
            "{initial}\n[storage_pool.B]\ngeneration=2\nroots={roots}\ndata_shards=2\nparity_shards=1\nsealed=false\n"
        );
        fs::write(&config_path, &expanded)?;
        self.start_server("pool-A-plus-B-2+1").await?;
        self.auth().await?;
        self.assert_download_sha256("old/object.bin", &checksum)
            .await?;
        self.upload_bytes("new/object.bin", payload, "application/octet-stream")
            .await?;
        self.assert_download_sha256("new/object.bin", &checksum)
            .await?;
        if self.flat_ncx_paths("new/object.bin").len() != 2 {
            return Err("new 2+1 pool has wrong NCX replica count".into());
        }
        if old_digest != self.flat_immutable_digest(&self.blob_roots[..1])?
            || old_pointer
                != fs::read(
                    self.flat_ncx_paths("old/object.bin")
                        .first()
                        .ok_or("old NCX lost")?,
                )?
        {
            return Err("expansion rewrote old pool payload/NDX/namespace".into());
        }
        self.assert_object_status_clean("old/object.bin", "old pool after expansion")
            .await?;
        self.assert_object_status_clean("new/object.bin", "new pool after expansion")
            .await?;
        self.stop_server()?;
        // Generation is immutable. This remains a valid two-pool config.
        fs::write(
            &config_path,
            expanded.replace("generation=1\n", "generation=99\n"),
        )?;
        let rejected = self.start_server("pool-geometry-edit-rejected").await;
        let _ = self.stop_server();
        fs::write(&config_path, &expanded)?;
        if rejected.is_ok() {
            return Err("enrolled geometry was editable".into());
        }
        let identity_path = self.args.out_dir.join("fs/.n1/topology-roots.json");
        let identities = fs::read(&identity_path)?;
        let start = expanded
            .find("[storage_pool.A]")
            .ok_or("missing initial pool section")?;
        let end = start
            + expanded[start + 1..]
                .find('[')
                .ok_or("missing following section")?
            + 1;
        let removed = format!("{}{}", &expanded[..start], &expanded[end..]);
        fs::write(&config_path, removed)?;
        let rejected = self.start_server("pool-removal-rejected").await;
        let _ = self.stop_server();
        fs::write(&config_path, &expanded)?;
        if rejected.is_ok() || fs::read(&identity_path)? != identities {
            return Err("pool removal was accepted or changed root identity authority".into());
        }
        self.start_server("pool-final-restart").await?;
        self.auth().await?;
        self.assert_download_sha256("old/object.bin", &checksum)
            .await?;
        self.assert_download_sha256("new/object.bin", &checksum)
            .await?;
        self.stop_server()?;
        self.summary = json!({"status":"PASS", "proof":"n1_v4_mixed_pool_expansion", "old_pool":"1+0", "new_pool":"2+1",
            "old_pool_immutable":"PASS", "new_pool_placement":"PASS", "no_mandatory_rebalance":"PASS", "restart":"PASS", "geometry_edit_rejected":"PASS", "pool_removal_rejected_without_identity_writes":"PASS"});
        self.write_summary()?;
        Ok(())
    }
}
