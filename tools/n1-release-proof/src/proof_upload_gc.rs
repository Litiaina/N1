// Pause a real HTTP one-shot body while both cleanup paths run.

impl Proof {
    fn gc_fixture_versions(&self) -> ProofResult<BTreeSet<PathBuf>> {
        let mut versions = BTreeSet::new();
        for root in &self.blob_roots {
            let mut stack =
                vec![physical_fragment_root(root, &self.stable_fragment_id).join(".objects")];
            while let Some(dir) = stack.pop() {
                if !dir.is_dir() {
                    continue;
                }
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    if !entry.file_type()?.is_dir() {
                        continue;
                    }
                    let path = entry.path();
                    if path
                        .parent()
                        .and_then(Path::file_name)
                        .is_some_and(|n| n.to_string_lossy().starts_with("@o_"))
                    {
                        let name = entry.file_name();
                        let name = name.to_string_lossy();
                        if name.len() == 26 && name.bytes().all(|c| c.is_ascii_alphanumeric()) {
                            versions.insert(path);
                        }
                    } else {
                        stack.push(path);
                    }
                }
            }
        }
        Ok(versions)
    }

    async fn paused_upload_gc_proof(
        &self,
        expire_existing: bool,
    ) -> ProofResult<(String, String, Value)> {
        // Logical keys and immutable physical identities are separate in v4.
        let key =
            sha256_bytes(format!("paused-upload-{}-{expire_existing}", self.stamp).as_bytes());
        if expire_existing {
            self.upload_bytes(&key, b"previous committed object".to_vec(), "text/plain")
                .await?;
        }
        let payload = deterministic_payload(128 * 1024, b"paused-http-one-shot");
        let digest = sha256_bytes(&payload);
        let before = self.gc_fixture_versions()?;
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(2);
        tx.send(Ok(payload[..64 * 1024].to_vec())).await?;
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|chunk| (chunk, rx))
        });
        let request = self
            .client
            .post(format!("{}/noa/v1/upload/one-shot/{key}", self.base_url))
            .bearer_auth(&self.token)
            .header(CONTENT_LENGTH, payload.len())
            .body(reqwest::Body::wrap_stream(stream));
        let upload = tokio::spawn(async move { request.send().await });
        let deadline = Instant::now() + Duration::from_secs(15);
        let versions = loop {
            let mut versions = Vec::new();
            for path in self.gc_fixture_versions()?.difference(&before) {
                if fs::read_dir(path)?.next().is_some() {
                    versions.push(path.clone());
                }
            }
            if versions.len() == usize::from(self.data_shards + self.parity_shards) {
                break versions;
            }
            if Instant::now() >= deadline {
                return Err("paused one-shot did not open its complete EC set".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let mut sent = 64 * 1024;
        if expire_existing {
            self.posix_post("/noa/v1/posix/trash", json!({"path":key}))
                .await?;
            let deadline =
                Instant::now() + Duration::from_secs(self.args.trash_retention_seconds + 2);
            while Instant::now() < deadline {
                tokio::time::sleep(Duration::from_secs(2)).await;
                // Real time and real TTL, without changing host clocks or
                // rewriting trash records. Keep the active body progressing.
                tx.send(Ok(payload[sent..sent + 1].to_vec())).await?;
                sent += 1;
            }
        }
        // Advance only the test directories' ages, never host time. These are
        // now eligible by TTL but belong to a still-active HTTP upload.
        let old = SystemTime::now() - Duration::from_secs(3600);
        for version in &versions {
            fs::File::open(version)?.set_times(fs::FileTimes::new().set_modified(old))?;
            fs::File::open(version.parent().unwrap())?
                .set_times(fs::FileTimes::new().set_modified(old))?;
        }
        let gc = self.gc_safety_run().await?;
        if gc["total_errors"].as_u64() != Some(0) {
            return Err(format!("GC failed during paused one-shot: {gc}").into());
        }
        if expire_existing && gc["total_trash_versions_expired"].as_u64().unwrap_or(0) == 0 {
            return Err("TTL upload proof did not actually expire the old trash anchor".into());
        }
        self.client
            .post(format!(
                "{}/noa/v1/maintenance/fragment/cleanup",
                self.base_url
            ))
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?;
        if versions.iter().any(|version| !version.is_dir()) {
            return Err("cleanup deleted shards belonging to the paused one-shot".into());
        }
        tx.send(Ok(payload[sent..].to_vec())).await?;
        drop(tx);
        let response = tokio::time::timeout(Duration::from_secs(30), upload).await???;
        if response.status() != StatusCode::CREATED {
            return Err(format!(
                "resumed one-shot failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        self.assert_download_sha256(&key, &digest).await?;
        Ok((
            key,
            digest,
            json!({"status":"PASS", "expired_existing":expire_existing, "protected_roots":versions.len(), "gc":gc}),
        ))
    }
}
