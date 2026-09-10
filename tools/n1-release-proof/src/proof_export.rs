#[derive(Debug, Deserialize)]
struct ExportCreateProofResponse {
    export_id: String,
}

struct ProofExportDownload {
    bytes: Vec<u8>,
    etag: String,
}

impl Proof {
    async fn run_export_proof(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("export-before-power-cut").await?;
        self.create_fragment().await?;
        self.auth().await?;

        let prefix = format!("{}export-proof", self.args.prefix);
        let expected = self.upload_export_fixture(&prefix).await?;
        let export_id = self
            .create_export_job(&prefix, "zip", "n1-export-proof.zip")
            .await?;

        let interrupted_phase = self
            .wait_for_export_build_phase(&export_id, Duration::from_secs(120))
            .await?;
        self.stop_server()?;
        self.start_server("export-after-power-cut").await?;
        self.auth().await?;

        let ready = self
            .wait_for_export_phase(&export_id, "ready", Duration::from_secs(600))
            .await?;
        let full = self.download_export_full(&export_id).await?;
        self.validate_zip_export(&full.bytes, &expected).await?;
        self.validate_export_ranges(&export_id, &full).await?;
        self.validate_export_corruption_detection(&export_id)
            .await?;
        let artifact_root_count =
            self.validate_export_chunk_spread(&export_id, full.bytes.len())?;

        let mapped_source = expected
            .keys()
            .next()
            .ok_or("export fixture did not contain a mapped source")?;
        let mapped_payload =
            deterministic_payload(self.args.export_object_bytes, mapped_source.as_bytes());
        let mapped_offset = mapped_payload.len().min(37);
        let mapped_length = mapped_payload.len().saturating_sub(mapped_offset).min(4096);
        let mapped_archive_path = "business-layer/patient-record.bin";
        let mapped_id = self
            .create_mapped_export(
                mapped_source,
                mapped_archive_path,
                mapped_offset,
                mapped_length,
            )
            .await?;
        self.wait_for_export_phase(&mapped_id, "ready", Duration::from_secs(300))
            .await?;
        let mapped_download = self.download_export_full(&mapped_id).await?;
        let mapped_expected = std::collections::BTreeMap::from([(
            mapped_archive_path.to_string(),
            sha256_bytes(&mapped_payload[mapped_offset..mapped_offset + mapped_length]),
        )]);
        self.validate_zip_export(&mapped_download.bytes, &mapped_expected)
            .await?;
        self.cancel_export_job(&mapped_id).await?;
        self.wait_for_export_cancellation_cleanup(&mapped_id, Duration::from_secs(30))
            .await?;

        let tar_expected = expected
            .iter()
            .take(8)
            .map(|(key, digest)| (key.clone(), digest.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let tar_id = self
            .create_export_keys(
                tar_expected.keys().cloned().collect(),
                "tar",
                "n1-export-proof.tar",
            )
            .await?;
        self.wait_for_export_phase(&tar_id, "ready", Duration::from_secs(300))
            .await?;
        let tar_download = self.download_export_full(&tar_id).await?;
        self.validate_tar_export(&tar_download.bytes, &tar_expected)?;
        self.cancel_export_job(&tar_id).await?;
        self.wait_for_export_cancellation_cleanup(&tar_id, Duration::from_secs(30))
            .await?;

        let performance_started = Instant::now();
        let performance_id = self
            .create_export_job(&prefix, "zip", "n1-export-performance.zip")
            .await?;
        let performance_ready = self
            .wait_for_export_phase(&performance_id, "ready", Duration::from_secs(600))
            .await?;
        let build_seconds = performance_started.elapsed().as_secs_f64();
        let download_started = Instant::now();
        let performance_download = self.download_export_full(&performance_id).await?;
        let download_seconds = download_started.elapsed().as_secs_f64();
        if format!("\"{}\"", sha256_bytes(&performance_download.bytes)) != performance_download.etag
        {
            return Err("performance export ETag does not match its artifact bytes".into());
        }
        self.cancel_export_job(&performance_id).await?;
        self.wait_for_export_cancellation_cleanup(&performance_id, Duration::from_secs(30))
            .await?;

        let cancel_id = self
            .create_export_job(&prefix, "tar", "n1-cancel-proof.tar")
            .await?;
        let cancelled = self.cancel_export_job(&cancel_id).await?;
        self.wait_for_export_cancellation_cleanup(&cancel_id, Duration::from_secs(30))
            .await?;

        let expires_at = ready
            .get("expires_at_unix")
            .and_then(Value::as_i64)
            .ok_or("ready export status has no expires_at_unix")?;
        self.wait_for_export_retention_cleanup(&export_id, expires_at)
            .await?;

        self.stop_server()?;
        let logs = self.scan_logs()?;
        self.summary = json!({
            "status": "PASS",
            "engine": "n1",
            "proof": "restart_safe_resumable_bulk_export",
            "objects": expected.len(),
            "source_bytes": expected.len().saturating_mul(self.args.export_object_bytes),
            "interrupted_phase": interrupted_phase,
            "restart_recovery": "PASS",
            "archive_integrity": "PASS",
            "tar_integrity": "PASS",
            "archive_bytes": full.bytes.len(),
            "etag": full.etag,
            "range_resume": "PASS",
            "artifact_corruption_detection": "PASS",
            "mapped_byte_range_export": "PASS",
            "cancel": cancelled.get("phase").cloned().unwrap_or(Value::Null),
            "retention_cleanup": "PASS",
            "artifact_root_count": artifact_root_count,
            "performance": {
                "build_seconds": round3(build_seconds),
                "source_mib_per_second": round3(
                    performance_ready
                        .get("source_bytes_total")
                        .and_then(Value::as_u64)
                        .unwrap_or(0) as f64
                        / (1024.0 * 1024.0)
                        / build_seconds.max(f64::EPSILON)
                ),
                "download_seconds": round3(download_seconds),
                "download_mib_per_second": round3(
                    performance_download.bytes.len() as f64
                        / (1024.0 * 1024.0)
                        / download_seconds.max(f64::EPSILON)
                ),
            },
            "topology": {
                "data_shards": self.data_shards,
                "parity_shards": self.parity_shards,
                "configured_roots": self.active_disk_count,
            },
            "log_scan": logs,
        });
        self.write_summary()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }

    async fn upload_export_fixture(
        &self,
        prefix: &str,
    ) -> ProofResult<std::collections::BTreeMap<String, String>> {
        let uploads = futures_util::stream::iter(0..self.args.export_objects)
            .map(|index| async move {
                let key = format!("{prefix}/object-{index:06}.bin");
                let payload = deterministic_payload(self.args.export_object_bytes, key.as_bytes());
                let digest = sha256_bytes(&payload);
                self.upload_bytes(&key, payload, "application/octet-stream")
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((key, digest))
            })
            .buffer_unordered(self.args.export_upload_concurrency);
        tokio::pin!(uploads);
        let mut expected = std::collections::BTreeMap::new();
        while let Some(result) = uploads.next().await {
            let (key, digest) = result?;
            expected.insert(key, digest);
        }
        Ok(expected)
    }

    async fn create_export_job(
        &self,
        prefix: &str,
        format: &str,
        filename: &str,
    ) -> ProofResult<String> {
        let response = self
            .client
            .post(format!("{}/noa/v1/exports", self.base_url))
            .bearer_auth(&self.token)
            .json(&json!({
                "keys": [],
                "prefixes": [prefix],
                "format": format,
                "filename": filename,
                "export_all": false,
            }))
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "create export failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response
            .json::<ExportCreateProofResponse>()
            .await?
            .export_id)
    }

    async fn create_export_keys(
        &self,
        keys: Vec<String>,
        format: &str,
        filename: &str,
    ) -> ProofResult<String> {
        let response = self
            .client
            .post(format!("{}/noa/v1/exports", self.base_url))
            .bearer_auth(&self.token)
            .json(&json!({
                "keys": keys,
                "prefixes": [],
                "format": format,
                "filename": filename,
                "export_all": false,
            }))
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "create key export failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response
            .json::<ExportCreateProofResponse>()
            .await?
            .export_id)
    }

    async fn create_mapped_export(
        &self,
        object_key: &str,
        archive_path: &str,
        offset_bytes: usize,
        length_bytes: usize,
    ) -> ProofResult<String> {
        let response = self
            .client
            .post(format!("{}/noa/v1/exports", self.base_url))
            .bearer_auth(&self.token)
            .json(&json!({
                "entries": [{
                    "object_key": object_key,
                    "archive_path": archive_path,
                    "offset_bytes": offset_bytes,
                    "length_bytes": length_bytes,
                }],
                "format": "zip",
                "filename": "n1-mapped-export-proof.zip",
            }))
            .send()
            .await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(format!(
                "create mapped export failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        Ok(response
            .json::<ExportCreateProofResponse>()
            .await?
            .export_id)
    }

    async fn export_status_value(&self, id: &str) -> ProofResult<(StatusCode, Value)> {
        let response = self
            .client
            .get(format!("{}/noa/v1/exports/{id}", self.base_url))
            .bearer_auth(&self.token)
            .send()
            .await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        Ok((status, body))
    }

    async fn wait_for_export_build_phase(
        &self,
        id: &str,
        timeout: Duration,
    ) -> ProofResult<String> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let (status, body) = self.export_status_value(id).await?;
            if status != StatusCode::OK {
                return Err(format!("export status failed before restart: {status} {body}").into());
            }
            let phase = body.get("phase").and_then(Value::as_str).unwrap_or("");
            if phase == "building" {
                return Ok(phase.to_string());
            }
            if phase == "ready" {
                return Err(
                    "export completed before the power-cut checkpoint; increase the export proof workload"
                        .into(),
                );
            }
            if matches!(phase, "failed" | "cancelled") {
                return Err(format!("export entered terminal phase before restart: {body}").into());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Err("timed out waiting for an interruptible export phase".into())
    }

    async fn wait_for_export_phase(
        &self,
        id: &str,
        expected: &str,
        timeout: Duration,
    ) -> ProofResult<Value> {
        let deadline = Instant::now() + timeout;
        let mut last = Value::Null;
        while Instant::now() < deadline {
            let (status, body) = self.export_status_value(id).await?;
            if status != StatusCode::OK {
                return Err(format!("export status failed: {status} {body}").into());
            }
            let phase = body.get("phase").and_then(Value::as_str).unwrap_or("");
            if phase == expected {
                return Ok(body);
            }
            if matches!(phase, "failed" | "cancelled") && phase != expected {
                return Err(format!("export entered unexpected terminal phase: {body}").into());
            }
            last = body;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!("timed out waiting for export phase {expected}; last={last}").into())
    }

    async fn download_export_full(&self, id: &str) -> ProofResult<ProofExportDownload> {
        let response = self
            .client
            .get(format!("{}/noa/v1/exports/{id}/download", self.base_url))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if response.status() != StatusCode::OK {
            return Err(format!(
                "full export download failed: {} {}",
                response.status(),
                response.text().await?
            )
            .into());
        }
        let length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or("export download has no valid Content-Length")?;
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .ok_or("export download has no ETag")?
            .to_string();
        if response
            .headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|value| value.to_str().ok())
            != Some("bytes")
        {
            return Err("export download does not advertise byte ranges".into());
        }
        let bytes = response.bytes().await?.to_vec();
        if bytes.len() != length {
            return Err(format!(
                "export Content-Length mismatch: header={length} actual={}",
                bytes.len()
            )
            .into());
        }
        Ok(ProofExportDownload { bytes, etag })
    }

    async fn validate_zip_export(
        &self,
        bytes: &[u8],
        expected: &std::collections::BTreeMap<String, String>,
    ) -> ProofResult<()> {
        let zip = async_zip::base::read::mem::ZipFileReader::new(bytes.to_vec()).await?;
        if zip.file().entries().len() != expected.len() {
            return Err(format!(
                "ZIP entry count mismatch: expected={} actual={}",
                expected.len(),
                zip.file().entries().len()
            )
            .into());
        }
        let entries = zip.file().entries().to_vec();
        for (index, entry) in entries.iter().enumerate() {
            let name = entry.filename().as_str()?.to_string();
            let expected_sha = expected
                .get(&name)
                .ok_or_else(|| format!("unexpected ZIP entry {name}"))?;
            let mut payload = Vec::new();
            zip.reader_with_entry(index)
                .await?
                .read_to_end_checked(&mut payload)
                .await?;
            if sha256_bytes(&payload) != *expected_sha {
                return Err(format!("ZIP payload hash mismatch for {name}").into());
            }
        }
        Ok(())
    }

    fn validate_tar_export(
        &self,
        bytes: &[u8],
        expected: &std::collections::BTreeMap<String, String>,
    ) -> ProofResult<()> {
        let actual = tar_payload_hashes(bytes)?;
        if &actual != expected {
            return Err(format!(
                "TAR content mismatch: expected_entries={} actual_entries={}",
                expected.len(),
                actual.len()
            )
            .into());
        }
        Ok(())
    }

    async fn validate_export_ranges(
        &self,
        id: &str,
        full: &ProofExportDownload,
    ) -> ProofResult<()> {
        let start = full.bytes.len() / 3;
        let end = (start + 1024 * 1024 - 1).min(full.bytes.len() - 1);
        let response = self
            .client
            .get(format!("{}/noa/v1/exports/{id}/download", self.base_url))
            .bearer_auth(&self.token)
            .header(reqwest::header::RANGE, format!("bytes={start}-{end}"))
            .header(reqwest::header::IF_RANGE, &full.etag)
            .send()
            .await?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(format!("export range download returned {}", response.status()).into());
        }
        if response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            != Some(full.etag.as_str())
        {
            return Err("export range ETag changed".into());
        }
        let expected_range = format!("bytes {start}-{end}/{}", full.bytes.len());
        if response
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            != Some(expected_range.as_str())
        {
            return Err("export Content-Range is incorrect".into());
        }
        let partial = response.bytes().await?;
        if partial.as_ref() != &full.bytes[start..=end] {
            return Err("resumed export bytes do not match the original artifact".into());
        }

        let response = self
            .client
            .get(format!("{}/noa/v1/exports/{id}/download", self.base_url))
            .bearer_auth(&self.token)
            .header(reqwest::header::IF_NONE_MATCH, &full.etag)
            .send()
            .await?;
        if response.status() != StatusCode::NOT_MODIFIED {
            return Err(format!("export conditional GET returned {}", response.status()).into());
        }
        Ok(())
    }

    async fn validate_export_corruption_detection(&self, id: &str) -> ProofResult<()> {
        let path = self
            .blob_roots
            .iter()
            .map(|root| root.join(".n1-export-artifacts").join(id))
            .filter_map(|directory| fs::read_dir(directory).ok())
            .flatten()
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("chunk-") && name.ends_with(".bin"))
            })
            .ok_or("export proof could not find an artifact chunk to corrupt")?;
        let original = fs::read(&path)?;
        if original.is_empty() {
            return Err("export proof selected an empty artifact chunk".into());
        }
        let mut corrupted = original.clone();
        corrupted[0] ^= 0x5a;
        fs::write(&path, &corrupted)?;

        let response_result: ProofResult<()> = async {
            let response = self
                .client
                .get(format!("{}/noa/v1/exports/{id}/download", self.base_url))
                .bearer_auth(&self.token)
                .send()
                .await?;
            if response.status() != StatusCode::OK {
                return Err(format!(
                    "corrupted export download did not enter its integrity-checked stream: {}",
                    response.status()
                )
                .into());
            }
            match response.bytes().await {
                Err(_) => Ok(()),
                Ok(bytes) => Err(format!(
                    "corrupted export was delivered successfully ({} bytes)",
                    bytes.len()
                )
                .into()),
            }
        }
        .await;

        fs::write(&path, &original)?;
        response_result?;
        let restored = self.download_export_full(id).await?;
        if format!("\"{}\"", sha256_bytes(&restored.bytes)) != restored.etag {
            return Err("restored export no longer matches its ETag".into());
        }
        Ok(())
    }

    async fn cancel_export_job(&self, id: &str) -> ProofResult<Value> {
        let response = self
            .client
            .delete(format!("{}/noa/v1/exports/{id}", self.base_url))
            .bearer_auth(&self.token)
            .send()
            .await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        if status != StatusCode::ACCEPTED {
            return Err(format!("cancel export failed: {} {}", status, body).into());
        }
        if body.get("phase").and_then(Value::as_str) != Some("cancelled") {
            return Err(format!("cancel export did not acknowledge cancellation: {body}").into());
        }
        Ok(body)
    }

    async fn wait_for_export_cancellation_cleanup(
        &self,
        id: &str,
        timeout: Duration,
    ) -> ProofResult<()> {
        let deadline = Instant::now() + timeout;
        let mut last = Value::Null;
        while Instant::now() < deadline {
            let (status, body) = self.export_status_value(id).await?;
            match status {
                StatusCode::NOT_FOUND if self.artifact_root_count(id) == 0 => return Ok(()),
                StatusCode::OK => {
                    if body.get("phase").and_then(Value::as_str) != Some("cancelled") {
                        return Err(format!(
                            "cancelled export returned an unexpected phase: {body}"
                        )
                        .into());
                    }
                    last = body;
                }
                StatusCode::NOT_FOUND => {
                    last = json!({"phase": "removed", "artifact_roots_remaining": self.artifact_root_count(id)});
                }
                _ => {
                    return Err(format!(
                        "cancelled export cleanup returned an unexpected status: {status} {body}"
                    )
                    .into());
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!(
            "timed out waiting for cancelled export cleanup; last={last}, artifact_roots_remaining={}",
            self.artifact_root_count(id)
        )
        .into())
    }

    fn artifact_root_count(&self, id: &str) -> usize {
        self.blob_roots
            .iter()
            .filter(|root| root.join(".n1-export-artifacts").join(id).is_dir())
            .count()
    }

    fn validate_export_chunk_spread(&self, id: &str, artifact_bytes: usize) -> ProofResult<usize> {
        let roots = self.artifact_root_count(id);
        let expected = self
            .active_disk_count
            .min(artifact_bytes.div_ceil(PROOF_EXPORT_CHUNK_SIZE));
        if roots != expected {
            return Err(format!(
                "export chunks did not use every eligible root: expected={expected} actual={roots}"
            )
            .into());
        }
        Ok(roots)
    }

    async fn wait_for_export_retention_cleanup(
        &self,
        id: &str,
        expires_at: i64,
    ) -> ProofResult<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let wait = retention_cleanup_wait(expires_at, now);
        tokio::time::sleep(Duration::from_secs(wait)).await;
        let (status, _) = self.export_status_value(id).await?;
        if status != StatusCode::NOT_FOUND {
            return Err(format!("expired export remained addressable: {status}").into());
        }
        if self.artifact_root_count(id) != 0 {
            return Err("expired export artifact chunks were not removed".into());
        }
        Ok(())
    }
}

fn retention_cleanup_wait(expires_at: i64, now: i64) -> u64 {
    expires_at.saturating_sub(now).max(0).saturating_add(8) as u64
}

#[cfg(test)]
mod export_proof_tests {
    use super::retention_cleanup_wait;

    #[test]
    fn expired_export_uses_only_the_cleanup_grace_period() {
        assert_eq!(retention_cleanup_wait(100, 200), 8);
        assert_eq!(retention_cleanup_wait(200, 100), 108);
    }
}

fn tar_payload_hashes(raw: &[u8]) -> ProofResult<std::collections::BTreeMap<String, String>> {
    let mut entries = std::collections::BTreeMap::new();
    let mut offset = 0usize;
    while offset + 512 <= raw.len() {
        let header = &raw[offset..offset + 512];
        if header.iter().all(|byte| *byte == 0) {
            break;
        }
        let name = parse_tar_string(&header[0..100]);
        let prefix = parse_tar_string(&header[345..500]);
        let full_name = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        let size_text = parse_tar_string(&header[124..136]);
        let size = usize::from_str_radix(size_text.trim(), 8)
            .map_err(|error| format!("invalid TAR entry size for {full_name}: {error}"))?;
        let data_start = offset.checked_add(512).ok_or("TAR entry offset overflow")?;
        let data_end = data_start
            .checked_add(size)
            .ok_or("TAR entry size overflow")?;
        if data_end > raw.len() {
            return Err(format!("TAR entry {full_name} is truncated").into());
        }
        if matches!(header[156], 0 | b'0') {
            if full_name.is_empty() {
                return Err("TAR contains an empty file name".into());
            }
            if entries
                .insert(full_name.clone(), sha256_bytes(&raw[data_start..data_end]))
                .is_some()
            {
                return Err(format!("TAR contains duplicate entry {full_name}").into());
            }
        }
        offset = data_start
            .checked_add(size.div_ceil(512).saturating_mul(512))
            .ok_or("TAR padding offset overflow")?;
    }
    Ok(entries)
}
