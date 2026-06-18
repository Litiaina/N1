#[derive(serde::Serialize)]
struct NativeProofRequest {
    id: String,
    op: String,
    #[serde(default)]
    payload: String,
    #[serde(default)]
    body: Option<serde_bytes::ByteBuf>,
    #[serde(default)]
    body_len: Option<u64>,
}

const NATIVE_UPLOAD_COPY_BUFFER_BYTES: usize = 256 * 1024;

#[derive(serde::Deserialize)]
struct NativeProofResponse {
    id: String,
    ok: bool,
    status: Option<u16>,
    payload: Option<String>,
    body: Option<serde_bytes::ByteBuf>,
    body_len: Option<u64>,
    error: Option<Value>,
}

impl Proof {
    async fn run_native_protocol_smoke(&mut self) -> ProofResult<()> {
        self.validate()?;
        self.prepare_dirs()?;
        self.write_config()?;
        self.start_server("native-before").await?;
        self.create_fragment().await?;
        self.auth().await?;

        let object_key = format!("{}native-protocol/object.txt", self.args.prefix);
        self.upload_bytes(
            &object_key,
            b"native protocol release proof payload".to_vec(),
            "text/plain",
        )
        .await?;
        let native = self.native_protocol_proof(&object_key).await?;
        self.assert_download_sha256(&object_key, &sha256_bytes(b"native protocol release proof payload"))
            .await?;
        let health = self
            .client
            .get(format!("{}/n1/maintenance/health", self.base_url))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if health.status() != StatusCode::OK {
            return Err(format!(
                "native smoke health failed: {} {}",
                health.status(),
                health.text().await?
            )
            .into());
        }
        self.summary = json!({
            "status": "PASS",
            "proof": "n1_native_protocol",
            "object_key": object_key,
            "native": native,
            "fragment_hash": self.fragment,
            "native_port": self.native_port(),
        });
        self.write_summary()?;
        self.stop_server()?;
        if self.args.cleanup_roots {
            self.cleanup_roots()?;
        }
        Ok(())
    }

    async fn native_protocol_proof(&self, object_key: &str) -> ProofResult<Value> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.native_port())).await?;
        stream.set_nodelay(true)?;

        let ping = native_round_trip(&mut stream, "native-ping", "ping", json!({}), None).await?;
        ensure_native_ok(&ping, "native ping")?;
        let protocol = ping
            .payload
            .as_ref()
            .and_then(|value| serde_json::from_str::<Value>(value).ok())
            .and_then(|value| value.get("protocol").and_then(Value::as_str).map(str::to_owned))
            .unwrap_or_default();
        if protocol != "n1/0.4" {
            return Err(format!("native ping returned unexpected protocol '{protocol}'").into());
        }

        let auth = native_round_trip(
            &mut stream,
            "native-auth",
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
        .await?;
        ensure_native_ok(&auth, "native auth")?;

        let native_one_shot_key = format!("{}native-protocol/native-one-shot.bin", self.args.prefix);
        let native_one_shot_payload = deterministic_payload(512 * 1024, b"native-one-shot");
        let native_one_shot_sha = sha256_bytes(&native_one_shot_payload);
        let mut live_object_keys = Vec::new();
        let native_upload = native_round_trip(
            &mut stream,
            "native-upload-one-shot",
            "upload.one_shot",
            json!({
                "object_key": native_one_shot_key,
                "content_type": "application/octet-stream",
                "durability": "batched",
            }),
            Some(native_one_shot_payload.clone()),
        )
        .await?;
        ensure_native_status(&native_upload, 201, "native one-shot upload")?;
        self.assert_download_sha256(&native_one_shot_key, &native_one_shot_sha)
            .await?;
        live_object_keys.push(native_one_shot_key.clone());

        let native_multipart_key = format!("{}native-protocol/native-multipart.bin", self.args.prefix);
        let native_multipart_payload = deterministic_payload(2 * 1024 * 1024, b"native-multipart");
        let native_multipart_sha = sha256_bytes(&native_multipart_payload);
        let part_size = 512 * 1024usize;
        let expected_parts = native_multipart_payload.len().div_ceil(part_size);
        let init = native_round_trip(
            &mut stream,
            "native-multipart-init",
            "multipart.init",
            json!({
                "object_key": native_multipart_key,
                "content_type": "application/octet-stream",
                "total_size_bytes": native_multipart_payload.len(),
                "expected_parts": expected_parts,
                "part_size_bytes": part_size,
            }),
            None,
        )
        .await?;
        ensure_native_status(&init, 201, "native multipart init")?;
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
        for (idx, chunk) in native_multipart_payload.chunks(part_size).enumerate() {
            ensure_native_status(
                &native_round_trip(
                    &mut stream,
                    &format!("native-multipart-part-{idx}"),
                    "multipart.put_part",
                    json!({
                        "version_id": version_id,
                        "part_index": idx,
                        "durability": "batched",
                    }),
                    Some(chunk.to_vec()),
                )
                .await?,
                200,
                "native multipart part",
            )?;
        }
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-multipart-finalize",
                "multipart.finalize",
                json!({
                    "object_key": native_multipart_key,
                    "version_id": version_id,
                    "session_id": session_id,
                    "content_type": "application/octet-stream",
                    "total_size_bytes": native_multipart_payload.len(),
                    "expected_parts": expected_parts,
                    "part_size_bytes": part_size,
                }),
                None,
            )
            .await?,
            200,
            "native multipart finalize",
        )?;
        self.assert_download_sha256(&native_multipart_key, &native_multipart_sha)
            .await?;
        live_object_keys.push(native_multipart_key.clone());
        let download_chunk = native_round_trip(
            &mut stream,
            "native-download-chunk",
            "object.download_chunk",
            json!({
                "object_key": native_multipart_key,
                "offset": 0,
                "chunk_size": 64 * 1024,
            }),
            None,
        )
        .await?;
        ensure_native_status(&download_chunk, 200, "native download chunk")?;
        if download_chunk.body.as_ref().map(|body| body.len()) != Some(64 * 1024) {
            return Err("native download chunk returned the wrong byte count".into());
        }

        let full_download = native_round_trip(
            &mut stream,
            "native-full-download",
            "object.download",
            json!({"object_key": native_one_shot_key}),
            None,
        )
        .await?;
        ensure_native_status(&full_download, 200, "native full download")?;
        if full_download.body.as_ref().map(|body| body.as_slice())
            != Some(native_one_shot_payload.as_slice())
        {
            return Err("native full download returned different bytes".into());
        }

        let stream_download = native_round_trip_stream_hash(
            &mut stream,
            "native-stream-download",
            "object.download_stream",
            json!({
                "object_key": native_multipart_key,
                "chunk_size": 1024 * 1024,
            }),
        )
        .await?;
        ensure_native_status(&stream_download.response, 200, "native stream download")?;
        if stream_download.bytes_read != native_multipart_payload.len() {
            return Err(format!(
                "native stream download byte count mismatch: got {} expected {}",
                stream_download.bytes_read,
                native_multipart_payload.len()
            )
            .into());
        }
        if stream_download.sha256 != native_multipart_sha {
            return Err(format!(
                "native stream download sha mismatch: got {} expected {}",
                stream_download.sha256, native_multipart_sha
            )
            .into());
        }

        let meta = native_round_trip(
            &mut stream,
            "native-object-meta",
            "object.meta",
            json!({"object_key": native_multipart_key}),
            None,
        )
        .await?;
        ensure_native_status(&meta, 200, "native object metadata")?;
        let meta_body = native_json_body(&meta)?;
        if meta_body
            .get("total_size_bytes")
            .and_then(Value::as_u64)
            != Some(native_multipart_payload.len() as u64)
        {
            return Err(format!("native metadata size mismatch: {meta_body}").into());
        }

        let versions = native_round_trip(
            &mut stream,
            "native-object-versions",
            "object.versions",
            json!({"object_key": native_multipart_key}),
            None,
        )
        .await?;
        ensure_native_status(&versions, 200, "native object versions")?;
        if native_json_body(&versions)?
            .as_array()
            .is_none_or(|versions| versions.is_empty())
        {
            return Err("native object versions returned no versions".into());
        }

        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-object-status",
                "object.status",
                json!({"object_key": native_multipart_key, "mode": "availability"}),
                None,
            )
            .await?,
            200,
            "native object status",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-object-repair",
                "object.repair",
                json!({"object_key": native_multipart_key, "mode": "availability"}),
                None,
            )
            .await?,
            200,
            "native object repair",
        )?;

        let usage = native_round_trip(
            &mut stream,
            "native-fragment-usage",
            "fragment.usage",
            json!({}),
            None,
        )
        .await?;
        ensure_native_status(&usage, 200, "native fragment usage")?;
        let usage_body = native_json_body(&usage)?;
        let used_bytes = usage_body
            .get("used_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if used_bytes == 0 {
            return Err(format!("native fragment usage reported zero bytes: {usage_body}").into());
        }

        let rename_source = format!("{}native-protocol/rename-source.bin", self.args.prefix);
        let rename_dest = format!("{}native-protocol/rename-dest.bin", self.args.prefix);
        let rename_payload = deterministic_payload(128 * 1024, b"native-rename-object");
        let rename_sha = sha256_bytes(&rename_payload);
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-rename-upload",
                "upload.one_shot",
                json!({
                    "object_key": rename_source,
                    "content_type": "application/octet-stream",
                    "durability": "batched",
                }),
                Some(rename_payload),
            )
            .await?,
            201,
            "native upload before object rename",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-object-rename",
                "object.rename",
                json!({"source_key": rename_source, "destination_key": rename_dest}),
                None,
            )
            .await?,
            200,
            "native object rename",
        )?;
        self.assert_download_sha256(&rename_dest, &rename_sha).await?;
        live_object_keys.push(rename_dest.clone());

        let recover_version_key = format!("{}native-protocol/version-recover.bin", self.args.prefix);
        let recover_payload = deterministic_payload(96 * 1024, b"native-version-recover");
        let recover_sha = sha256_bytes(&recover_payload);
        let recover_upload = native_round_trip(
            &mut stream,
            "native-version-recover-upload",
            "upload.one_shot",
            json!({
                "object_key": recover_version_key,
                "content_type": "application/octet-stream",
                "durability": "batched",
            }),
            Some(recover_payload),
        )
        .await?;
        ensure_native_status(&recover_upload, 201, "native version recover upload")?;
        let recover_version_id = native_json_body(&recover_upload)?
            .get("version_id")
            .and_then(Value::as_str)
            .ok_or("native version recover upload missing version_id")?
            .to_string();
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-version-recover-trash",
                "object.delete",
                json!({"object_key": recover_version_key}),
                None,
            )
            .await?,
            200,
            "native version recover soft delete",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-version-recover",
                "object.version.recover",
                json!({
                    "object_key": recover_version_key,
                    "version_id": recover_version_id,
                }),
                None,
            )
            .await?,
            200,
            "native version recover",
        )?;
        self.assert_download_sha256(&recover_version_key, &recover_sha)
            .await?;
        live_object_keys.push(recover_version_key.clone());

        let bulk_a = format!("{}native-protocol/bulk-a.bin", self.args.prefix);
        let bulk_b = format!("{}native-protocol/bulk-b.bin", self.args.prefix);
        for (idx, key) in [&bulk_a, &bulk_b].into_iter().enumerate() {
            ensure_native_status(
                &native_round_trip(
                    &mut stream,
                    &format!("native-bulk-upload-{idx}"),
                    "upload.one_shot",
                    json!({
                        "object_key": key,
                        "content_type": "application/octet-stream",
                        "durability": "batched",
                    }),
                    Some(deterministic_payload(64 * 1024, key.as_bytes())),
                )
                .await?,
                201,
                "native bulk seed upload",
            )?;
        }
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-bulk-delete",
                "object.bulk_delete",
                json!({"object_keys": [bulk_a.clone(), bulk_b.clone()]}),
                None,
            )
            .await?,
            200,
            "native bulk delete",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-bulk-recover",
                "object.bulk_recover",
                json!({"object_keys": [bulk_a.clone(), bulk_b.clone()]}),
                None,
            )
            .await?,
            200,
            "native bulk recover",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-bulk-delete-again",
                "object.bulk_delete",
                json!({"object_keys": [bulk_a.clone(), bulk_b.clone()]}),
                None,
            )
            .await?,
            200,
            "native bulk delete before purge",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-bulk-purge",
                "object.bulk_purge",
                json!({"object_keys": [bulk_a, bulk_b]}),
                None,
            )
            .await?,
            200,
            "native bulk purge",
        )?;

        let cancel_key = format!("{}native-protocol/cancel-me.bin", self.args.prefix);
        let cancel_init = native_round_trip(
            &mut stream,
            "native-multipart-cancel-init",
            "multipart.init",
            json!({
                "object_key": cancel_key,
                "content_type": "application/octet-stream",
                "total_size_bytes": 1024,
                "expected_parts": 1,
                "part_size_bytes": 1024,
            }),
            None,
        )
        .await?;
        ensure_native_status(&cancel_init, 201, "native multipart cancel init")?;
        let cancel_body = native_json_body(&cancel_init)?;
        let cancel_version_id = cancel_body
            .get("version_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("native cancel init missing version_id: {cancel_body}"))?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-uploads-list",
                "uploads.list",
                json!({"limit": 50, "offset": 0}),
                None,
            )
            .await?,
            200,
            "native uploads list",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-uploads-status",
                "uploads.status",
                json!({"version_id": cancel_version_id}),
                None,
            )
            .await?,
            200,
            "native uploads status",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-multipart-cancel",
                "multipart.cancel",
                json!({
                    "version_id": cancel_version_id,
                    "object_key": cancel_key,
                }),
                None,
            )
            .await?,
            204,
            "native multipart cancel",
        )?;

        let (parent, name) = split_parent_name(object_key);
        let list = native_round_trip(
            &mut stream,
            "native-list",
            "namespace.list",
            json!({"path": parent, "limit": 1000}),
            None,
        )
        .await?;
        ensure_native_status(&list, 200, "native namespace list")?;
        let list_json = native_json_body(&list)?;
        let contains = list_json
            .get("entries")
            .and_then(Value::as_array)
            .is_some_and(|entries| {
                entries
                    .iter()
                    .any(|entry| entry.get("name").and_then(Value::as_str) == Some(name))
            });
        if !contains {
            return Err(format!("native namespace list did not contain {object_key}: {list_json}").into());
        }

        let stat = native_round_trip(
            &mut stream,
            "native-stat",
            "namespace.stat",
            json!({"path": object_key}),
            None,
        )
        .await?;
        ensure_native_status(&stat, 200, "native namespace stat")?;

        let native_dir = format!("{}native-protocol/probe", self.args.prefix);
        let renamed_dir = format!("{}native-protocol/probe-renamed", self.args.prefix);

        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-mkdir",
                "namespace.mkdir",
                json!({"path": native_dir}),
                None,
            )
            .await?,
            200,
            "native mkdir",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-rename",
                "namespace.rename",
                json!({"source_path": native_dir, "destination_path": renamed_dir}),
                None,
            )
            .await?,
            200,
            "native rename",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-trash",
                "namespace.trash",
                json!({"path": renamed_dir}),
                None,
            )
            .await?,
            200,
            "native trash",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-recover",
                "namespace.recover",
                json!({"path": renamed_dir}),
                None,
            )
            .await?,
            200,
            "native recover",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-trash-again",
                "namespace.trash",
                json!({"path": renamed_dir}),
                None,
            )
            .await?,
            200,
            "native trash before purge",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-purge",
                "namespace.purge",
                json!({"path": renamed_dir}),
                None,
            )
            .await?,
            200,
            "native purge",
        )?;

        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-gc-status",
                "maintenance.gc.status",
                json!({}),
                None,
            )
            .await?,
            200,
            "native gc status",
        )?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-gc-start",
                "maintenance.gc.start",
                json!({"dry_run": true}),
                None,
            )
            .await?,
            202,
            "native gc start",
        )?;
        native_wait_job_not_running(
            &mut stream,
            "native-gc-status-wait",
            "maintenance.gc.status",
            "native gc wait",
        )
        .await?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-gc-stop",
                "maintenance.gc.stop",
                json!({}),
                None,
            )
            .await?,
            200,
            "native gc stop",
        )?;

        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-repair-start",
                "maintenance.repair.start",
                json!({
                    "dry_run": true,
                    "fast_mode": true,
                    "read_only_fast": true,
                    "batch": 8,
                    "mode": "availability",
                }),
                None,
            )
            .await?,
            202,
            "native repair start",
        )?;
        native_wait_job_not_running(
            &mut stream,
            "native-repair-status-wait",
            "maintenance.repair.status",
            "native repair wait",
        )
        .await?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-repair-stop",
                "maintenance.repair.stop",
                json!({}),
                None,
            )
            .await?,
            200,
            "native repair stop",
        )?;

        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-redistribution-start",
                "maintenance.redistribution.start",
                json!({
                    "dry_run": true,
                    "fast_mode": true,
                    "read_only_fast": true,
                    "mode": "fast",
                    "durability": "relaxed",
                    "scope": "all",
                    "object_concurrency": 1,
                    "status_batch": 8,
                }),
                None,
            )
            .await?,
            202,
            "native redistribution start",
        )?;
        native_wait_job_not_running(
            &mut stream,
            "native-redistribution-status-wait",
            "maintenance.redistribution.status",
            "native redistribution wait",
        )
        .await?;
        ensure_native_status(
            &native_round_trip(
                &mut stream,
                "native-redistribution-stop",
                "maintenance.redistribution.stop",
                json!({}),
                None,
            )
            .await?,
            200,
            "native redistribution stop",
        )?;

        Ok(json!({
            "native_port": self.native_port(),
            "object_key": object_key,
            "live_object_keys": live_object_keys,
            "checked_ops": [
                "ping",
                "auth.fragment",
                "namespace.list",
                "namespace.stat",
                "namespace.mkdir",
                "namespace.rename",
                "namespace.trash",
                "namespace.recover",
                "namespace.purge",
                "upload.one_shot",
                "multipart.init",
                "multipart.put_part",
                "multipart.finalize",
                "multipart.cancel",
                "uploads.list",
                "uploads.status",
                "object.download_chunk",
                "object.download",
                "object.download_stream",
                "object.meta",
                "object.versions",
                "object.status",
                "object.repair",
                "object.rename",
                "object.version.recover",
                "object.bulk_delete",
                "object.bulk_recover",
                "object.bulk_purge",
                "fragment.usage",
                "capacity.usage",
                "maintenance.gc.status",
                "maintenance.gc.start",
                "maintenance.gc.stop",
                "maintenance.repair.start",
                "maintenance.repair.status",
                "maintenance.repair.stop",
                "maintenance.redistribution.start",
                "maintenance.redistribution.status",
                "maintenance.redistribution.stop"
            ],
            "status": "PASS"
        }))
    }
}

struct NativeStreamBodyProof {
    response: NativeProofResponse,
    bytes_read: usize,
    sha256: String,
}

async fn native_round_trip_stream_hash(
    stream: &mut TcpStream,
    id: &str,
    op: &str,
    payload: Value,
) -> ProofResult<NativeStreamBodyProof> {
    use tokio::io::AsyncWriteExt;

    let request = NativeProofRequest {
        id: id.to_string(),
        op: op.to_string(),
        payload: serde_json::to_string(&payload)?,
        body_len: None,
        body: None,
    };
    let encoded = rmp_serde::to_vec(&request)?;
    stream
        .write_all(&(encoded.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&encoded).await?;
    stream.flush().await?;

    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await?;
    let response = rmp_serde::from_slice::<NativeProofResponse>(&frame)?;
    if response.id != id {
        return Err(format!(
            "native response id mismatch: got {} expected {}",
            response.id, id
        )
        .into());
    }

    let mut hasher = Sha256::new();
    let mut bytes_read = 0usize;
    if let Some(body) = response.body.as_ref() {
        hasher.update(body.as_slice());
        bytes_read = bytes_read.saturating_add(body.len());
    } else if let Some(mut body_len) = response.body_len {
        let mut buffer = vec![0u8; NATIVE_UPLOAD_COPY_BUFFER_BYTES];
        while body_len > 0 {
            let wanted = (body_len as usize).min(buffer.len());
            stream.read_exact(&mut buffer[..wanted]).await?;
            hasher.update(&buffer[..wanted]);
            bytes_read = bytes_read.saturating_add(wanted);
            body_len = body_len.saturating_sub(wanted as u64);
        }
    }

    Ok(NativeStreamBodyProof {
        response,
        bytes_read,
        sha256: format!("{:x}", hasher.finalize()),
    })
}

async fn native_round_trip(
    stream: &mut TcpStream,
    id: &str,
    op: &str,
    payload: Value,
    body: Option<Vec<u8>>,
) -> ProofResult<NativeProofResponse> {
    use tokio::io::AsyncWriteExt;

    let request = NativeProofRequest {
        id: id.to_string(),
        op: op.to_string(),
        payload: serde_json::to_string(&payload)?,
        body_len: body.as_ref().map(|body| body.len() as u64),
        body: None,
    };
    let encoded = rmp_serde::to_vec(&request)?;
    stream
        .write_all(&(encoded.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&encoded).await?;
    if let Some(body) = body {
        stream.write_all(&body).await?;
    }
    stream.flush().await?;
    read_native_response(stream, id).await
}

async fn native_round_trip_file_range(
    stream: &mut TcpStream,
    id: &str,
    op: &str,
    mut payload: Value,
    path: &Path,
    offset: u64,
    len: usize,
) -> ProofResult<NativeProofResponse> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    if let Value::Object(ref mut map) = payload {
        map.insert("expect_continue".to_string(), Value::Bool(true));
    }

    let request = NativeProofRequest {
        id: id.to_string(),
        op: op.to_string(),
        payload: serde_json::to_string(&payload)?,
        body_len: Some(len as u64),
        body: None,
    };
    let encoded = rmp_serde::to_vec(&request)?;
    stream
        .write_all(&(encoded.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&encoded).await?;
    stream.flush().await?;

    let continue_response = read_native_response(stream, id).await?;
    ensure_native_status(&continue_response, 100, "native upload continue")?;

    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut remaining = len;
    let mut buffer = vec![0u8; NATIVE_UPLOAD_COPY_BUFFER_BYTES];
    while remaining > 0 {
        let wanted = remaining.min(buffer.len());
        let read = file.read(&mut buffer[..wanted]).await?;
        if read == 0 {
            return Err(format!(
                "native file body ended early for {id}: remaining={remaining}"
            )
            .into());
        }
        stream.write_all(&buffer[..read]).await?;
        remaining -= read;
    }
    stream.flush().await?;
    read_native_response(stream, id).await
}

async fn read_native_response(
    stream: &mut TcpStream,
    id: &str,
) -> ProofResult<NativeProofResponse> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await?;
    let mut response = rmp_serde::from_slice::<NativeProofResponse>(&frame)?;
    if response.body.is_none() {
        if let Some(body_len) = response.body_len {
            let mut body = vec![0u8; body_len as usize];
            if !body.is_empty() {
                stream.read_exact(&mut body).await?;
            }
            response.body = Some(serde_bytes::ByteBuf::from(body));
        }
    }
    if response.id != id {
        return Err(format!("native response id mismatch: got {} expected {}", response.id, id).into());
    }
    Ok(response)
}

fn ensure_native_ok(response: &NativeProofResponse, label: &str) -> ProofResult<()> {
    if response.ok {
        Ok(())
    } else {
        Err(format!("{label} failed: {}", native_error_string(response)).into())
    }
}

fn ensure_native_status(
    response: &NativeProofResponse,
    expected: u16,
    label: &str,
) -> ProofResult<()> {
    if response.status == Some(expected) && response.ok {
        Ok(())
    } else {
        Err(format!(
            "{label} failed: status={:?} ok={} error={} body={}",
            response.status,
            response.ok,
            native_error_string(response),
            String::from_utf8_lossy(response.body.as_ref().map(|body| body.as_slice()).unwrap_or_default())
        )
        .into())
    }
}

fn native_json_body(response: &NativeProofResponse) -> ProofResult<Value> {
    let body = response
        .body
        .as_ref()
        .map(|body| body.as_slice())
        .ok_or("native response did not include body")?;
    Ok(rmp_serde::from_slice(body)?)
}

async fn native_wait_job_not_running(
    stream: &mut TcpStream,
    id_prefix: &str,
    status_op: &str,
    label: &str,
) -> ProofResult<Value> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempt = 0usize;
    loop {
        let response = native_round_trip(
            stream,
            &format!("{id_prefix}-{attempt}"),
            status_op,
            json!({}),
            None,
        )
        .await?;
        ensure_native_status(&response, 200, label)?;
        let body = native_json_body(&response)?;
        if body
            .get("is_running")
            .and_then(Value::as_bool)
            .is_none_or(|running| !running)
        {
            return Ok(body);
        }
        if Instant::now() >= deadline {
            return Err(format!("{label} did not finish within 30s: {body}").into());
        }
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn native_error_string(response: &NativeProofResponse) -> String {
    response
        .error
        .as_ref()
        .map(Value::to_string)
        .unwrap_or_else(|| "none".to_string())
}
