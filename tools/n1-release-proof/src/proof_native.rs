#[derive(serde::Serialize)]
struct NativeProofRequest {
    id: String,
    op: String,
    #[serde(default)]
    payload: String,
    #[serde(default)]
    body: Option<Vec<u8>>,
}

#[derive(serde::Deserialize)]
struct NativeProofResponse {
    id: String,
    ok: bool,
    status: Option<u16>,
    payload: Option<String>,
    body: Option<Vec<u8>>,
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
        if protocol != "n1/0.1" {
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

        let api_bridge = native_round_trip(
            &mut stream,
            "native-api",
            "api.request",
            json!({
                "method": "GET",
                "path": format!(
                    "/n1/posix/stat?path={}",
                    encode_query_value(object_key)
                ),
            }),
            None,
        )
        .await?;
        ensure_native_status(&api_bridge, 200, "native api bridge")?;

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

        Ok(json!({
            "native_port": self.native_port(),
            "object_key": object_key,
            "checked_ops": [
                "ping",
                "auth.fragment",
                "api.request",
                "namespace.list",
                "namespace.stat",
                "namespace.mkdir",
                "namespace.rename",
                "namespace.trash",
                "namespace.recover",
                "namespace.purge"
            ],
            "status": "PASS"
        }))
    }
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
        body,
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
            String::from_utf8_lossy(response.body.as_deref().unwrap_or_default())
        )
        .into())
    }
}

fn native_json_body(response: &NativeProofResponse) -> ProofResult<Value> {
    let body = response
        .body
        .as_deref()
        .ok_or("native response did not include body")?;
    Ok(serde_json::from_slice(body)?)
}

fn native_error_string(response: &NativeProofResponse) -> String {
    response
        .error
        .as_ref()
        .map(Value::to_string)
        .unwrap_or_else(|| "none".to_string())
}
