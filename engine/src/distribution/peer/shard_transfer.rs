use super::*;

pub struct RemoteShardWriter {
    target: PeerShardTarget,
    writer: Option<DuplexStream>,
    handle: tokio::task::JoinHandle<Result<ShardInfo, AppError>>,
}

impl RemoteShardWriter {
    pub fn spawn(target: PeerShardTarget) -> Result<Self, AppError> {
        let permit = acquire_peer_operation(&target.node_id, PeerOperation::Write)?;
        let (writer, reader) = tokio::io::duplex(1024 * 1024);
        let upload_target = target.clone();
        let handle = tokio::spawn(async move {
            let started = Instant::now();
            let label = if endpoint_is_native(&upload_target.endpoint) {
                "peer.write.native"
            } else {
                "peer.write.http"
            };
            let result = if endpoint_is_native(&upload_target.endpoint) {
                native_put_peer_shard(&upload_target, reader).await
            } else {
                http_put_peer_shard(&upload_target, reader).await
            };
            record_io_latency(
                label,
                started.elapsed(),
                upload_target.expected_len,
                result.is_ok(),
            );
            match result {
                Ok(info) => {
                    permit.complete();
                    Ok(info)
                }
                Err(err) => Err(err),
            }
        });
        Ok(Self {
            target,
            writer: Some(writer),
            handle,
        })
    }

    pub async fn finish(mut self) -> Result<ShardInfo, AppError> {
        if let Some(mut writer) = self.writer.take() {
            writer
                .shutdown()
                .await
                .map_err(|e| AppError::Io(e, "shutdown remote N2 shard upload stream".into()))?;
        }
        let mut info = (&mut self.handle).await.map_err(|e| {
            AppError::InternalServerError(format!("remote N2 shard writer join failed: {e}"))
        })??;
        info.absolute_path = encode_peer_shard_locator(&self.target);
        Ok(info)
    }
}

impl Drop for RemoteShardWriter {
    fn drop(&mut self) {
        // Dropping JoinHandle alone detaches the transfer. Keep cancellation
        // bounded instead; receivers still enforce length/SHA before publish.
        self.handle.abort();
    }
}

pub async fn put_peer_shard_bytes(
    target: PeerShardTarget,
    bytes: &[u8],
) -> Result<ShardInfo, AppError> {
    let mut writer = RemoteShardWriter::spawn(target)?;
    writer
        .write_all(bytes)
        .await
        .map_err(|e| AppError::Io(e, "write repaired N2 peer shard stream".into()))?;
    writer.finish().await
}

async fn http_put_peer_shard(
    target: &PeerShardTarget,
    reader: DuplexStream,
) -> Result<ShardInfo, AppError> {
    let url = peer_shard_url(target);
    let client = peer_http_client()?;
    let body = reqwest::Body::wrap_stream(ReaderStream::new(reader));
    let mut req = client.put(url).body(body);
    if let Some(secret) = cluster_secret() {
        req = req.header("x-n2-cluster-secret", secret);
    }
    let response = req
        .timeout(peer_transfer_timeout(target.expected_len))
        .send()
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("peer shard upload failed: {e}")))?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        return Err(AppError::ServiceUnavailable(format!(
            "peer shard upload returned {}: {}",
            status, text
        )));
    }
    response
        .json::<ShardInfo>()
        .await
        .map_err(|e| AppError::InternalServerError(format!("decode peer shard response: {e}")))
}

async fn native_put_peer_shard(
    target: &PeerShardTarget,
    mut reader: DuplexStream,
) -> Result<ShardInfo, AppError> {
    let addr = native_socket_addr(&target.endpoint)?;
    let mut stream = timeout(PEER_CONNECT_TIMEOUT, TcpStream::connect(&addr))
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "connect N2 native peer {} at {} timed out",
                target.node_id, addr
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!(
                "connect N2 native peer {} at {} failed: {}",
                target.node_id, addr, e
            ))
        })?;
    let _ = stream.set_nodelay(true);
    let payload = NativePeerShardPayload {
        secret: cluster_secret(),
        pool_id: target.pool_id.clone(),
        pool_generation: target.pool_generation,
        slot_id: target.node_id.clone(),
        instance_id: target.instance_id.clone(),
        binding_epoch: target.binding_epoch,
        fragment: target.fragment.clone(),
        root_id: target.root_id.clone(),
        object_id: target.object_id.clone(),
        version_id: target.version_id.clone(),
        part_index: target.part_index,
        shard_index: target.shard_index,
        parity: target.is_parity,
        expected_len: Some(target.expected_len),
        offset: None,
        length: None,
        deferred_durability: target.deferred_durability,
        repair: target.repair,
        expected_checksum: target.expected_checksum.clone(),
    };
    let request = NativeRequest {
        id: format!("peer-put-{}", Ulid::new()),
        op: "peer.shard.put".to_string(),
        payload: Some(serde_json::to_string(&payload).map_err(|e| {
            AppError::InternalServerError(format!("encode native peer put payload: {e}"))
        })?),
        body: None,
        body_len: Some(target.expected_len),
    };
    timeout(
        PEER_WRITE_TIMEOUT,
        write_request_control(&mut stream, &request),
    )
    .await
    .map_err(|_| {
        AppError::ServiceUnavailable(format!(
            "write N2 native peer put control to {} timed out",
            target.node_id
        ))
    })?
    .map_err(|e| {
        AppError::ServiceUnavailable(format!("write N2 native peer put control failed: {e}"))
    })?;
    let copied = copy_with_idle_timeout(&mut reader, &mut stream, peer_transfer_idle_timeout())
        .await
        .map_err(|e| {
            AppError::ServiceUnavailable(format!("stream N2 native peer shard failed: {e}"))
        })?;
    if copied != target.expected_len {
        return Err(AppError::ServiceUnavailable(format!(
            "stream N2 native peer shard to {} ended after {} bytes, expected {}",
            target.node_id, copied, target.expected_len
        )));
    }
    timeout(PEER_WRITE_TIMEOUT, stream.shutdown())
        .await
        .map_err(|_| {
            AppError::ServiceUnavailable(format!(
                "shutdown N2 native peer shard stream to {} timed out",
                target.node_id
            ))
        })?
        .map_err(|e| {
            AppError::ServiceUnavailable(format!(
                "shutdown N2 native peer shard stream failed: {e}"
            ))
        })?;
    let response = timeout(
        peer_transfer_idle_timeout(),
        read_native_peer_response(&mut stream),
    )
    .await
    .map_err(|_| {
        AppError::ServiceUnavailable(format!(
            "read N2 native peer put response from {} timed out",
            target.node_id
        ))
    })??;
    if !response.ok {
        return Err(native_peer_response_error("put", target, &response));
    }
    let payload = response.payload.as_deref().ok_or_else(|| {
        AppError::InternalServerError("N2 native peer put response missing payload".into())
    })?;
    serde_json::from_str::<ShardInfo>(payload).map_err(|e| {
        AppError::InternalServerError(format!("decode N2 native peer put response: {e}"))
    })
}

impl AsyncWrite for RemoteShardWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "peer closed"))?;
        match Pin::new(writer).poll_write(cx, buf) {
            Poll::Ready(Ok(bytes)) => {
                record_peer_progress(&self.target.node_id, PeerOperation::Write, bytes as u64);
                Poll::Ready(Ok(bytes))
            }
            other => other,
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "peer closed"))?;
        Pin::new(writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(writer) = self.writer.as_mut() {
            Pin::new(writer).poll_shutdown(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
