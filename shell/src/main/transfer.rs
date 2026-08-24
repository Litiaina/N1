use super::*;

pub(super) async fn upload_file(
    client: &mut N2NativeClient,
    args: UploadArgs,
    cli: &Cli,
    json_output: bool,
) -> NativeClientResult<()> {
    let mut file = File::open(&args.file).await?;
    let size = file.metadata().await?.len();
    let content_type = args
        .content_type
        .unwrap_or_else(|| content_type_from_path(&args.file));

    if size <= ONE_SHOT_LIMIT {
        let checksum = if args.sha256 {
            Some(sha256_range(&mut file, 0, size).await?)
        } else {
            None
        };
        file.seek(std::io::SeekFrom::Start(0)).await?;
        let response = client
            .operation_reader(
                "upload.one_shot",
                &json!({
                    "object_key": args.object_key,
                    "content_type": content_type,
                    "io_gate": !args.no_io_gate,
                    "checksum": checksum,
                }),
                &mut file,
                size,
            )
            .await?;
        return print_response(response, json_output);
    }

    let part_size = args.part_size.max(1024 * 1024);
    let expected_parts = size.div_ceil(part_size);
    let parallel_parts = args
        .parallel_parts
        .max(1)
        .min(expected_parts.max(1) as usize);
    let multipart_init_op = if args.durable {
        "upload.multipart.durable.init"
    } else {
        "upload.multipart.standard.init"
    };
    let multipart_part_op = if args.durable {
        "upload.multipart.durable.put_part"
    } else {
        "upload.multipart.standard.put_part"
    };
    let multipart_finalize_op = if args.durable {
        "upload.multipart.durable.finalize"
    } else {
        "upload.multipart.standard.finalize"
    };
    let init: Value = client
        .operation_json(
            multipart_init_op,
            &json!({
                "object_key": args.object_key,
                "content_type": content_type,
                "total_size_bytes": size,
                "expected_parts": expected_parts,
                "part_size_bytes": part_size,
            }),
            None,
        )
        .await?;
    let version_id = required_response_string(&init, "version_id")?;
    let session_id = required_response_string(&init, "session_id")?;

    if parallel_parts == 1 {
        for part_index in 0..expected_parts {
            let start = part_index.saturating_mul(part_size);
            let part_len = part_size.min(size.saturating_sub(start));
            let checksum = if args.sha256 {
                Some(sha256_range(&mut file, start, part_len).await?)
            } else {
                None
            };
            file.seek(std::io::SeekFrom::Start(start)).await?;
            let mut part_reader = (&mut file).take(part_len);
            client
                .operation_reader(
                    multipart_part_op,
                    &json!({
                        "version_id": version_id,
                        "part_index": part_index,
                        "io_gate": !args.no_io_gate,
                        "checksum": checksum,
                    }),
                    &mut part_reader,
                    part_len,
                )
                .await?;
            eprintln!(
                "uploaded part {}/{} ({})",
                part_index + 1,
                expected_parts,
                human_bytes(start.saturating_add(part_len))
            );
        }
    } else {
        let fragment = cli
            .fragment
            .clone()
            .ok_or_else(|| NativeClientError::protocol("parallel upload requires --fragment"))?;
        let secret = cli
            .secret
            .clone()
            .ok_or_else(|| NativeClientError::protocol("parallel upload requires --secret"))?;
        let native_url = cli.native_url.clone();
        let file_path = args.file.clone();
        let mut next_part = 0u64;
        let mut completed_parts = 0u64;
        let mut workers = JoinSet::new();

        while next_part < expected_parts || !workers.is_empty() {
            while next_part < expected_parts && workers.len() < parallel_parts {
                let part_index = next_part;
                next_part = next_part.saturating_add(1);
                workers.spawn(upload_part_with_new_client(
                    native_url.clone(),
                    fragment.clone(),
                    secret.clone(),
                    file_path.clone(),
                    version_id.clone(),
                    part_index,
                    part_size,
                    size,
                    args.sha256,
                    !args.no_io_gate,
                    multipart_part_op,
                ));
            }

            let Some(joined) = workers.join_next().await else {
                continue;
            };
            let (part_index, uploaded_bytes) = joined.map_err(|err| {
                NativeClientError::protocol(format!("upload worker failed: {err}"))
            })??;
            completed_parts = completed_parts.saturating_add(1);
            eprintln!(
                "uploaded part {}/{} ({})",
                part_index + 1,
                expected_parts,
                human_bytes(uploaded_bytes)
            );
        }

        if completed_parts != expected_parts {
            return Err(NativeClientError::protocol(format!(
                "uploaded {completed_parts} parts, expected {expected_parts}"
            )));
        }
    }

    let finalized = client
        .operation(
            multipart_finalize_op,
            &json!({
                "object_key": args.object_key,
                "content_type": content_type,
                "total_size_bytes": size,
                "expected_parts": expected_parts,
                "part_size_bytes": part_size,
                "version_id": version_id,
                "session_id": session_id,
            }),
            None,
        )
        .await?;
    print_response(finalized, json_output)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn upload_part_with_new_client(
    native_url: String,
    fragment: String,
    secret: String,
    file_path: PathBuf,
    version_id: String,
    part_index: u64,
    part_size: u64,
    total_size: u64,
    sha256: bool,
    io_gate: bool,
    multipart_part_op: &'static str,
) -> NativeClientResult<(u64, u64)> {
    let mut client = N2NativeClient::connect(&native_url)
        .await?
        .with_request_prefix(format!("n2-shell-part-{part_index}"));
    client.auth_fragment(&fragment, &secret).await?;

    let start = part_index.saturating_mul(part_size);
    let part_len = part_size.min(total_size.saturating_sub(start));
    let mut file = File::open(&file_path).await?;
    let checksum = if sha256 {
        Some(sha256_range(&mut file, start, part_len).await?)
    } else {
        None
    };
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let mut part_reader = (&mut file).take(part_len);
    client
        .operation_reader(
            multipart_part_op,
            &json!({
                "version_id": version_id,
                "part_index": part_index,
                "io_gate": io_gate,
                "checksum": checksum,
            }),
            &mut part_reader,
            part_len,
        )
        .await?;
    Ok((part_index, start.saturating_add(part_len)))
}

pub(super) async fn sha256_range(
    file: &mut File,
    offset: u64,
    len: u64,
) -> NativeClientResult<String> {
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut remaining = len;
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut hasher = Sha256::new();
    while remaining > 0 {
        let take = buffer.len().min(remaining as usize);
        let read = file.read(&mut buffer[..take]).await?;
        if read == 0 {
            return Err(NativeClientError::protocol(format!(
                "file ended while calculating SHA-256: expected {len} bytes from offset {offset}"
            )));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(super) async fn download_file(
    client: &mut N2NativeClient,
    args: DownloadArgs,
) -> NativeClientResult<()> {
    let mut output = std::fs::File::create(&args.file)?;
    let payload = json!({
        "object_key": args.object_key,
        "version_id": args.version_id,
    });
    let (response, bytes) = client
        .stream_response("object.download_stream", &payload, |chunk| {
            output.write_all(chunk)?;
            Ok(())
        })
        .await?;
    ensure_native_ok(&response)?;
    eprintln!(
        "downloaded {} to {}",
        human_bytes(bytes),
        args.file.display()
    );
    Ok(())
}
