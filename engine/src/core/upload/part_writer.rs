use super::*;

pub(super) async fn write_part_blob_to_disk(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    part_index: u64,
    content_length: u64,
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
    body: Body,
    expected_checksum: Option<&str>,
    strict_durability: bool,
) -> Result<PartMetadata, AppError> {
    let body_stream = body.into_data_stream().map(|chunk_result| {
        chunk_result.map_err(|e| {
            AppError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
                "stream read".into(),
            )
        })
    });
    write_part_blob_to_disk_from_stream(
        path_manager,
        fragment,
        object_key,
        version_id,
        part_index,
        content_length,
        data_shards,
        parity_shards,
        block_size,
        body_stream,
        expected_checksum,
        strict_durability,
    )
    .await
}

pub(super) async fn write_part_blob_to_disk_from_stream<S>(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    part_index: u64,
    content_length: u64,
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
    mut body_stream: S,
    expected_checksum: Option<&str>,
    strict_durability: bool,
) -> Result<PartMetadata, AppError>
where
    S: Stream<Item = Result<Bytes, AppError>> + Unpin,
{
    let total_shards = data_shards + parity_shards;

    if data_shards == 0 || total_shards == 0 {
        return Err(AppError::InternalServerError(
            "invalid erasure coding layout".into(),
        ));
    }

    if block_size == 0 || block_size % 4096 != 0 {
        return Err(AppError::InternalServerError(format!(
            "stripe_size_bytes must be a non-zero multiple of 4096 for O_DIRECT (got {})",
            block_size
        )));
    }

    let attempt_id = Ulid::new().to_string();
    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = (content_length + stripe_data_width - 1) / stripe_data_width;
    let final_shard_len = stripe_count * block_size as u64;

    preflight_blob_placement_capacity_for_parts(
        part_index,
        &[content_length],
        data_shards,
        parity_shards,
        block_size,
    )
    .await?;

    let blob_roots = path_manager.get_storage_roots();
    let num_disks = blob_roots.len();
    if num_disks == 0 {
        return Err(AppError::InternalServerError(
            "No blob storage disks configured".into(),
        ));
    }

    let use_buffered_small_write =
        !strict_durability && content_length <= SMALL_BUFFERED_WRITE_LIMIT;
    let mut writers: Vec<Option<UploadShardWriter>> = Vec::with_capacity(total_shards);
    let mut tmp_paths: Vec<PathBuf> = Vec::with_capacity(total_shards);
    let mut stable_paths: Vec<PathBuf> = Vec::with_capacity(total_shards);
    let mut stable_names: Vec<String> = Vec::with_capacity(total_shards);
    let mut successful_opens = 0usize;

    for i in 0..total_shards {
        let is_parity = i >= data_shards;
        let ext = if is_parity {
            PARITY_SUFFIX
        } else {
            SHARD_SUFFIX
        };

        let stable_name = format!("part_{}_shard_{}{}", part_index, i, ext);
        let tmp_name = format!("{}.tmp.{}", stable_name, attempt_id);
        let disk_idx = (part_index as usize + i) % num_disks;
        let target_disk = &blob_roots[disk_idx];
        let shard_dir =
            path_manager.get_shard_dir_on_disk(target_disk, fragment, object_key, version_id);

        let tmp_path = shard_dir.join(&tmp_name);
        let stable_path = shard_dir.join(&stable_name);

        if use_buffered_small_write {
            match create_buffered_file(&tmp_path).await {
                Ok(file) => {
                    writers.push(Some(UploadShardWriter::Buffered(file)));
                    successful_opens += 1;
                }
                Err(err) => {
                    warn!(
                        "blob shard open failed for {} on {}: {}",
                        stable_name,
                        target_disk.display(),
                        err
                    );
                    writers.push(None);
                }
            }
        } else {
            match create_direct_file(&tmp_path).await {
                Ok(file) => {
                    if strict_durability {
                        let file_clone = file
                            .try_clone()
                            .map_err(|e| AppError::Io(e, "clone file handle".into()))?;
                        let prealloc_result = tokio::task::spawn_blocking(move || {
                            file_clone.set_len(final_shard_len)
                        })
                        .await;

                        if let Ok(Ok(_)) = prealloc_result {
                            writers.push(Some(UploadShardWriter::Direct(DirectWriter::new(file))));
                            successful_opens += 1;
                        } else {
                            warn!(
                                "blob shard preallocation failed for {} on {}",
                                stable_name,
                                target_disk.display()
                            );
                            writers.push(None);
                        }
                    } else {
                        writers.push(Some(UploadShardWriter::Direct(DirectWriter::new(file))));
                        successful_opens += 1;
                    }
                }
                Err(err) => {
                    warn!(
                        "blob shard open failed for {} on {}: {}",
                        stable_name,
                        target_disk.display(),
                        err
                    );
                    writers.push(None);
                }
            }
        }

        tmp_paths.push(tmp_path);
        stable_paths.push(stable_path);
        stable_names.push(stable_name);
    }

    if successful_opens < data_shards {
        cleanup_files_best_effort(&tmp_paths).await;
        return Err(AppError::InternalServerError(format!(
            "Insufficient healthy disks for write quorum. Need {}, got {}.",
            data_shards, successful_opens
        )));
    }

    let write_result = async {
        let calc_total_checksum = expected_checksum.is_some();
        let mut stream_writer = ErasureStreamWriter::new(
            writers,
            data_shards,
            parity_shards,
            content_length,
            calc_total_checksum,
            block_size,
        )?;

        let mut total_bytes_read = 0u64;

        while let Some(chunk_result) = body_stream.next().await {
            let chunk = chunk_result?;

            if chunk.is_empty() {
                continue;
            }

            total_bytes_read += chunk.len() as u64;
            if total_bytes_read > content_length {
                return Err(AppError::BadRequest("Body exceeds Content-Length".into()));
            }

            stream_writer.write(&chunk).await?;
        }

        if total_bytes_read != content_length {
            return Err(AppError::BadRequest(format!(
                "Incomplete body: got {} bytes, expected {}",
                total_bytes_read, content_length
            )));
        }

        stream_writer.finalize().await?;

        if let Some(expected) = expected_checksum {
            let calculated = stream_writer.get_total_checksum();
            if let Some(calc_str) = calculated.as_ref() {
                if !expected.eq_ignore_ascii_case(calc_str) {
                    return Err(AppError::BadRequest("Checksum mismatch".into()));
                }
            }
        }

        let shard_checksums = stream_writer.get_shard_checksums();
        let total_checksum = stream_writer.get_total_checksum();
        let writers = stream_writer.into_writers();

        if strict_durability {
            let sync_futs = writers.into_iter().filter_map(|w_opt| {
                w_opt.map(|w| async move {
                    if let Err(e) = w.sync_data().await {
                        warn!("Failed to fsync shard: {}", e);
                        return Err(AppError::Io(e, "fsync shard".into()));
                    }
                    Ok(())
                })
            });

            let sync_results = join_all(sync_futs).await;
            for r in sync_results {
                r?;
            }
        }

        let publish_results = join_all((0..total_shards).map(|i| {
            let tmp_path = tmp_paths[i].clone();
            let stable_path = stable_paths[i].clone();
            let expected_sha256 = shard_checksums.get(i).cloned();
            async move {
                publish_one_shard(
                    tmp_path.as_path(),
                    stable_path.as_path(),
                    final_shard_len,
                    expected_sha256.as_deref(),
                )
                .await?;
                Ok::<Option<PathBuf>, AppError>(stable_path.parent().map(Path::to_path_buf))
            }
        }))
        .await;

        let mut dirs_to_sync: Vec<PathBuf> = Vec::new();
        for result in publish_results {
            if let Some(dir) = result? {
                dirs_to_sync.push(dir);
            }
        }

        dirs_to_sync.sort();
        dirs_to_sync.dedup();

        if strict_durability {
            let _ = join_all(dirs_to_sync.iter().map(|d| sync_dir(d.as_path()))).await;
        }

        let mut shards_info = Vec::with_capacity(total_shards);
        for i in 0..total_shards {
            shards_info.push(ShardInfo {
                shard_index: i,
                file_name: stable_names[i].clone(),
                absolute_path: stable_paths[i].to_string_lossy().to_string(),
                size_bytes: final_shard_len,
                checksum: shard_checksums.get(i).cloned().unwrap_or_default(),
                block_checksums: Vec::new(),
                checksum_block_size: 0,
                is_parity: i >= data_shards,
            });
        }

        Ok(PartMetadata {
            part_index,
            size_bytes: total_bytes_read,
            created_at: Utc::now().timestamp(),
            checksum: if calc_total_checksum {
                total_checksum
            } else {
                None
            },
            shards: shards_info,
        })
    }
    .await;

    if write_result.is_err() {
        cleanup_files_best_effort(&tmp_paths).await;
        cleanup_files_best_effort(&stable_paths).await;
    }

    write_result
}

pub(super) async fn write_part_blob_to_disk_from_reader<R>(
    path_manager: &ObjectPathManager,
    fragment: &str,
    object_key: &str,
    version_id: &str,
    part_index: u64,
    content_length: u64,
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
    reader: &mut R,
    expected_checksum: Option<&str>,
    strict_durability: bool,
) -> Result<PartMetadata, AppError>
where
    R: AsyncRead + Unpin + Send,
{
    let total_shards = data_shards + parity_shards;

    if data_shards == 0 || total_shards == 0 {
        return Err(AppError::InternalServerError(
            "invalid erasure coding layout".into(),
        ));
    }

    if block_size == 0 || block_size % 4096 != 0 {
        return Err(AppError::InternalServerError(format!(
            "stripe_size_bytes must be a non-zero multiple of 4096 for O_DIRECT (got {})",
            block_size
        )));
    }

    let attempt_id = Ulid::new().to_string();
    let stripe_data_width = block_size as u64 * data_shards as u64;
    let stripe_count = (content_length + stripe_data_width - 1) / stripe_data_width;
    let final_shard_len = stripe_count * block_size as u64;

    preflight_blob_placement_capacity_for_parts(
        part_index,
        &[content_length],
        data_shards,
        parity_shards,
        block_size,
    )
    .await?;

    let blob_roots = path_manager.get_storage_roots();
    let num_disks = blob_roots.len();
    if num_disks == 0 {
        return Err(AppError::InternalServerError(
            "No blob storage disks configured".into(),
        ));
    }

    let use_buffered_small_write =
        !strict_durability && content_length <= SMALL_BUFFERED_WRITE_LIMIT;
    let mut writers: Vec<Option<UploadShardWriter>> = Vec::with_capacity(total_shards);
    let mut tmp_paths: Vec<PathBuf> = Vec::with_capacity(total_shards);
    let mut stable_paths: Vec<PathBuf> = Vec::with_capacity(total_shards);
    let mut stable_names: Vec<String> = Vec::with_capacity(total_shards);
    let mut successful_opens = 0usize;

    for i in 0..total_shards {
        let is_parity = i >= data_shards;
        let ext = if is_parity {
            PARITY_SUFFIX
        } else {
            SHARD_SUFFIX
        };

        let stable_name = format!("part_{}_shard_{}{}", part_index, i, ext);
        let tmp_name = format!("{}.tmp.{}", stable_name, attempt_id);
        let disk_idx = (part_index as usize + i) % num_disks;
        let target_disk = &blob_roots[disk_idx];
        let shard_dir =
            path_manager.get_shard_dir_on_disk(target_disk, fragment, object_key, version_id);

        let tmp_path = shard_dir.join(&tmp_name);
        let stable_path = shard_dir.join(&stable_name);

        if use_buffered_small_write {
            match create_buffered_file(&tmp_path).await {
                Ok(file) => {
                    writers.push(Some(UploadShardWriter::Buffered(file)));
                    successful_opens += 1;
                }
                Err(err) => {
                    warn!(
                        "blob shard open failed for {} on {}: {}",
                        stable_name,
                        target_disk.display(),
                        err
                    );
                    writers.push(None);
                }
            }
        } else {
            match create_direct_file(&tmp_path).await {
                Ok(file) => {
                    if strict_durability {
                        let file_clone = file
                            .try_clone()
                            .map_err(|e| AppError::Io(e, "clone file handle".into()))?;
                        let prealloc_result = tokio::task::spawn_blocking(move || {
                            file_clone.set_len(final_shard_len)
                        })
                        .await;

                        if let Ok(Ok(_)) = prealloc_result {
                            writers.push(Some(UploadShardWriter::Direct(DirectWriter::new(file))));
                            successful_opens += 1;
                        } else {
                            warn!(
                                "blob shard preallocation failed for {} on {}",
                                stable_name,
                                target_disk.display()
                            );
                            writers.push(None);
                        }
                    } else {
                        writers.push(Some(UploadShardWriter::Direct(DirectWriter::new(file))));
                        successful_opens += 1;
                    }
                }
                Err(err) => {
                    warn!(
                        "blob shard open failed for {} on {}: {}",
                        stable_name,
                        target_disk.display(),
                        err
                    );
                    writers.push(None);
                }
            }
        }

        tmp_paths.push(tmp_path);
        stable_paths.push(stable_path);
        stable_names.push(stable_name);
    }

    if successful_opens < data_shards {
        cleanup_files_best_effort(&tmp_paths).await;
        return Err(AppError::InternalServerError(format!(
            "Insufficient healthy disks for write quorum. Need {}, got {}.",
            data_shards, successful_opens
        )));
    }

    let write_result = async {
        let calc_total_checksum = expected_checksum.is_some();
        let mut stream_writer = ErasureStreamWriter::new(
            writers,
            data_shards,
            parity_shards,
            content_length,
            calc_total_checksum,
            block_size,
        )?;

        let total_bytes_read = stream_writer
            .write_exact_from_reader(reader, content_length)
            .await?;

        stream_writer.finalize().await?;

        if let Some(expected) = expected_checksum {
            let calculated = stream_writer.get_total_checksum();
            if let Some(calc_str) = calculated.as_ref() {
                if !expected.eq_ignore_ascii_case(calc_str) {
                    return Err(AppError::BadRequest("Checksum mismatch".into()));
                }
            }
        }

        let shard_checksums = stream_writer.get_shard_checksums();
        let total_checksum = stream_writer.get_total_checksum();
        let writers = stream_writer.into_writers();

        if strict_durability {
            let sync_futs = writers.into_iter().filter_map(|w_opt| {
                w_opt.map(|w| async move {
                    if let Err(e) = w.sync_data().await {
                        warn!("Failed to fsync shard: {}", e);
                        return Err(AppError::Io(e, "fsync shard".into()));
                    }
                    Ok(())
                })
            });

            let sync_results = join_all(sync_futs).await;
            for r in sync_results {
                r?;
            }
        }

        let publish_results = join_all((0..total_shards).map(|i| {
            let tmp_path = tmp_paths[i].clone();
            let stable_path = stable_paths[i].clone();
            let expected_sha256 = shard_checksums.get(i).cloned();
            async move {
                publish_one_shard(
                    tmp_path.as_path(),
                    stable_path.as_path(),
                    final_shard_len,
                    expected_sha256.as_deref(),
                )
                .await?;
                Ok::<Option<PathBuf>, AppError>(stable_path.parent().map(Path::to_path_buf))
            }
        }))
        .await;

        let mut dirs_to_sync: Vec<PathBuf> = Vec::new();
        for result in publish_results {
            if let Some(dir) = result? {
                dirs_to_sync.push(dir);
            }
        }

        dirs_to_sync.sort();
        dirs_to_sync.dedup();

        if strict_durability {
            let _ = join_all(dirs_to_sync.iter().map(|d| sync_dir(d.as_path()))).await;
        }

        let mut shards_info = Vec::with_capacity(total_shards);
        for i in 0..total_shards {
            shards_info.push(ShardInfo {
                shard_index: i,
                file_name: stable_names[i].clone(),
                absolute_path: stable_paths[i].to_string_lossy().to_string(),
                size_bytes: final_shard_len,
                checksum: shard_checksums.get(i).cloned().unwrap_or_default(),
                block_checksums: Vec::new(),
                checksum_block_size: 0,
                is_parity: i >= data_shards,
            });
        }

        Ok(PartMetadata {
            part_index,
            size_bytes: total_bytes_read,
            created_at: Utc::now().timestamp(),
            checksum: if calc_total_checksum {
                total_checksum
            } else {
                None
            },
            shards: shards_info,
        })
    }
    .await;

    if write_result.is_err() {
        cleanup_files_best_effort(&tmp_paths).await;
        cleanup_files_best_effort(&stable_paths).await;
    }

    write_result
}

pub(super) async fn cleanup_files_best_effort(paths: &[PathBuf]) {
    let futs = paths.iter().cloned().map(|p| async move {
        let _ = tokio_fs::remove_file(&p).await;
    });
    let _ = join_all(futs).await;
}

pub(super) async fn cleanup_blob_version_dirs_for_version(
    path_manager: &ObjectPathManager,
    fragment: &str,
    version_id: &str,
) {
    let mut dirs = Vec::new();
    for root in path_manager.get_storage_roots() {
        let objects = crate::core::fragment_identity::fragment_root_on(&root, fragment)
            .join(crate::core::constants::N2_BLOB_DIR);
        for entry in WalkDir::new(objects)
            .min_depth(6)
            .max_depth(6)
            .into_iter()
            .filter_map(Result::ok)
        {
            if entry.file_type().is_dir() && entry.file_name() == version_id {
                dirs.push(entry.into_path());
            }
        }
    }
    let futs = dirs.into_iter().map(|dir| async move {
        let parent = dir.parent().map(Path::to_path_buf);
        let _ = tokio_fs::remove_dir_all(&dir).await;
        if let Some(parent) = parent {
            let _ = sync_dir(&parent).await;
        }
    });
    let _ = join_all(futs).await;
}

pub(super) async fn sha256_file_hex(path: &Path) -> Result<String, AppError> {
    let mut f = tokio_fs::File::open(path)
        .await
        .map_err(|e| AppError::Io(e, format!("open for SHA-256 {}", path.display())))?;

    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 256 * 1024];

    loop {
        let n = f
            .read(&mut buf)
            .await
            .map_err(|e| AppError::Io(e, format!("read SHA-256 {}", path.display())))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hex::encode(hasher.finalize()))
}

pub(super) async fn publish_one_shard(
    tmp_path: &Path,
    stable_path: &Path,
    expected_len: u64,
    expected_sha256: Option<&str>,
) -> Result<(), AppError> {
    if !tokio_fs::try_exists(stable_path).await.unwrap_or(false) {
        tokio_fs::rename(tmp_path, stable_path).await.map_err(|e| {
            AppError::Io(
                e,
                format!(
                    "rename tmp->stable {} -> {}",
                    tmp_path.display(),
                    stable_path.display()
                ),
            )
        })?;
        return Ok(());
    }

    let len_ok = tokio_fs::metadata(stable_path)
        .await
        .map(|m| m.len() == expected_len)
        .unwrap_or(false);

    if len_ok {
        if let Some(exp) = expected_sha256.filter(|s| !s.is_empty()) {
            let got = sha256_file_hex(stable_path).await?;
            if got.eq_ignore_ascii_case(exp) {
                let _ = tokio_fs::remove_file(tmp_path).await;
                return Ok(());
            }
        } else {
            let _ = tokio_fs::remove_file(tmp_path).await;
            return Ok(());
        }
    }

    let _ = tokio_fs::remove_file(stable_path).await;
    tokio_fs::rename(tmp_path, stable_path).await.map_err(|e| {
        AppError::Io(
            e,
            format!(
                "rename tmp->stable {} -> {}",
                tmp_path.display(),
                stable_path.display()
            ),
        )
    })?;

    Ok(())
}

pub(super) fn is_tmp_attempt_shard(name: &str) -> bool {
    if !name.starts_with("part_") || !name.contains("_shard_") || !name.contains(".tmp.") {
        return false;
    }

    name.contains(&format!("{}.tmp.", SHARD_SUFFIX))
        || name.contains(&format!("{}.tmp.", PARITY_SUFFIX))
}

pub(super) async fn cleanup_blob_tmp_attempt_files_for_version(
    path_manager: &ObjectPathManager,
    fragment: &str,
    version_id: &str,
) {
    let mut dirs = Vec::new();
    for root in path_manager.get_storage_roots() {
        let objects = crate::core::fragment_identity::fragment_root_on(&root, fragment)
            .join(crate::core::constants::N2_BLOB_DIR);
        for entry in WalkDir::new(objects)
            .min_depth(6)
            .max_depth(6)
            .into_iter()
            .filter_map(Result::ok)
        {
            if entry.file_type().is_dir() && entry.file_name() == version_id {
                dirs.push(entry.into_path());
            }
        }
    }

    let futs = dirs.into_iter().map(|dir| async move {
        if !tokio_fs::try_exists(&dir).await.unwrap_or(false) {
            return;
        }

        let mut entries = match tokio_fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(_) => return,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let p = entry.path();
            if !p.is_file() {
                continue;
            }

            let name = entry.file_name().to_string_lossy().to_string();
            if is_tmp_attempt_shard(&name) {
                let _ = tokio_fs::remove_file(&p).await;
            }
        }

        let _ = sync_dir(&dir).await;
    });

    let _ = join_all(futs).await;
}
