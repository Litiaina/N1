use super::*;

pub(crate) fn create_multipart_reader(
    metadata: &ObjectMetadata,
) -> StreamReader<impl Stream<Item = std::io::Result<Bytes>> + Unpin, Bytes> {
    create_multipart_range_reader(metadata, 0, metadata.total_size_bytes)
}

pub(crate) fn create_multipart_range_reader(
    metadata: &ObjectMetadata,
    offset: u64,
    length: u64,
) -> StreamReader<impl Stream<Item = std::io::Result<Bytes>> + Unpin, Bytes> {
    let mut sorted_parts = metadata.parts.clone();
    sorted_parts.sort_by_key(|part| part.part_index);
    let mut parts: std::collections::VecDeque<PartMetadata> = sorted_parts.into();
    let mut cursor = offset;
    while parts.front().is_some_and(|part| cursor >= part.size_bytes) {
        cursor -= parts.pop_front().unwrap().size_bytes;
    }
    let stripe_size_bytes = metadata.stripe_size_bytes;

    struct ReaderState {
        parts_queue: std::collections::VecDeque<PartMetadata>,
        blob_dir: PathBuf,
        cursor_in_part: u64,
        file_cache: HashMap<String, File>,
        cluster_cache: crate::distribution::download::VerifiedShardCache,
        stripe_size: usize,
        remaining: u64,
    }

    let initial_state = ReaderState {
        parts_queue: parts,
        blob_dir: PathBuf::new(),
        cursor_in_part: cursor,
        file_cache: HashMap::new(),
        cluster_cache: crate::distribution::download::VerifiedShardCache::for_sequential_export(),
        stripe_size: stripe_size_bytes,
        remaining: length,
    };

    let stream = futures_util::stream::unfold(initial_state, move |mut state| async move {
        let chunk_req_size = ARCHIVE_CHUNK_REQ_SIZE;
        let mut buf = Vec::with_capacity(chunk_req_size);

        loop {
            if state.remaining == 0 {
                return if buf.is_empty() {
                    None
                } else {
                    Some((Ok(Bytes::from(buf)), state))
                };
            }
            let part = match state.parts_queue.front() {
                Some(p) => p,
                None => {
                    return Some((
                        Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "immutable object ended before its declared length",
                        )),
                        state,
                    ));
                }
            };

            if state.cursor_in_part >= part.size_bytes {
                state.parts_queue.pop_front();
                state.cursor_in_part = 0;
                state.file_cache.clear();
                state.cluster_cache.clear();
                continue;
            }

            let bytes_left_in_part = part.size_bytes - state.cursor_in_part;
            let bytes_to_fill = chunk_req_size - buf.len();
            let bytes_to_read = bytes_left_in_part
                .min(bytes_to_fill as u64)
                .min(state.remaining) as usize;

            if bytes_to_read == 0 {
                state.parts_queue.pop_front();
                state.cursor_in_part = 0;
                state.file_cache.clear();
                continue;
            }

            let data_shards_info: Vec<&ShardInfo> =
                part.shards.iter().filter(|s| !s.is_parity).collect();
            let data_shards_count = data_shards_info.len() as u64;

            if data_shards_count == 0 || state.stripe_size == 0 {
                return Some((Err(std::io::Error::other("Corrupt metadata")), state));
            }

            let mut temp_bytes_read = 0;
            while temp_bytes_read < bytes_to_read {
                let stripe_block_size = state.stripe_size as u64;
                let stripe_size = stripe_block_size * data_shards_count;

                let offset_in_stripe = state.cursor_in_part % stripe_size;
                let shard_index = (offset_in_stripe / stripe_block_size) as usize;
                let offset_in_block = offset_in_stripe % stripe_block_size;

                let stripe_index = state.cursor_in_part / stripe_size;
                let file_offset = (stripe_index * stripe_block_size) + offset_in_block;

                let bytes_left_in_block = stripe_block_size - offset_in_block;
                let remaining_req = bytes_to_read - temp_bytes_read;
                let chunk_len = std::cmp::min(remaining_req as u64, bytes_left_in_block) as usize;

                let part_arc = Arc::new(part.clone());

                let read = if crate::distribution::download::part_has_remote_shards(part) {
                    crate::distribution::download::read_cluster_data_chunk_cached(
                        part,
                        shard_index,
                        file_offset,
                        chunk_len,
                        &mut state.cluster_cache,
                    )
                    .await
                } else {
                    read_or_recover_chunk(
                        &mut state.file_cache,
                        &state.blob_dir,
                        part_arc,
                        shard_index,
                        file_offset,
                        chunk_len,
                        state.stripe_size,
                    )
                    .await
                };
                match read {
                    Ok(data) => {
                        if data.len() != chunk_len {
                            return Some((
                                Err(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "short immutable shard read",
                                )),
                                state,
                            ));
                        }
                        buf.extend_from_slice(&data);
                        state.remaining -= chunk_len as u64;
                        state.cursor_in_part += chunk_len as u64;
                        temp_bytes_read += chunk_len;
                    }
                    Err(e) => {
                        return Some((Err(std::io::Error::other(e.to_string())), state));
                    }
                }
            }

            if buf.len() >= chunk_req_size {
                return Some((Ok(Bytes::from(buf)), state));
            }
        }
    });

    StreamReader::new(Box::pin(stream))
}
