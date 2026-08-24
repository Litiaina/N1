use super::*;

pub(super) fn create_multipart_reader(
    metadata: &ObjectMetadata,
) -> StreamReader<impl Stream<Item = std::io::Result<Bytes>> + Unpin, Bytes> {
    let parts: std::collections::VecDeque<PartMetadata> = metadata.parts.clone().into();
    let stripe_size_bytes = metadata.stripe_size_bytes;

    struct ReaderState {
        parts_queue: std::collections::VecDeque<PartMetadata>,
        blob_dir: PathBuf,
        cursor_in_part: u64,
        file_cache: HashMap<String, File>,
        stripe_size: usize,
    }

    let initial_state = ReaderState {
        parts_queue: parts,
        blob_dir: PathBuf::new(),
        cursor_in_part: 0,
        file_cache: HashMap::new(),
        stripe_size: stripe_size_bytes,
    };

    let stream = futures_util::stream::unfold(initial_state, move |mut state| async move {
        let chunk_req_size = ARCHIVE_CHUNK_REQ_SIZE;
        let mut buf = Vec::with_capacity(chunk_req_size);

        loop {
            let part = match state.parts_queue.front() {
                Some(p) => p,
                None => {
                    if !buf.is_empty() {
                        return Some((Ok(Bytes::from(buf)), state));
                    }
                    return None;
                }
            };

            if state.cursor_in_part >= part.size_bytes {
                state.parts_queue.pop_front();
                state.cursor_in_part = 0;
                state.file_cache.clear();
                continue;
            }

            let bytes_left_in_part = part.size_bytes - state.cursor_in_part;
            let bytes_to_fill = chunk_req_size - buf.len();
            let bytes_to_read = std::cmp::min(bytes_left_in_part, bytes_to_fill as u64) as usize;

            if bytes_to_read == 0 {
                state.parts_queue.pop_front();
                state.cursor_in_part = 0;
                state.file_cache.clear();
                continue;
            }

            let data_shards_info: Vec<&ShardInfo> =
                part.shards.iter().filter(|s| !s.is_parity).collect();
            let data_shards_count = data_shards_info.len() as u64;

            if data_shards_count == 0 {
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

                match read_or_recover_chunk(
                    &mut state.file_cache,
                    &state.blob_dir,
                    part_arc,
                    shard_index,
                    file_offset,
                    chunk_len,
                    state.stripe_size,
                )
                .await
                {
                    Ok(data) => {
                        buf.extend_from_slice(&data);
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
