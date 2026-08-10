use crate::fs::fs_constants::{CHUNK_PREFIX, TMP_EXT};
use tokio::fs::ReadDir;

pub async fn parse_uploaded_parts(reader: &mut ReadDir) -> Vec<u64> {
    let mut chunks = Vec::new();
    while let Ok(Some(entry)) = reader.next_entry().await {
        if let Some(name_str) = entry.file_name().to_str() {
            if name_str.starts_with(CHUNK_PREFIX) && !name_str.ends_with(TMP_EXT) {
                if let Some(index_str) = name_str.strip_prefix(CHUNK_PREFIX) {
                    if let Ok(index) = index_str.parse::<u64>() {
                        chunks.push(index);
                    }
                }
            }
        }
    }
    chunks
}
