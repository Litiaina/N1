use crc32fast::Hasher;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::fs::File;
use reed_solomon_simd::ReedSolomonEncoder;
use futures_util::future::join_all;
use std::sync::Arc;

use crate::enums::app_error_enum::AppError;

pub const STRIPE_BLOCK_SIZE: usize = 16 * 1024; 

pub struct ErasureStreamWriter {
    writers: Vec<BufWriter<File>>,
    data_shards: usize,
    parity_shards: usize,
    shard_len_limit: usize,
    buffer: Vec<u8>,
    parity_buffer: Vec<u8>,
    bytes_written_to_shards: usize,
    calc_checksum: bool,
    total_hasher: Option<Hasher>,
    shard_hashers: Option<Vec<Hasher>>,
}

impl ErasureStreamWriter {
    pub fn new(
        writers: Vec<BufWriter<File>>,
        data_shards: usize,
        parity_shards: usize,
        shard_len_limit: usize,
        calc_checksum: bool,
    ) -> Self {
        let total_hasher = if calc_checksum { Some(Hasher::new()) } else { None };
        let shard_hashers = if calc_checksum {
            Some((0..(data_shards + parity_shards)).map(|_| Hasher::new()).collect())
        } else {
            None
        };

        Self {
            writers,
            data_shards,
            parity_shards,
            shard_len_limit,
            buffer: Vec::with_capacity(STRIPE_BLOCK_SIZE * data_shards),
            parity_buffer: Vec::with_capacity(STRIPE_BLOCK_SIZE * parity_shards),
            bytes_written_to_shards: 0,
            calc_checksum,
            total_hasher,
            shard_hashers,
        }
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<(), AppError> {
        if self.calc_checksum {
            if let Some(h) = self.total_hasher.as_mut() {
                h.update(data);
            }
        }

        let mut offset = 0;
        let stripe_capacity = STRIPE_BLOCK_SIZE * self.data_shards;

        while offset < data.len() {
            let remaining_in_buffer = stripe_capacity - self.buffer.len();
            let to_copy = std::cmp::min(remaining_in_buffer, data.len() - offset);
            
            self.buffer.extend_from_slice(&data[offset..offset + to_copy]);
            offset += to_copy;

            if self.buffer.len() == stripe_capacity {
                self.process_stripe(STRIPE_BLOCK_SIZE).await?;
            }
        }
        
        Ok(())
    }

    pub async fn finalize(&mut self) -> Result<(), AppError> {
        if !self.buffer.is_empty() {
            let stripe_capacity = STRIPE_BLOCK_SIZE * self.data_shards;
            let remaining = stripe_capacity - self.buffer.len();
            if remaining > 0 {
                self.buffer.extend(std::iter::repeat(0).take(remaining));
            }
            self.process_stripe(STRIPE_BLOCK_SIZE).await?;
        }

        let current_len = self.bytes_written_to_shards;
        if current_len < self.shard_len_limit {
            let needed = self.shard_len_limit - current_len;
            let zeros = vec![0u8; needed];
            
            if let Some(hashers) = self.shard_hashers.as_mut() {
                for h in hashers.iter_mut() {
                    h.update(&zeros);
                }
            }

            let zeros_arc = Arc::new(zeros);
            let mut writers = std::mem::take(&mut self.writers);
            let mut futures = Vec::with_capacity(writers.len());

            for writer in writers.iter_mut() {
                let z_arc = zeros_arc.clone();
                futures.push(async move {
                    writer.write_all(&z_arc).await
                });
            }
            
            let results: Vec<std::io::Result<()>> = join_all(futures).await;
            self.writers = writers;
            
            for res in results {
                 res.map_err(|e| AppError::Io(e, "pad shard".into()))?;
            }
        }

        let mut flush_futures = Vec::new();
        for w in self.writers.iter_mut() {
            flush_futures.push(w.flush());
        }
        
        let results: Vec<std::io::Result<()>> = join_all(flush_futures).await;
        for res in results {
            res.map_err(|e| AppError::Io(e, "flush shard".into()))?;
        }

        Ok(())
    }

    async fn process_stripe(&mut self, block_size: usize) -> Result<(), AppError> {
        let mut encoder = ReedSolomonEncoder::new(self.data_shards, self.parity_shards, block_size)
            .map_err(|e| AppError::InternalServerError(format!("RS init: {:?}", e)))?;

        for i in 0..self.data_shards {
            let start = i * block_size;
            let end = start + block_size;
            encoder.add_original_shard(&self.buffer[start..end])
                .map_err(|e| AppError::InternalServerError(format!("RS add shard: {:?}", e)))?;
        }

        let result = encoder.encode()
            .map_err(|e| AppError::InternalServerError(format!("RS encode: {:?}", e)))?;

        if let Some(hashers) = self.shard_hashers.as_mut() {
            for i in 0..self.data_shards {
                let start = i * block_size;
                let end = start + block_size;
                hashers[i].update(&self.buffer[start..end]);
            }
            for (i, slice) in result.recovery_iter().enumerate() {
                hashers[self.data_shards + i].update(slice);
            }
        }

        self.parity_buffer.clear();
        for parity_shard in result.recovery_iter() {
            self.parity_buffer.extend_from_slice(parity_shard);
        }

        let data_arc = Arc::new(std::mem::take(&mut self.buffer));
        let parity_arc = Arc::new(std::mem::take(&mut self.parity_buffer));
        
        let mut writers = std::mem::take(&mut self.writers);
        let mut futures = Vec::with_capacity(writers.len());

        for (i, writer) in writers.iter_mut().enumerate() {
            let (source_arc, start, end) = if i < self.data_shards {
                (data_arc.clone(), i * block_size, i * block_size + block_size)
            } else {
                let parity_idx = i - self.data_shards;
                (parity_arc.clone(), parity_idx * block_size, parity_idx * block_size + block_size)
            };

            futures.push(async move {
                writer.write_all(&source_arc[start..end]).await
            });
        }

        let results: Vec<std::io::Result<()>> = join_all(futures).await;
        
        self.writers = writers;
        
        self.buffer = Arc::try_unwrap(data_arc).expect("Write futures not complete");
        self.buffer.clear();
        
        self.parity_buffer = Arc::try_unwrap(parity_arc).expect("Write futures not complete");
        self.parity_buffer.clear();

        for res in results {
            res.map_err(|e| AppError::Io(e, "write stripe".into()))?;
        }

        self.bytes_written_to_shards += block_size;
        Ok(())
    }

    pub fn get_total_checksum(&self) -> Option<String> {
        self.total_hasher.as_ref().map(|h| format!("{:08x}", h.clone().finalize()))
    }

    pub fn get_shard_checksums(&self) -> Vec<String> {
        if let Some(hashers) = &self.shard_hashers {
            hashers.iter().map(|h| format!("{:08x}", h.clone().finalize())).collect()
        } else {
            Vec::new()
        }
    }
}
