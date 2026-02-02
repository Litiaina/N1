use crate::enums::app_error_enum::AppError;
use crc32fast::Hasher;
use reed_solomon_simd::ReedSolomonEncoder;
use std::alloc::{Layout, alloc, dealloc};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::warn;

pub const ALIGNMENT: usize = 4096;

pub struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
    len: usize,
}

impl AlignedBuffer {
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, ALIGNMENT).expect("Invalid alignment");
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            panic!("Aligned allocation failed");
        }
        Self {
            ptr,
            layout,
            len: 0,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_full_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.layout.size()) }
    }

    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let remaining = self.layout.size() - self.len;
        let to_copy = std::cmp::min(remaining, data.len());
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(self.len), to_copy);
        }
        self.len += to_copy;
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.layout.size()
    }

    pub fn fill_zeros(&mut self) {
        let remaining = self.layout.size() - self.len;
        if remaining > 0 {
            unsafe {
                std::ptr::write_bytes(self.ptr.add(self.len), 0, remaining);
            }
            self.len = self.layout.size();
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

pub struct ErasureStreamWriter<W: AsyncWrite + Unpin + Send> {
    writers: Vec<Option<W>>,
    data_shards: usize,
    parity_shards: usize,
    shard_len_limit: usize,
    buffer: AlignedBuffer,
    parity_buffer: AlignedBuffer,
    bytes_written_to_shards: usize,
    calc_checksum: bool,
    total_hasher: Option<Hasher>,
    shard_hashers: Option<Vec<Hasher>>,
    block_size: usize,
}

impl<W: AsyncWrite + Unpin + Send> ErasureStreamWriter<W> {
    pub fn new(
        writers: Vec<Option<W>>,
        data_shards: usize,
        parity_shards: usize,
        shard_len_limit: usize,
        calc_checksum: bool,
        block_size: usize,
    ) -> Self {
        let total_hasher = if calc_checksum {
            Some(Hasher::new())
        } else {
            None
        };
        let shard_hashers = if calc_checksum {
            Some(
                (0..(data_shards + parity_shards))
                    .map(|_| Hasher::new())
                    .collect(),
            )
        } else {
            None
        };

        Self {
            writers,
            data_shards,
            parity_shards,
            shard_len_limit,
            buffer: AlignedBuffer::new(block_size * data_shards),
            parity_buffer: AlignedBuffer::new(block_size * parity_shards),
            bytes_written_to_shards: 0,
            calc_checksum,
            total_hasher,
            shard_hashers,
            block_size,
        }
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<(), AppError> {
        if self.calc_checksum {
            if let Some(h) = self.total_hasher.as_mut() {
                h.update(data);
            }
        }

        let mut offset = 0;
        let stripe_capacity = self.buffer.capacity();

        while offset < data.len() {
            let remaining_in_buffer = stripe_capacity - self.buffer.len();
            let to_copy = std::cmp::min(remaining_in_buffer, data.len() - offset);

            self.buffer
                .extend_from_slice(&data[offset..offset + to_copy]);
            offset += to_copy;

            if self.buffer.len() == stripe_capacity {
                self.process_stripe().await?;
            }
        }

        Ok(())
    }

    pub async fn finalize(&mut self) -> Result<(), AppError> {
        if self.buffer.len() > 0 {
            self.buffer.fill_zeros();
            self.process_stripe().await?;
        }

        let current_len = self.bytes_written_to_shards;
        if current_len < self.shard_len_limit {
            let needed = self.shard_len_limit - current_len;
            let mut padding_buf = AlignedBuffer::new(needed);
            padding_buf.fill_zeros();

            if let Some(hashers) = self.shard_hashers.as_mut() {
                for h in hashers.iter_mut() {
                    h.update(padding_buf.as_slice());
                }
            }

            for writer_opt in self.writers.iter_mut() {
                if let Some(writer) = writer_opt {
                    if let Err(e) = writer.write_all(padding_buf.as_slice()).await {
                        warn!("Failed to write padding to shard: {}", e);
                        *writer_opt = None;
                    }
                }
            }
        }

        for w_opt in self.writers.iter_mut() {
            if let Some(w) = w_opt {
                let _ = w.flush().await;
            }
        }

        Ok(())
    }

    async fn process_stripe(&mut self) -> Result<(), AppError> {
        let block_size = self.block_size;
        let buffer_slice = self.buffer.as_full_slice();

        if self.parity_shards > 0 {
            let mut encoder =
                ReedSolomonEncoder::new(self.data_shards, self.parity_shards, block_size)
                    .map_err(|e| AppError::InternalServerError(format!("RS init: {:?}", e)))?;

            for i in 0..self.data_shards {
                let start = i * block_size;
                let end = start + block_size;
                encoder
                    .add_original_shard(&buffer_slice[start..end])
                    .map_err(|e| AppError::InternalServerError(format!("RS add: {:?}", e)))?;
            }

            let result = encoder
                .encode()
                .map_err(|e| AppError::InternalServerError(format!("RS encode: {:?}", e)))?;

            if let Some(hashers) = self.shard_hashers.as_mut() {
                for i in 0..self.data_shards {
                    let start = i * block_size;
                    let end = start + block_size;
                    hashers[i].update(&buffer_slice[start..end]);
                }
                for (i, slice) in result.recovery_iter().enumerate() {
                    hashers[self.data_shards + i].update(slice);
                    self.parity_buffer.extend_from_slice(slice);
                }
            } else {
                for slice in result.recovery_iter() {
                    self.parity_buffer.extend_from_slice(slice);
                }
            }
        } else if let Some(hashers) = self.shard_hashers.as_mut() {
            for i in 0..self.data_shards {
                let start = i * block_size;
                let end = start + block_size;
                hashers[i].update(&buffer_slice[start..end]);
            }
        }

        let mut successful_writes = 0;

        for (i, writer_opt) in self.writers.iter_mut().enumerate() {
            if let Some(writer) = writer_opt {
                let slice = if i < self.data_shards {
                    let start = i * block_size;
                    let end = start + block_size;
                    &self.buffer.as_full_slice()[start..end]
                } else {
                    let parity_idx = i - self.data_shards;
                    let start = parity_idx * block_size;
                    let end = start + block_size;
                    &self.parity_buffer.as_full_slice()[start..end]
                };

                if let Err(e) = writer.write_all(slice).await {
                    warn!("Failed to write stripe to shard: {}", e);
                    *writer_opt = None;
                } else {
                    successful_writes += 1;
                }
            }
        }

        if successful_writes < self.data_shards {
            return Err(AppError::InternalServerError(
                "Not enough healthy disks to persist data".into(),
            ));
        }

        self.buffer.clear();
        self.parity_buffer.clear();
        self.bytes_written_to_shards += block_size;
        Ok(())
    }

    pub fn get_total_checksum(&self) -> Option<String> {
        self.total_hasher
            .as_ref()
            .map(|h| format!("{:08x}", h.clone().finalize()))
    }

    pub fn get_shard_checksums(&self) -> Vec<String> {
        if let Some(hashers) = &self.shard_hashers {
            hashers
                .iter()
                .map(|h| format!("{:08x}", h.clone().finalize()))
                .collect()
        } else {
            Vec::new()
        }
    }
}
