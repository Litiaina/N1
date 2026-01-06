use tokio::io::{AsyncWriteExt, BufWriter};

use crate::enums::app_error_enum::AppError;

pub struct AsyncShardWriter {
    pub writers: Vec<BufWriter<tokio::fs::File>>,
    pub shard_len: usize,
    pub shard_index: usize,
    pub offset: usize,
}

impl AsyncShardWriter {
    pub async fn write_data(&mut self, mut data: &[u8]) -> Result<(), AppError> {
        while !data.is_empty() {
            if self.shard_index >= self.writers.len() {
                return Err(AppError::InternalServerError(
                    "Stream larger than expected".into(),
                ));
            }

            let remain = self.shard_len - self.offset;
            let n = remain.min(data.len());

            self.writers[self.shard_index]
                .write_all(&data[..n])
                .await
                .map_err(|e| AppError::Io(e, "writing shard".into()))?;

            self.offset += n;
            data = &data[n..];

            if self.offset == self.shard_len {
                self.shard_index += 1;
                self.offset = 0;
            }
        }
        Ok(())
    }

    pub async fn flush_all(&mut self) -> Result<(), AppError> {
        let zeros = [0u8; 8192];
        while self.shard_index < self.writers.len() {
            let mut remain = self.shard_len - self.offset;
            while remain > 0 {
                let to_write = remain.min(zeros.len());
                self.writers[self.shard_index]
                    .write_all(&zeros[..to_write])
                    .await
                    .map_err(|e| AppError::Io(e, "flushing shard".into()))?;
                remain -= to_write;
            }
            self.shard_index += 1;
            self.offset = 0;
        }

        for w in &mut self.writers {
            w.flush()
                .await
                .map_err(|e| AppError::Io(e, "flush shard".into()))?;
        }
        Ok(())
    }
}
