use std::sync::{Arc, atomic::Ordering};

use crate::{enums::app_error_enum::AppError, object_storage::{constants::SHARD_SUFFIX, upload::WRITER_BUFFER_SIZE_ATOMIC}};
use reed_solomon_simd::ReedSolomonEncoder;
use tokio::{fs::File, io::{AsyncReadExt, AsyncWriteExt, BufWriter}, sync::{Semaphore, mpsc}};

pub struct EcJob {
    pub blob_dir: std::path::PathBuf,
    pub part_index: u64,
    pub shard_len: usize,
    pub data_shards: usize,
    pub parity_shards: usize,
}

pub fn init_ec_workers() -> mpsc::Sender<EcJob> {
    let (tx, mut rx) = mpsc::channel(32_000);
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(num_cpus::get().max(4)));
        while let Some(job) = rx.recv().await {
            let permit = sem.clone().acquire_owned().await.unwrap();
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = run_ec_job_async(job).await {
                    tracing::error!("EC Job failed: {:?}", e);
                }
            });
        }
    });
    tx
}

pub async fn run_ec_job_async(job: EcJob) -> Result<(), AppError> {
    let max_buffer_size = WRITER_BUFFER_SIZE_ATOMIC
        .get()
        .expect("Config not initialized!")
        .load(Ordering::Relaxed);

    let EcJob {
        blob_dir,
        part_index,
        shard_len,
        data_shards,
        parity_shards,
    } = job;

    let mut data_files = Vec::with_capacity(data_shards);
    for i in 0..data_shards {
        let path = blob_dir.join(format!("part_{}_shard_{}{}", part_index, i, SHARD_SUFFIX));
        let file = File::open(path)
            .await
            .map_err(|e| AppError::Io(e, "open data shard".into()))?;
        data_files.push(file);
    }

    let mut parity_files = Vec::with_capacity(parity_shards);
    for i in 0..parity_shards {
        let path = blob_dir.join(format!(
            "part_{}_shard_{}{}",
            part_index,
            data_shards + i,
            SHARD_SUFFIX
        ));
        let f = File::create(path)
            .await
            .map_err(|e| AppError::Io(e, "create parity shard".into()))?;
        parity_files.push(BufWriter::with_capacity(max_buffer_size, f));
    }

    const STRIPE_SIZE: usize = 1024 * 1024;
    let mut processed = 0;
    let mut buffers: Vec<Vec<u8>> = vec![vec![0u8; STRIPE_SIZE]; data_shards + parity_shards];

    while processed < shard_len {
        let batch_size = std::cmp::min(STRIPE_SIZE, shard_len - processed);

        let mut encoder = ReedSolomonEncoder::new(data_shards, parity_shards, batch_size)
            .map_err(|e| AppError::InternalServerError(format!("RS encoder init failed: {:?}", e)))?;

        for (i, file) in data_files.iter_mut().enumerate() {
            let buf = &mut buffers[i][..batch_size];
            let n = file
                .read_exact(buf)
                .await
                .map_err(|e| AppError::Io(e, "read data shard".into()))?;
            if n < batch_size {
                buf[n..].fill(0);
            }
        }

        for i in 0..data_shards {
            encoder
                .add_original_shard(&buffers[i][..batch_size])
                .map_err(|e| {
                    AppError::InternalServerError(format!("RS add shard failed: {:?}", e))
                })?;
        }

        let encoded = encoder
            .encode()
            .map_err(|e| AppError::InternalServerError(format!("RS encode failed: {:?}", e)))?;

        for (i, parity_buf) in encoded.recovery_iter().enumerate() {
            parity_files[i]
                .write_all(parity_buf)
                .await
                .map_err(|e| AppError::Io(e, "write parity shard".into()))?;
        }

        processed += batch_size;
    }

    for w in &mut parity_files {
        w.flush()
            .await
            .map_err(|e| AppError::Io(e, "flush parity shard".into()))?;
    }

    Ok(())
}
