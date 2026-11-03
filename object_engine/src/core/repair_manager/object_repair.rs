use crate::core::{
    Error, Result, corrupt,
    download::{VerifiedStripe, check_health, decode_stripe},
    ec::StripeCodec,
    storage_pool::Pool,
    store::{Store, blocking, collect, joined},
};
use futures_util::StreamExt;
use n1_link::{Fingerprint, shard};
use serde::Serialize;
use sha2::Digest;
use std::{
    path::Path,
    sync::{Arc, atomic::Ordering},
};

#[derive(Debug, Serialize)]
pub struct RepairResult {
    pub repaired_shards: Vec<u16>,
    pub total_bytes: u64,
}

impl Store {
    pub async fn repair(self: &Arc<Self>, scope: [u8; 16], key: String) -> Result<RepairResult> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            let identity = store.signer.verify_object(&scope, &key)?;
            let pool = store.pool(identity)?;
            store.repair_owned(pool, identity.fingerprint(), None).await
        })
        .await
    }

    pub(crate) async fn queue_read_repair(self: &Arc<Self>, pool: Arc<Pool>, fp: Fingerprint) {
        if !self.read_repair.load(Ordering::Acquire) {
            return;
        }
        let mut job = match self.repair_manager.read(fp) {
            Ok(Some(job)) => job,
            Ok(None) => return, // An existing owner coalesced this repair request.
            Err(error) => {
                tracing::debug!(%error,"read repair not admitted");
                return;
            }
        };
        let control = Arc::new(crate::core::maintenance::WorkControl::new(
            self.repair_manager.stop.child_token(),
            128 * 1024 * 1024,
            3600,
        ));
        // Keep the bounded backpressure of the original recovery reader. The
        // foreground data/memory guards have already been released by GET.
        let permit =
            match super::super::recovery::admit(&self.repair_manager.stop, Some(&control)).await {
                Ok(permit) => permit,
                Err(error) => {
                    tracing::debug!(%error,"read repair admission stopped");
                    return;
                }
            };
        let store = self.clone();
        super::super::recovery::spawn(async move {
            loop {
                if let Err(error) = store
                    .clone()
                    .repair_admitted(pool.clone(), fp, Some(control.clone()), &permit)
                    .await
                {
                    tracing::debug!("read repair incomplete: {error}");
                }
                if control.stopped() {
                    break;
                }
                match job.finish_or_again() {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(error) => {
                        tracing::debug!(%error,"read repair completion failed");
                        break;
                    }
                }
            }
        });
    }

    pub(crate) async fn repair_owned(
        self: Arc<Self>,
        pool: Arc<Pool>,
        fp: Fingerprint,
        control: Option<Arc<crate::core::maintenance::WorkControl>>,
    ) -> Result<RepairResult> {
        let _job = self.repair_manager.register()?;
        let permit =
            super::super::recovery::admit(&self.repair_manager.stop, control.as_deref()).await?;
        self.repair_admitted(pool, fp, control, &permit).await
    }

    async fn repair_admitted(
        self: Arc<Self>,
        pool: Arc<Pool>,
        fp: Fingerprint,
        control: Option<Arc<crate::core::maintenance::WorkControl>>,
        _permit: &tokio::sync::OwnedSemaphorePermit,
    ) -> Result<RepairResult> {
        use crate::core::io_scheduler::{DiskIoClass, acquire_disk_io};
        let class = if control.is_some() {
            DiskIoClass::BackgroundRepair
        } else {
            DiskIoClass::ForegroundWrite
        };
        let io = if let Some(control) = &control {
            tokio::select! {
                _ = control.until_stopped() => return Err(Error::Conflict("repair stopped".into())),
                _ = self.repair_manager.stop.cancelled() => return Err(Error::Conflict("repair stopped".into())),
                io = acquire_disk_io(class, None, "LINK shard repair") => io?,
            }
        } else {
            tokio::select! {
                _ = self.repair_manager.stop.cancelled() => return Err(Error::Conflict("repair stopped".into())),
                io = acquire_disk_io(class,None,"LINK shard repair") => io?,
            }
        };
        let _admission = if control.is_some() {
            self.admit_background(&pool).await?
        } else {
            self.admit(&pool).await?
        };
        let lease = if control.is_some() {
            self.lease(fp)?
        } else {
            self.foreground(fp)?
        };
        let background = control.as_ref().map(|_| lease.state().background());
        let control = control.map(|c| {
            Arc::new(
                c.for_object(
                    background
                        .as_ref()
                        .expect("background registration")
                        .cancel
                        .clone(),
                ),
            )
        });
        let manual_control =
            crate::core::maintenance::WorkControl::unpaced(self.repair_manager.stop.child_token());
        let work = control.as_deref().unwrap_or(&manual_control);
        let _guard = lease.state().lifecycle.clone().write_owned().await;
        work.charge(0).await?;
        let object = self.open_fingerprint(pool.clone(), fp).await?;
        let health = check_health(
            &object,
            &pool.layout,
            fp,
            true,
            Some(work),
            Some(&io.progress_handle()),
        )
        .await?;
        let mut targets = health.missing_shards;
        targets.extend(health.corrupt_shards);
        targets.sort_unstable();
        targets.dedup();
        if targets.is_empty() {
            io.complete();
            return Ok(RepairResult {
                repaired_shards: Vec::new(),
                total_bytes: object.header.total_bytes,
            });
        }
        let check = pool.clone();
        blocking(move || check.writable()).await?;
        let mut files = Vec::new();
        for &shard in &targets {
            let member = pool.holder(fp, shard)?;
            let m = member.clone();
            let file = blocking(move || m.create_repair(fp, shard)).await?;
            let mut header = object.header.clone();
            header.shard = shard;
            crate::core::linux_io::write_at_async(file.clone(), header.encode(fp).to_vec(), 0)
                .await?;
            files.push((member, file, header));
        }
        let result = async {
            let mut codec = StripeCodec::new(&pool.layout)?;
            let mut root = shard::object_hasher(
                object.header.format,
                object.header.total_bytes,
                object.header.part_bytes,
            );
            for part in 0..object.header.parts() {
                let mut part_root = shard::part_hasher(object.header.format, part);
                for stripe in object.header.part_stripes(part)? {
                    work.charge(
                        object.header.record_bytes() * (pool.layout.width() + targets.len()) as u64,
                    )
                    .await?;
                    let VerifiedStripe {
                        source,
                        source_hash: hash,
                        ..
                    } = decode_stripe(&object, fp, stripe, &mut codec, false).await?;
                    io.record_progress(source.len() as u64);
                    part_root.update(hash);
                    let pieces = codec.encode(&source)?;
                    let writes = files
                        .iter()
                        .map(|(_, file, header)| {
                            let record = shard::encode_frame(
                                header,
                                fp,
                                stripe,
                                hash,
                                &pieces[usize::from(header.shard)],
                            );
                            let file = file.clone();
                            let header = header.clone();
                            async move {
                                crate::core::linux_io::write_at_async(
                                    file.clone(),
                                    record?,
                                    header.frame_offset(stripe)?,
                                )
                                .await
                                .map_err(Error::from)
                            }
                        })
                        .collect::<Vec<_>>();
                    collect(
                        futures_util::stream::iter(writes)
                            .buffer_unordered(self.runtime().shard_repair_concurrency)
                            .collect::<Vec<_>>()
                            .await,
                    )?;
                }
                root.update(part_root.finalize());
            }
            if <[u8; 32]>::from(root.finalize()) != object.root {
                return Err(corrupt(
                    "repair reconstruction failed object root verification",
                ));
            }
            collect(
                futures_util::future::join_all(files.iter().map(|(_, file, header)| async move {
                    crate::core::linux_io::write_at_async(
                        file.clone(),
                        shard::encode_seal(header, fp, object.root).to_vec(),
                        header.seal_offset(),
                    )
                    .await?;
                    crate::core::linux_io::fdatasync_async(file.clone())
                        .await
                        .map_err(Error::from)
                }))
                .await,
            )?;
            let _snapshot = self.snapshot_gate.clone().read_owned().await;
            work.charge(0).await?;
            collect(
                futures_util::future::join_all(files.iter().map(|(member, _, header)| {
                    let m = member.clone();
                    let shard = header.shard;
                    blocking(move || m.publish_repair(fp, shard))
                }))
                .await,
            )?;
            Ok(RepairResult {
                repaired_shards: targets.clone(),
                total_bytes: object.header.total_bytes,
            })
        }
        .await;
        if result.is_err() {
            // Cleanup only private repair temporaries. Never roll back a final
            // replacement which may already have reached a durability boundary.
            for (member, _, header) in &files {
                let m = member.clone();
                let path = format!(".uploads/{fp}/repair.s{}.tmp", header.shard);
                let _ = blocking(move || m.unlink(Path::new(&path), true)).await;
            }
        }
        if result.is_ok() {
            io.complete();
        }
        result
    }
}
