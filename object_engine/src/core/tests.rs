//! Functional tests use isolated directories on RAM. These do not qualify
//! physical member independence, power loss or sustained hardware performance.
mod integrity;
use super::*;

#[tokio::test]
async fn configured_gc_age_retains_recent_expired_staging_without_touching_ticket_validity() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    store
        .configure_runtime(crate::settings::runtime::RuntimeConfig {
            staging_min_age_seconds: 3600,
            ..Default::default()
        })
        .unwrap();
    let pending = store
        .init(SCOPE, 0, 1, 12288, n1_link::Durability::Durable, 1)
        .unwrap();
    store
        .write_part(
            SCOPE,
            pending.upload_key.clone(),
            0,
            std::io::Cursor::new(vec![7]),
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    let report = store
        .garbage_collect(tokio_util::sync::CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.removed_uploads, 0);
    assert!(report.retained > 0);
    assert!(
        store
            .upload_status(SCOPE, pending.upload_key, 0, 1)
            .await
            .is_ok()
    );
}
use n1_link::{Durability, PoolId, PoolLayout};
use rand::{RngCore, rngs::OsRng};
use std::{path::PathBuf, sync::Arc};

const SCOPE: [u8; 16] = [11; 16];

#[test]
fn restored_runtime_controls_govern_link_requests() {
    const CHILD: &str = "N1_RESTORED_CONTROL_TEST";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "core::tests::restored_runtime_controls_govern_link_requests",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        use super::io_scheduler::{disk_io_scheduler_snapshot, set_foreground_io_target};
        use tokio::io::AsyncWriteExt;
        crate::settings::config::ConcurrencyConfig {
            max_streams: 2,
            adaptive_enabled: false,
            ..Default::default()
        }
        .configure(2, 4)
        .unwrap();
        let fixture = Fixture::new();
        let store = fixture.enroll();
        let first = store
            .init(SCOPE, 0, 12288, 12288, Durability::Durable, 60)
            .unwrap();
        let second = store
            .init(SCOPE, 0, 12288, 12288, Durability::Durable, 60)
            .unwrap();
        set_foreground_io_target(1);
        let (reader, mut writer) = tokio::io::duplex(1024);
        let writing = {
            let store = store.clone();
            tokio::spawn(async move { store.put(SCOPE, first.upload_key, reader).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while disk_io_scheduler_snapshot().active_foreground != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let uploading = {
            let store = store.clone();
            let ticket = second.upload_key.clone();
            tokio::spawn(async move {
                store
                    .put(SCOPE, ticket, std::io::Cursor::new(vec![7; 12288]))
                    .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while disk_io_scheduler_snapshot().queued_foreground != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!uploading.is_finished());
        set_foreground_io_target(2);
        tokio::time::timeout(std::time::Duration::from_secs(3), uploading)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            !writing.is_finished(),
            "the queued upload must finish while the first source is still paused"
        );
        writer.write_all(&vec![3; 12288]).await.unwrap();
        drop(writer);
        writing.await.unwrap().unwrap();
        super::object_parser::set_stream_target(1);
        let held = super::object_parser::acquire_adaptive_stream_guard()
            .await
            .unwrap();
        let downloading = {
            let store = store.clone();
            let key = second.object_key.clone();
            tokio::spawn(async move { store.download(SCOPE, key, 0, None, Vec::new()).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(!downloading.is_finished());
        drop(held);
        let (bytes, _) = tokio::time::timeout(std::time::Duration::from_secs(3), downloading)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(bytes, vec![7; 12288]);
        let snapshot = disk_io_scheduler_snapshot();
        let writes = snapshot
            .classes
            .iter()
            .find(|c| c.class == "foreground_write")
            .unwrap();
        let reads = snapshot
            .classes
            .iter()
            .find(|c| c.class == "foreground_read")
            .unwrap();
        assert_eq!(writes.completed, 2);
        assert_eq!(writes.bytes, 24576);
        assert_eq!(reads.completed, 1);
        assert_eq!(reads.bytes, 12288);
        assert_eq!(snapshot.active_foreground, 0);
        assert_eq!(snapshot.queued_foreground, 0);
        let stop = tokio_util::sync::CancellationToken::new();
        let worker = tokio::spawn(super::adaptive_concurrency::run_adaptive_concurrency_until(
            stop.clone(),
        ));
        stop.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap();
    });
}
pub(crate) struct Fixture {
    pub base: PathBuf,
    pub paths: Vec<PathBuf>,
    pub layout: PoolLayout,
}
impl Fixture {
    pub(crate) fn new() -> Self {
        let base = PathBuf::from(
            std::env::var_os("N1_LINK_TEST_ROOT").unwrap_or_else(|| "/tmp/n1-link-tests".into()),
        )
        .join(format!(
            "engine-{}-{:016x}",
            std::process::id(),
            OsRng.next_u64()
        ));
        let paths = (0..4)
            .map(|i| {
                let p = base.join(i.to_string());
                std::fs::create_dir_all(&p).unwrap();
                p
            })
            .collect();
        let layout = PoolLayout {
            id: PoolId([6; 16]),
            generation: 1,
            data_shards: 3,
            parity_shards: 1,
            block_bytes: 4096,
            placement_version: 1,
            shard_format: n1_link::ShardFormat::Crc32cSha256V2,
        };
        Self {
            base,
            paths,
            layout,
        }
    }
    pub(crate) fn enroll(&self) -> Arc<Store> {
        let s = Store::new(
            vec![storage_pool::Pool::fixture(self.layout.clone(), &self.paths).unwrap()],
            [17; 32],
            16,
        )
        .unwrap();
        s.configure_read_repair(false);
        s
    }
    fn reopen(&self) -> Arc<Store> {
        let s = Store::new(
            vec![storage_pool::Pool::reopen_fixture(self.layout.clone(), &self.paths).unwrap()],
            [17; 32],
            16,
        )
        .unwrap();
        s.configure_read_repair(false);
        s
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            std::fs::remove_dir_all(&self.base).unwrap();
        }
    }
}
fn source(size: usize) -> Vec<u8> {
    (0..size).map(|i| ((i * 197 + 13) % 251) as u8).collect()
}
async fn upload(store: &Arc<Store>, data: &[u8], durability: Durability) -> InitializedUpload {
    let init = store
        .init(SCOPE, 0, data.len() as u64, 12288, durability, 3600)
        .unwrap();
    for (i, part) in data.chunks(12288).enumerate().rev() {
        store
            .write_part(
                SCOPE,
                init.upload_key.clone(),
                i as u64,
                std::io::Cursor::new(part.to_vec()),
            )
            .await
            .unwrap();
    }
    init
}

#[tokio::test]
async fn multipart_out_of_order_status_and_repeated_finalize_use_same_identity() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(24577);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            12288,
            Durability::Durable,
            3600,
        )
        .unwrap();
    store
        .write_part(
            SCOPE,
            init.upload_key.clone(),
            2,
            std::io::Cursor::new(data[24576..].to_vec()),
        )
        .await
        .unwrap();
    let status = store
        .upload_status(SCOPE, init.upload_key.clone(), 0, 100)
        .await
        .unwrap();
    assert_eq!(status.missing_parts, vec![0, 1]);
    assert!(!status.finalized);
    assert!(
        store
            .finalize(SCOPE, init.upload_key.clone())
            .await
            .is_err()
    );
    for i in 0..2 {
        store
            .write_part(
                SCOPE,
                init.upload_key.clone(),
                i,
                std::io::Cursor::new(data[i as usize * 12288..(i as usize + 1) * 12288].to_vec()),
            )
            .await
            .unwrap();
    }
    let final1 = store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    assert!(!final1.already_finalized);
    assert_eq!(final1.object_key, init.object_key);
    let final2 = store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    assert!(final2.already_finalized);
    assert_eq!(final1.object_key, final2.object_key);
    let status = store
        .upload_status(SCOPE, init.upload_key, 0, 100)
        .await
        .unwrap();
    assert!(status.finalized);
    assert!(status.missing_parts.is_empty());
}

#[tokio::test]
async fn durable_restart_discovers_progress_without_receipts_or_manifests() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(49153);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            12288,
            Durability::Durable,
            3600,
        )
        .unwrap();
    for i in [0, 2, 4] {
        let start = i * 12288;
        let end = (start + 12288).min(data.len());
        store
            .write_part(
                SCOPE,
                init.upload_key.clone(),
                i as u64,
                std::io::Cursor::new(data[start..end].to_vec()),
            )
            .await
            .unwrap();
    }
    drop(store);
    let store = fixture.reopen();
    let status = store
        .upload_status(SCOPE, init.upload_key.clone(), 0, 100)
        .await
        .unwrap();
    assert_eq!(status.missing_parts, vec![1, 3]);
    for i in [1, 3] {
        let start = i * 12288;
        store
            .write_part(
                SCOPE,
                init.upload_key.clone(),
                i as u64,
                std::io::Cursor::new(data[start..start + 12288].to_vec()),
            )
            .await
            .unwrap();
    }
    store.finalize(SCOPE, init.upload_key).await.unwrap();
    for path in &fixture.paths {
        assert_eq!(std::fs::read_dir(path.join(".uploads")).unwrap().count(), 0);
    }
}

#[tokio::test]
async fn exact_duplicates_are_idempotent_and_conflicts_preserve_existing_bytes() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    for length in [12288, 9 * 12288 + 37] {
        let data = source(length);
        let init = store
            .init(
                SCOPE,
                0,
                data.len() as u64,
                (data.len() as u64).div_ceil(12288) * 12288,
                Durability::Durable,
                3600,
            )
            .unwrap();
        for _ in 0..2 {
            store
                .write_part(
                    SCOPE,
                    init.upload_key.clone(),
                    0,
                    std::io::Cursor::new(data.clone()),
                )
                .await
                .unwrap();
        }
        let mut changed = data.clone();
        // The larger case conflicts after two complete coalesced write groups.
        changed[length - 5] ^= 1;
        assert!(matches!(
            store
                .write_part(
                    SCOPE,
                    init.upload_key.clone(),
                    0,
                    std::io::Cursor::new(changed.clone())
                )
                .await,
            Err(Error::Conflict(_))
        ));
        store
            .finalize(SCOPE, init.upload_key.clone())
            .await
            .unwrap();
        assert!(matches!(
            store
                .write_part(
                    SCOPE,
                    init.upload_key.clone(),
                    0,
                    std::io::Cursor::new(changed)
                )
                .await,
            Err(Error::Conflict(_))
        ));
        store
            .write_part(
                SCOPE,
                init.upload_key,
                0,
                std::io::Cursor::new(data.clone()),
            )
            .await
            .unwrap();
        let (actual, _) = store
            .download(SCOPE, init.object_key, 0, None, Vec::new())
            .await
            .unwrap();
        assert_eq!(actual, data);
    }
}

#[tokio::test]
async fn short_body_never_becomes_a_complete_part_and_retry_recovers() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(24576);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            24576,
            Durability::Durable,
            3600,
        )
        .unwrap();
    assert!(
        store
            .write_part(
                SCOPE,
                init.upload_key.clone(),
                0,
                std::io::Cursor::new(data[..13000].to_vec())
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .upload_status(SCOPE, init.upload_key.clone(), 0, 100)
            .await
            .unwrap()
            .missing_parts,
        vec![0]
    );
    assert!(
        store
            .finalize(SCOPE, init.upload_key.clone())
            .await
            .is_err()
    );
    store
        .write_part(
            SCOPE,
            init.upload_key.clone(),
            0,
            std::io::Cursor::new(data),
        )
        .await
        .unwrap();
    store.finalize(SCOPE, init.upload_key).await.unwrap();
}

#[tokio::test]
async fn zero_length_objects_finalize_without_payload_or_sidecars() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let init = upload(&store, &[], Durability::Durable).await;
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    assert!(
        store
            .finalize(SCOPE, init.upload_key)
            .await
            .unwrap()
            .already_finalized
    );
}

#[tokio::test]
async fn finalize_after_partial_member_promotion_survives_restart() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(24577);
    let init = upload(&store, &data, Durability::Durable).await;
    let plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    let pool = store.pool(plan.identity).unwrap();
    let fp = plan.identity.fingerprint();
    // Finalize once, then move two files back to staging to reproduce the exact
    // state of a crash between member renames, retaining the same sealed bytes.
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    for shard in 0..2 {
        let member = pool.holder(fp, shard).unwrap();
        let stage = member.path.join(fp.staging_path());
        std::fs::create_dir_all(stage.parent().unwrap()).unwrap();
        std::fs::rename(member.path.join(fp.shard_path(shard)), stage).unwrap();
    }
    drop(pool);
    drop(store);
    let store = fixture.reopen();
    assert!(
        !store
            .upload_status(SCOPE, init.upload_key.clone(), 0, 100)
            .await
            .unwrap()
            .finalized
    );
    assert!(
        !store
            .finalize(SCOPE, init.upload_key.clone())
            .await
            .unwrap()
            .already_finalized
    );
    assert!(
        store
            .finalize(SCOPE, init.upload_key)
            .await
            .unwrap()
            .already_finalized
    );
}

#[tokio::test]
async fn get_and_ranges_return_exact_original_bytes_without_an_index() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(200001);
    let init = upload(&store, &data, Durability::Durable).await;
    store.finalize(SCOPE, init.upload_key).await.unwrap();
    for (start, len) in [
        (0, data.len()),
        (0, 1),
        (4095, 16385),
        (12287, 123),
        (data.len(), 0),
    ] {
        let (bytes, info) = store
            .download(
                SCOPE,
                init.object_key.clone(),
                start as u64,
                Some(len as u64),
                Vec::new(),
            )
            .await
            .unwrap();
        assert_eq!(bytes, data[start..start + len]);
        assert_eq!(info.total_bytes, data.len() as u64);
    }
    assert!(
        store
            .download(
                SCOPE,
                init.object_key.clone(),
                data.len() as u64 + 1,
                None,
                Vec::new()
            )
            .await
            .is_err()
    );
    let health = store
        .object_health(SCOPE, init.object_key, true)
        .await
        .unwrap();
    assert_eq!(health.status, "healthy");
    assert_eq!(health.verified_bytes, data.len() as u64);
}

#[tokio::test]
async fn missing_and_corrupt_data_are_reconstructed_and_reported() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(65537);
    let init = upload(&store, &data, Durability::Durable).await;
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    let fp = plan.identity.fingerprint();
    let pool = store.pool(plan.identity).unwrap();
    let holder = pool.holder(fp, 0).unwrap();
    let path = holder.path.join(fp.shard_path(0));
    let original = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    let (bytes, _) = store
        .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
        .await
        .unwrap();
    assert_eq!(bytes, data);
    let health = store
        .object_health(SCOPE, init.object_key.clone(), true)
        .await
        .unwrap();
    assert_eq!(health.status, "degraded");
    assert_eq!(health.missing_shards, vec![0]);
    std::fs::write(&path, &original).unwrap();
    let mut damaged = original;
    damaged[128 + 96 + 19] ^= 1;
    std::fs::write(&path, damaged).unwrap();
    let (bytes, _) = store
        .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
        .await
        .unwrap();
    assert_eq!(bytes, data);
    let health = store
        .object_health(SCOPE, init.object_key.clone(), true)
        .await
        .unwrap();
    assert_eq!(health.corrupt_shards, vec![0]);
    assert_eq!(health.status, "degraded");
    let second = pool.holder(fp, 1).unwrap();
    std::fs::remove_file(second.path.join(fp.shard_path(1))).unwrap();
    assert!(
        store
            .download(SCOPE, init.object_key, 0, None, Vec::new())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn prefetched_ranges_recover_independent_bad_frames_without_skipping_neighbors() {
    assert_prefetched_ranges_recover_independent_bad_frames(4096).await;
}

#[tokio::test]
async fn large_prefetched_ranges_recover_independent_bad_frames_without_skipping_neighbors() {
    assert_prefetched_ranges_recover_independent_bad_frames(256 * 1024).await;
}

async fn assert_prefetched_ranges_recover_independent_bad_frames(block_bytes: u32) {
    use std::os::unix::fs::FileExt;
    let mut fixture = Fixture::new();
    fixture.layout.block_bytes = block_bytes;
    let store = fixture.enroll();
    let stripe = fixture.layout.stripe_bytes() as usize;
    let data = source(13 * stripe + 4097);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            (14 * stripe) as u64,
            Durability::Durable,
            3600,
        )
        .unwrap();
    store
        .put(
            SCOPE,
            init.upload_key.clone(),
            std::io::Cursor::new(data.clone()),
        )
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    let fp = plan.identity.fingerprint();
    let pool = store.pool(plan.identity).unwrap();
    let header = n1_link::shard::ShardHeader::new(&fixture.layout, plan, 0).unwrap();
    for (shard, index) in [(0, 1), (1, 2), (2, 7)] {
        let path = pool
            .holder(fp, shard)
            .unwrap()
            .path
            .join(fp.shard_path(shard));
        let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.write_all_at(
            &[255],
            header.frame_offset(index).unwrap() + n1_link::shard::FRAME_HEADER_BYTES as u64 + 41,
        )
        .unwrap();
    }
    for (start, length) in [
        (0, data.len()),
        (stripe - 17, 9 * stripe + 33),
        (4 * stripe + 13, 5 * stripe - 5),
    ] {
        let bytes = store
            .download(
                SCOPE,
                init.object_key.clone(),
                start as u64,
                Some(length as u64),
                Vec::new(),
            )
            .await
            .unwrap()
            .0;
        assert_eq!(bytes, data[start..start + length]);
    }
    let health = store
        .object_health(SCOPE, init.object_key.clone(), true)
        .await
        .unwrap();
    assert_eq!(health.corrupt_shards, vec![0, 1, 2]);
    let parity = pool.holder(fp, 3).unwrap().path.join(fp.shard_path(3));
    std::fs::remove_file(parity).unwrap();
    assert!(
        store
            .download(SCOPE, init.object_key, 0, None, Vec::new())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sparse_durable_tail_survives_restart_conflicting_retry_and_padding_corruption() {
    use std::os::unix::fs::{FileExt, MetadataExt};
    let mut fixture = Fixture::new();
    fixture.layout.block_bytes = 262144;
    let store = fixture.enroll();
    let data = source(4097);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            fixture.layout.stripe_bytes(),
            Durability::Durable,
            3600,
        )
        .unwrap();
    store
        .write_part(
            SCOPE,
            init.upload_key.clone(),
            0,
            std::io::Cursor::new(data.clone()),
        )
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    let fp = plan.identity.fingerprint();
    let header = n1_link::shard::ShardHeader::new(&fixture.layout, plan, 0).unwrap();
    for member in store.pools[0].members.iter().flatten() {
        let meta = std::fs::metadata(member.path.join(fp.staging_path())).unwrap();
        assert_eq!(meta.len(), header.seal_offset());
        assert!(
            meta.blocks() * 512 < meta.len() / 4,
            "zero padding allocated physical data: {meta:?}"
        );
    }
    drop(store);
    let store = fixture.reopen();
    assert!(
        store
            .upload_status(SCOPE, init.upload_key.clone(), 0, 10)
            .await
            .unwrap()
            .missing_parts
            .is_empty()
    );
    let mut different = data.clone();
    different[31] ^= 1;
    assert!(
        store
            .write_part(
                SCOPE,
                init.upload_key.clone(),
                0,
                std::io::Cursor::new(different)
            )
            .await
            .is_err()
    );
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    let pool = store.pool(plan.identity).unwrap();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(pool.holder(fp, 0).unwrap().path.join(fp.shard_path(0)))
        .unwrap();
    file.write_all_at(&[19], 128 + 96 + 128000).unwrap();
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    assert_eq!(
        store
            .object_health(SCOPE, init.object_key.clone(), true)
            .await
            .unwrap()
            .corrupt_shards,
        vec![0]
    );
    store.repair(SCOPE, init.object_key.clone()).await.unwrap();
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    store.delete(SCOPE, init.object_key).await.unwrap();
    assert!(matches!(
        store.finalize(SCOPE, init.upload_key).await,
        Err(Error::Retired)
    ));
}

#[tokio::test]
async fn delete_and_cancel_are_idempotent_and_cannot_cancel_final_objects() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(24577);
    let abandoned = upload(&store, &data, Durability::Durable).await;
    assert_eq!(
        store
            .cancel_upload(SCOPE, abandoned.upload_key.clone())
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        store
            .cancel_upload(SCOPE, abandoned.upload_key)
            .await
            .unwrap(),
        0
    );
    let init = upload(&store, &data, Durability::Durable).await;
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    assert!(matches!(
        store.cancel_upload(SCOPE, init.upload_key).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        store.delete(SCOPE, init.object_key.clone()).await.unwrap(),
        4
    );
    assert_eq!(
        store.delete(SCOPE, init.object_key.clone()).await.unwrap(),
        0
    );
    assert!(matches!(
        store.stat(SCOPE, init.object_key).await,
        Err(Error::Retired)
    ));
}

#[tokio::test]
async fn cancelled_download_owner_drains_before_delete_can_proceed() {
    use tokio::io::AsyncReadExt;
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(512 * 1024);
    let init = upload(&store, &data, Durability::Durable).await;
    store.finalize(SCOPE, init.upload_key).await.unwrap();
    let (mut receiver, sender) = tokio::io::duplex(4096);
    let s = store.clone();
    let key = init.object_key.clone();
    let download = tokio::spawn(async move { s.download(SCOPE, key, 0, None, sender).await });
    receiver.read_u8().await.unwrap();
    download.abort();
    let s = store.clone();
    let key = init.object_key.clone();
    let mut deletion = tokio::spawn(async move { s.delete(SCOPE, key).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut deletion)
            .await
            .is_err()
    );
    drop(receiver);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), deletion)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        4
    );
    assert!(matches!(
        store.stat(SCOPE, init.object_key).await,
        Err(Error::Retired)
    ));
}

#[tokio::test]
async fn manual_repair_preserves_healthy_inodes_and_full_original_hash() {
    use std::os::unix::fs::MetadataExt;
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(200001);
    let init = upload(&store, &data, Durability::Durable).await;
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    let fp = plan.identity.fingerprint();
    let pool = store.pool(plan.identity).unwrap();
    let paths = (0..4)
        .map(|i| pool.holder(fp, i).unwrap().path.join(fp.shard_path(i)))
        .collect::<Vec<_>>();
    let original = paths
        .iter()
        .map(|p| std::fs::read(p).unwrap())
        .collect::<Vec<_>>();
    let inodes = paths
        .iter()
        .map(|p| std::fs::metadata(p).unwrap().ino())
        .collect::<Vec<_>>();
    let mut damaged = original[0].clone();
    damaged[128 + 96 + 77] ^= 1;
    std::fs::write(&paths[0], damaged).unwrap();
    let repaired = store.repair(SCOPE, init.object_key.clone()).await.unwrap();
    assert_eq!(repaired.repaired_shards, vec![0]);
    for i in 0..4 {
        assert_eq!(std::fs::read(&paths[i]).unwrap(), original[i]);
        if i != 0 {
            assert_eq!(std::fs::metadata(&paths[i]).unwrap().ino(), inodes[i]);
        }
    }
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    assert_eq!(
        store
            .object_health(SCOPE, init.object_key.clone(), true)
            .await
            .unwrap()
            .status,
        "healthy"
    );
    assert!(
        store
            .repair(SCOPE, init.object_key)
            .await
            .unwrap()
            .repaired_shards
            .is_empty()
    );
}

#[tokio::test]
async fn read_repair_runs_after_verified_download_without_holding_its_reader() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(32769);
    let init = upload(&store, &data, Durability::Durable).await;
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    let fp = plan.identity.fingerprint();
    let pool = store.pool(plan.identity).unwrap();
    std::fs::remove_file(pool.holder(fp, 0).unwrap().path.join(fp.shard_path(0))).unwrap();
    store.configure_read_repair(true);
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if store
                .object_health(SCOPE, init.object_key.clone(), true)
                .await
                .unwrap()
                .status
                == "healthy"
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn permanent_retirement_rejects_old_tickets_after_restart() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(24577);
    let init = upload(&store, &data, Durability::Durable).await;
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    store.delete(SCOPE, init.object_key.clone()).await.unwrap();
    drop(store);
    let store = fixture.reopen();
    assert!(matches!(
        store.stat(SCOPE, init.object_key.clone()).await,
        Err(Error::Retired)
    ));
    assert!(matches!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await,
        Err(Error::Retired)
    ));
    assert!(matches!(
        store
            .object_health(SCOPE, init.object_key.clone(), true)
            .await,
        Err(Error::Retired)
    ));
    assert!(matches!(
        store.repair(SCOPE, init.object_key.clone()).await,
        Err(Error::Retired)
    ));
    assert!(matches!(
        store
            .write_part(
                SCOPE,
                init.upload_key.clone(),
                0,
                std::io::Cursor::new(data[..12288].to_vec())
            )
            .await,
        Err(Error::Retired)
    ));
    assert!(matches!(
        store
            .upload_status(SCOPE, init.upload_key.clone(), 0, 100)
            .await,
        Err(Error::Retired)
    ));
    assert!(matches!(
        store.finalize(SCOPE, init.upload_key).await,
        Err(Error::Retired)
    ));
    assert_eq!(store.delete(SCOPE, init.object_key).await.unwrap(), 0);
    for member in &fixture.paths {
        assert_eq!(
            std::fs::read_dir(member.join(".uploads")).unwrap().count(),
            0
        );
    }
}

#[tokio::test]
async fn delete_unfinished_or_never_written_identity_also_revokes_ticket() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    for written in [false, true] {
        let data = source(12288);
        let init = store
            .init(
                SCOPE,
                0,
                data.len() as u64,
                12288,
                Durability::Durable,
                3600,
            )
            .unwrap();
        if written {
            store
                .write_part(
                    SCOPE,
                    init.upload_key.clone(),
                    0,
                    std::io::Cursor::new(data.clone()),
                )
                .await
                .unwrap();
        }
        assert_eq!(store.delete(SCOPE, init.object_key).await.unwrap(), 4);
        assert!(matches!(
            store
                .write_part(
                    SCOPE,
                    init.upload_key.clone(),
                    0,
                    std::io::Cursor::new(data)
                )
                .await,
            Err(Error::Retired)
        ));
        assert!(matches!(
            store.finalize(SCOPE, init.upload_key).await,
            Err(Error::Retired)
        ));
    }
}

#[tokio::test]
async fn failed_delete_never_publishes_completed_certificate_and_retry_finishes() {
    use n1_link::retirement::{Phase, Retirement};
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(12288);
    let init = upload(&store, &data, Durability::Durable).await;
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    let fp = plan.identity.fingerprint();
    let pool = store.pool(plan.identity).unwrap();
    let blocked = pool
        .holder(fp, 0)
        .unwrap()
        .path
        .join(format!(".uploads/{fp}"));
    // All final-file probes succeed; this blocks only publication on slot 0.
    std::fs::write(&blocked, b"publication blocker").unwrap();
    assert!(store.delete(SCOPE, init.object_key.clone()).await.is_err());
    for shard in 1..4 {
        let member = pool.holder(fp, shard).unwrap();
        let bytes = std::fs::read(member.path.join(fp.shard_path(shard))).unwrap();
        assert_eq!(
            Retirement::decode(&bytes, fp, shard).unwrap().phase,
            Phase::Retiring
        );
    }
    assert!(matches!(
        store.stat(SCOPE, init.object_key.clone()).await,
        Err(Error::Retiring)
    ));
    std::fs::remove_file(blocked).unwrap();
    drop(pool);
    drop(store);
    let store = fixture.reopen();
    assert!(matches!(
        store.finalize(SCOPE, init.upload_key).await,
        Err(Error::Retiring)
    ));
    store.delete(SCOPE, init.object_key.clone()).await.unwrap();
    assert!(matches!(
        store.stat(SCOPE, init.object_key).await,
        Err(Error::Retired)
    ));
}

#[tokio::test]
async fn completed_certificate_survives_an_unavailable_other_member() {
    use n1_link::retirement::{Phase, Retirement};
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(12288);
    let init = upload(&store, &data, Durability::Durable).await;
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    let fp = plan.identity.fingerprint();
    let pool = store.pool(plan.identity).unwrap();
    for shard in 0..4 {
        pool.holder(fp, shard)
            .unwrap()
            .publish_retirement(
                fp,
                Retirement {
                    phase: Phase::Retiring,
                    shard,
                    nonce: [55; 16],
                    timestamp: 1,
                },
            )
            .unwrap();
    }
    pool.holder(fp, 0)
        .unwrap()
        .publish_retirement(
            fp,
            Retirement {
                phase: Phase::Retired,
                shard: 0,
                nonce: [55; 16],
                timestamp: 1,
            },
        )
        .unwrap();
    let missing = pool.holder(fp, 1).unwrap().path.clone();
    drop(pool);
    drop(store);
    let detached = fixture.base.join("detached");
    std::fs::rename(&missing, &detached).unwrap();
    let store = fixture.reopen();
    assert!(matches!(
        store.stat(SCOPE, init.object_key.clone()).await,
        Err(Error::Retired)
    ));
    drop(store);
    std::fs::rename(detached, missing).unwrap();
    let store = fixture.reopen();
    store.delete(SCOPE, init.object_key.clone()).await.unwrap();
    assert!(matches!(
        store.stat(SCOPE, init.object_key).await,
        Err(Error::Retired)
    ));
}

#[tokio::test]
async fn normal_put_and_group_batch_preserve_retry_identity_and_all_bytes() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let mut objects = Vec::new();
    let mut body = Vec::new();
    for size in [0, 1, 4095, 12288, 24577, 80000] {
        let data = source(size);
        let part = (size as u64).max(1).div_ceil(12288) * 12288;
        let init = store
            .init(SCOPE, 0, size as u64, part, Durability::Durable, 3600)
            .unwrap();
        body.extend_from_slice(&data);
        objects.push((init, data));
    }
    let tickets = objects
        .iter()
        .map(|(init, _)| init.upload_key.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        store.batch_length(SCOPE, &tickets).unwrap(),
        body.len() as u64
    );
    let completed = store
        .put_batch(SCOPE, tickets.clone(), std::io::Cursor::new(body.clone()))
        .await
        .unwrap();
    assert_eq!(completed.len(), objects.len());
    let repeated = store
        .put_batch(SCOPE, tickets, std::io::Cursor::new(body))
        .await
        .unwrap();
    assert!(repeated.iter().all(|o| o.already_finalized));
    for ((init, data), object) in objects.iter().zip(completed) {
        assert_eq!(object.object_key, init.object_key);
        assert_eq!(
            store
                .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
                .await
                .unwrap()
                .0,
            *data
        );
    }
    let data = source(90000);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            98304,
            Durability::Durable,
            3600,
        )
        .unwrap();
    assert!(
        !store
            .put(
                SCOPE,
                init.upload_key.clone(),
                std::io::Cursor::new(data.clone())
            )
            .await
            .unwrap()
            .already_finalized
    );
    assert!(
        store
            .put(SCOPE, init.upload_key, std::io::Cursor::new(data.clone()))
            .await
            .unwrap()
            .already_finalized
    );
    assert_eq!(
        store
            .download(SCOPE, init.object_key, 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
}

#[tokio::test]
async fn batch_short_body_never_publishes_earlier_items_and_can_resume() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(12288);
    let items = (0..3)
        .map(|_| {
            store
                .init(SCOPE, 0, 12288, 12288, Durability::Durable, 3600)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let tickets = items
        .iter()
        .map(|i| i.upload_key.clone())
        .collect::<Vec<_>>();
    let body = data.repeat(3);
    assert!(
        store
            .put_batch(
                SCOPE,
                tickets.clone(),
                std::io::Cursor::new(body[..13000].to_vec())
            )
            .await
            .is_err()
    );
    for item in &items {
        assert!(matches!(
            store.stat(SCOPE, item.object_key.clone()).await,
            Err(Error::NotFound)
        ));
    }
    store
        .put_batch(SCOPE, tickets, std::io::Cursor::new(body))
        .await
        .unwrap();
    for item in items {
        assert_eq!(
            store
                .download(SCOPE, item.object_key, 0, None, Vec::new())
                .await
                .unwrap()
                .0,
            data
        );
    }
}

#[tokio::test]
async fn batches_reject_duplicate_identities_and_cannot_recreate_retired_keys() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let init = store
        .init(SCOPE, 0, 0, 12288, Durability::Durable, 3600)
        .unwrap();
    assert!(matches!(
        store
            .put_batch(
                SCOPE,
                vec![init.upload_key.clone(), init.upload_key.clone()],
                std::io::Cursor::new(Vec::new())
            )
            .await,
        Err(Error::Invalid(_))
    ));
    store.delete(SCOPE, init.object_key).await.unwrap();
    assert!(matches!(
        store
            .put(
                SCOPE,
                init.upload_key.clone(),
                std::io::Cursor::new(Vec::new())
            )
            .await,
        Err(Error::Retired)
    ));
    assert!(matches!(
        store
            .put_batch(
                SCOPE,
                vec![init.upload_key],
                std::io::Cursor::new(Vec::new())
            )
            .await,
        Err(Error::Retired)
    ));
}

fn expire_staging(store: &Store, init: &InitializedUpload) {
    let mut plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    plan.expires_at = 1;
    let pool = store.pool(plan.identity).unwrap();
    let fp = plan.identity.fingerprint();
    // Reproduce the bytes of a genuinely expired session without sleeping for
    // wall-clock expiry. GC still validates real inline headers on every holder.
    for shard in 0..pool.layout.width() {
        let member = pool.holder(fp, shard as u16).unwrap();
        let (file, _) = member.open_stage(fp, false, false).unwrap();
        let header = n1_link::shard::ShardHeader::new(&pool.layout, plan, shard as u16).unwrap();
        crate::core::linux_io::pwrite_all_at(&*file, &header.encode(fp), 0).unwrap();
    }
}
#[tokio::test]
async fn gc_only_reclaims_known_expired_private_uploads_and_skips_active_owners() {
    use tokio_util::sync::CancellationToken;
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(12000);
    let expired = upload(&store, &data, Durability::Durable).await;
    expire_staging(&store, &expired);
    let active = upload(&store, &data, Durability::Durable).await;
    expire_staging(&store, &active);
    let live = upload(&store, &data, Durability::Durable).await;
    store.finalize(SCOPE, live.upload_key).await.unwrap();
    let retired = upload(&store, &data, Durability::Durable).await;
    store
        .delete(SCOPE, retired.object_key.clone())
        .await
        .unwrap();
    let fresh = upload(&store, &data, Durability::Durable).await;
    let fp = store
        .inspect_upload(SCOPE, &active.upload_key)
        .unwrap()
        .identity
        .fingerprint();
    let lease = store.lease(fp).unwrap();
    let guard = lease.state().lifecycle.clone().read_owned().await;
    let report = store
        .garbage_collect(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.removed_uploads, 1);
    assert_eq!(report.removed_shards, 4);
    assert_eq!(report.active_skipped, 1);
    assert!(report.errors.is_empty());
    assert!(store.stat(SCOPE, live.object_key).await.is_ok());
    assert!(matches!(
        store.stat(SCOPE, retired.object_key).await,
        Err(Error::Retired)
    ));
    assert!(
        store
            .upload_status(SCOPE, fresh.upload_key, 0, 100)
            .await
            .unwrap()
            .missing_parts
            .is_empty()
    );
    drop(guard);
    drop(lease);
    assert_eq!(
        store
            .garbage_collect(CancellationToken::new())
            .await
            .unwrap()
            .removed_uploads,
        1
    );
}
#[tokio::test]
async fn paced_scrub_repairs_damage_preserves_retirement_and_keeps_exact_data() {
    use tokio_util::sync::CancellationToken;
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40000);
    let init = upload(&store, &data, Durability::Durable).await;
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    let fp = plan.identity.fingerprint();
    let pool = store.pool(plan.identity).unwrap();
    let path = pool.holder(fp, 0).unwrap().path.join(fp.shard_path(0));
    let mut damaged = std::fs::read(&path).unwrap();
    damaged[128 + 96 + 8] ^= 1;
    std::fs::write(path, damaged).unwrap();
    let retired = upload(&store, &data, Durability::Durable).await;
    store
        .delete(SCOPE, retired.object_key.clone())
        .await
        .unwrap();
    let options = ScrubOptions {
        repair: true,
        bytes_per_second: 1024 * 1024 * 1024,
        window_seconds: 30,
    };
    let report = store
        .scrub(options, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.degraded, 1);
    assert_eq!(report.repaired, 1);
    assert_eq!(report.retired, 1);
    assert_eq!(report.incomplete, 0);
    assert!(!report.stopped);
    assert_eq!(
        store
            .download(SCOPE, init.object_key, 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    assert!(matches!(
        store.stat(SCOPE, retired.object_key).await,
        Err(Error::Retired)
    ));
}
#[tokio::test]
async fn foreground_read_preempts_slow_scrub_without_waiting_for_its_io_window() {
    use tokio_util::sync::CancellationToken;
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(10000);
    let init = upload(&store, &data, Durability::Durable).await;
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    let fp = store
        .inspect_upload(SCOPE, &init.upload_key)
        .unwrap()
        .identity
        .fingerprint();
    let s = store.clone();
    let scrub = tokio::spawn(async move {
        s.scrub(
            ScrubOptions {
                repair: true,
                bytes_per_second: 1,
                window_seconds: 60,
            },
            CancellationToken::new(),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !store.background_active(fp) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (bytes, _) = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        store.download(SCOPE, init.object_key, 0, None, Vec::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(bytes, data);
    let report = tokio::time::timeout(std::time::Duration::from_secs(1), scrub)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(report.incomplete > 0);
    assert!(!report.stopped);
}

#[tokio::test]
async fn corrupt_retirement_fences_old_payload_and_repair_even_when_k_live_shards_remain() {
    use n1_link::retirement::{Phase, Retirement};
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let bytes = source(9000);
    let init = upload(&store, &bytes, Durability::Durable).await;
    store
        .finalize(SCOPE, init.upload_key.clone())
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.upload_key).unwrap();
    let fp = plan.identity.fingerprint();
    let holder = store.pools[0].holder(fp, 0).unwrap();
    holder
        .publish_retirement(
            fp,
            Retirement {
                phase: Phase::Retiring,
                shard: 0,
                nonce: [3; 16],
                timestamp: 1,
            },
        )
        .unwrap();
    let path = holder.path.join(fp.shard_path(0));
    let mut record = std::fs::read(&path).unwrap();
    record[64] ^= 1;
    std::fs::write(path, record).unwrap();
    assert!(matches!(
        store.stat(SCOPE, init.object_key.clone()).await,
        Err(Error::RetirementCorrupt)
    ));
    assert!(matches!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await,
        Err(Error::RetirementCorrupt)
    ));
    assert!(matches!(
        store
            .object_health(SCOPE, init.object_key.clone(), true)
            .await,
        Err(Error::RetirementCorrupt)
    ));
    assert!(matches!(
        store.repair(SCOPE, init.object_key.clone()).await,
        Err(Error::RetirementCorrupt)
    ));
    assert!(matches!(
        store.finalize(SCOPE, init.upload_key.clone()).await,
        Err(Error::RetirementCorrupt)
    ));
    assert!(matches!(
        store
            .upload_status(SCOPE, init.upload_key.clone(), 0, 100)
            .await,
        Err(Error::RetirementCorrupt)
    ));
    assert!(matches!(
        store
            .write_part(
                SCOPE,
                init.upload_key.clone(),
                0,
                std::io::Cursor::new(bytes.clone())
            )
            .await,
        Err(Error::RetirementCorrupt)
    ));
    assert!(matches!(
        store
            .put(SCOPE, init.upload_key.clone(), std::io::Cursor::new(bytes))
            .await,
        Err(Error::RetirementCorrupt)
    ));
    store.delete(SCOPE, init.object_key.clone()).await.unwrap();
    drop(holder);
    drop(store);
    let reopened = fixture.reopen();
    assert!(matches!(
        reopened.stat(SCOPE, init.object_key).await,
        Err(Error::Retired)
    ));
    assert!(matches!(
        reopened.finalize(SCOPE, init.upload_key).await,
        Err(Error::Retired)
    ));
}

#[tokio::test]
async fn metadata_operations_obey_shared_admission_before_touching_shards() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let bytes = source(9000);
    let live = upload(&store, &bytes, Durability::Durable).await;
    store.finalize(SCOPE, live.upload_key).await.unwrap();
    let doomed = upload(&store, &bytes, Durability::Durable).await;
    store.finalize(SCOPE, doomed.upload_key).await.unwrap();
    let pending = upload(&store, &bytes, Durability::Durable).await;
    let mut permits = Vec::new();
    for _ in 0..16 {
        permits.push(store.admit_metadata(&store.pools[0]).await.unwrap());
    }
    let stat = {
        let store = store.clone();
        tokio::spawn(async move { store.stat(SCOPE, live.object_key).await })
    };
    let delete = {
        let store = store.clone();
        tokio::spawn(async move { store.delete(SCOPE, doomed.object_key).await })
    };
    let cancel = {
        let store = store.clone();
        tokio::spawn(async move { store.cancel_upload(SCOPE, pending.upload_key).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert!(!stat.is_finished());
    assert!(!delete.is_finished());
    assert!(!cancel.is_finished());
    drop(permits);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        assert_eq!(stat.await.unwrap().unwrap().total_bytes, 9000);
        assert_eq!(delete.await.unwrap().unwrap(), 4);
        assert_eq!(cancel.await.unwrap().unwrap(), 4);
    })
    .await
    .unwrap();
}
