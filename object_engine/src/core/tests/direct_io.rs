use super::*;
use std::os::{fd::AsRawFd, unix::fs::MetadataExt};

#[tokio::test]
async fn direct_stage_open_is_existing_only_and_checks_inode_and_flags() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let init = store
        .init(SCOPE, 0, 2 * 1024 * 1024, 12288, Durability::Durable, 3600)
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let pool = store.pool(plan.identity).unwrap();
    let fp = plan.identity.fingerprint();
    let files = store.files(pool, plan).await.unwrap();
    let file = files[0].as_ref().unwrap();
    let direct = file.member.open_stage_direct(fp, &file.file).unwrap();
    let flags = unsafe { libc::fcntl(direct.as_raw_fd(), libc::F_GETFL) };
    assert!(flags >= 0);
    assert_ne!(flags & libc::O_DIRECT, 0);
    assert_eq!(
        direct.metadata().unwrap().ino(),
        file.file.metadata().unwrap().ino()
    );
    assert_eq!(
        direct.metadata().unwrap().len(),
        128,
        "opening cannot preallocate or truncate"
    );
    let other = std::fs::File::create(fixture.base.join("other-inode")).unwrap();
    assert!(file.member.open_stage_direct(fp, &other).is_err());
    store.delete(SCOPE, init.object_key).await.unwrap();
    assert!(file.member.open_stage_direct(fp, &file.file).is_err());
    assert!(!file.member.path.join(fp.staging_path()).exists());
}

#[tokio::test]
async fn sparse_upload_retry_preserves_written_frames_after_an_interrupted_part() {
    interrupted_part(4096, 3 * 1024 * 1024).await;
}

#[tokio::test]
async fn sparse_large_parts_preserve_restart_holes_and_conflicting_retries() {
    interrupted_part(262144, 12 * 1024 * 1024).await;
}

async fn interrupted_part(block_bytes: u32, part_bytes: usize) {
    for durability in [Durability::Standard, Durability::Durable] {
        let mut fixture = Fixture::new();
        fixture.layout.block_bytes = block_bytes;
        let store = fixture.enroll();
        let data = source(2 * part_bytes);
        let init = store
            .init(
                SCOPE,
                0,
                data.len() as u64,
                part_bytes as u64,
                durability,
                3600,
            )
            .await
            .unwrap();
        // Several complete frame groups precede the truncated input. The
        // interrupted part therefore contains real records followed by holes.
        assert!(
            store
                .write_part(
                    SCOPE,
                    init.object_key.clone(),
                    0,
                    std::io::Cursor::new(data[..part_bytes / 2].to_vec()),
                )
                .await
                .is_err()
        );
        drop(store);
        let store = fixture.reopen();
        // Unwritten sparse ranges are not acknowledged parts and cannot make
        // an interrupted upload appear complete after a restart.
        let status = store
            .upload_status(SCOPE, init.object_key.clone(), 0, 2)
            .await
            .unwrap();
        assert_eq!(status.missing_parts, vec![0, 1]);
        assert!(
            store
                .finalize(SCOPE, init.object_key.clone())
                .await
                .is_err()
        );
        let mut conflicting = data[..part_bytes].to_vec();
        conflicting[0] ^= 1;
        assert!(matches!(
            store
                .write_part(
                    SCOPE,
                    init.object_key.clone(),
                    0,
                    std::io::Cursor::new(conflicting)
                )
                .await,
            Err(Error::Conflict(_))
        ));
        // Fill the other part first, then resume the partially written range.
        // Extent queries must not hide either existing records or their bytes.
        for part in [1, 0] {
            let start = part * part_bytes;
            store
                .write_part(
                    SCOPE,
                    init.object_key.clone(),
                    part as u64,
                    std::io::Cursor::new(data[start..start + part_bytes].to_vec()),
                )
                .await
                .unwrap();
        }
        store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .unwrap();
        let (download, _) = store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap();
        assert_eq!(download, data);
        store.delete(SCOPE, init.object_key).await.unwrap();
    }
}

#[tokio::test]
async fn direct_multipart_preserves_shared_boundary_pages_retries_and_restart() {
    for writer_buffer_size in [0, 4096, 12288] {
        let fixture = Fixture::new();
        let store = fixture.enroll();
        store
            .configure_runtime(crate::settings::runtime::RuntimeConfig {
                writer_buffer_size,
                ..Default::default()
            })
            .unwrap();
        let mut data = source(2 * 1024 * 1024 + 17);
        // A final partial source stripe still has immutable zero padding.
        let tail = data.len() - 17;
        data[tail..].fill(0);
        let part_bytes = 147456;
        let init = store
            .init(
                SCOPE,
                0,
                data.len() as u64,
                part_bytes,
                Durability::Durable,
                3600,
            )
            .await
            .unwrap();
        let parts = data.len().div_ceil(part_bytes as usize);
        let mut writers = Vec::new();
        // Alternate holes with out-of-order concurrent writers, leaving the
        // other halves for a fresh process-local Store after reopening.
        for part in (0..parts).rev().filter(|part| part % 2 == 0) {
            let store = store.clone();
            let key = init.object_key.clone();
            let start = part * part_bytes as usize;
            let body = data[start..(start + part_bytes as usize).min(data.len())].to_vec();
            writers.push(tokio::spawn(async move {
                store
                    .write_at(SCOPE, key, start as u64, std::io::Cursor::new(body))
                    .await
            }));
        }
        for writer in writers {
            writer.await.unwrap().unwrap();
        }
        drop(store);
        let store = fixture.reopen();
        let status = store
            .upload_status(SCOPE, init.object_key.clone(), 0, parts as u64)
            .await
            .unwrap();
        assert_eq!(
            status.missing_parts,
            (0..parts as u64).filter(|p| p % 2 == 1).collect::<Vec<_>>()
        );
        let mut writers = Vec::new();
        // Retry acknowledged even parts while completing odd parts. Status
        // reads above also populate the cache before direct writes invalidate
        // their complete pages. Every neighboring part must remain intact.
        for part in (0..parts).rev() {
            let store = store.clone();
            let key = init.object_key.clone();
            let start = part * part_bytes as usize;
            let body = data[start..(start + part_bytes as usize).min(data.len())].to_vec();
            writers.push(tokio::spawn(async move {
                store
                    .write_at(SCOPE, key, start as u64, std::io::Cursor::new(body))
                    .await
            }));
        }
        for writer in writers {
            writer.await.unwrap().unwrap();
        }
        let mut conflict = data[..part_bytes as usize].to_vec();
        conflict[4095] ^= 1;
        assert!(matches!(
            store
                .write_at(
                    SCOPE,
                    init.object_key.clone(),
                    0,
                    std::io::Cursor::new(conflict)
                )
                .await,
            Err(Error::Conflict(_))
        ));
        store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .unwrap();
        drop(store);
        let store = fixture.reopen();
        assert!(
            store
                .finalize(SCOPE, init.object_key.clone())
                .await
                .unwrap()
                .already_finalized
        );
        let (download, _) = store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap();
        assert_eq!(download, data);
        store.delete(SCOPE, init.object_key.clone()).await.unwrap();
        assert!(matches!(
            store
                .write_at(
                    SCOPE,
                    init.object_key,
                    0,
                    std::io::Cursor::new(data[..part_bytes as usize].to_vec())
                )
                .await,
            Err(Error::NotFound)
        ));
    }
}

#[tokio::test]
#[ignore = "requires a physical XFS test root; proves payload bypasses the page cache"]
async fn large_put_bypasses_page_cache_and_survives_reopen() {
    let mut fixture = Fixture::new();
    fixture.layout.block_bytes = 262144;
    let store = fixture.enroll();
    let data = source(16 * 1024 * 1024 + 1);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            24 * 1024 * 1024,
            Durability::Durable,
            3600,
        )
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let fp = plan.identity.fingerprint();
    store
        .put(
            SCOPE,
            init.object_key.clone(),
            std::io::Cursor::new(data.clone()),
        )
        .await
        .unwrap();
    let pool = store.pool(plan.identity).unwrap();
    for shard in 0..pool.layout.width() {
        let member = pool.holder(fp, shard as u16).unwrap();
        let file = member
            .open_read(std::path::Path::new(&fp.shard_path(shard as u16)))
            .unwrap();
        let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
        assert_eq!(unsafe { libc::fstatfs(file.as_raw_fd(), &mut stat) }, 0);
        assert_eq!(
            stat.f_type,
            libc::XFS_SUPER_MAGIC,
            "this proof requires XFS"
        );
        let offset = 2 * 1024 * 1024;
        let length = 1024 * 1024;
        let mapping = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                offset,
            )
        };
        assert_ne!(mapping, libc::MAP_FAILED);
        let mut resident = vec![0_u8; length / 4096];
        let result = unsafe { libc::mincore(mapping, length, resident.as_mut_ptr()) };
        assert_eq!(unsafe { libc::munmap(mapping, length) }, 0);
        assert_eq!(result, 0);
        assert!(
            resident.iter().all(|page| page & 1 == 0),
            "a complete interior MiB must bypass the page cache on every member"
        );
    }
    drop(pool);
    drop(store);
    let store = fixture.reopen();
    assert_eq!(
        store
            .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
    store.delete(SCOPE, init.object_key).await.unwrap();
}
