use super::format::{Kind, Root};
use super::repository::Repository;
use super::*;
use crate::core::instance_lease::{InstanceLease, RESTORE_PENDING};
use std::fs;
use std::io::{BufReader, Write};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::PathBuf;

#[test]
fn mislabeled_v4_snapshot_recovers_authenticated_logical_membership_only() {
    let f = Fixture::new();
    let repo = f.repo("legacy-v4");
    let pool = crate::core::storage_pool::StoragePool {
        id: "A".into(),
        generation: 1,
        data_shards: 1,
        parity_shards: 0,
        roots: vec![f.root()],
        members: Vec::new(),
        legacy_roots: Vec::new(),
        sealed: false,
    };
    fs::write(
        f.root().join(".n1-pool.json"),
        serde_json::to_vec(&pool).unwrap(),
    )
    .unwrap();
    let mut snapshot = f.capture(&repo);
    snapshot.engine_format = 3;
    let encrypted = repo
        .key
        .seal(
            &format!("{}:snapshot:{}", format::FORMAT, snapshot.id),
            &serde_json::to_vec(&snapshot).unwrap(),
        )
        .unwrap();
    fs::write(
        repo.path
            .join("snapshots")
            .join(&snapshot.id)
            .join("manifest"),
        encrypted,
    )
    .unwrap();
    let normalized = repo.snapshot(&snapshot.id).unwrap();
    assert_eq!(normalized.engine_format, 4);
    assert_eq!(normalized.roots[0].member.as_ref().unwrap().slot, 0);
    assert_eq!(normalized.roots[0].member.as_ref().unwrap().pool_id, "A");
    // No authenticated flat-pool evidence means no guessed legacy migration.
    let old = f.repo("legacy-unsupported");
    fs::remove_file(f.root().join(".n1-pool.json")).unwrap();
    let mut snapshot = f.capture(&old);
    snapshot.engine_format = 3;
    let encrypted = old
        .key
        .seal(
            &format!("{}:snapshot:{}", format::FORMAT, snapshot.id),
            &serde_json::to_vec(&snapshot).unwrap(),
        )
        .unwrap();
    fs::write(
        old.path
            .join("snapshots")
            .join(&snapshot.id)
            .join("manifest"),
        encrypted,
    )
    .unwrap();
    assert!(old.snapshot(&snapshot.id).is_err());
}

#[test]
fn automatic_retention_authenticates_before_deleting_and_preserves_protected_point() {
    let f = Fixture::new();
    let repo = f.repo("retention");
    fs::write(f.root().join("data"), b"first").unwrap();
    let first = f.capture(&repo);
    fs::write(f.root().join("data"), b"second").unwrap();
    let second = f.capture(&repo);
    let block = repo.block_path(&crypto::digest(b"second")).unwrap();
    let original = fs::read(&block).unwrap();
    fs::write(&block, b"corrupted").unwrap();
    assert!(retention::prune(&repo, 1, &first.id).is_err());
    assert_eq!(repo.list().unwrap().len(), 2);
    fs::write(&block, original).unwrap();
    assert!(retention::prune(&repo, 0, &first.id).is_err());
    retention::prune(&repo, 1, &first.id).unwrap();
    assert_eq!(repo.list().unwrap().len(), 1);
    restore::verify(&repo, &repo.snapshot(&first.id).unwrap()).unwrap();
    assert!(repo.snapshot(&second.id).is_err());
}

struct Fixture {
    base: PathBuf,
    key: PathBuf,
    roots: Vec<Root>,
}

#[test]
fn corrupt_source_replication_does_not_replace_last_verified_recovery_point() {
    let f = Fixture::new();
    let source = f.repo("source-repository");
    let target = f.repo("target-repository");
    fs::write(f.root().join("data"), b"first-good").unwrap();
    let first = f.capture(&source);
    transfer::local(&source, &first, &target).unwrap();
    fs::write(f.root().join("data"), b"new-data").unwrap();
    let second = f.capture(&source);
    let block = source.block_path(&crypto::digest(b"new-data")).unwrap();
    let healthy = fs::read(&block).unwrap();
    fs::write(&block, &healthy[..healthy.len() / 2]).unwrap();
    assert!(transfer::local(&source, &second, &target).is_err());
    assert!(target.snapshot(&second.id).is_err());
    assert_eq!(target.list().unwrap().len(), 1);
    restore::verify(&target, &target.snapshot(&first.id).unwrap()).unwrap();
    assert_eq!(fs::read(f.root().join("data")).unwrap(), b"new-data");
    fs::write(&block, healthy).unwrap();
    transfer::local(&source, &second, &target).unwrap();
    transfer::local(&source, &second, &target).unwrap();
    assert_eq!(target.list().unwrap().len(), 2);
    restore::verify(&target, &target.snapshot(&second.id).unwrap()).unwrap();
}

#[test]
fn wrong_key_and_corrupt_replica_never_become_verified_snapshots() {
    let f = Fixture::new();
    let source = f.repo("source-repository");
    let target = f.repo("target-repository");
    fs::write(f.root().join("data"), b"replicated").unwrap();
    let snapshot = f.capture(&source);
    let wrong_key = f.base.join("wrong-key");
    crypto::create_key(&wrong_key).unwrap();
    let error = Repository::open(&source.path, &wrong_key, false)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("key") && !error.contains("busy"));
    let hash = crypto::digest(b"replicated");
    let conflicting = target.block_path(&hash).unwrap();
    fs::create_dir_all(conflicting.parent().unwrap()).unwrap();
    fs::write(&conflicting, b"corrupt-replica").unwrap();
    assert!(transfer::local(&source, &snapshot, &target).is_err());
    assert!(target.snapshot(&snapshot.id).is_err());
    assert_eq!(fs::read(&conflicting).unwrap(), b"corrupt-replica");
    restore::verify(&source, &snapshot).unwrap();
}
impl Fixture {
    fn capture(&self, repo: &Repository) -> super::format::Snapshot {
        self.capture_with_workers(repo, 1)
    }
    fn capture_with_workers(&self, repo: &Repository, workers: usize) -> super::format::Snapshot {
        // A parallel test's fork can briefly inherit this fixture's CLOEXEC
        // lease before exec. Only positive fresh-fixture acquisition waits;
        // active-engine/overlap rejection tests still call capture directly.
        for attempt in 0..100 {
            match capture::create(repo, self.roots.clone(), workers) {
                Ok(snapshot) => return snapshot,
                Err(error)
                    if attempt < 99
                        && error
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|e| e.kind() == std::io::ErrorKind::WouldBlock) =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(error) => panic!("fresh fixture capture: {error}"),
            }
        }
        unreachable!()
    }
    fn new() -> Self {
        let base = std::env::temp_dir().join(format!("n1-backup-tests-{}", ulid::Ulid::new()));
        fs::create_dir(&base).unwrap();
        let key = base.join("key");
        crypto::create_key(&key).unwrap();
        let root = base.join("source");
        fs::create_dir(&root).unwrap();
        drop(InstanceLease::engine(std::slice::from_ref(&root)).unwrap());
        Self {
            key,
            base,
            roots: vec![Root {
                path: root.to_str().unwrap().to_owned(),
                index: true,
                blob: true,
                member: None,
                legacy_path: None,
            }],
        }
    }
    fn root(&self) -> PathBuf {
        PathBuf::from(&self.roots[0].path)
    }
    fn repo(&self, name: &str) -> Repository {
        let path = self.base.join(name);
        Repository::initialize(&path, &self.key).unwrap();
        Repository::open(&path, &self.key, true).unwrap()
    }
    fn target(&self, name: &str) -> Vec<Root> {
        let root = self.base.join(name);
        fs::create_dir(&root).unwrap();
        vec![Root {
            path: root.to_str().unwrap().to_owned(),
            index: true,
            blob: true,
            member: None,
            legacy_path: None,
        }]
    }
    fn data(&self) -> Vec<u8> {
        let bytes = (0..format::BLOCK_BYTES + 137)
            .map(|i| (i.wrapping_mul(17) % 251) as u8)
            .collect::<Vec<_>>();
        fs::create_dir_all(self.root().join("@f_test/docs/empty")).unwrap();
        fs::create_dir_all(self.root().join(".fragment")).unwrap();
        fs::write(self.root().join("payload"), &bytes).unwrap();
        fs::write(self.root().join("empty-file"), []).unwrap();
        symlink("@o_test", self.root().join("@f_test/docs/图片.mp4")).unwrap();
        symlink("../@f_test", self.root().join(".fragment/alias")).unwrap();
        bytes
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.base);
    }
}

#[test]
fn encrypted_capture_restore_preserves_bytes_links_and_empty_directories() {
    let f = Fixture::new();
    let bytes = f.data();
    let repo = f.repo("repo");
    let snapshot = f.capture_with_workers(&repo, 4);
    restore::verify(&repo, &snapshot).unwrap();
    let target = f.target("target");
    restore::restore(&repo, &snapshot, target.clone(), false).unwrap();
    let root = PathBuf::from(&target[0].path);
    assert_eq!(fs::read(root.join("payload")).unwrap(), bytes);
    assert!(root.join("@f_test/docs/empty").is_dir());
    assert_eq!(
        fs::read_link(root.join("@f_test/docs/图片.mp4")).unwrap(),
        PathBuf::from("@o_test")
    );
    assert_eq!(
        fs::read_link(root.join(".fragment/alias")).unwrap(),
        PathBuf::from("../@f_test")
    );
    assert_eq!(fs::metadata(root.join("empty-file")).unwrap().len(), 0);
    drop(InstanceLease::engine(&[root]).unwrap());
    assert!(restore::restore(&repo, &snapshot, target.clone(), false).is_err());
    assert!(restore::restore(&repo, &snapshot, target, true).is_err());
    let hash = crypto::digest(&bytes[..format::BLOCK_BYTES]);
    assert_ne!(
        fs::read(repo.block_path(&hash).unwrap()).unwrap(),
        bytes[..format::BLOCK_BYTES]
    );
}

#[test]
fn capture_refuses_active_uninitialized_or_incomplete_engine() {
    let f = Fixture::new();
    let repo = f.repo("repo");
    let lease = InstanceLease::engine(&[f.root()]).unwrap();
    assert!(capture::create(&repo, f.roots.clone(), 2).is_err());
    drop(lease);
    fs::write(f.root().join(RESTORE_PENDING), b"incomplete").unwrap();
    assert!(InstanceLease::engine(&[f.root()]).is_err());
    assert!(capture::create(&repo, f.roots.clone(), 2).is_err());
    fs::remove_file(f.root().join(RESTORE_PENDING)).unwrap();
    fs::remove_file(f.root().join(crate::core::instance_lease::LEASE_FILE)).unwrap();
    assert!(capture::create(&repo, f.roots.clone(), 2).is_err());
}

#[test]
fn wrong_key_tampering_and_truncation_fail_before_restore() {
    let f = Fixture::new();
    let bytes = f.data();
    let repo = f.repo("repo");
    let snapshot = f.capture_with_workers(&repo, 2);
    let wrong = f.base.join("wrong-key");
    crypto::create_key(&wrong).unwrap();
    assert!(Repository::open(&repo.path, &wrong, false).is_err());
    let hash = crypto::digest(&bytes[..format::BLOCK_BYTES]);
    let path = repo.block_path(&hash).unwrap();
    let original = fs::read(&path).unwrap();
    let mut damaged = original.clone();
    damaged[40] ^= 1;
    fs::write(&path, &damaged).unwrap();
    let target = f.target("target");
    assert!(restore::restore(&repo, &snapshot, target.clone(), false).is_err());
    assert_eq!(fs::read_dir(&target[0].path).unwrap().count(), 0);
    fs::write(&path, &original[..7]).unwrap();
    assert!(restore::verify(&repo, &snapshot).is_err());
    fs::write(&path, &original).unwrap();
    restore::verify(&repo, &snapshot).unwrap();
}

#[test]
fn replica_is_verified_idempotent_incremental_and_retained() {
    let f = Fixture::new();
    f.data();
    let repo = f.repo("repo");
    let replica = f.repo("replica");
    let first = f.capture_with_workers(&repo, 2);
    transfer::local(&repo, &first, &replica).unwrap();
    let before = walkdir::WalkDir::new(replica.path.join("blocks"))
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .count();
    transfer::local(&repo, &first, &replica).unwrap();
    let after = walkdir::WalkDir::new(replica.path.join("blocks"))
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .count();
    assert_eq!(before, after);
    fs::write(f.root().join("new"), b"second recovery point").unwrap();
    let second = f.capture_with_workers(&repo, 4);
    transfer::local(&repo, &second, &replica).unwrap();
    assert_eq!(replica.list().unwrap().len(), 2);
    retention::forget(&repo, &first.id).unwrap();
    retention::gc(&repo, true).unwrap();
    restore::verify(&repo, &second).unwrap();
    restore::verify(&replica, &first).unwrap();
    assert!(retention::forget(&repo, &second.id).is_err());
    let target = f.target("target");
    restore::restore(&replica, &first, target.clone(), false).unwrap();
    assert!(!PathBuf::from(&target[0].path).join("new").exists());
}

#[test]
fn interrupted_transfer_is_never_published_and_retry_reuses_blocks() {
    let f = Fixture::new();
    f.data();
    let repo = f.repo("repo");
    let replica = f.repo("replica");
    let snapshot = f.capture_with_workers(&repo, 2);
    let begin = serde_json::json!({"message":"Begin","snapshot":snapshot,"key_id":repo.key.id});
    let mut bytes = serde_json::to_vec(&begin).unwrap();
    bytes.push(b'\n');
    assert!(
        transfer::receive(
            &replica,
            &mut BufReader::new(bytes.as_slice()),
            &mut Vec::new()
        )
        .is_err()
    );
    assert!(replica.list().unwrap().is_empty());
    transfer::local(&repo, &snapshot, &replica).unwrap();
    retention::gc(&replica, true).unwrap();
    restore::verify(&replica, &snapshot).unwrap();
}

#[test]
fn worker_error_does_not_deadlock_a_full_capture_queue() {
    let f = Fixture::new();
    let repo = f.repo("repo");
    for i in 0..200 {
        symlink("/etc/passwd", f.root().join(format!("bad-{i}"))).unwrap();
    }
    let start = std::time::Instant::now();
    assert!(capture::create(&repo, f.roots.clone(), 1).is_err());
    assert!(start.elapsed() < std::time::Duration::from_secs(5));
    assert!(repo.list().unwrap().is_empty());
}

#[test]
fn paths_permissions_repository_overlap_and_pending_intents_are_rejected() {
    for path in ["../etc", "/etc", "a/../b", "a/./b", "a//b", "", "a\0b"] {
        assert!(super::fs::safe_relative(path).is_err());
    }
    let f = Fixture::new();
    let repo = f.repo("repo");
    fs::set_permissions(&f.key, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Repository::open(&repo.path, &f.key, false).is_err());
    fs::set_permissions(&f.key, fs::Permissions::from_mode(0o600)).unwrap();
    fs::create_dir_all(f.root().join("@f_test/.intents")).unwrap();
    fs::write(f.root().join("@f_test/.intents/pending.ndx"), b"intent").unwrap();
    assert!(capture::create(&repo, f.roots.clone(), 2).is_err());
    let inside = f.root().join("backup");
    Repository::initialize(&inside, &f.key).unwrap();
    let inside = Repository::open(&inside, &f.key, true).unwrap();
    assert!(capture::create(&inside, f.roots.clone(), 2).is_err());
}

#[test]
fn interrupted_restore_is_fenced_resumable_and_refuses_foreign_files() {
    let f = Fixture::new();
    let bytes = f.data();
    let repo = f.repo("repo");
    let snapshot = f.capture_with_workers(&repo, 2);
    let target = f.target("target");
    let paths = vec![PathBuf::from(&target[0].path)];
    let marker =
        serde_json::to_vec(&(snapshot.id.as_str(), repo.key.id.as_str(), &target)).unwrap();
    drop(InstanceLease::acquire(&paths, false).unwrap());
    restore_guard::prepare(&paths, &marker, false).unwrap();
    fs::write(paths[0].join("payload"), &bytes).unwrap();
    fs::write(
        paths[0]
            .join(restore_guard::WORK)
            .join(format!("temporary-{}", ulid::Ulid::new())),
        b"partial",
    )
    .unwrap();
    fs::write(paths[0].join("foreign"), b"do not delete").unwrap();
    assert!(restore::restore(&repo, &snapshot, target.clone(), true).is_err());
    assert_eq!(
        fs::read(paths[0].join("foreign")).unwrap(),
        b"do not delete"
    );
    assert!(InstanceLease::engine(&paths).is_err());
    fs::remove_file(paths[0].join("foreign")).unwrap();
    restore::restore(&repo, &snapshot, target, true).unwrap();
    assert_eq!(fs::read(paths[0].join("payload")).unwrap(), bytes);
    drop(InstanceLease::engine(&paths).unwrap());
}

#[test]
fn metadata_restore_remaps_paths_and_rejects_shard_colocation() {
    use crate::core::disk_manager::{PartMetadata, ShardInfo};
    let f = Fixture::new();
    let target = f.target("target");
    let shard = ShardInfo {
        shard_index: 0,
        file_name: "data".to_owned(),
        absolute_path: f.root().join("data").to_str().unwrap().to_owned(),
        size_bytes: 1,
        checksum: "123".to_owned(),
        is_parity: false,
    };
    let part = PartMetadata {
        part_index: 0,
        size_bytes: 1,
        created_at: 1,
        shards: vec![shard.clone()],
        checksum: None,
    };
    let bytes = metadata::remap(
        "@f_f/.uploads/session/part_0.ndx",
        rmp_serde::to_vec(&part).unwrap(),
        &f.roots,
        &target,
    )
    .unwrap();
    let mapped: PartMetadata = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(
        mapped.shards[0].absolute_path,
        format!("{}/data", target[0].path)
    );
    let mut bad = part;
    bad.shards.push(shard);
    assert!(
        metadata::remap(
            ".uploads/session/part_0.ndx",
            rmp_serde::to_vec(&bad).unwrap(),
            &f.roots,
            &target
        )
        .is_err()
    );
    assert!(metadata::remap_path("/outside/data", &f.roots, &target).is_err());
}

#[test]
fn missing_or_foreign_pins_never_reclaim_a_completed_snapshot() {
    let f = Fixture::new();
    let bytes = f.data();
    let repo = f.repo("repo");
    let snapshot = f.capture_with_workers(&repo, 2);
    let hash = crypto::digest(&bytes[..format::BLOCK_BYTES]);
    let pin = repo
        .path
        .join("snapshots")
        .join(&snapshot.id)
        .join("blocks")
        .join(&hash[..2])
        .join(&hash);
    fs::remove_file(&pin).unwrap();
    retention::gc(&repo, true).unwrap();
    restore::verify(&repo, &snapshot).unwrap();
    fs::remove_file(&pin).unwrap();
    fs::File::create(&pin)
        .unwrap()
        .write_all(b"foreign")
        .unwrap();
    assert!(retention::gc(&repo, true).is_err());
    restore::verify(&repo, &snapshot).unwrap();
}

#[test]
fn portable_backup_keeps_logical_addresses_and_converts_original_prefixes() {
    use crate::core::backup::format::Member;
    use crate::core::disk_manager::{PartMetadata, ShardInfo};
    let f = Fixture::new();
    let mut source = f.roots.clone();
    let mut target = f.target("portable-target");
    let member = Member {
        pool_id: "A".into(),
        generation: 1,
        data_shards: 1,
        parity_shards: 0,
        slot: 0,
    };
    source[0].member = Some(member.clone());
    target[0].member = Some(member);
    source[0].legacy_path = Some("/original/member".into());
    let logical = "n1://A/1/0/@f_f/.objects/a/b/@o_o/v/piece";
    assert_eq!(
        metadata::remap_path(logical, &source, &target).unwrap(),
        logical
    );
    assert_eq!(
        metadata::remap_path(
            "/original/member/@f_f/.objects/a/b/@o_o/v/piece",
            &source,
            &target
        )
        .unwrap(),
        logical
    );
    let part = PartMetadata {
        part_index: 0,
        size_bytes: 1,
        created_at: 0,
        checksum: None,
        shards: vec![ShardInfo {
            shard_index: 0,
            file_name: "piece".into(),
            absolute_path: logical.into(),
            size_bytes: 1,
            checksum: "00000000".into(),
            is_parity: false,
        }],
    };
    let bytes = rmp_serde::to_vec(&part).unwrap();
    assert_eq!(
        metadata::remap(".uploads/u/part_0.ndx", bytes.clone(), &source, &target).unwrap(),
        bytes
    );
    for bad in [
        "n1://A/1/1/piece",
        "n1://A/2/0/piece",
        "n1://B/1/0/piece",
        "n1://A/1/0/../../escape",
    ] {
        assert!(metadata::remap_path(bad, &source, &target).is_err());
    }
}

#[test]
fn malformed_manifest_counts_and_duplicate_paths_fail_closed() {
    let f = Fixture::new();
    f.data();
    let repo = f.repo("repo");
    let mut snapshot = f.capture_with_workers(&repo, 2);
    snapshot.records += 1;
    assert!(restore::verify(&repo, &snapshot).is_err());
    snapshot.records -= 1;
    snapshot.source_bytes += 1;
    assert!(restore::verify(&repo, &snapshot).is_err());
    let pending = repo.pending(&ulid::Ulid::new().to_string()).unwrap();
    let mut writer = repository::ManifestWriter::new(&repo, pending);
    let record = format::Record {
        root: 0,
        path: "duplicate".to_owned(),
        modified_seconds: 1,
        modified_nanos: 0,
        kind: Kind::Directory,
    };
    writer.record(&record).unwrap();
    writer.record(&record).unwrap();
    writer.finish().unwrap();
    snapshot.manifest = writer.blocks;
    snapshot.records = 2;
    snapshot.source_bytes = 0;
    let target = f.target("target");
    assert!(restore::restore(&repo, &snapshot, target.clone(), false).is_err());
    assert!(InstanceLease::engine(&[PathBuf::from(&target[0].path)]).is_err());
}

#[test]
fn partial_multi_root_restore_fences_can_resume_before_and_after_completion() {
    let mut f = Fixture::new();
    f.data();
    let second = f.base.join("source2");
    fs::create_dir(&second).unwrap();
    fs::write(second.join("other"), b"second root bytes").unwrap();
    drop(InstanceLease::engine(std::slice::from_ref(&second)).unwrap());
    f.roots.push(Root {
        path: second.to_str().unwrap().to_owned(),
        index: true,
        blob: true,
        member: None,
        legacy_path: None,
    });
    let repo = f.repo("repo");
    let snapshot = f.capture_with_workers(&repo, 2);
    let mut target = f.target("target");
    target.extend(f.target("target2"));
    let paths = target
        .iter()
        .map(|r| PathBuf::from(&r.path))
        .collect::<Vec<_>>();
    drop(InstanceLease::acquire(&paths, false).unwrap());
    let marker =
        serde_json::to_vec(&(snapshot.id.as_str(), repo.key.id.as_str(), &target)).unwrap();
    super::fs::atomic_new(&paths[0].join(RESTORE_PENDING), &marker).unwrap();
    assert!(InstanceLease::engine(&paths).is_err());
    restore::restore(&repo, &snapshot, target.clone(), true).unwrap();
    assert_eq!(
        fs::read(paths[1].join("other")).unwrap(),
        b"second root bytes"
    );
    // Crash while clearing fences: one root completed, another still fenced.
    super::fs::atomic_new(&paths[1].join(RESTORE_PENDING), &marker).unwrap();
    assert!(InstanceLease::engine(&paths).is_err());
    restore::restore(&repo, &snapshot, target, true).unwrap();
    drop(InstanceLease::engine(&paths).unwrap());
}

#[test]
fn oversized_internal_metadata_is_rejected_before_snapshot_publication() {
    let f = Fixture::new();
    let repo = f.repo("repo");
    let directory = f.root().join("@f_test/.objects/aa/bb/@o_test");
    fs::create_dir_all(&directory).unwrap();
    let file = fs::File::create(directory.join("version.ndx")).unwrap();
    file.set_len(format::MAX_METADATA_BYTES as u64 + 1).unwrap();
    assert!(capture::create(&repo, f.roots.clone(), 2).is_err());
    assert!(repo.list().unwrap().is_empty());
}

#[test]
fn unsupported_timestamps_never_become_completed_snapshots() {
    let f = Fixture::new();
    let repo = f.repo("repo");
    let file = fs::File::create(f.root().join("bad-time")).unwrap();
    file.set_times(
        fs::FileTimes::new()
            .set_modified(std::time::UNIX_EPOCH - std::time::Duration::from_secs(1)),
    )
    .unwrap();
    assert!(capture::create(&repo, f.roots.clone(), 2).is_err());
    assert!(repo.list().unwrap().is_empty());
}
