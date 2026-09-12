use super::format::{Kind, Root};
use super::repository::Repository;
use super::*;
use crate::core::instance_lease::{InstanceLease, RESTORE_PENDING};
use std::fs;
use std::io::{BufReader, Write};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::PathBuf;

#[test]
fn automatic_retention_authenticates_before_deleting_and_preserves_protected_point() {
    let f = Fixture::new();
    let repo = f.repo("retention");
    fs::write(f.root().join("data"), b"first").unwrap();
    let first = capture::create(&repo, f.roots.clone(), 1).unwrap();
    fs::write(f.root().join("data"), b"second").unwrap();
    let second = capture::create(&repo, f.roots.clone(), 1).unwrap();
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
impl Fixture {
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
    let snapshot = capture::create(&repo, f.roots.clone(), 4).unwrap();
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
    let snapshot = capture::create(&repo, f.roots.clone(), 2).unwrap();
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
    let first = capture::create(&repo, f.roots.clone(), 2).unwrap();
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
    let second = capture::create(&repo, f.roots.clone(), 4).unwrap();
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
    let snapshot = capture::create(&repo, f.roots.clone(), 2).unwrap();
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
    let snapshot = capture::create(&repo, f.roots.clone(), 2).unwrap();
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
    let snapshot = capture::create(&repo, f.roots.clone(), 2).unwrap();
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
fn malformed_manifest_counts_and_duplicate_paths_fail_closed() {
    let f = Fixture::new();
    f.data();
    let repo = f.repo("repo");
    let mut snapshot = capture::create(&repo, f.roots.clone(), 2).unwrap();
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
    });
    let repo = f.repo("repo");
    let snapshot = capture::create(&repo, f.roots.clone(), 2).unwrap();
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
