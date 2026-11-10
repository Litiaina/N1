use super::*;
use crate::{Database, Options, State, format::Generation};
use std::{fs::OpenOptions, os::unix::fs::PermissionsExt};

#[test]
fn append_and_sync_errors_never_publish_or_ack_and_stop_later_writes() {
    for device in ["/dev/full", "/dev/null"] {
        let root = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let db = Database::open(root.path(), Options::default()).unwrap();
        let original = db.0.core.current();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(device)
            .unwrap();
        let generation = Arc::new(Generation::new(
            file,
            999,
            root.path().join("never-created"),
            db.0.core.cache.clone(),
            db.0.core.poisoned.clone(),
            false,
        ));
        db.0.core.publish(State {
            generation,
            sequence: 0,
            root: 0,
            end: crate::format::DATA_START,
            commit: 0,
            anchor_end: crate::format::DATA_START,
        });
        let mut tx = db.write().unwrap();
        tx.put(b"\0must-not-appear", b"payload").unwrap();
        assert!(matches!(tx.commit(), Err(Error::Io(_))), "device={device}");
        assert_eq!(db.0.core.current().sequence, 0);
        assert_eq!(db.0.core.current().root, 0);
        assert!(matches!(db.write(), Err(Error::Poisoned)));
        db.0.core.publish(State {
            generation: original.generation.clone(),
            sequence: original.sequence,
            root: original.root,
            end: original.end,
            commit: original.commit,
            anchor_end: original.anchor_end,
        });
        drop(original);
        drop(db);
        let reopened = Database::open(root.path(), Options::default()).unwrap();
        assert!(
            reopened
                .snapshot()
                .get(b"\0must-not-appear")
                .unwrap()
                .is_none()
        );
        let mut retry = reopened.write().unwrap();
        retry.put(b"\0after-safe-reopen", b"durable").unwrap();
        retry.commit().unwrap();
    }
}

#[test]
fn drained_owner_releases_the_lease_even_while_a_duplicate_descriptor_exists() {
    let root = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let db = Database::open(root.path(), Options::default()).unwrap();
    let inherited_descriptor = db.0.duplicate_lease();
    drop(db);
    let reopened = Database::open(root.path(), Options::default()).unwrap();
    let mut tx = reopened.write().unwrap();
    tx.put(b"\0reopened", b"owns lease").unwrap();
    tx.commit().unwrap();
    drop(inherited_descriptor);
}

#[test]
fn large_batch_settings_do_not_combine_valid_transactions_into_an_oversized_record() {
    let root = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let options = Options {
        write_buffer_bytes: 64 * 1024 * 1024,
        ..Options::default()
    };
    let db = Database::open(root.path(), options.clone()).unwrap();
    // Queue the burst before starting this coordinator so scheduling cannot
    // accidentally split the oversized group and hide the failure. The
    // database's normal coordinator stays idle: no request is sent to it.
    let core = Arc::new(Core {
        path: root.path().to_owned(),
        identity: db.0.core.identity,
        options,
        state: std::sync::RwLock::new(db.0.core.current()),
        cache: db.0.core.cache.clone(),
        poisoned: db.0.core.poisoned.clone(),
        stopping: std::sync::atomic::AtomicBool::new(false),
    });
    let (sender, receiver) = mpsc::sync_channel(64);
    let mut replies = Vec::new();
    for batch in 0..36u32 {
        let mut writes = BTreeMap::new();
        for index in 0..256u32 {
            let mut key = vec![0; 4096];
            key[1..5].copy_from_slice(&(batch * 256 + index).to_be_bytes());
            writes.insert(
                key,
                transaction::Mutation {
                    sequence: 0,
                    value: Some(Arc::from(&b"durable"[..])),
                },
            );
        }
        let (reply, result) = mpsc::sync_channel(1);
        sender
            .send(Command::Commit(Request {
                data: transaction::TransactionData {
                    base: core.current(),
                    points: BTreeMap::new(),
                    ranges: Vec::new(),
                    writes,
                },
                reply,
            }))
            .unwrap();
        replies.push(result);
    }
    sender.send(Command::Shutdown).unwrap();
    run(core.clone(), receiver);
    for reply in replies {
        reply.recv().unwrap().unwrap();
    }
    let committed = core.current();
    assert_eq!(
        tree::verify(&committed.generation, committed.root, committed.sequence)
            .unwrap()
            .records,
        36 * 256
    );
    drop(committed);
    drop(core);
    drop(db);
    let reopened = Database::open(root.path(), Options::default()).unwrap();
    assert_eq!(reopened.verify().unwrap().records, 36 * 256);
}
