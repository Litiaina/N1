//! Rust process witness: durable acknowledgements survive a forced process exit.
use n1_link::PoolId;
mod common;
use n1_metadata_engine::{MetadataEngine, Options, RecordKey, Table};
use std::{
    io::{BufRead, Write},
    process::{Command, Stdio},
    sync::mpsc,
    time::Duration,
};

fn key(index: u64) -> RecordKey {
    RecordKey::new(PoolId([19; 16]), format!("committed/{index:04}")).unwrap()
}

#[test]
#[ignore = "subprocess fixture; exercised by the crash recovery test"]
fn metadata_crash_child() {
    let path = std::env::var_os("N1_METADATA_CRASH_CHILD")
        .expect("subprocess fixture requires a private database directory");
    let engine = MetadataEngine::open(path, Options::default()).unwrap();
    for index in 0u64..32 {
        engine
            .transact(1, |tx| {
                tx.put(Table::Entries, &key(index), &index.to_be_bytes())?;
                tx.put(Table::Versions, &key(index), &index.to_be_bytes())
            })
            .unwrap();
    }
    // Leave a live transaction behind the acknowledgement boundary. It must
    // not appear after process death, while all acknowledged rows must appear.
    let mut pending = engine.write().unwrap();
    pending
        .put(Table::Uploads, &key(32), b"uncommitted")
        .unwrap();
    println!("N1_METADATA_ACK=32");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

struct ChildGuard(std::process::Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn durable_cross_table_acknowledgements_survive_process_kill() {
    let directory = match std::env::var_os("N1_METADATA_TEST_ROOT") {
        Some(parent) => common::directory(Some(std::path::Path::new(&parent))),
        None => common::directory(None),
    };
    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "metadata_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("N1_METADATA_CRASH_CHILD", directory.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let output = child.0.stdout.take().unwrap();
    let (ready, wait_ready) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(output).lines() {
            if line.unwrap() == "N1_METADATA_ACK=32" {
                ready.send(()).unwrap();
                return;
            }
        }
    });
    wait_ready
        .recv_timeout(Duration::from_secs(60))
        .expect("child did not durably acknowledge its transactions");
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    reader.join().unwrap();
    let engine = MetadataEngine::open(directory.path(), Options::default()).unwrap();
    let snapshot = engine.snapshot();
    for index in 0u64..32 {
        for table in [Table::Entries, Table::Versions] {
            assert_eq!(
                snapshot.get(table, &key(index)).unwrap().unwrap(),
                index.to_be_bytes()
            );
        }
    }
    assert!(snapshot.get(Table::Uploads, &key(32)).unwrap().is_none());
    println!(
        "verified 32 acknowledged cross-table transactions after process kill at {}",
        directory.path().display()
    );
}
