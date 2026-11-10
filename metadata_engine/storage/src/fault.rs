//! Process-kill barriers exist only in this crate's unit-test executable.
use std::io::Write;

pub(crate) fn pause(point: &str) {
    if std::env::var("M1_STORAGE_FAULT").as_deref() == Ok(point) {
        println!("M1_FAULT={point}");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Database, Options};
    use std::{
        fs,
        io::BufRead,
        os::unix::fs::PermissionsExt,
        process::{Command, Stdio},
        sync::mpsc,
        time::Duration,
    };

    fn options() -> Options {
        Options {
            journal_bytes: 1024 * 1024 * 1024,
            ..Options::default()
        }
    }

    #[test]
    #[ignore = "child fixture, invoked and killed by process_barriers_preserve_atomic_state"]
    fn crash_child() {
        let path = std::env::var_os("M1_STORAGE_FAULT_ROOT").unwrap();
        let db = Database::open(path, options()).unwrap();
        if std::env::var("M1_STORAGE_FAULT")
            .unwrap()
            .contains("checkpoint")
            || std::env::var("M1_STORAGE_FAULT").unwrap() == "manifest-renamed"
        {
            db.checkpoint().unwrap();
        } else {
            let mut tx = db.write().unwrap();
            tx.put(b"\0name", b"after").unwrap();
            tx.put(b"\x01version", b"after").unwrap();
            tx.put(b"\x02new", b"new").unwrap();
            tx.commit().unwrap();
        }
        panic!("fault barrier did not trigger");
    }

    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn process_barriers_preserve_atomic_state() {
        for point in [
            "values-written",
            "tree-written",
            "commit-written",
            "commit-synced",
            "root-published",
            "checkpoint-synced",
            "manifest-renamed",
            "checkpoint-installed",
        ] {
            let root = tempfile::Builder::new()
                .permissions(fs::Permissions::from_mode(0o700))
                .tempdir()
                .unwrap();
            let db = Database::open(root.path(), options()).unwrap();
            let mut tx = db.write().unwrap();
            tx.put(b"\0name", b"before").unwrap();
            tx.put(b"\x01version", b"before").unwrap();
            tx.put(b"\x03ack", b"must survive").unwrap();
            tx.commit().unwrap();
            drop(db);
            let mut child = Child(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "fault::tests::crash_child",
                        "--ignored",
                        "--nocapture",
                    ])
                    .env("M1_STORAGE_FAULT_ROOT", root.path())
                    .env("M1_STORAGE_FAULT", point)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap(),
            );
            let output = child.0.stdout.take().unwrap();
            let (ready, waiting) = mpsc::sync_channel(1);
            let selected = point.to_owned();
            let reader = std::thread::spawn(move || {
                for line in std::io::BufReader::new(output).lines() {
                    if line.unwrap().contains(&format!("M1_FAULT={selected}")) {
                        ready.send(()).unwrap();
                        return;
                    }
                }
            });
            waiting
                .recv_timeout(Duration::from_secs(30))
                .unwrap_or_else(|error| panic!("barrier={point}: {error}"));
            child.0.kill().unwrap();
            assert!(!child.0.wait().unwrap().success());
            reader.join().unwrap();
            let db = Database::open(root.path(), options()).unwrap();
            let view = db.snapshot();
            let name = view.get(b"\0name").unwrap().unwrap();
            assert_eq!(
                view.get(b"\x01version").unwrap().unwrap(),
                name,
                "barrier={point}"
            );
            assert_eq!(view.get(b"\x03ack").unwrap().unwrap(), b"must survive");
            let changed = name == b"after";
            assert_eq!(
                view.get(b"\x02new").unwrap().is_some(),
                changed,
                "barrier={point}"
            );
            if matches!(point, "commit-synced" | "root-published") {
                assert!(changed);
            }
            if matches!(point, "values-written" | "tree-written") {
                assert!(!changed);
            }
        }
    }
}
