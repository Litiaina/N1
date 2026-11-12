//! Acknowledged staged completion must outlive the process that received it.
use super::*;
use std::io::{BufRead, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::time::Duration;

const CHILD: &str = "N1_FINALIZE_CRASH_CASE";
const READY: &str = "N1_DURABLE_COMPLETION_ACKNOWLEDGED";

#[derive(serde::Serialize, serde::Deserialize)]
struct Case {
    layout: PoolLayout,
    paths: Vec<PathBuf>,
    key: String,
    length: usize,
    part_bytes: u64,
}

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn durable_completion_finalizes_after_the_acknowledging_process_is_killed() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    if let Some(path) = std::env::var_os(CHILD) {
        let case: Case = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        runtime.block_on(async {
            let pool = storage_pool::Pool::reopen_fixture(case.layout, &case.paths).unwrap();
            let store = Store::new(vec![pool], [17; 32], 16).unwrap();
            let bytes = source(case.length);
            for part in (0..(case.length as u64).div_ceil(case.part_bytes)).rev() {
                let start = (part * case.part_bytes) as usize;
                let end = (start + case.part_bytes as usize).min(bytes.len());
                store
                    .write_at(
                        SCOPE,
                        case.key.clone(),
                        start as u64,
                        std::io::Cursor::new(bytes[start..end].to_vec()),
                    )
                    .await
                    .unwrap();
            }
            // Signal readiness only after every durable part call returned.
            println!("{READY}");
            std::io::stdout().flush().unwrap();
            std::future::pending::<()>().await;
        });
        unreachable!();
    }

    let fixture = Fixture::new();
    let data = source(40001);
    let store = fixture.enroll();
    let init = runtime
        .block_on(store.init(
            SCOPE,
            0,
            data.len() as u64,
            12288,
            Durability::Durable,
            3600,
        ))
        .unwrap();
    let plan = runtime
        .block_on(store.inspect_upload(SCOPE, &init.object_key))
        .unwrap();
    let fp = plan.identity.fingerprint();
    drop(store);
    let path = fixture.base.join("crash-case.json");
    std::fs::write(
        &path,
        serde_json::to_vec(&Case {
            layout: fixture.layout.clone(),
            paths: fixture.paths.clone(),
            key: init.object_key.clone(),
            length: data.len(),
            part_bytes: 12288,
        })
        .unwrap(),
    )
    .unwrap();
    let mut child = Child(Command::new(std::env::current_exe().unwrap())
        .args(["--exact","core::tests::finalize_crash::durable_completion_finalizes_after_the_acknowledging_process_is_killed","--nocapture"])
        .env(CHILD,path).stdout(Stdio::piped()).spawn().unwrap());
    let output = child.0.stdout.take().unwrap();
    let (ready, waiting) = std::sync::mpsc::channel();
    let observer = std::thread::spawn(move || {
        for line in std::io::BufReader::new(output).lines() {
            if line.unwrap().contains(READY) {
                let _ = ready.send(());
                break;
            }
        }
    });
    waiting
        .recv_timeout(Duration::from_secs(30))
        .expect("child did not acknowledge payload");
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(libc::SIGKILL));
    observer.join().unwrap();
    runtime.block_on(async {
        let store = fixture.reopen();
        assert!(store.completion_state(fp, plan).is_none());
        let result = store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .unwrap();
        assert_eq!(result.object_key, init.object_key);
        assert!(!result.already_finalized);
        assert!(
            store
                .finalize(SCOPE, init.object_key.clone())
                .await
                .unwrap()
                .already_finalized
        );
        assert_eq!(
            store
                .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
                .await
                .unwrap()
                .0,
            data
        );
        store.delete(SCOPE, init.object_key).await.unwrap();
    });
}
