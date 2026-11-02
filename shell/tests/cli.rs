//! CLI file/stream boundary tests. Object integrity, real authentication and TLS
//! are exercised against the real engine in its native socket test suite.
use n1_link::noa::{Command, Request, Response};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::{Command as Process, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "n1-link-cli-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn credentials(&self, endpoint: &str) -> PathBuf {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let path = self.0.join("credentials.json");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        write!(
            file,
            "{}",
            json!({"native_url":endpoint,"fragment":"test","secret":"local-cli-test"})
        )
        .unwrap();
        path
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
fn process(args: &[String], cwd: &std::path::Path) -> std::process::Output {
    let mut child = Process::new(env!("CARGO_BIN_EXE_litiaina-n1-shell"))
        .args(args)
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() > end {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("CLI timed out: {output:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}
async fn request(stream: &mut TcpStream) -> Request {
    let length = stream.read_u32().await.unwrap();
    assert!(length < 65536);
    let mut bytes = vec![0; length as usize];
    stream.read_exact(&mut bytes).await.unwrap();
    rmp_serde::from_slice(&bytes).unwrap()
}
async fn reply(stream: &mut TcpStream, id: u64, value: Value, length: u64) {
    let mut response = Response::ok(id, value);
    response.body_len = length;
    let bytes = rmp_serde::to_vec_named(&response).unwrap();
    stream.write_u32(bytes.len() as u32).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();
}
async fn auth(listener: TcpListener) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    let request = request(&mut stream).await;
    assert!(matches!(request.command, Command::Login { .. }));
    reply(&mut stream, request.id, json!({"token":"test"}), 0).await;
    stream
}

#[test]
fn version_does_not_initialize_storage_or_credentials() {
    let fixture = Fixture::new();
    let output = process(&["--version".into()], &fixture.0);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("litiaina-n1-shell {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
}
#[test]
fn removed_namespace_commands_are_rejected() {
    let fixture = Fixture::new();
    for command in ["rename", "move", "list", "ls", "history", "mkdir", "export"] {
        let output = process(&[command.into()], &fixture.0);
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("unrecognized subcommand"),
            "{output:?}"
        );
    }
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_never_overwrites_an_existing_local_file() {
    let fixture = Fixture::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let credentials = fixture.credentials(&format!("n1://{}", listener.local_addr().unwrap()));
    let target = fixture.0.join("existing");
    fs::write(&target, b"preserved").unwrap();
    let server = tokio::spawn(async move {
        let mut stream = auth(listener).await;
        let mut byte = [0; 1];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
    });
    let args = vec![
        "--credentials".into(),
        credentials.display().to_string(),
        "get".into(),
        "--key".into(),
        "client-key".into(),
        "--output".into(),
        target.display().to_string(),
    ];
    let cwd = fixture.0.clone();
    let output = tokio::task::spawn_blocking(move || process(&args, &cwd))
        .await
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(fs::read(target).unwrap(), b"preserved");
    server.await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_streams_exact_ranges_and_does_not_accept_a_short_body() {
    for short in [false, true] {
        let fixture = Fixture::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let credentials = fixture.credentials(&format!("n1://{}", listener.local_addr().unwrap()));
        let target = fixture.0.join("download");
        let server = tokio::spawn(async move {
            let mut stream = auth(listener).await;
            let req = request(&mut stream).await;
            assert!(matches!(
                req.command,
                Command::ObjectGet {
                    offset: 5,
                    length: Some(8),
                    ..
                }
            ));
            reply(&mut stream, req.id, json!({"total_bytes":100}), 8).await;
            stream
                .write_all(if short { b"abc" } else { b"abcdefgh" })
                .await
                .unwrap();
        });
        let args = vec![
            "--credentials".into(),
            credentials.display().to_string(),
            "get".into(),
            "--key".into(),
            "client-key".into(),
            "--offset".into(),
            "5".into(),
            "--length".into(),
            "8".into(),
            "--output".into(),
            target.display().to_string(),
        ];
        let cwd = fixture.0.clone();
        let output = tokio::task::spawn_blocking(move || process(&args, &cwd))
            .await
            .unwrap();
        assert_eq!(output.status.success(), !short, "{output:?}");
        assert_eq!(
            fs::read(target).unwrap(),
            if short { &b"abc"[..] } else { &b"abcdefgh"[..] }
        );
        server.await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_streams_regular_file_bytes_through_native_single_part_completion() {
    let fixture = Fixture::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let credentials = fixture.credentials(&format!("n1://{}", listener.local_addr().unwrap()));
    let source = fixture.0.join("source");
    let original = (0..9000).map(|n| (n * 197 % 251) as u8).collect::<Vec<_>>();
    fs::write(&source, &original).unwrap();
    let server = tokio::spawn(async move {
        let mut stream = auth(listener).await;
        let req = request(&mut stream).await;
        assert!(matches!(req.command, Command::Pools));
        reply(&mut stream,req.id,json!({"pools":[{"id":"00000000000000000000000000000000","sealed":false,"data_shards":1,"block_bytes":4096}]}),0).await;
        let req = request(&mut stream).await;
        assert!(matches!(
            req.command,
            Command::UploadInit {
                total_bytes: 9000,
                part_bytes: 12288,
                ..
            }
        ));
        reply(
            &mut stream,
            req.id,
            json!({"upload_key":"ticket","object_key":"object"}),
            0,
        )
        .await;
        let req = request(&mut stream).await;
        assert!(matches!(req.command,Command::UploadPut{upload_key}if upload_key=="ticket"));
        assert_eq!(req.body_len, 9000);
        let mut received = vec![0; 9000];
        stream.read_exact(&mut received).await.unwrap();
        assert_eq!(received, original);
        reply(
            &mut stream,
            req.id,
            json!({"object_key":"object","total_bytes":9000}),
            0,
        )
        .await;
    });
    let args = vec![
        "--credentials".into(),
        credentials.display().to_string(),
        "put".into(),
        source.display().to_string(),
    ];
    let cwd = fixture.0.clone();
    let output = tokio::task::spawn_blocking(move || process(&args, &cwd))
        .await
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["result"]["object_key"],
        "object"
    );
    server.await.unwrap();
}
