//! The native protocol is deliberately exercised at the transport boundary,
//! including cancellation between frames and independent in-flight calls.
#[path = "object_client/finalization.rs"]
mod finalization;
use m1::object_store::{NativeObjectStore, ObjectStoreConfig};
use n1_link::noa::{Command, Request, Response};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Barrier, oneshot},
};

async fn read(stream: &mut TcpStream) -> Request {
    let length = stream.read_u32().await.unwrap() as usize;
    assert!((1..=8192).contains(&length));
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    rmp_serde::from_slice(&body).unwrap()
}
async fn write(stream: &mut TcpStream, value: Response) {
    let bytes = rmp_serde::to_vec_named(&value).unwrap();
    stream.write_u32(bytes.len() as u32).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
}
async fn login(stream: &mut TcpStream) -> bool {
    let request = read(stream).await;
    let password = match request.command {
        Command::Login { .. } => true,
        Command::Bearer { token } => {
            assert_eq!(token, "service-test-token");
            false
        }
        command => panic!("unexpected authentication command: {command:?}"),
    };
    write(
        stream,
        Response::ok(
            request.id,
            json!({"token":"service-test-token","session":{"exp":9_999_999_999u64}}),
        ),
    )
    .await;
    password
}
fn stat(id: u64) -> Response {
    Response::ok(
        id,
        json!({
            "total_bytes": 17,
            "data_shards": 1,
            "parity_shards": 1,
            "finalized_shards": 2,
            "integrity_algorithm": "LINK-SHA256-tree-v2",
            "integrity_root": "ab".repeat(32)
        }),
    )
}
fn client(listener: &TcpListener, connections: usize) -> NativeObjectStore {
    NativeObjectStore::new(ObjectStoreConfig {
        native_url: format!("n1://{}", listener.local_addr().unwrap()),
        fragment: "private".into(),
        secret: "private-test-secret".into(),
        max_connections: connections,
        tls: None,
    })
    .unwrap()
}

#[tokio::test]
async fn absent_upload_status_is_not_synthesized_as_a_resumable_session() {
    use m1::object_store::ObjectUpload;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = client(&listener, 1);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        login(&mut stream).await;
        let request = read(&mut stream).await;
        assert!(matches!(request.command, Command::UploadStatus { .. }));
        write(&mut stream, Response::error(request.id, 404, "absent")).await;
    });
    let upload = ObjectUpload {
        object_key: "N1A1.absent".into(),
        total_bytes: 8192,
        part_bytes: 4096,
        parts: 2,
        expires_at: 9_999_999_999,
        durable: true,
    };
    assert_eq!(store.status(&upload, 0, 2).await.unwrap_err().status(), 404);
    server.await.unwrap();
    store.shutdown().await;
}

#[tokio::test]
async fn download_reconnects_a_closed_idle_transport_before_forwarding_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = client(&listener, 1);
    let (closed, disconnected) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut old, _) = listener.accept().await.unwrap();
        assert!(login(&mut old).await);
        let request = read(&mut old).await;
        assert!(matches!(request.command, Command::ObjectStat { .. }));
        write(&mut old, stat(request.id)).await;
        drop(old);
        closed.send(()).unwrap();
        let (mut fresh, _) = listener.accept().await.unwrap();
        assert!(
            !login(&mut fresh).await,
            "reconnect must reuse the valid service session"
        );
        let request = read(&mut fresh).await;
        assert!(matches!(request.command, Command::ObjectGet { .. }));
        let mut response = Response::ok(request.id, json!({}));
        response.body_len = 5;
        write(&mut fresh, response).await;
        fresh.write_all(b"fresh").await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        assert_eq!(store.stat("N1A1.seed").await.unwrap().total_bytes, 17);
        disconnected.await.unwrap();
        let mut bytes = Vec::new();
        assert_eq!(
            store
                .download("N1A1.read", 0, Some(5), &mut bytes)
                .await
                .unwrap(),
            5
        );
        assert_eq!(bytes, b"fresh");
        server.await.unwrap();
    })
    .await
    .expect("closed idle download transport did not recover");
    store.shutdown().await;
}

#[tokio::test]
async fn download_never_replays_after_forwarding_partial_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = client(&listener, 1);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        login(&mut stream).await;
        let request = read(&mut stream).await;
        let mut response = Response::ok(request.id, json!({}));
        response.body_len = 6;
        write(&mut stream, response).await;
        stream.write_all(b"abc").await.unwrap();
        drop(stream);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "partial download was replayed"
        );
    });
    let mut bytes = Vec::new();
    assert!(
        store
            .download("N1A1.partial", 0, Some(6), &mut bytes)
            .await
            .is_err()
    );
    assert_eq!(bytes, b"abc");
    server.await.unwrap();
    store.shutdown().await;
}

#[tokio::test]
async fn download_transport_reconnect_is_bounded_to_one_fresh_attempt() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = client(&listener, 1);
    let server = tokio::spawn(async move {
        for attempt in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert_eq!(login(&mut stream).await, attempt == 0);
            let request = read(&mut stream).await;
            assert!(matches!(request.command, Command::ObjectGet { .. }));
            drop(stream);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "download reconnect loop exceeded one retry"
        );
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        assert!(
            store
                .download("N1A1.unavailable", 0, None, &mut bytes)
                .await
                .is_err()
        );
        assert!(bytes.is_empty());
        server.await.unwrap();
    })
    .await
    .expect("download reconnect did not stop");
    store.shutdown().await;
}

#[tokio::test]
async fn download_does_not_retry_complete_application_errors() {
    for status in [404, 410, 401] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let store = client(&listener, 1);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            login(&mut stream).await;
            let request = read(&mut stream).await;
            write(
                &mut stream,
                Response::error(request.id, status, "explicit object error"),
            )
            .await;
            drop(stream);
            assert!(
                tokio::time::timeout(Duration::from_millis(200), listener.accept())
                    .await
                    .is_err(),
                "application error was retried"
            );
        });
        let mut bytes = Vec::new();
        assert_eq!(
            store
                .download("N1A1.error", 0, None, &mut bytes)
                .await
                .unwrap_err()
                .status(),
            status
        );
        assert!(bytes.is_empty());
        server.await.unwrap();
        store.shutdown().await;
    }
}

#[tokio::test]
async fn download_does_not_retry_a_destination_failure_before_the_first_byte() {
    struct FailedDestination;
    impl tokio::io::AsyncWrite for FailedDestination {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "destination closed",
            )))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = client(&listener, 1);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        login(&mut stream).await;
        let request = read(&mut stream).await;
        let mut response = Response::ok(request.id, json!({}));
        response.body_len = 6;
        write(&mut stream, response).await;
        stream.write_all(b"direct").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "destination error caused backend replay"
        );
    });
    assert!(
        store
            .download("N1A1.destination", 0, Some(6), &mut FailedDestination)
            .await
            .is_err()
    );
    server.await.unwrap();
    store.shutdown().await;
}

#[tokio::test]
async fn reconnecting_download_retains_admission_and_shutdown_waits_for_its_completion() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = client(&listener, 1);
    let (closed, disconnected) = oneshot::channel();
    let (arrived, reconnected) = oneshot::channel();
    let (reply, permitted) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut old, _) = listener.accept().await.unwrap();
        login(&mut old).await;
        let request = read(&mut old).await;
        write(&mut old, stat(request.id)).await;
        drop(old);
        closed.send(()).unwrap();
        let (mut fresh, _) = listener.accept().await.unwrap();
        assert!(!login(&mut fresh).await);
        let request = read(&mut fresh).await;
        arrived.send(()).unwrap();
        permitted.await.unwrap();
        let mut response = Response::ok(request.id, json!({}));
        response.body_len = 5;
        write(&mut fresh, response).await;
        fresh.write_all(b"owned").await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        store.stat("N1A1.seed").await.unwrap();
        disconnected.await.unwrap();
        let active = store.clone();
        let download = tokio::spawn(async move {
            let mut bytes = Vec::new();
            active
                .download("N1A1.owned", 0, Some(5), &mut bytes)
                .await
                .unwrap();
            bytes
        });
        reconnected.await.unwrap();
        let stopping = store.clone();
        let shutdown = tokio::spawn(async move { stopping.shutdown().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !shutdown.is_finished(),
            "shutdown abandoned a reconnecting accepted read"
        );
        reply.send(()).unwrap();
        assert_eq!(download.await.unwrap(), b"owned");
        shutdown.await.unwrap();
        server.await.unwrap();
    })
    .await
    .expect("reconnecting download lost its admission/shutdown ownership");
    assert_eq!(store.stat("N1A1.closed").await.unwrap_err().status(), 503);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_native_calls_run_concurrently_within_the_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = client(&listener, 4);
    let barrier = Arc::new(Barrier::new(4));
    let server = tokio::spawn(async move {
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                let password = login(&mut stream).await;
                let request = read(&mut stream).await;
                assert!(matches!(request.command, Command::ObjectStat { .. }));
                barrier.wait().await;
                write(&mut stream, stat(request.id)).await;
                password
            }));
        }
        let mut passwords = 0;
        for task in tasks {
            passwords += usize::from(task.await.unwrap());
        }
        assert_eq!(
            passwords, 1,
            "one password session must authenticate all four independent transports"
        );
    });
    let mut calls = Vec::new();
    for _ in 0..4 {
        let store = store.clone();
        calls.push(tokio::spawn(async move {
            store.stat("N1A1.test").await.unwrap()
        }));
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        for call in calls {
            assert_eq!(call.await.unwrap().total_bytes, 17);
        }
        server.await.unwrap();
    })
    .await
    .expect("object calls were serialized or deadlocked");
    store.shutdown().await;
}

#[tokio::test]
async fn cancelled_exchange_is_not_returned_to_the_connection_pool() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = client(&listener, 1);
    let (arrived, requested) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut old, _) = listener.accept().await.unwrap();
        login(&mut old).await;
        let _ = read(&mut old).await;
        arrived.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            old.read(&mut byte).await.unwrap(),
            0,
            "cancelled connection remained live"
        );
        let (mut new, _) = listener.accept().await.unwrap();
        login(&mut new).await;
        let request = read(&mut new).await;
        write(&mut new, stat(request.id)).await;
    });
    let cancelled_store = store.clone();
    let task = tokio::spawn(async move { cancelled_store.stat("N1A1.cancelled").await });
    requested.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let value = tokio::time::timeout(Duration::from_secs(10), store.stat("N1A1.next"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value.total_bytes, 17);
    server.await.unwrap();
    store.shutdown().await;
    assert_eq!(store.stat("N1A1.closed").await.unwrap_err().status(), 503);
}

#[tokio::test]
async fn complete_control_errors_reuse_one_authenticated_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = client(&listener, 1);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        login(&mut stream).await;
        for status in [404, 410, 413, 429] {
            let request = read(&mut stream).await;
            assert!(matches!(request.command, Command::ObjectStat { .. }));
            write(
                &mut stream,
                Response::error(request.id, status, "complete control error"),
            )
            .await;
        }
        let request = read(&mut stream).await;
        write(&mut stream, stat(request.id)).await;
        let request = read(&mut stream).await;
        write(
            &mut stream,
            Response::error(request.id, 401, "session revoked"),
        )
        .await;
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
        let (mut replacement, _) = listener.accept().await.unwrap();
        assert!(
            login(&mut replacement).await,
            "a revoked session must not be reused"
        );
        let request = read(&mut replacement).await;
        write(&mut replacement, stat(request.id)).await;
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        for status in [404, 410, 413, 429] {
            assert_eq!(
                store.stat("N1A1.missing").await.unwrap_err().status(),
                status
            );
        }
        assert_eq!(store.stat("N1A1.present").await.unwrap().total_bytes, 17);
        assert_eq!(store.stat("N1A1.revoked").await.unwrap_err().status(), 401);
        assert_eq!(
            store
                .stat("N1A1.reauthenticated")
                .await
                .unwrap()
                .total_bytes,
            17
        );
        server.await.unwrap();
    })
    .await
    .expect("a complete error discarded the authenticated connection");
    store.shutdown().await;
}

#[tokio::test]
async fn operation_status_and_shutdown_ownership_are_preserved() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = client(&listener, 1);
    let (arrived, requested) = oneshot::channel();
    let (reply, allowed) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        login(&mut stream).await;
        let request = read(&mut stream).await;
        arrived.send(()).unwrap();
        allowed.await.unwrap();
        write(
            &mut stream,
            Response::error(request.id, 410, "retired N1A1.private-key"),
        )
        .await;
    });
    let active = store.clone();
    let request = tokio::spawn(async move { active.stat("N1A1.private-key").await });
    requested.await.unwrap();
    let shutdown_store = store.clone();
    let mut shutdown = tokio::spawn(async move {
        shutdown_store.shutdown().await;
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut shutdown)
            .await
            .is_err()
    );
    reply.send(()).unwrap();
    let error = request.await.unwrap().unwrap_err();
    assert_eq!(error.status(), 410);
    assert!(!error.to_string().contains("N1A1."));
    assert!(!format!("{error:?}").contains("N1A1."));
    shutdown.await.unwrap();
    server.await.unwrap();
}
