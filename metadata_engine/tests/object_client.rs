//! The native protocol is deliberately exercised at the transport boundary,
//! including cancellation between frames and independent in-flight calls.
use n1_link::noa::{Command, Request, Response};
use n1_metadata_engine::object_store::{NativeObjectStore, ObjectStoreConfig};
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
        json!({"total_bytes":17,"data_shards":1,"parity_shards":1,"finalized_shards":2,"integrity_algorithm":"LINK-SHA256-tree-v2","integrity_root":"ab".repeat(32)}),
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
