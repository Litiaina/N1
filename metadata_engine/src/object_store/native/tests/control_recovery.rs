//! Disconnected read-only controls retry once; mutations and API errors do not.
use super::*;

async fn login(stream: &mut TcpStream) {
    let auth = request(stream).await;
    assert!(matches!(auth.command, Command::Bearer { .. }));
    respond(
        stream,
        Response::ok(
            auth.id,
            json!({
                "session":{"exp":9_999_999_999u64}
            }),
        ),
    )
    .await;
}

fn store(listener: &TcpListener) -> NativeObjectStore {
    NativeObjectStore::new(ObjectStoreConfig {
        native_url: format!("n1://{}", listener.local_addr().unwrap()),
        fragment: "private".into(),
        secret: "private-test-secret".into(),
        max_connections: 1,
        tls: None,
    })
    .unwrap()
}

#[tokio::test]
async fn upload_status_reconnects_once_after_a_pooled_transport_disconnects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let objects = store(&listener);
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        authenticated_stat(&mut first, true).await;
        let failed = request(&mut first).await;
        assert!(matches!(
            &failed.command,
            Command::UploadStatus {
                start: 1,
                limit: 1,
                ..
            }
        ));
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        login(&mut second).await;
        let recovered = request(&mut second).await;
        assert_eq!(
            serde_json::to_value(&failed.command).unwrap(),
            serde_json::to_value(&recovered.command).unwrap()
        );
        let status = json!({
            "finalized": false,
            "finalized_shards": 0,
            "parts": 1,
            "checked_start": 1,
            "checked_end": 1,
            "missing_parts": [],
            "unavailable_members": []
        });
        respond(&mut second, Response::ok(recovered.id, status)).await;
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        objects.stat("N1A1.seed").await.unwrap();
        let upload = ObjectUpload {
            object_key: "N1A1.pending".into(),
            total_bytes: 4096,
            part_bytes: 4096,
            parts: 1,
            expires_at: 9_999_999_999,
            durable: true,
        };
        let status = objects.status(&upload, 1, 1).await.unwrap();
        assert_eq!(status.checked_start, 1);
        assert!(status.missing_parts.is_empty());
        server.await.unwrap();
    })
    .await
    .unwrap();
    objects.shutdown().await;
}

#[tokio::test]
async fn a_second_disconnected_read_fails_without_another_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let objects = store(&listener);
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        authenticated_stat(&mut first, true).await;
        assert!(matches!(
            request(&mut first).await.command,
            Command::ObjectStat { .. }
        ));
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        login(&mut second).await;
        assert!(matches!(
            request(&mut second).await.command,
            Command::ObjectStat { .. }
        ));
        drop(second);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        );
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        objects.stat("N1A1.seed").await.unwrap();
        assert_eq!(objects.stat("N1A1.query").await.unwrap_err().status(), 502);
        server.await.unwrap();
    })
    .await
    .unwrap();
    objects.shutdown().await;
}

#[tokio::test]
async fn allocation_is_never_replayed_after_a_disconnected_reply() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let objects = store(&listener);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        authenticated_stat(&mut stream, true).await;
        assert!(matches!(
            request(&mut stream).await.command,
            Command::UploadInit { .. }
        ));
        drop(stream);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        );
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        objects.stat("N1A1.seed").await.unwrap();
        assert_eq!(
            objects
                .init(None, 4096, 4096, true, 3600)
                .await
                .err()
                .unwrap()
                .status(),
            502
        );
        server.await.unwrap();
    })
    .await
    .unwrap();
    objects.shutdown().await;
}

#[tokio::test]
async fn a_complete_read_error_is_returned_without_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let objects = store(&listener);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        authenticated_stat(&mut stream, true).await;
        let stat = request(&mut stream).await;
        respond(&mut stream, Response::error(stat.id, 404, "absent")).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(200), request(&mut stream))
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        );
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        objects.stat("N1A1.seed").await.unwrap();
        assert_eq!(objects.stat("N1A1.absent").await.unwrap_err().status(), 404);
        server.await.unwrap();
    })
    .await
    .unwrap();
    objects.shutdown().await;
}
