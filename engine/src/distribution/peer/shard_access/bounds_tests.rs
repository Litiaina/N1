use super::*;
use crate::protocol::frame::{read_request_control, write_response};

#[tokio::test]
async fn dropped_temporary_cleanup_removes_only_its_attempt_and_can_be_disarmed() {
    let root = std::env::temp_dir().join(format!("n2-peer-temp-cleanup-{}", Ulid::new()));
    fs::create_dir_all(&root).await.unwrap();
    let stable = root.join("committed.shard");
    let failed = root.join("committed.shard.tmp.failed");
    let retry = root.join("committed.shard.tmp.retry");
    fs::write(&stable, b"committed").await.unwrap();
    fs::write(&failed, b"partial").await.unwrap();
    fs::write(&retry, b"other attempt").await.unwrap();
    let mut retained = TemporaryShardCleanup::new(&retry);
    retained.disarm();
    drop(retained);
    drop(TemporaryShardCleanup::new(&failed));
    tokio::time::timeout(Duration::from_secs(3), async {
        while failed.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("exact failed attempt is unlinked");
    assert_eq!(fs::read(&stable).await.unwrap(), b"committed");
    assert_eq!(fs::read(&retry).await.unwrap(), b"other attempt");
    fs::remove_dir_all(root).await.unwrap();
}

fn locator(endpoint: String) -> PeerShardLocator {
    PeerShardLocator {
        pool_id: "pool".into(),
        pool_generation: 1,
        node_id: "node".into(),
        instance_id: "instance".into(),
        binding_epoch: 1,
        root_id: "root".into(),
        endpoint,
        fragment: "fragment".into(),
        object_id: "object".into(),
        version_id: "version".into(),
        part_index: 0,
        shard_index: 0,
        is_parity: false,
    }
}

async fn http_reply(reply: &'static [u8]) -> (PeerShardLocator, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 8192];
        socket.read(&mut request).await.unwrap();
        socket.write_all(reply).await.unwrap();
    });
    (locator(endpoint), task)
}

#[tokio::test]
async fn http_ranges_reject_declared_and_chunked_overflow_without_collecting_it() {
    let (peer, server) = http_reply(
        b"HTTP/1.1 200 OK\r\nContent-Length: 1099511627776\r\nConnection: close\r\n\r\n",
    )
    .await;
    let error = http_get_peer_shard(&peer, 0, 4).await.unwrap_err();
    assert!(error.to_string().contains("exceeds requested range"));
    server.await.unwrap();

    let (peer, server) = http_reply(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n",
    )
    .await;
    let error = http_get_peer_shard(&peer, 0, 4).await.unwrap_err();
    assert!(error.to_string().contains("exceeds requested range"));
    server.await.unwrap();
}

#[tokio::test]
async fn http_ranges_allow_exact_short_and_empty_bodies_for_caller_validation() {
    for (reply, limit, expected) in [
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndata".as_slice(),
            4,
            b"data".as_slice(),
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nda".as_slice(),
            4,
            b"da".as_slice(),
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".as_slice(),
            0,
            b"".as_slice(),
        ),
    ] {
        let (peer, server) = http_reply(reply).await;
        assert_eq!(
            http_get_peer_shard(&peer, 0, limit).await.unwrap(),
            expected
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn native_ranges_reject_huge_declared_body_before_allocating_or_reading() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = locator(format!("n2://{}", listener.local_addr().unwrap()));
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request_control(&mut socket).await.unwrap().unwrap();
        write_response(
            &mut socket,
            &NativeResponse {
                id: request.id,
                ok: true,
                status: Some(200),
                payload: None,
                body_len: Some(1 << 40),
                body: None,
                error: None,
            },
        )
        .await
        .unwrap();
    });
    let error = native_get_peer_shard(&peer, 0, 4).await.unwrap_err();
    assert!(error.to_string().contains("exceeds requested range"));
    server.await.unwrap();
}
