use super::*;
use crate::core::{Error, tests::Fixture};
use n1_link::Durability;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

const SCOPE: [u8; 16] = [11; 16];

async fn response(socket: &mut DuplexStream) -> Response {
    let length = socket.read_u32().await.unwrap() as usize;
    assert!(length < 65536);
    let mut bytes = vec![0; length];
    socket.read_exact(&mut bytes).await.unwrap();
    rmp_serde::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn direct_body_preserves_fragmented_payload_and_next_control_bytes() {
    for chunk in [1, 16384, 65537] {
        let fixture = Fixture::new();
        let store = fixture.enroll();
        let data: Vec<u8> = (0..1024 * 1024 + 17)
            .map(|i| (i * 197 + 17) as u8)
            .collect();
        let init = store
            .init(
                SCOPE,
                0,
                data.len() as u64,
                3 * 1024 * 1024,
                Durability::Durable,
                3600,
            )
            .await
            .unwrap();
        let (server, mut client) = tokio::io::duplex(65536);
        let owner = store.activity.clone();
        let target = store.clone();
        let key = init.object_key.clone();
        let bytes = data.len() as u64;
        let work = tokio::spawn(async move {
            owner
                .run(part(
                    server,
                    target,
                    Part {
                        id: 41,
                        scope: SCOPE,
                        key,
                        offset: 0,
                        bytes,
                    },
                    Ok(()),
                ))
                .await
        });
        for bytes in data.chunks(chunk) {
            client.write_all(bytes).await.unwrap();
        }
        client.write_all(b"next control frame").await.unwrap();
        let reply = response(&mut client).await;
        assert_eq!(reply.id, 41);
        assert_eq!(reply.status, 200);
        assert_eq!(reply.body_len, 0);
        assert_eq!(reply.result.unwrap()["complete"], true);
        let mut server = work.await.unwrap().unwrap();
        let mut next = [0; 18];
        server.read_exact(&mut next).await.unwrap();
        assert_eq!(&next, b"next control frame");
        store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .unwrap();
        assert_eq!(
            store
                .download(SCOPE, init.object_key.clone(), 0, None, Vec::new())
                .await
                .unwrap()
                .0,
            data
        );
        store.delete(SCOPE, init.object_key).await.unwrap();
    }
}

#[tokio::test]
async fn rejected_part_drains_only_its_body() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let (server, mut client) = tokio::io::duplex(65536);
    client.write_all(&vec![3; 16385]).await.unwrap();
    client.write_all(b"next control frame").await.unwrap();
    let mut server = part(
        server,
        store,
        Part {
            id: 42,
            scope: SCOPE,
            key: String::new(),
            offset: 0,
            bytes: 16385,
        },
        Err(Error::Conflict("rejected test part".into())),
    )
    .await
    .unwrap();
    let reply = response(&mut client).await;
    assert_eq!(reply.id, 42);
    assert_eq!(reply.status, 409);
    let mut next = [0; 18];
    server.read_exact(&mut next).await.unwrap();
    assert_eq!(&next, b"next control frame");
}

#[tokio::test]
async fn cancelled_caller_retains_part_and_socket_until_io_drains() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let init = store
        .init(
            SCOPE,
            0,
            2 * 1024 * 1024,
            3 * 1024 * 1024,
            Durability::Durable,
            3600,
        )
        .await
        .unwrap();
    let (server, mut client) = tokio::io::duplex(4096);
    let target = store.clone();
    let owner = store.activity.clone();
    let key = init.object_key.clone();
    let caller = tokio::spawn(async move {
        owner
            .run(part(
                server,
                target,
                Part {
                    id: 43,
                    scope: SCOPE,
                    key,
                    offset: 0,
                    bytes: 2 * 1024 * 1024,
                },
                Ok(()),
            ))
            .await
    });
    client.write_all(&vec![7; 48 * 1024]).await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let target = store.clone();
    let key = init.object_key.clone();
    let mut deletion = tokio::spawn(async move { target.delete(SCOPE, key).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut deletion)
            .await
            .is_err()
    );
    drop(client);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), deletion)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        4
    );
    store.activity.wait_idle().await.unwrap();
    assert!(matches!(
        store.stat(SCOPE, init.object_key).await,
        Err(Error::NotFound)
    ));
}

struct Stalled {
    first_byte: bool,
}
impl AsyncRead for Stalled {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.first_byte && bytes.remaining() != 0 {
            self.first_byte = false;
            bytes.put_slice(&[7]);
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

#[tokio::test]
async fn sender_stall_expires_and_progress_resets_the_deadline() {
    let mut socket = Stalled { first_byte: false };
    let mut reader = ProgressReader {
        socket: &mut socket,
        idle: Box::pin(tokio::time::sleep(Duration::ZERO)),
        stalled: true,
    };
    assert_eq!(
        reader.read_u8().await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    let mut socket = Stalled { first_byte: true };
    let mut reader = ProgressReader {
        socket: &mut socket,
        idle: Box::pin(tokio::time::sleep(Duration::ZERO)),
        stalled: true,
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(10), reader.read_exact(&mut [0; 2]))
            .await
            .is_err()
    );
}
