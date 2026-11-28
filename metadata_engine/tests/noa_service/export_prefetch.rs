//! Hold one source ahead of a large entry without stalling the N1 process.
use super::*;
use futures_util::TryStreamExt;
use n1_link::noa::{MAX_CONTROL_BYTES, Request, Response};
use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

const FIRST_BYTES: usize = 2 * 1024 * 1024;
const SECOND_BYTES: usize = 24 * 1024 * 1024;

async fn relay_control(source: &mut TcpStream, destination: &mut TcpStream) -> io::Result<Vec<u8>> {
    let length = source.read_u32().await?;
    assert!((1..=MAX_CONTROL_BYTES).contains(&(length as usize)));
    let mut control = vec![0; length as usize];
    source.read_exact(&mut control).await?;
    destination.write_u32(length).await?;
    destination.write_all(&control).await?;
    Ok(control)
}

async fn delay_first_object(mut metadata: TcpStream, target: String) -> io::Result<()> {
    let mut object = TcpStream::connect(target).await?;
    metadata.set_nodelay(true)?;
    object.set_nodelay(true)?;
    loop {
        let request = relay_control(&mut metadata, &mut object).await?;
        let request: Request = rmp_serde::from_slice(&request).unwrap();
        assert_eq!(request.body_len, 0, "this proxy is for read-only fixtures");
        let response = relay_control(&mut object, &mut metadata).await?;
        let response: Response = rmp_serde::from_slice(&response).unwrap();
        if response.body_len == FIRST_BYTES as u64 {
            let mut bytes = vec![0; FIRST_BYTES];
            object.read_exact(&mut bytes).await?;
            // N1 has sent the first entry. Delay its delivery to M1 long
            // enough to make a later, undrained GET hit N1's socket deadline.
            tokio::time::sleep(Duration::from_secs(35)).await;
            metadata.write_all(&bytes).await?;
        } else if response.body_len != 0 {
            let mut body = (&mut object).take(response.body_len);
            let bytes = tokio::io::copy(&mut body, &mut metadata).await?;
            if bytes != response.body_len {
                eprintln!(
                    "N1 ended the source stream at {bytes}/{} bytes",
                    response.body_len
                );
                return Ok(());
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real N1/M1 processes with a delayed first export object"]
async fn retained_tar_streams_large_entries_when_the_previous_entry_is_slow() {
    let mut stack = Stack::with_block_bytes(262144).await;
    let client = Client::builder()
        .timeout(Duration::from_secs(90))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token, _) = maintenance::auth(&client, &base, "export-prefetch").await;
    let entries = [
        ("folder/01-first.bin", FIRST_BYTES, 0x13),
        ("folder/02-later.bin", SECOND_BYTES, 0x24),
        ("folder/03-small.bin", 4096, 0x35),
    ];
    for (path, size, byte) in entries {
        maintenance::put(&client, &base, &token, path, &vec![byte; size]).await;
    }
    drop(stack.metadata.take());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let path = stack.root.join("m1_config.ini");
    let mut ini = ini::Ini::load_from_file(&path).unwrap();
    let mut config = ini.with_section(Some("metadata_engine"));
    let target = config
        .get("object_native_url")
        .unwrap()
        .strip_prefix("n1://")
        .unwrap()
        .to_owned();
    config.set(
        "object_native_url",
        format!("n1://{}", listener.local_addr().unwrap()),
    );
    ini.with_section(Some("export"))
        .set("object_read_concurrency", "3");
    ini.write_to_file(path).unwrap();
    let proxy = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        while let Ok((socket, _)) = listener.accept().await {
            connections.spawn(delay_first_object(socket, target.clone()));
            while connections.try_join_next().is_some() {}
        }
    });
    stack.start_metadata().await;

    let created = json_response(
        client
            .post(format!("{base}/exports"))
            .bearer_auth(&token)
            .json(&json!({"prefix":"folder","format":"tar"}))
            .send()
            .await
            .unwrap(),
        202,
    )
    .await;
    let id = created["export_id"].as_str().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let state = json_response(
            client
                .get(format!("{base}/exports/{id}"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        if state["phase"] == "ready" {
            assert_eq!(state["objects_completed"], entries.len());
            break;
        }
        assert_ne!(state["phase"], "failed", "{state}");
        assert!(tokio::time::Instant::now() < deadline, "{state}");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let archive = client
        .get(format!("{base}/exports/{id}/download"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(archive.status(), 200);
    let bytes = archive.bytes().await.unwrap();
    let mut archive = tokio_tar::Archive::new(bytes.as_ref());
    let mut files = archive.entries().unwrap();
    for (path, size, byte) in entries {
        let mut entry = files.try_next().await.unwrap().unwrap();
        assert_eq!(entry.path().unwrap().to_str(), Some(path));
        let mut actual = Vec::new();
        entry.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual.len(), size);
        assert!(actual.iter().all(|actual| *actual == byte));
    }
    assert!(files.try_next().await.unwrap().is_none());
    proxy.abort();
}
