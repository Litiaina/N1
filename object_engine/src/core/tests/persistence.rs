use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

struct ObservedRead {
    input: tokio::io::DuplexStream,
    started: Option<tokio::sync::oneshot::Sender<()>>,
}
impl AsyncRead for ObservedRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        Pin::new(&mut self.input).poll_read(cx, buf)
    }
}

#[tokio::test]
async fn lost_unused_allocation_returns_not_found_and_continuation_never_recreates_it() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let init = store
        .init(SCOPE, 0, 12288, 12288, Durability::Durable, 3600)
        .await
        .unwrap();
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    drop(store);
    // Model an unused allocation lost on power failure. No payload has ACKed.
    for root in &fixture.paths {
        std::fs::remove_file(root.join(fp.staging_path())).unwrap();
        std::fs::remove_dir(root.join(format!(".uploads/{fp}"))).unwrap();
    }
    let store = fixture.reopen();
    assert!(matches!(
        store
            .write_at(
                SCOPE,
                init.object_key.clone(),
                0,
                std::io::Cursor::new(source(12288))
            )
            .await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        store.finalize(SCOPE, init.object_key.clone()).await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        store.upload_status(SCOPE, init.object_key, 0, 1).await,
        Err(Error::NotFound)
    ));
    for root in &fixture.paths {
        assert!(!root.join(format!(".uploads/{fp}")).exists());
    }
}

#[tokio::test]
async fn first_durable_payload_cannot_ack_before_staging_ancestry_is_persisted() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            49152,
            Durability::Durable,
            3600,
        )
        .await
        .unwrap();
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    let (input, mut output) = tokio::io::duplex(1024);
    let (started, ready) = tokio::sync::oneshot::channel();
    let worker = store.clone();
    let key = init.object_key.clone();
    let caller = tokio::spawn(async move {
        worker
            .write_at(
                SCOPE,
                key,
                0,
                ObservedRead {
                    input,
                    started: Some(started),
                },
            )
            .await
    });
    ready.await.unwrap();
    let parent = fixture.paths[0].join(format!(".uploads/{fp}"));
    let displaced = fixture.paths[0].join(".uploads/displaced-for-fault");
    // Data descriptors are already open. Payload writes/data sync can complete,
    // but persisting the calculated staging ancestry must fail the first reply.
    std::fs::rename(&parent, &displaced).unwrap();
    output.write_all(&data).await.unwrap();
    output.shutdown().await.unwrap();
    assert!(caller.await.unwrap().is_err());
    std::fs::rename(&displaced, &parent).unwrap();
    store
        .write_at(
            SCOPE,
            init.object_key.clone(),
            0,
            std::io::Cursor::new(data.clone()),
        )
        .await
        .unwrap();
    drop(store);
    let store = fixture.reopen();
    assert!(
        store
            .upload_status(SCOPE, init.object_key.clone(), 0, 1)
            .await
            .unwrap()
            .missing_parts
            .is_empty()
    );
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

#[tokio::test]
async fn sparse_planned_length_never_counts_missing_frames_as_uploaded_after_restart() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            12288,
            Durability::Durable,
            3600,
        )
        .await
        .unwrap();
    store
        .write_at(
            SCOPE,
            init.object_key.clone(),
            24576,
            std::io::Cursor::new(data[24576..36864].to_vec()),
        )
        .await
        .unwrap();
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let fp = plan.identity.fingerprint();
    let length = n1_link::shard::ShardHeader::new(&fixture.layout, plan, 0)
        .unwrap()
        .file_bytes()
        .unwrap();
    for root in &fixture.paths {
        assert_eq!(
            std::fs::metadata(root.join(fp.staging_path()))
                .unwrap()
                .len(),
            length
        );
    }
    assert!(
        store
            .finalize(SCOPE, init.object_key.clone())
            .await
            .is_err()
    );
    assert!(matches!(
        store.stat(SCOPE, init.object_key.clone()).await,
        Err(Error::NotFound)
    ));
    drop(store);
    let store = fixture.reopen();
    assert_eq!(
        store
            .upload_status(SCOPE, init.object_key.clone(), 0, 4)
            .await
            .unwrap()
            .missing_parts,
        vec![0, 1, 3]
    );
    let mut writers = Vec::new();
    for part in [3, 0, 1] {
        let worker = store.clone();
        let key = init.object_key.clone();
        let start = part * 12288;
        let body = data[start..(start + 12288).min(data.len())].to_vec();
        writers.push(tokio::spawn(async move {
            worker
                .write_at(SCOPE, key, start as u64, std::io::Cursor::new(body))
                .await
        }));
    }
    for writer in writers {
        writer.await.unwrap().unwrap();
    }
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
    for root in &fixture.paths {
        assert!(!root.join(fp.staging_path()).exists());
        assert!(!root.join(format!(".uploads/{fp}")).exists());
    }
}

#[tokio::test]
async fn normal_put_failed_publication_joins_peers_and_recovers_after_reopen() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            49152,
            Durability::Durable,
            3600,
        )
        .await
        .unwrap();
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    let paths = (0..fixture.layout.width())
        .map(|shard| {
            let slot = fixture.layout.member_slot(fp, shard as u16).unwrap();
            fixture.paths[slot].join(fp.shard_path(shard as u16))
        })
        .collect::<Vec<_>>();
    // One destination refuses rename after every shard has been sealed.
    // Successful peers must still finish and the request must not ACK success.
    let (input, mut output) = tokio::io::duplex(1024);
    let (started, ready) = tokio::sync::oneshot::channel();
    let worker = store.clone();
    let key = init.object_key.clone();
    let caller = tokio::spawn(async move {
        worker
            .put(
                SCOPE,
                key,
                ObservedRead {
                    input,
                    started: Some(started),
                },
            )
            .await
    });
    ready.await.unwrap();
    std::fs::create_dir_all(&paths[0]).unwrap();
    output.write_all(&data).await.unwrap();
    output.shutdown().await.unwrap();
    assert!(caller.await.unwrap().is_err());
    for path in &paths[1..] {
        assert!(path.is_file(), "publication peer was abandoned: {path:?}");
    }
    let slot = fixture.layout.member_slot(fp, 0).unwrap();
    assert!(fixture.paths[slot].join(fp.staging_path()).is_file());
    std::fs::remove_dir(&paths[0]).unwrap();
    drop(store);
    let store = fixture.reopen();
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
    for path in paths {
        assert!(!path.exists());
    }
    for root in &fixture.paths {
        assert!(!root.join(format!(".uploads/{fp}")).exists());
    }
}

#[tokio::test]
async fn cancelled_normal_put_keeps_its_owner_until_publication_and_shutdown_drain() {
    let fixture = Fixture::new();
    let store = fixture.enroll();
    let data = source(40001);
    let init = store
        .init(
            SCOPE,
            0,
            data.len() as u64,
            49152,
            Durability::Durable,
            3600,
        )
        .await
        .unwrap();
    let fp = n1_link::ObjectKey::inspect(&init.object_key)
        .unwrap()
        .fingerprint();
    // Holding the backup fence lets bytes seal, but prevents publication.
    let plan = store.inspect_upload(SCOPE, &init.object_key).await.unwrap();
    let pool = store.pool(plan.identity).unwrap();
    let length = n1_link::shard::ShardHeader::new(&fixture.layout, plan, 0)
        .unwrap()
        .file_bytes()
        .unwrap();
    let fence = store.snapshot_gate.clone().write_owned().await;
    let worker = store.clone();
    let key = init.object_key.clone();
    let body = data.clone();
    let caller =
        tokio::spawn(async move { worker.put(SCOPE, key, std::io::Cursor::new(body)).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if pool.members.iter().flatten().all(|member| {
                std::fs::metadata(member.path.join(fp.staging_path()))
                    .is_ok_and(|m| m.len() == length)
            }) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    store.activity.close().unwrap();
    let activity = store.activity.clone();
    let mut drained = tokio::spawn(async move { activity.wait_idle().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut drained)
            .await
            .is_err()
    );
    drop(fence);
    tokio::time::timeout(std::time::Duration::from_secs(5), drained)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(pool);
    drop(store);
    let store = fixture.reopen();
    assert_eq!(
        store
            .download(SCOPE, init.object_key, 0, None, Vec::new())
            .await
            .unwrap()
            .0,
        data
    );
}
