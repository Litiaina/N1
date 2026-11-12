//! Wire-level recovery bounds, negative responses and shutdown ownership.
use super::*;
use m1::object_store::ObjectUpload;

const RECONCILE: &str =
    "completion state unavailable; query upload status or resume before finalizing";

fn upload(parts: u64) -> ObjectUpload {
    ObjectUpload {
        object_key: "N1A1.original-test-identity".into(),
        total_bytes: parts * 4096,
        part_bytes: 4096,
        parts,
        expires_at: 9_999_999_999,
        durable: true,
    }
}
fn finalized(id: u64, upload: &ObjectUpload) -> Response {
    Response::ok(
        id,
        json!({"object_key":upload.object_key,"total_bytes":upload.total_bytes,"already_finalized":false}),
    )
}
fn page(id: u64, parts: u64, start: u64, end: u64) -> Response {
    Response::ok(
        id,
        json!({"parts":parts,"checked_start":start,"checked_end":end,"missing_parts":[],
            "unavailable_members":[],"finalized":false,"finalized_shards":0}),
    )
}
async fn closed(stream: &mut TcpStream) {
    assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
}

#[tokio::test]
async fn healthy_finalize_and_unrelated_errors_never_reconcile_or_allocate() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for status in [None, Some(409), Some(404), Some(401)] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let store = client(&listener, 1);
            let upload = upload(2);
            let expected = upload.clone();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                login(&mut stream).await;
                let request = read(&mut stream).await;
                assert!(matches!(request.command,Command::UploadFinalize { object_key } if object_key==expected.object_key));
                let response = status.map_or_else(
                    || finalized(request.id, &expected),
                    |code| Response::error(request.id, code, "upload key has expired"),
                );
                write(&mut stream, response).await;
                closed(&mut stream).await;
            });
            let result = store.finalize(&upload).await;
            match status {
                Some(code) => assert_eq!(result.err().unwrap().status(), code),
                None => assert_eq!(result.unwrap().object_key, upload.object_key),
            }
            store.shutdown().await;
            server.await.unwrap();
        }
    })
    .await
    .expect("healthy finalize added an unexpected native operation");
}

#[tokio::test]
async fn cold_finalize_reconciles_exact_pages_and_retries_at_most_once() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for retry_fails in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let store = client(&listener, 1);
            let upload = upload(5001);
            let expected = upload.clone();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                login(&mut stream).await;
                let request = read(&mut stream).await;
                assert!(matches!(request.command,Command::UploadFinalize { object_key } if object_key==expected.object_key));
                write(&mut stream, Response::error(request.id, 409, RECONCILE)).await;
                for (start, end) in [(0, 4096), (4096, 5001)] {
                    let request = read(&mut stream).await;
                    assert!(matches!(request.command,Command::UploadStatus { object_key,start:actual,limit:4096 }
                        if object_key==expected.object_key && actual==start));
                    write(&mut stream, page(request.id, 5001, start, end)).await;
                }
                let request = read(&mut stream).await;
                assert!(matches!(request.command,Command::UploadFinalize { object_key } if object_key==expected.object_key));
                let response = if retry_fails {
                    Response::error(request.id, 409, RECONCILE)
                } else {
                    finalized(request.id, &expected)
                };
                write(&mut stream, response).await;
                closed(&mut stream).await;
            });
            let result = store.finalize(&upload).await;
            if retry_fails {
                assert_eq!(result.err().unwrap().status(), 409);
            } else {
                assert_eq!(result.unwrap().object_key, upload.object_key);
            }
            store.shutdown().await;
            server.await.unwrap();
        }
    })
    .await
    .expect("completion recovery exceeded its page or retry bound");
}

#[tokio::test]
async fn large_part_recovery_bounds_each_status_rpc_by_bytes_and_rejects_zero_part_geometry() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let store = client(&listener, 1);
        let mut upload = upload(3);
        upload.part_bytes = 48 * 1024 * 1024;
        upload.total_bytes = upload.parts * upload.part_bytes;
        let expected = upload.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            login(&mut stream).await;
            // Direct status and cold-finalize recovery must share the bound.
            let request = read(&mut stream).await;
            assert!(matches!(request.command, Command::UploadStatus { start: 0, limit: 1, .. }));
            write(&mut stream, page(request.id, 3, 0, 1)).await;
            let request = read(&mut stream).await;
            assert!(matches!(request.command, Command::UploadFinalize { .. }));
            write(&mut stream, Response::error(request.id, 409, RECONCILE)).await;
            for start in 0..3 {
                let request = read(&mut stream).await;
                assert!(matches!(request.command, Command::UploadStatus { start: actual, limit: 1, .. } if actual==start));
                write(&mut stream, page(request.id, 3, start, start + 1)).await;
            }
            let request = read(&mut stream).await;
            assert!(matches!(request.command, Command::UploadFinalize { .. }));
            write(&mut stream, finalized(request.id, &expected)).await;
            closed(&mut stream).await;
        });
        assert_eq!(store.status(&upload, 0, 4096).await.unwrap().checked_end, 1);
        assert_eq!(store.finalize(&upload).await.unwrap().object_key, upload.object_key);
        let mut invalid = upload.clone();
        invalid.part_bytes = 0;
        assert_eq!(store.status(&invalid, 0, 1).await.err().unwrap().status(), 400);
        store.shutdown().await;
        server.await.unwrap();
    })
    .await
    .expect("large-part recovery failed to bound a status page");
}

#[tokio::test]
async fn missing_unavailable_malformed_and_purged_uploads_are_never_published() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for case in 0..6 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let store = client(&listener, 1);
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                login(&mut stream).await;
                let request = read(&mut stream).await;
                write(&mut stream, Response::error(request.id, 409, RECONCILE)).await;
                let request = read(&mut stream).await;
                assert!(matches!(request.command, Command::UploadStatus { .. }));
                let mut response = page(request.id, 2, 0, 2);
                match case {
                    0 => response.result.as_mut().unwrap()["missing_parts"] = json!([1]),
                    1 => response.result.as_mut().unwrap()["unavailable_members"] = json!([1]),
                    2 => response.result.as_mut().unwrap()["checked_end"] = json!(0),
                    3 => response.result.as_mut().unwrap()["parts"] = json!(3),
                    4 => response.result.as_mut().unwrap()["missing_parts"] = json!([2]),
                    _ => response = Response::error(request.id, 404, "absent"),
                }
                write(&mut stream, response).await;
                closed(&mut stream).await;
            });
            let error = store.finalize(&upload(2)).await.err().unwrap();
            assert_eq!(
                error.status(),
                match case {
                    0 | 1 => 409,
                    2..=4 => 502,
                    _ => 404,
                }
            );
            store.shutdown().await;
            server.await.unwrap();
        }
    })
    .await
    .expect("failed completion recovery attempted publication or replay");
}

#[tokio::test]
async fn shutdown_drains_a_finalize_across_reconciliation_and_its_single_retry() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let store = client(&listener, 1);
        let upload = upload(2);
        let expected = upload.clone();
        let (arrived, reconciling) = oneshot::channel();
        let (reply, allowed) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            login(&mut stream).await;
            let request = read(&mut stream).await;
            write(&mut stream, Response::error(request.id, 409, RECONCILE)).await;
            let request = read(&mut stream).await;
            assert!(matches!(request.command, Command::UploadStatus { .. }));
            arrived.send(()).unwrap();
            allowed.await.unwrap();
            write(&mut stream, page(request.id, 2, 0, 2)).await;
            let request = read(&mut stream).await;
            assert!(matches!(request.command, Command::UploadFinalize { .. }));
            write(&mut stream, finalized(request.id, &expected)).await;
            closed(&mut stream).await;
        });
        let active = store.clone();
        let finalize = tokio::spawn(async move { active.finalize(&upload).await });
        reconciling.await.unwrap();
        let closing = store.clone();
        let shutdown = tokio::spawn(async move { closing.shutdown().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!shutdown.is_finished());
        reply.send(()).unwrap();
        assert!(finalize.await.unwrap().is_ok());
        shutdown.await.unwrap();
        server.await.unwrap();
        assert_eq!(store.stat("N1A1.closed").await.unwrap_err().status(), 503);
    })
    .await
    .expect("shutdown stranded or abandoned a reconciling finalize");
}
