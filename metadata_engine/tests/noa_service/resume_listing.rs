//! Public recovery discovery, restart, and completion against real object I/O.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit object binary and isolated storage roots"]
async fn interrupted_upload_listing_and_resume_survive_metadata_restart() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let created = json_response(client.post(format!("{base}/fragments"))
        .json(&json!({"name":"Resume witness","secret":"test-only-master","key":"test-only-fragment"})).send().await.unwrap(),201).await;
    let logged = json_response(client.post(format!("{base}/auth/fragment"))
        .json(&json!({"fragment":created["fragment_hash"],"secret":"test-only-fragment","read":true,"write":true,"delete":true})).send().await.unwrap(),200).await;
    let token = logged["access_token"].as_str().unwrap();
    for durable in [false, true] {
        let route = format!(
            "{base}/upload/multipart{}",
            if durable { "/durable" } else { "" }
        );
        // Completed and cancelled reservations sort before the interrupted file.
        json_response(
            client
                .post(format!("{base}/upload/one-shot/history-{durable}"))
                .bearer_auth(token)
                .body("completed history")
                .send()
                .await
                .unwrap(),
            201,
        )
        .await;
        let shape = json!({"total_size_bytes":8192,"part_size_bytes":4096,"expected_parts":2});
        let cancelled = json_response(
            client
                .post(format!("{route}/init/cancelled-{durable}"))
                .bearer_auth(token)
                .json(&shape)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        json_response(
            client
                .delete(format!(
                    "{route}/{}",
                    cancelled["version_id"].as_str().unwrap()
                ))
                .bearer_auth(token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        let mut sessions = Vec::new();
        for n in 0..2 {
            let saved = json_response(
                client
                    .post(format!("{route}/init/resume-{durable}-{n}"))
                    .bearer_auth(token)
                    .json(&shape)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            json_response(
                client
                    .put(format!(
                        "{route}/{}/0",
                        saved["version_id"].as_str().unwrap()
                    ))
                    .bearer_auth(token)
                    .body(vec![42; 4096])
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            sessions.push(saved);
        }
        drop(stack.metadata.take());
        stack.start_metadata().await;
        let mut next = None;
        for saved in &sessions {
            let mut request = client.get(format!("{route}?limit=1")).bearer_auth(token);
            if let Some(cursor) = next {
                request = request.query(&[("continuation_token", cursor)]);
            }
            let page = json_response(request.send().await.unwrap(), 200).await;
            assert_eq!(page["sessions"], json!([saved]));
            next = page["continuation_token"].as_str().map(str::to_owned);
        }
        assert!(next.is_none(), "last eligible session must end pagination");
        for saved in sessions {
            let version = saved["version_id"].as_str().unwrap();
            let session = saved["session_id"].as_str().unwrap();
            let status = json_response(
                client
                    .get(format!("{route}/{version}"))
                    .bearer_auth(token)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            assert_eq!(status["uploaded_parts"], json!([0]));
            json_response(
                client
                    .put(format!("{route}/{version}/1"))
                    .bearer_auth(token)
                    .body(vec![43; 4096])
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            json_response(
                client
                    .post(format!("{route}/{version}/{session}/finalize"))
                    .bearer_auth(token)
                    .json(&shape)
                    .send()
                    .await
                    .unwrap(),
                200,
            )
            .await;
            let read = client
                .get(format!(
                    "{base}/objects/get/{}",
                    saved["object_key"].as_str().unwrap()
                ))
                .bearer_auth(token)
                .send()
                .await
                .unwrap();
            assert_eq!(read.status(), 200);
            assert_eq!(
                read.bytes().await.unwrap().as_ref(),
                [vec![42; 4096], vec![43; 4096]].concat()
            );
        }
        let empty = json_response(
            client
                .get(format!("{route}?limit=1"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await;
        assert_eq!(empty["sessions"], json!([]));
        assert!(empty["continuation_token"].is_null());
    }
}
