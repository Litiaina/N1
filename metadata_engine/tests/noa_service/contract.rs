//! Public contract checks against the packaged N1 and M1 service binaries.
use super::*;
use reqwest::RequestBuilder;

async fn checked(request: RequestBuilder, status: u16) -> Value {
    json_response(request.send().await.unwrap(), status).await
}

async fn control(request: RequestBuilder, command: Value, expected: u16) -> Value {
    let response = checked(
        request.json(&json!({"id":77,"command":command,"body_len":0})),
        expected,
    )
    .await;
    assert_eq!(response["id"], 77);
    assert_eq!(response["status"], expected);
    assert_eq!(response["body_len"], 0);
    if expected == 200 {
        assert!(response["error"].is_null());
    } else {
        assert!(response["error"].is_string());
        assert!(response["result"].is_null());
    }
    response["result"].clone()
}

#[tokio::test]
#[ignore = "requires the packaged N1 executable and private physical member roots"]
async fn link_http_and_native_share_permissions_ranges_and_immutable_retries() {
    let mut stack = Stack::new().await;
    drop(stack.metadata.take());
    drop(stack.object.take());
    let port = port();
    let mut settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    let stripe = native_part_bytes(&settings);
    settings["http_address"] = json!(format!("127.0.0.1:{port}"));
    std::fs::write(&stack.object_config, serde_json::to_vec(&settings).unwrap()).unwrap();
    stack.start_object().await;

    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}");
    let health = checked(client.get(format!("{base}/health")), 200).await;
    assert_eq!(health["version"], "1.0.0-RC.2");
    let authenticated = |token: &str| client.post(format!("{base}/noa")).bearer_auth(token);
    let credentials: Value = serde_json::from_slice(
        &std::fs::read(stack.object_config.with_extension("credentials.json")).unwrap(),
    )
    .unwrap();
    let login = |permissions: Value| {
        json!({"op":"auth.fragment","args":{
            "fragment":credentials["fragment"],"secret":credentials["secret"],"permissions":permissions,
            "ttl_seconds":3600
        }})
    };
    let writer = control(client.post(format!("{base}/noa")), login(Value::Null), 200).await;
    let write_token = writer["token"].as_str().unwrap();
    let reader = control(
        client.post(format!("{base}/noa")),
        login(json!({"read":true,"write":false,"delete":false,"repair":false})),
        200,
    )
    .await;
    let read_token = reader["token"].as_str().unwrap();
    let payload: Vec<_> = (0..stripe + 17).map(|i| (i % 251) as u8).collect();
    let plan = control(authenticated(write_token), json!({"op":"upload.init","args":{
        "pool":settings["pools"][0]["layout"]["id"],"total_bytes":payload.len(),"part_bytes":stripe,
        "durable":true,"ttl_seconds":3600
    }}), 200).await;
    let key = plan["object_key"].as_str().unwrap();
    assert_eq!(plan["part_bytes"], stripe);
    assert_eq!(plan["parts"], 2);

    let first = format!("{base}/noa/upload/{key}/bytes/0");
    checked(
        client
            .put(&first)
            .bearer_auth(read_token)
            .body(payload[..stripe].to_vec()),
        403,
    )
    .await;
    let part = checked(
        client
            .put(&first)
            .bearer_auth(write_token)
            .body(payload[..stripe].to_vec()),
        200,
    )
    .await;
    assert_eq!(part, json!({"offset":0,"complete":true}));
    let mut native = maintenance::direct(&stack).await;
    let status = native
        .control(n1_link::noa::Command::UploadStatus {
            object_key: key.into(),
            start: 0,
            limit: 2,
        })
        .await
        .unwrap()
        .result
        .unwrap();
    assert_eq!(status["missing_parts"], json!([1]));
    checked(
        client
            .put(&first)
            .bearer_auth(write_token)
            .body(vec![9; stripe]),
        409,
    )
    .await;
    let last = format!("{base}/noa/upload/{key}/bytes/{stripe}");
    checked(
        client
            .put(last)
            .bearer_auth(write_token)
            .body(payload[stripe..].to_vec()),
        200,
    )
    .await;
    for repeated in [false, true] {
        let result = control(
            authenticated(write_token),
            json!({"op":"upload.finalize","args":{"object_key":key}}),
            200,
        )
        .await;
        assert_eq!(result["object_key"], key);
        assert_eq!(result["already_finalized"], repeated);
    }
    let url = format!("{base}/noa/object/{key}");
    let head = client
        .head(&url)
        .bearer_auth(read_token)
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 200);
    assert_eq!(head.headers()["content-length"], payload.len().to_string());
    assert_eq!(head.headers()["accept-ranges"], "bytes");
    assert_eq!(head.headers()["cache-control"], "private, no-store");
    let etag = head.headers()["etag"].clone();
    assert!(head.bytes().await.unwrap().is_empty());
    let cached = client
        .get(&url)
        .bearer_auth(read_token)
        .header("if-none-match", etag)
        .send()
        .await
        .unwrap();
    assert_eq!(cached.status(), 304);
    let invalid = client
        .get(&url)
        .bearer_auth(read_token)
        .header("if-match", "\"different\"")
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), 412);
    let range = client
        .get(&url)
        .bearer_auth(read_token)
        .header("range", "bytes=1-3")
        .send()
        .await
        .unwrap();
    assert_eq!(range.status(), 206);
    assert_eq!(
        range.headers()["content-range"],
        format!("bytes 1-3/{}", payload.len())
    );
    assert_eq!(range.bytes().await.unwrap().as_ref(), &payload[1..4]);

    let grant = control(
        authenticated(read_token),
        json!({"op":"object.signed_read",
        "args":{"object_key":key,"offset":1,"length":3,"ttl_seconds":60}}),
        200,
    )
    .await;
    let signed_url = format!("{base}/noa/signed");
    let response = client.get(&signed_url).query(&grant).send().await.unwrap();
    assert_eq!(response.status(), 206);
    assert_eq!(response.bytes().await.unwrap().as_ref(), &payload[1..4]);
    let command = json!({"op":"object.delete","args":{"object_key":key}});
    control(authenticated(read_token), command.clone(), 403).await;
    for removed in [settings["pools"][0]["roots"].as_array().unwrap().len(), 0] {
        let purged = control(authenticated(write_token), command.clone(), 200).await;
        assert_eq!(purged["removed_shards"], removed);
    }
    let read = client
        .get(&url)
        .bearer_auth(read_token)
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), 404);
    let status = native
        .control(n1_link::noa::Command::UploadStatus {
            object_key: key.into(),
            start: 0,
            limit: 2,
        })
        .await;
    assert!(
        matches!(status, Err(n1_native_client::NativeClientError::Native {code,..}) if code=="404")
    );
}

#[tokio::test]
#[ignore = "requires private real N1/M1 binaries and storage roots"]
async fn named_namespace_permissions_and_response_fields_match_noa_v1() {
    let stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (fragment, token, operator) = maintenance::auth(&client, &base, "Contract primary").await;
    let (other, other_token, _) = maintenance::auth(&client, &base, "Contract secondary").await;
    let login = checked(
        client.post(format!("{base}/auth/fragment")).json(&json!({
            "fragment":fragment,"secret":"test-only-fragment","read":true
        })),
        200,
    )
    .await;
    let reader = login["access_token"].as_str().unwrap();

    assert!(checked(client.get(format!("{base}/fragments")), 401).await["error"].is_string());
    let listing = checked(
        client.get(format!("{base}/fragments")).bearer_auth(reader),
        200,
    )
    .await;
    assert_eq!(listing, json!([fragment]));
    let preview = checked(
        client
            .get(format!("{base}/fragments/hash"))
            .query(&[("name", "Contract primary")]),
        200,
    )
    .await;
    assert_eq!(preview["fragment_hash"], fragment);
    let exists = checked(
        client
            .get(format!("{base}/fragments/check"))
            .query(&[("fragment", &fragment)]),
        200,
    )
    .await;
    assert_eq!(exists, json!({"exists":true}));

    for (identity, body) in [
        (&fragment, json!({"new_name":"unauthorized rename"})),
        (&other, json!({"new_key":"unauthorized replacement"})),
    ] {
        assert!(
            checked(
                client
                    .patch(format!("{base}/fragments/{identity}"))
                    .bearer_auth(reader)
                    .json(&body),
                403,
            )
            .await["error"]
                .is_string()
        );
    }
    let backups = format!("{base}/maintenance/metadata/backups");
    assert!(checked(client.get(&backups).bearer_auth(&token), 403).await["error"].is_string());
    checked(client.get(&backups).bearer_auth(&operator), 200).await;

    let bytes = b"contract witness";
    for path in ["docs/a.txt", "docs/b.txt", "docs/nested/c.txt"] {
        maintenance::put(&client, &base, &token, path, bytes).await;
    }
    for (path, kind, is_virtual) in [("docs", "directory", true), ("docs/a.txt", "file", false)] {
        let info = checked(
            client
                .get(format!("{base}/posix/stat"))
                .bearer_auth(reader)
                .query(&[("path", path)]),
            200,
        )
        .await;
        assert_eq!(info["path"], path);
        assert_eq!(info["kind"], kind);
        assert_eq!(info["is_virtual"], is_virtual);
        assert_eq!(info["scan_trash"], false);
        if !is_virtual {
            assert_eq!(info["size_bytes"], bytes.len());
            assert!(info["version_id"].is_string());
            assert!(info["modified_at_unix"].is_i64());
        }
    }
    let mkdir = checked(
        client
            .post(format!("{base}/posix/mkdir"))
            .bearer_auth(&token)
            .json(&json!({"path":"empty"})),
        400,
    )
    .await;
    assert!(
        mkdir["error"]
            .as_str()
            .unwrap()
            .contains("virtual prefixes")
    );

    let list = format!("{base}/posix/list");
    let mut after = String::new();
    let mut rows = Vec::new();
    for _ in 0..4 {
        let mut request = client
            .get(&list)
            .bearer_auth(reader)
            .query(&[("path", "docs"), ("limit", "1")]);
        if !after.is_empty() {
            request = request.query(&[("continuation_token", after.as_str())]);
        }
        let page = checked(request, 200).await;
        assert_eq!(page["path"], "docs");
        let entries = page["entries"].as_array().unwrap();
        assert!(entries.len() <= 1);
        rows.extend(
            entries
                .iter()
                .map(|entry| entry["path"].as_str().unwrap().to_owned()),
        );
        let Some(next) = page["continuation_token"].as_str() else {
            break;
        };
        assert_ne!(next, after);
        after = next.to_owned();
    }
    assert_eq!(rows, ["docs/a.txt", "docs/b.txt", "docs/nested"]);
    assert!(!after.is_empty());
    checked(
        client.get(&list).bearer_auth(&other_token).query(&[
            ("path", "docs"),
            ("limit", "1"),
            ("continuation_token", after.as_str()),
        ]),
        400,
    )
    .await;
    checked(
        client.get(&list).bearer_auth(reader).query(&[
            ("path", "elsewhere"),
            ("continuation_token", after.as_str()),
        ]),
        400,
    )
    .await;

    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    assert!(
        native(&mut socket, "auth.bearer", json!({"token":reader}), None)
            .await
            .0
            .ok
    );
    let (native_stat, data) = native(
        &mut socket,
        "posix.stat",
        json!({"path":"docs/a.txt"}),
        None,
    )
    .await;
    assert_eq!(native_stat.status, Some(200));
    let metadata: Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(metadata["kind"], "file");
    assert_eq!(metadata["size_bytes"], bytes.len());

    let deleted = checked(
        client
            .post(format!("{base}/objects/bulk-delete"))
            .bearer_auth(&token)
            .json(&json!({"object_keys":["docs/b.txt","absent"]})),
        200,
    )
    .await;
    assert_eq!(deleted["affected"], 1);
    assert_eq!(deleted["failed"][0]["object_key"], "absent");
    assert_eq!(deleted["failed"][0]["status"], 404);
    let trash = checked(client.get(format!("{base}/trash")).bearer_auth(reader), 200).await;
    assert_eq!(trash["entries"][0]["path"], "docs/b.txt");
    assert!(trash["continuation_token"].is_null());
    let second_delete = checked(
        client
            .delete(format!("{base}/objects/docs/a.txt"))
            .bearer_auth(&token),
        200,
    )
    .await;
    assert_eq!(second_delete["affected"], 1);
    for endpoint in ["trash?limit=1", "posix/list?trash=true&limit=1"] {
        let url = format!("{base}/{endpoint}");
        let first = checked(client.get(&url).bearer_auth(reader), 200).await;
        assert_eq!(first["entries"].as_array().unwrap().len(), 1);
        let cursor = first["continuation_token"].as_str().unwrap();
        let last = checked(
            client
                .get(&url)
                .bearer_auth(reader)
                .query(&[("continuation_token", cursor)]),
            200,
        )
        .await;
        assert_eq!(last["entries"].as_array().unwrap().len(), 1);
        assert_ne!(first["entries"][0]["path"], last["entries"][0]["path"]);
        assert!(last["continuation_token"].is_null());
    }
    let purge = checked(
        client
            .post(format!("{base}/posix/purge"))
            .bearer_auth(&token)
            .json(&json!({"path":"docs/b.txt"})),
        410,
    )
    .await;
    assert!(purge["error"].is_string());
    let recovered = checked(
        client
            .post(format!("{base}/trash/recover-all"))
            .bearer_auth(&token),
        200,
    )
    .await;
    assert_eq!(recovered["affected"], 2);
    for path in ["docs/a.txt", "docs/b.txt", "docs/nested/c.txt"] {
        let read = client
            .get(format!("{base}/objects/get/{path}"))
            .bearer_auth(reader)
            .send()
            .await
            .unwrap();
        assert_eq!(read.status(), 200);
        assert_eq!(read.bytes().await.unwrap().as_ref(), bytes);
    }

    let snapshot = client
        .get(format!("{base}/namespace/snapshot"))
        .bearer_auth(reader)
        .send()
        .await
        .unwrap();
    assert_eq!(snapshot.status(), 200);
    let snapshot = snapshot.text().await.unwrap();
    let mut lines = snapshot
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        lines.pop().unwrap(),
        json!({"kind":"complete","entry_count":3})
    );
    assert_eq!(lines.len(), 3);
    assert!(
        lines
            .iter()
            .all(|entry| entry["kind"] == "entry" && entry["version_id"].is_string())
    );
    let refused = client
        .get(format!("{base}/objects/get/docs/a.txt"))
        .bearer_auth(reader)
        .header("range", "bytes=9999-")
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 416);
}

#[tokio::test]
#[ignore = "requires private real N1/M1 binaries and storage roots"]
async fn fragment_alias_changes_and_key_rotation_preserve_identity_and_enforce_revocation() {
    let stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let create = client.post(format!("{base}/fragments"));
    let fragment = checked(
        create.json(&json!({
            "name":"Contract lifecycle","secret":"test-only-master","key":"test-only-original-key"
        })),
        201,
    )
    .await;
    let id = fragment["fragment_id"].as_str().unwrap();
    let original = fragment["fragment_hash"].as_str().unwrap();
    let url = format!("{base}/fragments/{id}");
    let login = |identity: &str, secret: &str| {
        client.post(format!("{base}/auth/fragment"))
        .json(&json!({"fragment":identity,"secret":secret,"read":true,"write":true,"modify":true,"delete":true}))
    };
    let first = checked(login(original, "test-only-original-key"), 200).await;
    let token = first["access_token"].as_str().unwrap();
    let version = maintenance::put(&client, &base, token, "retained.txt", b"retained").await;
    let renamed = checked(
        client.patch(&url).bearer_auth(token).json(&json!({
            "new_name":"Renamed contract lifecycle","secret":"test-only-master"
        })),
        200,
    )
    .await;
    let alias = renamed["fragment_hash"].as_str().unwrap();
    assert_ne!(original, alias);
    let fragments = format!("{base}/fragments");
    assert_eq!(
        checked(client.get(&fragments).bearer_auth(token), 200).await,
        json!([alias])
    );
    let old_alias = checked(
        client
            .get(format!("{fragments}/check"))
            .query(&[("fragment", original)]),
        200,
    )
    .await;
    assert_eq!(old_alias, json!({"exists":false}));

    checked(
        client
            .patch(&url)
            .bearer_auth(token)
            .json(&json!({"new_key":"test-only-rotated-key"})),
        200,
    )
    .await;
    checked(login(alias, "test-only-original-key"), 401).await;
    checked(client.get(&fragments).bearer_auth(token), 401).await;
    checked(
        client
            .post(format!("{base}/auth/refresh"))
            .json(&json!({"refresh_token":first["refresh_token"]})),
        401,
    )
    .await;
    let updated = checked(login(id, "test-only-rotated-key"), 200).await;
    let token = updated["access_token"].as_str().unwrap();
    let request = client
        .get(format!("{base}/objects/meta/retained.txt"))
        .bearer_auth(token);
    assert_eq!(checked(request, 200).await["version_id"], version);

    checked(
        client.delete(&url).bearer_auth(token).json(&json!({
            "secret":"test-only-master","confirm_fragment":"incorrect-confirmation"
        })),
        400,
    )
    .await;
    let deleted = checked(
        client.delete(&url).bearer_auth(token).json(&json!({
            "secret":"test-only-master","confirm_fragment":id
        })),
        200,
    )
    .await;
    assert_eq!(deleted, json!({"deleted":true}));
    checked(client.get(&fragments).bearer_auth(token), 401).await;
}
