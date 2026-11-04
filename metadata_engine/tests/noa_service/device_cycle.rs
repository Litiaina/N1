//! Opt-in OS filesystem detach/reconnect. The block device must match the
//! explicit second-member filesystem; this never guesses a disk or formats it.
use super::reliability::{fragment, record};
use super::*;
use std::os::unix::fs::{FileTypeExt, MetadataExt};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "private two-HDD negative read contract: remove all backend payload shards"]
async fn metadata_cannot_report_a_complete_success_when_every_backing_shard_is_lost() {
    let mut stack = Stack::new().await;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = fragment(&client, &base, "Unavailable backend read contract").await;
    let mut objects = Vec::new();
    for (path, bytes) in [("empty", Vec::new()), ("nonempty", vec![73; 4097])] {
        let ack = json_response(
            client
                .post(format!("{base}/upload/one-shot/{path}?durability=strict"))
                .bearer_auth(&token)
                .body(bytes.clone())
                .send()
                .await
                .unwrap(),
            201,
        )
        .await;
        objects.push((
            path.to_owned(),
            bytes,
            ack["version_id"].as_str().unwrap().to_owned(),
        ));
    }
    exact_reads(&client, &base, &token, &objects).await;
    let positive_download = client
        .get(format!("{base}/objects/download?object_key=empty"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(positive_download.status(), 200);
    assert!(positive_download.bytes().await.unwrap().is_empty());
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    let (auth, _) = native(&mut socket, "auth.bearer", json!({"token":token}), None).await;
    assert!(auth.ok);
    let (positive_native, bytes) = native(
        &mut socket,
        "object.get",
        json!({"object_key":"empty"}),
        None,
    )
    .await;
    assert!(positive_native.ok && bytes.is_empty());
    // A second response proves the previous zero-length native exchange has
    // finished server-side, before the deliberate backend process loss.
    let (ping, _) = native(&mut socket, "ping", json!({}), None).await;
    assert!(ping.ok);
    drop(socket);
    drop(stack.object.take());
    let settings: Value =
        serde_json::from_slice(&std::fs::read(&stack.object_config).unwrap()).unwrap();
    fn remove_shards(path: &Path) -> usize {
        let mut count = 0;
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                count += remove_shards(&entry.path());
            } else if entry.file_name().to_str().unwrap().ends_with(".shard") {
                std::fs::remove_file(entry.path()).unwrap();
                count += 1;
            }
        }
        count
    }
    let mut removed = 0;
    for root in settings["pools"][0]["roots"].as_array().unwrap() {
        removed += remove_shards(Path::new(root.as_str().unwrap()));
    }
    assert_eq!(removed, 4);
    stack.start_object().await;
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", stack.native))
        .await
        .unwrap();
    let (auth, _) = native(&mut socket, "auth.bearer", json!({"token":token}), None).await;
    assert!(auth.ok);
    let mut observations = Vec::new();
    let mut false_successes = Vec::new();
    let (native_result, native_bytes) = native(
        &mut socket,
        "object.get",
        json!({"object_key":"empty"}),
        None,
    )
    .await;
    observations.push(json!({"transport":"n1://","object":"empty","ok":native_result.ok,"status":native_result.status,"bytes":native_bytes.len()}));
    if native_result.ok {
        false_successes.push("native empty GET".to_owned());
    }
    for path in [
        "objects/get/empty",
        "objects/download?object_key=empty",
        "objects/get/nonempty",
    ] {
        match client
            .get(format!("{base}/{path}"))
            .bearer_auth(&token)
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                let body = response.bytes().await;
                observations.push(json!({"transport":"HTTP","route":path,"status":status.as_u16(),"complete_body":body.is_ok()}));
                if status.is_success() && body.is_ok() {
                    false_successes.push(path.to_owned());
                }
            }
            Err(error) => observations
                .push(json!({"transport":"HTTP","route":path,"transport_error":error.to_string()})),
        }
    }
    record(
        &stack.root.join("unavailable-backend-observations.json"),
        &observations,
    );
    assert!(
        false_successes.is_empty(),
        "complete successful reads after every backing shard was lost: {false_successes:?}"
    );
    record(
        &stack.root.join("UNAVAILABLE-BACKEND-READ-PASSED.json"),
        &json!({
            "passed":true,"empty_and_nonempty_objects":2,"all_backing_shards_removed":removed,
            "successful_complete_reads":0
        }),
    );
}

struct MountGuard {
    block: PathBuf,
    root: PathBuf,
    detached: bool,
}
impl Drop for MountGuard {
    fn drop(&mut self) {
        if self.detached
            && let Ok(output) = Command::new("udisksctl")
                .args(["mount", "--block-device"])
                .arg(&self.block)
                .arg("--no-user-interaction")
                .output()
        {
            let _ = std::fs::write(
                self.root.join("emergency-remount.json"),
                json!({
                    "success":output.status.success(),
                    "stdout":String::from_utf8_lossy(&output.stdout),
                    "stderr":String::from_utf8_lossy(&output.stderr)
                })
                .to_string(),
            );
        }
    }
}

async fn exact_reads(
    client: &Client,
    base: &str,
    token: &str,
    objects: &[(String, Vec<u8>, String)],
) {
    for (path, bytes, version) in objects {
        let response = client
            .get(format!("{base}/objects/get/{path}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()["etag"]
                .to_str()
                .unwrap()
                .trim_matches('"'),
            version
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "OS unmount/remount of explicitly identified disposable second HDD; run alone after all other HDD tests"]
async fn real_second_hdd_unmount_degraded_reads_and_remount_keep_every_named_ack() {
    let block = PathBuf::from(
        std::env::var_os("N1_DEVICE_TEST_BLOCK")
            .expect("explicit disposable block device required"),
    )
    .canonicalize()
    .unwrap();
    let mount = PathBuf::from(
        std::env::var_os("N1_DEVICE_TEST_MOUNT").expect("explicit disposable mount root required"),
    )
    .canonicalize()
    .unwrap();
    let second = PathBuf::from(std::env::var_os("N1_OBJECT_TEST_SECOND_ROOT").unwrap())
        .canonicalize()
        .unwrap();
    let block_metadata = block.metadata().unwrap();
    let filesystem = mount.metadata().unwrap().dev();
    assert!(block_metadata.file_type().is_block_device());
    assert_eq!(
        block_metadata.rdev(),
        filesystem,
        "block device is not the selected filesystem"
    );
    assert_eq!(second.metadata().unwrap().dev(), filesystem);
    assert!(
        second.starts_with(&mount) && second != mount,
        "fixture must be inside the explicit disposable mount"
    );
    assert_ne!(
        filesystem,
        Path::new("/").metadata().unwrap().dev(),
        "system filesystem cannot be detached"
    );
    let mut stack = Stack::new().await;
    assert_ne!(
        stack.root.metadata().unwrap().dev(),
        filesystem,
        "metadata/control/oracle must survive the device detach"
    );
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap();
    let base = format!("{}/noa/v1", stack.url);
    let (_, token) = fragment(&client, &base, "Real HDD reconnect").await;
    let mut objects = Vec::new();
    for index in 0..16 {
        let size = [0, 1, 4095, 4096, 4097, 65536, 65537, 1048593][index % 8];
        let bytes: Vec<_> = (0..size)
            .map(|offset| ((offset * 17 + index * 31) % 251) as u8)
            .collect();
        let path = format!("device/object-{index:02}");
        let ack = json_response(
            client
                .post(format!("{base}/upload/one-shot/{path}?durability=strict"))
                .bearer_auth(&token)
                .body(bytes.clone())
                .send()
                .await
                .unwrap(),
            201,
        )
        .await;
        objects.push((path, bytes, ack["version_id"].as_str().unwrap().to_owned()));
    }
    record(&stack.root.join("device-ack-oracle.json"), &objects.iter().map(|(path, bytes, version)| {
        use sha2::Digest;
        json!({"path":path,"version":version,"bytes":bytes.len(),"sha256":hex::encode(sha2::Sha256::digest(bytes))})
    }).collect::<Vec<_>>());
    exact_reads(&client, &base, &token, &objects).await;
    // Release the object process's pinned directories for a clean OS unmount.
    // Metadata remains alive on the independent first HDD throughout this test.
    let metadata_pid = stack.metadata.as_ref().unwrap().0.id();
    drop(stack.object.take());
    let output = Command::new("udisksctl")
        .args(["unmount", "--block-device"])
        .arg(&block)
        .arg("--no-user-interaction")
        .output()
        .unwrap();
    record(
        &stack.root.join("device-unmount.json"),
        &json!({
            "block":block,"mount":mount,"filesystem_device":filesystem,
            "success":output.status.success(),"stdout":String::from_utf8_lossy(&output.stdout),
            "stderr":String::from_utf8_lossy(&output.stderr)
        }),
    );
    assert!(
        output.status.success(),
        "OS denied unmount: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut guard = MountGuard {
        block,
        root: stack.root.clone(),
        detached: true,
    };
    assert!(
        !second.exists(),
        "second member filesystem is still reachable through its original mount"
    );
    stack.start_object().await;
    exact_reads(&client, &base, &token, &objects).await;
    assert_eq!(stack.metadata.as_ref().unwrap().0.id(), metadata_pid);
    assert!(
        stack
            .metadata
            .as_mut()
            .unwrap()
            .0
            .try_wait()
            .unwrap()
            .is_none()
    );
    assert!(
        !second.exists(),
        "degraded reads created a fallback directory on the system disk"
    );
    drop(stack.object.take());
    let output = Command::new("udisksctl")
        .args(["mount", "--block-device"])
        .arg(&guard.block)
        .arg("--no-user-interaction")
        .output()
        .unwrap();
    record(
        &stack.root.join("device-remount.json"),
        &json!({
            "success":output.status.success(),"stdout":String::from_utf8_lossy(&output.stdout),
            "stderr":String::from_utf8_lossy(&output.stderr)
        }),
    );
    assert!(
        output.status.success(),
        "OS denied remount: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    guard.detached = false;
    assert_eq!(second.metadata().unwrap().dev(), filesystem);
    stack.start_object().await;
    exact_reads(&client, &base, &token, &objects).await;
    assert_eq!(stack.metadata.as_ref().unwrap().0.id(), metadata_pid);
    record(
        &stack.root.join("DEVICE-CYCLE-PASSED.json"),
        &json!({
            "passed":true,"objects_and_original_versions_verified_per_phase":objects.len(),
            "phases":["both_disks","second_filesystem_unmounted","second_filesystem_remounted"],
            "metadata_process_unchanged":true,"os_unmount_remount":true,
            "physical_power_cut_or_cable_pull":false
        }),
    );
}
