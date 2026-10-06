use super::{
    RepairCheckMode, ensure_no_unresolved_repairs, list_all_fragments, parse_check_mode,
    reachable_dir_roots,
};
use crate::core::constants::FRAGMENT_METADATA_FILE;
use crate::core::fragment_identity::{
    fragment_alias_dir, physical_fragment_dir_name, physical_fragment_root_on,
};
use crate::core::operation_gate::MaintenanceAccessMode;
use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::symlink;
use ulid::Ulid;

#[tokio::test]
async fn fragment_deletion_waits_for_healing_and_stale_scan_cannot_resurrect_it() {
    use super::object_repair::{fragment_is_present, repair_fragment_metadata_replicas};
    use crate::core::fragment_lifecycle;
    use std::time::Duration;

    let fragment = Ulid::new().to_string();
    let base = std::env::temp_dir().join(format!("n1-heal-delete-{fragment}"));
    let roots: Vec<_> = (0..4)
        .map(|slot| base.join(format!("root-{slot}")))
        .collect();
    let bytes = rmp_serde::to_vec_named(&serde_json::json!({"fragment": fragment})).unwrap();
    for root in &roots {
        fs::create_dir_all(physical_fragment_root_on(root, &fragment)).unwrap();
    }
    fs::write(
        physical_fragment_root_on(&roots[0], &fragment).join(FRAGMENT_METADATA_FILE),
        &bytes,
    )
    .unwrap();

    let repair_guard = fragment_lifecycle::acquire(&fragment).await.unwrap();
    let delete_fragment = fragment.clone();
    let delete_roots = roots.clone();
    let (waiting_tx, waiting_rx) = tokio::sync::oneshot::channel();
    let (deleted_tx, mut deleted_rx) = tokio::sync::oneshot::channel();
    let delete = tokio::task::spawn_blocking(move || {
        waiting_tx.send(()).unwrap();
        let _guard = fragment_lifecycle::acquire_blocking(&delete_fragment).unwrap();
        for root in delete_roots {
            fs::remove_dir_all(physical_fragment_root_on(&root, &delete_fragment)).unwrap();
        }
        deleted_tx.send(()).unwrap();
    });
    waiting_rx.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut deleted_rx)
            .await
            .is_err()
    );
    assert!(fragment_is_present(&roots, &fragment).await.unwrap());
    assert_eq!(
        repair_fragment_metadata_replicas(&roots, &fragment, false)
            .await
            .unwrap(),
        (3, 0, 0),
        "genuine missing replicas must still be repaired"
    );
    drop(repair_guard);
    delete.await.unwrap();
    deleted_rx.await.unwrap();

    let _stale_scan_guard = fragment_lifecycle::acquire(&fragment).await.unwrap();
    assert!(!fragment_is_present(&roots, &fragment).await.unwrap());
    assert!(
        roots
            .iter()
            .all(|root| !physical_fragment_root_on(root, &fragment).exists())
    );
    fs::remove_dir_all(base).unwrap();
}

#[tokio::test]
async fn fragment_lifecycle_does_not_hide_real_io_failures() {
    use super::object_repair::fragment_is_present;
    let fragment = Ulid::new().to_string();
    let base = std::env::temp_dir().join(format!("n1-lifecycle-io-{fragment}"));
    fs::create_dir_all(&base).unwrap();
    let blocked_root = base.join("not-a-directory");
    fs::write(&blocked_root, b"blocker").unwrap();
    assert!(
        fragment_is_present(&[blocked_root], &fragment)
            .await
            .is_err()
    );
    fs::remove_dir_all(base).unwrap();
}

#[test]
fn parse_check_mode_is_case_insensitive_and_accepts_aliases() {
    let mut params = HashMap::new();
    params.insert("mode".to_string(), "ScRuB".to_string());
    assert_eq!(parse_check_mode(&params), RepairCheckMode::Scrub);

    params.insert("mode".to_string(), "READ_ONLY".to_string());
    assert_eq!(parse_check_mode(&params), RepairCheckMode::Strict);

    params.insert("mode".to_string(), "checksum".to_string());
    assert_eq!(parse_check_mode(&params), RepairCheckMode::Checksum);
}

#[test]
fn unresolved_automatic_repair_debt_fails_the_scan() {
    assert!(ensure_no_unresolved_repairs(0, None).is_ok());
    let error = ensure_no_unresolved_repairs(2, Some("no distinct replacement"))
        .expect_err("unresolved repairs must fail closed");
    let message = error.to_string();
    assert!(message.contains("2 unresolved"));
    assert!(message.contains("no distinct replacement"));
}

#[test]
fn parse_check_mode_falls_back_to_access_mode_when_mode_absent() {
    let mut params = HashMap::new();
    params.insert("access_mode".to_string(), "quiesced".to_string());
    assert_eq!(parse_check_mode(&params), RepairCheckMode::Scrub);

    params.insert("access_mode".to_string(), "readonly".to_string());
    assert_eq!(parse_check_mode(&params), RepairCheckMode::Strict);
}

#[test]
fn checksum_gate_distinguishes_manual_default_from_autonomous_minimum() {
    assert_eq!(
        RepairCheckMode::Checksum.default_maintenance_access_mode(),
        MaintenanceAccessMode::ReadOnly
    );
    assert_eq!(
        RepairCheckMode::Checksum.minimum_maintenance_access_mode(),
        MaintenanceAccessMode::Availability
    );
    assert_eq!(
        RepairCheckMode::Strict.minimum_maintenance_access_mode(),
        MaintenanceAccessMode::ReadOnly
    );
    assert_eq!(
        RepairCheckMode::Scrub.minimum_maintenance_access_mode(),
        MaintenanceAccessMode::Quiesced
    );
}

#[tokio::test]
async fn list_all_fragments_ignores_filesystem_trash_directories() {
    let base = std::env::temp_dir().join(format!("n1-repair-fragments-{}", Ulid::new()));
    let index_root = base.join("index-root");
    fs::create_dir_all(&index_root).expect("create index root");

    let real_fragment = "real-fragment";
    let stable_fragment = "stable-fragment";
    let real_fragment_dir = physical_fragment_root_on(&index_root, stable_fragment);
    fs::create_dir_all(&real_fragment_dir).expect("create fragment dir");
    fs::write(real_fragment_dir.join(FRAGMENT_METADATA_FILE), b"fragment")
        .expect("write fragment metadata marker");
    fs::create_dir_all(fragment_alias_dir(&index_root)).expect("create alias dir");
    symlink(
        std::path::PathBuf::from("..").join(physical_fragment_dir_name(stable_fragment)),
        fragment_alias_dir(&index_root).join(real_fragment),
    )
    .expect("create fragment alias");

    fs::create_dir_all(index_root.join(".Trash-1000")).expect("create desktop trash dir");
    fs::create_dir_all(index_root.join("lost+found")).expect("create unrelated dir");

    let fragments = list_all_fragments(&[index_root]).await;
    assert_eq!(fragments, vec![stable_fragment.to_string()]);

    let _ = fs::remove_dir_all(base);
}

#[tokio::test]
async fn reachable_dir_roots_ignores_blocker_files() {
    let base = std::env::temp_dir().join(format!("n1-repair-roots-{}", Ulid::new()));
    let live_root = base.join("live-root");
    let blocked_root = base.join("blocked-root");

    fs::create_dir_all(&live_root).expect("create live root");
    fs::create_dir_all(&base).expect("create base");
    fs::write(&blocked_root, b"down").expect("create blocker file");

    let live = reachable_dir_roots(vec![live_root.clone(), blocked_root.clone()]).await;
    assert_eq!(live, vec![live_root]);

    let _ = fs::remove_dir_all(base);
}
