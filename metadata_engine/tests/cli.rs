use std::process::Command;

#[test]
fn init_check_reject_reinitialization_and_never_create_during_check() {
    let parent = tempfile::tempdir().unwrap();
    let directory = parent.path().join("metadata");
    let run = |command: &str| {
        Command::new(env!("CARGO_BIN_EXE_litiaina-n1-metadata"))
            .args([command, "--data-dir"])
            .arg(&directory)
            .output()
            .unwrap()
    };
    assert!(!run("check").status.success());
    assert!(!directory.exists());
    let result = run("init");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let status: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(status["component"], "metadata_engine");
    assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));
    assert!(!run("init").status.success());
    assert!(run("check").status.success());
}
