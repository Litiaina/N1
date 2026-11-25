use serde_json::Value;
use std::{path::Path, process::Command};

#[test]
fn system_release_selects_the_declared_component_versions_and_binaries() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--format-version=1",
            "--no-deps",
            "--offline",
            "--locked",
        ])
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: Value = serde_json::from_slice(&output.stdout).unwrap();
    let release: Value =
        serde_json::from_str(include_str!("../../deploy/system-release.json")).unwrap();
    assert_eq!(release["schema_version"], 1);
    assert_eq!(
        release["system"]["tag"],
        format!("v{}", release["system"]["version"].as_str().unwrap())
    );
    for name in ["n1", "m1"] {
        let component = &release["components"][name];
        let package = metadata["packages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|package| package["name"] == component["package"])
            .unwrap();
        assert_eq!(component["version"], package["version"], "{name}");
        assert!(component.get("tag").is_none(), "{name} uses the system tag");
        for binary in component["binaries"].as_array().unwrap() {
            assert!(
                package["targets"].as_array().unwrap().iter().any(|target| {
                    target["name"] == *binary
                        && target["kind"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|kind| kind == "bin")
                }),
                "{name} release names an unavailable binary: {binary}"
            );
        }
    }
}
