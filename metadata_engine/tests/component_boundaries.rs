//! Prevent architecture changes from introducing metadata into LINK's runtime.
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    process::Command,
};

#[test]
fn object_engine_is_independent_and_metadata_has_no_external_database_engine() {
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--offline",
            "--no-deps",
            "--format-version",
            "1",
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: Value = serde_json::from_slice(&output.stdout).unwrap();
    let packages = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|package| (package["name"].as_str().unwrap(), package))
        .collect::<HashMap<_, _>>();
    let dependencies = |root: &str| {
        let mut seen = HashSet::new();
        let mut pending = vec![root.to_owned()];
        while let Some(name) = pending.pop() {
            if !seen.insert(name.clone()) {
                continue;
            }
            if let Some(package) = packages.get(name.as_str()) {
                for dependency in package["dependencies"].as_array().unwrap() {
                    if dependency["kind"] != "dev" {
                        pending.push(dependency["name"].as_str().unwrap().to_owned());
                    }
                }
            }
        }
        seen
    };
    let object = dependencies("n1");
    assert!(!object.contains("m1"));
    assert!(!object.contains("procyon-engine"));
    assert!(!object.contains("m1_storage"));
    let metadata = dependencies("m1");
    assert!(metadata.contains("m1_storage"));
    for external in [
        "procyon-engine",
        "fjall",
        "lsm-tree",
        "surrealkv",
        "rusqlite",
    ] {
        assert!(
            !metadata.contains(external),
            "M1 must own its database implementation: {external}"
        );
    }
    assert!(metadata.contains("n1_native_client"));
    assert!(
        !metadata.contains("n1"),
        "metadata must use the public native client, not object-engine internals"
    );
}
