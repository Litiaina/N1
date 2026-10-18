use std::path::{Path, PathBuf};
use std::process::Command;

struct EmptyWorkingDirectory(PathBuf);

impl EmptyWorkingDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("n1-release-version-{}", ulid::Ulid::new()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for EmptyWorkingDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn version(binary: &str, directory: &Path) -> String {
    let output = Command::new(binary)
        .arg("--version")
        .current_dir(directory)
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
    assert!(output.stderr.is_empty(), "{:?}", output);
    assert_eq!(std::fs::read_dir(directory).unwrap().count(), 0);
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn engine_reports_release_and_storage_contract_without_initializing_storage() {
    let directory = EmptyWorkingDirectory::new();
    assert_eq!(
        version(env!("CARGO_BIN_EXE_litiaina-n1"), &directory.0),
        format!(
            "litiaina-n1 {}\nstorage_format 1.0.0-RC.1\nstorage_encoding 4\nnoa v1\nnative_protocol n1/0.5\n",
            env!("CARGO_PKG_VERSION"),
        )
    );
}

#[test]
fn backup_helper_reports_the_matching_release_without_initializing_storage() {
    let directory = EmptyWorkingDirectory::new();
    assert_eq!(
        version(env!("CARGO_BIN_EXE_litiaina-n1-backup"), &directory.0),
        format!("litiaina-n1-backup {}\n", env!("CARGO_PKG_VERSION"))
    );
}
