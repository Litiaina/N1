use std::path::Path;

/// Keep recovery fixtures immutable; some recovery paths create lock files even
/// when they reject a format. The scratch directory is owned by this test only.
pub fn fixture(name: &str) -> std::io::Result<tempfile::TempDir> {
    fn copy(source: &Path, target: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(target)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                copy(&entry.path(), &target.join(entry.file_name()))?;
            } else {
                std::fs::copy(entry.path(), target.join(entry.file_name()))?;
            }
        }
        Ok(())
    }
    let directory = tempfile::tempdir()?;
    copy(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_fixture")
            .join(name),
        directory.path(),
    )?;
    Ok(directory)
}
