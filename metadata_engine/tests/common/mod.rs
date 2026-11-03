use std::path::Path;

/// Database fixtures must honor the same credential directory permissions as
/// deployment; tempfile's default directory mode follows the process umask.
pub fn directory(parent: Option<&Path>) -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(std::fs::Permissions::from_mode(0o700));
    }
    match parent {
        Some(path) => builder.tempdir_in(path),
        None => builder.tempdir(),
    }
    .unwrap()
}
