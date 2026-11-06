use super::*;
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

fn publish(path: &Path, bytes: Option<&[u8]>) -> Result<()> {
    if path.symlink_metadata().is_ok() {
        return Ok(());
    }
    let temporary = path.with_file_name(format!(".n1-default-{}", ulid::Ulid::new()));
    let result = (|| {
        if let Some(bytes) = bytes {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
        } else {
            cli_core::settings::create_default_config_ini(&temporary)?;
            File::open(&temporary)?.sync_all()?;
        }
        match fs::hard_link(&temporary, path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        File::open(path.parent().expect("absolute configuration parent"))?.sync_all()?;
        Ok(())
    })();
    let _ = fs::remove_file(temporary);
    result
}
pub(super) fn ensure(path: &Path) -> Result<()> {
    let path: PathBuf = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let parent = path
        .parent()
        .ok_or_else(|| Error::Invalid("configuration has no parent".into()))?;
    fs::create_dir_all(parent)?;
    let new_config = !path.try_exists()?;
    publish(&path, None)?;
    // Existing operator configurations may use service credentials or an
    // explicit secrets file. Starting those must not write a sibling env file.
    if new_config {
        publish(
            &path.with_file_name("n1.env"),
            Some(
                concat!(
                    "# N1 service secrets. Configure before starting the server.\n",
                    "JWT_SECRET=\n",
                    "AUTH_KEY=\n",
                    "MASTER_KEY=\n",
                    "ADMIN_KEY=\n",
                    "ACCESS_TOKEN_DEFAULT_TTL_SECS=900\n",
                    "ACCESS_TOKEN_MAX_TTL_SECS=3600\n",
                    "REFRESH_TOKEN_TTL_SECS=604800\n",
                )
                .as_bytes(),
            ),
        )?;
    }
    Ok(())
}
