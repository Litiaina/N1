use super::{Result, invalid};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path};

pub fn safe_relative(value: &str) -> Result<&Path> {
    let path = Path::new(value);
    if value.is_empty()
        || value.len() > 4096
        || value.contains('\0')
        || value
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
        || path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(invalid("backup entry has an unsafe relative path"));
    }
    Ok(path)
}
pub fn safe_hash(value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(invalid("invalid backup block identifier"));
    }
    Ok(())
}
pub fn safe_id(value: &str) -> Result<()> {
    if value
        .parse::<ulid::Ulid>()
        .map(|id| id.to_string() != value)
        .unwrap_or(true)
    {
        return Err(invalid("invalid snapshot identifier"));
    }
    Ok(())
}
pub fn read_regular(path: &Path, max: usize) -> Result<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.len() > max as u64 {
        return Err(invalid("backup file type/size exceeds format limit"));
    }
    let mut data = Vec::new();
    (&mut file).take(max as u64 + 1).read_to_end(&mut data)?;
    if data.len() > max {
        return Err(invalid("backup file grew beyond limit"));
    }
    Ok(data)
}
pub fn sync_parent(path: &Path) -> Result<()> {
    File::open(path.parent().ok_or_else(|| invalid("path has no parent"))?)?.sync_all()?;
    Ok(())
}
pub fn private_dir(path: &Path) -> Result<()> {
    make_dir(path, true)
}
// Only for fenced, unpublished restore work. The restore commit phase must
// synchronize every final directory before removing any engine-start fence.
pub fn restore_dir(path: &Path) -> Result<()> {
    make_dir(path, false)
}
fn make_dir(path: &Path, durable: bool) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {
            if durable {
                sync_parent(path)?;
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if !fs::symlink_metadata(path)?.file_type().is_dir() {
                return Err(invalid("backup directory must not be a symlink"));
            }
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
pub fn atomic_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = path.with_file_name(format!(".pending-{}", ulid::Ulid::new()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        // Link is no-replace, unlike rename. Never overwrite an accepted snapshot.
        fs::hard_link(&temp, path)?;
        sync_parent(path)?;
        Ok(())
    })();
    let _ = fs::remove_file(temp);
    result
}
