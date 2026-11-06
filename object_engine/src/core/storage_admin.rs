//! Explicit offline member maintenance. Keep byte-exact copy, resumable fences,
//! inventory verification and process exclusion independent of object addressing.
//! LINK-specific verification reads the inline shard format rather than NDX/NCX.
use super::{
    backup::{
        Result, invalid,
        restore::{Target, validate_member_file},
    },
    instance_lease::{InstanceLease, LEASE_FILE, RESTORE_PENDING},
    storage_pool::{Member, Pool},
};
use crate::settings::config::Config;
use clap::{Args, Parser, Subcommand};
use n1_link::PoolId;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Component, Path, PathBuf},
};
use walkdir::{DirEntry, WalkDir};
mod replacement;

const WORK: &str = ".n1-pool-work";
const MAX_METADATA: u64 = 65536;

#[derive(Parser)]
#[command(about = "Offline LINK member verification and byte-exact cloning")]
struct Cli {
    #[command(subcommand)]
    command: Operation,
}
#[derive(Args)]
struct Selection {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    pool: PoolId,
    #[arg(long)]
    member: usize,
}
#[derive(Subcommand)]
enum Operation {
    /// Validate registered member identities and every inline shard file.
    Verify {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        pool: Option<PoolId>,
    },
    /// Fence an empty target before a manual byte-exact copy.
    PrepareCopy {
        #[command(flatten)]
        selection: Selection,
        #[arg(long)]
        copy_root: PathBuf,
    },
    /// Verify a fenced manual copy before permitting activation.
    VerifyCopy {
        #[command(flatten)]
        selection: Selection,
        #[arg(long)]
        copy_root: PathBuf,
    },
    /// Copy one immutable member identity, verify every byte, then remove its fence.
    Clone {
        #[command(flatten)]
        selection: Selection,
        #[arg(long)]
        copy_root: PathBuf,
        #[arg(long)]
        resume: bool,
    },
    /// Reconstruct one blank configured member; retain its immutable identity.
    Replace {
        #[command(flatten)]
        selection: Selection,
        #[arg(long)]
        resume: bool,
        #[arg(long, required = true)]
        authorize: bool,
    },
}
#[derive(Default, Serialize)]
struct Report {
    files: u64,
    bytes: u64,
    reconstructed_shards: u64,
    unrecoverable_private_stripes: u64,
    elapsed_ms: f64,
}

fn reserved(entry: &DirEntry) -> bool {
    entry.depth() == 1
        && matches!(
            entry.file_name().to_str(),
            Some(
                LEASE_FILE
                    | RESTORE_PENDING
                    | WORK
                    | ".n1-restore-work"
                    | ".n1-restore-complete"
                    | ".n1-backup-views"
            )
        )
}
fn walk(root: &Path) -> impl Iterator<Item = walkdir::Result<DirEntry>> {
    WalkDir::new(root)
        .min_depth(1)
        .max_open(16)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| !reserved(e))
}

fn verify_member(member: &Member, report: &mut Report) -> Result<()> {
    member.check_binding()?;
    for entry in walk(&member.path) {
        let entry = entry?;
        if entry.file_type().is_dir() {
            continue;
        }
        if !entry.file_type().is_file() {
            return Err(invalid(
                "LINK members cannot contain aliases or special files",
            ));
        }
        let relative = entry.path().strip_prefix(&member.path)?;
        let relative = relative
            .to_str()
            .ok_or_else(|| invalid("non-UTF8 member path"))?;
        let mut file = member.open_read(Path::new(relative))?.try_clone()?;
        let length = file.metadata()?.len();
        verify_file(relative, &member.marker, &mut file, length)?;
        report.files += 1;
        report.bytes += length;
    }
    member.check_binding()?;
    Ok(())
}

/// Interrupted private frames may be missing or torn. Validate their identity
/// and read every allocated extent for I/O errors, without reading a potentially
/// enormous sparse hole as if it were an uploaded payload. Committed data still
/// passes the complete frame/root verifier.
fn verify_file(
    path: &str,
    marker: &super::storage_pool::MemberMarker,
    file: &mut File,
    length: u64,
) -> Result<()> {
    use super::backup::restore::{FileKind, allowed_path};
    use std::os::fd::AsRawFd;
    if let Some(FileKind::Staged(fp, index)) = allowed_path(path, marker, true)? {
        let mut bytes = [0; 128];
        super::linux_io::pread_exact_at(&*file, &mut bytes, 0)?;
        let header = n1_link::shard::ShardHeader::decode(&bytes, &marker.layout, fp, index)?;
        if length < 128 || length > header.file_bytes()? {
            return Err(invalid("invalid staged maintenance length"));
        }
        let mut offset = 128;
        let mut buffer = vec![0; 128 * 1024];
        while offset < length {
            let data = unsafe { libc::lseek(file.as_raw_fd(), offset as i64, libc::SEEK_DATA) };
            if data < 0 {
                let error = std::io::Error::last_os_error();
                match error.raw_os_error() {
                    Some(libc::ENXIO) => break,
                    Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS) => {}
                    _ => return Err(error.into()),
                }
            } else {
                offset = data as u64;
            }
            let hole = unsafe { libc::lseek(file.as_raw_fd(), offset as i64, libc::SEEK_HOLE) };
            let end = if hole < 0 {
                let error = std::io::Error::last_os_error();
                match error.raw_os_error() {
                    Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS) => length,
                    _ => return Err(error.into()),
                }
            } else {
                (hole as u64).min(length)
            };
            if end <= offset {
                return Err(invalid("invalid staged extent boundary"));
            }
            while offset < end {
                let n = (end - offset).min(buffer.len() as u64) as usize;
                super::linux_io::pread_exact_at(&*file, &mut buffer[..n], offset)?;
                offset += n as u64;
            }
        }
        return Ok(());
    }
    validate_member_file(path, marker, file, length, true)
}

struct CopyOptions {
    prepare_only: bool,
    manual: bool,
    resume: bool,
    independent: bool,
}
fn clone_member(
    pool: &Pool,
    slot: usize,
    target: &Path,
    options: CopyOptions,
    report: &mut Report,
) -> Result<()> {
    let CopyOptions {
        prepare_only,
        manual,
        resume,
        independent,
    } = options;
    let source = pool
        .members
        .get(slot)
        .and_then(Option::as_ref)
        .ok_or_else(|| invalid("selected member is unavailable or outside pool"))?;
    pool.writable()?;
    let target = fs::canonicalize(target)?;
    for member in pool.members.iter().flatten() {
        let root = fs::canonicalize(&member.path)?;
        if root.starts_with(&target) || target.starts_with(&root) {
            return Err(invalid("copy target overlaps a configured member"));
        }
    }
    let lease = Target::open(&target)?;
    if independent && fs::metadata(&source.path)?.dev() == fs::metadata(&target)?.dev() {
        return Err(invalid(
            "clone destination must be on another mounted filesystem",
        ));
    }
    let fence = serde_json::to_vec(&serde_json::json!({
        "format": "n1-offline-link-pool-v1",
        "operation": "clone",
        "member": source.marker,
        "source": fs::canonicalize(&source.path)?,
        "target": target,
    }))?;
    if prepare_only {
        prepare(&target, &fence, false, false)?;
        return Ok(());
    }
    // Manual copies must have been fenced by prepare-copy; an arbitrary
    // populated directory cannot acquire maintenance ownership retroactively.
    prepare(&target, &fence, resume || manual, manual)?;
    verify_member(source, &mut Report::default())?;
    if !manual {
        for entry in walk(&source.path) {
            let entry = entry?;
            lease.check()?;
            source.check_binding()?;
            copy_entry(
                entry.path(),
                &target,
                entry.path().strip_prefix(&source.path)?,
            )?;
        }
    }
    verify_copy(&source.path, &target, report)?;
    let copied = read_regular(&target.join(".n1-arc-pool.json"), MAX_METADATA)?;
    if serde_json::from_slice::<super::storage_pool::MemberMarker>(&copied)? != source.marker {
        return Err(invalid(
            "copied member identity differs from the configured map",
        ));
    }
    source.check_binding()?;
    lease.check()?;
    complete(&target)
}

pub fn run_from(args: Vec<std::ffi::OsString>) -> Result<()> {
    let started = std::time::Instant::now();
    let cli = Cli::parse_from(args);
    let (config_path, selected) = match &cli.command {
        Operation::Verify { config, pool } => (config, *pool),
        Operation::PrepareCopy { selection, .. }
        | Operation::VerifyCopy { selection, .. }
        | Operation::Clone { selection, .. }
        | Operation::Replace { selection, .. } => (&selection.config, Some(selection.pool)),
    };
    let config = Config::load(config_path)?;
    let state = config.ensure_state_root()?;
    let _control_lease = InstanceLease::acquire(&[state], false)?;
    if let Operation::Replace {
        selection,
        resume,
        authorize,
    } = &cli.command
    {
        if !authorize {
            return Err(invalid("replacement requires --authorize"));
        }
        let configured = config
            .pools
            .iter()
            .find(|p| p.layout.id == selection.pool)
            .ok_or_else(|| invalid("unknown storage pool"))?;
        let pool = Pool::open_survivors(
            configured.layout.clone(),
            &configured.roots,
            selection.member,
        )?;
        super::root_identity::verify_pool(&pool, &configured.members)?;
        let mut report = Report::default();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        runtime.block_on(replacement::replace(
            &config,
            configured,
            &pool,
            selection.member,
            *resume,
            true,
            &mut report,
        ))?;
        report.elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }
    let configured = config
        .pools
        .iter()
        .filter(|p| selected.is_none_or(|id| p.layout.id == id));
    let mut pools = Vec::new();
    for configured in configured {
        let pool = Pool::open(configured.layout.clone(), &configured.roots)?;
        super::root_identity::verify_pool(&pool, &configured.members)?;
        pool.writable()?;
        pools.push(pool);
    }
    if pools.is_empty() {
        return Err(invalid("unknown storage pool"));
    }
    let mut report = Report::default();
    match cli.command {
        Operation::Verify { .. } => {
            for pool in &pools {
                for member in pool.members.iter().flatten() {
                    verify_member(member, &mut report)?;
                }
            }
        }
        operation => {
            let (selection, target, prepare, manual, resume) = match operation {
                Operation::PrepareCopy {
                    selection,
                    copy_root,
                } => (selection, copy_root, true, false, false),
                Operation::VerifyCopy {
                    selection,
                    copy_root,
                } => (selection, copy_root, false, true, false),
                Operation::Clone {
                    selection,
                    copy_root,
                    resume,
                } => (selection, copy_root, false, false, resume),
                _ => unreachable!(),
            };
            let canonical = fs::canonicalize(&target)?;
            let control = fs::canonicalize(config.state_root())?;
            if canonical.starts_with(&control) || control.starts_with(&canonical) {
                return Err(invalid("copy target overlaps engine control state"));
            }
            for root in config.pools.iter().flat_map(|p| &p.roots) {
                let root = fs::canonicalize(root)?;
                if canonical.starts_with(&root) || root.starts_with(&canonical) {
                    return Err(invalid("copy target overlaps configured storage"));
                }
            }
            clone_member(
                &pools[0],
                selection.member,
                &canonical,
                CopyOptions {
                    prepare_only: prepare,
                    manual,
                    resume,
                    independent: true,
                },
                &mut report,
            )?;
        }
    }
    report.elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

// Copy, fence and inventory primitives below are retained from the original
// storage_admin implementation. Format-specific verification is above.
fn read_regular(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut file = open_regular(path)?;
    if file.metadata()?.len() > limit {
        return Err(invalid("metadata exceeds maintenance limit"));
    }
    let mut bytes = Vec::new();
    (&mut file).take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(invalid("metadata grew during maintenance"));
    }
    Ok(bytes)
}
fn open_regular(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("expected regular storage file"));
    }
    Ok(file)
}
fn relative(path: &Path) -> Result<&Path> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(invalid("invalid member-relative storage path"));
    }
    Ok(path)
}
fn safe_join(root: &Path, path: &Path) -> Result<PathBuf> {
    relative(path)?;
    let mut current = root.to_path_buf();
    for component in path.components() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(invalid("storage path traverses a symlink"));
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(current)
}
fn digest(path: &Path) -> Result<[u8; 32]> {
    let mut file = open_regular(path)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0u8; 256 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hash.finalize().into())
}
fn prepare(target: &Path, marker: &[u8], resume: bool, populated_copy: bool) -> Result<()> {
    let fence = target.join(RESTORE_PENDING);
    if fence.try_exists()? {
        if !resume || read_regular(&fence, MAX_METADATA)? != marker {
            return Err(invalid("maintenance fence does not match this operation"));
        }
    } else {
        if resume {
            return Err(invalid("no matching interrupted maintenance to resume"));
        }
        if !populated_copy {
            for entry in fs::read_dir(target)? {
                if entry?.file_name() != LEASE_FILE {
                    return Err(invalid("maintenance requires an empty target"));
                }
            }
        }
        atomic_bytes(&fence, marker)?;
    }
    Ok(())
}
fn atomic_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = path.with_file_name(format!(".n1-pool-{}.tmp", ulid::Ulid::new()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        File::open(path.parent().unwrap())?.sync_all()?;
        Ok(())
    })();
    let _ = fs::remove_file(&temp);
    result
}
fn ensure_parents(target: &Path, rel: &Path) -> Result<PathBuf> {
    let final_path = safe_join(target, rel)?;
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(final_path)
}
fn copy_entry(source: &Path, target: &Path, rel: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        return Err(invalid("LINK member copy refuses aliases"));
    } else {
        let path = ensure_parents(target, rel)?;
        if metadata.is_dir() {
            fs::create_dir_all(path)?;
        } else if metadata.is_file() {
            if path.try_exists()? {
                if digest(source)? != digest(&path)? {
                    return Err(invalid("conflicting existing copy; no silent overwrite"));
                }
                return Ok(());
            }
            fs::create_dir_all(safe_join(target, Path::new(WORK))?)?;
            let tmp = target
                .join(WORK)
                .join(format!("copy-{}", ulid::Ulid::new()));
            let mut input = open_regular(source)?;
            let mut output = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(metadata.mode() & 0o777)
                .open(&tmp)?;
            std::io::copy(&mut input, &mut output)?;
            output.set_times(fs::FileTimes::new().set_modified(metadata.modified()?))?;
            output.sync_all()?;
            fs::hard_link(&tmp, &path)?;
            fs::remove_file(tmp)?;
        } else {
            return Err(invalid("copy refuses special files"));
        }
    }
    Ok(())
}
fn verify_copy(source: &Path, target: &Path, report: &mut Report) -> Result<()> {
    for (from, to, forward) in [(source, target, true), (target, source, false)] {
        for entry in walk(from) {
            let entry = entry?;
            let rel = entry.path().strip_prefix(from)?;
            let path = to.join(relative(rel)?);
            let other = fs::symlink_metadata(&path)?;
            let meta = entry.metadata()?;
            if meta.file_type() != other.file_type() {
                return Err(invalid("copy inventory type mismatch"));
            }
            if entry.file_type().is_symlink() {
                if fs::read_link(entry.path())? != fs::read_link(&path)? {
                    return Err(invalid("copy alias mismatch"));
                }
            } else if entry.file_type().is_file() && forward {
                if meta.len() != other.len()
                    || digest(entry.path())? != digest(&safe_join(to, rel)?)?
                {
                    return Err(invalid("copy content mismatch"));
                }
                report.files += 1;
                report.bytes += meta.len();
            }
        }
    }
    Ok(())
}
fn complete(target: &Path) -> Result<()> {
    // Full file/directory synchronization is ONLY for offline cutover. It does
    // not add fsyncs to ordinary LINK object operations.
    for entry in WalkDir::new(target)
        .min_depth(1)
        .max_open(16)
        .contents_first(true)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| !(e.depth() == 1 && e.file_name() == WORK))
    {
        let entry = entry?;
        if entry.file_type().is_file() {
            open_regular(entry.path())?.sync_all()?;
        } else if entry.file_type().is_dir() {
            File::open(entry.path())?.sync_all()?;
        }
    }
    File::open(target)?.sync_all()?;
    if target.join(WORK).try_exists()? {
        fs::remove_dir_all(target.join(WORK))?;
    }
    fs::remove_file(target.join(RESTORE_PENDING))?;
    File::open(target)?.sync_all()?;
    Ok(())
}

fn expect(target: &Path, rel: &Path, kind: &str) -> Result<()> {
    relative(rel)?;
    let key = hex::encode(Sha256::digest(rel.as_os_str().as_encoded_bytes()));
    let path = safe_join(
        target,
        &Path::new(WORK).join("inventory").join(&key[..2]).join(&key),
    )?;
    fs::create_dir_all(path.parent().unwrap())?;
    let bytes = format!("{kind}\n{}", rel.display());
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
    {
        Ok(mut file) => file.write_all(bytes.as_bytes())?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if read_regular(&path, 8192)? != bytes.as_bytes() {
                return Err(invalid("conflicting replacement inventory"));
            }
        }
        Err(e) => return Err(e.into()),
    }
    if let Some(parent) = rel.parent().filter(|p| !p.as_os_str().is_empty()) {
        expect(target, parent, "directory")?;
    }
    Ok(())
}
fn audit_replacement(target: &Path) -> Result<()> {
    // A fresh replacement has only its own fence/work area. Do not let the
    // clone scanner's exclusions hide unrelated backup/restore state here.
    for entry in WalkDir::new(target)
        .min_depth(1)
        .max_open(16)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| {
            !(entry.depth() == 1
                && matches!(entry.file_name().to_str(), Some(WORK | RESTORE_PENDING)))
        })
    {
        let entry = entry?;
        let rel = entry.path().strip_prefix(target)?;
        let kind = if entry.file_type().is_dir() {
            "directory"
        } else if entry.file_type().is_file() {
            "file"
        } else if entry.file_type().is_symlink() {
            "link"
        } else {
            return Err(invalid("unexpected replacement file type"));
        };
        let key = hex::encode(Sha256::digest(rel.as_os_str().as_encoded_bytes()));
        let expected = target
            .join(WORK)
            .join("inventory")
            .join(&key[..2])
            .join(key);
        if read_regular(&expected, 8192)? != format!("{kind}\n{}", rel.display()).as_bytes() {
            return Err(invalid("unexpected file in replacement; refuse activation"));
        }
    }
    for entry in WalkDir::new(safe_join(target, &Path::new(WORK).join("inventory"))?)
        .max_open(16)
        .follow_links(false)
    {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let expected = String::from_utf8(read_regular(entry.path(), 8192)?)?;
        let (kind, rel) = expected
            .split_once('\n')
            .ok_or_else(|| invalid("invalid inventory record"))?;
        let rel = relative(Path::new(rel))?;
        let path = if kind == "link" {
            target.join(rel)
        } else {
            safe_join(target, rel)?
        };
        let meta = fs::symlink_metadata(path)?;
        if !match kind {
            "file" => meta.is_file(),
            "directory" => meta.is_dir(),
            "link" => meta.file_type().is_symlink(),
            _ => false,
        } {
            return Err(invalid("missing or wrong-type replacement inventory entry"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;
    #[test]
    fn replacement_inventory_detects_missing_and_unexpected_files() {
        let f = Fixture::new();
        let target = f.dir("target");
        fs::write(target.join("data"), b"data").unwrap();
        expect(&target, Path::new("data"), "file").unwrap();
        audit_replacement(&target).unwrap();
        fs::write(target.join("extra"), b"extra").unwrap();
        assert!(audit_replacement(&target).is_err());
        fs::remove_file(target.join("extra")).unwrap();
        fs::create_dir(target.join(".n1-backup-views")).unwrap();
        assert!(audit_replacement(&target).is_err());
        fs::remove_dir(target.join(".n1-backup-views")).unwrap();
        fs::remove_file(target.join("data")).unwrap();
        assert!(audit_replacement(&target).is_err());
    }
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("n1-pool-admin-{}", ulid::Ulid::new()));
            fs::create_dir(&root).unwrap();
            Self(root)
        }
        fn dir(&self, name: &str) -> PathBuf {
            let path = self.0.join(name);
            fs::create_dir(&path).unwrap();
            path
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn copied_inventory_requires_exact_content_and_no_extra_entries() {
        let f = Fixture::new();
        let source = f.dir("source");
        let target = f.dir("target");
        fs::write(source.join("data"), b"abcd").unwrap();
        copy_entry(&source.join("data"), &target, Path::new("data")).unwrap();
        verify_copy(&source, &target, &mut Report::default()).unwrap();
        fs::write(target.join("data"), b"abce").unwrap();
        assert!(verify_copy(&source, &target, &mut Report::default()).is_err());
        assert!(copy_entry(&source.join("data"), &target, Path::new("data")).is_err());
        fs::write(target.join("data"), b"abcd").unwrap();
        fs::write(target.join("extra"), b"x").unwrap();
        assert!(verify_copy(&source, &target, &mut Report::default()).is_err());
        fs::remove_file(target.join("extra")).unwrap();
        fs::remove_file(target.join("data")).unwrap();
        assert!(verify_copy(&source, &target, &mut Report::default()).is_err());
    }
    #[test]
    fn resumable_fence_is_bound_to_operation_and_removed_only_by_completion() {
        let f = Fixture::new();
        let target = f.dir("target");
        prepare(&target, b"operation-a", false, false).unwrap();
        assert!(prepare(&target, b"operation-b", true, false).is_err());
        assert!(prepare(&target, b"operation-a", false, false).is_err());
        prepare(&target, b"operation-a", true, false).unwrap();
        fs::create_dir(target.join(WORK)).unwrap();
        fs::write(target.join(WORK).join("partial"), b"partial").unwrap();
        complete(&target).unwrap();
        assert!(!target.join(RESTORE_PENDING).exists());
        assert!(!target.join(WORK).exists());
    }
    #[test]
    fn copy_cannot_follow_a_symlinked_work_directory() {
        let f = Fixture::new();
        let source = f.dir("source");
        let target = f.dir("target");
        let elsewhere = f.dir("elsewhere");
        fs::write(source.join("data"), b"data").unwrap();
        symlink(&elsewhere, target.join(WORK)).unwrap();
        assert!(copy_entry(&source.join("data"), &target, Path::new("data")).is_err());
        assert_eq!(fs::read_dir(elsewhere).unwrap().count(), 0);
        assert!(safe_join(&target, Path::new("../escape")).is_err());
    }

    fn options(prepare_only: bool, manual: bool, resume: bool) -> CopyOptions {
        CopyOptions {
            prepare_only,
            manual,
            resume,
            independent: false,
        }
    }

    #[tokio::test]
    async fn verified_member_cutover_preserves_keys_retirements_and_durable_multipart_progress() {
        use crate::core::{Store, tests::Fixture as EngineFixture};
        use n1_link::Durability;
        let f = EngineFixture::new();
        let store = f.enroll();
        let scope = [11; 16];
        let payload = vec![63; 37000];
        let live = store
            .init(
                scope,
                0,
                payload.len() as u64,
                49152,
                Durability::Durable,
                3600,
            )
            .await
            .unwrap();
        store
            .put(
                scope,
                live.object_key.clone(),
                std::io::Cursor::new(payload.clone()),
            )
            .await
            .unwrap();
        let dead = store
            .init(scope, 0, 1, 12288, Durability::Durable, 3600)
            .await
            .unwrap();
        store
            .put(
                scope,
                dead.object_key.clone(),
                std::io::Cursor::new(vec![7]),
            )
            .await
            .unwrap();
        store.delete(scope, dead.object_key.clone()).await.unwrap();
        let upload = store
            .init(scope, 0, 36864, 12288, Durability::Durable, 3600)
            .await
            .unwrap();
        store
            .write_part(
                scope,
                upload.object_key.clone(),
                1,
                std::io::Cursor::new(vec![41; 12288]),
            )
            .await
            .unwrap();
        let target = f.base.join("copy");
        fs::create_dir(&target).unwrap();
        let mut report = Report::default();
        clone_member(
            &store.pools[0],
            0,
            &target,
            options(false, false, false),
            &mut report,
        )
        .unwrap();
        // One pool marker, one live shard and one incomplete shard. Purged
        // identities contribute no files to a member copy.
        assert_eq!(report.files, 3);
        assert!(!target.join(RESTORE_PENDING).exists());
        let identities = store.pools[0].members[0]
            .as_ref()
            .unwrap()
            .marker
            .members
            .clone();
        drop(store);
        let mut paths = f.paths.clone();
        paths[0] = target;
        let pool = Pool::reopen_fixture(f.layout.clone(), &paths).unwrap();
        crate::core::root_identity::verify_pool(&pool, &identities).unwrap();
        let recovered = Store::new(vec![pool], [17; 32], 16).unwrap();
        recovered.configure_read_repair(false);
        assert_eq!(
            recovered
                .download(scope, live.object_key.clone(), 0, None, Vec::new())
                .await
                .unwrap()
                .0,
            payload
        );
        assert!(matches!(
            recovered.stat(scope, dead.object_key.clone()).await,
            Err(super::super::Error::NotFound)
        ));
        assert!(matches!(
            recovered
                .put(
                    scope,
                    dead.object_key.clone(),
                    std::io::Cursor::new(vec![7])
                )
                .await,
            Err(super::super::Error::NotFound)
        ));
        let status = recovered
            .upload_status(scope, upload.object_key.clone(), 0, 16)
            .await
            .unwrap();
        assert_eq!(status.missing_parts, vec![0, 2]);
        for part in [0, 2] {
            recovered
                .write_part(
                    scope,
                    upload.object_key.clone(),
                    part,
                    std::io::Cursor::new(vec![41; 12288]),
                )
                .await
                .unwrap();
        }
        recovered
            .finalize(scope, upload.object_key.clone())
            .await
            .unwrap();
        assert_eq!(
            recovered
                .download(scope, upload.object_key.clone(), 0, None, Vec::new())
                .await
                .unwrap()
                .0,
            vec![41; 36864]
        );
    }

    #[test]
    fn clone_resume_requires_owned_fence_and_refuses_corrupted_source_or_extra_copy_entries() {
        let f = crate::core::tests::Fixture::new();
        let store = f.enroll();
        let target = f.base.join("copy");
        fs::create_dir(&target).unwrap();
        let pool = &store.pools[0];
        let mut report = Report::default();
        assert!(clone_member(pool, 0, &target, options(false, true, false), &mut report).is_err());
        clone_member(pool, 0, &target, options(true, false, false), &mut report).unwrap();
        assert!(
            Pool::reopen_fixture(
                f.layout.clone(),
                &[
                    target.clone(),
                    f.paths[1].clone(),
                    f.paths[2].clone(),
                    f.paths[3].clone()
                ]
            )
            .is_err()
        );
        fs::write(target.join("extra"), b"never remove unknown data").unwrap();
        assert!(clone_member(pool, 0, &target, options(false, false, true), &mut report).is_err());
        assert!(target.join(RESTORE_PENDING).exists());
        assert_eq!(
            fs::read(target.join("extra")).unwrap(),
            b"never remove unknown data"
        );
        fs::remove_file(target.join("extra")).unwrap();
        clone_member(pool, 0, &target, options(false, false, true), &mut report).unwrap();
        assert!(!target.join(RESTORE_PENDING).exists());

        let bad = f.base.join("bad-copy");
        fs::create_dir(&bad).unwrap();
        fs::write(f.paths[0].join("unexpected"), b"unknown").unwrap();
        assert!(clone_member(pool, 0, &bad, options(false, false, false), &mut report).is_err());
        assert!(bad.join(RESTORE_PENDING).exists());
        assert!(!bad.join("unexpected").exists());
    }
}
