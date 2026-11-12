//! LINK adapter for the retained offline replacement workflow. No client key,
//! permanent inventory or upload receipt is introduced. The temporary expected
//! inventory belongs to the fenced offline operation and is removed at cutover.
use super::*;
use crate::core::{
    Error as ObjectError,
    backup::restore::{FileKind, allowed_path},
    download::{OpenObject, decode_stripe_strict},
    ec::StripeCodec,
    store::{ShardFile, working_bytes},
};
use crate::settings::config::ConfiguredPool;
use n1_link::{
    Fingerprint,
    shard::{self, ShardHeader},
};
use rustix::fs::{self as rfs, AtFlags, Mode, OFlags};
use std::sync::Arc;

pub(super) async fn replace(
    config: &Config,
    configured: &ConfiguredPool,
    pool: &Pool,
    slot: usize,
    resume: bool,
    independent: bool,
    report: &mut Report,
) -> Result<()> {
    if slot >= pool.layout.width() || pool.layout.parity_shards == 0 {
        return Err(invalid("lost member requires parity or a verified backup"));
    }
    if pool
        .members
        .iter()
        .filter(|member| member.is_some())
        .count()
        < usize::from(pool.layout.data_shards)
    {
        return Err(invalid(
            "replacement requires at least K surviving enrolled members",
        ));
    }
    if working_bytes(pool) > config.operation_memory_bytes {
        return Err(invalid(
            "replacement stripe workspace exceeds configured memory budget",
        ));
    }
    crate::core::root_identity::verify_pool(pool, &configured.members)?;
    let target = fs::canonicalize(&configured.roots[slot])?;
    let control = fs::canonicalize(config.state_root())?;
    if target.starts_with(&control) || control.starts_with(&target) {
        return Err(invalid("replacement overlaps engine control state"));
    }
    for p in &config.pools {
        for (s, path) in p.roots.iter().enumerate() {
            if p.layout.id == pool.layout.id && s == slot {
                continue;
            }
            let path = match fs::canonicalize(path) {
                Ok(path) => path,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            if path.starts_with(&target) || target.starts_with(&path) {
                return Err(invalid("replacement overlaps configured storage"));
            }
        }
    }
    let pinned = Target::open(&target)?;
    let target_device = fs::metadata(&target)?.dev();
    for member in pool.members.iter().flatten() {
        if independent && fs::metadata(&member.path)?.dev() == target_device {
            return Err(invalid("replacement must use an independent filesystem"));
        }
        verify_member(member, &mut Report::default())?;
    }
    let marker = super::super::storage_pool::MemberMarker {
        format: "LINK-pool-v1".into(),
        layout: pool.layout.clone(),
        members: configured.members.clone(),
        slot,
    };
    let sources: Vec<_> = pool
        .members
        .iter()
        .flatten()
        .map(|m| fs::canonicalize(&m.path))
        .collect::<std::io::Result<_>>()?;
    let fence = serde_json::to_vec(&serde_json::json!({
        "format": "n1-offline-link-pool-v1",
        "operation": "replace",
        "member": marker,
        "sources": sources,
        "target": target,
    }))?;
    prepare(&target, &fence, resume, false)?;
    let marker_path = target.join(".n1-arc-pool.json");
    let mut marker_bytes = serde_json::to_vec(&marker)?;
    marker_bytes.push(b'\n');
    if marker_path.try_exists()? {
        if read_regular(&marker_path, MAX_METADATA)? != marker_bytes {
            return Err(invalid("conflicting replacement member marker"));
        }
    } else {
        atomic_bytes(&marker_path, &marker_bytes)?;
    }
    // These are accelerators for this invocation only, never recovery authority.
    // Resume re-derives the entire expected inventory from leased sources.
    let work = safe_join(&target, Path::new(WORK))?;
    fs::create_dir_all(&work)?;
    for name in ["inventory", "seen"] {
        let path = safe_join(&target, &Path::new(WORK).join(name))?;
        if path.try_exists()? {
            fs::remove_dir_all(&path)?;
        }
        fs::create_dir(&path)?;
    }
    expect(&target, Path::new(".n1-arc-pool.json"), "file")?;
    let _ = pinned.parent(Path::new(".uploads/placeholder"), true)?;
    expect(&target, Path::new(".uploads"), "directory")?;
    let mut codec = StripeCodec::new(&pool.layout)?;
    for member in pool.members.iter().flatten() {
        for entry in walk(&member.path) {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            pinned.check()?;
            member.check_binding()?;
            let rel = entry
                .path()
                .strip_prefix(&member.path)?
                .to_str()
                .ok_or_else(|| invalid("non-UTF8 member path"))?;
            let fp = match allowed_path(rel, &member.marker, true)? {
                Some(FileKind::Final(fp, _)) => fp,
                Some(FileKind::Staged(fp, _)) if rel.ends_with("/shard.tmp") => fp,
                _ => continue,
            };
            let fingerprint = fp.to_string();
            let seen_parent = work.join("seen").join(&fingerprint[..2]);
            fs::create_dir_all(&seen_parent)?;
            let seen_path = seen_parent.join(fingerprint);
            if seen_path.try_exists()? {
                continue;
            }
            rebuild(pool, &pinned, &target, slot, fp, &mut codec, report).await?;
            let _ = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(seen_path)?;
        }
    }
    // No extra files, aliases, wrong types or missing expected files can be
    // hidden by a matching operation fence. Validate output framing again.
    audit_replacement(&target)?;
    for entry in walk(&target) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry
            .path()
            .strip_prefix(&target)?
            .to_str()
            .ok_or_else(|| invalid("non-UTF8 replacement path"))?;
        let mut file = open_regular(entry.path())?;
        let len = file.metadata()?.len();
        verify_file(rel, &marker, &mut file, len)?;
        report.files += 1;
        report.bytes += len;
    }
    for member in pool.members.iter().flatten() {
        verify_member(member, &mut Report::default())?;
    }
    pinned.check()?;
    complete(&target)
}

fn optional_read(member: &Member, path: &str) -> Result<Option<Arc<File>>> {
    match member.open_read(Path::new(path)) {
        Ok(file) => Ok(Some(file)),
        Err(ObjectError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn input(pool: &Pool, fp: Fingerprint, target_shard: u16) -> Result<Option<(OpenObject, bool)>> {
    let mut files = vec![None; pool.layout.width()];
    let mut expected: Option<ShardHeader> = None;
    let mut final_count = 0;
    let mut root: Option<[u8; 32]> = None;
    for member in pool.members.iter().flatten() {
        let shard = pool.layout.shard_at_slot(fp, member.marker.slot)?;
        let (file, finalized) = if let Some(file) = optional_read(member, &fp.shard_path(shard))? {
            (file, true)
        } else if let Some(file) = optional_read(member, &fp.staging_path())? {
            (file, false)
        } else {
            continue;
        };
        let mut bytes = [0; shard::HEADER_BYTES];
        crate::core::linux_io::pread_exact_at(&*file, &mut bytes, 0)?;
        let header = ShardHeader::decode(&bytes, &pool.layout, fp, shard)?;
        let mut normalized = header.clone();
        normalized.shard = target_shard;
        if expected.as_ref().is_some_and(|other| *other != normalized) {
            return Err(invalid(
                "upload parameters disagree across surviving members",
            ));
        }
        expected = Some(normalized);
        let len = file.metadata()?.len();
        let seal = if finalized {
            if len != header.file_bytes()? {
                return Err(invalid("invalid finalized source length"));
            }
            let mut bytes = [0; shard::SEAL_BYTES];
            crate::core::linux_io::pread_exact_at(&*file, &mut bytes, header.seal_offset())?;
            let seal = shard::decode_seal(&header, fp, &bytes)?;
            if root.is_some_and(|old| old != seal) {
                return Err(invalid("surviving object seals disagree"));
            }
            root = Some(seal);
            final_count += 1;
            Some(seal)
        } else {
            None
        };
        files[usize::from(shard)] = Some(ShardFile {
            member: member.clone(),
            file,
            header,
            finalized,
            root: seal,
            readable_bytes: len,
        });
    }
    Ok(expected.map(|header| {
        (
            OpenObject {
                files,
                header,
                root: root.unwrap_or([0; 32]),
                missing: Vec::new(),
                corrupt: Vec::new(),
                unavailable: Vec::new(),
            },
            final_count >= usize::from(pool.layout.data_shards),
        )
    }))
}

fn next_private_stripe(object: &OpenObject, start: u64, end: u64) -> Result<u64> {
    use std::os::fd::AsRawFd;
    let offset = object.header.frame_offset(start)?;
    let mut next = end;
    for file in object.files.iter().flatten() {
        if file.readable_bytes <= offset {
            continue;
        }
        // Data/hole queries are hints only. Every candidate is still decoded,
        // checksummed and reconstructed. Filesystems without extent reporting
        // use the conservative sequential path. Sources are opened privately;
        // all payload reads use pread, independent of this descriptor offset.
        let data = unsafe { libc::lseek(file.file.as_raw_fd(), offset as i64, libc::SEEK_DATA) };
        if data < 0 {
            let error = std::io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::ENXIO) => continue,
                Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS) => return Ok(start),
                _ => return Err(error.into()),
            }
        }
        let stripe =
            (data as u64).saturating_sub(shard::HEADER_BYTES as u64) / object.header.record_bytes();
        next = next.min(stripe.max(start));
    }
    Ok(next)
}

async fn rebuild(
    pool: &Pool,
    pinned: &Target,
    target: &Path,
    slot: usize,
    fp: Fingerprint,
    codec: &mut StripeCodec,
    report: &mut Report,
) -> Result<()> {
    let shard_index = pool.layout.shard_at_slot(fp, slot)?;
    let Some(inputs) = input(pool, fp, shard_index)? else {
        return Ok(());
    };
    let temporary = Path::new(WORK).join(format!("rebuild-{fp}"));
    let parent = pinned.parent(&temporary, true)?;
    let name = temporary
        .file_name()
        .ok_or_else(|| invalid("invalid rebuild temporary"))?;
    match rfs::statat(&*parent, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) if rfs::FileType::from_raw_mode(stat.st_mode) == rfs::FileType::RegularFile => {
            rfs::unlinkat(&*parent, name, AtFlags::empty())?;
        }
        Ok(_) => return Err(invalid("unsafe interrupted rebuild temporary")),
        Err(rustix::io::Errno::NOENT) => {}
        Err(e) => return Err(e.into()),
    }
    let output = Arc::new(File::from(rfs::openat(
        &*parent,
        name,
        OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?));
    let relative = {
        let (object, finalized) = inputs;
        crate::core::linux_io::write_at_async(output.clone(), object.header.encode(fp).to_vec(), 0)
            .await?;
        let mut object_hash = shard::object_hasher(
            object.header.format,
            object.header.total_bytes,
            object.header.part_bytes,
        );
        // A header may describe a huge upload that never supplied any frames.
        // Its unwritten tail is not work proportional to the declared size.
        let stripes = if finalized {
            object.header.stripes()
        } else {
            object
                .files
                .iter()
                .flatten()
                .map(|file| {
                    file.readable_bytes
                        .saturating_sub(shard::HEADER_BYTES as u64)
                        / object.header.record_bytes()
                })
                .max()
                .unwrap_or(0)
                .min(object.header.stripes())
        };
        report.unrecoverable_private_stripes += object.header.stripes() - stripes;
        let parts = (stripes * object.header.stripe_bytes()).div_ceil(object.header.part_bytes);
        let per_part = object.header.part_bytes / object.header.stripe_bytes();
        let mut next = 0;
        let mut part = 0;
        while part < parts {
            if !finalized {
                let candidate = next_private_stripe(&object, next, stripes)?;
                report.unrecoverable_private_stripes += candidate - next;
                next = candidate;
                if next == stripes {
                    break;
                }
                part = next / per_part;
            }
            let mut part_hash = shard::part_hasher(object.header.format, part);
            let range = object.header.part_stripes(part)?;
            let part_end = range.end.min(stripes);
            let mut stripe = range.start.max(next);
            while stripe < part_end {
                if !finalized {
                    let candidate = next_private_stripe(&object, stripe, stripes)?;
                    report.unrecoverable_private_stripes += candidate - stripe;
                    stripe = candidate;
                    if stripe >= part_end {
                        break;
                    }
                }
                let current = stripe;
                stripe += 1;
                let verified = match decode_stripe_strict(&object, fp, current, codec).await {
                    Ok(verified) => verified,
                    Err(ObjectError::Unavailable(_)) if !finalized => {
                        report.unrecoverable_private_stripes += 1;
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                };
                let source = verified.source;
                let hash = verified.source_hash;
                part_hash.update(hash);
                let pieces = codec.encode(&source)?;
                let bytes = shard::encode_frame(
                    &object.header,
                    fp,
                    current,
                    hash,
                    &pieces[usize::from(shard_index)],
                )?;
                crate::core::linux_io::write_at_async(
                    output.clone(),
                    bytes,
                    object.header.frame_offset(current)?,
                )
                .await?;
            }
            next = stripe;
            object_hash.update(part_hash.finalize());
            part += 1;
        }
        if finalized {
            if <[u8; 32]>::from(object_hash.finalize()) != object.root {
                return Err(invalid(
                    "reconstructed object does not match original integrity root",
                ));
            }
            crate::core::linux_io::write_at_async(
                output.clone(),
                shard::encode_seal(&object.header, fp, object.root).to_vec(),
                object.header.seal_offset(),
            )
            .await?;
        }
        report.reconstructed_shards += 1;
        if finalized {
            fp.shard_path(shard_index)
        } else {
            fp.staging_path()
        }
    };
    // A complete output is published without replacing an existing file.
    // A resumed complete output must match exactly; interrupted private writes
    // remain under the operation's work directory rather than at a live path.
    crate::core::linux_io::fdatasync_async(output).await?;
    pinned.check()?;
    let relative = Path::new(&relative);
    let dest = pinned.parent(relative, true)?;
    let dest_name = relative
        .file_name()
        .ok_or_else(|| invalid("invalid output path"))?;
    match rfs::linkat(&*parent, name, &*dest, dest_name, AtFlags::empty()) {
        Ok(()) => {}
        Err(rustix::io::Errno::EXIST) => {
            if digest(&target.join(&temporary))? != digest(&safe_join(target, relative)?)? {
                return Err(invalid("conflicting previously published replacement file"));
            }
        }
        Err(e) => return Err(e.into()),
    }
    rfs::unlinkat(&*parent, name, AtFlags::empty())?;
    expect(target, relative, "file")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Store, tests::Fixture};
    use n1_link::Durability;
    const SCOPE: [u8; 16] = [11; 16];

    fn config(f: &Fixture, members: Vec<n1_link::PoolId>, target: &Path, slot: usize) -> Config {
        let mut roots = f.paths.clone();
        roots[slot] = target.to_owned();
        Config {
            format: crate::settings::config::CONFIG_FORMAT.into(),
            native_address: "127.0.0.1:19001".parse().unwrap(),
            http_address: None,
            signing_key_file: f.base.join("signing.key"),
            state_dir: None,
            max_connections: 16,
            max_operations: 16,
            operation_memory_bytes: 256 * 1024 * 1024,
            read_repair: false,
            concurrency: Default::default(),
            rate_limits: Default::default(),
            repositories: Default::default(),
            maintenance: Default::default(),
            runtime: Default::default(),
            tls: None,
            fragments: Vec::new(),
            pools: vec![ConfiguredPool {
                layout: f.layout.clone(),
                members,
                roots,
                sealed: false,
            }],
        }
    }
    async fn put(store: &Arc<Store>, data: &[u8]) -> crate::core::store::InitializedUpload {
        let upload = store
            .init(
                SCOPE,
                0,
                data.len() as u64,
                49152,
                Durability::Durable,
                3600,
            )
            .await
            .unwrap();
        store
            .put(
                SCOPE,
                upload.object_key.clone(),
                std::io::Cursor::new(data.to_vec()),
            )
            .await
            .unwrap();
        upload
    }
    fn identities(store: &Store) -> Vec<n1_link::PoolId> {
        store.pools[0].members[0]
            .as_ref()
            .unwrap()
            .marker
            .members
            .clone()
    }
    fn pool(f: &Fixture, c: &Config, slot: usize) -> Pool {
        Pool::survivors_fixture(f.layout.clone(), &c.pools[0].roots, slot).unwrap()
    }

    #[tokio::test]
    async fn two_lost_members_are_replaced_from_the_surviving_two_member_quorum() {
        replace_two_lost_members(false).await;
    }

    #[tokio::test]
    async fn two_connected_blank_replacements_do_not_enroll_an_unauthorized_peer() {
        replace_two_lost_members(true).await;
    }

    async fn replace_two_lost_members(blank_peers: bool) {
        let mut f = Fixture::new();
        f.layout.data_shards = 2;
        f.layout.parity_shards = 2;
        let store = f.enroll();
        let bytes = (0..37001).map(|i| (i * 71 % 251) as u8).collect::<Vec<_>>();
        let live = put(&store, &bytes).await;
        let empty = put(&store, &[]).await;
        let pending = store
            .init(SCOPE, 0, 49152, 16384, Durability::Durable, 3600)
            .await
            .unwrap();
        store
            .write_part(
                SCOPE,
                pending.object_key.clone(),
                1,
                std::io::Cursor::new(vec![41; 16384]),
            )
            .await
            .unwrap();
        let targets = [f.base.join("replacement-0"), f.base.join("replacement-1")];
        let mut c = config(&f, identities(&store), &targets[0], 0);
        c.ensure_state_root().unwrap();
        let mut unchanged = Vec::new();
        for member in store.pools[0].members[2..].iter().flatten() {
            for entry in walk(&member.path) {
                let entry = entry.unwrap();
                if entry.file_type().is_file() {
                    unchanged.push((
                        entry.path().to_owned(),
                        entry.metadata().unwrap().ino(),
                        digest(entry.path()).unwrap(),
                    ));
                }
            }
        }
        drop(store);
        for slot in 0..2 {
            fs::rename(&f.paths[slot], f.base.join(format!("lost-{slot}"))).unwrap();
        }
        if blank_peers {
            for (slot, target) in targets.iter().enumerate() {
                fs::create_dir(target).unwrap();
                c.pools[0].roots[slot] = target.clone();
            }
            // Ordinary startup must still reject unregistered roots.
            assert!(Pool::reopen_fixture(f.layout.clone(), &c.pools[0].roots).is_err());
            // A present but corrupt marker is never treated as a blank peer.
            fs::write(targets[1].join(".n1-arc-pool.json"), b"foreign-marker").unwrap();
            assert!(Pool::survivors_fixture(f.layout.clone(), &c.pools[0].roots, 0).is_err());
            fs::remove_file(targets[1].join(".n1-arc-pool.json")).unwrap();
        }
        let withheld = f.base.join("third-loss");
        fs::rename(&f.paths[2], &withheld).unwrap();
        assert!(Pool::survivors_fixture(f.layout.clone(), &c.pools[0].roots, 0).is_err());
        assert!(!targets[0].join(".n1-arc-pool.json").exists());
        fs::rename(withheld, &f.paths[2]).unwrap();
        for (slot, target) in targets.iter().enumerate() {
            // Only the selected target is enrolled by each authorized command.
            // The other lost slot remains unavailable until its own replacement.
            if !blank_peers {
                fs::create_dir(target).unwrap();
            }
            c.pools[0].roots[slot] = target.clone();
            let survivors = pool(&f, &c, slot);
            let mut report = Report::default();
            replace(&c, &c.pools[0], &survivors, slot, false, false, &mut report)
                .await
                .unwrap();
            assert_eq!(report.reconstructed_shards, 3);
            assert!(!target.join(RESTORE_PENDING).exists());
            if blank_peers && slot == 0 {
                assert_eq!(fs::read_dir(&targets[1]).unwrap().count(), 0);
            }
        }
        for (path, inode, hash) in unchanged {
            assert_eq!(path.metadata().unwrap().ino(), inode);
            assert_eq!(digest(&path).unwrap(), hash);
        }
        let recovered = Store::new(
            vec![Pool::reopen_fixture(f.layout.clone(), &c.pools[0].roots).unwrap()],
            [17; 32],
            16,
        )
        .unwrap();
        for (key, expected) in [(live.object_key, bytes), (empty.object_key, Vec::new())] {
            assert_eq!(
                recovered
                    .download(SCOPE, key, 0, None, Vec::new())
                    .await
                    .unwrap()
                    .0,
                expected
            );
        }
        assert_eq!(
            recovered
                .upload_status(SCOPE, pending.object_key.clone(), 0, 16)
                .await
                .unwrap()
                .missing_parts,
            vec![0, 2]
        );
        for part in [0, 2] {
            recovered
                .write_part(
                    SCOPE,
                    pending.object_key.clone(),
                    part,
                    std::io::Cursor::new(vec![41; 16384]),
                )
                .await
                .unwrap();
        }
        recovered
            .finalize(SCOPE, pending.object_key.clone())
            .await
            .unwrap();
        assert_eq!(
            recovered
                .download(SCOPE, pending.object_key, 0, None, Vec::new())
                .await
                .unwrap()
                .0,
            vec![41; 49152]
        );
    }

    #[tokio::test]
    async fn reconstruction_preserves_every_slot_live_bytes_retirements_and_sparse_durable_progress()
     {
        // All physical member positions rotate across shard indices. Exercise
        // each lost logical slot without relaxing product filesystem guards.
        for slot in 0..4 {
            let f = Fixture::new();
            let store = f.enroll();
            let data: Vec<_> = (0..37001).map(|i| (i * 71 % 251) as u8).collect();
            let live = put(&store, &data).await;
            let empty = put(&store, &[]).await;
            let dead = put(&store, &[9; 4096]).await;
            store.delete(SCOPE, dead.object_key.clone()).await.unwrap();
            let pending = store
                .init(SCOPE, 0, 36864, 12288, Durability::Durable, 3600)
                .await
                .unwrap();
            store
                .write_part(
                    SCOPE,
                    pending.object_key.clone(),
                    1,
                    std::io::Cursor::new(vec![41; 12288]),
                )
                .await
                .unwrap();
            let target = f.base.join("replacement");
            fs::create_dir(&target).unwrap();
            let c = config(&f, identities(&store), &target, slot);
            c.ensure_state_root().unwrap();
            // Keep evidence that no surviving file is replaced or rewritten.
            let mut snapshots = Vec::new();
            for (s, m) in store.pools[0].members.iter().enumerate() {
                if s == slot {
                    continue;
                }
                for entry in walk(&m.as_ref().unwrap().path) {
                    let entry = entry.unwrap();
                    if entry.file_type().is_file() {
                        let metadata = entry.metadata().unwrap();
                        snapshots.push((
                            entry.path().to_owned(),
                            metadata.ino(),
                            metadata.modified().unwrap(),
                            digest(entry.path()).unwrap(),
                        ));
                    }
                }
            }
            drop(store);
            let survivors = pool(&f, &c, slot);
            let mut report = Report::default();
            replace(&c, &c.pools[0], &survivors, slot, false, false, &mut report)
                .await
                .unwrap();
            assert_eq!(report.reconstructed_shards, 3); // live, empty, private
            assert!(report.unrecoverable_private_stripes >= 2);
            assert!(!target.join(RESTORE_PENDING).exists());
            assert!(!target.join(WORK).exists());
            drop(survivors);
            for (path, inode, modified, hash) in snapshots {
                let metadata = path.metadata().unwrap();
                assert_eq!(metadata.ino(), inode);
                assert_eq!(metadata.modified().unwrap(), modified);
                assert_eq!(digest(&path).unwrap(), hash);
            }
            let rebuilt = Pool::reopen_fixture(f.layout.clone(), &c.pools[0].roots).unwrap();
            crate::core::root_identity::verify_pool(&rebuilt, &c.pools[0].members).unwrap();
            let recovered = Store::new(vec![rebuilt], [17; 32], 16).unwrap();
            recovered.configure_read_repair(false);
            assert_eq!(
                recovered
                    .download(SCOPE, live.object_key.clone(), 0, None, Vec::new())
                    .await
                    .unwrap()
                    .0,
                data
            );
            assert!(
                recovered
                    .download(SCOPE, empty.object_key.clone(), 0, None, Vec::new())
                    .await
                    .unwrap()
                    .0
                    .is_empty()
            );
            assert!(matches!(
                recovered.stat(SCOPE, dead.object_key.clone()).await,
                Err(ObjectError::NotFound)
            ));
            assert!(matches!(
                recovered.finalize(SCOPE, dead.object_key.clone()).await,
                Err(ObjectError::NotFound)
            ));
            let status = recovered
                .upload_status(SCOPE, pending.object_key.clone(), 0, 16)
                .await
                .unwrap();
            assert_eq!(status.parts, 3);
            assert_eq!(status.missing_parts, vec![0, 2]);
            for part in [0, 2] {
                recovered
                    .write_part(
                        SCOPE,
                        pending.object_key.clone(),
                        part,
                        std::io::Cursor::new(vec![40 + part as u8; 12288]),
                    )
                    .await
                    .unwrap();
            }
            recovered
                .finalize(SCOPE, pending.object_key.clone())
                .await
                .unwrap();
            recovered
                .finalize(SCOPE, pending.object_key.clone())
                .await
                .unwrap();
            let expected = [vec![40; 12288], vec![41; 12288], vec![42; 12288]].concat();
            assert_eq!(
                recovered
                    .download(SCOPE, pending.object_key.clone(), 0, None, Vec::new())
                    .await
                    .unwrap()
                    .0,
                expected
            );
        }
    }

    #[tokio::test]
    async fn failed_activation_keeps_fence_resume_revalidates_inventory_and_rejects_other_owners() {
        let f = Fixture::new();
        let store = f.enroll();
        let live = put(&store, &[31; 17000]).await;
        let target = f.base.join("replacement");
        fs::create_dir(&target).unwrap();
        let mut c = config(&f, identities(&store), &target, 0);
        c.ensure_state_root().unwrap();
        drop(store);
        let survivors = pool(&f, &c, 0);
        // An injected extra file after reconstruction must prevent activation.
        // Resume must recompute its expected inventory, never trust old work.
        let marker = super::super::super::storage_pool::MemberMarker {
            format: "LINK-pool-v1".into(),
            layout: f.layout.clone(),
            members: c.pools[0].members.clone(),
            slot: 0,
        };
        let sources: Vec<_> = survivors
            .members
            .iter()
            .flatten()
            .map(|m| fs::canonicalize(&m.path).unwrap())
            .collect();
        let fence = serde_json::to_vec(&serde_json::json!({
            "format": "n1-offline-link-pool-v1",
            "operation": "replace",
            "member": marker,
            "sources": sources,
            "target": target
        }))
        .unwrap();
        prepare(&target, &fence, false, false).unwrap();
        fs::write(target.join("unexpected"), b"not replacement output").unwrap();
        assert!(
            replace(
                &c,
                &c.pools[0],
                &survivors,
                0,
                true,
                false,
                &mut Report::default()
            )
            .await
            .is_err()
        );
        assert!(target.join(RESTORE_PENDING).exists());
        assert!(Pool::reopen_fixture(f.layout.clone(), &c.pools[0].roots).is_err());
        c.pools[0].members.swap(0, 1);
        assert!(
            replace(
                &c,
                &c.pools[0],
                &survivors,
                0,
                true,
                false,
                &mut Report::default()
            )
            .await
            .is_err()
        );
        c.pools[0].members.swap(0, 1);
        fs::remove_file(target.join("unexpected")).unwrap();
        replace(
            &c,
            &c.pools[0],
            &survivors,
            0,
            true,
            false,
            &mut Report::default(),
        )
        .await
        .unwrap();
        drop(survivors);
        let rebuilt = Pool::reopen_fixture(f.layout.clone(), &c.pools[0].roots).unwrap();
        let recovered = Store::new(vec![rebuilt], [17; 32], 16).unwrap();
        assert_eq!(
            recovered
                .download(SCOPE, live.object_key.clone(), 0, None, Vec::new())
                .await
                .unwrap()
                .0,
            vec![31; 17000]
        );
    }

    #[tokio::test]
    async fn sparse_tib_uploads_rebuild_the_actual_tail_without_scanning_unwritten_space() {
        for near_end in [false, true] {
            let f = Fixture::new();
            let store = f.enroll();
            // Model existing sparse private headers and an optional written
            // tail. This fixture exercises replacement independently of upload
            // admission and allocates no TiB payload or product capacity bypass.
            let plan = n1_link::UploadPlan {
                identity: n1_link::Identity {
                    pool: f.layout.id,
                    generation: f.layout.generation,
                    token: [u8::from(near_end); 32],
                },
                total_bytes: 1_u64 << 40,
                part_bytes: 12288 * 1024,
                durability: Durability::Durable,
                expires_at: crate::core::store::now() + 3600,
            };
            plan.validate(&f.layout).unwrap();
            let object_key = store
                .signer
                .object_key(&SCOPE, plan.identity)
                .as_str()
                .to_owned();
            let fp = plan.identity.fingerprint();
            let header = ShardHeader::new(&f.layout, plan, 0).unwrap();
            let last = header.stripes() - 1;
            let bytes = vec![29; header.source_bytes_in_stripe(last).unwrap() as usize];
            let hash = header.source_hash(fp, last, &bytes).unwrap();
            let pieces = StripeCodec::new(&f.layout).unwrap().encode(&bytes).unwrap();
            for (index, piece) in pieces.iter().enumerate() {
                let member = store.pools[0].holder(fp, index as u16).unwrap();
                let (file, _) = member.open_stage(fp, true, true).unwrap();
                let header = ShardHeader::new(&f.layout, plan, index as u16).unwrap();
                crate::core::linux_io::pwrite_all_at(&*file, &header.encode(fp), 0).unwrap();
                if near_end {
                    let frame = shard::encode_frame(&header, fp, last, hash, piece).unwrap();
                    crate::core::linux_io::pwrite_all_at(
                        &*file,
                        &frame,
                        header.frame_offset(last).unwrap(),
                    )
                    .unwrap();
                }
                file.sync_all().unwrap();
            }
            let target = f.base.join("replacement");
            fs::create_dir(&target).unwrap();
            let c = config(&f, identities(&store), &target, 0);
            c.ensure_state_root().unwrap();
            drop(store);
            let survivors = pool(&f, &c, 0);
            let mut report = Report::default();
            tokio::time::timeout(
                std::time::Duration::from_secs(3),
                replace(&c, &c.pools[0], &survivors, 0, false, false, &mut report),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(report.unrecoverable_private_stripes > 1_000_000);
            let fp = n1_link::ObjectKey::inspect(&object_key)
                .unwrap()
                .fingerprint();
            assert_eq!(
                target.join(fp.staging_path()).metadata().unwrap().len(),
                if near_end { header.seal_offset() } else { 128 }
            );
            if near_end {
                let output = open_regular(&target.join(fp.staging_path())).unwrap();
                let mut actual = vec![0; header.record_bytes() as usize];
                crate::core::linux_io::pread_exact_at(
                    &output,
                    &mut actual,
                    header.frame_offset(last).unwrap(),
                )
                .unwrap();
                let mut expected = header.clone();
                expected.shard = f.layout.shard_at_slot(fp, 0).unwrap();
                let decoded = shard::decode_frame(&expected, fp, last, &actual).unwrap();
                assert_eq!(decoded.source_hash, hash);
                assert_eq!(decoded.payload, &pieces[usize::from(expected.shard)]);
            }
            drop(survivors);
            let recovered = Store::new(
                vec![Pool::reopen_fixture(f.layout.clone(), &c.pools[0].roots).unwrap()],
                [17; 32],
                16,
            )
            .unwrap();
            let status = recovered
                .upload_status(SCOPE, object_key, 0, 4)
                .await
                .unwrap();
            assert_eq!(status.missing_parts, vec![0, 1, 2, 3]);
            assert!(!status.finalized);
        }
    }

    #[tokio::test]
    async fn partially_published_objects_remain_resumable_without_being_falsely_finalized() {
        let f = Fixture::new();
        let store = f.enroll();
        let bytes = vec![43; 37001];
        let live = put(&store, &bytes).await;
        let fp = store
            .inspect_upload(SCOPE, &live.object_key.clone())
            .await
            .unwrap()
            .identity
            .fingerprint();
        // One surviving final shard, the others still staged, models a crash
        // between member promotions. Fewer than K final paths prove no object.
        for slot in [2, 3] {
            let index = f.layout.shard_at_slot(fp, slot).unwrap();
            let member = store.pools[0].members[slot].as_ref().unwrap();
            member
                .directory(Path::new(&format!(".uploads/{fp}")), true, true)
                .unwrap();
            fs::rename(
                member.path.join(fp.shard_path(index)),
                member.path.join(fp.staging_path()),
            )
            .unwrap();
        }
        let target = f.base.join("replacement");
        fs::create_dir(&target).unwrap();
        let c = config(&f, identities(&store), &target, 0);
        c.ensure_state_root().unwrap();
        drop(store);
        let survivors = pool(&f, &c, 0);
        replace(
            &c,
            &c.pools[0],
            &survivors,
            0,
            false,
            false,
            &mut Report::default(),
        )
        .await
        .unwrap();
        assert!(target.join(fp.staging_path()).exists());
        assert!(
            !target
                .join(fp.shard_path(f.layout.shard_at_slot(fp, 0).unwrap()))
                .exists()
        );
        drop(survivors);
        let recovered = Store::new(
            vec![Pool::reopen_fixture(f.layout.clone(), &c.pools[0].roots).unwrap()],
            [17; 32],
            16,
        )
        .unwrap();
        assert!(
            recovered
                .stat(SCOPE, live.object_key.clone())
                .await
                .is_err()
        );
        let status = recovered
            .upload_status(SCOPE, live.object_key.clone(), 0, 16)
            .await
            .unwrap();
        assert!(!status.finalized);
        assert!(status.missing_parts.is_empty());
        recovered
            .finalize(SCOPE, live.object_key.clone())
            .await
            .unwrap();
        recovered
            .finalize(SCOPE, live.object_key.clone())
            .await
            .unwrap();
        assert_eq!(
            recovered
                .download(SCOPE, live.object_key.clone(), 0, None, Vec::new())
                .await
                .unwrap()
                .0,
            bytes
        );
    }

    #[tokio::test]
    async fn a_corrupt_survivor_cannot_certify_a_replacement_or_be_rewritten_by_it() {
        use std::os::unix::fs::FileExt;
        let f = Fixture::new();
        let store = f.enroll();
        let live = put(&store, &[32; 37001]).await;
        let fp = store
            .inspect_upload(SCOPE, &live.object_key.clone())
            .await
            .unwrap()
            .identity
            .fingerprint();
        let shard = f.layout.shard_at_slot(fp, 1).unwrap();
        let source = f.paths[1].join(fp.shard_path(shard));
        let file = OpenOptions::new().write(true).open(&source).unwrap();
        file.write_at(
            &[123],
            (shard::HEADER_BYTES + shard::FRAME_HEADER_BYTES) as u64,
        )
        .unwrap();
        file.sync_all().unwrap();
        drop(file);
        let corrupted = digest(&source).unwrap();
        let target = f.base.join("replacement");
        fs::create_dir(&target).unwrap();
        let c = config(&f, identities(&store), &target, 0);
        c.ensure_state_root().unwrap();
        drop(store);
        let survivors = pool(&f, &c, 0);
        assert!(
            replace(
                &c,
                &c.pools[0],
                &survivors,
                0,
                false,
                false,
                &mut Report::default()
            )
            .await
            .is_err()
        );
        assert_eq!(digest(&source).unwrap(), corrupted);
        assert!(!target.join(".n1-arc-pool.json").exists());
        assert!(Pool::reopen_fixture(f.layout.clone(), &c.pools[0].roots).is_err());
    }
}
