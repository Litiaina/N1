//! Explicit offline pool maintenance. No namespace journal, object cache or
//! online admission exception: ordinary engine operations never enter here.
//! Healthy clones copy unchanged bytes; replacement reconstructs only its slot.
use crate::core::backup::{Result, invalid};
use crate::core::disk_manager::{ObjectMetadata, ObjectPathManager, PartMetadata, ShardInfo};
use crate::core::instance_lease::{InstanceLease, LEASE_FILE, RESTORE_PENDING};
use crate::core::{
    root_identity,
    storage_pool::{self, StoragePool},
};
use crate::namespace::flat;
use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, symlink};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use walkdir::{DirEntry, WalkDir};

const WORK: &str = ".n1-pool-work";
const MAX_METADATA: u64 = 64 * 1024 * 1024;

#[derive(Parser)]
#[command(
    about = "Offline N1 pool verification, byte-exact cloning and enrolled-member replacement"
)]
struct Cli {
    #[command(subcommand)]
    command: Operation,
}
#[derive(Args)]
struct Selection {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    pool: String,
    /// Zero-based logical member slot, identified by the member descriptor.
    #[arg(long)]
    member: usize,
}
#[derive(Subcommand)]
enum Operation {
    /// Verify enrolled identities, namespace/metadata replicas and every recorded piece.
    Verify {
        #[arg(long)]
        config: PathBuf,
        /// Verify only this pool (other pools are not leased or scanned).
        #[arg(long)]
        pool: Option<String>,
        /// Stable fragment ID; omit to verify every fragment in the selected pools.
        #[arg(long)]
        fragment: Option<String>,
    },
    /// Fence an empty clone target before copying manually. Then run verify-copy.
    PrepareCopy {
        #[command(flatten)]
        selection: Selection,
        #[arg(long)]
        copy_root: PathBuf,
    },
    /// Verify a complete manual copy; synchronize it and remove only its matching fence.
    VerifyCopy {
        #[command(flatten)]
        selection: Selection,
        #[arg(long)]
        copy_root: PathBuf,
    },
    /// Copy unchanged member contents to another device for verified cutover.
    Clone {
        #[command(flatten)]
        selection: Selection,
        #[arg(long)]
        copy_root: PathBuf,
        #[arg(long)]
        resume: bool,
    },
    /// Rebuild a blank replacement at its configured location; activate only after verification.
    Replace {
        #[command(flatten)]
        selection: Selection,
        #[arg(long)]
        resume: bool,
        /// Explicit authorization; never initializes replacement storage automatically.
        #[arg(long, required = true)]
        authorize: bool,
    },
}

#[derive(Default, Serialize)]
pub(crate) struct Report {
    pub files: u64,
    pub bytes: u64,
    pub versions: u64,
    pub pointers: u64,
    reconstructed_shards: u64,
    elapsed_ms: f64,
    peak_rss_kib: i64,
    final_open_fds: usize,
}
struct Context {
    control: PathBuf,
    pools: Vec<StoragePool>,
    _lease: InstanceLease,
}

impl Context {
    fn open(
        config: &Path,
        replacement: Option<(&str, usize)>,
        extra: Option<&Path>,
    ) -> Result<Self> {
        Self::open_scoped(config, replacement, extra, None)
    }

    fn open_scoped(
        config: &Path,
        replacement: Option<(&str, usize)>,
        extra: Option<&Path>,
        selected_pool: Option<&str>,
    ) -> Result<Self> {
        let config = ini::Ini::load_from_file(config)?;
        let control = fs::canonicalize(
            config
                .section(Some("disk_config"))
                .and_then(|s| s.get("fs_storage_path"))
                .ok_or_else(|| invalid("missing fs_storage_path"))?,
        )?;
        let pools =
            storage_pool::bind_locations(storage_pool::parse(&config).map_err(invalid)?, &control)
                .map_err(invalid)?;
        let enrolled: Vec<StoragePool> = serde_json::from_slice(&read_regular(
            &control.join(".n1/storage-pools.json"),
            MAX_METADATA,
        )?)?;
        storage_pool::validate_catalog(&enrolled, &pools).map_err(invalid)?;
        if selected_pool.is_some_and(|id| !pools.iter().any(|pool| pool.id == id)) {
            return Err(invalid("unknown storage pool"));
        }
        let mut paths = vec![control.clone()];
        for pool in pools
            .iter()
            .filter(|pool| selected_pool.is_none_or(|id| pool.id == id))
        {
            let mut devices = BTreeSet::new();
            for (slot, root) in pool.roots.iter().enumerate() {
                let replacing = replacement == Some((pool.id.as_str(), slot));
                let metadata = fs::symlink_metadata(root)?;
                if !metadata.is_dir() || !devices.insert(metadata.dev()) {
                    return Err(invalid(
                        "pool members must be directories on distinct mounted filesystems",
                    ));
                }
                if !replacing {
                    root_identity::verify_registered_identity(&control, root).map_err(invalid)?;
                    if root.join(RESTORE_PENDING).try_exists()? {
                        return Err(invalid("source member has unfinished offline maintenance"));
                    }
                    check_pool_marker(pool, root)?;
                    // A source never receives a newly fabricated lease/identity.
                    if !root.join(LEASE_FILE).is_file() {
                        return Err(invalid("source lease is not initialized"));
                    }
                }
                paths.push(root.clone());
            }
        }
        if let Some(extra) = extra {
            let extra = fs::canonicalize(extra)?;
            if paths
                .iter()
                .any(|p| p.starts_with(&extra) || extra.starts_with(p))
            {
                return Err(invalid("copy target overlaps configured storage/control"));
            }
            paths.push(extra);
        }
        let lease = InstanceLease::acquire(&paths, false)?;
        // Recheck after obtaining exclusion, including the enrolled catalog.
        let current: Vec<StoragePool> = serde_json::from_slice(&read_regular(
            &control.join(".n1/storage-pools.json"),
            MAX_METADATA,
        )?)?;
        storage_pool::validate_catalog(&current, &pools).map_err(invalid)?;
        storage_pool::STORAGE_POOLS
            .set(pools.clone())
            .map_err(|_| invalid("offline pool locations already initialized"))?;
        Ok(Self {
            control,
            pools: pools
                .into_iter()
                .filter(|pool| selected_pool.is_none_or(|id| pool.id == id))
                .collect(),
            _lease: lease,
        })
    }
    fn selected(&self, selection: &Selection) -> Result<&StoragePool> {
        self.pools
            .iter()
            .find(|p| p.id == selection.pool && selection.member < p.width())
            .ok_or_else(|| invalid("unknown pool/member slot"))
    }
}

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
        .custom_flags(libc::O_NOFOLLOW)
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
                    | crate::core::backup::online::VIEW_DIR
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
fn check_pool_marker(pool: &StoragePool, root: &Path) -> Result<()> {
    if let Some(member) = storage_pool::read_member(root).map_err(invalid)? {
        if !member.pool.same_geometry(pool)
            || member.pool.members != pool.members
            || member.pool.address_roots() != pool.address_roots()
            || pool
                .roots
                .iter()
                .position(|r| r == root)
                .is_some_and(|slot| slot != member.member_slot)
        {
            return Err(invalid("member pool identity/geometry/slot mismatch"));
        }
        return Ok(());
    }
    let actual: StoragePool =
        serde_json::from_slice(&read_regular(&root.join(".n1-pool.json"), MAX_METADATA)?)?;
    let mut expected = pool.clone();
    expected.sealed = false;
    if actual != expected {
        return Err(invalid("member pool identity/geometry/order mismatch"));
    }
    Ok(())
}
fn checksum(path: &Path, expected: &ShardInfo) -> Result<()> {
    if expected.checksum.len() != 8 || !expected.checksum.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(invalid("recorded piece has no valid integrity checksum"));
    }
    let mut file = open_regular(path)?;
    if file.metadata()?.len() != expected.size_bytes {
        return Err(invalid("piece length mismatch"));
    }
    let mut digest = crc32fast::Hasher::new();
    let mut buffer = vec![0u8; 256 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    if digest.finalize() != u32::from_str_radix(&expected.checksum, 16)? {
        return Err(invalid("piece checksum mismatch"));
    }
    Ok(())
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
fn piece_path(pool: &StoragePool, shard: &ShardInfo) -> Result<PathBuf> {
    let root = pool
        .roots
        .get(shard.shard_index)
        .ok_or_else(|| invalid("piece slot outside pool"))?;
    let path = storage_pool::resolve_address_in(std::slice::from_ref(pool), &shard.absolute_path)
        .map_err(invalid)?;
    let rel = path
        .strip_prefix(root)
        .map_err(|_| invalid("piece address outside its enrolled slot"))?;
    if rel.file_name().and_then(|n| n.to_str()) != Some(shard.file_name.as_str()) {
        return Err(invalid("piece filename/address mismatch"));
    }
    safe_join(root, rel)
}
fn verify_part(pool: &StoragePool, part: &PartMetadata, missing: Option<usize>) -> Result<()> {
    if part.shards.len() != pool.width() {
        return Err(invalid("part geometry differs from pool"));
    }
    let mut slots = BTreeSet::new();
    for shard in &part.shards {
        if !slots.insert(shard.shard_index)
            || shard.is_parity != (shard.shard_index >= usize::from(pool.data_shards))
        {
            return Err(invalid("duplicate/invalid EC member slot"));
        }
        let path = piece_path(pool, shard)?;
        if missing != Some(shard.shard_index) {
            checksum(&path, shard)?;
        }
    }
    Ok(())
}
fn version(pool: &StoragePool, rel: &Path, bytes: &[u8]) -> Result<ObjectMetadata> {
    let meta: ObjectMetadata = rmp_serde::from_slice(bytes)?;
    if meta.data_shards != pool.data_shards
        || meta.parity_shards != pool.parity_shards
        || meta.version_id.parse::<ulid::Ulid>().is_err()
        || meta.parts.is_empty()
    {
        return Err(invalid("invalid committed version or pool geometry"));
    }
    let parts: Vec<_> = rel.iter().filter_map(|s| s.to_str()).collect();
    if parts.len() != 6
        || parts[1] != ".objects"
        || parts[4] != format!("@o_{}", meta.object_key)
        || !parts[5].starts_with(&format!("{}.", meta.version_id))
    {
        return Err(invalid("version metadata identity/location mismatch"));
    }
    let fanout = ObjectPathManager::object_fanout_segments(&meta.object_key);
    if parts[2] != fanout[0] || parts[3] != fanout[1] {
        return Err(invalid("object fanout mismatch"));
    }
    let directory = rel.parent().unwrap().join(&meta.version_id);
    for part in &meta.parts {
        for shard in &part.shards {
            let path = piece_path(pool, shard)?;
            let relative = path.strip_prefix(&pool.roots[shard.shard_index])?;
            if relative.parent() != Some(directory.as_path()) {
                return Err(invalid("piece belongs to another object/version"));
            }
        }
    }
    Ok(meta)
}
fn is_version(rel: &Path) -> bool {
    rel.extension().is_some_and(|v| v == "ndx")
        && rel.components().any(|c| c.as_os_str() == ".objects")
}
fn is_upload_part(rel: &Path) -> bool {
    rel.components().any(|c| c.as_os_str() == ".uploads")
        && rel
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("part_") && n.ends_with(".ndx"))
}
fn pointer_placement(
    pool: &StoragePool,
    rel: &Path,
    bytes: &[u8],
    path: &Path,
    known_pools: &[StoragePool],
) -> Result<Vec<PathBuf>> {
    let pointer = flat::read(path)
        .map_err(|e| invalid(e.to_string()))?
        .ok_or_else(|| invalid("missing NCX"))?;
    if pointer.pool_id != pool.id {
        return Err(invalid("NCX belongs to a different pool"));
    }
    let components: Vec<_> = rel.iter().filter_map(|s| s.to_str()).collect();
    let fragment = components
        .first()
        .and_then(|s| s.strip_prefix("@f_"))
        .ok_or_else(|| invalid("invalid NCX fragment"))?;
    let offset = match components.get(1).copied() {
        Some(".namespace") => 2,
        Some(".trash") => 3,
        _ => return Err(invalid("NCX outside live/trash namespace")),
    };
    let encoded: PathBuf = components.iter().skip(offset).collect();
    let key = flat::decoded_key(&encoded).ok_or_else(|| invalid("invalid encoded NCX key"))?;
    let holders = pool.namespace_roots(fragment, &key);
    let fanout = ObjectPathManager::object_fanout_segments(&pointer.object_id);
    let object = pool.roots[0]
        .join(components[0])
        .join(".objects")
        .join(&fanout[0])
        .join(&fanout[1])
        .join(format!("@o_{}", pointer.object_id));
    // At least one committed version is required; dangling pointers cannot be
    // certified by merely copying the small NCX successfully.
    // NCX's pool owns its replica placement, not every future version of its
    // stable object. After expansion new versions may live in another pool.
    // A local-only lookup falsely labels that valid namespace binding dangling
    // after older local versions are reclaimed. Use the same stable fanout on
    // known pools; the selected pool's content scan still verifies only its own
    // NDX/pieces, while full offline verification checks every selected pool.
    let exists = pool
        .roots
        .iter()
        .chain(
            known_pools
                .iter()
                .filter(|other| other.id != pool.id)
                .flat_map(|other| &other.roots),
        )
        .any(|root| {
            let candidate = root.join(object.strip_prefix(&pool.roots[0]).unwrap());
            fs::read_dir(candidate).is_ok_and(|entries| {
                entries.filter_map(|e| e.ok()).any(|e| {
                    e.path().extension().is_some_and(|x| x == "ndx")
                        && e.file_type().is_ok_and(|t| t.is_file())
                })
            })
        });
    if !exists || bytes.is_empty() {
        return Err(invalid("NCX has no committed object metadata"));
    }
    Ok(holders)
}

fn verify_replicas(
    pool: &StoragePool,
    missing: Option<usize>,
    rel: &Path,
    bytes: &[u8],
) -> Result<()> {
    for (slot, root) in pool.roots.iter().enumerate() {
        if Some(slot) != missing && read_regular(&safe_join(root, rel)?, MAX_METADATA)? != bytes {
            return Err(invalid(
                "missing or divergent offline metadata/receipt replica",
            ));
        }
    }
    Ok(())
}

fn verify_pool(pool: &StoragePool, missing: Option<usize>, report: &mut Report) -> Result<()> {
    verify_pool_scoped(pool, missing, None, report)
}

/// Read-only content inspection shared by online diagnostics and offline
/// verification. Online callers must label this a live scan, not a snapshot.
pub(crate) fn verify_pool_scoped(
    pool: &StoragePool,
    missing: Option<usize>,
    fragment: Option<&str>,
    report: &mut Report,
) -> Result<()> {
    verify_pool_scoped_on(pool, missing, fragment, storage_pool::configured(), report)
}

fn verify_pool_scoped_on(
    pool: &StoragePool,
    missing: Option<usize>,
    fragment: Option<&str>,
    known_pools: &[StoragePool],
    report: &mut Report,
) -> Result<()> {
    if fragment.is_some_and(|id| id.parse::<ulid::Ulid>().is_err()) {
        return Err(invalid("expected a stable fragment ULID"));
    }
    let primary = (0..pool.width())
        .find(|s| Some(*s) != missing)
        .ok_or_else(|| invalid("no surviving member"))?;
    for (slot, root) in pool
        .roots
        .iter()
        .enumerate()
        .filter(|(s, _)| Some(*s) != missing)
    {
        let scan_root = fragment
            .map(|id| root.join(format!("@f_{id}")))
            .unwrap_or_else(|| root.clone());
        // Unlike a full-pool walk, min_depth(1) on this directory still includes
        // its .fragment.ndx. Missing fragment roots must not become an empty pass.
        if fragment.is_some() && !fs::symlink_metadata(&scan_root).is_ok_and(|meta| meta.is_dir()) {
            return Err(invalid("fragment is absent from a selected pool member"));
        }
        for entry in walk(&scan_root) {
            let entry = entry?;
            let rel = relative(entry.path().strip_prefix(root)?)?;
            // Never follow a symlink standing in for the scan root itself.
            if entry.file_type().is_symlink() {
                let target = fs::read_link(entry.path())?;
                if !rel.starts_with(".fragment")
                    || target.components().count() != 2
                    || !target.to_string_lossy().starts_with("../@f_")
                {
                    return Err(invalid("unsupported storage symlink"));
                }
                let fragment = root.join(".fragment").join(&target).join(".fragment.ndx");
                if !fragment.is_file() {
                    return Err(invalid("dangling fragment alias"));
                }
            } else if entry.file_type().is_file() {
                report.files += 1;
                report.bytes += entry.metadata()?.len();
                if is_version(rel) {
                    let bytes = read_regular(entry.path(), MAX_METADATA)?;
                    let meta = version(pool, rel, &bytes)?;
                    for (other_slot, other) in pool.roots.iter().enumerate() {
                        if Some(other_slot) != missing
                            && read_regular(&safe_join(other, rel)?, MAX_METADATA)? != bytes
                        {
                            return Err(invalid("committed metadata replicas differ"));
                        }
                    }
                    if slot == primary {
                        for part in &meta.parts {
                            verify_part(pool, part, missing)?;
                        }
                        report.versions += 1;
                    }
                } else if is_upload_part(rel) {
                    let bytes = read_regular(entry.path(), MAX_METADATA)?;
                    let part: PartMetadata = rmp_serde::from_slice(&bytes)?;
                    for (other_slot, other) in pool.roots.iter().enumerate() {
                        if Some(other_slot) != missing
                            && read_regular(&safe_join(other, rel)?, MAX_METADATA)? != bytes
                        {
                            return Err(invalid("durable part metadata replicas differ"));
                        }
                    }
                    if slot == primary {
                        verify_part(pool, &part, missing)?;
                    }
                } else if entry.file_name() == "manifest.ndx"
                    && rel.components().any(|c| c.as_os_str() == ".uploads")
                {
                    let bytes = read_regular(entry.path(), MAX_METADATA)?;
                    verify_replicas(pool, missing, rel, &bytes)?;
                    let manifest = crate::core::index_store::offline_session_manifest(&bytes)
                        .map_err(|e| invalid(e.to_string()))?;
                    if rel
                        .parent()
                        .and_then(|p| p.file_name())
                        .and_then(|s| s.to_str())
                        != Some(manifest.version_id.as_str())
                    {
                        return Err(invalid("durable manifest version/location mismatch"));
                    }
                    if manifest.data_shards != pool.data_shards
                        || manifest.parity_shards != pool.parity_shards
                        || manifest
                            .placement_roots
                            .iter()
                            .map(|root| {
                                storage_pool::resolve_address_in(std::slice::from_ref(pool), root)
                                    .map_err(invalid)
                            })
                            .collect::<Result<Vec<_>>>()?
                            != pool.roots
                    {
                        return Err(invalid(
                            "durable upload geometry/placement differs from pool",
                        ));
                    }
                    if slot == primary {
                        let fragment = rel
                            .iter()
                            .next()
                            .and_then(|s| s.to_str())
                            .and_then(|s| s.strip_prefix("@f_"))
                            .ok_or_else(|| invalid("invalid upload fragment"))?;
                        for part in crate::core::index_store::offline_session_parts(
                            entry.path().parent().unwrap(),
                            manifest,
                            fragment,
                        )
                        .map_err(|e| invalid(e.to_string()))?
                        {
                            verify_part(pool, &part.map_err(|e| invalid(e.to_string()))?, missing)?;
                        }
                    }
                } else if rel.extension().is_some_and(|s| s == "receipt")
                    && rel.components().any(|c| c.as_os_str() == ".uploads")
                {
                    let bytes = read_regular(entry.path(), MAX_METADATA)?;
                    if !bytes.is_empty() {
                        return Err(invalid("durable receipt is not an empty integrity marker"));
                    }
                    verify_replicas(pool, missing, rel, &bytes)?;
                } else if rel.extension().is_some_and(|s| s == "ncx") {
                    let bytes = read_regular(entry.path(), MAX_METADATA)?;
                    let holders = pointer_placement(pool, rel, &bytes, entry.path(), known_pools)?;
                    if !holders.contains(root) {
                        return Err(invalid("NCX on an unexpected member slot"));
                    }
                    for holder in holders {
                        if missing.is_none_or(|s| holder != pool.roots[s])
                            && read_regular(&safe_join(&holder, rel)?, MAX_METADATA)? != bytes
                        {
                            return Err(invalid("missing/divergent NCX replica"));
                        }
                    }
                    report.pointers += 1;
                } else if entry.file_name() == ".fragment.ndx" {
                    let bytes = read_regular(entry.path(), MAX_METADATA)?;
                    verify_replicas(pool, missing, rel, &bytes)?;
                    let meta: crate::core::index_store::FragmentMeta =
                        rmp_serde::from_slice(&bytes)?;
                    if meta.engine_format_version != 4
                        || rel.iter().next().unwrap()
                            != format!("@f_{}", meta.stable_fragment_id).as_str()
                    {
                        return Err(invalid("fragment metadata identity/format mismatch"));
                    }
                }
            } else if !entry.file_type().is_dir() {
                return Err(invalid("unsupported storage file type"));
            }
        }
    }
    Ok(())
}

/// Shared full-integrity gate for offline capture/restore. Caller holds all
/// member leases. Anonymous low-level backup fixtures have no N1 topology.
pub(crate) fn verify_backup_roots(roots: &[crate::core::backup::format::Root]) -> Result<()> {
    if roots.iter().all(|r| r.member.is_none()) {
        return Ok(());
    }
    let mut ids = BTreeSet::new();
    let mut pools = Vec::new();
    for root in roots {
        let member = root
            .member
            .as_ref()
            .ok_or_else(|| invalid("missing backup member identity"))?;
        if !ids.insert(member.pool_id.clone()) {
            continue;
        }
        let width = usize::from(member.data_shards) + usize::from(member.parity_shards);
        let mut members: Vec<_> = roots
            .iter()
            .filter(|r| {
                r.member
                    .as_ref()
                    .is_some_and(|m| m.pool_id == member.pool_id)
            })
            .collect();
        members.sort_by_key(|r| r.member.as_ref().unwrap().slot);
        if members.len() != width
            || members.iter().enumerate().any(|(slot, r)| {
                let m = r.member.as_ref().unwrap();
                m.slot != slot
                    || m.generation != member.generation
                    || m.data_shards != member.data_shards
                    || m.parity_shards != member.parity_shards
            })
        {
            return Err(invalid("incomplete or conflicting backup pool membership"));
        }
        let mut pool = StoragePool {
            id: member.pool_id.clone(),
            generation: member.generation,
            data_shards: member.data_shards,
            parity_shards: member.parity_shards,
            roots: members.iter().map(|r| PathBuf::from(&r.path)).collect(),
            members: Vec::new(),
            legacy_roots: Vec::new(),
            sealed: false,
        };
        if let Some(member) = storage_pool::read_member(&pool.roots[0]).map_err(invalid)? {
            pool.members = member.pool.members.clone();
            pool.legacy_roots = member.pool.address_roots().to_vec();
        }
        for root in &pool.roots {
            check_pool_marker(&pool, root)?;
        }
        pools.push(pool);
    }
    for pool in &pools {
        verify_pool_scoped_on(pool, None, None, &pools, &mut Report::default())?;
    }
    Ok(())
}

fn marker(pool: &StoragePool, slot: usize, operation: &str, target: &Path) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(
        &serde_json::json!({"format":"n1-offline-pool-v1",
        "operation":operation,"pool":pool,"slot":slot,"target":target}),
    )?)
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
        if !populated_copy
            && fs::read_dir(target)?.any(|e| e.is_ok_and(|e| e.file_name() != LEASE_FILE))
        {
            return Err(invalid("maintenance requires an empty target"));
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
        let path = target.join(relative(rel)?);
        let link = fs::read_link(source)?;
        safe_join(
            target,
            rel.parent().ok_or_else(|| invalid("invalid alias path"))?,
        )?;
        fs::create_dir_all(path.parent().unwrap())?;
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_symlink() && fs::read_link(&path)? == link => {}
            Ok(_) => return Err(invalid("conflicting copied alias")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => symlink(link, path)?,
            Err(e) => return Err(e.into()),
        }
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
    // not add fsyncs to ordinary NCX namespace mutation.
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
    for entry in walk(target) {
        let entry = entry?;
        let rel = entry.path().strip_prefix(target)?;
        if rel == Path::new(".n1-root-id") {
            continue;
        }
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

async fn rebuild_part(
    pool: &StoragePool,
    slot: usize,
    part: PartMetadata,
    stripe: usize,
    report: &mut Report,
) -> Result<()> {
    verify_part(pool, &part, Some(slot))?;
    let shard = part
        .shards
        .iter()
        .find(|s| s.shard_index == slot)
        .ok_or_else(|| invalid("missing replacement slot"))?;
    let path = piece_path(pool, shard)?;
    expect(
        &pool.roots[slot],
        path.strip_prefix(&pool.roots[slot])?,
        "file",
    )?;
    if let Some(parent) = path.parent().filter(|p| p.is_dir()) {
        let prefix = format!("{}.tmp.repair.", shard.file_name);
        for entry in fs::read_dir(parent)? {
            let entry = entry?;
            if entry
                .file_name()
                .to_str()
                .and_then(|n| n.strip_prefix(&prefix))
                .is_some_and(|suffix| suffix.parse::<ulid::Ulid>().is_ok())
            {
                if !entry.file_type()?.is_file() {
                    return Err(invalid("invalid interrupted repair temporary"));
                }
                fs::remove_file(entry.path())?;
            }
        }
    }
    if checksum(&path, shard).is_err() {
        if stripe == 0 || stripe % 4096 != 0 {
            return Err(invalid("invalid recorded stripe geometry"));
        }
        crate::core::recovery::repair_entire_shard(Arc::new(part.clone()), slot, stripe)
            .await
            .map_err(|e| invalid(e.to_string()))?;
        checksum(&path, shard)?;
        report.reconstructed_shards += 1;
    }
    Ok(())
}

async fn replace(
    context: &Context,
    pool: &StoragePool,
    slot: usize,
    resume: bool,
    report: &mut Report,
) -> Result<()> {
    if pool.parity_shards == 0 {
        return Err(invalid(
            "lost 1+0 member requires a verified backup; use clone for a healthy source",
        ));
    }
    let target = &pool.roots[slot];
    // Context bound and validated the accepted logical member already. A blank
    // replacement at a new location deliberately has no path-keyed identity.
    if pool.members.get(slot).is_none_or(|id| id.is_empty()) {
        return Err(invalid("replacement slot has no accepted member identity"));
    }
    let marker = marker(pool, slot, "replace", target)?;
    prepare(target, &marker, resume, false)?;
    storage_pool::write_member(pool, slot).map_err(invalid)?;
    fs::create_dir_all(safe_join(target, Path::new(WORK))?)?;
    let inventory = safe_join(target, &Path::new(WORK).join("inventory"))?;
    if inventory.try_exists()? {
        fs::remove_dir_all(inventory)?;
    }
    expect(target, Path::new(".n1-pool.json"), "file")?;
    // Require all surviving peers to be complete and checksum-valid. Do not
    // certify a partial inventory or guess through conflicting metadata.
    verify_pool(pool, Some(slot), &mut Report::default())?;
    for (peer_slot, peer) in pool.roots.iter().enumerate().filter(|(s, _)| *s != slot) {
        for entry in walk(peer) {
            let entry = entry?;
            let rel = relative(entry.path().strip_prefix(peer)?)?;
            if rel == Path::new(".n1-root-id") || entry.file_type().is_dir() {
                continue;
            }
            if entry.file_type().is_symlink() || entry.file_name() == ".fragment.ndx" {
                expect(
                    target,
                    rel,
                    if entry.file_type().is_symlink() {
                        "link"
                    } else {
                        "file"
                    },
                )?;
                copy_entry(entry.path(), target, rel)?;
            } else if rel.extension().is_some_and(|s| s == "ncx") {
                let bytes = read_regular(entry.path(), MAX_METADATA)?;
                if pointer_placement(pool, rel, &bytes, entry.path(), storage_pool::configured())?
                    .contains(target)
                {
                    expect(target, rel, "file")?;
                    copy_entry(entry.path(), target, rel)?;
                }
            } else if is_version(rel) || rel.components().any(|c| c.as_os_str() == ".uploads") {
                expect(target, rel, "file")?;
                copy_entry(entry.path(), target, rel)?;
                // A single surviving metadata copy drives each reconstruction;
                // subsequent replicas are compared, not re-encoded repeatedly.
                if peer_slot == (0..pool.width()).find(|s| *s != slot).unwrap() {
                    if is_version(rel) {
                        let meta = version(pool, rel, &read_regular(entry.path(), MAX_METADATA)?)?;
                        for part in meta.parts {
                            rebuild_part(pool, slot, part, meta.stripe_size_bytes, report).await?;
                        }
                    } else if is_upload_part(rel) {
                        let part = rmp_serde::from_slice::<PartMetadata>(&read_regular(
                            entry.path(),
                            MAX_METADATA,
                        )?)?;
                        let manifest =
                            crate::core::index_store::offline_session_manifest(&read_regular(
                                &safe_join(peer, &rel.parent().unwrap().join("manifest.ndx"))?,
                                MAX_METADATA,
                            )?)
                            .map_err(|e| invalid(e.to_string()))?;
                        rebuild_part(pool, slot, part, manifest.stripe_size_bytes, report).await?;
                    } else if entry.file_name() == "manifest.ndx" {
                        let manifest = crate::core::index_store::offline_session_manifest(
                            &read_regular(entry.path(), MAX_METADATA)?,
                        )
                        .map_err(|e| invalid(e.to_string()))?;
                        let stripe = manifest.stripe_size_bytes;
                        let fragment = rel
                            .iter()
                            .next()
                            .and_then(|s| s.to_str())
                            .and_then(|s| s.strip_prefix("@f_"))
                            .ok_or_else(|| invalid("invalid upload fragment"))?;
                        for part in crate::core::index_store::offline_session_parts(
                            entry.path().parent().unwrap(),
                            manifest,
                            fragment,
                        )
                        .map_err(|e| invalid(e.to_string()))?
                        {
                            rebuild_part(
                                pool,
                                slot,
                                part.map_err(|e| invalid(e.to_string()))?,
                                stripe,
                                report,
                            )
                            .await?;
                        }
                    }
                }
            }
        }
    }
    verify_pool(pool, None, report)?;
    audit_replacement(target)?;
    // Root identity becomes trusted ONLY after the complete pool is verified.
    root_identity::restore_verified_marker(&context.control, target).map_err(invalid)?;
    complete(target)?;
    Ok(())
}

pub fn run_from(args: Vec<std::ffi::OsString>) -> Result<()> {
    let started = std::time::Instant::now();
    let cli = Cli::parse_from(args);
    let mut report = Report::default();
    match cli.command {
        Operation::Verify {
            config,
            pool,
            fragment,
        } => {
            let context = Context::open_scoped(&config, None, None, pool.as_deref())?;
            for pool in &context.pools {
                verify_pool_scoped(pool, None, fragment.as_deref(), &mut report)?;
            }
        }
        operation @ (Operation::PrepareCopy { .. }
        | Operation::VerifyCopy { .. }
        | Operation::Clone { .. }) => {
            let (selection, copy_root, prepare_only, manual, resume) = match operation {
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
            let copy_root = fs::canonicalize(copy_root)?;
            let context = Context::open(&selection.config, None, Some(&copy_root))?;
            let pool = context.selected(&selection)?;
            let source = &pool.roots[selection.member];
            if fs::metadata(source)?.dev() == fs::metadata(&copy_root)?.dev() {
                return Err(invalid(
                    "clone destination must be on another mounted filesystem",
                ));
            }
            let fence = marker(pool, selection.member, "clone", &copy_root)?;
            if prepare_only {
                prepare(&copy_root, &fence, false, false)?;
            } else {
                if manual {
                    let pending = copy_root.join(RESTORE_PENDING).try_exists()?;
                    prepare(&copy_root, &fence, pending, true)?;
                } else {
                    prepare(&copy_root, &fence, resume, false)?;
                }
                verify_pool(pool, None, &mut Report::default())?;
                if !manual {
                    fs::create_dir_all(copy_root.join(WORK))?;
                    for entry in walk(source) {
                        let entry = entry?;
                        copy_entry(entry.path(), &copy_root, entry.path().strip_prefix(source)?)?;
                    }
                }
                verify_copy(source, &copy_root, &mut report)?;
                check_pool_marker(pool, &copy_root)?;
                complete(&copy_root)?;
            }
        }
        Operation::Replace {
            selection,
            resume,
            authorize,
        } => {
            if !authorize {
                return Err(invalid("replacement requires --authorize"));
            }
            let context = Context::open(
                &selection.config,
                Some((&selection.pool, selection.member)),
                None,
            )?;
            let pool = context.selected(&selection)?;
            // Reuse N1's existing repair reader with only the explicitly leased
            // offline topology. No server, root admission bypass or cache is
            // started; Context already verified all surviving identities.
            let roots: Vec<_> = context
                .pools
                .iter()
                .flat_map(|p| p.roots.iter())
                .map(|p| p.to_string_lossy().into_owned())
                .collect();
            crate::core::disk_manager::INDEX_STORE_DISKS
                .set(roots.clone())
                .map_err(|_| invalid("offline metadata topology was already initialized"))?;
            crate::core::disk_manager::BLOB_STORE_DISKS
                .set(roots)
                .map_err(|_| invalid("offline repair topology was already initialized"))?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(replace(
                &context,
                pool,
                selection.member,
                resume,
                &mut report,
            ))?;
        }
    }
    report.elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } == 0 {
        report.peak_rss_kib = unsafe { usage.assume_init() }.ru_maxrss;
    }
    report.final_open_fds = fs::read_dir("/proc/self/fd")
        .map(|entries| entries.count())
        .unwrap_or(0);
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
    #[test]
    fn replacement_inventory_detects_missing_and_unexpected_files() {
        let f = Fixture::new();
        let target = f.dir("target");
        expect(&target, Path::new("a/data"), "file").unwrap();
        fs::create_dir(target.join("a")).unwrap();
        fs::write(target.join("a/data"), b"data").unwrap();
        audit_replacement(&target).unwrap();
        fs::remove_file(target.join("a/data")).unwrap();
        assert!(audit_replacement(&target).is_err());
        fs::write(target.join("a/data"), b"data").unwrap();
        fs::write(target.join("extra"), b"extra").unwrap();
        assert!(audit_replacement(&target).is_err());
    }
    #[test]
    fn piece_integrity_requires_checksum_size_and_bytes() {
        let f = Fixture::new();
        let path = f.0.join("piece");
        fs::write(&path, b"piece").unwrap();
        let mut shard = ShardInfo {
            shard_index: 0,
            is_parity: false,
            file_name: "piece".into(),
            absolute_path: path.to_string_lossy().into_owned(),
            size_bytes: 5,
            checksum: format!("{:08x}", crc32fast::hash(b"piece")),
        };
        checksum(&path, &shard).unwrap();
        shard.checksum.clear();
        assert!(checksum(&path, &shard).is_err());
        shard.checksum = format!("{:08x}", crc32fast::hash(b"piece"));
        fs::write(&path, b"other").unwrap();
        assert!(checksum(&path, &shard).is_err());
        fs::write(&path, b"piec").unwrap();
        assert!(checksum(&path, &shard).is_err());
    }
    #[test]
    fn offline_activation_rejects_missing_or_divergent_receipt_and_manifest_replicas() {
        let f = Fixture::new();
        let pool = StoragePool {
            id: "A".into(),
            generation: 1,
            data_shards: 1,
            parity_shards: 1,
            roots: vec![f.dir("a"), f.dir("b")],
            members: Vec::new(),
            legacy_roots: Vec::new(),
            sealed: false,
        };
        for (name, bytes) in [
            ("manifest.ndx", &b"manifest"[..]),
            ("piece.receipt", &b""[..]),
        ] {
            let rel = Path::new(name);
            fs::write(pool.roots[0].join(rel), bytes).unwrap();
            assert!(verify_replicas(&pool, None, rel, bytes).is_err());
            verify_replicas(&pool, Some(1), rel, bytes).unwrap();
            fs::write(pool.roots[1].join(rel), bytes).unwrap();
            verify_replicas(&pool, None, rel, bytes).unwrap();
            fs::write(pool.roots[1].join(rel), b"different").unwrap();
            assert!(verify_replicas(&pool, None, rel, bytes).is_err());
        }
    }

    #[test]
    fn backup_verifies_cross_pool_version_references_without_global_engine_state() {
        use crate::core::backup::format::{Member, Root};
        let f = Fixture::new();
        let fragment = ulid::Ulid::new().to_string();
        let object = ulid::Ulid::new().to_string();
        let version_id = ulid::Ulid::new().to_string();
        let pools = ["A", "B"]
            .into_iter()
            .enumerate()
            .map(|(i, id)| StoragePool {
                id: id.into(),
                generation: i as u64 + 1,
                data_shards: 1,
                parity_shards: 0,
                roots: vec![f.dir(id)],
                members: vec![ulid::Ulid::new().to_string()],
                legacy_roots: vec![],
                sealed: false,
            })
            .collect::<Vec<_>>();
        let meta = crate::core::index_store::FragmentMeta {
            engine_format_version: 4,
            stable_fragment_id: fragment.clone(),
            state: "active".into(),
            committed_bytes: 0,
            reserved_bytes: 0,
            trash_bytes: 0,
            created_at_unix: 1,
            updated_at_unix: 1,
            key_phc: None,
            nt_hash_hex: None,
            index_partition_count: 64,
        };
        for pool in &pools {
            storage_pool::write_member(pool, 0).unwrap();
            let physical = pool.roots[0].join(format!("@f_{fragment}"));
            fs::create_dir_all(&physical).unwrap();
            fs::write(
                physical.join(".fragment.ndx"),
                rmp_serde::to_vec_named(&meta).unwrap(),
            )
            .unwrap();
        }
        let fanout = ObjectPathManager::object_fanout_segments(&object);
        let object_rel = PathBuf::from(format!("@f_{fragment}"))
            .join(".objects")
            .join(&fanout[0])
            .join(&fanout[1])
            .join(format!("@o_{object}"));
        let directory = pools[1].roots[0].join(&object_rel);
        fs::create_dir_all(directory.join(&version_id)).unwrap();
        let piece = directory.join(&version_id).join("0.shard");
        fs::write(&piece, b"data").unwrap();
        let metadata = ObjectMetadata {
            object_key: object.clone(),
            fragment: fragment.clone(),
            version_id: version_id.clone(),
            upload_session_id: version_id.clone(),
            owner_uid: fragment.clone(),
            content_type: "application/octet-stream".into(),
            total_size_bytes: 4,
            created_at: 1,
            data_shards: 1,
            parity_shards: 0,
            placement_generation: 1,
            is_delete_marker: false,
            stripe_size_bytes: 4,
            parts: vec![PartMetadata {
                part_index: 0,
                size_bytes: 4,
                created_at: 1,
                checksum: None,
                shards: vec![ShardInfo {
                    shard_index: 0,
                    file_name: "0.shard".into(),
                    absolute_path: format!(
                        "n1://B/2/0/{}/{version_id}/0.shard",
                        object_rel.display()
                    ),
                    size_bytes: 4,
                    checksum: format!("{:08x}", crc32fast::hash(b"data")),
                    is_parity: false,
                }],
            }],
        };
        let ndx = directory.join(format!("{version_id}.ndx"));
        fs::write(&ndx, rmp_serde::to_vec_named(&metadata).unwrap()).unwrap();
        let pointer_path = pools[0].roots[0].join(format!("@f_{fragment}/.namespace/existing.ncx"));
        flat::write(
            &pools[0].roots[0],
            &pointer_path,
            &flat::Pointer {
                format: 4,
                pool_id: "A".into(),
                object_id: object,
            },
            true,
        )
        .unwrap();
        let roots = pools
            .iter()
            .map(|pool| Root {
                path: pool.roots[0].display().to_string(),
                legacy_path: None,
                index: true,
                blob: true,
                member: Some(Member {
                    pool_id: pool.id.clone(),
                    generation: pool.generation,
                    data_shards: 1,
                    parity_shards: 0,
                    slot: 0,
                }),
            })
            .collect::<Vec<_>>();
        let before_ndx = fs::read(&ndx).unwrap();
        let before_ncx = fs::read(&pointer_path).unwrap();
        verify_backup_roots(&roots).unwrap();
        assert_eq!(fs::read(&ndx).unwrap(), before_ndx);
        assert_eq!(fs::read(&pointer_path).unwrap(), before_ncx);
        fs::write(&piece, b"evil").unwrap();
        assert!(verify_backup_roots(&roots).is_err());
        assert_eq!(fs::read(&piece).unwrap(), b"evil");
        fs::write(&piece, b"data").unwrap();
        fs::remove_file(ndx).unwrap();
        assert!(verify_backup_roots(&roots).is_err());
    }
}
