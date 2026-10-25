use super::*;
use rustix::fs::{CWD, Dir, FileType, Mode, OFlags, SeekFrom, fstat, openat, seek};

const MAX_PAGE_ENTRIES: usize = 1000;

#[derive(Clone, Default)]
struct InventoryCursor {
    root_index: usize,
    cookie: u64,
    directory_identity: Option<(u64, u64)>,
}

/// A single best-effort pass over active ownership directories, not a snapshot
/// or destructive authority. New work may be observed on the next GC cycle.
/// The cursor is local to this pass; it is neither an index nor durable state.
pub struct UploadInventory {
    store: OwnershipStore,
    fragment: String,
    cursor: InventoryCursor,
}

pub struct UploadInventoryPage {
    pub records: Vec<UploadLifecycleRecord>,
    pub finished: bool,
    #[cfg(test)]
    pub(super) scanned_entries: usize,
}

impl UploadInventory {
    pub fn new(runtime: &ClusterRuntime, fragment: &str) -> Result<Self, AppError> {
        if fragment.is_empty()
            || fragment.len() > 128
            || !fragment
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        {
            return Err(AppError::BadRequest(
                "invalid upload ownership fragment".into(),
            ));
        }
        Ok(Self {
            store: OwnershipStore::from_runtime(runtime)?,
            fragment: fragment.to_string(),
            cursor: InventoryCursor::default(),
        })
    }

    /// Inspect at most `limit` directory entries (including names that are not
    /// ownership candidates). Empty pages can precede completion. Never walk
    /// `.objects` or `.fences`, and retain no directory FD between calls.
    pub async fn next_page(&mut self, limit: usize) -> Result<UploadInventoryPage, AppError> {
        let store = self.store.clone();
        let fragment = self.fragment.clone();
        let cursor = self.cursor.clone();
        let (cursor, page) = tokio::task::spawn_blocking(move || {
            scan_page(&store, &fragment, cursor, limit.clamp(1, MAX_PAGE_ENTRIES))
        })
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("upload ownership inventory worker: {e}"))
        })??;
        // If cancelled, the original cursor remains usable. Repeating one
        // bounded page is safe because candidates must be revalidated anyway.
        self.cursor = cursor;
        Ok(page)
    }
}

fn scan_page(
    store: &OwnershipStore,
    fragment: &str,
    mut cursor: InventoryCursor,
    limit: usize,
) -> Result<(InventoryCursor, UploadInventoryPage), AppError> {
    let _binding = store.guard_binding()?;
    store.check_roots()?;
    let mut records = Vec::new();
    let mut scanned_entries = 0;
    while cursor.root_index < store.roots.len() && scanned_entries < limit {
        let root = &store.roots[cursor.root_index];
        let uploads = physical_fragment_root_on(root, fragment).join(".uploads");
        let fd = match openat(
            CWD,
            &uploads,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(rustix::io::Errno::NOENT) if cursor.directory_identity.is_none() => {
                // A healthy enrolled root may not yet contain this fragment.
                advance_root(&mut cursor);
                continue;
            }
            Err(error) => {
                return Err(AppError::Io(
                    error.into(),
                    format!("open active upload inventory {}", uploads.display()),
                ));
            }
        };
        let stat = fstat(&fd)
            .map_err(|e| AppError::Io(e.into(), "inspect active upload inventory".into()))?;
        let identity = (stat.st_dev as u64, stat.st_ino as u64);
        if cursor
            .directory_identity
            .is_some_and(|expected| expected != identity)
        {
            return Err(AppError::ServiceUnavailable(
                "active upload inventory directory changed; retry next GC pass".into(),
            ));
        }
        cursor.directory_identity = Some(identity);
        if cursor.cookie != 0 {
            seek(&fd, SeekFrom::Start(cursor.cookie))
                .map_err(|e| AppError::Io(e.into(), "resume active upload inventory".into()))?;
        }
        let mut directory = Dir::new(fd)
            .map_err(|e| AppError::Io(e.into(), "read active upload inventory".into()))?;
        while scanned_entries < limit {
            let Some(entry) = directory.read() else {
                advance_root(&mut cursor);
                break;
            };
            let entry = match entry {
                Ok(entry) => entry,
                Err(rustix::io::Errno::NOENT) => {
                    // A concurrent completed upload may retire its active dir.
                    scanned_entries += 1;
                    continue;
                }
                Err(e) => {
                    return Err(AppError::Io(
                        e.into(),
                        "read active upload inventory entry".into(),
                    ));
                }
            };
            scanned_entries += 1;
            cursor.cookie = entry.offset().try_into().map_err(|_| {
                AppError::Conflict("active upload inventory has an invalid directory cookie".into())
            })?;
            let Ok(version) = entry.file_name().to_str() else {
                continue;
            };
            if version.len() != 26 || Ulid::from_string(version).is_err() {
                continue;
            }
            let path = uploads.join(version);
            let is_directory = match entry.file_type() {
                FileType::Directory => true,
                FileType::Unknown => match fs::symlink_metadata(&path) {
                    Ok(metadata) => metadata.is_dir(),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                    Err(e) => {
                        return Err(AppError::Io(e, "inspect upload inventory candidate".into()));
                    }
                },
                _ => false,
            };
            if !is_directory {
                continue;
            }
            if let Some(record) = read_candidate(store, fragment, version, cursor.root_index)? {
                records.push(record);
            }
        }
        // `directory` is dropped before opening another root or returning.
    }
    let finished = cursor.root_index == store.roots.len();
    Ok((
        cursor,
        UploadInventoryPage {
            records,
            finished,
            #[cfg(test)]
            scanned_entries,
        },
    ))
}

fn advance_root(cursor: &mut InventoryCursor) {
    cursor.root_index += 1;
    cursor.cookie = 0;
    cursor.directory_identity = None;
}

fn read_candidate(
    store: &OwnershipStore,
    fragment: &str,
    version: &str,
    root_index: usize,
) -> Result<Option<UploadLifecycleRecord>, AppError> {
    let path = physical_fragment_root_on(&store.roots[root_index], fragment)
        .join(".uploads")
        .join(version)
        .join(OWNERSHIP_FILE);
    let Some(record) = read_record(&path)? else {
        return Ok(None);
    };
    if record.identity.fragment != fragment || record.identity.version_id != version {
        return Err(AppError::Conflict(
            "upload inventory candidate path differs from identity".into(),
        ));
    }
    // Read every control copy even for duplicates: suppressing a replica must
    // not hide conflicting terminal states or a corrupt ownership record.
    let record = store
        .load(&record.identity)?
        .ok_or_else(|| AppError::Conflict("upload inventory candidate disappeared".into()))?;
    for root in &store.roots[..root_index] {
        if let Some(earlier) = read_record(&OwnershipStore::path(root, &record.identity))? {
            if earlier.identity != record.identity
                || matches!(
                    (earlier.state, record.state),
                    (
                        UploadLifecycleState::Committed,
                        UploadLifecycleState::Aborted
                    ) | (
                        UploadLifecycleState::Aborted,
                        UploadLifecycleState::Committed
                    )
                )
            {
                return Err(AppError::Conflict(
                    "upload inventory replicas disagree".into(),
                ));
            }
            return Ok(None);
        }
    }
    Ok(Some(record))
}
