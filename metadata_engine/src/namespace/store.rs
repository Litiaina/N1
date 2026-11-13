use crate::{
    Error, MetadataEngine, MetadataTransaction, RecordKey, Result, Table,
    object_store::{ObjectFinalization, ObjectStat, ObjectUpload},
};
use n1_link::PoolId;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use ulid::Ulid;

const ATTEMPTS: usize = 64;
pub(crate) mod lifecycle;
type SnapshotFiles = (Vec<(RecordKey, PublishedVersion)>, Option<RecordKey>);
pub(crate) struct ShallowEntry {
    pub path: String,
    pub directory: bool,
    pub version: Option<PublishedVersion>,
}
pub(crate) struct ShallowPage {
    pub entries: Vec<ShallowEntry>,
    pub after: Option<String>,
}

pub(crate) struct Reservation {
    pub path: String,
    pub version: String,
    pub session: String,
    pub upload: ObjectUpload,
    pub content_type: String,
    pub created_at: i64,
}
pub(crate) struct Publication {
    pub version: String,
    pub session: String,
    pub finalized: ObjectFinalization,
    pub stat: ObjectStat,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedVersion {
    pub object_id: String,
    pub version_id: String,
    pub content_type: String,
    pub total_size_bytes: u64,
    pub created_at_unix: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UploadHandle {
    pub version_id: String,
    pub session_id: String,
    /// The logical user key, never a LINK credential.
    pub object_key: String,
    pub total_size_bytes: u64,
    pub part_size_bytes: u64,
    pub expected_parts: u64,
    pub durable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Entry {
    object_id: String,
    head: Option<PublishedVersion>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct StoredVersion {
    pub metadata: PublishedVersion,
    pub object_key: String,
    pub integrity_algorithm: String,
    pub integrity_root: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct PendingUpload {
    pub handle: UploadHandle,
    pub object_id: String,
    pub upload: ObjectUpload,
    pub content_type: String,
    pub created_at_unix: i64,
    pub completed: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct ObjectBinding {
    path: String,
    trashed: bool,
    #[serde(default)]
    reclaiming: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Trash {
    pub path: String,
    entry: Entry,
    pub deleted_at_unix: i64,
    pub restored: bool,
    #[serde(default)]
    reclaiming: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct NamespaceFile {
    pub path: String,
    #[serde(flatten)]
    pub version: PublishedVersion,
}

#[derive(Clone, Debug)]
pub struct NamespaceFilePage {
    pub files: Vec<NamespaceFile>,
    /// Internal keyset cursor. The NOA adapter binds and encodes this cursor to
    /// the authenticated fragment and query; it is not a public token itself.
    pub after: Option<String>,
}

#[derive(Clone)]
pub struct Namespace {
    engine: MetadataEngine,
}

impl Namespace {
    /// Read bounded, sharded counters instead of scanning all logical names.
    pub fn usage(&self, fragment: PoolId) -> Result<Option<Usage>> {
        let snapshot = self.engine.snapshot();
        if snapshot
            .get(
                Table::Reclamation,
                &RecordKey::new(fragment, "usage/initialized")?,
            )?
            .is_none()
        {
            return Ok(None);
        }
        let mut result = Usage::default();
        for (key, value) in
            snapshot.scan(Table::Reclamation, fragment, "usage/bucket/", None, 256)?
        {
            if key.identifier().len() != 15 {
                return Err(Error::Incompatible("invalid namespace usage bucket".into()));
            }
            let usage: Usage = decode(value)?;
            result.used_bytes = result
                .used_bytes
                .checked_add(usage.used_bytes)
                .ok_or_else(|| Error::Incompatible("namespace usage overflow".into()))?;
            result.object_count = result
                .object_count
                .checked_add(usage.object_count)
                .ok_or_else(|| Error::Incompatible("namespace count overflow".into()))?;
        }
        Ok(Some(result))
    }
    /// Explicit offline administration only; may read the full namespace.
    pub fn recount_usage(&self, fragment: PoolId) -> Result<Usage> {
        let snapshot = self.engine.snapshot();
        let mut after = None;
        let mut buckets = std::collections::BTreeMap::<String, Usage>::new();
        loop {
            let page = snapshot.scan(Table::Entries, fragment, "", after.as_ref(), 1000)?;
            if page.is_empty() {
                break;
            }
            for (key, value) in page {
                after = Some(key);
                let entry: Entry = decode(value)?;
                if let Some(head) = entry.head {
                    let bucket = buckets.entry(usage_bucket(&entry.object_id)?).or_default();
                    bucket.used_bytes = bucket
                        .used_bytes
                        .checked_add(head.total_size_bytes)
                        .ok_or_else(|| Error::Invalid("namespace byte count overflow".into()))?;
                    bucket.object_count += 1;
                }
            }
        }
        self.engine.transact(ATTEMPTS, |tx| {
            for (key, _) in tx.scan(Table::Reclamation, fragment, "usage/bucket/", None, 256)? {
                tx.remove(Table::Reclamation, &key)?;
            }
            for (bucket, value) in &buckets {
                put(
                    tx,
                    Table::Reclamation,
                    &RecordKey::new(fragment, bucket)?,
                    value,
                )?;
            }
            tx.put(
                Table::Reclamation,
                &RecordKey::new(fragment, "usage/initialized")?,
                b"1",
            )?;
            Ok(())
        })?;
        Ok(self.usage(fragment)?.expect("recount initialized usage"))
    }
    /// Seek past a reported virtual prefix instead of visiting every child.
    /// Inclusive successor bounds preserve a real key at the exact boundary.
    pub(crate) fn shallow(
        &self,
        fragment: PoolId,
        prefix: &str,
        delimiter: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ShallowPage> {
        if !(1..=1000).contains(&limit) {
            return Err(Error::Invalid("invalid namespace page size".into()));
        }
        let view = self.engine.snapshot();
        let mut cursor = after.map(str::to_owned);
        let mut entries = Vec::new();
        while entries.len() <= limit {
            let inclusive = !delimiter.is_empty()
                && cursor
                    .as_deref()
                    .is_some_and(|key| key.ends_with(delimiter));
            let seek = if inclusive {
                cursor.as_deref().and_then(prefix_successor)
            } else {
                cursor.clone()
            };
            if inclusive && seek.is_none() {
                break;
            }
            if seek.as_deref().is_some_and(|key| !key.starts_with(prefix)) {
                break;
            }
            let key = seek
                .as_deref()
                .map(|key| RecordKey::new(fragment, key))
                .transpose()?;
            let rows = if inclusive {
                view.scan_from(
                    Table::Entries,
                    fragment,
                    prefix,
                    key.as_ref().expect("successor key"),
                    1,
                )?
            } else {
                view.scan(Table::Entries, fragment, prefix, key.as_ref(), 1)?
            };
            let Some((key, bytes)) = rows.into_iter().next() else {
                break;
            };
            cursor = Some(key.identifier().into());
            let entry: Entry = decode(bytes)?;
            let Some(version) = entry.head else {
                continue;
            };
            let tail = key
                .identifier()
                .strip_prefix(prefix)
                .unwrap_or(key.identifier());
            if !delimiter.is_empty()
                && let Some(position) = tail.find(delimiter)
            {
                let path = format!("{}{}", prefix, &tail[..position + delimiter.len()]);
                cursor = Some(path.clone());
                entries.push(ShallowEntry {
                    path,
                    directory: true,
                    version: None,
                });
            } else {
                entries.push(ShallowEntry {
                    path: key.identifier().into(),
                    directory: false,
                    version: Some(version),
                });
            }
        }
        let has_more = entries.len() > limit;
        entries.truncate(limit);
        let after = if has_more {
            entries.last().map(|entry| entry.path.clone())
        } else {
            None
        };
        Ok(ShallowPage { entries, after })
    }
    pub(crate) fn reserve_batch(
        &self,
        fragment: PoolId,
        records: &[Reservation],
    ) -> Result<Vec<UploadHandle>> {
        if records.is_empty() || records.len() > 64 {
            return Err(Error::Invalid(
                "metadata upload group must contain 1 through 64 objects".into(),
            ));
        }
        self.engine.transact(ATTEMPTS, |tx| {
            records
                .iter()
                .map(|r| {
                    reserve_record(
                        tx,
                        fragment,
                        &r.path,
                        &r.version,
                        &r.session,
                        &r.upload,
                        &r.content_type,
                        r.created_at,
                    )
                })
                .collect()
        })
    }
    pub(crate) fn publish_batch(
        &self,
        fragment: PoolId,
        records: &[Publication],
    ) -> Result<Vec<PublishedVersion>> {
        if records.is_empty() || records.len() > 64 {
            return Err(Error::Invalid(
                "metadata publication group must contain 1 through 64 objects".into(),
            ));
        }
        self.engine.transact(ATTEMPTS, |tx| {
            records
                .iter()
                .map(|r| {
                    publish_record(tx, fragment, &r.version, &r.session, &r.finalized, &r.stat)
                })
                .collect()
        })
    }
    pub(crate) fn files_from_snapshot(
        view: &crate::MetadataSnapshot,
        fragment: PoolId,
        prefix: &str,
        after: Option<&RecordKey>,
    ) -> Result<SnapshotFiles> {
        let rows = view.scan(Table::Entries, fragment, prefix, after, 1000)?;
        let cursor = rows.last().map(|row| row.0.clone());
        let mut result = Vec::new();
        for (key, value) in rows {
            let entry: Entry = decode(value)?;
            if let Some(head) = entry.head {
                result.push((key, head));
            }
        }
        Ok((result, cursor))
    }
    pub(crate) fn resolve_from_snapshot(
        view: &crate::MetadataSnapshot,
        fragment: PoolId,
        path: &str,
        version: Option<&str>,
    ) -> Result<StoredVersion> {
        let key = RecordKey::new(fragment, user_key(path)?)?;
        let entry: Entry = decode(view.get(Table::Entries, &key)?.ok_or(Error::NotFound)?)?;
        let head = entry.head.ok_or(Error::NotFound)?;
        let version = version.unwrap_or(&head.version_id);
        valid_ulid(version)?;
        decode(
            view.get(
                Table::Versions,
                &version_key(fragment, &entry.object_id, version)?,
            )?
            .ok_or(Error::NotFound)?,
        )
    }
    pub(crate) fn by_identity(
        &self,
        fragment: PoolId,
        object: &str,
        version: &str,
    ) -> Result<StoredVersion> {
        valid_ulid(object)?;
        valid_ulid(version)?;
        decode(
            self.engine
                .snapshot()
                .get(Table::Versions, &version_key(fragment, object, version)?)?
                .ok_or(Error::NotFound)?,
        )
    }
    pub fn versions(&self, fragment: PoolId, path: &str) -> Result<Vec<PublishedVersion>> {
        let head = self.stat(fragment, path, None)?;
        let prefix = format!("{}/", head.object_id);
        let mut result = Vec::new();
        let mut after = None;
        loop {
            let rows = self.engine.snapshot().scan(
                Table::Versions,
                fragment,
                &prefix,
                after.as_ref(),
                1000,
            )?;
            if rows.is_empty() {
                break;
            }
            after = rows.last().map(|(key, _)| key.clone());
            for (_, value) in rows {
                result.push(decode::<StoredVersion>(value)?.metadata);
            }
            if result.len() > 131072 {
                return Err(Error::Invalid(
                    "version listing exceeds response limit".into(),
                ));
            }
        }
        result.sort_by(|a, b| b.version_id.cmp(&a.version_id));
        Ok(result)
    }
    pub(crate) fn trash_page(
        &self,
        fragment: PoolId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Trash, PublishedVersion)>> {
        let after = after
            .map(|value| RecordKey::new(fragment, value))
            .transpose()?;
        let rows = self.engine.snapshot().scan(
            Table::Reclamation,
            fragment,
            "trash/",
            after.as_ref(),
            limit,
        )?;
        rows.into_iter()
            .map(|(key, value)| {
                let trash: Trash = decode(value)?;
                let head = trash
                    .entry
                    .head
                    .clone()
                    .ok_or_else(|| Error::Incompatible("trash record has no head".into()))?;
                Ok((key.identifier().to_owned(), trash, head))
            })
            .collect()
    }
    pub fn new(engine: MetadataEngine) -> Self {
        Self { engine }
    }

    /// The object service's init is stateless. Persist its returned identity
    /// here before admitting bytes. Concurrent initial writers share one stable
    /// logical object identity, while each gets its own immutable LINK object.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reserve_upload(
        &self,
        fragment: PoolId,
        path: &str,
        version_id: &str,
        session_id: &str,
        upload: &ObjectUpload,
        content_type: &str,
        created_at_unix: i64,
    ) -> Result<UploadHandle> {
        self.engine.transact(ATTEMPTS, |tx| {
            reserve_record(
                tx,
                fragment,
                path,
                version_id,
                session_id,
                upload,
                content_type,
                created_at_unix,
            )
        })
    }

    pub(crate) fn pending(
        &self,
        fragment: PoolId,
        version_id: &str,
        session_id: Option<&str>,
    ) -> Result<PendingUpload> {
        valid_ulid(version_id)?;
        if self
            .engine
            .snapshot()
            .get(
                Table::Reclamation,
                &RecordKey::new(fragment, format!("upload-gc/{version_id}"))?,
            )?
            .is_some()
        {
            return Err(Error::NameConflict("upload reclamation has started".into()));
        }
        let pending: PendingUpload = decode(
            self.engine
                .snapshot()
                .get(Table::Uploads, &RecordKey::new(fragment, version_id)?)?
                .ok_or(Error::NotFound)?,
        )?;
        if session_id.is_some_and(|session| session != pending.handle.session_id) {
            return Err(Error::NotFound);
        }
        Ok(pending)
    }

    /// Include ownership in the part-ACK transaction so a concurrent trash or
    /// reclamation claim conflicts with an already prepared acknowledgement.
    pub(crate) fn check_upload_owner(
        tx: &MetadataTransaction,
        fragment: PoolId,
        object: &str,
    ) -> Result<()> {
        let binding: ObjectBinding =
            read(tx, Table::Versions, &binding_key(fragment, object)?)?.ok_or(Error::NotFound)?;
        if binding.trashed || binding.reclaiming {
            return Err(Error::NameConflict(
                "upload owner is no longer accepting parts".into(),
            ));
        }
        Ok(())
    }

    /// Call only after the existing object API has durably finalized and stat'd
    /// this exact LINK identity. No object I/O occurs inside a retrying database
    /// transaction. A lost reply can repeat this same publication safely.
    pub(crate) fn publish(
        &self,
        fragment: PoolId,
        version_id: &str,
        session_id: &str,
        finalized: &ObjectFinalization,
        stat: &ObjectStat,
    ) -> Result<PublishedVersion> {
        self.engine.transact(ATTEMPTS, |tx| {
            publish_record(tx, fragment, version_id, session_id, finalized, stat)
        })
    }

    pub(crate) fn resolve(
        &self,
        fragment: PoolId,
        path: &str,
        version_id: Option<&str>,
    ) -> Result<StoredVersion> {
        let key = RecordKey::new(fragment, user_key(path)?)?;
        let view = self.engine.snapshot();
        let entry: Entry = decode(view.get(Table::Entries, &key)?.ok_or(Error::NotFound)?)?;
        let head = entry.head.ok_or(Error::NotFound)?;
        let version = version_id.unwrap_or(&head.version_id);
        valid_ulid(version)?;
        let missing = if version_id.is_some() {
            Error::NotFound
        } else {
            Error::Incompatible("logical object is missing its committed version".into())
        };
        decode(
            view.get(
                Table::Versions,
                &version_key(fragment, &entry.object_id, version)?,
            )?
            .ok_or(missing)?,
        )
    }

    pub fn stat(
        &self,
        fragment: PoolId,
        path: &str,
        version_id: Option<&str>,
    ) -> Result<PublishedVersion> {
        Ok(self.resolve(fragment, path, version_id)?.metadata)
    }

    pub fn list(
        &self,
        fragment: PoolId,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<NamespaceFilePage> {
        let after = after.map(|key| RecordKey::new(fragment, key)).transpose()?;
        let rows =
            self.engine
                .snapshot()
                .scan(Table::Entries, fragment, prefix, after.as_ref(), limit)?;
        let cursor = rows.last().map(|(key, _)| key.identifier().to_owned());
        let mut files = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let entry: Entry = decode(value)?;
            if let Some(version) = entry.head {
                files.push(NamespaceFile {
                    path: key.identifier().into(),
                    version,
                });
            }
        }
        Ok(NamespaceFilePage {
            files,
            after: cursor,
        })
    }

    /// Exact logical-key rename, updating only metadata. Prefix adapters must
    /// apply this per key in bounded pages to retain non-atomic N1 semantics.
    pub fn rename(&self, fragment: PoolId, source: &str, destination: &str) -> Result<()> {
        let source = RecordKey::new(fragment, user_key(source)?)?;
        let destination = RecordKey::new(fragment, user_key(destination)?)?;
        self.engine.transact(ATTEMPTS, |tx| {
            let entry: Entry = read(tx, Table::Entries, &source)?
                .filter(|entry: &Entry| entry.head.is_some())
                .ok_or(Error::NotFound)?;
            if source == destination {
                return Ok(());
            }
            let previous_destination = read::<Entry>(tx, Table::Entries, &destination)?;
            if let Some(other) = previous_destination.as_ref()
                && other.object_id != entry.object_id
            {
                return Err(Error::NameConflict(
                    "destination belongs to another logical object".into(),
                ));
            }
            if let Some(other) = previous_destination {
                update_usage(tx, fragment, &other.object_id, other.head.as_ref(), None)?;
            }
            put(tx, Table::Entries, &destination, &entry)?;
            tx.remove(Table::Entries, &source)?;
            put(
                tx,
                Table::Versions,
                &binding_key(fragment, &entry.object_id)?,
                &ObjectBinding {
                    path: destination.identifier().into(),
                    trashed: false,
                    reclaiming: false,
                },
            )
        })
    }

    /// Soft deletion. Never calls LINK's permanent object.delete operation.
    pub fn trash(&self, fragment: PoolId, path: &str, deleted_at_unix: i64) -> Result<String> {
        let key = RecordKey::new(fragment, user_key(path)?)?;
        let trash_id = Ulid::new().to_string();
        let trash_key = RecordKey::new(fragment, format!("trash/{trash_id}"))?;
        self.engine.transact(ATTEMPTS, |tx| {
            let entry: Entry = read(tx, Table::Entries, &key)?
                .filter(|entry: &Entry| entry.head.is_some())
                .ok_or(Error::NotFound)?;
            update_usage(tx, fragment, &entry.object_id, entry.head.as_ref(), None)?;
            put(
                tx,
                Table::Versions,
                &binding_key(fragment, &entry.object_id)?,
                &ObjectBinding {
                    path: key.identifier().into(),
                    trashed: true,
                    reclaiming: false,
                },
            )?;
            put(
                tx,
                Table::Reclamation,
                &trash_key,
                &Trash {
                    path: key.identifier().into(),
                    entry,
                    deleted_at_unix,
                    restored: false,
                    reclaiming: false,
                },
            )?;
            lifecycle::index_trash(tx, fragment, &trash_id, deleted_at_unix)?;
            tx.remove(Table::Entries, &key)?;
            Ok(trash_id.clone())
        })
    }

    pub fn recover(&self, fragment: PoolId, trash_id: &str) -> Result<String> {
        valid_ulid(trash_id)?;
        let trash_key = RecordKey::new(fragment, format!("trash/{trash_id}"))?;
        self.engine.transact(ATTEMPTS, |tx| {
            let mut trash: Trash =
                read(tx, Table::Reclamation, &trash_key)?.ok_or(Error::NotFound)?;
            if trash.reclaiming {
                return Err(Error::NameConflict(
                    "trash reclamation has already started".into(),
                ));
            }
            let key = RecordKey::new(fragment, &trash.path)?;
            let previous = read::<Entry>(tx, Table::Entries, &key)?;
            if let Some(other) = read::<Entry>(tx, Table::Entries, &key)? {
                if other.object_id != trash.entry.object_id {
                    return Err(Error::NameConflict(
                        "recovery destination belongs to another object".into(),
                    ));
                }
                if trash.restored {
                    return Ok(trash.path);
                }
            } else if trash.restored {
                return Err(Error::NameConflict(
                    "recovered object has since moved or been trashed again".into(),
                ));
            }
            put(tx, Table::Entries, &key, &trash.entry)?;
            update_usage(
                tx,
                fragment,
                &trash.entry.object_id,
                previous.as_ref().and_then(|entry| entry.head.as_ref()),
                trash.entry.head.as_ref(),
            )?;
            put(
                tx,
                Table::Versions,
                &binding_key(fragment, &trash.entry.object_id)?,
                &ObjectBinding {
                    path: trash.path.clone(),
                    trashed: false,
                    reclaiming: false,
                },
            )?;
            trash.restored = true;
            put(tx, Table::Reclamation, &trash_key, &trash)?;
            lifecycle::unindex_trash(
                tx,
                &lifecycle::TrashWork {
                    fragment,
                    trash: trash_id.into(),
                    object: trash.entry.object_id.clone(),
                    deleted_at: trash.deleted_at_unix,
                },
            )?;
            Ok(trash.path)
        })
    }
}

pub(crate) fn validate_upload(path: &str, content_type: &str) -> Result<String> {
    if content_type.is_empty()
        || content_type.len() > 1024
        || content_type.contains(['\r', '\n', '\0'])
    {
        return Err(Error::Invalid("invalid object content type".into()));
    }
    user_key(path)
}

fn user_key(path: &str) -> Result<String> {
    let path =
        super::paths::normalize_path(path).map_err(|error| Error::Invalid(error.to_string()))?;
    if path.is_empty() || path.len() > 4096 || path.contains('\0') {
        return Err(Error::Invalid(
            "object key must contain 1–4096 UTF-8 bytes".into(),
        ));
    }
    // The established API reserves N1's internal names even though metadata no
    // longer maps these logical segments into physical member paths.
    for segment in path.split('/') {
        if [
            "@o_",
            "@f_",
            ".fragment",
            ".objects",
            ".trash",
            ".uploads",
            ".tmp",
            ".locks",
            ".gc",
            ".repair",
            ".intents",
            ".trash-jobs",
        ]
        .iter()
        .any(|prefix| segment.starts_with(prefix))
        {
            return Err(Error::Invalid(format!(
                "path segment '{segment}' is reserved by N1"
            )));
        }
    }
    Ok(path)
}

fn valid_ulid(value: &str) -> Result<()> {
    if Ulid::from_string(value).map_or(true, |id| id.to_string() != value) {
        return Err(Error::Invalid("identifier must be a canonical ULID".into()));
    }
    Ok(())
}

fn prefix_successor(value: &str) -> Option<String> {
    let mut value = value.to_owned();
    while let Some(last) = value.pop() {
        if last == char::MAX {
            continue;
        }
        let mut next = last as u32 + 1;
        if next == 0xd800 {
            next = 0xe000;
        }
        if let Some(next) = char::from_u32(next) {
            value.push(next);
            return Some(value);
        }
    }
    None
}

fn binding_key(fragment: PoolId, object_id: &str) -> Result<RecordKey> {
    RecordKey::new(fragment, format!("@{object_id}"))
}
fn version_key(fragment: PoolId, object_id: &str, version_id: &str) -> Result<RecordKey> {
    RecordKey::new(fragment, format!("{object_id}/{version_id}"))
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))
}
fn decode<T: DeserializeOwned>(value: Vec<u8>) -> Result<T> {
    serde_json::from_slice(&value)
        .map_err(|error| Error::Incompatible(format!("invalid namespace record: {error}")))
}
fn read<T: DeserializeOwned>(
    tx: &MetadataTransaction,
    table: Table,
    key: &RecordKey,
) -> Result<Option<T>> {
    tx.get(table, key)?.map(decode).transpose()
}
fn put(
    tx: &mut MetadataTransaction,
    table: Table,
    key: &RecordKey,
    value: &impl Serialize,
) -> Result<()> {
    tx.put(table, key, &encode(value)?)
}

#[cfg(test)]
mod tests;

#[allow(clippy::too_many_arguments)]
fn reserve_record(
    tx: &mut MetadataTransaction,
    fragment: PoolId,
    path: &str,
    version_id: &str,
    session_id: &str,
    upload: &ObjectUpload,
    content_type: &str,
    created_at_unix: i64,
) -> Result<UploadHandle> {
    let path = validate_upload(path, content_type)?;
    valid_ulid(version_id)?;
    valid_ulid(session_id)?;
    let candidate_id = Ulid::new().to_string();
    let key = RecordKey::new(fragment, &path)?;
    let pending_key = RecordKey::new(fragment, version_id)?;

    if tx.get(Table::Uploads, &pending_key)?.is_some() {
        return Err(Error::NameConflict("upload version already exists".into()));
    }
    let entry = match read::<Entry>(tx, Table::Entries, &key)? {
        Some(entry) => entry,
        None => {
            let entry = Entry {
                object_id: candidate_id.clone(),
                head: None,
            };
            put(tx, Table::Entries, &key, &entry)?;
            put(
                tx,
                Table::Versions,
                &binding_key(fragment, &entry.object_id)?,
                &ObjectBinding {
                    path: path.clone(),
                    trashed: false,
                    reclaiming: false,
                },
            )?;
            entry
        }
    };
    let handle = UploadHandle {
        version_id: version_id.into(),
        session_id: session_id.into(),
        object_key: path.clone(),
        total_size_bytes: upload.total_bytes,
        part_size_bytes: upload.part_bytes,
        expected_parts: upload.parts,
        durable: upload.durable,
    };
    put(
        tx,
        Table::Uploads,
        &pending_key,
        &PendingUpload {
            handle: handle.clone(),
            object_id: entry.object_id.clone(),
            upload: upload.clone(),
            content_type: content_type.into(),
            created_at_unix,
            completed: false,
        },
    )?;
    lifecycle::index_upload(tx, fragment, &entry.object_id, version_id, upload)?;
    put(
        tx,
        Table::Reclamation,
        &RecordKey::new(fragment, format!("pending/{version_id}"))?,
        &serde_json::json!({"version":version_id,"session":session_id}),
    )?;
    Ok(handle)
}

fn publish_record(
    tx: &mut MetadataTransaction,
    fragment: PoolId,
    version_id: &str,
    session_id: &str,
    finalized: &ObjectFinalization,
    stat: &ObjectStat,
) -> Result<PublishedVersion> {
    valid_ulid(version_id)?;
    let pending_key = RecordKey::new(fragment, version_id)?;

    let mut pending: PendingUpload =
        read(tx, Table::Uploads, &pending_key)?.ok_or(Error::NotFound)?;
    if tx
        .get(
            Table::Reclamation,
            &RecordKey::new(fragment, format!("upload-gc/{version_id}"))?,
        )?
        .is_some()
    {
        return Err(Error::NameConflict("upload reclamation has started".into()));
    }
    if pending.handle.session_id != session_id {
        return Err(Error::NotFound);
    }
    if finalized.object_key != pending.upload.object_key
        || finalized.total_bytes != pending.upload.total_bytes
        || stat.total_bytes != pending.upload.total_bytes
    {
        return Err(Error::Incompatible(
            "finalized object differs from its reserved upload".into(),
        ));
    }
    let metadata = PublishedVersion {
        object_id: pending.object_id.clone(),
        version_id: version_id.into(),
        content_type: pending.content_type.clone(),
        total_size_bytes: stat.total_bytes,
        created_at_unix: pending.created_at_unix,
    };
    let version_key = version_key(fragment, &pending.object_id, version_id)?;
    let stored = StoredVersion {
        metadata: metadata.clone(),
        object_key: pending.upload.object_key.clone(),
        integrity_algorithm: stat.integrity_algorithm.clone(),
        integrity_root: stat.integrity_root.clone(),
    };
    if let Some(existing) = tx.get(Table::Versions, &version_key)? {
        if existing != encode(&stored)? {
            return Err(Error::Incompatible(
                "immutable version record differs from finalize retry".into(),
            ));
        }
        if !pending.completed {
            return Err(Error::Incompatible(
                "version published without a completed upload".into(),
            ));
        }
        tx.remove(
            Table::Reclamation,
            &RecordKey::new(fragment, format!("pending/{version_id}"))?,
        )?;
        crate::uploads::progress::remove(tx, fragment, version_id)?;
        return Ok(metadata);
    }
    if pending.completed {
        return Err(Error::Incompatible(
            "completed upload is missing its immutable version".into(),
        ));
    }
    let binding: ObjectBinding = read(
        tx,
        Table::Versions,
        &binding_key(fragment, &pending.object_id)?,
    )?
    .ok_or(Error::NotFound)?;
    if binding.trashed || binding.reclaiming {
        return Err(Error::NameConflict(
            "upload's logical object has been trashed".into(),
        ));
    }
    let key = RecordKey::new(fragment, binding.path)?;
    let mut entry: Entry = read(tx, Table::Entries, &key)?.ok_or(Error::NotFound)?;
    if entry.object_id != pending.object_id {
        return Err(Error::NameConflict(
            "logical key now belongs to another object".into(),
        ));
    }
    put(tx, Table::Versions, &version_key, &stored)?;
    lifecycle::index_version(tx, fragment, &stored)?;
    // Completion order never changes N1's maximum-ULID rule.
    if entry
        .head
        .as_ref()
        .is_none_or(|head| head.version_id.as_str() < version_id)
    {
        update_usage(
            tx,
            fragment,
            &entry.object_id,
            entry.head.as_ref(),
            Some(&metadata),
        )?;
        entry.head = Some(metadata.clone());
        put(tx, Table::Entries, &key, &entry)?;
    }
    pending.completed = true;
    lifecycle::unindex_upload(
        tx,
        &lifecycle::Target {
            fragment,
            object: pending.object_id.clone(),
            version: version_id.into(),
        },
        pending.upload.expires_at,
    )?;
    put(tx, Table::Uploads, &pending_key, &pending)?;
    tx.remove(
        Table::Reclamation,
        &RecordKey::new(fragment, format!("pending/{version_id}"))?,
    )?;
    crate::uploads::progress::remove(tx, fragment, version_id)?;
    Ok(metadata)
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Usage {
    pub used_bytes: u64,
    pub object_count: u64,
}
fn usage_bucket(object: &str) -> Result<String> {
    let id = Ulid::from_string(object)
        .map_err(|_| Error::Incompatible("invalid object ID for namespace usage".into()))?;
    Ok(format!("usage/bucket/{:02x}", id.to_bytes()[15]))
}
fn update_usage(
    tx: &mut MetadataTransaction,
    fragment: PoolId,
    object: &str,
    old: Option<&PublishedVersion>,
    new: Option<&PublishedVersion>,
) -> Result<()> {
    if tx
        .get(
            Table::Reclamation,
            &RecordKey::new(fragment, "usage/initialized")?,
        )?
        .is_none()
    {
        return Ok(());
    }
    let key = RecordKey::new(fragment, usage_bucket(object)?)?;
    let mut usage: Usage = read(tx, Table::Reclamation, &key)?.unwrap_or_default();
    usage.used_bytes = usage
        .used_bytes
        .checked_sub(old.map_or(0, |v| v.total_size_bytes))
        .and_then(|n| n.checked_add(new.map_or(0, |v| v.total_size_bytes)))
        .ok_or_else(|| Error::Incompatible("namespace usage counter differs".into()))?;
    usage.object_count = usage
        .object_count
        .checked_sub(u64::from(old.is_some()))
        .and_then(|n| n.checked_add(u64::from(new.is_some())))
        .ok_or_else(|| Error::Incompatible("namespace object counter differs".into()))?;
    put(tx, Table::Reclamation, &key, &usage)
}
