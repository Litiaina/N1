use super::*;

impl NcnxStore {
    pub fn new(root_id: impl Into<String>, storage_root: impl Into<PathBuf>) -> Self {
        Self {
            root_id: root_id.into(),
            storage_root: storage_root.into(),
        }
    }

    pub fn root_id(&self) -> &str {
        &self.root_id
    }

    pub fn namespace_root(&self, fragment_id: &str) -> PathBuf {
        physical_fragment_root_on(&self.storage_root, fragment_id).join(NAMESPACE_DIR)
    }

    pub fn trash_root(&self, fragment_id: &str) -> PathBuf {
        physical_fragment_root_on(&self.storage_root, fragment_id).join(NAMESPACE_TRASH_DIR)
    }

    pub fn live_path(&self, fragment_id: &str, logical_key: &str) -> Result<PathBuf, AppError> {
        let key = validate_logical_key(logical_key)?;
        let parent = parent_path(&key);
        let name = base_name(&key);
        Ok(self
            .namespace_root(fragment_id)
            .join(parent)
            .join(format!("{name}{NCNX_SUFFIX}")))
    }

    pub fn trash_path(
        &self,
        fragment_id: &str,
        logical_key: &str,
        deleted_at_unix_nanos: i64,
        delete_mutation_id: &str,
    ) -> Result<PathBuf, AppError> {
        let key = validate_logical_key(logical_key)?;
        let parent = parent_path(&key);
        let name = base_name(&key);
        let generation = trash_generation_name(deleted_at_unix_nanos, delete_mutation_id)?;
        Ok(self
            .trash_root(fragment_id)
            .join(generation)
            .join(parent)
            .join(format!("{name}{NCNX_SUFFIX}")))
    }

    pub fn read_live(
        &self,
        fragment_id: &str,
        logical_key: &str,
    ) -> Result<Option<(NcnxRecord, Vec<u8>)>, AppError> {
        read_record_if_present(&self.live_path(fragment_id, logical_key)?)
    }

    pub fn read_trash(
        &self,
        fragment_id: &str,
        locator: &TrashLocator,
    ) -> Result<Option<(NcnxRecord, Vec<u8>)>, AppError> {
        read_record_if_present(&self.trash_path(
            fragment_id,
            &locator.logical_key,
            locator.deleted_at_unix_nanos,
            &locator.delete_mutation_id,
        )?)
    }

    /// Immutable create. Existing identical bytes make retry idempotent;
    /// existing different bytes are a same-key conflict.
    pub fn create_live(
        &self,
        fragment_id: &str,
        logical_key: &str,
        bytes: &[u8],
    ) -> Result<bool, AppError> {
        let record = NcnxRecord::decode(bytes)?;
        if record.fragment_id != fragment_id {
            return Err(AppError::Conflict(
                "NCNX fragment identity differs from publication target".into(),
            ));
        }
        create_immutable(&self.live_path(fragment_id, logical_key)?, bytes)
    }

    /// Publishes a bounded group of live NCNX records with one durability
    /// barrier per affected directory. Every file body is still fdatasync'd
    /// before its immutable link becomes visible. Existing byte-identical
    /// records make a retry idempotent; any other generation is a hard
    /// conflict so replicas can never silently diverge.
    pub fn create_live_batch(
        &self,
        fragment_id: &str,
        entries: &[(String, Vec<u8>)],
    ) -> Result<usize, AppError> {
        if entries.is_empty() {
            return Ok(0);
        }

        let mut keys = BTreeSet::new();
        let mut prepared = Vec::with_capacity(entries.len());
        for (logical_key, bytes) in entries {
            if !keys.insert(logical_key.as_str()) {
                return Err(AppError::Conflict(format!(
                    "duplicate NCNX key '{logical_key}' in one publication batch"
                )));
            }
            let desired = NcnxRecord::decode(bytes)?;
            if desired.fragment_id != fragment_id {
                return Err(AppError::Conflict(
                    "NCNX fragment identity differs from publication target".into(),
                ));
            }
            prepared.push((self.live_path(fragment_id, logical_key)?, bytes.clone()));
        }
        create_immutable_batch(prepared)
    }

    pub fn create_trash(
        &self,
        fragment_id: &str,
        locator: &TrashLocator,
        bytes: &[u8],
    ) -> Result<bool, AppError> {
        let record = NcnxRecord::decode(bytes)?;
        if record.fragment_id != fragment_id {
            return Err(AppError::Conflict(
                "NCNX fragment identity differs from trash target".into(),
            ));
        }
        create_immutable(
            &self.trash_path(
                fragment_id,
                &locator.logical_key,
                locator.deleted_at_unix_nanos,
                &locator.delete_mutation_id,
            )?,
            bytes,
        )
    }

    /// Publishes independent trash generations as one bounded durability
    /// group. Trash remains immutable and each logical key keeps its own
    /// deletion timestamp and mutation identity.
    pub fn create_trash_batch(
        &self,
        fragment_id: &str,
        entries: &[(TrashLocator, Vec<u8>)],
    ) -> Result<usize, AppError> {
        let mut identities = BTreeSet::new();
        let mut prepared = Vec::with_capacity(entries.len());
        for (locator, bytes) in entries {
            let identity = (
                locator.logical_key.as_str(),
                locator.deleted_at_unix_nanos,
                locator.delete_mutation_id.as_str(),
            );
            if !identities.insert(identity) {
                return Err(AppError::Conflict(format!(
                    "duplicate trash generation for '{}' in one publication batch",
                    locator.logical_key
                )));
            }
            let record = NcnxRecord::decode(bytes)?;
            if record.fragment_id != fragment_id {
                return Err(AppError::Conflict(
                    "NCNX fragment identity differs from trash target".into(),
                ));
            }
            prepared.push((
                self.trash_path(
                    fragment_id,
                    &locator.logical_key,
                    locator.deleted_at_unix_nanos,
                    &locator.delete_mutation_id,
                )?,
                bytes.clone(),
            ));
        }
        create_immutable_batch(prepared)
    }

    pub fn remove_live_if_exact(
        &self,
        fragment_id: &str,
        logical_key: &str,
        expected_bytes: &[u8],
    ) -> Result<bool, AppError> {
        remove_if_exact(&self.live_path(fragment_id, logical_key)?, expected_bytes)
    }

    /// Removes exact live pointers in parallel and coalesces parent-directory
    /// durability. A changed pointer is never removed.
    pub fn remove_live_batch_if_exact(
        &self,
        fragment_id: &str,
        entries: &[(String, Vec<u8>)],
    ) -> Result<usize, AppError> {
        let mut keys = BTreeSet::new();
        let mut paths = Vec::with_capacity(entries.len());
        for (logical_key, expected) in entries {
            if !keys.insert(logical_key.as_str()) {
                return Err(AppError::Conflict(format!(
                    "duplicate NCNX key '{logical_key}' in one removal batch"
                )));
            }
            paths.push((self.live_path(fragment_id, logical_key)?, expected));
        }

        let results = paths
            .par_iter()
            .map(|(path, expected)| remove_if_exact_deferred(path, expected))
            .collect::<Vec<_>>();
        let mut parents = BTreeSet::new();
        let mut changed = 0usize;
        let mut first_error = None;
        for result in results {
            match result {
                Ok((true, Some(parent))) => {
                    changed = changed.saturating_add(1);
                    parents.insert(parent);
                }
                Ok(_) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        sync_directories(&parents)?;
        match first_error {
            Some(error) => Err(error),
            None => Ok(changed),
        }
    }

    pub fn remove_trash_if_exact(
        &self,
        fragment_id: &str,
        locator: &TrashLocator,
        expected_bytes: &[u8],
    ) -> Result<bool, AppError> {
        let trash_root = self.trash_root(fragment_id);
        let path = self.trash_path(
            fragment_id,
            &locator.logical_key,
            locator.deleted_at_unix_nanos,
            &locator.delete_mutation_id,
        )?;
        let removed = remove_if_exact(&path, expected_bytes)?;
        if removed {
            prune_empty_trash_parents(&path, &trash_root)?;
        }
        Ok(removed)
    }

    /// Removes exact trash generations in parallel, then coalesces the
    /// directory durability barriers and prunes only empty generation paths.
    pub fn remove_trash_batch_if_exact(
        &self,
        fragment_id: &str,
        entries: &[(TrashLocator, Vec<u8>)],
    ) -> Result<usize, AppError> {
        let trash_root = self.trash_root(fragment_id);
        let mut identities = BTreeSet::new();
        let mut paths = Vec::with_capacity(entries.len());
        for (locator, expected) in entries {
            let identity = (
                locator.logical_key.as_str(),
                locator.deleted_at_unix_nanos,
                locator.delete_mutation_id.as_str(),
            );
            if !identities.insert(identity) {
                return Err(AppError::Conflict(format!(
                    "duplicate trash generation for '{}' in one removal batch",
                    locator.logical_key
                )));
            }
            paths.push((
                self.trash_path(
                    fragment_id,
                    &locator.logical_key,
                    locator.deleted_at_unix_nanos,
                    &locator.delete_mutation_id,
                )?,
                expected,
            ));
        }

        let results = paths
            .par_iter()
            .map(|(path, expected)| {
                remove_if_exact_deferred(path, expected)
                    .map(|(changed, parent)| (path.clone(), changed, parent))
            })
            .collect::<Vec<_>>();
        let mut parents = BTreeSet::new();
        let mut removed_paths = Vec::new();
        let mut first_error = None;
        for result in results {
            match result {
                Ok((path, true, Some(parent))) => {
                    parents.insert(parent);
                    removed_paths.push(path);
                }
                Ok(_) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        sync_directories(&parents)?;
        for path in &removed_paths {
            prune_empty_trash_parents(path, &trash_root)?;
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(removed_paths.len()),
        }
    }

    pub fn repair_live(
        &self,
        fragment_id: &str,
        logical_key: &str,
        bytes: &[u8],
    ) -> Result<bool, AppError> {
        NcnxRecord::decode(bytes)?;
        replace_durable(&self.live_path(fragment_id, logical_key)?, bytes)
    }

    pub fn repair_trash(
        &self,
        fragment_id: &str,
        locator: &TrashLocator,
        bytes: &[u8],
    ) -> Result<bool, AppError> {
        NcnxRecord::decode(bytes)?;
        replace_durable(
            &self.trash_path(
                fragment_id,
                &locator.logical_key,
                locator.deleted_at_unix_nanos,
                &locator.delete_mutation_id,
            )?,
            bytes,
        )
    }

    /// Streams filesystem names only. NCNX bodies are never opened here.
    pub fn list_live_names(
        &self,
        fragment_id: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<NcnxListItem>, AppError> {
        let prefix = validate_prefix(prefix)?;
        list_names(
            &self.namespace_root(fragment_id),
            &prefix,
            after,
            false,
            limit,
        )
    }

    pub fn list_trash_names(
        &self,
        fragment_id: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<NcnxListItem>, AppError> {
        let prefix = validate_prefix(prefix)?;
        list_generation_trash_names(&self.trash_root(fragment_id), &prefix, after, limit)
    }
}
