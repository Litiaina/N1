use crate::distribution::dentry_quorum::{quorum_get_dentry, quorum_list_parent};
use crate::distribution::dentry_transaction::commit_namespace_transaction;
use crate::distribution::membership::active_cluster_runtime;
use crate::enums::app_error_enum::AppError;
use crate::namespace::dentry::{
    DentryKey, DentryTarget, DirectoryLayout, NamespaceDentry, TrashContext, new_directory_id,
    root_directory_id, trash_directory_id, virtual_partition,
};
use crate::namespace::paths::{base_name, is_same_or_descendant, normalize_path, parent_path};
use crate::namespace::transaction::{NamespaceTransactionLeg, PreparedNamespacePartition};
use crate::settings::config::CONFIG;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct DistributedResolvedDirectory {
    pub path: String,
    pub directory_id: String,
    pub layout: DirectoryLayout,
}

#[derive(Debug, Clone)]
pub struct DistributedResolvedEntry {
    pub path: String,
    pub parent_directory_id: String,
    pub parent_layout: DirectoryLayout,
    pub record: NamespaceDentry,
}

pub struct DistributedNamespace;

impl DistributedNamespace {
    pub async fn resolve_directory(
        fragment: &str,
        path: &str,
    ) -> Result<DistributedResolvedDirectory, AppError> {
        let path = normalize_user_path(path)?;
        let mut directory = DistributedResolvedDirectory {
            path: String::new(),
            directory_id: root_directory_id(fragment),
            layout: DirectoryLayout::default(),
        };
        for segment in path.split('/').filter(|segment| !segment.is_empty()) {
            let key = DentryKey {
                fragment: fragment.to_string(),
                parent_directory_id: directory.directory_id.clone(),
                name: segment.to_string(),
            };
            let entry = quorum_get_dentry(key, directory.layout)
                .await?
                .ok_or(AppError::NotFound)?;
            match entry.target {
                DentryTarget::Directory {
                    directory_id,
                    layout,
                } => {
                    directory = DistributedResolvedDirectory {
                        path: join_path(&directory.path, segment),
                        directory_id,
                        layout,
                    };
                }
                DentryTarget::Object { .. } => {
                    return Err(AppError::BadRequest(format!(
                        "namespace path '{}' is a file",
                        join_path(&directory.path, segment)
                    )));
                }
            }
        }
        Ok(directory)
    }

    pub async fn resolve_entry(
        fragment: &str,
        path: &str,
    ) -> Result<Option<DistributedResolvedEntry>, AppError> {
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Ok(None);
        }
        let parent = Self::resolve_directory(fragment, &parent_path(&path)).await?;
        let key = DentryKey {
            fragment: fragment.to_string(),
            parent_directory_id: parent.directory_id.clone(),
            name: base_name(&path),
        };
        Ok(quorum_get_dentry(key, parent.layout)
            .await?
            .map(|record| DistributedResolvedEntry {
                path,
                parent_directory_id: parent.directory_id,
                parent_layout: parent.layout,
                record,
            }))
    }

    pub async fn list_directory(
        fragment: &str,
        path: &str,
    ) -> Result<Vec<NamespaceDentry>, AppError> {
        let directory = Self::resolve_directory(fragment, path).await?;
        quorum_list_parent(fragment, &directory.directory_id, directory.layout).await
    }

    pub async fn list_trash(fragment: &str) -> Result<Vec<NamespaceDentry>, AppError> {
        quorum_list_parent(
            fragment,
            &trash_directory_id(fragment),
            DirectoryLayout::default(),
        )
        .await
    }

    pub async fn mkdir_all(
        fragment: &str,
        path: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<DistributedResolvedDirectory, AppError> {
        let path = normalize_user_path(path)?;
        let mut directory = DistributedResolvedDirectory {
            path: String::new(),
            directory_id: root_directory_id(fragment),
            layout: DirectoryLayout::default(),
        };
        for (index, segment) in path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .enumerate()
        {
            let key = DentryKey {
                fragment: fragment.to_string(),
                parent_directory_id: directory.directory_id.clone(),
                name: segment.to_string(),
            };
            let entry = match quorum_get_dentry(key.clone(), directory.layout).await? {
                Some(entry) => entry,
                None => {
                    let child_id = new_directory_id();
                    let child_layout = DirectoryLayout {
                        shard_bits: DirectoryLayout::default().shard_bits,
                        epoch: active_cluster_runtime()?.membership.epoch,
                    };
                    let record = NamespaceDentry {
                        key: key.clone(),
                        target: DentryTarget::Directory {
                            directory_id: child_id,
                            layout: child_layout,
                        },
                        generation: 1,
                        last_mutation_id: format!("{mutation_id}-{index}"),
                        created_at_unix: now_unix,
                        updated_at_unix: now_unix,
                        trash: None,
                    };
                    let partition = virtual_partition(
                        fragment,
                        &key.parent_directory_id,
                        &key.name,
                        directory.layout,
                    );
                    let prepared = PreparedNamespacePartition {
                        transaction_id: record.last_mutation_id.clone(),
                        virtual_partition: partition,
                        prepared_at_unix: now_unix,
                        legs: vec![NamespaceTransactionLeg::Insert {
                            record: record.clone(),
                            parent_layout: directory.layout,
                        }],
                    };
                    match commit_namespace_transaction(vec![prepared]).await {
                        Ok(_) => record,
                        Err(err @ AppError::Conflict(_)) => {
                            match quorum_get_dentry(key.clone(), directory.layout).await? {
                                Some(entry) => entry,
                                None => return Err(err),
                            }
                        }
                        Err(err) => return Err(err),
                    }
                }
            };
            match entry.target {
                DentryTarget::Directory {
                    directory_id,
                    layout,
                } => {
                    directory = DistributedResolvedDirectory {
                        path: join_path(&directory.path, segment),
                        directory_id,
                        layout,
                    };
                }
                DentryTarget::Object { .. } => {
                    return Err(AppError::Conflict(format!(
                        "cannot create directory through file '{}'",
                        join_path(&directory.path, segment)
                    )));
                }
            }
        }
        Ok(directory)
    }

    pub async fn publish_object(
        fragment: &str,
        path: &str,
        object_id: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<NamespaceDentry, AppError> {
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Err(AppError::BadRequest(
                "root cannot reference an object".into(),
            ));
        }
        let parent = Self::mkdir_all(
            fragment,
            &parent_path(&path),
            &format!("{mutation_id}-parents"),
            now_unix,
        )
        .await?;
        let key = DentryKey {
            fragment: fragment.to_string(),
            parent_directory_id: parent.directory_id,
            name: base_name(&path),
        };
        if let Some(existing) = quorum_get_dentry(key.clone(), parent.layout).await? {
            if existing.target
                == (DentryTarget::Object {
                    object_id: object_id.to_string(),
                })
            {
                return Ok(existing);
            }
            return Err(AppError::Conflict(format!(
                "namespace path '{path}' already exists"
            )));
        }
        let record = NamespaceDentry {
            key: key.clone(),
            target: DentryTarget::Object {
                object_id: object_id.to_string(),
            },
            generation: 1,
            last_mutation_id: mutation_id.to_string(),
            created_at_unix: now_unix,
            updated_at_unix: now_unix,
            trash: None,
        };
        let partition =
            virtual_partition(fragment, &key.parent_directory_id, &key.name, parent.layout);
        commit_namespace_transaction(vec![PreparedNamespacePartition {
            transaction_id: mutation_id.to_string(),
            virtual_partition: partition,
            prepared_at_unix: now_unix,
            legs: vec![NamespaceTransactionLeg::Insert {
                record: record.clone(),
                parent_layout: parent.layout,
            }],
        }])
        .await?;
        Ok(record)
    }

    pub async fn rename(
        fragment: &str,
        source_path: &str,
        destination_path: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<NamespaceDentry, AppError> {
        let source_path = normalize_user_path(source_path)?;
        let destination_path = normalize_user_path(destination_path)?;
        if source_path.is_empty() || destination_path.is_empty() {
            return Err(AppError::BadRequest("root cannot be renamed".into()));
        }
        if is_same_or_descendant(&destination_path, &source_path) {
            return Err(AppError::Conflict(
                "destination cannot be inside the source subtree".into(),
            ));
        }
        let source = Self::resolve_entry(fragment, &source_path)
            .await?
            .ok_or(AppError::NotFound)?;
        let destination_parent =
            Self::resolve_directory(fragment, &parent_path(&destination_path)).await?;
        let destination_key = DentryKey {
            fragment: fragment.to_string(),
            parent_directory_id: destination_parent.directory_id,
            name: base_name(&destination_path),
        };
        Self::move_entry(
            source,
            destination_key,
            destination_parent.layout,
            mutation_id,
            now_unix,
            None,
        )
        .await
    }

    pub async fn trash(
        fragment: &str,
        path: &str,
        trash_entry_id: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<NamespaceDentry, AppError> {
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Err(AppError::BadRequest("root cannot be trashed".into()));
        }
        let source = Self::resolve_entry(fragment, &path)
            .await?
            .ok_or(AppError::NotFound)?;
        let context = TrashContext {
            original_parent_directory_id: source.parent_directory_id.clone(),
            original_parent_layout: source.parent_layout,
            original_name: source.record.key.name.clone(),
            original_path: path,
            deleted_at_unix: now_unix,
        };
        Self::move_entry(
            source,
            DentryKey {
                fragment: fragment.to_string(),
                parent_directory_id: trash_directory_id(fragment),
                name: trash_entry_id.to_string(),
            },
            DirectoryLayout::default(),
            mutation_id,
            now_unix,
            Some(context),
        )
        .await
    }

    pub async fn restore(
        fragment: &str,
        trash_entry_id: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<NamespaceDentry, AppError> {
        let key = DentryKey {
            fragment: fragment.to_string(),
            parent_directory_id: trash_directory_id(fragment),
            name: trash_entry_id.to_string(),
        };
        let record = quorum_get_dentry(key.clone(), DirectoryLayout::default())
            .await?
            .ok_or(AppError::NotFound)?;
        let context = record.trash.clone().ok_or_else(|| {
            AppError::InternalServerError("trash dentry has no restore context".into())
        })?;
        let expires_at = context.deleted_at_unix.saturating_add(
            i64::try_from(CONFIG.garbage_collector.retention_seconds).unwrap_or(i64::MAX),
        );
        if now_unix >= expires_at {
            return Err(AppError::Conflict(
                "trash retention expired; lifecycle owns permanent deletion".into(),
            ));
        }
        let destination_key = DentryKey {
            fragment: fragment.to_string(),
            parent_directory_id: context.original_parent_directory_id.clone(),
            name: context.original_name.clone(),
        };
        let restored = Self::move_entry(
            DistributedResolvedEntry {
                path: context.original_path.clone(),
                parent_directory_id: key.parent_directory_id,
                parent_layout: DirectoryLayout::default(),
                record: record.clone(),
            },
            destination_key.clone(),
            context.original_parent_layout,
            mutation_id,
            now_unix,
            None,
        )
        .await;
        match restored {
            Ok(record) => Ok(record),
            Err(AppError::Conflict(message)) if message.contains("destination") => {
                let Some(existing) =
                    quorum_get_dentry(destination_key, context.original_parent_layout).await?
                else {
                    return Err(AppError::Conflict(message));
                };
                if existing.target == record.target && existing.trash.is_none() {
                    Ok(existing)
                } else {
                    Err(AppError::Conflict(message))
                }
            }
            Err(err) => Err(err),
        }
    }

    pub async fn purge_expired_reference(
        fragment: &str,
        trash_entry_id: &str,
        ttl_seconds: u64,
        now_unix: i64,
        mutation_id: &str,
    ) -> Result<DentryTarget, AppError> {
        let key = DentryKey {
            fragment: fragment.to_string(),
            parent_directory_id: trash_directory_id(fragment),
            name: trash_entry_id.to_string(),
        };
        let record = quorum_get_dentry(key.clone(), DirectoryLayout::default())
            .await?
            .ok_or(AppError::NotFound)?;
        let context = record.trash.as_ref().ok_or_else(|| {
            AppError::InternalServerError("trash dentry has no lifecycle context".into())
        })?;
        let expires_at = context
            .deleted_at_unix
            .saturating_add(i64::try_from(ttl_seconds).unwrap_or(i64::MAX));
        if now_unix < expires_at {
            return Err(AppError::Conflict(format!(
                "trash entry is retained until unix timestamp {expires_at}"
            )));
        }
        let partition = virtual_partition(
            fragment,
            &key.parent_directory_id,
            &key.name,
            DirectoryLayout::default(),
        );
        commit_namespace_transaction(vec![PreparedNamespacePartition {
            transaction_id: mutation_id.to_string(),
            virtual_partition: partition,
            prepared_at_unix: now_unix,
            legs: vec![NamespaceTransactionLeg::Remove {
                key,
                parent_layout: DirectoryLayout::default(),
                expected_generation: record.generation,
            }],
        }])
        .await?;
        Ok(record.target)
    }

    pub async fn collect_target_object_ids(
        fragment: &str,
        target: &DentryTarget,
    ) -> Result<BTreeSet<String>, AppError> {
        Ok(Self::collect_target_reference_counts(fragment, target)
            .await?
            .into_keys()
            .collect())
    }

    pub async fn collect_target_reference_counts(
        fragment: &str,
        target: &DentryTarget,
    ) -> Result<BTreeMap<String, u64>, AppError> {
        let mut object_ids = BTreeMap::new();
        let mut directories = Vec::new();
        match target {
            DentryTarget::Object { object_id } => {
                object_ids.insert(object_id.clone(), 1);
            }
            DentryTarget::Directory {
                directory_id,
                layout,
            } => directories.push((directory_id.clone(), *layout)),
        }
        while let Some((directory_id, layout)) = directories.pop() {
            for entry in quorum_list_parent(fragment, &directory_id, layout).await? {
                match entry.target {
                    DentryTarget::Object { object_id } => {
                        let count = object_ids.entry(object_id).or_insert(0u64);
                        *count = count.saturating_add(1);
                    }
                    DentryTarget::Directory {
                        directory_id,
                        layout,
                    } => directories.push((directory_id, layout)),
                }
            }
        }
        Ok(object_ids)
    }

    pub async fn object_reference_counts(
        fragment: &str,
    ) -> Result<BTreeMap<String, u64>, AppError> {
        let mut counts = BTreeMap::new();
        let mut directories = vec![
            (root_directory_id(fragment), DirectoryLayout::default()),
            (trash_directory_id(fragment), DirectoryLayout::default()),
        ];
        while let Some((directory_id, layout)) = directories.pop() {
            for entry in quorum_list_parent(fragment, &directory_id, layout).await? {
                match entry.target {
                    DentryTarget::Object { object_id } => {
                        let count = counts.entry(object_id).or_insert(0u64);
                        *count = count.saturating_add(1);
                    }
                    DentryTarget::Directory {
                        directory_id,
                        layout,
                    } => directories.push((directory_id, layout)),
                }
            }
        }
        Ok(counts)
    }

    pub async fn purge_expired_tree(
        fragment: &str,
        trash_entry_id: &str,
        ttl_seconds: u64,
        now_unix: i64,
        mutation_id: &str,
    ) -> Result<BTreeSet<String>, AppError> {
        let trash_key = DentryKey {
            fragment: fragment.to_string(),
            parent_directory_id: trash_directory_id(fragment),
            name: trash_entry_id.to_string(),
        };
        let trash_record = quorum_get_dentry(trash_key, DirectoryLayout::default())
            .await?
            .ok_or(AppError::NotFound)?;
        let context = trash_record.trash.as_ref().ok_or_else(|| {
            AppError::InternalServerError("trash dentry has no lifecycle context".into())
        })?;
        let expires_at = context
            .deleted_at_unix
            .saturating_add(i64::try_from(ttl_seconds).unwrap_or(i64::MAX));
        if now_unix < expires_at {
            return Err(AppError::Conflict(format!(
                "trash entry is retained until unix timestamp {expires_at}"
            )));
        }

        let mut objects = BTreeSet::new();
        let mut descendants = Vec::new();
        let mut directories = Vec::new();
        match &trash_record.target {
            DentryTarget::Object { object_id } => {
                objects.insert(object_id.clone());
            }
            DentryTarget::Directory {
                directory_id,
                layout,
            } => directories.push((directory_id.clone(), *layout)),
        }
        while let Some((directory_id, layout)) = directories.pop() {
            for entry in quorum_list_parent(fragment, &directory_id, layout).await? {
                match &entry.target {
                    DentryTarget::Object { object_id } => {
                        objects.insert(object_id.clone());
                    }
                    DentryTarget::Directory {
                        directory_id,
                        layout,
                    } => directories.push((directory_id.clone(), *layout)),
                }
                descendants.push((entry, layout));
            }
        }

        for (index, (entry, parent_layout)) in descendants.into_iter().enumerate() {
            let partition = virtual_partition(
                fragment,
                &entry.key.parent_directory_id,
                &entry.key.name,
                parent_layout,
            );
            let transaction_id = format!("{mutation_id}-descendant-{index}");
            commit_namespace_transaction(vec![PreparedNamespacePartition {
                transaction_id,
                virtual_partition: partition,
                prepared_at_unix: now_unix,
                legs: vec![NamespaceTransactionLeg::Remove {
                    key: entry.key,
                    parent_layout,
                    expected_generation: entry.generation,
                }],
            }])
            .await?;
        }
        Self::purge_expired_reference(fragment, trash_entry_id, ttl_seconds, now_unix, mutation_id)
            .await?;
        Ok(objects)
    }

    async fn move_entry(
        source: DistributedResolvedEntry,
        destination_key: DentryKey,
        destination_layout: DirectoryLayout,
        mutation_id: &str,
        now_unix: i64,
        trash: Option<TrashContext>,
    ) -> Result<NamespaceDentry, AppError> {
        if quorum_get_dentry(destination_key.clone(), destination_layout)
            .await?
            .is_some()
        {
            return Err(AppError::Conflict(format!(
                "namespace destination '{}' already exists",
                destination_key.name
            )));
        }
        let destination = NamespaceDentry {
            key: destination_key.clone(),
            target: source.record.target.clone(),
            generation: source.record.generation.saturating_add(1),
            last_mutation_id: mutation_id.to_string(),
            created_at_unix: source.record.created_at_unix,
            updated_at_unix: now_unix,
            trash,
        };
        let remove_partition = virtual_partition(
            &source.record.key.fragment,
            &source.record.key.parent_directory_id,
            &source.record.key.name,
            source.parent_layout,
        );
        let insert_partition = virtual_partition(
            &destination.key.fragment,
            &destination.key.parent_directory_id,
            &destination.key.name,
            destination_layout,
        );
        let mut partitions: BTreeMap<u64, Vec<NamespaceTransactionLeg>> = BTreeMap::new();
        partitions
            .entry(remove_partition)
            .or_default()
            .push(NamespaceTransactionLeg::Remove {
                key: source.record.key,
                parent_layout: source.parent_layout,
                expected_generation: source.record.generation,
            });
        partitions
            .entry(insert_partition)
            .or_default()
            .push(NamespaceTransactionLeg::Insert {
                record: destination.clone(),
                parent_layout: destination_layout,
            });
        commit_namespace_transaction(
            partitions
                .into_iter()
                .map(|(virtual_partition, legs)| PreparedNamespacePartition {
                    transaction_id: mutation_id.to_string(),
                    virtual_partition,
                    prepared_at_unix: now_unix,
                    legs,
                })
                .collect(),
        )
        .await?;
        Ok(destination)
    }
}

fn normalize_user_path(path: &str) -> Result<String, AppError> {
    let path = normalize_path(path).map_err(|err| AppError::BadRequest(err.to_string()))?;
    if path.split('/').any(|segment| segment == ".trash") {
        return Err(AppError::BadRequest(
            "'.trash' is reserved for N2 lifecycle management".into(),
        ));
    }
    Ok(path)
}

fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}
