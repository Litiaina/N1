use crate::distribution::inode::{
    InodeReadKind, PeerInodeReadRequest, ReplicatedInodeMutation, commit_replicated_mutation,
    commit_replicated_transaction, create_replicated_inode, quorum_read, select_inode_layout,
};
use crate::distribution::membership::{ClusterRuntime, active_cluster_runtime};
use crate::enums::app_error_enum::AppError;
use crate::namespace::catalog::{FragmentNamespace, resolve_fragment_namespace};
use crate::namespace::inode_store::{
    DirectoryInode, InodeDentry, InodeDentryTarget, InodeLayout, InodeMutationChange,
    PreparedInodeMutation, TrashOrigin,
};
use crate::namespace::paths::{base_name, is_same_or_descendant, normalize_path, parent_path};
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use ulid::Ulid;

#[derive(Debug, Clone)]
pub struct DistributedResolvedDirectory {
    pub path: String,
    pub inode: DirectoryInode,
}

#[derive(Debug, Clone)]
pub struct DistributedResolvedEntry {
    pub path: String,
    pub parent: DirectoryInode,
    pub record: InodeDentry,
}

pub struct DistributedNamespace;

impl DistributedNamespace {
    pub async fn resolve_directory(
        fragment: &str,
        path: &str,
    ) -> Result<DistributedResolvedDirectory, AppError> {
        let runtime = active_cluster_runtime()?;
        let namespace = resolve_fragment_namespace(runtime.clone(), fragment).await?;
        resolve_directory_with(runtime, &namespace, path).await
    }

    pub async fn resolve_entry(
        fragment: &str,
        path: &str,
    ) -> Result<Option<DistributedResolvedEntry>, AppError> {
        let runtime = active_cluster_runtime()?;
        let namespace = resolve_fragment_namespace(runtime.clone(), fragment).await?;
        resolve_entry_with(runtime, &namespace, path).await
    }

    pub async fn list_directory(fragment: &str, path: &str) -> Result<Vec<InodeDentry>, AppError> {
        let directory = Self::resolve_directory(fragment, path).await?;
        list_inode(&directory.inode).await
    }

    pub async fn list_trash(fragment: &str) -> Result<Vec<InodeDentry>, AppError> {
        let runtime = active_cluster_runtime()?;
        let namespace = resolve_fragment_namespace(runtime, fragment).await?;
        let trash = read_inode(
            &namespace.stable_fragment_id,
            &namespace.trash_inode_id,
            &namespace.trash_layout,
        )
        .await?;
        list_inode(&trash).await
    }

    pub async fn mkdir_all(
        fragment: &str,
        path: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<DistributedResolvedDirectory, AppError> {
        crate::core::operation_gate::ensure_client_mutation_allowed()?;
        let runtime = active_cluster_runtime()?;
        let namespace = resolve_fragment_namespace(runtime.clone(), fragment).await?;
        let path = normalize_user_path(path)?;
        let mut directory = read_root(&namespace).await?;
        let mut resolved_path = String::new();

        for (index, segment) in path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .enumerate()
        {
            if let Some(entry) = read_dentry(&directory, segment).await? {
                directory = directory_from_target(
                    &namespace.stable_fragment_id,
                    &entry,
                    &join_path(&resolved_path, segment),
                )
                .await?;
                resolved_path = join_path(&resolved_path, segment);
                continue;
            }

            let inode_id = Ulid::new().to_string();
            let layout =
                select_inode_layout(&runtime, &namespace.stable_fragment_id, &inode_id).await?;
            let child = DirectoryInode::new(
                namespace.stable_fragment_id.clone(),
                inode_id.clone(),
                Some(directory.inode_id.clone()),
                layout.clone(),
                unix_seconds_to_nanos(now_unix),
            )?;
            create_replicated_inode(runtime.clone(), child.clone()).await?;
            let dentry = InodeDentry::live(
                segment.to_string(),
                InodeDentryTarget::Directory { inode_id, layout },
                unix_seconds_to_nanos(now_unix),
            )?;
            let mutation = PreparedInodeMutation::new(
                namespace.stable_fragment_id.clone(),
                directory.inode_id.clone(),
                directory.generation,
                format!("{mutation_id}-{index}"),
                unix_seconds_to_nanos(now_unix),
                vec![InodeMutationChange::Upsert { dentry }],
            )?;
            match commit_replicated_mutation(runtime.clone(), directory.layout.clone(), mutation)
                .await
            {
                Ok(_) => directory = child,
                Err(error) => {
                    let refreshed = read_inode(
                        &namespace.stable_fragment_id,
                        &directory.inode_id,
                        &directory.layout,
                    )
                    .await?;
                    match read_dentry(&refreshed, segment).await? {
                        Some(entry) => {
                            directory = directory_from_target(
                                &namespace.stable_fragment_id,
                                &entry,
                                &join_path(&resolved_path, segment),
                            )
                            .await?;
                        }
                        None => return Err(error),
                    }
                }
            }
            resolved_path = join_path(&resolved_path, segment);
        }
        Ok(DistributedResolvedDirectory {
            path: resolved_path,
            inode: directory,
        })
    }

    pub async fn publish_object(
        fragment: &str,
        path: &str,
        object_id: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<InodeDentry, AppError> {
        crate::core::operation_gate::ensure_client_mutation_allowed()?;
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
        let name = base_name(&path);
        if let Some(existing) = read_dentry(&parent.inode, &name).await? {
            if existing.target
                == (InodeDentryTarget::Object {
                    object_id: object_id.into(),
                })
            {
                return Ok(existing);
            }
            return Err(AppError::Conflict(format!(
                "namespace path '{path}' already exists"
            )));
        }
        let dentry = InodeDentry::live(
            name,
            InodeDentryTarget::Object {
                object_id: object_id.into(),
            },
            unix_seconds_to_nanos(now_unix),
        )?;
        commit_single(
            active_cluster_runtime()?,
            &parent.inode,
            mutation_id,
            now_unix,
            vec![InodeMutationChange::Upsert {
                dentry: dentry.clone(),
            }],
        )
        .await?;
        Ok(dentry)
    }

    pub async fn rename(
        fragment: &str,
        source_path: &str,
        destination_path: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<InodeDentry, AppError> {
        crate::core::operation_gate::ensure_client_mutation_allowed()?;
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
        let runtime = active_cluster_runtime()?;
        let namespace = resolve_fragment_namespace(runtime.clone(), fragment).await?;
        let source = resolve_entry_with(runtime.clone(), &namespace, &source_path)
            .await?
            .ok_or(AppError::NotFound)?;
        let destination_parent =
            resolve_directory_with(runtime.clone(), &namespace, &parent_path(&destination_path))
                .await?;
        let destination_name = base_name(&destination_path);
        if read_dentry(&destination_parent.inode, &destination_name)
            .await?
            .is_some()
        {
            return Err(AppError::Conflict(format!(
                "destination '{destination_path}' already exists"
            )));
        }
        let mut moved = source.record.clone();
        moved.name = destination_name;
        moved.trash = None;
        move_between_inodes(
            runtime,
            &namespace.stable_fragment_id,
            &source.parent,
            &source.record.name,
            &destination_parent.inode,
            moved.clone(),
            mutation_id,
            now_unix,
        )
        .await?;
        Ok(moved)
    }

    pub async fn trash(
        fragment: &str,
        path: &str,
        trash_entry_id: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<InodeDentry, AppError> {
        crate::core::operation_gate::ensure_client_mutation_allowed()?;
        let path = normalize_user_path(path)?;
        if path.is_empty() {
            return Err(AppError::BadRequest("root cannot be trashed".into()));
        }
        let runtime = active_cluster_runtime()?;
        let namespace = resolve_fragment_namespace(runtime.clone(), fragment).await?;
        let source = resolve_entry_with(runtime.clone(), &namespace, &path)
            .await?
            .ok_or(AppError::NotFound)?;
        let trash = read_inode(
            &namespace.stable_fragment_id,
            &namespace.trash_inode_id,
            &namespace.trash_layout,
        )
        .await?;
        if read_dentry(&trash, trash_entry_id).await?.is_some() {
            return Err(AppError::Conflict(
                "trash entry identity already exists".into(),
            ));
        }
        let mut moved = source.record.clone();
        moved.name = trash_entry_id.into();
        moved.trash = Some(TrashOrigin {
            parent_inode_id: source.parent.inode_id.clone(),
            parent_layout: source.parent.layout.clone(),
            name: source.record.name.clone(),
            path: path.clone(),
            deleted_at_unix_nanos: unix_seconds_to_nanos(now_unix),
        });
        move_between_inodes(
            runtime,
            &namespace.stable_fragment_id,
            &source.parent,
            &source.record.name,
            &trash,
            moved.clone(),
            mutation_id,
            now_unix,
        )
        .await?;
        Ok(moved)
    }

    pub async fn restore(
        fragment: &str,
        trash_entry_id: &str,
        mutation_id: &str,
        now_unix: i64,
    ) -> Result<InodeDentry, AppError> {
        crate::core::operation_gate::ensure_client_mutation_allowed()?;
        let runtime = active_cluster_runtime()?;
        let namespace = resolve_fragment_namespace(runtime.clone(), fragment).await?;
        let trash = read_inode(
            &namespace.stable_fragment_id,
            &namespace.trash_inode_id,
            &namespace.trash_layout,
        )
        .await?;
        let source = read_dentry(&trash, trash_entry_id)
            .await?
            .ok_or(AppError::NotFound)?;
        let origin = source
            .trash
            .clone()
            .ok_or_else(|| AppError::Conflict("trash entry has no restore origin".into()))?;
        let live_parent =
            resolve_directory_with(runtime.clone(), &namespace, &parent_path(&origin.path))
                .await
                .map_err(|_| {
                    AppError::Conflict("restore parent is not live; restore it first".into())
                })?;
        if live_parent.inode.inode_id != origin.parent_inode_id
            || live_parent.inode.layout != origin.parent_layout
        {
            return Err(AppError::Conflict(
                "restore parent identity no longer matches the recorded origin".into(),
            ));
        }
        if read_dentry(&live_parent.inode, &origin.name)
            .await?
            .is_some()
        {
            return Err(AppError::Conflict(format!(
                "restore destination '{}' already exists",
                origin.path
            )));
        }
        let mut restored = source.clone();
        restored.name = origin.name;
        restored.trash = None;
        move_between_inodes(
            runtime,
            &namespace.stable_fragment_id,
            &trash,
            trash_entry_id,
            &live_parent.inode,
            restored.clone(),
            mutation_id,
            now_unix,
        )
        .await?;
        Ok(restored)
    }

    pub async fn purge_expired(
        fragment: &str,
        trash_entry_id: &str,
        ttl_seconds: u64,
        now_unix: i64,
        mutation_id: &str,
    ) -> Result<InodeDentryTarget, AppError> {
        crate::core::operation_gate::ensure_client_mutation_allowed()?;
        let runtime = active_cluster_runtime()?;
        let namespace = resolve_fragment_namespace(runtime.clone(), fragment).await?;
        let trash = read_inode(
            &namespace.stable_fragment_id,
            &namespace.trash_inode_id,
            &namespace.trash_layout,
        )
        .await?;
        let entry = read_dentry(&trash, trash_entry_id)
            .await?
            .ok_or(AppError::NotFound)?;
        let origin = entry
            .trash
            .as_ref()
            .ok_or_else(|| AppError::Conflict("trash entry has no lifecycle timestamp".into()))?;
        let expires_at = nanos_to_unix_seconds(origin.deleted_at_unix_nanos)
            .saturating_add(i64::try_from(ttl_seconds).unwrap_or(i64::MAX));
        if now_unix < expires_at {
            return Err(AppError::Conflict(format!(
                "trash entry is retained until unix timestamp {expires_at}"
            )));
        }
        commit_single(
            runtime,
            &trash,
            mutation_id,
            now_unix,
            vec![InodeMutationChange::Remove {
                name: trash_entry_id.into(),
            }],
        )
        .await?;
        Ok(entry.target)
    }

    pub async fn list_inode_dentries(
        fragment: &str,
        inode_id: &str,
        layout: &InodeLayout,
    ) -> Result<Vec<InodeDentry>, AppError> {
        let inode = read_inode(fragment, inode_id, layout).await?;
        list_inode(&inode).await
    }

    pub async fn collect_target_reference_counts(
        fragment: &str,
        target: &InodeDentryTarget,
    ) -> Result<BTreeMap<String, u64>, AppError> {
        let namespace = resolve_fragment_namespace(active_cluster_runtime()?, fragment).await?;
        let mut counts = BTreeMap::new();
        let mut directories = Vec::new();
        collect_target(target, &mut counts, &mut directories)?;
        while let Some((inode_id, layout)) = directories.pop() {
            for entry in
                Self::list_inode_dentries(&namespace.stable_fragment_id, &inode_id, &layout).await?
            {
                collect_target(&entry.target, &mut counts, &mut directories)?;
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
        let entry = Self::list_trash(fragment)
            .await?
            .into_iter()
            .find(|entry| entry.name == trash_entry_id)
            .ok_or(AppError::NotFound)?;
        let objects = Self::collect_target_reference_counts(fragment, &entry.target)
            .await?
            .into_keys()
            .collect();
        Self::purge_expired(fragment, trash_entry_id, ttl_seconds, now_unix, mutation_id).await?;
        Ok(objects)
    }
}

fn collect_target(
    target: &InodeDentryTarget,
    counts: &mut BTreeMap<String, u64>,
    directories: &mut Vec<(String, InodeLayout)>,
) -> Result<(), AppError> {
    match target {
        InodeDentryTarget::Object { object_id } => {
            let count = counts.entry(object_id.clone()).or_insert(0u64);
            *count = count.saturating_add(1);
        }
        InodeDentryTarget::Directory { inode_id, layout } => {
            directories.push((inode_id.clone(), layout.clone()));
        }
        InodeDentryTarget::Symlink { .. } => {}
        InodeDentryTarget::Fragment { .. } => {
            return Err(AppError::Conflict(
                "fragment catalog target appeared inside a fragment namespace".into(),
            ));
        }
    }
    Ok(())
}

async fn read_root(namespace: &FragmentNamespace) -> Result<DirectoryInode, AppError> {
    read_inode(
        &namespace.stable_fragment_id,
        &namespace.root_inode_id,
        &namespace.root_layout,
    )
    .await
}

async fn read_inode(
    fragment_id: &str,
    inode_id: &str,
    layout: &InodeLayout,
) -> Result<DirectoryInode, AppError> {
    Ok(quorum_read(
        active_cluster_runtime()?,
        PeerInodeReadRequest {
            fragment_id: fragment_id.into(),
            inode_id: inode_id.into(),
            layout: layout.clone(),
            kind: InodeReadKind::Inode,
            name: None,
        },
    )
    .await?
    .inode)
}

async fn read_dentry(
    directory: &DirectoryInode,
    name: &str,
) -> Result<Option<InodeDentry>, AppError> {
    Ok(quorum_read(
        active_cluster_runtime()?,
        PeerInodeReadRequest {
            fragment_id: directory.fragment_id.clone(),
            inode_id: directory.inode_id.clone(),
            layout: directory.layout.clone(),
            kind: InodeReadKind::Dentry,
            name: Some(name.into()),
        },
    )
    .await?
    .dentry)
}

async fn list_inode(directory: &DirectoryInode) -> Result<Vec<InodeDentry>, AppError> {
    Ok(quorum_read(
        active_cluster_runtime()?,
        PeerInodeReadRequest {
            fragment_id: directory.fragment_id.clone(),
            inode_id: directory.inode_id.clone(),
            layout: directory.layout.clone(),
            kind: InodeReadKind::List,
            name: None,
        },
    )
    .await?
    .entries)
}

async fn resolve_directory_with(
    runtime: Arc<ClusterRuntime>,
    namespace: &FragmentNamespace,
    path: &str,
) -> Result<DistributedResolvedDirectory, AppError> {
    let path = normalize_user_path(path)?;
    let mut inode = read_root(namespace).await?;
    let mut resolved_path = String::new();
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        let entry = quorum_read(
            runtime.clone(),
            PeerInodeReadRequest {
                fragment_id: namespace.stable_fragment_id.clone(),
                inode_id: inode.inode_id.clone(),
                layout: inode.layout.clone(),
                kind: InodeReadKind::Dentry,
                name: Some(segment.into()),
            },
        )
        .await?
        .dentry
        .ok_or(AppError::NotFound)?;
        resolved_path = join_path(&resolved_path, segment);
        inode =
            directory_from_target(&namespace.stable_fragment_id, &entry, &resolved_path).await?;
    }
    Ok(DistributedResolvedDirectory {
        path: resolved_path,
        inode,
    })
}

async fn resolve_entry_with(
    runtime: Arc<ClusterRuntime>,
    namespace: &FragmentNamespace,
    path: &str,
) -> Result<Option<DistributedResolvedEntry>, AppError> {
    let path = normalize_user_path(path)?;
    if path.is_empty() {
        return Ok(None);
    }
    let parent = resolve_directory_with(runtime.clone(), namespace, &parent_path(&path)).await?;
    let record = quorum_read(
        runtime,
        PeerInodeReadRequest {
            fragment_id: namespace.stable_fragment_id.clone(),
            inode_id: parent.inode.inode_id.clone(),
            layout: parent.inode.layout.clone(),
            kind: InodeReadKind::Dentry,
            name: Some(base_name(&path)),
        },
    )
    .await?
    .dentry;
    Ok(record.map(|record| DistributedResolvedEntry {
        path,
        parent: parent.inode,
        record,
    }))
}

async fn directory_from_target(
    fragment_id: &str,
    entry: &InodeDentry,
    path: &str,
) -> Result<DirectoryInode, AppError> {
    let InodeDentryTarget::Directory { inode_id, layout } = &entry.target else {
        return Err(AppError::BadRequest(format!(
            "namespace path '{path}' is a file, not a directory"
        )));
    };
    read_inode(fragment_id, inode_id, layout).await
}

async fn commit_single(
    runtime: Arc<ClusterRuntime>,
    inode: &DirectoryInode,
    mutation_id: &str,
    now_unix: i64,
    changes: Vec<InodeMutationChange>,
) -> Result<(), AppError> {
    let mutation = PreparedInodeMutation::new(
        inode.fragment_id.clone(),
        inode.inode_id.clone(),
        inode.generation,
        mutation_id.into(),
        unix_seconds_to_nanos(now_unix),
        changes,
    )?;
    commit_replicated_mutation(runtime, inode.layout.clone(), mutation).await?;
    Ok(())
}

async fn move_between_inodes(
    runtime: Arc<ClusterRuntime>,
    fragment_id: &str,
    source: &DirectoryInode,
    source_name: &str,
    destination: &DirectoryInode,
    moved: InodeDentry,
    mutation_id: &str,
    now_unix: i64,
) -> Result<(), AppError> {
    let now = unix_seconds_to_nanos(now_unix);
    if source.inode_id == destination.inode_id {
        return commit_single(
            runtime,
            source,
            mutation_id,
            now_unix,
            vec![
                InodeMutationChange::Remove {
                    name: source_name.into(),
                },
                InodeMutationChange::Upsert { dentry: moved },
            ],
        )
        .await;
    }
    let moved_directory = match &moved.target {
        InodeDentryTarget::Directory { inode_id, layout } => {
            Some(read_inode(fragment_id, inode_id, layout).await?)
        }
        _ => None,
    };
    let source_mutation = PreparedInodeMutation::new(
        fragment_id.into(),
        source.inode_id.clone(),
        source.generation,
        format!("{mutation_id}-source"),
        now,
        vec![InodeMutationChange::Remove {
            name: source_name.into(),
        }],
    )?;
    let destination_mutation = PreparedInodeMutation::new(
        fragment_id.into(),
        destination.inode_id.clone(),
        destination.generation,
        format!("{mutation_id}-destination"),
        now,
        vec![InodeMutationChange::Upsert { dentry: moved }],
    )?;
    let mut participants = vec![
        ReplicatedInodeMutation {
            layout: source.layout.clone(),
            mutation: source_mutation,
        },
        ReplicatedInodeMutation {
            layout: destination.layout.clone(),
            mutation: destination_mutation,
        },
    ];
    if let Some(directory) = moved_directory {
        let parent_mutation = PreparedInodeMutation::new_parent_move(
            fragment_id.into(),
            directory.inode_id.clone(),
            directory.generation,
            format!("{mutation_id}-moved-inode"),
            now,
            destination.inode_id.clone(),
        )?;
        participants.push(ReplicatedInodeMutation {
            layout: directory.layout,
            mutation: parent_mutation,
        });
    }
    commit_replicated_transaction(runtime, mutation_id.into(), participants).await?;
    Ok(())
}

fn normalize_user_path(path: &str) -> Result<String, AppError> {
    let path = normalize_path(path).map_err(|error| AppError::BadRequest(error.to_string()))?;
    if path.split('/').any(|segment| segment == ".trash") {
        return Err(AppError::BadRequest(
            "'.trash' is reserved for N2 lifecycle management".into(),
        ));
    }
    Ok(path)
}

fn unix_seconds_to_nanos(seconds: i64) -> i64 {
    seconds.saturating_mul(1_000_000_000)
}

fn nanos_to_unix_seconds(nanos: i64) -> i64 {
    nanos / 1_000_000_000
}

fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.into()
    } else {
        format!("{parent}/{name}")
    }
}

pub fn now_unix() -> i64 {
    Utc::now().timestamp()
}
