use crate::distribution::inode::{
    InodeReadKind, InodeRepairOutcome, PeerInodeReadRequest, commit_replicated_mutation,
    create_replicated_inode, inspect_and_repair_inode, quorum_read, select_inode_layout,
};
use crate::distribution::membership::{ClusterRuntime, NodeRole};
use crate::enums::app_error_enum::AppError;
use crate::namespace::inode_store::{
    DirectoryInode, InodeDentry, InodeDentryTarget, InodeLayout, InodeMutationChange, InodeReplica,
    PreparedInodeMutation,
};
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub(crate) const CATALOG_INODE_ID: &str = "fragment-catalog";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentNamespace {
    pub stable_fragment_id: String,
    pub root_inode_id: String,
    pub root_layout: InodeLayout,
    pub trash_inode_id: String,
    pub trash_layout: InodeLayout,
}

fn now_nanos() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp().saturating_mul(1_000_000_000))
}

pub(crate) fn catalog_fragment_id(cluster_id: &str) -> String {
    let digest = Sha256::digest(cluster_id.as_bytes());
    format!("cluster-{}", hex::encode(&digest[..16]))
}

pub async fn inspect_and_repair_namespace_catalog(
    runtime: Arc<ClusterRuntime>,
) -> InodeRepairOutcome {
    let layout = match catalog_layout(&runtime) {
        Ok(layout) => layout,
        Err(error) => {
            return InodeRepairOutcome {
                snapshot: None,
                observations: Vec::new(),
                degraded: true,
                deferred: false,
                repaired_replicas: 0,
                healthy: false,
                error: Some(error.to_string()),
            };
        }
    };
    inspect_and_repair_inode(
        runtime.clone(),
        &catalog_fragment_id(&runtime.membership.cluster_id),
        CATALOG_INODE_ID,
        &layout,
    )
    .await
}

pub fn catalog_layout(runtime: &ClusterRuntime) -> Result<InodeLayout, AppError> {
    let replicas = runtime
        .membership
        .namespace_catalog
        .replicas
        .iter()
        .map(|catalog| {
            let node = runtime
                .membership
                .nodes
                .iter()
                .find(|node| node.id == catalog.node_id)
                .ok_or_else(|| {
                    AppError::Conflict(format!(
                        "namespace catalog references missing node '{}'",
                        catalog.node_id
                    ))
                })?;
            if node.role != NodeRole::Namespace
                || !node
                    .storage_roots
                    .iter()
                    .any(|root| root.id == catalog.root_id)
            {
                return Err(AppError::Conflict(format!(
                    "namespace catalog replica '{}:{}' is invalid",
                    catalog.node_id, catalog.root_id
                )));
            }
            Ok(InodeReplica {
                node_id: node.id.clone(),
                root_id: catalog.root_id.clone(),
                endpoint: node.endpoint.clone(),
                native_endpoint: node.native_endpoint.clone(),
            })
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    let layout = InodeLayout {
        generation: runtime.membership.namespace_catalog.generation,
        replicas,
    };
    layout.validate()?;
    Ok(layout)
}

pub async fn ensure_namespace_catalog(runtime: Arc<ClusterRuntime>) -> Result<(), AppError> {
    let layout = catalog_layout(&runtime)?;
    let inode = DirectoryInode::new(
        catalog_fragment_id(&runtime.membership.cluster_id),
        CATALOG_INODE_ID.into(),
        None,
        layout,
        now_nanos(),
    )?;
    create_replicated_inode(runtime, inode).await
}

fn reference_from_dentry(dentry: InodeDentry) -> Result<FragmentNamespace, AppError> {
    match dentry.target {
        InodeDentryTarget::Fragment {
            stable_fragment_id,
            root_inode_id,
            root_layout,
            trash_inode_id,
            trash_layout,
        } => Ok(FragmentNamespace {
            stable_fragment_id,
            root_inode_id,
            root_layout,
            trash_inode_id,
            trash_layout,
        }),
        _ => Err(AppError::Conflict(
            "fragment catalog entry has a non-fragment target".into(),
        )),
    }
}

pub async fn resolve_fragment_namespace(
    runtime: Arc<ClusterRuntime>,
    alias_or_stable_id: &str,
) -> Result<FragmentNamespace, AppError> {
    let layout = catalog_layout(&runtime)?;
    let fragment_id = catalog_fragment_id(&runtime.membership.cluster_id);
    for name in [
        alias_or_stable_id.to_string(),
        format!("@stable_{alias_or_stable_id}"),
    ] {
        let response = quorum_read(
            runtime.clone(),
            PeerInodeReadRequest {
                fragment_id: fragment_id.clone(),
                inode_id: CATALOG_INODE_ID.into(),
                layout: layout.clone(),
                kind: InodeReadKind::Dentry,
                name: Some(name),
            },
        )
        .await?;
        if let Some(dentry) = response.dentry {
            return reference_from_dentry(dentry);
        }
    }
    Err(AppError::NotFound)
}

pub async fn provision_fragment_namespace(
    runtime: Arc<ClusterRuntime>,
    alias_hash: &str,
    stable_fragment_id: &str,
) -> Result<FragmentNamespace, AppError> {
    ensure_namespace_catalog(runtime.clone()).await?;
    if let Ok(existing) = resolve_fragment_namespace(runtime.clone(), alias_hash).await {
        if existing.stable_fragment_id == stable_fragment_id {
            return Ok(existing);
        }
        return Err(AppError::Conflict(
            "fragment alias already points to another stable fragment".into(),
        ));
    }

    let root_inode_id = format!("root-{stable_fragment_id}");
    let trash_inode_id = format!("trash-{stable_fragment_id}");
    let root_layout = select_inode_layout(&runtime, stable_fragment_id, &root_inode_id).await?;
    let trash_layout = select_inode_layout(&runtime, stable_fragment_id, &trash_inode_id).await?;
    let now = now_nanos();
    create_replicated_inode(
        runtime.clone(),
        DirectoryInode::new(
            stable_fragment_id.into(),
            root_inode_id.clone(),
            None,
            root_layout.clone(),
            now,
        )?,
    )
    .await?;
    create_replicated_inode(
        runtime.clone(),
        DirectoryInode::new(
            stable_fragment_id.into(),
            trash_inode_id.clone(),
            None,
            trash_layout.clone(),
            now,
        )?,
    )
    .await?;

    let reference = FragmentNamespace {
        stable_fragment_id: stable_fragment_id.into(),
        root_inode_id,
        root_layout,
        trash_inode_id,
        trash_layout,
    };
    let catalog_layout = catalog_layout(&runtime)?;
    let catalog_fragment = catalog_fragment_id(&runtime.membership.cluster_id);
    let catalog = quorum_read(
        runtime.clone(),
        PeerInodeReadRequest {
            fragment_id: catalog_fragment.clone(),
            inode_id: CATALOG_INODE_ID.into(),
            layout: catalog_layout.clone(),
            kind: InodeReadKind::Inode,
            name: None,
        },
    )
    .await?;
    let target = InodeDentryTarget::Fragment {
        stable_fragment_id: reference.stable_fragment_id.clone(),
        root_inode_id: reference.root_inode_id.clone(),
        root_layout: reference.root_layout.clone(),
        trash_inode_id: reference.trash_inode_id.clone(),
        trash_layout: reference.trash_layout.clone(),
    };
    let changes = [
        alias_hash.to_string(),
        format!("@stable_{stable_fragment_id}"),
    ]
    .into_iter()
    .map(|name| {
        Ok(InodeMutationChange::Upsert {
            dentry: InodeDentry::live(name, target.clone(), now)?,
        })
    })
    .collect::<Result<Vec<_>, AppError>>()?;
    let mutation = PreparedInodeMutation::new(
        catalog_fragment,
        CATALOG_INODE_ID.into(),
        catalog.inode.generation,
        format!("fragment-provision-{stable_fragment_id}"),
        now,
        changes,
    )?;
    commit_replicated_mutation(runtime, catalog_layout, mutation).await?;
    Ok(reference)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::layout::{ClusterLayoutMode, ClusterLayoutPolicy};
    use crate::distribution::membership::{
        ClusterMembership, ClusterNode, ClusterPolicy, N2_MEMBERSHIP_FORMAT_VERSION,
        NamespaceCatalogLayout, NamespaceCatalogReplica, NodeState, StorageRoot,
    };
    use std::path::PathBuf;

    #[test]
    fn catalog_layout_is_exact_and_independent_of_blob_nodes() {
        let nodes = vec![
            ClusterNode {
                id: "ns-a".into(),
                role: NodeRole::Namespace,
                endpoint: "https://ns-a".into(),
                native_endpoint: None,
                state: Some(NodeState::Online),
                weight: 1,
                storage_roots: vec![StorageRoot {
                    id: "root-a".into(),
                    path: "/ns-a".into(),
                }],
                joined_epoch: 1,
                capacity_bytes: None,
                used_bytes: None,
                last_seen_unix: None,
            },
            ClusterNode {
                id: "ns-b".into(),
                role: NodeRole::Namespace,
                endpoint: "https://ns-b".into(),
                native_endpoint: None,
                state: Some(NodeState::Online),
                weight: 1,
                storage_roots: vec![StorageRoot {
                    id: "root-b".into(),
                    path: "/ns-b".into(),
                }],
                joined_epoch: 1,
                capacity_bytes: None,
                used_bytes: None,
                last_seen_unix: None,
            },
        ];
        let runtime = ClusterRuntime {
            local_node_id: "ns-a".into(),
            local_role: NodeRole::Namespace,
            local_storage_roots: nodes[0].storage_roots.clone(),
            membership_path: PathBuf::new(),
            layout_policy: ClusterLayoutPolicy {
                mode: ClusterLayoutMode::Fixed,
                fixed_data_shards: Some(1),
                fixed_parity_shards: Some(1),
                ..ClusterLayoutPolicy::default()
            },
            membership: ClusterMembership {
                format_version: N2_MEMBERSHIP_FORMAT_VERSION,
                cluster_id: "cluster".into(),
                epoch: 1,
                policy: ClusterPolicy {
                    namespace_redundancy: 2,
                    object_data_shards: 1,
                    object_parity_shards: 1,
                },
                namespace_catalog: NamespaceCatalogLayout {
                    generation: 1,
                    replicas: vec![
                        NamespaceCatalogReplica {
                            node_id: "ns-a".into(),
                            root_id: "root-a".into(),
                        },
                        NamespaceCatalogReplica {
                            node_id: "ns-b".into(),
                            root_id: "root-b".into(),
                        },
                    ],
                },
                nodes,
            },
            auto_rebalance: false,
        };
        let layout = catalog_layout(&runtime).expect("catalog layout");
        assert_eq!(layout.replicas.len(), 2);
        assert_eq!(layout.replicas[0].root_id, "root-a");
        assert_eq!(layout.replicas[1].root_id, "root-b");
    }
}
