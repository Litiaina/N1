use super::*;

pub(super) fn reserve_probe_node(reserve: &ReserveNode) -> ClusterNode {
    ClusterNode {
        id: reserve.instance_id.clone(),
        instance_id: reserve.instance_id.clone(),
        binding_epoch: 0,
        endpoint: reserve.endpoint.clone(),
        native_endpoint: reserve.native_endpoint.clone(),
        state: reserve.state.clone(),
        weight: 1,
        storage_roots: reserve.storage_roots.clone(),
        joined_epoch: 0,
        capacity_bytes: reserve.capacity_bytes,
        used_bytes: None,
        last_seen_unix: reserve.last_seen_unix,
    }
}

pub(super) fn compatible_reserve(
    failed: &ClusterNode,
    reserve: &ReserveNode,
) -> Result<(), AppError> {
    if !reserve.effective_state().accepts_new_writes() {
        return Err(AppError::ServiceUnavailable(format!(
            "reserve instance '{}' is not write-capable",
            reserve.instance_id
        )));
    }
    if failed.storage_roots.len() != reserve.storage_roots.len() {
        return Err(AppError::Conflict(format!(
            "reserve instance '{}' has {} root(s), but logical slot '{}' requires {}",
            reserve.instance_id,
            reserve.storage_roots.len(),
            failed.id,
            failed.storage_roots.len()
        )));
    }
    if let (Some(required), Some(capacity)) = (failed.used_bytes, reserve.capacity_bytes)
        && capacity < required
    {
        return Err(AppError::Conflict(format!(
            "reserve instance '{}' capacity {} is below slot '{}' recorded usage {}",
            reserve.instance_id, capacity, failed.id, required
        )));
    }
    Ok(())
}

pub(super) fn mapped_replacement_roots(
    failed: &ClusterNode,
    reserve: &ReserveNode,
) -> Result<Vec<StorageRoot>, AppError> {
    compatible_reserve(failed, reserve)?;
    let mut logical = failed.storage_roots.clone();
    logical.sort_by(|left, right| left.id.cmp(&right.id));
    let mut physical = reserve.storage_roots.clone();
    physical.sort_by(|left, right| {
        left.physical_id()
            .cmp(right.physical_id())
            .then_with(|| left.path.cmp(&right.path))
    });
    Ok(logical
        .into_iter()
        .zip(physical)
        .map(|(logical, physical)| StorageRoot {
            id: logical.id,
            physical_id: Some(physical.physical_id().to_string()),
            path: physical.path,
        })
        .collect())
}

pub fn plan_reserve_promotion(
    current: &ClusterMembership,
    slot_id: &str,
    reserve_instance_id: &str,
) -> Result<ClusterMembership, AppError> {
    current.validate()?;
    let failed_index = current
        .nodes
        .iter()
        .position(|node| node.id == slot_id)
        .ok_or_else(|| AppError::NotFound)?;
    let reserve_index = current
        .reserve_nodes
        .iter()
        .position(|node| node.instance_id == reserve_instance_id)
        .ok_or_else(|| {
            AppError::Conflict(format!(
                "physical instance '{}' is not in the reserve set",
                reserve_instance_id
            ))
        })?;
    let failed = current.nodes[failed_index].clone();
    let reserve = current.reserve_nodes[reserve_index].clone();
    let roots = mapped_replacement_roots(&failed, &reserve)?;
    let next_epoch = current
        .binding_epoch
        .checked_add(1)
        .ok_or_else(|| AppError::InternalServerError("N2 binding epoch exhausted".into()))?;

    let mut next = current.clone();
    next.binding_epoch = next_epoch;
    next.nodes[failed_index] = ClusterNode {
        id: failed.id.clone(),
        instance_id: reserve.instance_id.clone(),
        binding_epoch: next_epoch,
        endpoint: reserve.endpoint.clone(),
        native_endpoint: reserve.native_endpoint.clone(),
        state: Some(NodeState::Degraded),
        weight: failed.weight,
        storage_roots: roots,
        joined_epoch: failed.joined_epoch,
        capacity_bytes: reserve.capacity_bytes,
        used_bytes: Some(0),
        last_seen_unix: reserve.last_seen_unix,
    };
    next.reserve_nodes.remove(reserve_index);
    next.retired_instances.push(RetiredInstance {
        instance_id: failed.instance_id,
        former_slot_id: failed.id,
        fenced_at_binding_epoch: next_epoch,
        endpoint: failed.endpoint,
        native_endpoint: failed.native_endpoint,
        storage_roots: failed.storage_roots,
    });
    next.validate()?;
    Ok(next)
}

pub fn validate_reserve_transition(
    current: &ClusterMembership,
    next: &ClusterMembership,
) -> Result<(String, String), AppError> {
    current.validate()?;
    next.validate()?;
    if current.cluster_id != next.cluster_id
        || current.epoch != next.epoch
        || current.pools.len() != next.pools.len()
        || serde_json::to_value(&current.pools).ok() != serde_json::to_value(&next.pools).ok()
        || next.binding_epoch != current.binding_epoch.saturating_add(1)
        || next.nodes.len() != current.nodes.len()
        || next.reserve_nodes.len().saturating_add(1) != current.reserve_nodes.len()
        || next.retired_instances.len() != current.retired_instances.len().saturating_add(1)
    {
        return Err(AppError::Conflict(
            "reserve transition changed topology or did not advance exactly one binding".into(),
        ));
    }
    let changed = current
        .nodes
        .iter()
        .zip(&next.nodes)
        .filter(|(before, after)| {
            before.id != after.id
                || before.instance_id != after.instance_id
                || before.binding_epoch != after.binding_epoch
                || before.endpoint != after.endpoint
                || before.native_endpoint != after.native_endpoint
                || before.storage_roots != after.storage_roots
        })
        .collect::<Vec<_>>();
    if changed.len() != 1 {
        return Err(AppError::Conflict(format!(
            "reserve transition changed {} active slot bindings; exactly one is required",
            changed.len()
        )));
    }
    let (before, after) = changed[0];
    if before.id != after.id
        || after.binding_epoch != next.binding_epoch
        || !current
            .reserve_nodes
            .iter()
            .any(|reserve| reserve.instance_id == after.instance_id)
        || !next.retired_instances.iter().any(|retired| {
            retired.instance_id == before.instance_id
                && retired.former_slot_id == before.id
                && retired.fenced_at_binding_epoch == next.binding_epoch
        })
    {
        return Err(AppError::Conflict(
            "reserve transition did not preserve logical slot identity and fencing".into(),
        ));
    }
    Ok((before.id.clone(), after.instance_id.clone()))
}

pub(super) fn activation_membership(
    current: &ClusterMembership,
    slot_id: &str,
) -> Result<ClusterMembership, AppError> {
    let mut next = current.clone();
    let node = next
        .nodes
        .iter_mut()
        .find(|node| node.id == slot_id)
        .ok_or(AppError::NotFound)?;
    if node.effective_state() != NodeState::Degraded {
        return Err(AppError::Conflict(format!(
            "logical slot '{slot_id}' is not in recovering state"
        )));
    }
    node.state = Some(NodeState::Online);
    next.validate()?;
    Ok(next)
}

pub(super) fn validate_activation_transition(
    current: &ClusterMembership,
    next: &ClusterMembership,
) -> Result<String, AppError> {
    current.validate()?;
    next.validate()?;
    if current.cluster_id != next.cluster_id
        || current.epoch != next.epoch
        || current.binding_epoch != next.binding_epoch
        || serde_json::to_value(&current.pools).ok() != serde_json::to_value(&next.pools).ok()
        || serde_json::to_value(&current.reserve_nodes).ok()
            != serde_json::to_value(&next.reserve_nodes).ok()
        || serde_json::to_value(&current.retired_instances).ok()
            != serde_json::to_value(&next.retired_instances).ok()
        || current.nodes.len() != next.nodes.len()
    {
        return Err(AppError::Conflict(
            "replacement activation attempted to change topology or bindings".into(),
        ));
    }
    let changed = current
        .nodes
        .iter()
        .zip(&next.nodes)
        .filter(|(before, after)| {
            before.id != after.id
                || before.instance_id != after.instance_id
                || before.binding_epoch != after.binding_epoch
                || before.endpoint != after.endpoint
                || before.native_endpoint != after.native_endpoint
                || before.storage_roots != after.storage_roots
                || before.state != after.state
        })
        .collect::<Vec<_>>();
    if changed.len() != 1 {
        return Err(AppError::Conflict(
            "replacement activation must change exactly one slot state".into(),
        ));
    }
    let (before, after) = changed[0];
    if before.id != after.id
        || before.instance_id != after.instance_id
        || before.binding_epoch != after.binding_epoch
        || before.effective_state() != NodeState::Degraded
        || after.effective_state() != NodeState::Online
    {
        return Err(AppError::Conflict(
            "replacement activation is not a degraded-to-online transition".into(),
        ));
    }
    Ok(after.id.clone())
}

pub(super) fn promotion_plan_id(
    membership: &ClusterMembership,
    slot_id: &str,
    reserve_instance_id: &str,
) -> Result<String, AppError> {
    let mut hasher = Sha256::new();
    hasher.update(b"n2-reserve-promotion-v1");
    hasher.update([0]);
    hasher.update(membership.cluster_id.as_bytes());
    hasher.update([0]);
    hasher.update(membership.epoch.to_be_bytes());
    hasher.update(membership.binding_epoch.to_be_bytes());
    hasher.update(slot_id.as_bytes());
    hasher.update([0]);
    hasher.update(reserve_instance_id.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}
