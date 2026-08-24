use super::*;
use crate::distribution::membership::{ClusterNode, NodeState};

fn node(id: &str) -> ClusterNode {
    ClusterNode {
        id: id.to_string(),
        instance_id: id.to_string(),
        binding_epoch: 1,
        endpoint: format!("https://{id}:50001"),
        native_endpoint: Some(format!("n2://{id}:50002")),
        state: Some(NodeState::Online),
        weight: 1,
        storage_roots: Vec::new(),
        joined_epoch: 1,
        capacity_bytes: None,
        used_bytes: None,
        last_seen_unix: None,
    }
}

#[test]
fn fragment_assignment_spreads_across_live_nodes() {
    let fragments = vec![
        "f3".to_string(),
        "f1".to_string(),
        "f2".to_string(),
        "f4".to_string(),
    ];
    let assignments = assign_fragments_to_nodes(&fragments, &[node("node-a"), node("node-b")]);
    let by_node = assignments.into_iter().fold(
        BTreeMap::<String, usize>::new(),
        |mut counts, assignment| {
            *counts.entry(assignment.node_id).or_default() += 1;
            counts
        },
    );
    assert_eq!(by_node.get("node-a").copied(), Some(2));
    assert_eq!(by_node.get("node-b").copied(), Some(2));
}

#[test]
fn fragment_assignment_is_stable_after_sorting() {
    let left = assign_fragments_to_nodes(
        &["b".to_string(), "a".to_string(), "c".to_string()],
        &[node("node-b"), node("node-a")],
    );
    let right = assign_fragments_to_nodes(
        &["c".to_string(), "b".to_string(), "a".to_string()],
        &[node("node-a"), node("node-b")],
    );
    let left_pairs = left
        .into_iter()
        .map(|assignment| (assignment.fragment, assignment.node_id))
        .collect::<Vec<_>>();
    let right_pairs = right
        .into_iter()
        .map(|assignment| (assignment.fragment, assignment.node_id))
        .collect::<Vec<_>>();
    assert_eq!(left_pairs, right_pairs);
}
