use crate::distribution_core::model::{NodeDescriptor, ObjectIdentity};
use sha2::{Digest, Sha256};

fn rendezvous_score(identity: &ObjectIdentity, node: &NodeDescriptor) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(identity.placement_key().as_bytes());
    hasher.update(b"|");
    hasher.update(node.node_id.as_bytes());
    hasher.finalize().into()
}

pub fn rendezvous_placement(
    nodes: &[NodeDescriptor],
    identity: &ObjectIdentity,
    target_count: usize,
) -> Vec<NodeDescriptor> {
    if target_count == 0 {
        return Vec::new();
    }

    let mut scored = nodes
        .iter()
        .filter(|n| n.active)
        .map(|n| (rendezvous_score(identity, n), n))
        .collect::<Vec<_>>();

    // Highest-random-weight first with stable tie-break by node_id.
    scored.sort_by(|(score_a, node_a), (score_b, node_b)| {
        score_b
            .cmp(score_a)
            .then_with(|| node_a.node_id.cmp(&node_b.node_id))
    });

    scored
        .into_iter()
        .take(target_count)
        .map(|(_, node)| node.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str) -> NodeDescriptor {
        NodeDescriptor {
            node_id: id.to_string(),
            endpoint: format!("https://{}.n2.local", id),
            epoch: 1,
            active: true,
        }
    }

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            uid: "u1".to_string(),
            fragment: "frag".to_string(),
            object_key: "videos/x.mp4".to_string(),
            version_id: "v1".to_string(),
        }
    }

    #[test]
    fn rendezvous_is_deterministic() {
        let nodes = vec![node("a"), node("b"), node("c"), node("d")];
        let id = identity();

        let first = rendezvous_placement(&nodes, &id, 3);
        let second = rendezvous_placement(&nodes, &id, 3);
        assert_eq!(first, second);
    }

    #[test]
    fn rendezvous_respects_target_count_and_active_nodes() {
        let mut nodes = vec![node("a"), node("b"), node("c"), node("d")];
        nodes[2].active = false;
        let id = identity();

        let out = rendezvous_placement(&nodes, &id, 3);
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|n| n.active));
        assert!(out.iter().all(|n| n.node_id != "c"));
    }

    #[test]
    fn rendezvous_two_node_multi_fragment_spreads_primary() {
        let nodes = vec![node("node-a"), node("node-b")];
        let mut primary_hits = std::collections::HashMap::<String, usize>::new();

        for fragment in ["frag-a", "frag-b"] {
            for i in 0..64usize {
                let id = ObjectIdentity {
                    uid: fragment.to_string(),
                    fragment: fragment.to_string(),
                    object_key: format!("videos/object-{}.bin", i),
                    version_id: format!("v{}", i),
                };
                let out = rendezvous_placement(&nodes, &id, 2);
                assert_eq!(out.len(), 2);
                assert_ne!(out[0].node_id, out[1].node_id);
                *primary_hits.entry(out[0].node_id.clone()).or_insert(0) += 1;
            }
        }

        assert_eq!(primary_hits.len(), 2);
    }
}
