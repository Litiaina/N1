use super::*;

#[test]
fn public_mutation_set_has_no_manual_purge_operations() {
    let encoded = serde_json::to_string(&[
        NamespaceMutationKind::Mkdir,
        NamespaceMutationKind::Rename,
        NamespaceMutationKind::Trash,
        NamespaceMutationKind::Recover,
        NamespaceMutationKind::UploadCommit,
    ])
    .expect("encode mutation kinds");
    assert!(!encoded.contains("purge"));
    assert!(!encoded.contains("empty"));
    assert!(!encoded.contains("recover_all"));
}

#[test]
fn zero_decode_listing_entry_has_no_object_metadata() {
    let entry = zero_decode_file_entry("fragment", "videos/a.mp4", "a.mp4");
    assert!(entry.object_id.is_none());
    assert!(entry.version_id.is_none());
    assert!(entry.size_bytes.is_none());
}

#[test]
fn recovery_group_reports_the_component_aligned_common_parent() {
    assert_eq!(
        common_parent_for_keys(["videos/a.mp4", "videos/nested/b.mp4"].into_iter()),
        "videos"
    );
    assert_eq!(common_parent_for_keys(["a/file", "b/file"].into_iter()), "");
}
