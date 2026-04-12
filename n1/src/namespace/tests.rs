use super::catalog::NamespaceCatalog;
use super::models::{LifecycleState, NamespaceEntryKind};

#[test]
fn directory_rename_preserves_object_identity_and_versions() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    catalog.mkdir("images", 100).unwrap();
    catalog
        .put_file(
            "images/cover.jpg",
            "obj-1",
            "ver-1",
            "storage-1",
            "image/jpeg",
            4_096,
            262_144,
            101,
        )
        .unwrap();

    let summary = catalog.rename_path("images", "My Images", 200).unwrap();
    assert_eq!(summary.affected_paths, 2);
    assert_eq!(summary.affected_objects, 1);

    assert!(catalog.entry("images").unwrap().is_none());
    let entry = catalog.entry("My Images/cover.jpg").unwrap().unwrap();
    assert_eq!(entry.kind, NamespaceEntryKind::File);
    assert_eq!(entry.object_id.as_deref(), Some("obj-1"));

    let head = catalog.object_head("obj-1").unwrap();
    assert_eq!(head.current_version_id, "ver-1");
    assert_eq!(head.storage_object_id, "storage-1");
    assert!(head.lifecycle.is_live());
}

#[test]
fn trash_and_recover_preserve_visible_paths_and_object_identity() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    catalog
        .put_file(
            "docs/a.txt",
            "obj-a",
            "ver-a",
            "storage-a",
            "text/plain",
            12,
            262_144,
            100,
        )
        .unwrap();
    catalog
        .put_file(
            "docs/b.txt",
            "obj-b",
            "ver-b",
            "storage-b",
            "text/plain",
            12,
            262_144,
            101,
        )
        .unwrap();

    let trash = catalog.trash_path("docs", 200).unwrap();
    assert_eq!(trash.affected_paths, 3);
    assert_eq!(trash.affected_objects, 2);
    assert!(catalog.entry("docs").unwrap().is_none());
    assert!(
        catalog
            .trashed_entry("docs")
            .unwrap()
            .unwrap()
            .lifecycle
            .is_trashed()
    );
    assert!(catalog.trashed_entry("docs/a.txt").unwrap().is_some());
    assert!(catalog.object_head("obj-a").unwrap().lifecycle.is_trashed());

    let trash_root = catalog.list_dir("", true).unwrap();
    assert_eq!(trash_root.len(), 1);
    assert_eq!(trash_root[0].path, "docs");

    let recover = catalog.recover_path("docs", 260).unwrap();
    assert_eq!(recover.affected_objects, 2);
    assert!(catalog.entry("docs").unwrap().unwrap().lifecycle.is_live());
    assert!(catalog.trashed_entry("docs").unwrap().is_none());
    assert!(catalog.object_head("obj-a").unwrap().lifecycle.is_live());
}

#[test]
fn explicit_directories_exist_without_marker_objects() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    assert!(catalog.mkdir("empty", 10).unwrap());
    assert!(!catalog.mkdir("empty", 11).unwrap());

    let root = catalog.list_dir("", false).unwrap();
    assert_eq!(root.len(), 1);
    assert_eq!(root[0].path, "empty");
    assert_eq!(root[0].kind, NamespaceEntryKind::Directory);
    assert_eq!(root[0].object_id, None);
    assert_eq!(root[0].lifecycle, LifecycleState::Live);
}

#[test]
fn list_page_paginates_logical_files_and_prefixes_together() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    catalog.mkdir("My Files", 10).unwrap();
    catalog
        .put_file(
            "Screenshots/cover.jpg",
            "obj-cover",
            "ver-cover",
            "storage-cover",
            "image/jpeg",
            1_024,
            262_144,
            11,
        )
        .unwrap();
    catalog
        .put_file(
            "mhw.mp4",
            "obj-video",
            "ver-video",
            "storage-video",
            "video/mp4",
            2_048,
            262_144,
            12,
        )
        .unwrap();
    catalog.mkdir("videos", 13).unwrap();

    let page_one = catalog.list_page(None, Some("/"), 2, None, false).unwrap();
    assert_eq!(page_one.files.len() + page_one.common_prefixes.len(), 2);
    assert_eq!(
        page_one.common_prefixes,
        vec!["My Files/".to_string(), "Screenshots/".to_string()]
    );
    assert!(page_one.files.is_empty());
    assert!(page_one.continuation_token.is_some());

    let page_two = catalog
        .list_page(
            None,
            Some("/"),
            2,
            page_one.continuation_token.as_deref(),
            false,
        )
        .unwrap();
    assert_eq!(page_two.files.len() + page_two.common_prefixes.len(), 2);
    assert_eq!(page_two.common_prefixes, vec!["videos/".to_string()]);
    assert_eq!(page_two.files[0].path, "mhw.mp4");
    assert!(page_two.continuation_token.is_none());
}

#[test]
fn delimiter_listing_only_returns_immediate_prefixes_for_nested_directories() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    catalog
        .mkdir("Patients/26-0993 TEVES, MARIA/MiniViewer", 10)
        .unwrap();
    catalog
        .put_file(
            "Images/20251117_162552.jpg",
            "obj-image",
            "ver-image",
            "storage-image",
            "image/jpeg",
            1_024,
            262_144,
            11,
        )
        .unwrap();

    let page = catalog
        .list_page(None, Some("/"), 200, None, false)
        .unwrap();
    assert_eq!(page.files.len(), 0);
    assert_eq!(
        page.common_prefixes,
        vec!["Images/".to_string(), "Patients/".to_string()]
    );
}

#[test]
fn recover_all_and_empty_trash_follow_the_new_lifecycle_model() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    catalog
        .put_file(
            "docs/a.txt",
            "obj-a",
            "ver-a",
            "storage-a",
            "text/plain",
            12,
            262_144,
            100,
        )
        .unwrap();
    catalog
        .put_file(
            "docs/b.txt",
            "obj-b",
            "ver-b",
            "storage-b",
            "text/plain",
            12,
            262_144,
            101,
        )
        .unwrap();
    catalog
        .put_file(
            "media/c.txt",
            "obj-c",
            "ver-c",
            "storage-c",
            "text/plain",
            12,
            262_144,
            102,
        )
        .unwrap();

    catalog.trash_path("docs", 200).unwrap();
    catalog.trash_path("media/c.txt", 201).unwrap();

    let recover_all = catalog.recover_all(260);
    assert_eq!(recover_all.operation, "recover_all");
    assert_eq!(recover_all.affected_objects, 3);
    assert!(catalog.entry("docs").unwrap().is_some());
    assert!(catalog.entry("media/c.txt").unwrap().is_some());

    catalog.trash_path("docs", 300).unwrap();
    let empty = catalog.empty_trash(320);
    assert_eq!(empty.operation, "empty_trash");
    assert_eq!(empty.affected_objects, 2);
    assert!(catalog.trashed_entry("docs").unwrap().is_none());
    assert!(catalog.object_head("obj-a").is_none());
    assert!(catalog.object_head("obj-b").is_none());
}

#[test]
fn trash_listing_materializes_virtual_images_directory_for_nested_files() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    catalog
        .put_file(
            "images/photo.jpg",
            "obj-image",
            "ver-image",
            "storage-image",
            "image/jpeg",
            1_024,
            262_144,
            100,
        )
        .unwrap();

    catalog.trash_path("images/photo.jpg", 200).unwrap();

    let trash_root = catalog.list_dir("", true).unwrap();
    assert_eq!(trash_root.len(), 1);
    assert_eq!(trash_root[0].path, "images");
    assert_eq!(trash_root[0].kind, NamespaceEntryKind::Directory);
    assert_eq!(trash_root[0].object_id, None);
    assert!(catalog.trashed_entry("images").unwrap().is_some());

    let images_dir = catalog.list_dir("images", true).unwrap();
    assert_eq!(images_dir.len(), 1);
    assert_eq!(images_dir[0].path, "images/photo.jpg");
    assert_eq!(images_dir[0].kind, NamespaceEntryKind::File);
    assert_eq!(images_dir[0].object_id.as_deref(), Some("obj-image"));
}

#[test]
fn recover_and_empty_trash_clean_up_virtual_images_directory() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    catalog
        .put_file(
            "images/photo.jpg",
            "obj-image",
            "ver-image",
            "storage-image",
            "image/jpeg",
            1_024,
            262_144,
            100,
        )
        .unwrap();

    catalog.trash_path("images/photo.jpg", 200).unwrap();
    let recover = catalog.recover_path("images/photo.jpg", 260).unwrap();
    assert_eq!(recover.operation, "recover");
    assert!(catalog.list_dir("", true).unwrap().is_empty());
    assert!(catalog.trashed_entry("images").unwrap().is_none());
    assert!(catalog.entry("images/photo.jpg").unwrap().is_some());

    catalog.trash_path("images/photo.jpg", 300).unwrap();
    let empty = catalog.empty_trash(320);
    assert_eq!(empty.operation, "empty_trash");
    assert!(catalog.list_dir("", true).unwrap().is_empty());
    assert!(catalog.trashed_entry("images").unwrap().is_none());
    assert!(catalog.object_head("obj-image").is_none());
}

#[test]
fn recover_virtual_screenshots_directory_restores_single_deleted_child() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    catalog
        .put_file(
            "Screenshots/shot-1.png",
            "obj-shot-1",
            "ver-shot-1",
            "storage-shot-1",
            "image/png",
            2_048,
            262_144,
            100,
        )
        .unwrap();

    catalog.trash_path("Screenshots/shot-1.png", 200).unwrap();

    let recover = catalog.recover_path("Screenshots", 260).unwrap();
    assert_eq!(recover.operation, "recover");
    assert_eq!(recover.kind, NamespaceEntryKind::Directory);
    assert_eq!(recover.affected_objects, 1);
    assert!(catalog.list_dir("", true).unwrap().is_empty());
    assert!(catalog.entry("Screenshots/shot-1.png").unwrap().is_some());
}

#[test]
fn recover_virtual_screenshots_directory_restores_multiple_deleted_children() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    for (path, object_id, version_id, storage_object_id) in [
        (
            "Screenshots/shot-1.png",
            "obj-shot-1",
            "ver-shot-1",
            "storage-shot-1",
        ),
        (
            "Screenshots/shot-2.png",
            "obj-shot-2",
            "ver-shot-2",
            "storage-shot-2",
        ),
    ] {
        catalog
            .put_file(
                path,
                object_id,
                version_id,
                storage_object_id,
                "image/png",
                2_048,
                262_144,
                100,
            )
            .unwrap();
        catalog.trash_path(path, 200).unwrap();
    }

    let trash_root = catalog.list_dir("", true).unwrap();
    assert_eq!(trash_root.len(), 1);
    assert_eq!(trash_root[0].path, "Screenshots");
    assert_eq!(trash_root[0].kind, NamespaceEntryKind::Directory);

    let recover = catalog.recover_path("Screenshots", 260).unwrap();
    assert_eq!(recover.operation, "recover");
    assert_eq!(recover.kind, NamespaceEntryKind::Directory);
    assert_eq!(recover.affected_objects, 2);
    assert!(catalog.list_dir("", true).unwrap().is_empty());
    assert!(catalog.entry("Screenshots/shot-1.png").unwrap().is_some());
    assert!(catalog.entry("Screenshots/shot-2.png").unwrap().is_some());
}

#[test]
fn empty_trash_removes_children_represented_by_virtual_screenshots_directory() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    for (path, object_id, version_id, storage_object_id) in [
        (
            "Screenshots/shot-1.png",
            "obj-shot-1",
            "ver-shot-1",
            "storage-shot-1",
        ),
        (
            "Screenshots/shot-2.png",
            "obj-shot-2",
            "ver-shot-2",
            "storage-shot-2",
        ),
    ] {
        catalog
            .put_file(
                path,
                object_id,
                version_id,
                storage_object_id,
                "image/png",
                2_048,
                262_144,
                100,
            )
            .unwrap();
        catalog.trash_path(path, 200).unwrap();
    }

    let empty = catalog.empty_trash(320);
    assert_eq!(empty.operation, "empty_trash");
    assert_eq!(empty.affected_objects, 2);
    assert!(catalog.list_dir("", true).unwrap().is_empty());
    assert!(catalog.trashed_entry("Screenshots").unwrap().is_none());
    assert!(catalog.object_head("obj-shot-1").is_none());
    assert!(catalog.object_head("obj-shot-2").is_none());
}

#[test]
fn trash_listing_materializes_virtual_directory_chain_for_deep_paths() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    catalog
        .put_file(
            "videos/2025/My Videos/clip.mp4",
            "obj-video",
            "ver-video",
            "storage-video",
            "video/mp4",
            4_096,
            262_144,
            100,
        )
        .unwrap();

    catalog
        .trash_path("videos/2025/My Videos/clip.mp4", 200)
        .unwrap();

    let trash_root = catalog.list_dir("", true).unwrap();
    assert_eq!(trash_root.len(), 1);
    assert_eq!(trash_root[0].path, "videos");
    assert_eq!(trash_root[0].kind, NamespaceEntryKind::Directory);
    assert!(catalog.trashed_entry("videos").unwrap().is_some());

    let videos_dir = catalog.list_dir("videos", true).unwrap();
    assert_eq!(videos_dir.len(), 1);
    assert_eq!(videos_dir[0].path, "videos/2025");
    assert_eq!(videos_dir[0].kind, NamespaceEntryKind::Directory);
    assert!(catalog.trashed_entry("videos/2025").unwrap().is_some());

    let year_dir = catalog.list_dir("videos/2025", true).unwrap();
    assert_eq!(year_dir.len(), 1);
    assert_eq!(year_dir[0].path, "videos/2025/My Videos");
    assert_eq!(year_dir[0].kind, NamespaceEntryKind::Directory);
    assert!(
        catalog
            .trashed_entry("videos/2025/My Videos")
            .unwrap()
            .is_some()
    );

    let nested_dir = catalog.list_dir("videos/2025/My Videos", true).unwrap();
    assert_eq!(nested_dir.len(), 1);
    assert_eq!(nested_dir[0].path, "videos/2025/My Videos/clip.mp4");
    assert_eq!(nested_dir[0].kind, NamespaceEntryKind::File);
    assert_eq!(nested_dir[0].object_id.as_deref(), Some("obj-video"));
}

#[test]
fn recover_and_empty_trash_clean_up_virtual_directory_chain_for_deep_paths() {
    let mut catalog = NamespaceCatalog::new("fragment-a");
    catalog
        .put_file(
            "videos/2025/My Videos/clip.mp4",
            "obj-video",
            "ver-video",
            "storage-video",
            "video/mp4",
            4_096,
            262_144,
            100,
        )
        .unwrap();

    catalog
        .trash_path("videos/2025/My Videos/clip.mp4", 200)
        .unwrap();
    let recover = catalog
        .recover_path("videos/2025/My Videos/clip.mp4", 260)
        .unwrap();
    assert_eq!(recover.operation, "recover");
    assert!(catalog.list_dir("", true).unwrap().is_empty());
    assert!(catalog.trashed_entry("videos").unwrap().is_none());
    assert!(catalog.trashed_entry("videos/2025").unwrap().is_none());
    assert!(
        catalog
            .trashed_entry("videos/2025/My Videos")
            .unwrap()
            .is_none()
    );
    assert!(
        catalog
            .entry("videos/2025/My Videos/clip.mp4")
            .unwrap()
            .is_some()
    );

    catalog
        .trash_path("videos/2025/My Videos/clip.mp4", 300)
        .unwrap();
    let empty = catalog.empty_trash(320);
    assert_eq!(empty.operation, "empty_trash");
    assert!(catalog.list_dir("", true).unwrap().is_empty());
    assert!(catalog.trashed_entry("videos").unwrap().is_none());
    assert!(catalog.trashed_entry("videos/2025").unwrap().is_none());
    assert!(
        catalog
            .trashed_entry("videos/2025/My Videos")
            .unwrap()
            .is_none()
    );
    assert!(catalog.object_head("obj-video").is_none());
}
