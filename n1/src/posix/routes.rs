use std::sync::Arc;

use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::enums::app_error_enum::AppError;
use crate::namespace::models::NamespaceEntryKind;
use crate::namespace::runtime::global_namespace_runtime;
use crate::posix::models::{
    PosixEntry, PosixListQuery, PosixListResponse, PosixMutationResponse, PosixNodeKind,
    PosixPathRequest, PosixRenameRequest, PosixStatQuery, PosixStatResponse,
};
use crate::routes::storage_routes::AppState;
use crate::structs::auth_structs::Claims;

async fn posix_list(
    claims: Claims,
    Query(query): Query<PosixListQuery>,
) -> Result<Json<PosixListResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let scan_trash = query.trash.unwrap_or(false);
    let path = query.path.unwrap_or_default();
    let limit = query.limit.unwrap_or(200).clamp(1, 1000);
    let (entries, continuation_token) = global_namespace_runtime()
        .list_dir_page(
            &claims.fragment,
            &path,
            scan_trash,
            limit,
            query.continuation_token.as_deref(),
        )
        .await?;
    let object_ids = entries
        .iter()
        .filter_map(|entry| {
            (entry.kind == NamespaceEntryKind::File)
                .then(|| entry.object_id.clone())
                .flatten()
        })
        .collect::<Vec<_>>();
    let heads = global_namespace_runtime()
        .object_heads(&claims.fragment, &object_ids)
        .await?;
    let mut mapped = entries
        .into_iter()
        .map(|entry| PosixEntry {
            size_bytes: entry
                .object_id
                .as_ref()
                .and_then(|object_id| heads.get(object_id).map(|head| head.total_size_bytes)),
            name: entry.name,
            path: entry.path,
            kind: match entry.kind {
                NamespaceEntryKind::File => PosixNodeKind::File,
                NamespaceEntryKind::Directory => PosixNodeKind::Directory,
            },
            modified_at_unix: Some(entry.updated_at_unix),
            content_type: None,
            version_id: None,
            trash_entry_id: entry.trash_entry_id,
            deleted_at_unix: entry.deleted_at_unix,
        })
        .collect::<Vec<_>>();
    mapped.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(Json(PosixListResponse {
        path,
        scan_trash,
        entries: mapped,
        continuation_token,
    }))
}

async fn posix_stat(
    claims: Claims,
    Query(query): Query<PosixStatQuery>,
) -> Result<Json<PosixStatResponse>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    let path = query.path.unwrap_or_default();
    let scan_trash = query.trash.unwrap_or(false);
    let entry = if scan_trash {
        global_namespace_runtime()
            .trashed_entry(&claims.fragment, &path)
            .await?
    } else {
        global_namespace_runtime()
            .entry(&claims.fragment, &path)
            .await?
    }
    .ok_or(AppError::NotFound)?;
    let kind = match entry.kind {
        NamespaceEntryKind::File => PosixNodeKind::File,
        NamespaceEntryKind::Directory => PosixNodeKind::Directory,
    };
    let is_virtual = false;
    let (size_bytes, content_type, version_id) = if entry.kind == NamespaceEntryKind::File {
        let head = global_namespace_runtime()
            .head_for_path(&claims.fragment, &entry.path)
            .await?;
        (
            head.as_ref().map(|value| value.total_size_bytes),
            head.as_ref().map(|value| value.content_type.clone()),
            head.as_ref().map(|value| value.current_version_id.clone()),
        )
    } else {
        (None, None, None)
    };
    let _ = scan_trash;
    Ok(Json(PosixStatResponse {
        path: entry.path,
        scan_trash,
        kind,
        is_virtual,
        size_bytes,
        modified_at_unix: Some(entry.updated_at_unix),
        content_type,
        version_id,
        trash_entry_id: entry.trash_entry_id,
        deleted_at_unix: entry.deleted_at_unix,
    }))
}

async fn posix_mkdir(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Json(req): Json<PosixPathRequest>,
) -> Result<Json<PosixMutationResponse>, AppError> {
    let _ = state;
    if !claims.write {
        return Err(AppError::Forbidden("Write permission required".into()));
    }
    let created = global_namespace_runtime()
        .mkdir(&claims.fragment, &req.path)
        .await?;
    Ok(Json(PosixMutationResponse {
        operation: "mkdir".to_string(),
        kind: PosixNodeKind::Directory,
        affected: usize::from(created),
        path: Some(req.path),
        source_path: None,
        destination_path: None,
        created: Some(created),
        trash_entry_id: None,
        deleted_at_unix: None,
    }))
}

async fn posix_rename(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Json(req): Json<PosixRenameRequest>,
) -> Result<Json<PosixMutationResponse>, AppError> {
    let _ = state;
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    let summary = global_namespace_runtime()
        .rename(&claims.fragment, &req.source_path, &req.destination_path)
        .await?;
    Ok(Json(PosixMutationResponse {
        operation: summary.operation,
        kind: match summary.kind {
            NamespaceEntryKind::File => PosixNodeKind::File,
            NamespaceEntryKind::Directory => PosixNodeKind::Directory,
        },
        affected: summary.affected_paths,
        path: None,
        source_path: summary.source_path,
        destination_path: summary.destination_path,
        created: None,
        trash_entry_id: summary.trash_entry_id,
        deleted_at_unix: summary.deleted_at_unix,
    }))
}

async fn posix_trash(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Json(req): Json<PosixPathRequest>,
) -> Result<Json<PosixMutationResponse>, AppError> {
    let _ = state;
    let summary = global_namespace_runtime()
        .trash(&claims.fragment, &req.path)
        .await?;
    Ok(Json(PosixMutationResponse {
        operation: summary.operation,
        kind: match summary.kind {
            NamespaceEntryKind::File => PosixNodeKind::File,
            NamespaceEntryKind::Directory => PosixNodeKind::Directory,
        },
        affected: summary.affected_paths,
        path: summary.source_path,
        source_path: None,
        destination_path: None,
        created: None,
        trash_entry_id: summary.trash_entry_id,
        deleted_at_unix: summary.deleted_at_unix,
    }))
}

async fn posix_recover(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Json(req): Json<PosixPathRequest>,
) -> Result<Json<PosixMutationResponse>, AppError> {
    let _ = state;
    let summary = match req.trash_entry_id.as_deref().map(str::trim) {
        Some(trash_entry_id) if !trash_entry_id.is_empty() => {
            global_namespace_runtime()
                .recover_trash_entry_id(&claims.fragment, trash_entry_id)
                .await?
        }
        _ => {
            global_namespace_runtime()
                .recover(&claims.fragment, &req.path)
                .await?
        }
    };
    Ok(Json(PosixMutationResponse {
        operation: summary.operation,
        kind: match summary.kind {
            NamespaceEntryKind::File => PosixNodeKind::File,
            NamespaceEntryKind::Directory => PosixNodeKind::Directory,
        },
        affected: summary.affected_paths,
        path: summary.source_path,
        source_path: None,
        destination_path: None,
        created: None,
        trash_entry_id: summary.trash_entry_id,
        deleted_at_unix: summary.deleted_at_unix,
    }))
}

async fn posix_purge(
    State(state): State<Arc<AppState>>,
    claims: Claims,
    Json(req): Json<PosixPathRequest>,
) -> Result<Json<PosixMutationResponse>, AppError> {
    let _ = state;
    if !claims.delete {
        return Err(AppError::Forbidden("Delete permission required".into()));
    }
    let summary = match req.trash_entry_id.as_deref().map(str::trim) {
        Some(trash_entry_id) if !trash_entry_id.is_empty() => {
            global_namespace_runtime()
                .purge_trash_entry_id(&claims.fragment, trash_entry_id)
                .await?
        }
        _ => {
            global_namespace_runtime()
                .purge_trash_path(&claims.fragment, &req.path)
                .await?
        }
    };
    Ok(Json(PosixMutationResponse {
        operation: summary.operation,
        kind: match summary.kind {
            NamespaceEntryKind::File => PosixNodeKind::File,
            NamespaceEntryKind::Directory => PosixNodeKind::Directory,
        },
        affected: summary.affected_paths,
        path: summary.source_path,
        source_path: None,
        destination_path: None,
        created: None,
        trash_entry_id: summary.trash_entry_id,
        deleted_at_unix: summary.deleted_at_unix,
    }))
}

pub fn build_posix_read_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n1/posix/list", get(posix_list))
        .route("/n1/posix/stat", get(posix_stat))
}

pub fn build_posix_write_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n1/posix/mkdir", post(posix_mkdir))
        .route("/n1/posix/rename", post(posix_rename))
        .route("/n1/posix/move", post(posix_rename))
        .route("/n1/posix/trash", post(posix_trash))
        .route("/n1/posix/recover", post(posix_recover))
        .route("/n1/posix/purge", post(posix_purge))
}

#[cfg(test)]
mod tests {
    use super::{posix_list, posix_mkdir, posix_purge, posix_recover, posix_trash};
    use crate::core::disk_manager::{
        BLOB_STORE_DISKS, FS_STORAGE_ROOT, INDEX_STORE_DISKS, ObjectMetadata,
    };
    use crate::core::fragment_metadata::{
        DEFAULT_INDEX_PARTITION_COUNT, clear_fragment_partition_count_cache_for_test,
        set_configured_index_partition_count,
    };
    use crate::core::indexdb_store;
    use crate::core::quota_manager::init_quota_file;
    use crate::core::sync::ensure_fragment_blob_directories;
    use crate::posix::models::{PosixListQuery, PosixPathRequest};
    use crate::routes::storage_routes::AppState;
    use crate::settings::constants::CONFIG_FILE;
    use crate::settings::create_config::create_default_config_ini;
    use crate::structs::auth_structs::Claims;
    use crate::test_support::global_test_lock;
    use crate::websocket::manager::WsConnectionManager;
    use axum::Json;
    use axum::extract::{Query, State};
    use sha2::{Digest, Sha256};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::fs;
    use ulid::Ulid;

    fn ensure_test_config_file() {
        let config_path = PathBuf::from(CONFIG_FILE);
        if !config_path.exists() {
            create_default_config_ini(&config_path).expect("create default test config");
        }
    }

    async fn ensure_storage_config() {
        ensure_test_config_file();
        set_configured_index_partition_count(DEFAULT_INDEX_PARTITION_COUNT)
            .expect("reset configured index partition count");
        clear_fragment_partition_count_cache_for_test();
        indexdb_store::clear_cached_dbs_for_test();

        let base = std::env::temp_dir().join(format!("n1-posix-routes-{}", std::process::id()));
        let index_roots = if let Some(existing) = INDEX_STORE_DISKS.get() {
            existing.iter().map(PathBuf::from).collect::<Vec<_>>()
        } else {
            vec![base.join("index_1"), base.join("index_2")]
        };
        let blob_roots = if let Some(existing) = BLOB_STORE_DISKS.get() {
            existing.iter().map(PathBuf::from).collect::<Vec<_>>()
        } else {
            vec![
                base.join("blob_1"),
                base.join("blob_2"),
                base.join("blob_3"),
            ]
        };
        let fs_root = if let Some(existing) = FS_STORAGE_ROOT.get() {
            PathBuf::from(existing)
        } else {
            base.join("fs")
        };

        for root in index_roots
            .iter()
            .chain(blob_roots.iter())
            .chain(std::iter::once(&fs_root))
        {
            let _ = fs::create_dir_all(root).await;
        }

        if INDEX_STORE_DISKS.get().is_none() {
            INDEX_STORE_DISKS
                .set(
                    index_roots
                        .iter()
                        .map(|path| path.to_string_lossy().to_string())
                        .collect(),
                )
                .expect("set INDEX_STORE_DISKS");
        }
        if BLOB_STORE_DISKS.get().is_none() {
            BLOB_STORE_DISKS
                .set(
                    blob_roots
                        .iter()
                        .map(|path| path.to_string_lossy().to_string())
                        .collect(),
                )
                .expect("set BLOB_STORE_DISKS");
        }
        if FS_STORAGE_ROOT.get().is_none() {
            FS_STORAGE_ROOT
                .set(fs_root.to_string_lossy().to_string())
                .expect("set FS_STORAGE_ROOT");
        }
    }

    async fn setup_fragment(fragment: &str) {
        init_quota_file(fragment, 64 * 1024 * 1024)
            .await
            .expect("init quota");
        ensure_fragment_blob_directories(fragment).await;
    }

    fn full_claims(fragment: &str) -> Claims {
        Claims {
            fragment: fragment.to_string(),
            read: true,
            write: true,
            modify: true,
            delete: true,
            exp: usize::MAX,
        }
    }

    async fn seed_logical_file(
        fragment: &str,
        logical_path: &str,
        physical_id: &str,
        changed_at: i64,
    ) {
        let metadata = ObjectMetadata {
            object_key: physical_id.to_string(),
            fragment: fragment.to_string(),
            version_id: Ulid::new().to_string(),
            upload_session_id: String::new(),
            owner_uid: fragment.to_string(),
            content_type: "application/octet-stream".to_string(),
            total_size_bytes: 1024,
            created_at: changed_at,
            parts: Vec::new(),
            is_delete_marker: false,
            stripe_size_bytes: 262_144,
        };
        crate::namespace::runtime::global_namespace_runtime()
            .finalize_logical_upload(fragment, logical_path, physical_id, &metadata)
            .await
            .expect("finalize logical upload");
    }

    fn looks_like_flat_object_id(path: &str) -> bool {
        !path.is_empty()
            && !path.contains('/')
            && path.len() == 64
            && path.bytes().all(|byte| byte.is_ascii_hexdigit())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn posix_list_respects_limit_and_continuation_tokens() {
        let _guard = global_test_lock().lock().await;
        ensure_storage_config().await;

        let fragment = hex::encode(Sha256::digest(
            format!("fragment-{}", Ulid::new()).as_bytes(),
        ));
        setup_fragment(&fragment).await;

        for index in 0..205usize {
            let logical_path = format!("file-{index:03}.txt");
            let physical_id = format!("{:064x}", index + 1);
            seed_logical_file(
                &fragment,
                &logical_path,
                &physical_id,
                1_700_000_000 + index as i64,
            )
            .await;
        }

        let page_one = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(false),
            }),
        )
        .await
        .expect("first posix page")
        .0;

        assert_eq!(page_one.entries.len(), 200);
        assert!(page_one.continuation_token.is_some());

        let page_two = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: page_one.continuation_token.clone(),
                trash: Some(false),
            }),
        )
        .await
        .expect("second posix page")
        .0;

        assert_eq!(page_two.entries.len(), 5);
        assert!(page_two.continuation_token.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn posix_list_survives_restart_and_mkdir_without_degrading_to_object_ids() {
        let _guard = global_test_lock().lock().await;
        ensure_storage_config().await;

        let fragment = hex::encode(Sha256::digest(
            format!("fragment-{}", Ulid::new()).as_bytes(),
        ));
        setup_fragment(&fragment).await;

        seed_logical_file(
            &fragment,
            "video.mp4",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            1_700_000_100,
        )
        .await;
        seed_logical_file(
            &fragment,
            "notes.txt",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            1_700_000_101,
        )
        .await;

        let initial = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(false),
            }),
        )
        .await
        .expect("initial posix list")
        .0;
        assert_eq!(
            initial
                .entries
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec!["notes.txt", "video.mp4"]
        );
        assert!(
            initial
                .entries
                .iter()
                .filter(|entry| entry.kind == crate::posix::models::PosixNodeKind::File)
                .all(|entry| entry.size_bytes == Some(1024))
        );
        assert!(
            initial
                .entries
                .iter()
                .all(|entry| entry.modified_at_unix.unwrap_or_default() > 0)
        );

        crate::namespace::runtime::global_namespace_runtime().evict_fragment(&fragment);

        let after_restart = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(false),
            }),
        )
        .await
        .expect("posix list after cache eviction")
        .0;
        assert_eq!(
            after_restart
                .entries
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec!["notes.txt", "video.mp4"]
        );

        let state = Arc::new(AppState {
            ws_manager: WsConnectionManager::new(),
        });
        let _ = posix_mkdir(
            State(state),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "Images".to_string(),
                trash_entry_id: None,
            }),
        )
        .await
        .expect("mkdir after restart");

        let after_mkdir = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(false),
            }),
        )
        .await
        .expect("posix list after mkdir")
        .0;

        assert_eq!(
            after_mkdir
                .entries
                .iter()
                .map(|entry| (entry.path.as_str(), entry.kind))
                .collect::<Vec<_>>(),
            vec![
                ("Images", crate::posix::models::PosixNodeKind::Directory),
                ("notes.txt", crate::posix::models::PosixNodeKind::File),
                ("video.mp4", crate::posix::models::PosixNodeKind::File),
            ]
        );
        assert!(
            after_mkdir
                .entries
                .iter()
                .all(|entry| !looks_like_flat_object_id(&entry.path))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn posix_trash_root_lists_actual_nested_file_path() {
        let _guard = global_test_lock().lock().await;
        ensure_storage_config().await;

        let fragment = hex::encode(Sha256::digest(
            format!("fragment-{}", Ulid::new()).as_bytes(),
        ));
        setup_fragment(&fragment).await;

        seed_logical_file(
            &fragment,
            "images/photo.jpg",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            1_700_000_102,
        )
        .await;

        let state = Arc::new(AppState {
            ws_manager: WsConnectionManager::new(),
        });
        let _ = posix_trash(
            State(state.clone()),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "images/photo.jpg".to_string(),
                trash_entry_id: None,
            }),
        )
        .await
        .expect("trash nested image");

        let trash_root = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash root list")
        .0;
        assert_eq!(trash_root.entries.len(), 1);
        assert_eq!(trash_root.entries[0].path, "images/photo.jpg");
        assert_eq!(
            trash_root.entries[0].kind,
            crate::posix::models::PosixNodeKind::File
        );

        let trash_images = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: Some("images".to_string()),
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash images list")
        .0;
        assert_eq!(trash_images.entries.len(), 1);
        assert_eq!(trash_images.entries[0].path, "images/photo.jpg");
        assert_eq!(
            trash_images.entries[0].kind,
            crate::posix::models::PosixNodeKind::File
        );

        let _ = posix_recover(
            State(state),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "images/photo.jpg".to_string(),
                trash_entry_id: None,
            }),
        )
        .await
        .expect("recover nested image");

        let empty_trash = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash list after recover")
        .0;
        assert!(empty_trash.entries.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn posix_trash_root_lists_actual_deep_file_path() {
        let _guard = global_test_lock().lock().await;
        ensure_storage_config().await;

        let fragment = hex::encode(Sha256::digest(
            format!("fragment-{}", Ulid::new()).as_bytes(),
        ));
        setup_fragment(&fragment).await;

        seed_logical_file(
            &fragment,
            "videos/2025/My Videos/clip.mp4",
            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            1_700_000_103,
        )
        .await;

        let state = Arc::new(AppState {
            ws_manager: WsConnectionManager::new(),
        });
        let _ = posix_trash(
            State(state.clone()),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "videos/2025/My Videos/clip.mp4".to_string(),
                trash_entry_id: None,
            }),
        )
        .await
        .expect("trash deep nested video");

        let trash_root = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash root list")
        .0;
        assert_eq!(trash_root.entries.len(), 1);
        assert_eq!(trash_root.entries[0].path, "videos/2025/My Videos/clip.mp4");
        assert_eq!(
            trash_root.entries[0].kind,
            crate::posix::models::PosixNodeKind::File
        );

        let videos_page = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: Some("videos".to_string()),
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash videos list")
        .0;
        assert_eq!(videos_page.entries.len(), 1);
        assert_eq!(videos_page.entries[0].path, "videos/2025");
        assert_eq!(
            videos_page.entries[0].kind,
            crate::posix::models::PosixNodeKind::Directory
        );

        let year_page = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: Some("videos/2025".to_string()),
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash year list")
        .0;
        assert_eq!(year_page.entries.len(), 1);
        assert_eq!(year_page.entries[0].path, "videos/2025/My Videos");
        assert_eq!(
            year_page.entries[0].kind,
            crate::posix::models::PosixNodeKind::Directory
        );

        let nested_page = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: Some("videos/2025/My Videos".to_string()),
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash nested page")
        .0;
        assert_eq!(nested_page.entries.len(), 1);
        assert_eq!(
            nested_page.entries[0].path,
            "videos/2025/My Videos/clip.mp4"
        );
        assert_eq!(
            nested_page.entries[0].kind,
            crate::posix::models::PosixNodeKind::File
        );

        let _ = posix_recover(
            State(state),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "videos/2025/My Videos/clip.mp4".to_string(),
                trash_entry_id: None,
            }),
        )
        .await
        .expect("recover deep nested video");

        let empty_trash = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash list after recover")
        .0;
        assert!(empty_trash.entries.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn posix_trash_root_lists_actual_screenshot_paths_for_selective_recover() {
        let _guard = global_test_lock().lock().await;
        ensure_storage_config().await;

        let fragment = hex::encode(Sha256::digest(
            format!("fragment-{}", Ulid::new()).as_bytes(),
        ));
        setup_fragment(&fragment).await;

        seed_logical_file(
            &fragment,
            "Screenshots/shot-1.png",
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            1_700_000_104,
        )
        .await;
        seed_logical_file(
            &fragment,
            "Screenshots/shot-2.png",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            1_700_000_105,
        )
        .await;

        let state = Arc::new(AppState {
            ws_manager: WsConnectionManager::new(),
        });
        for path in ["Screenshots/shot-1.png", "Screenshots/shot-2.png"] {
            let _ = posix_trash(
                State(state.clone()),
                full_claims(&fragment),
                Json(PosixPathRequest {
                    path: path.to_string(),
                    trash_entry_id: None,
                }),
            )
            .await
            .expect("trash screenshot child");
        }

        let trash_root = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash root list")
        .0;
        assert_eq!(trash_root.entries.len(), 2);
        assert_eq!(trash_root.entries[0].path, "Screenshots/shot-1.png");
        assert_eq!(trash_root.entries[1].path, "Screenshots/shot-2.png");

        let recover = posix_recover(
            State(state.clone()),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "Screenshots/shot-1.png".to_string(),
                trash_entry_id: None,
            }),
        )
        .await
        .expect("recover selected screenshot")
        .0;
        assert_eq!(recover.operation, "recover");
        assert_eq!(recover.affected, 1);
        assert_eq!(recover.path.as_deref(), Some("Screenshots/shot-1.png"));

        let remaining_trash = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash list after selective recover")
        .0;
        assert_eq!(remaining_trash.entries.len(), 1);
        assert_eq!(remaining_trash.entries[0].path, "Screenshots/shot-2.png");

        let _ = posix_recover(
            State(state.clone()),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "Screenshots/shot-2.png".to_string(),
                trash_entry_id: None,
            }),
        )
        .await
        .expect("recover second screenshot");

        let live_screenshots = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: Some("Screenshots".to_string()),
                limit: Some(200),
                continuation_token: None,
                trash: Some(false),
            }),
        )
        .await
        .expect("live screenshots list after recover")
        .0;
        assert_eq!(live_screenshots.entries.len(), 2);
        assert_eq!(live_screenshots.entries[0].path, "Screenshots/shot-1.png");
        assert_eq!(live_screenshots.entries[1].path, "Screenshots/shot-2.png");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn posix_purge_removes_single_trashed_item_from_listing() {
        let _guard = global_test_lock().lock().await;
        ensure_storage_config().await;

        let fragment = hex::encode(Sha256::digest(
            format!("fragment-{}", Ulid::new()).as_bytes(),
        ));
        setup_fragment(&fragment).await;

        seed_logical_file(
            &fragment,
            "videos/video.mp4",
            "abababababababababababababababababababababababababababababababab",
            1_700_000_200,
        )
        .await;

        let state = Arc::new(AppState {
            ws_manager: WsConnectionManager::new(),
        });
        let _ = posix_trash(
            State(state.clone()),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "videos/video.mp4".to_string(),
                trash_entry_id: None,
            }),
        )
        .await
        .expect("trash item before purge");

        let purge = posix_purge(
            State(state),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "videos/video.mp4".to_string(),
                trash_entry_id: None,
            }),
        )
        .await
        .expect("purge trashed item")
        .0;
        assert_eq!(purge.operation, "purge_trash");
        assert_eq!(purge.affected, 1);
        assert_eq!(purge.path.as_deref(), Some("videos/video.mp4"));

        let empty_trash = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash list after purge")
        .0;
        assert!(empty_trash.entries.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn posix_trash_supports_same_path_generations_by_trash_entry_id() {
        let _guard = global_test_lock().lock().await;
        ensure_storage_config().await;

        let fragment = hex::encode(Sha256::digest(
            format!("fragment-{}", Ulid::new()).as_bytes(),
        ));
        setup_fragment(&fragment).await;

        seed_logical_file(
            &fragment,
            "images/first.jpg",
            "1111111111111111111111111111111111111111111111111111111111111111",
            1_700_000_300,
        )
        .await;

        let state = Arc::new(AppState {
            ws_manager: WsConnectionManager::new(),
        });
        let first_trash = posix_trash(
            State(state.clone()),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "images".to_string(),
                trash_entry_id: None,
            }),
        )
        .await
        .expect("trash first images generation")
        .0;
        let first_entry_id = first_trash
            .trash_entry_id
            .clone()
            .expect("first trash entry id");

        seed_logical_file(
            &fragment,
            "images/second.jpg",
            "2222222222222222222222222222222222222222222222222222222222222222",
            1_700_000_301,
        )
        .await;
        let second_trash = posix_trash(
            State(state.clone()),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "images".to_string(),
                trash_entry_id: None,
            }),
        )
        .await
        .expect("trash second images generation")
        .0;
        let second_entry_id = second_trash
            .trash_entry_id
            .clone()
            .expect("second trash entry id");
        assert_ne!(first_entry_id, second_entry_id);

        let trash_root = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash root list")
        .0;
        assert_eq!(trash_root.entries.len(), 2);
        assert_eq!(trash_root.entries[0].path, "images");
        assert_eq!(
            trash_root.entries[0].trash_entry_id.as_deref(),
            Some(first_entry_id.as_str())
        );
        assert_eq!(trash_root.entries[1].path, "images");
        assert_eq!(
            trash_root.entries[1].trash_entry_id.as_deref(),
            Some(second_entry_id.as_str())
        );

        let recover_by_path = posix_recover(
            State(state.clone()),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "images".to_string(),
                trash_entry_id: None,
            }),
        )
        .await;
        assert!(
            recover_by_path.is_err(),
            "path-only recover must be ambiguous for duplicate trash generations"
        );

        let recovered = posix_recover(
            State(state.clone()),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "images".to_string(),
                trash_entry_id: Some(second_entry_id.clone()),
            }),
        )
        .await
        .expect("recover second generation by id")
        .0;
        assert_eq!(
            recovered.trash_entry_id.as_deref(),
            Some(second_entry_id.as_str())
        );

        let live_images = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: Some("images".to_string()),
                limit: Some(200),
                continuation_token: None,
                trash: Some(false),
            }),
        )
        .await
        .expect("live images list after id recover")
        .0;
        assert_eq!(live_images.entries.len(), 1);
        assert_eq!(live_images.entries[0].path, "images/second.jpg");

        let purged = posix_purge(
            State(state),
            full_claims(&fragment),
            Json(PosixPathRequest {
                path: "images".to_string(),
                trash_entry_id: Some(first_entry_id.clone()),
            }),
        )
        .await
        .expect("purge first generation by id")
        .0;
        assert_eq!(
            purged.trash_entry_id.as_deref(),
            Some(first_entry_id.as_str())
        );

        let empty_trash = posix_list(
            full_claims(&fragment),
            Query(PosixListQuery {
                path: None,
                limit: Some(200),
                continuation_token: None,
                trash: Some(true),
            }),
        )
        .await
        .expect("trash list after id purge")
        .0;
        assert!(empty_trash.entries.is_empty());
    }
}
