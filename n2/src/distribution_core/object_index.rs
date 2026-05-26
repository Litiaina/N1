use crate::core::disk_manager::ObjectPathManager;
use crate::core::modify::sync_dir;
use crate::distribution_core::config::n2_runtime_config;
use crate::distribution_core::model::N2ObjectRecord;
use crate::distribution_core::nmtd::{
    N2_OBJECTS_DIR, OBJECT_METADATA_EXT, decode_namespace_relative_path, namespace_path_for_root,
    namespace_root_for_root, new_object_id, normalize_object_id, normalize_object_key,
    object_dir_for_root, object_id_from_dir_name, record_path_for_root, write_atomic,
};
use crate::distribution_core::peer::{
    InternalNamespaceRenameRequest, InternalNamespaceRenameResponse,
    InternalObjectRecordDeleteResponse, add_internal_peer_headers, normalize_endpoint,
};
use crate::distribution_core::state::get_membership;
use crate::enums::app_error_enum::AppError;
use axum::http::StatusCode;
use std::collections::HashSet;
use std::path::{Path as StdPath, PathBuf};
use std::time::Duration;
use tokio::fs;
use tracing::warn;

#[derive(Clone, Debug)]
pub(crate) struct N2NamespaceListPage {
    pub(crate) records: Vec<N2ObjectRecord>,
    pub(crate) common_prefixes: Vec<String>,
    pub(crate) has_more: bool,
    pub(crate) next_continuation: Option<String>,
}

fn metadata_version_from_name(name: &str) -> Option<String> {
    name.strip_suffix(&format!(".{}", OBJECT_METADATA_EXT))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
}

fn apply_namespace_key(mut record: N2ObjectRecord, object_key: &str) -> N2ObjectRecord {
    record.object.object_key = normalize_object_key(object_key);
    record
}

fn object_id_from_object_dir(path: &StdPath) -> Option<String> {
    path.file_name()
        .and_then(|v| v.to_str())
        .and_then(object_id_from_dir_name)
}

async fn remove_namespace_entry_for_root(
    root: &StdPath,
    fragment: &str,
    object_key: &str,
) -> Result<bool, AppError> {
    let namespace_path = namespace_path_for_root(root, fragment, object_key);
    let exists = fs::symlink_metadata(&namespace_path).await.is_ok();
    if !exists {
        return Ok(false);
    }
    fs::remove_file(&namespace_path)
        .await
        .map_err(|e| AppError::Io(e, format!("remove N2 namespace entry {:?}", namespace_path)))?;
    if let Some(parent) = namespace_path.parent() {
        let _ = sync_dir(parent).await;
    }
    Ok(true)
}

async fn write_namespace_entry_for_root(
    root: &StdPath,
    fragment: &str,
    object_key: &str,
    object_id: &str,
) -> Result<(), AppError> {
    let namespace_path = namespace_path_for_root(root, fragment, object_key);
    let Some(parent) = namespace_path.parent() else {
        return Err(AppError::InternalServerError(
            "N2 namespace path missing parent".to_string(),
        ));
    };
    fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir N2 namespace {:?}", parent)))?;
    let target = object_dir_for_root(root, fragment, object_id);
    let tmp = namespace_path.with_extension(format!("tmp.{}", ulid::Ulid::new()));

    #[cfg(unix)]
    {
        let target_for_link = target.clone();
        let tmp_for_link = tmp.clone();
        tokio::task::spawn_blocking(move || {
            std::os::unix::fs::symlink(target_for_link, tmp_for_link)
        })
        .await
        .map_err(|e| AppError::InternalServerError(format!("join symlink task failed: {}", e)))?
        .map_err(|e| AppError::Io(e, format!("symlink N2 namespace {:?}", tmp)))?;
    }

    #[cfg(not(unix))]
    {
        fs::write(&tmp, target.to_string_lossy().as_bytes())
            .await
            .map_err(|e| AppError::Io(e, format!("write N2 namespace pointer {:?}", tmp)))?;
    }

    match fs::rename(&tmp, &namespace_path).await {
        Ok(_) => {}
        Err(_) => {
            let _ = fs::remove_file(&namespace_path).await;
            fs::rename(&tmp, &namespace_path).await.map_err(|e| {
                AppError::Io(e, format!("publish N2 namespace {:?}", namespace_path))
            })?;
        }
    }
    let _ = sync_dir(parent).await;
    Ok(())
}

async fn refresh_namespace_entry_local(
    fragment: &str,
    object_key: &str,
    object_id: &str,
) -> Result<(), AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    for root in &roots {
        write_namespace_entry_for_root(root, fragment, object_key, object_id).await?;
    }
    Ok(())
}

async fn remove_namespace_entry_local(fragment: &str, object_key: &str) -> Result<usize, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    let mut removed = 0usize;
    for root in &roots {
        if remove_namespace_entry_for_root(root, fragment, object_key).await? {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

async fn read_namespace_target_for_root(
    root: &StdPath,
    fragment: &str,
    object_key: &str,
) -> Result<Option<PathBuf>, AppError> {
    let namespace_path = namespace_path_for_root(root, fragment, object_key);
    if !fs::symlink_metadata(&namespace_path).await.is_ok() {
        return Ok(None);
    }

    #[cfg(unix)]
    {
        let target = fs::read_link(&namespace_path)
            .await
            .map_err(|e| AppError::Io(e, format!("read N2 namespace link {:?}", namespace_path)))?;
        return Ok(Some(target));
    }

    #[cfg(not(unix))]
    {
        let target = fs::read_to_string(&namespace_path).await.map_err(|e| {
            AppError::Io(e, format!("read N2 namespace pointer {:?}", namespace_path))
        })?;
        return Ok(Some(PathBuf::from(target.trim())));
    }
}

pub(crate) async fn resolve_n2_object_id_local(
    fragment: &str,
    object_key: &str,
) -> Result<Option<String>, AppError> {
    let path_manager = ObjectPathManager::new();
    for root in path_manager.get_all_index_roots() {
        let Some(target) = read_namespace_target_for_root(&root, fragment, object_key).await?
        else {
            continue;
        };
        if let Some(object_id) = object_id_from_object_dir(&target) {
            return Ok(Some(object_id));
        }
    }
    Ok(None)
}

pub(crate) async fn resolve_or_create_n2_object_id_local(
    fragment: &str,
    object_key: &str,
) -> Result<String, AppError> {
    if let Some(object_id) = resolve_n2_object_id_local(fragment, object_key).await? {
        return Ok(object_id);
    }
    Ok(new_object_id())
}

async fn rename_namespace_entry_for_root(
    root: &StdPath,
    fragment: &str,
    source_object_key: &str,
    destination_object_key: &str,
) -> Result<bool, AppError> {
    let source = namespace_path_for_root(root, fragment, source_object_key);
    let destination = namespace_path_for_root(root, fragment, destination_object_key);
    if !fs::symlink_metadata(&source).await.is_ok() {
        return Ok(false);
    }
    if fs::symlink_metadata(&destination).await.is_ok() {
        return Err(AppError::Conflict(format!(
            "destination namespace entry already exists: {}",
            destination_object_key
        )));
    }
    let Some(parent) = destination.parent() else {
        return Err(AppError::InternalServerError(
            "N2 destination namespace path missing parent".to_string(),
        ));
    };
    fs::create_dir_all(parent)
        .await
        .map_err(|e| AppError::Io(e, format!("mkdir N2 namespace {:?}", parent)))?;
    fs::rename(&source, &destination)
        .await
        .map_err(|e| AppError::Io(e, format!("rename {:?} -> {:?}", source, destination)))?;
    if let Some(source_parent) = source.parent() {
        let _ = sync_dir(source_parent).await;
    }
    let _ = sync_dir(parent).await;
    Ok(true)
}

pub(crate) async fn rename_n2_namespace_entry_local(
    fragment: &str,
    source_object_key: &str,
    destination_object_key: &str,
) -> Result<usize, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    let mut moved = 0usize;
    for root in &roots {
        if rename_namespace_entry_for_root(
            root,
            fragment,
            source_object_key,
            destination_object_key,
        )
        .await?
        {
            moved = moved.saturating_add(1);
        }
    }
    Ok(moved)
}

pub(crate) async fn persist_n2_object_record_local(
    record: &N2ObjectRecord,
) -> Result<usize, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Err(AppError::InternalServerError(
            "No index roots configured".to_string(),
        ));
    }

    let object_id = normalize_object_id(record.object.object_id.as_str());
    if object_id.is_empty() {
        return Err(AppError::BadRequest(
            "N2 object record missing object_id".to_string(),
        ));
    }
    let record_bytes = rmp_serde::to_vec(record).map_err(|e| {
        AppError::InternalServerError(format!("encode n2 object record failed: {}", e))
    })?;

    let mut writes_ok = 0usize;
    for root in &roots {
        let record_path = record_path_for_root(
            root,
            record.object.fragment.as_str(),
            &object_id,
            record.object.version_id.as_str(),
        );
        if write_atomic(&record_path, &record_bytes).await.is_ok() {
            writes_ok = writes_ok.saturating_add(1);
        } else {
            warn!(
                "n2 record mirror write failed root={} key={} version={}",
                root.display(),
                record.object.object_key,
                record.object.version_id
            );
        }
    }

    if writes_ok == 0 {
        return Err(AppError::InternalServerError(
            "failed to persist n2 object record on all mirrors".to_string(),
        ));
    }

    refresh_namespace_entry_local(
        record.object.fragment.as_str(),
        record.object.object_key.as_str(),
        &object_id,
    )
    .await?;

    Ok(writes_ok)
}

pub(crate) async fn list_n2_record_versions_local(
    fragment: &str,
    object_key: &str,
) -> Result<Vec<String>, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Ok(Vec::new());
    }

    let Some(object_id) = resolve_n2_object_id_local(fragment, object_key).await? else {
        return Ok(Vec::new());
    };
    let mut versions = HashSet::new();
    for root in &roots {
        let dir = object_dir_for_root(root, fragment, object_id.as_str());
        let exists = fs::try_exists(&dir)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", dir)))?;
        if !exists {
            continue;
        }
        let mut entries = match fs::read_dir(&dir).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(version) = metadata_version_from_name(name.as_str()) {
                versions.insert(version);
            }
        }
    }

    let mut out = versions.into_iter().collect::<Vec<_>>();
    out.sort();
    Ok(out)
}

pub(crate) async fn remove_n2_namespace_if_headless_local(
    fragment: &str,
    object_key: &str,
) -> Result<(), AppError> {
    if list_n2_record_versions_local(fragment, object_key)
        .await?
        .is_empty()
    {
        let _ = remove_namespace_entry_local(fragment, object_key).await?;
    }
    Ok(())
}

pub(crate) async fn delete_n2_object_record_local(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<usize, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Ok(0);
    }

    let Some(object_id) = resolve_n2_object_id_local(fragment, object_key).await? else {
        return Ok(0);
    };
    let mut deleted = 0usize;
    for root in &roots {
        let record_path = record_path_for_root(root, fragment, object_id.as_str(), version_id);
        let exists = fs::try_exists(&record_path)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", record_path)))?;
        if !exists {
            continue;
        }
        fs::remove_file(&record_path)
            .await
            .map_err(|e| AppError::Io(e, format!("remove {:?}", record_path)))?;
        deleted = deleted.saturating_add(1);
        if let Some(parent) = record_path.parent() {
            let _ = sync_dir(parent).await;
        }
    }

    remove_n2_namespace_if_headless_local(fragment, object_key).await?;
    Ok(deleted)
}

pub(crate) async fn replicate_n2_object_record_remote(
    http_client: &reqwest::Client,
    endpoint: &str,
    record: &N2ObjectRecord,
) -> Result<(), AppError> {
    let url = format!(
        "{}/n2/internal/object-record/upsert",
        endpoint.trim_end_matches('/')
    );
    let response = add_internal_peer_headers(http_client.post(url.as_str()).json(record))
        .await?
        .send()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("send n2 object record upsert failed: {}", e))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "n2 object record upsert failed status={} body={}",
            status, body
        )));
    }
    Ok(())
}

pub(crate) async fn delete_n2_object_record_remote(
    http_client: &reqwest::Client,
    endpoint: &str,
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<usize, AppError> {
    let url = format!(
        "{}/n2/internal/object-record/{}",
        endpoint.trim_end_matches('/'),
        fragment
    );
    let response = add_internal_peer_headers(
        http_client
            .delete(url.as_str())
            .query(&[("object_key", object_key), ("version_id", version_id)]),
    )
    .await?
    .send()
    .await
    .map_err(|e| {
        AppError::InternalServerError(format!("send n2 object record delete failed: {}", e))
    })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "n2 object record delete failed status={} body={}",
            status, body
        )));
    }
    let parsed = response
        .json::<InternalObjectRecordDeleteResponse>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!(
                "decode n2 object record delete response failed: {}",
                e
            ))
        })?;
    Ok(parsed.local_metadata_files_deleted)
}

pub(crate) async fn rename_n2_namespace_entry_remote(
    http_client: &reqwest::Client,
    endpoint: &str,
    fragment: &str,
    source_object_key: &str,
    destination_object_key: &str,
) -> Result<usize, AppError> {
    let url = format!(
        "{}/n2/internal/namespace/rename",
        endpoint.trim_end_matches('/')
    );
    let request = InternalNamespaceRenameRequest {
        fragment: fragment.to_string(),
        source_object_key: source_object_key.to_string(),
        destination_object_key: destination_object_key.to_string(),
    };
    let response = add_internal_peer_headers(http_client.post(url.as_str()).json(&request))
        .await?
        .send()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("send n2 namespace rename failed: {}", e))
        })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "n2 namespace rename failed status={} body={}",
            status, body
        )));
    }
    let parsed = response
        .json::<InternalNamespaceRenameResponse>()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!(
                "decode n2 namespace rename response failed: {}",
                e
            ))
        })?;
    Ok(parsed.local_namespace_entries_moved)
}

pub(crate) async fn rename_n2_namespace_entry(
    fragment: &str,
    source_object_key: &str,
    destination_object_key: &str,
    remote_endpoints: &[String],
) -> Result<usize, AppError> {
    let mut moved = 0usize;
    if remote_endpoints.is_empty() {
        return rename_n2_namespace_entry_local(
            fragment,
            source_object_key,
            destination_object_key,
        )
        .await;
    }

    let cfg = n2_runtime_config();
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;
    for endpoint in remote_endpoints {
        match rename_n2_namespace_entry_remote(
            &http_client,
            endpoint.as_str(),
            fragment,
            source_object_key,
            destination_object_key,
        )
        .await
        {
            Ok(count) => moved = moved.saturating_add(count),
            Err(err) => {
                warn!(
                    "n2 remote namespace rename failed fragment={} source={} destination={} endpoint={} err={}",
                    fragment, source_object_key, destination_object_key, endpoint, err
                );
                return Err(AppError::Conflict(format!(
                    "n2 namespace rename did not reach peer {}: {}",
                    endpoint, err
                )));
            }
        }
    }
    moved = moved.saturating_add(
        rename_n2_namespace_entry_local(fragment, source_object_key, destination_object_key)
            .await?,
    );
    Ok(moved)
}

pub(crate) async fn persist_n2_object_record(record: &N2ObjectRecord) -> Result<(), AppError> {
    let local_writes = persist_n2_object_record_local(record).await?;
    let cfg = n2_runtime_config();

    let mut remote_endpoints = record
        .shards
        .iter()
        .map(|s| s.endpoint.clone())
        .filter(|endpoint| normalize_endpoint(endpoint) != normalize_endpoint(&cfg.node_endpoint))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    remote_endpoints.sort();

    if remote_endpoints.is_empty() {
        return Ok(());
    }

    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;
    let mut replicated = 0usize;
    for endpoint in &remote_endpoints {
        if replicate_n2_object_record_remote(&http_client, endpoint.as_str(), record)
            .await
            .is_ok()
        {
            replicated = replicated.saturating_add(1);
        } else {
            warn!(
                "n2 object record replication failed fragment={} object_key={} version={} endpoint={}",
                record.object.fragment,
                record.object.object_key,
                record.object.version_id,
                endpoint
            );
        }
    }

    let required_remote_acks = remote_endpoints
        .len()
        .min(usize::from(record.write_quorum.saturating_sub(1)));

    if replicated < required_remote_acks {
        return Err(AppError::Conflict(format!(
            "object committed locally ({} mirrors) but object record replication acknowledged only {} remote targets, need {}",
            local_writes, replicated, required_remote_acks
        )));
    }

    Ok(())
}

pub(crate) async fn read_n2_record_version(
    fragment: &str,
    object_key: &str,
    version_id: &str,
) -> Result<N2ObjectRecord, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    let Some(object_id) = resolve_n2_object_id_local(fragment, object_key).await? else {
        return Err(AppError::NotFound);
    };
    let record =
        read_n2_record_version_by_object_id(roots, fragment, &object_id, version_id).await?;
    Ok(apply_namespace_key(record, object_key))
}

pub(crate) async fn read_n2_record_version_by_object_id(
    roots: Vec<PathBuf>,
    fragment: &str,
    object_id: &str,
    version_id: &str,
) -> Result<N2ObjectRecord, AppError> {
    for root in roots {
        let record_path = record_path_for_root(&root, fragment, object_id, version_id);
        let exists = fs::try_exists(&record_path)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", record_path)))?;
        if !exists {
            continue;
        }

        let bytes = fs::read(&record_path)
            .await
            .map_err(|e| AppError::Io(e, format!("read {:?}", record_path)))?;
        let parsed = rmp_serde::from_slice::<N2ObjectRecord>(&bytes).map_err(|e| {
            AppError::InternalServerError(format!("decode n2 object record failed: {}", e))
        })?;
        return Ok(parsed);
    }

    Err(AppError::NotFound)
}

pub(crate) async fn discover_latest_n2_record_version(
    fragment: &str,
    object_key: &str,
) -> Result<String, AppError> {
    let versions = list_n2_record_versions_local(fragment, object_key).await?;
    versions.into_iter().max().ok_or(AppError::NotFound)
}

async fn discover_latest_n2_record_version_by_object_dir(
    object_dir: &StdPath,
) -> Result<Option<String>, AppError> {
    let mut discovered = Vec::new();
    let exists = fs::try_exists(object_dir)
        .await
        .map_err(|e| AppError::Io(e, format!("check {:?}", object_dir)))?;
    if !exists {
        return Ok(None);
    }
    let mut entries = match fs::read_dir(object_dir).await {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(version) = metadata_version_from_name(name.as_str()) {
            discovered.push(version);
        }
    }

    discovered.sort();
    Ok(discovered.pop())
}

pub(crate) async fn load_n2_object_record_local(
    fragment: &str,
    object_key: &str,
    version_id: Option<&str>,
) -> Result<N2ObjectRecord, AppError> {
    let version = match version_id {
        Some(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => discover_latest_n2_record_version(fragment, object_key).await?,
    };
    read_n2_record_version(fragment, object_key, version.as_str()).await
}

pub(crate) async fn load_n2_object_record_remote(
    http_client: &reqwest::Client,
    endpoint: &str,
    fragment: &str,
    object_key: &str,
    version_id: Option<&str>,
) -> Result<N2ObjectRecord, AppError> {
    let url = format!(
        "{}/n2/internal/object-record/{}",
        endpoint.trim_end_matches('/'),
        fragment
    );
    let mut request = http_client
        .get(url.as_str())
        .query(&[("object_key", object_key)]);
    if let Some(v) = version_id {
        request = request.query(&[("version_id", v)]);
    }
    let response = add_internal_peer_headers(request)
        .await?
        .send()
        .await
        .map_err(|e| {
            AppError::InternalServerError(format!("request remote n2 record failed: {}", e))
        })?;
    if response.status() == StatusCode::NOT_FOUND {
        return Err(AppError::NotFound);
    }
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(AppError::InternalServerError(format!(
            "remote n2 object record read failed status={} body={}",
            status, body
        )));
    }
    response.json::<N2ObjectRecord>().await.map_err(|e| {
        AppError::InternalServerError(format!("decode remote n2 record failed: {}", e))
    })
}

pub(crate) async fn load_n2_object_record(
    fragment: &str,
    object_key: &str,
    version_id: Option<&str>,
) -> Result<N2ObjectRecord, AppError> {
    let local_result = load_n2_object_record_local(fragment, object_key, version_id).await;
    if local_result.is_ok() {
        return local_result;
    }

    let membership = match get_membership().await {
        Ok(v) => v,
        Err(_) => return local_result,
    };
    let cfg = n2_runtime_config();
    let local_endpoint = normalize_endpoint(&cfg.node_endpoint);
    let mut remote_endpoints = membership
        .nodes
        .iter()
        .filter(|n| n.active)
        .map(|n| n.endpoint.clone())
        .filter(|endpoint| normalize_endpoint(endpoint) != local_endpoint)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    remote_endpoints.sort();
    if remote_endpoints.is_empty() {
        return local_result;
    }

    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_millis(cfg.remote_request_timeout_ms))
        .build()
        .map_err(|e| {
            AppError::InternalServerError(format!("build n2 http client failed: {}", e))
        })?;
    let mut last_err = None;
    for endpoint in remote_endpoints {
        match load_n2_object_record_remote(
            &http_client,
            endpoint.as_str(),
            fragment,
            object_key,
            version_id,
        )
        .await
        {
            Ok(record) => return Ok(record),
            Err(AppError::NotFound) => {}
            Err(e) => last_err = Some(e),
        }
    }

    if let Some(err) = last_err {
        return Err(err);
    }
    local_result
}

async fn read_latest_record_from_object_dir(
    object_dir: &StdPath,
) -> Result<Option<N2ObjectRecord>, AppError> {
    let Some(version_id) = discover_latest_n2_record_version_by_object_dir(object_dir).await?
    else {
        return Ok(None);
    };
    let Some(object_id) = object_id_from_object_dir(object_dir) else {
        return Ok(None);
    };
    let record_path = object_dir.join(format!("{}.{}", version_id, OBJECT_METADATA_EXT));
    if !fs::try_exists(&record_path)
        .await
        .map_err(|e| AppError::Io(e, format!("check {:?}", record_path)))?
    {
        return Ok(None);
    }
    let bytes = fs::read(&record_path)
        .await
        .map_err(|e| AppError::Io(e, format!("read {:?}", record_path)))?;
    let parsed = rmp_serde::from_slice::<N2ObjectRecord>(&bytes).map_err(|e| {
        AppError::InternalServerError(format!("decode n2 namespace record failed: {}", e))
    })?;
    if normalize_object_id(parsed.object.object_id.as_str()) != object_id {
        return Ok(None);
    }
    Ok(Some(parsed))
}

async fn read_latest_record_from_namespace_entry(
    entry_path: &StdPath,
) -> Result<Option<N2ObjectRecord>, AppError> {
    #[cfg(unix)]
    {
        let target = fs::read_link(entry_path)
            .await
            .map_err(|e| AppError::Io(e, format!("read N2 namespace link {:?}", entry_path)))?;
        return read_latest_record_from_object_dir(&target).await;
    }

    #[cfg(not(unix))]
    {
        let target = fs::read_to_string(entry_path)
            .await
            .map_err(|e| AppError::Io(e, format!("read N2 namespace pointer {:?}", entry_path)))?;
        return read_latest_record_from_object_dir(StdPath::new(target.trim())).await;
    }
}

fn split_namespace_prefix(prefix: &str) -> (String, String) {
    let normalized = normalize_object_key(prefix);
    if normalized.is_empty() {
        return (String::new(), String::new());
    }
    if prefix.ends_with('/') {
        return (normalized, String::new());
    }
    match normalized.rsplit_once('/') {
        Some((dir, filter)) => (dir.to_string(), filter.to_string()),
        None => (String::new(), normalized),
    }
}

fn join_object_key(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", prefix, name)
    }
}

async fn collect_namespace_recursive(
    root_dir: &StdPath,
    dir_prefix: &str,
    name_filter: &str,
    out: &mut Vec<N2ObjectRecord>,
    max_entries: usize,
) -> Result<(), AppError> {
    let mut stack = vec![(root_dir.to_path_buf(), dir_prefix.to_string())];
    while let Some((dir, prefix)) = stack.pop() {
        let mut entries = match fs::read_dir(&dir).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AppError::Io(e, format!("read N2 namespace {:?}", dir)))?
        {
            let file_name = entry.file_name();
            let Some(encoded_name) = file_name.to_str() else {
                continue;
            };
            let decoded_name = decode_namespace_relative_path(StdPath::new(encoded_name))?;
            if prefix == dir_prefix
                && !name_filter.is_empty()
                && !decoded_name.starts_with(name_filter)
            {
                continue;
            }
            let key = join_object_key(prefix.as_str(), decoded_name.as_str());
            let metadata = match fs::symlink_metadata(entry.path()).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            if metadata.file_type().is_dir() {
                stack.push((entry.path(), key));
                continue;
            }
            if metadata.file_type().is_symlink() || metadata.file_type().is_file() {
                if let Some(record) =
                    read_latest_record_from_namespace_entry(entry.path().as_path()).await?
                {
                    out.push(apply_namespace_key(record, key.as_str()));
                    if out.len() >= max_entries {
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) async fn list_fragment_n2_namespace_page(
    fragment: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
    limit: usize,
    continuation: Option<&str>,
) -> Result<N2NamespaceListPage, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Ok(N2NamespaceListPage {
            records: Vec::new(),
            common_prefixes: Vec::new(),
            has_more: false,
            next_continuation: None,
        });
    }

    let (dir_prefix, name_filter) = split_namespace_prefix(prefix.unwrap_or_default());
    let mut records = Vec::new();
    let mut prefixes = HashSet::<String>::new();
    let scan_limit = limit.saturating_mul(8).clamp(limit, 1_000_000);
    let delimiter_is_dir = delimiter == Some("/");

    for root in roots {
        let dir = if dir_prefix.is_empty() {
            namespace_root_for_root(&root, fragment)
        } else {
            namespace_path_for_root(&root, fragment, dir_prefix.as_str())
        };
        if !fs::try_exists(&dir)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", dir)))?
        {
            continue;
        }

        if delimiter_is_dir {
            let mut entries = fs::read_dir(&dir)
                .await
                .map_err(|e| AppError::Io(e, format!("read N2 namespace {:?}", dir)))?;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| AppError::Io(e, format!("read N2 namespace {:?}", dir)))?
            {
                let file_name = entry.file_name();
                let Some(encoded_name) = file_name.to_str() else {
                    continue;
                };
                let decoded_name = decode_namespace_relative_path(StdPath::new(encoded_name))?;
                if !name_filter.is_empty() && !decoded_name.starts_with(name_filter.as_str()) {
                    continue;
                }
                let metadata = match fs::symlink_metadata(entry.path()).await {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let key = join_object_key(dir_prefix.as_str(), decoded_name.as_str());
                if metadata.file_type().is_dir() {
                    prefixes.insert(format!("{}/", key));
                    continue;
                }
                if metadata.file_type().is_symlink() || metadata.file_type().is_file() {
                    if let Some(record) =
                        read_latest_record_from_namespace_entry(entry.path().as_path()).await?
                    {
                        records.push(apply_namespace_key(record, key.as_str()));
                    }
                }
                if records.len().saturating_add(prefixes.len()) >= scan_limit {
                    break;
                }
            }
        } else {
            collect_namespace_recursive(
                &dir,
                dir_prefix.as_str(),
                name_filter.as_str(),
                &mut records,
                scan_limit,
            )
            .await?;
        }
    }

    records.sort_by(|a, b| a.object.object_key.cmp(&b.object.object_key));
    records.dedup_by(|a, b| a.object.object_key == b.object.object_key);
    let mut common_prefixes = prefixes.into_iter().collect::<Vec<_>>();
    common_prefixes.sort();

    let continuation = continuation.unwrap_or_default();
    if !continuation.is_empty() {
        records.retain(|record| record.object.object_key.as_str() > continuation);
        common_prefixes.retain(|p| p.as_str() > continuation);
    }

    let mut combined = records
        .iter()
        .map(|record| record.object.object_key.clone())
        .chain(common_prefixes.iter().cloned())
        .collect::<Vec<_>>();
    combined.sort();
    let has_more = combined.len() > limit;
    let next_continuation = if has_more {
        combined.get(limit.saturating_sub(1)).cloned()
    } else {
        None
    };

    let allowed = combined.into_iter().take(limit).collect::<HashSet<_>>();
    records.retain(|record| allowed.contains(record.object.object_key.as_str()));
    common_prefixes.retain(|p| allowed.contains(p.as_str()));

    Ok(N2NamespaceListPage {
        records,
        common_prefixes,
        has_more,
        next_continuation,
    })
}

pub(crate) async fn collect_n2_record_paths(
    root: &StdPath,
    max_paths: usize,
) -> Result<Vec<PathBuf>, AppError> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = match fs::read_dir(&dir).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AppError::Io(e, format!("read dir entry {:?}", dir)))?
        {
            let path = entry.path();
            let file_type = entry
                .file_type()
                .await
                .map_err(|e| AppError::Io(e, format!("file type {:?}", path)))?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
                continue;
            };
            if metadata_version_from_name(name).is_none() {
                continue;
            }
            out.push(path);
            if out.len() >= max_paths {
                return Ok(out);
            }
        }
    }
    Ok(out)
}

pub(crate) async fn list_fragment_n2_records(
    fragment: &str,
    max_objects: usize,
) -> Result<Vec<N2ObjectRecord>, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Ok(Vec::new());
    }

    let mut seen_record_ids = HashSet::new();
    let mut out = Vec::new();
    for root in roots {
        let index_root = root.join(fragment).join("n2").join(N2_OBJECTS_DIR);
        let exists = fs::try_exists(&index_root)
            .await
            .map_err(|e| AppError::Io(e, format!("check {:?}", index_root)))?;
        if !exists {
            continue;
        }

        // Pull a bounded number of files per root; mirrors typically contain the same keys.
        let mut record_paths =
            collect_n2_record_paths(&index_root, max_objects.saturating_mul(8)).await?;
        record_paths.sort();
        for path in record_paths {
            let record_id = path
                .strip_prefix(&index_root)
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| path.to_string_lossy().to_string());
            if !seen_record_ids.insert(record_id) {
                continue;
            }
            let bytes = match fs::read(&path).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            let record = match rmp_serde::from_slice::<N2ObjectRecord>(&bytes) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if record.object.fragment != fragment {
                continue;
            }
            out.push(record);
            if out.len() >= max_objects {
                return Ok(out);
            }
        }
    }

    Ok(out)
}

pub(crate) async fn list_n2_fragments(max_fragments: usize) -> Result<Vec<String>, AppError> {
    let path_manager = ObjectPathManager::new();
    let roots = path_manager.get_all_index_roots();
    if roots.is_empty() {
        return Ok(Vec::new());
    }

    let mut fragments = HashSet::new();
    for root in roots {
        let mut entries = match fs::read_dir(&root).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| AppError::Io(e, format!("read dir entry {:?}", root)))?
        {
            let path = entry.path();
            let file_type = entry
                .file_type()
                .await
                .map_err(|e| AppError::Io(e, format!("file type {:?}", path)))?;
            if !file_type.is_dir() {
                continue;
            }
            let Some(fragment) = path.file_name().and_then(|v| v.to_str()) else {
                continue;
            };
            let n2_index = path.join("n2").join(N2_OBJECTS_DIR);
            let exists = fs::try_exists(&n2_index)
                .await
                .map_err(|e| AppError::Io(e, format!("check {:?}", n2_index)))?;
            if exists {
                fragments.insert(fragment.to_string());
                if fragments.len() >= max_fragments {
                    let mut out = fragments.into_iter().collect::<Vec<_>>();
                    out.sort();
                    return Ok(out);
                }
            }
        }
    }

    let mut out = fragments.into_iter().collect::<Vec<_>>();
    out.sort();
    Ok(out)
}

pub(crate) async fn list_n2_records_for_object(
    fragment: &str,
    object_key: &str,
    max_versions: usize,
) -> Result<Vec<N2ObjectRecord>, AppError> {
    let key = normalize_object_key(object_key);
    if key.is_empty() {
        return Ok(Vec::new());
    }

    let versions = list_n2_record_versions_local(fragment, key.as_str()).await?;
    let mut records = Vec::with_capacity(versions.len().min(max_versions));
    for version in versions.into_iter().rev().take(max_versions) {
        match read_n2_record_version(fragment, key.as_str(), version.as_str()).await {
            Ok(record) => records.push(record),
            Err(AppError::NotFound) => {}
            Err(err) => return Err(err),
        }
    }
    records.sort_by(|a, b| {
        b.committed_at
            .cmp(&a.committed_at)
            .then_with(|| b.object.version_id.cmp(&a.object.version_id))
    });
    Ok(records)
}
