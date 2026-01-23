use crate::enums::app_error_enum::AppError;
use crate::structs::object_storage_structs::ObjectPathManager;
use futures_util::future::join_all;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::fs;

pub const STRUCTURE_ROOT: &str = "structure";

#[derive(Clone)]
pub struct DirectoryIndexManager {
    path_manager: ObjectPathManager,
}

impl DirectoryIndexManager {
    pub fn new() -> Self {
        Self {
            path_manager: ObjectPathManager::new(),
        }
    }

    pub fn trigger_background_insert(uid: String, fragment: String, object_key: String) {
        tokio::spawn(async move {
            let mut hasher = Sha256::new();
            hasher.update(object_key.as_bytes());
            let hash = hex::encode(hasher.finalize());

            let mgr = DirectoryIndexManager::new();
            if let Err(e) = mgr.add_entry(&uid, &fragment, &object_key, &hash).await {
                tracing::warn!(
                    "(Background) Failed to update shadow index for {}: {}",
                    object_key,
                    e
                );
            }
        });
    }

    pub fn trigger_background_removal(uid: String, fragment: String, object_key: String) {
        tokio::spawn(async move {
            let mgr = DirectoryIndexManager::new();
            if let Err(e) = mgr.remove_entry(&uid, &fragment, &object_key).await {
                tracing::warn!(
                    "(Background) Failed to remove shadow index for {}: {}",
                    object_key,
                    e
                );
            }
        });
    }

    fn get_all_structure_paths(&self, uid: &str, fragment: &str, object_key: &str) -> Vec<PathBuf> {
        let roots = self.path_manager.get_all_index_roots();
        let safe_key = object_key.trim_start_matches('/');

        roots
            .iter()
            .map(|root| {
                root.join(STRUCTURE_ROOT)
                    .join(uid)
                    .join(fragment)
                    .join(safe_key)
            })
            .collect()
    }

    pub async fn add_entry(
        &self,
        uid: &str,
        fragment: &str,
        object_key: &str,
        hash: &str,
    ) -> Result<(), AppError> {
        let paths = self.get_all_structure_paths(uid, fragment, object_key);

        if paths.is_empty() {
            return Err(AppError::InternalServerError("No storage roots".into()));
        }

        let hash_arc = std::sync::Arc::new(hash.to_string());

        let write_futures = paths.into_iter().map(|path| {
            let h = hash_arc.clone();
            async move {
                if let Some(parent) = path.parent() {
                    let _ = fs::create_dir_all(parent).await;
                }

                let _ = fs::write(&path, h.as_bytes()).await;
            }
        });

        join_all(write_futures).await;

        Ok(())
    }

    pub async fn remove_entry(
        &self,
        uid: &str,
        fragment: &str,
        object_key: &str,
    ) -> Result<(), AppError> {
        let paths = self.get_all_structure_paths(uid, fragment, object_key);
        let mgr = self.clone();

        let remove_futures = paths.into_iter().map(|path| {
            let m = mgr.clone();
            async move {
                if fs::try_exists(&path).await.unwrap_or(false) {
                    let _ = fs::remove_file(&path).await;
                    let _ = m.cleanup_empty_parents(path.parent()).await;
                }
            }
        });

        join_all(remove_futures).await;

        Ok(())
    }

    async fn cleanup_empty_parents(&self, mut dir: Option<&Path>) -> std::io::Result<()> {
        while let Some(d) = dir {
            if d.ends_with(STRUCTURE_ROOT) || d.file_name().map_or(false, |n| n.len() == 24) {
                break;
            }
            if fs::remove_dir(d).await.is_err() {
                break;
            }
            dir = d.parent();
        }
        Ok(())
    }

    pub async fn list_hashes_by_prefix(
        &self,
        uid: &str,
        fragment: &str,
        prefix: &str,
    ) -> Result<Vec<String>, AppError> {
        let mut out: HashSet<String> = HashSet::new();
        let candidate_paths = self.get_all_structure_paths(uid, fragment, prefix);

        for target_path in candidate_paths {
            if let Ok(metadata) = fs::metadata(&target_path).await {
                if metadata.is_dir() {
                    self.scan_recursive(&target_path, &mut out).await?;
                } else if let Ok(hash) = fs::read_to_string(&target_path).await {
                    let h = hash.trim();
                    if !h.is_empty() {
                        out.insert(h.to_string());
                    }
                }
                continue;
            }

            let parent = match target_path.parent() {
                Some(p) => p,
                None => continue,
            };

            if !fs::try_exists(parent).await.unwrap_or(false) {
                continue;
            }

            let prefix_name = target_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");

            if prefix_name.is_empty() {
                continue;
            }

            let mut entries = match fs::read_dir(parent).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if !name.starts_with(prefix_name) {
                    continue;
                }

                let path = entry.path();
                if let Ok(m) = fs::metadata(&path).await {
                    if m.is_dir() {
                        self.scan_recursive(&path, &mut out).await?;
                    } else if let Ok(hash) = fs::read_to_string(&path).await {
                        let h = hash.trim();
                        if !h.is_empty() {
                            out.insert(h.to_string());
                        }
                    }
                }
            }
        }

        let mut hashes: Vec<String> = out.into_iter().collect();
        hashes.sort();
        Ok(hashes)
    }

    #[async_recursion::async_recursion]
    async fn scan_recursive(
        &self,
        dir: &Path,
        hashes: &mut HashSet<String>,
    ) -> Result<(), AppError> {
        let mut entries = fs::read_dir(dir)
            .await
            .map_err(|e| AppError::Io(e, "read structure".into()))?;

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();

            if let Ok(m) = fs::metadata(&path).await {
                if m.is_dir() {
                    self.scan_recursive(&path, hashes).await?;
                } else if let Ok(hash) = fs::read_to_string(&path).await {
                    let h = hash.trim();
                    if !h.is_empty() {
                        hashes.insert(h.to_string());
                    }
                }
            }
        }

        Ok(())
    }
}
