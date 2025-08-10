use std::path::{Component, Path, PathBuf};
use mongodb::bson::doc;
use mongodb::error::Error;
use tokio::fs;
use walkdir::WalkDir;
use crate::database::api::get_collection;
use crate::fs::fs_constants::AUTH_COLLECTION;

pub async fn get_allocated_storage(uid: &str) -> Result<Option<u64>, Error> {
    let user_doc = get_collection(AUTH_COLLECTION)
        .await
        .find_one(doc! { "uid": uid })
        .await?;

    if let Some(doc) = user_doc {
        if let Ok(allocated) = doc.get_i64("allocated_storage") {
            if let Ok(value) = u64::try_from(allocated) {
                return Ok(Some(value));
            }
        }
    }

    Ok(None)
}

pub fn sanitize_path(path: &str) -> String {
    let mut clean_path = PathBuf::new();

    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => {
                let part_str = part.to_string_lossy()
                    .replace('\0', "")
                    .replace(['/', '\\'], "");
                clean_path.push(part_str);
            }
            _ => {
                continue;
            }
        }
    }

    clean_path.to_string_lossy().to_string()
}

pub fn sanitize_fragmented_path(path: &str) -> PathBuf {
    path.split('/')
        .filter_map(|s| {
            if s.is_empty() || s == "." || s == ".." {
                None
            } else {
                Some(s)
            }
        })
        .collect()
}

pub async fn get_dir_size(path: &PathBuf) -> u64 {
    let mut total_size = 0;

    for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        let metadata = fs::metadata(entry.path()).await;

        if let Ok(metadata) = metadata {
            if metadata.is_file() {
                total_size += metadata.len();
            }
        }
    }

    total_size
}
