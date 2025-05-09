use std::path::{Component, Path, PathBuf};
use tokio::fs;
use walkdir::WalkDir;

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