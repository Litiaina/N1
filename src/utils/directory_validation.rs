use std::path::{Component, Path, PathBuf};

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