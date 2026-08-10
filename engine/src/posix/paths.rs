use crate::enums::app_error_enum::AppError;

pub const DIR_MARKER_NAME: &str = ".n1fsd_dir";
pub const TRASH_VIRTUAL_NAME: &str = ".n1-trash";

pub fn normalize_posix_path(path: Option<&str>) -> Result<String, AppError> {
    let raw = path.unwrap_or_default().trim();
    if raw.is_empty() || raw == "/" {
        return Ok(String::new());
    }

    let mut segments = Vec::new();
    for segment in raw.split('/') {
        let clean = segment.trim();
        if clean.is_empty() {
            continue;
        }
        if clean == "." || clean == ".." {
            return Err(AppError::BadRequest(
                "POSIX paths must not contain '.' or '..' segments".to_string(),
            ));
        }
        if clean == DIR_MARKER_NAME || clean == TRASH_VIRTUAL_NAME {
            return Err(AppError::BadRequest(format!(
                "'{}' is reserved for the N1 POSIX facade",
                clean
            )));
        }
        segments.push(clean.to_string());
    }

    Ok(segments.join("/"))
}

pub fn directory_prefix(path: &str) -> Option<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(format!("{trimmed}/"))
    }
}

pub fn marker_key(path: &str) -> Option<String> {
    let normalized = path.trim_matches('/');
    if normalized.is_empty() {
        None
    } else {
        Some(format!("{normalized}/{DIR_MARKER_NAME}"))
    }
}

pub fn child_name(parent: &str, full_path: &str) -> Option<String> {
    let clean_path = full_path.trim_matches('/');
    if clean_path.is_empty() {
        return None;
    }

    let relative = if parent.trim().is_empty() {
        clean_path
    } else {
        let prefix = parent.trim_matches('/');
        clean_path.strip_prefix(prefix)?.strip_prefix('/')?
    };

    let name = relative.split('/').next().unwrap_or_default().trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

pub fn join_child(parent: &str, name: &str) -> String {
    let clean_parent = parent.trim_matches('/');
    let clean_name = name.trim_matches('/');
    if clean_parent.is_empty() {
        clean_name.to_string()
    } else {
        format!("{clean_parent}/{clean_name}")
    }
}
