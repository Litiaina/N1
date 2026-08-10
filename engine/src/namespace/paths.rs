use super::models::NamespaceError;

pub fn normalize_path(path: &str) -> Result<String, NamespaceError> {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return Ok(String::new());
    }

    let normalized = trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("/");

    if normalized.is_empty() {
        return Ok(String::new());
    }

    for segment in normalized.split('/') {
        if segment == "." || segment == ".." {
            return Err(NamespaceError::InvalidPath(format!(
                "path segment '{segment}' is not allowed"
            )));
        }
    }

    Ok(normalized)
}

pub fn parent_path(path: &str) -> String {
    match path.rsplit_once('/') {
        Some((parent, _)) => parent.to_string(),
        None => String::new(),
    }
}

pub fn base_name(path: &str) -> String {
    path.rsplit('/').next().unwrap_or_default().to_string()
}

pub fn is_same_or_descendant(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub fn rename_prefix(
    path: &str,
    source: &str,
    destination: &str,
) -> Result<String, NamespaceError> {
    if path == source {
        return Ok(destination.to_string());
    }

    let suffix = path.strip_prefix(source).ok_or_else(|| {
        NamespaceError::Invariant(format!(
            "path '{path}' did not match source prefix '{source}'"
        ))
    })?;

    if !suffix.starts_with('/') {
        return Err(NamespaceError::Invariant(format!(
            "path '{path}' is not a descendant of '{source}'"
        )));
    }

    Ok(format!("{destination}{suffix}"))
}
