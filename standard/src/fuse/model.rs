use crate::enums::app_error_enum::AppError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeKind {
    File,
    Directory,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeMetadata {
    pub kind: NodeKind,
    pub size_bytes: u64,
    pub modified_at_unix: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FsdPath {
    pub fragment: String,
    pub object_key: String,
}

impl FsdPath {
    pub fn parse(mount_relative_path: &str) -> Result<Self, AppError> {
        let cleaned = mount_relative_path.trim_matches('/');
        if cleaned.is_empty() {
            return Err(AppError::BadRequest("fsd path is empty".to_string()));
        }

        let mut parts = cleaned.splitn(2, '/');
        let fragment = parts.next().unwrap_or_default().trim().to_string();
        if fragment.is_empty() {
            return Err(AppError::BadRequest(
                "fragment segment is empty".to_string(),
            ));
        }

        let object_key = parts.next().unwrap_or_default().to_string();
        Ok(Self {
            fragment,
            object_key,
        })
    }

    pub fn mount_relative(&self) -> String {
        if self.object_key.is_empty() {
            self.fragment.clone()
        } else {
            format!("{}/{}", self.fragment, self.object_key)
        }
    }

    pub fn is_fragment_root(&self) -> bool {
        self.object_key.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteHandle {
    pub session_id: String,
    pub version_id: String,
    pub expected_size_bytes: Option<u64>,
    pub path: FsdPath,
}
