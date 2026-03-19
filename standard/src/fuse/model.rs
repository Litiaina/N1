use crate::enums::app_error_enum::AppError;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};

const FSD_ESCAPED_NAME_SET: &AsciiSet = &CONTROLS
    .add(b'%')
    .add(b'/')
    .add(b'\\')
    .add(b':')
    .add(b'*')
    .add(b'?')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'|');

const FSD_TRASH_DIR_NAME: &str = ".n1-trash";

fn decode_mount_name(raw: &str) -> Result<String, AppError> {
    percent_decode_str(raw)
        .decode_utf8()
        .map(|v| v.into_owned())
        .map_err(|_| AppError::BadRequest("invalid percent-encoded fsd path".to_string()))
}

pub fn encode_virtual_name(name: &str) -> String {
    utf8_percent_encode(name, FSD_ESCAPED_NAME_SET).to_string()
}

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

        let raw_object_key = parts.next().unwrap_or_default();
        let object_key = if raw_object_key == FSD_TRASH_DIR_NAME {
            FSD_TRASH_DIR_NAME.to_string()
        } else if let Some(raw_trash_key) = raw_object_key
            .strip_prefix(FSD_TRASH_DIR_NAME)
            .and_then(|v| v.strip_prefix('/'))
        {
            format!("{}/{}", FSD_TRASH_DIR_NAME, decode_mount_name(raw_trash_key)?)
        } else if raw_object_key.contains('/') {
            raw_object_key.to_string()
        } else {
            decode_mount_name(raw_object_key)?
        };
        Ok(Self {
            fragment,
            object_key,
        })
    }

    pub fn mount_relative(&self) -> String {
        if self.object_key.is_empty() {
            self.fragment.clone()
        } else if self.object_key == FSD_TRASH_DIR_NAME {
            format!("{}/{}", self.fragment, FSD_TRASH_DIR_NAME)
        } else if let Some(trash_key) = self
            .object_key
            .strip_prefix(FSD_TRASH_DIR_NAME)
            .and_then(|v| v.strip_prefix('/'))
        {
            format!(
                "{}/{}/{}",
                self.fragment,
                FSD_TRASH_DIR_NAME,
                encode_virtual_name(trash_key)
            )
        } else {
            format!("{}/{}", self.fragment, encode_virtual_name(&self.object_key))
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

#[cfg(test)]
mod tests {
    use super::{FSD_TRASH_DIR_NAME, FsdPath, encode_virtual_name};

    #[test]
    fn parse_and_render_flat_object_key_round_trip() {
        let key = "patients/12-1222 PATIENT-NAME/file:1?.dcm";
        let mount = format!("frag/{}", encode_virtual_name(key));
        let path = FsdPath::parse(&mount).expect("path should parse");
        assert_eq!(path.fragment, "frag");
        assert_eq!(path.object_key, key);
        assert_eq!(path.mount_relative(), mount);
    }

    #[test]
    fn parse_and_render_flat_trash_object_round_trip() {
        let key = "patients/12-1222 PATIENT-NAME/file:1?.dcm";
        let mount = format!("frag/{}/{}", FSD_TRASH_DIR_NAME, encode_virtual_name(key));
        let path = FsdPath::parse(&mount).expect("trash path should parse");
        assert_eq!(path.fragment, "frag");
        assert_eq!(path.object_key, format!("{}/{}", FSD_TRASH_DIR_NAME, key));
        assert_eq!(path.mount_relative(), mount);
    }
}
