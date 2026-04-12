pub const NAMESPACE_ENTRIES_TABLE: &str = "namespace_entries_v2";
pub const OBJECT_HEADS_TABLE: &str = "object_heads_v2";
pub const OBJECT_VERSIONS_TABLE: &str = "object_versions_v2";
pub const UPLOAD_SESSIONS_TABLE: &str = "upload_sessions_v2";
pub const GC_QUEUE_TABLE: &str = "gc_queue_v2";

pub fn namespace_entry_row_key(fragment: &str, path: &str) -> String {
    format!("{fragment}\u{0000}{path}")
}

pub fn namespace_children_prefix(fragment: &str, parent_path: &str) -> String {
    if parent_path.is_empty() {
        format!("{fragment}\u{0000}")
    } else {
        format!("{fragment}\u{0000}{parent_path}/")
    }
}

pub fn object_head_row_key(fragment: &str, object_id: &str) -> String {
    format!("{fragment}\u{0000}{object_id}")
}

pub fn object_version_row_key(fragment: &str, object_id: &str, version_id: &str) -> String {
    format!("{fragment}\u{0000}{object_id}\u{0000}{version_id}")
}

pub fn gc_queue_row_key(fragment: &str, object_id: &str) -> String {
    format!("{fragment}\u{0000}{object_id}")
}
