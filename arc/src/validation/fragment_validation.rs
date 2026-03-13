use crate::n1_core::raw_index;

pub async fn does_fragment_exist(fragment: &str) -> bool {
    raw_index::fragment_exists(fragment).await.unwrap_or(false)
}
