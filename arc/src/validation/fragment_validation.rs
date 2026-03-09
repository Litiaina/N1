use crate::n1_core::disk_manager::ObjectPathManager;
use crate::n1_core::index_facade::{IndexBackendMode, index_backend_mode};
use crate::n1_core::raw_index;

pub async fn does_fragment_exist(fragment: &str) -> bool {
    if index_backend_mode() != IndexBackendMode::Fs {
        if let Ok(fragments) = raw_index::list_fragments().await {
            return fragments.iter().any(|f| f == fragment);
        }
        return false;
    }

    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    for root in index_roots {
        if tokio::fs::metadata(root.join(fragment)).await.is_ok() {
            return true;
        }
    }
    false
}
