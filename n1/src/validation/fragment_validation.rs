use crate::core::disk_manager::ObjectPathManager;
use crate::core::fragment_identity::fragment_alias_path;

pub async fn does_fragment_exist(fragment: &str) -> bool {
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    for root in index_roots {
        if tokio::fs::metadata(fragment_alias_path(&root, fragment))
            .await
            .is_ok()
        {
            return true;
        }
    }
    false
}
