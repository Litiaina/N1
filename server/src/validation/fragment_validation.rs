use crate::object_storage::disk_manager::ObjectPathManager;

pub async fn does_fragment_exist(fragment: &str) -> bool {
    let path_manager = ObjectPathManager::new();
    let index_roots = path_manager.get_all_index_roots();

    for root in index_roots {
        if tokio::fs::metadata(root.join(fragment)).await.is_ok() {
            return true;
        }
    }
    false
}
