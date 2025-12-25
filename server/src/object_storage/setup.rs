use futures_util::StreamExt;
use mongodb::{Client, bson::doc, options::ClientOptions};
use std::error::Error;
use tracing::{error, info, warn};
use walkdir::WalkDir;

use crate::{
    database::database_constants::DATABASE,
    object_storage::{
        constants::OBJECT_META_FILE,
        quota_manager::{init_quota_file, set_storage_used},
    },
    statics::database_statics::DATABASE_URL,
    structs::object_storage_structs::{ObjectMetadata, ObjectPathManager, UserRecord},
};

pub async fn setup_quota() -> Result<(), Box<dyn Error>> {
    info!("Background Task: Connecting to MongoDB for Quota Sync...");

    let client_options = ClientOptions::parse(&*DATABASE_URL).await?;
    let client = Client::with_options(client_options)?;
    let db = client.database(DATABASE);
    let auth_collection = db.collection::<UserRecord>("auth");

    info!("Background Task: Verifying consistency (MongoDB <-> Disk)...");

    let mut cursor = auth_collection.find(doc! {}).await?;
    let mut count = 0;
    let path_manager = ObjectPathManager::new();

    while let Some(result) = cursor.next().await {
        match result {
            Ok(user) => {
                let uid = user.uid.clone();

                if let Err(e) = init_quota_file(&uid, user.allocated_storage).await {
                    error!("Failed to sync quota file for {}: {}", uid, e);
                    continue;
                }

                let user_dir = path_manager.get_user_dir(&uid);

                if user_dir.exists() {
                    let calculated_usage = tokio::task::spawn_blocking(move || {
                        let mut total = 0u64;
                        for entry in WalkDir::new(&user_dir).into_iter().filter_map(|e| e.ok()) {
                            if entry.file_name() == OBJECT_META_FILE {
                                if let Ok(data) = std::fs::read(entry.path()) {
                                    if let Ok(meta) =
                                        mongodb::bson::from_slice::<ObjectMetadata>(&data)
                                    {
                                        if !meta.is_delete_marker {
                                            total += meta.total_size_bytes;
                                        }
                                    }
                                }
                            }
                        }
                        total
                    })
                    .await?;

                    if let Err(e) = set_storage_used(&uid, calculated_usage).await {
                        warn!("Failed to update usage stats for {}: {}", uid, e);
                    }
                }

                count += 1;
            }
            Err(e) => {
                error!("Failed to retrieve user document during sync: {}", e);
            }
        }
    }

    info!(
        "Background Task: Consistency check complete. Verified {} users.",
        count
    );

    Ok(())
}
