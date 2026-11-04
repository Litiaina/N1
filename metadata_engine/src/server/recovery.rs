//! Recover only metadata's unfinished publication intents. This is neither an
//! object census nor an automatic healing scan.
use super::{
    App,
    error::{ApiError, ApiResult},
};
use crate::{Table, fragments};
use futures_util::{StreamExt, TryStreamExt};
use std::sync::Arc;
pub(super) async fn reconcile(app: &Arc<App>) -> ApiResult<()> {
    let mut after = None;
    loop {
        let cursor = after.clone();
        let fragments = app
            .db
            .execute(move |db| {
                db.snapshot().scan(
                    Table::Reclamation,
                    fragments::CATALOG,
                    "fragment/",
                    cursor.as_ref(),
                    1000,
                )
            })
            .await?;
        if fragments.is_empty() {
            break;
        }
        after = fragments.last().map(|r| r.0.clone());
        for (_, bytes) in fragments {
            let fragment: fragments::Fragment = fragments::decode(bytes)?;
            if fragment.deleted {
                continue;
            }
            let id = fragment.id;
            let mut pending_after = None;
            loop {
                let cursor = pending_after.clone();
                let rows = app
                    .db
                    .execute(move |db| {
                        db.snapshot().scan(
                            Table::Reclamation,
                            id,
                            "pending/",
                            cursor.as_ref(),
                            1000,
                        )
                    })
                    .await?;
                if rows.is_empty() {
                    break;
                }
                pending_after = rows.last().map(|r| r.0.clone());
                let work = rows
                    .into_iter()
                    .map(|(_, bytes)| {
                        let app = app.clone();
                        async move {
                            let record: serde_json::Value = fragments::decode(bytes)?;
                            let version = record["version"].as_str().ok_or_else(|| {
                                ApiError::invalid("invalid pending publication intent")
                            })?;
                            let session = record["session"].as_str().ok_or_else(|| {
                                ApiError::invalid("invalid pending publication session")
                            })?;
                            let gc_key = crate::RecordKey::new(id, format!("upload-gc/{version}"))?;
                            if app
                                .db
                                .execute(move |db| {
                                    Ok(db.snapshot().get(Table::Reclamation, &gc_key)?.is_some())
                                })
                                .await?
                            {
                                // A durable expiry claim owns this identity.
                                // Let maintenance resume retirement; do not
                                // attempt named publication or prevent startup.
                                return Ok(());
                            }
                            let pending = app.service.pending(id, version, Some(session)).await?;
                            match app.service.objects().stat(&pending.upload.object_key).await {
                                Ok(_) => {
                                    if app
                                        .service
                                        .recover_publication(id, version, session)
                                        .await
                                        .is_err()
                                    {
                                        let mut errors = app
                                            .recovery_errors
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner());
                                        if errors.len() < 32 {
                                            errors.push(format!(
                                                "unfinished metadata publication {version}"
                                            ));
                                        }
                                    }
                                }
                                Err(error) if error.status() == 404 => (),
                                Err(_) => {
                                    let mut errors = app
                                        .recovery_errors
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner());
                                    if errors.len() < 32 {
                                        errors.push(format!(
                                            "object unavailable for pending publication {version}"
                                        ));
                                    }
                                }
                            }
                            Ok::<(), ApiError>(())
                        }
                    })
                    .collect::<Vec<_>>();
                futures_util::stream::iter(work)
                    .buffer_unordered(16)
                    .try_collect::<Vec<_>>()
                    .await?;
            }
        }
    }
    Ok(())
}
