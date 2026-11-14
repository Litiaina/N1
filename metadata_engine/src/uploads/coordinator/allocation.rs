//! A durable M1 source can outlive N1's intentionally volatile unused Begin.
//! A zero-length status range probes headers only. Recover confirmed total
//! absence with a fresh N1 Begin, never by recreating the missing LINK key.
use super::*;
use crate::object_store::{ObjectStoreError, ObjectUpload};

pub(super) async fn ready(
    app: &App,
    fragment: PoolId,
    version: &str,
    upload: &ObjectUpload,
) -> ApiResult<ObjectUpload> {
    let objects = app.service.objects();
    match objects.status(upload, upload.parts, 1).await {
        Ok(status) => {
            if status.parts != upload.parts
                || status.checked_start != upload.parts
                || status.checked_end != upload.parts
                || !status.missing_parts.is_empty()
            {
                return Err(ObjectStoreError::Contract(
                    "object service returned a different allocation probe",
                )
                .into());
            }
            if !status.unavailable_members.is_empty() {
                return Err(ApiError(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "Some upload members are unavailable".into(),
                ));
            }
            return Ok(upload.clone());
        }
        Err(error) if error.status() == 404 => (),
        Err(error) => return Err(error.into()),
    }
    let now = crate::middleware::auth::now();
    if upload.expires_at <= now {
        return Err(crate::Error::NameConflict("upload has expired".into()).into());
    }
    let identity = n1_link::ObjectKey::inspect(&upload.object_key)
        .map_err(|_| crate::Error::Incompatible("invalid saved object allocation".into()))?;
    // Clear any interrupted deletion of this already-absent identity before
    // starting another one. The existing pending row retains the old key until
    // the atomic reference/expiry switch, so interruption can repeat this step.
    objects.purge(&upload.object_key).await?;
    let fresh = objects
        .init(
            Some(identity.pool),
            upload.total_bytes,
            upload.part_bytes,
            upload.durable,
            upload.expires_at - now,
        )
        .await?;
    let replacement = n1_link::ObjectKey::inspect(&fresh.object_key)
        .map_err(|_| ObjectStoreError::Contract("invalid replacement object allocation"))?;
    let expected = upload.object_key.clone();
    let saved = fresh.clone();
    let version = version.to_owned();
    let result =
        if replacement.pool != identity.pool || replacement.generation != identity.generation {
            Err(crate::Error::NameConflict(
                "object pool generation changed".into(),
            ))
        } else {
            app.db
                .execute(move |db| {
                    db.transact(64, |tx| {
                        lifecycle::accepting_parts(tx, fragment, &version)?;
                        crate::namespace::store::lifecycle::replace_upload_allocation(
                            tx, fragment, &version, &expected, &saved,
                        )
                    })
                })
                .await
        };
    if let Err(error) = result {
        let _ = objects.cancel(&fresh).await;
        return Err(error.into());
    }
    Ok(fresh)
}
