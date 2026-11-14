//! Named upload coordination. External multipart geometry is independent of
//! immutable LINK stripe geometry; only incompatible shapes use HDD staging.
use super::private_directory;
use crate::{
    RecordKey, Table, fragments,
    namespace::UploadHandle,
    server::{
        App,
        error::{ApiError, ApiResult},
    },
    service::UploadRequest,
};
use n1_link::PoolId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
mod allocation;
mod bridge;
mod lifecycle;
mod listing;
pub(crate) use listing::list;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Plan {
    pub handle: UploadHandle,
    pub bridge: bool,
    pub cancelled: bool,
    pub durable: bool,
    pub strict: bool,
    pub content_type: String,
    /// Only durable native ACKs may replace reconstruction of actual parts.
    /// An interrupted development session without this index is reconciled once.
    #[serde(default)]
    pub acknowledgements_indexed: bool,
    /// Established with the first strict staged payload, before its receipt.
    #[serde(default)]
    pub staging_parents_persisted: bool,
}
#[derive(Deserialize)]
pub(crate) struct InitRequest {
    pub content_type: Option<String>,
    pub total_size_bytes: u64,
    pub expected_parts: u64,
    pub part_size_bytes: Option<u64>,
    /// Opt in to server-selected geometry; the returned shape is authoritative.
    #[serde(default)]
    pub negotiate_part_size: bool,
}
#[derive(Clone, Serialize, Deserialize)]
struct Part {
    bytes: u64,
    sha256: String,
    strict: bool,
}
fn plan_key(fragment: PoolId, version: &str) -> crate::Result<RecordKey> {
    RecordKey::new(fragment, format!("upload-plan/{version}"))
}
fn directory(app: &App, fragment: PoolId, version: &str) -> PathBuf {
    app.config
        .staging_dir
        .join(fragment.to_string())
        .join(version)
}
pub(crate) async fn plan(
    app: &App,
    fragment: PoolId,
    version: &str,
    durable: bool,
) -> ApiResult<Plan> {
    if ulid::Ulid::from_string(version).map_or(true, |v| v.to_string() != version) {
        return Err(ApiError::invalid("invalid upload version"));
    }
    let key = plan_key(fragment, version)?;
    let plan: Plan = app
        .db
        .execute(move |db| {
            fragments::decode(
                db.snapshot()
                    .get(Table::Reclamation, &key)?
                    .ok_or(crate::Error::NotFound)?,
            )
        })
        .await?;
    if plan.cancelled {
        return Err(ApiError(
            axum::http::StatusCode::GONE,
            "Upload was cancelled".into(),
        ));
    }
    if plan.durable != durable {
        return Err(crate::Error::NotFound.into());
    }
    Ok(plan)
}
pub(crate) async fn init(
    app: &App,
    fragment: PoolId,
    path: String,
    request: InitRequest,
    durable: bool,
    fresh: bool,
    strict: bool,
) -> ApiResult<UploadHandle> {
    struct Shape {
        content_type: String,
        total_size_bytes: u64,
        expected_parts: u64,
        part_size_bytes: u64,
        negotiate_part_size: bool,
    }
    let part_size_bytes = request
        .part_size_bytes
        .unwrap_or(if request.expected_parts <= 1 {
            request.total_size_bytes
        } else {
            0
        });
    let mut request = Shape {
        content_type: request
            .content_type
            .unwrap_or_else(|| "application/octet-stream".into()),
        total_size_bytes: request.total_size_bytes,
        expected_parts: request.expected_parts,
        part_size_bytes,
        negotiate_part_size: request.negotiate_part_size,
    };
    crate::namespace::store::validate_upload(&path, &request.content_type)?;
    let computed = if request.total_size_bytes == 0 {
        1
    } else if request.part_size_bytes == 0 {
        0
    } else {
        request.total_size_bytes.div_ceil(request.part_size_bytes)
    };
    app.config.check_object_size(request.total_size_bytes)?;
    if !request.negotiate_part_size
        && (request.part_size_bytes > app.config.max_part_size
            || request.expected_parts > super::geometry::MAX_PARTS)
    {
        return Err(ApiError(
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            "Multipart upload exceeds configured size or part-count limits".into(),
        ));
    }
    if (request.part_size_bytes == 0 && request.total_size_bytes != 0)
        || request.expected_parts != computed
    {
        return Err(ApiError::invalid("invalid multipart upload shape"));
    }
    let mut fingerprint = serde_json::json!([
        path,
        request.content_type,
        request.total_size_bytes,
        request.part_size_bytes,
        durable,
        strict
    ]);
    let explicit_digest = hex::encode(Sha256::digest(
        serde_json::to_vec(&fingerprint).expect("JSON upload shape"),
    ));
    if request.negotiate_part_size {
        fingerprint
            .as_array_mut()
            .expect("upload shape")
            .push(serde_json::json!("negotiate"));
    }
    let digest = hex::encode(Sha256::digest(
        serde_json::to_vec(&fingerprint).expect("JSON upload shape"),
    ));
    // Both init policies serialize on the original hint. Negotiation may
    // discover an upload prepared by an explicit client; keep its ACKs and
    // saved geometry instead of allocating a replacement identity.
    let lock = app.upload_lock(fragment, &format!("reuse-{explicit_digest}"));
    let _guard = lock.write().await;
    let reuse_key = RecordKey::new(fragment, format!("upload-reuse/{digest}"))?;
    if !fresh {
        let mut lookups = vec![reuse_key.clone()];
        if request.negotiate_part_size {
            lookups.push(RecordKey::new(
                fragment,
                format!("upload-reuse/{explicit_digest}"),
            )?);
        }
        let existing: Vec<String> = app
            .db
            .execute(move |db| {
                let snapshot = db.snapshot();
                let mut versions = Vec::new();
                for key in lookups {
                    if let Some(bytes) = snapshot.get(Table::Reclamation, &key)? {
                        versions.push(fragments::decode(bytes)?);
                    }
                }
                Ok(versions)
            })
            .await?;
        for version in existing {
            if let Ok(saved) = plan(app, fragment, &version, durable).await {
                let pending = app.service.pending(fragment, &version, None).await?;
                if !pending.completed && pending.upload.expires_at > crate::middleware::auth::now()
                {
                    return Ok(saved.handle);
                }
            }
        }
    }
    let profile = app
        .pools
        .iter()
        .rev()
        .find(|p| !p.sealed)
        .ok_or_else(|| ApiError::invalid("all object pools are sealed"))?;
    let stripe = u64::from(profile.data_shards) * u64::from(profile.block_bytes);
    if request.negotiate_part_size {
        (request.part_size_bytes, request.expected_parts) = super::geometry::negotiate(
            request.total_size_bytes,
            request.part_size_bytes,
            app.config.max_part_size,
            stripe,
        )?;
    }
    let aligned = request.part_size_bytes > 0 && request.part_size_bytes.is_multiple_of(stripe);
    let backend_part = if aligned {
        request.part_size_bytes
    } else {
        (app.config.max_part_size / stripe).max(1) * stripe
    };
    // One external part can use native part zero's short tail. It must fit:
    // rounding the configured maximum down can leave a larger external tail.
    // Keep the client's saved geometry whenever conversion still needs a spool.
    let bridge = request.total_size_bytes == 0
        || (!aligned && (request.expected_parts > 1 || request.total_size_bytes > backend_part));
    let mut handle = app
        .service
        .begin_upload(UploadRequest {
            fragment,
            path,
            content_type: request.content_type.clone(),
            pool: Some(profile.id),
            total_bytes: request.total_size_bytes,
            part_bytes: backend_part,
            // Bridged ACKs are backed by M1's synchronized staged payload and
            // receipt. Conversion can use normal N1 parts: native finalize
            // persists all shards before named publication. Avoid flushing the
            // second copy after every part; retain M1 staging until success.
            durable: !bridge && (durable || strict),
            ttl_seconds: 30 * 86400,
        })
        .await?;
    handle.part_size_bytes = request.part_size_bytes;
    handle.expected_parts = request.expected_parts;
    handle.durable = durable;
    let saved = Plan {
        handle: handle.clone(),
        bridge,
        cancelled: false,
        durable,
        strict,
        content_type: request.content_type,
        acknowledgements_indexed: !bridge && (strict || durable),
        staging_parents_persisted: false,
    };
    let key = plan_key(fragment, &handle.version_id)?;
    let version = handle.version_id.clone();
    app.db
        .execute(move |db| {
            db.transact(64, |tx| {
                tx.put(Table::Reclamation, &key, &fragments::encode(&saved)?)?;
                tx.put(
                    Table::Reclamation,
                    &reuse_key,
                    &fragments::encode(&version)?,
                )
            })
        })
        .await?;
    Ok(handle)
}
pub(crate) struct PartRequest<'a> {
    pub fragment: PoolId,
    pub version: &'a str,
    pub index: u64,
    pub length: u64,
    pub durable: bool,
    pub strict: bool,
}
pub(crate) async fn part<R: AsyncRead + Unpin>(
    app: &App,
    request: PartRequest<'_>,
    source: &mut R,
) -> ApiResult<()> {
    let PartRequest {
        fragment,
        version,
        index,
        length,
        durable,
        strict,
    } = request;
    let lock = app.upload_lock(fragment, version);
    let _guard = lock.read().await;
    let plan = plan(app, fragment, version, durable).await?;
    if index >= plan.handle.expected_parts
        || length
            != plan
                .handle
                .total_size_bytes
                .saturating_sub(index * plan.handle.part_size_bytes)
                .min(plan.handle.part_size_bytes)
    {
        return Err(ApiError::invalid(
            "part length or index differs from upload shape",
        ));
    }
    let strict = strict || plan.strict || durable;
    if strict && !plan.strict && !durable {
        return Err(crate::Error::NameConflict(
            "strict durability cannot upgrade an existing relaxed upload".into(),
        )
        .into());
    }
    let part_lock = app.upload_lock(fragment, &format!("{version}/part/{index}"));
    let _part_guard = part_lock.try_write().map_err(|_| {
        ApiError(
            axum::http::StatusCode::CONFLICT,
            "Part is already being written".into(),
        )
    })?;
    if !plan.bridge {
        app.service
            .upload_part(fragment, version, index, length, source)
            .await?;
        if plan.strict || durable {
            let version = version.to_owned();
            app.db
                .execute(move |db| {
                    db.transact(64, |tx| {
                        // A cancelled HTTP future does not cancel an already-owned DB
                        // worker. Recheck lifecycle inside its retrying transaction.
                        lifecycle::accepting_parts(tx, fragment, &version)?;
                        super::progress::acknowledge(tx, fragment, &version, index)
                    })
                })
                .await?;
        }
        return Ok(());
    }
    let pending = app.service.pending(fragment, version, None).await?;
    if pending.completed {
        return Err(crate::Error::NameConflict("upload is already finalized".into()).into());
    }
    let dir = directory(app, fragment, version);
    private_directory(&dir).await?;
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let temp = dir.join(format!("{index}.{}.tmp", ulid::Ulid::new()));
    let result = async {
        let mut file = options.open(&temp).await?;
        let mut remaining = length;
        let mut hash = Sha256::new();
        let (mut buffer, _memory) = super::io_buffer(app).await?;
        while remaining > 0 {
            let wanted = remaining.min(buffer.len() as u64) as usize;
            // Coalesce short HTTP/native reads before submitting filesystem
            // work. Only complete parts can receive an acknowledgement.
            if let Err(error) = source.read_exact(&mut buffer[..wanted]).await {
                if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Err(ApiError::invalid("part body ended early"));
                }
                return Err(error.into());
            }
            file.write_all(&buffer[..wanted]).await?;
            hash.update(&buffer[..wanted]);
            remaining -= wanted as u64;
        }
        if strict {
            file.sync_all().await?;
        } else {
            file.flush().await?;
        }
        drop(file);
        let record = Part {
            bytes: length,
            sha256: hex::encode(hash.finalize()),
            strict,
        };
        let part_key = RecordKey::new(fragment, format!("upload-part/{version}/{index:06}"))?;
        let lookup = part_key.clone();
        let previous: Option<Part> = app
            .db
            .execute(move |db| {
                db.snapshot()
                    .get(Table::Reclamation, &lookup)?
                    .map(fragments::decode)
                    .transpose()
            })
            .await?;
        if let Some(previous) = previous
            && (previous.bytes != record.bytes
                || previous.sha256 != record.sha256
                || (strict && !previous.strict))
        {
            return Err(crate::Error::NameConflict(
                "part retry differs from its prior bytes or durability".into(),
            )
            .into());
        }
        tokio::fs::rename(&temp, dir.join(format!("{index}.part"))).await?;
        super::persist_part_directory(
            &dir,
            &app.config.staging_dir,
            strict && !plan.staging_parents_persisted,
        )
        .await?;
        let record_version = version.to_owned();
        app.db
            .execute(move |db| {
                db.transact(64, |tx| {
                    let mut saved = lifecycle::accepting_parts(tx, fragment, &record_version)?;
                    if strict && !saved.staging_parents_persisted {
                        saved.staging_parents_persisted = true;
                        tx.put(
                            Table::Reclamation,
                            &plan_key(fragment, &record_version)?,
                            &fragments::encode(&saved)?,
                        )?;
                    }
                    tx.put(Table::Reclamation, &part_key, &fragments::encode(&record)?)
                })
            })
            .await?;
        Ok(())
    }
    .await;
    let _ = tokio::fs::remove_file(&temp).await;
    result
}
pub(crate) async fn status(
    app: &App,
    fragment: PoolId,
    version: &str,
    durable: bool,
) -> ApiResult<serde_json::Value> {
    let lock = app.upload_lock(fragment, version);
    {
        let _guard = lock.read().await;
        let saved = plan(app, fragment, version, durable).await?;
        if saved.bridge || saved.acknowledgements_indexed {
            // Native ACK snapshots and bridge receipt/file probes are read
            // only. Parts already hold a shared lease; observing committed
            // ACKs must not wait for a body or a delayed durability barrier.
            return status_from_plan(app, fragment, version, durable, saved).await;
        }
    }
    // Reconciliation can change the saved ACK index. Keep its exclusive lease
    // and reload after upgrading so finalize/cancel/GC cannot cross the query.
    let _guard = lock.write().await;
    status_locked(app, fragment, version, durable).await
}
async fn status_locked(
    app: &App,
    fragment: PoolId,
    version: &str,
    durable: bool,
) -> ApiResult<serde_json::Value> {
    let plan = plan(app, fragment, version, durable).await?;
    status_from_plan(app, fragment, version, durable, plan).await
}
async fn status_from_plan(
    app: &App,
    fragment: PoolId,
    version: &str,
    durable: bool,
    plan: Plan,
) -> ApiResult<serde_json::Value> {
    let pending = app.service.pending(fragment, version, None).await?;
    let mut uploaded = Vec::new();
    if pending.completed {
        uploaded.extend(0..plan.handle.expected_parts);
    } else if plan.bridge {
        let prefix = format!("upload-part/{version}/");
        let rows = app
            .db
            .execute(move |db| {
                let mut parts = Vec::new();
                let mut after = None;
                loop {
                    let rows = db.snapshot().scan(
                        Table::Reclamation,
                        fragment,
                        &prefix,
                        after.as_ref(),
                        1000,
                    )?;
                    if rows.is_empty() {
                        break;
                    }
                    after = rows.last().map(|(key, _)| key.clone());
                    for (key, bytes) in rows {
                        let part: Part = fragments::decode(bytes)?;
                        parts.push((
                            key.identifier()
                                .rsplit('/')
                                .next()
                                .unwrap_or("")
                                .parse::<u64>()
                                .map_err(|_| {
                                    crate::Error::Incompatible("invalid staged part index".into())
                                })?,
                            part,
                        ));
                    }
                }
                Ok(parts)
            })
            .await?;
        let dir = directory(app, fragment, version);
        for (index, record) in rows {
            if tokio::fs::metadata(dir.join(format!("{index}.part")))
                .await
                .is_ok_and(|m| m.is_file() && m.len() == record.bytes)
            {
                uploaded.push(index);
            }
        }
    } else if plan.acknowledgements_indexed {
        let version = version.to_owned();
        let expected = plan.handle.expected_parts;
        uploaded = app
            .db
            .execute(move |db| {
                super::progress::uploaded(&db.snapshot(), fragment, &version, expected)
            })
            .await?;
    } else {
        let mut start = 0;
        while start < plan.handle.expected_parts {
            let report = app
                .service
                .upload_status(fragment, version, start, 4096)
                .await?;
            if !report.unavailable_members.is_empty() {
                return Err(ApiError(
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "Some upload members are unavailable".into(),
                ));
            }
            uploaded.extend(
                (report.checked_start..report.checked_end)
                    .filter(|part| !report.missing_parts.contains(part)),
            );
            start = report.checked_end;
        }
        if plan.strict || durable {
            let version = version.to_owned();
            let verified = uploaded.clone();
            let expected = plan.handle.expected_parts;
            uploaded = app
                .db
                .execute(move |db| {
                    // Valid frames alone do not prove that a failed/abandoned
                    // native operation completed its durability barrier. Keep
                    // only durable ACKs that still pass actual reconstruction;
                    // uncertain parts are safely retransmitted with the same
                    // LINK key rather than promoted into new ACKs.
                    let acknowledged =
                        super::progress::uploaded(&db.snapshot(), fragment, &version, expected)?;
                    let retained = acknowledged
                        .into_iter()
                        .filter(|part| verified.binary_search(part).is_ok())
                        .collect::<Vec<_>>();
                    db.transact(64, |tx| {
                        let key = plan_key(fragment, &version)?;
                        let mut saved: Plan = fragments::decode(
                            tx.get(Table::Reclamation, &key)?
                                .ok_or(crate::Error::NotFound)?,
                        )?;
                        let pending: crate::namespace::PendingUpload = fragments::decode(
                            tx.get(Table::Uploads, &RecordKey::new(fragment, &version)?)?
                                .ok_or(crate::Error::NotFound)?,
                        )?;
                        if saved.cancelled || pending.completed {
                            return Ok(Vec::new());
                        }
                        super::progress::replace(tx, fragment, &version, &retained)?;
                        saved.acknowledgements_indexed = true;
                        tx.put(Table::Reclamation, &key, &fragments::encode(&saved)?)?;
                        Ok(retained.clone())
                    })
                })
                .await?;
        }
    }
    let bytes: u64 = uploaded
        .iter()
        .map(|i| {
            plan.handle
                .total_size_bytes
                .saturating_sub(i * plan.handle.part_size_bytes)
                .min(plan.handle.part_size_bytes)
        })
        .sum();
    Ok(serde_json::json!({
        "version_id": version,
        "session_id": plan.handle.session_id,
        "object_key": plan.handle.object_key,
        "total_size_bytes": plan.handle.total_size_bytes,
        "part_size_bytes": plan.handle.part_size_bytes,
        "expected_parts": plan.handle.expected_parts,
        "uploaded_parts": uploaded,
        "committed_parts": uploaded.len(),
        "uploaded_bytes": bytes,
        "committed_bytes": bytes,
        "completed": pending.completed,
        "durable": durable
    }))
}
pub(crate) async fn finalize(
    app: Arc<App>,
    fragment: PoolId,
    version: String,
    session: String,
    durable: bool,
    body: serde_json::Value,
) -> ApiResult<crate::namespace::PublishedVersion> {
    let lock = app.upload_lock(fragment, &version);
    let _guard = lock.write().await;
    let plan = plan(&app, fragment, &version, durable).await?;
    if plan.handle.session_id != session {
        return Err(crate::Error::NotFound.into());
    }
    for (name, expected) in [
        ("total_size_bytes", plan.handle.total_size_bytes),
        ("expected_parts", plan.handle.expected_parts),
    ] {
        if body.get(name).is_some_and(|v| v.as_u64() != Some(expected)) {
            return Err(crate::Error::NameConflict(
                "finalize body differs from upload shape".into(),
            )
            .into());
        }
    }
    if body
        .get("content_type")
        .is_some_and(|v| v.as_str() != Some(plan.content_type.as_str()))
    {
        return Err(crate::Error::NameConflict(
            "finalize content type differs from upload shape".into(),
        )
        .into());
    }
    let pending = app
        .service
        .pending(fragment, &version, Some(&session))
        .await?;
    let physically_complete = if plan.bridge && !pending.completed {
        match app.service.objects().stat(&pending.upload.object_key).await {
            Ok(_) => true,
            Err(error) if error.status() == 404 => false,
            Err(error) => return Err(error.into()),
        }
    } else {
        pending.completed
    };
    if plan.bridge && !physically_complete {
        let state = status_locked(&app, fragment, &version, durable).await?;
        if state["uploaded_parts"].as_array().map_or(0, Vec::len) as u64
            != plan.handle.expected_parts
        {
            return Err(crate::Error::NameConflict("upload is missing parts".into()).into());
        }
        let dir = directory(&app, fragment, &version);
        let upload = allocation::ready(&app, fragment, &version, &pending.upload).await?;
        // One memory reservation covers verification and the following file
        // stream. No bytes from a file reach N1 before its checksum passes.
        let (buffer, memory) = super::io_buffer(&app).await?;
        let memory = Arc::new(memory);
        let capacity = buffer.len();
        drop(buffer);
        // Keep its permit while replacing the verification buffer with one
        // ReaderStream buffer. Only one staged file is open at a time.
        if upload.parts == 0 {
            bridge::verified_file(&app, fragment, &version, &dir, 0, capacity, memory.clone())
                .await?;
            app.service
                .objects()
                .put(&upload, &mut tokio::io::empty())
                .await?;
        }
        let mut source = bridge::source(
            app.clone(),
            fragment,
            version.clone(),
            dir,
            plan.handle.expected_parts,
            capacity,
            memory,
        );
        for index in 0..upload.parts {
            let bytes = upload
                .total_bytes
                .saturating_sub(index * upload.part_bytes)
                .min(upload.part_bytes);
            app.service
                .objects()
                .part(&upload, index, bytes, &mut (&mut source).take(bytes))
                .await
                .map_err(bridge::forward_error)?;
        }
    }
    let result = match app.service.finalize(fragment, &version, &session).await {
        Ok(result) => result,
        Err(error) => {
            // An ACK records successful persistence, not perpetual media
            // health. If native validation now fails, the next resume must
            // reconcile actual shards rather than repeatedly skipping them.
            if !plan.bridge && matches!(&error, crate::service::ServiceError::Object(_)) {
                let key = plan_key(fragment, &version)?;
                app.db
                    .execute(move |db| {
                        db.transact(64, |tx| {
                            let mut saved: Plan = fragments::decode(
                                tx.get(Table::Reclamation, &key)?
                                    .ok_or(crate::Error::NotFound)?,
                            )?;
                            saved.acknowledgements_indexed = false;
                            tx.put(Table::Reclamation, &key, &fragments::encode(&saved)?)
                        })
                    })
                    .await?;
            }
            return Err(error.into());
        }
    };
    app.emit(fragment);
    if plan.bridge {
        let _ = tokio::fs::remove_dir_all(directory(&app, fragment, &version)).await;
    }
    Ok(result)
}
pub(crate) async fn cancel(
    app: &App,
    fragment: PoolId,
    version: &str,
    durable: bool,
) -> ApiResult<()> {
    let lock = app.upload_lock(fragment, version);
    let _guard = lock.write().await;
    let mut saved = plan(app, fragment, version, durable).await?;
    let pending = app.service.pending(fragment, version, None).await?;
    if pending.completed {
        return Err(crate::Error::NameConflict("upload is already finalized".into()).into());
    }
    saved.cancelled = true;
    let key = plan_key(fragment, version)?;
    let pending_key = RecordKey::new(fragment, format!("pending/{version}"))?;
    let cleanup_version = version.to_owned();
    app.db
        .execute(move |db| {
            db.transact(64, |tx| {
                tx.put(Table::Reclamation, &key, &fragments::encode(&saved)?)?;
                tx.remove(Table::Reclamation, &pending_key)?;
                super::progress::remove(tx, fragment, &cleanup_version)
            })
        })
        .await?;
    app.service.objects().cancel(&pending.upload).await?;
    let _ = tokio::fs::remove_dir_all(directory(app, fragment, version)).await;
    Ok(())
}
