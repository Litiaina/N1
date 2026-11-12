//! Restart admission is paced through the same bounded queue as new exports.
use super::{models::PersistedExport, store};
use crate::{Table, fragments};
use std::sync::atomic::Ordering;

pub(super) async fn recover() -> crate::server::error::ApiResult<()> {
    let ctx = store::context();
    loop {
        match recover_once().await {
            Ok(()) => return Ok(()),
            Err(error) => tracing::warn!(%error, "export restart admission deferred"),
        }
        tokio::select! {
            _ = ctx.stop.cancelled() => return Ok(()),
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
        }
    }
}

async fn recover_once() -> crate::server::error::ApiResult<()> {
    let ctx = store::context();
    let mut after = None;
    loop {
        if ctx.stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        let cursor = after.clone();
        let rows = ctx
            .db
            .execute(move |db| {
                db.snapshot().scan(
                    Table::Reclamation,
                    fragments::CATALOG,
                    "export/",
                    cursor.as_ref(),
                    128,
                )
            })
            .await?;
        if rows.is_empty() {
            return Ok(());
        }
        after = rows.last().map(|row| row.0.clone());
        for (_, bytes) in rows {
            let record: PersistedExport = fragments::decode(bytes)?;
            if record.phase.is_terminal() {
                let id = record.id;
                ctx.db
                    .execute(move |db| crate::namespace::Namespace::new(db).unpin_job(&id))
                    .await?;
                continue;
            }
            if record.expires_at_unix <= crate::middleware::auth::now() as i64 {
                continue;
            }
            let permit = tokio::select! {
                _ = ctx.stop.cancelled() => return Ok(()),
                permit = ctx.queue.clone().acquire_owned() => permit
                    .map_err(|_| crate::server::error::ApiError::invalid("export queue closed"))?,
            };
            store::enqueue_admitted(record, permit)?;
        }
    }
}

pub(super) async fn run() {
    let ctx = store::context();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(
        ctx.config.cleanup_interval_seconds.max(1),
    ));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut after = None;
    loop {
        tokio::select! {
            _ = ctx.stop.cancelled() => return,
            _ = interval.tick() => {
                match super::lifecycle::sweep(after.take()).await {
                    Ok(next) => after = next,
                    Err(error) => tracing::warn!(%error, "export retention cleanup deferred"),
                }
            }
        }
    }
}
