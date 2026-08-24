use super::*;

pub fn spawn_reserve_set_task() {
    if !CONFIG.reserve_set.enabled {
        return;
    }
    let cfg = CONFIG.reserve_set.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(cfg.initial_delay_seconds)).await;
        loop {
            if cfg.automatic_promotion
                && let Err(error) = automatic_cycle().await
            {
                let mut status = STATUS.write().await;
                status.running = false;
                status.last_error = Some(error.to_string());
            }
            sleep(Duration::from_secs(cfg.probe_interval_seconds)).await;
        }
    });
}

pub async fn reserve_status(claims: Claims) -> Result<Json<ReserveSetStatus>, AppError> {
    if !claims.read {
        return Err(AppError::Forbidden("Read permission required".into()));
    }
    Ok(Json(status_snapshot().await))
}

pub async fn promote_reserve(
    claims: Claims,
    Json(request): Json<ReservePromotionRequest>,
) -> Result<Json<ReserveSetStatus>, AppError> {
    if !claims.modify {
        return Err(AppError::Forbidden("Modify permission required".into()));
    }
    Ok(Json(
        execute_promotion(&request.slot_id, &request.reserve_instance_id).await?,
    ))
}

pub(super) async fn peer_prepare(
    headers: HeaderMap,
    Json(request): Json<PeerReservePrepareRequest>,
) -> Result<Json<PeerReservePrepareResponse>, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(local_prepare_vote(&request).await?))
}

pub(super) async fn peer_commit(
    headers: HeaderMap,
    Json(request): Json<PeerReserveCommitRequest>,
) -> Result<Json<PeerReserveCommitResponse>, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(local_commit(&request).await?))
}

pub(super) async fn peer_activate(
    headers: HeaderMap,
    Json(request): Json<PeerReserveActivateRequest>,
) -> Result<Json<PeerReserveCommitResponse>, AppError> {
    authorize_peer(&headers)?;
    Ok(Json(local_activate(&request).await?))
}

pub(super) async fn peer_membership(
    headers: HeaderMap,
) -> Result<Json<PeerReserveMembershipResponse>, AppError> {
    authorize_peer(&headers)?;
    let runtime = active_cluster_runtime()?;
    Ok(Json(PeerReserveMembershipResponse {
        local_instance_id: runtime.local_instance_id.clone(),
        membership: runtime.membership.clone(),
    }))
}

pub fn build_reserve_peer_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/n2/peer/reserve/membership", get(peer_membership))
        .route("/n2/peer/reserve/prepare", post(peer_prepare))
        .route("/n2/peer/reserve/commit", post(peer_commit))
        .route("/n2/peer/reserve/activate", post(peer_activate))
}

pub fn reserve_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/noa/v1/cluster/reserve", get(reserve_status))
        .route("/noa/v1/cluster/reserve/promote", post(promote_reserve))
}
