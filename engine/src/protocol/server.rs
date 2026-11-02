//! Direct NOA over n1://. A control frame is u32be length + MessagePack Request
//! or Response. Declared object bytes follow outside the control frame.
mod download;
pub mod startup;
use crate::{
    core::{Error, Result},
    middleware::auth_middleware::Claims,
    protocol::ops::Service,
};
use n1_link::noa::{
    Command, MAX_BODY_BYTES, MAX_CONTROL_BYTES, MAX_PREAUTH_BYTES, Request, Response,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
};

pub async fn serve(listener: TcpListener, service: Arc<Service>) -> Result<()> {
    serve_until(
        listener,
        service,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
}
pub async fn serve_until(
    listener: TcpListener,
    service: Arc<Service>,
    stop: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let connections = Arc::new(Semaphore::new(service.config.max_connections));
    // KiB permits cover control buffers across all connections, including
    // callers that stall after announcing a large frame.
    let control_memory = Arc::new(Semaphore::new(64 * 1024));
    let tls = match &service.config.tls {
        Some(config) => Some(tokio_rustls::TlsAcceptor::from(
            axum_server::tls_rustls::RustlsConfig::from_pem_file(
                &config.certificate,
                &config.private_key,
            )
            .await?
            .get_inner(),
        )),
        None => None,
    };
    let mut workers = tokio::task::JoinSet::new();
    loop {
        let accepted = tokio::select! {_=stop.cancelled()=>break,result=listener.accept()=>result};
        let (stream, peer) = accepted?;
        let Ok(permit) = connections.clone().try_acquire_owned() else {
            drop(stream);
            continue;
        };
        stream.set_nodelay(true)?;
        let service = service.clone();
        let tls = tls.clone();
        let control_memory = control_memory.clone();
        let stop = stop.clone();
        workers.spawn(async move {
            let _permit = permit;
            let result = match tls {
                Some(tls) => {
                    match tokio::time::timeout(Duration::from_secs(15), tls.accept(stream)).await {
                        Ok(Ok(stream)) => {
                            connection(stream, service, control_memory, stop, peer.ip()).await
                        }
                        Ok(Err(e)) => Err(e.into()),
                        Err(_) => Err(Error::Unavailable("native TLS handshake timed out".into())),
                    }
                }
                None => connection(stream, service, control_memory, stop, peer.ip()).await,
            };
            if let Err(e) = result {
                tracing::debug!("native connection ended: {e}");
            }
        });
        while workers.try_join_next().is_some() {}
    }
    drop(listener);
    while workers.join_next().await.is_some() {}
    Ok(())
}

async fn connection<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    mut socket: S,
    service: Arc<Service>,
    control_memory: Arc<Semaphore>,
    stop: tokio_util::sync::CancellationToken,
    peer: std::net::IpAddr,
) -> Result<()> {
    let mut session: Option<Claims> = None;
    loop {
        let limit = if session.is_some() {
            MAX_CONTROL_BYTES
        } else {
            MAX_PREAUTH_BYTES
        };
        let frame = tokio::select! {_=stop.cancelled()=>return Ok(()),frame=read_frame::<_,Request>(&mut socket,limit,control_memory.clone())=>frame};
        let Some((mut request, _control_memory)) = frame? else {
            return Ok(());
        };
        let id = request.id;
        let signed_claims = if let Command::SignedGet { grant } = &request.command {
            if request.body_len != 0 {
                return Err(Error::Invalid("signed download cannot carry a body".into()));
            }
            match service.auth.signed_claims(grant) {
                Ok(claims) => {
                    request.command = Command::ObjectGet {
                        object_key: grant.object_key.clone(),
                        offset: grant.offset,
                        length: grant.length,
                    };
                    Some(claims)
                }
                Err(e) => {
                    write_frame(&mut socket, &Response::error(id, e.status(), e.to_string()))
                        .await?;
                    continue;
                }
            }
        } else {
            None
        };
        let authentication = matches!(
            &request.command,
            Command::Login { .. }
                | Command::Bearer { .. }
                | Command::Refresh { .. }
                | Command::Logout
        );
        if let Err(error) = service.limits.peer(peer, false).and_then(|()| {
            if authentication {
                service.limits.peer(peer, true)
            } else {
                Ok(())
            }
        }) {
            write_frame(
                &mut socket,
                &Response::error(id, error.status(), error.to_string()),
            )
            .await?;
            if request.body_len != 0 {
                return Ok(());
            }
            continue;
        }
        let max_body = if matches!(&request.command, Command::UploadPut { .. }) {
            i64::MAX as u64
        } else {
            MAX_BODY_BYTES
        };
        if request.body_len > max_body {
            write_frame(
                &mut socket,
                &Response::error(id, 413, "native body exceeds limit"),
            )
            .await?;
            return Ok(());
        }
        let mut response = match request.command {
            Command::Ping => {
                if request.body_len != 0 {
                    return Err(Error::Invalid("ping cannot carry a body".into()));
                }
                Response::ok(id, service.ping())
            }
            Command::Login {
                fragment,
                secret,
                permissions,
                ttl_seconds,
            } => {
                if request.body_len != 0 {
                    return Err(Error::Invalid("authentication cannot carry a body".into()));
                }
                match service
                    .auth
                    .login(fragment, secret, permissions, ttl_seconds)
                    .await
                {
                    Ok(login) => {
                        session = Some(login.session.clone());
                        let value = json!(login);
                        Response::ok(id, value)
                    }
                    Err(e) => Response::error(id, e.status(), e.to_string()),
                }
            }
            Command::Refresh { refresh_token } => {
                if request.body_len != 0 {
                    return Err(Error::Invalid("authentication cannot carry a body".into()));
                }
                match service.auth.refresh(&refresh_token).await {
                    Ok(login) => {
                        session = Some(login.session.clone());
                        Response::ok(id, json!(login))
                    }
                    Err(e) => {
                        session = None;
                        Response::error(id, e.status(), e.to_string())
                    }
                }
            }
            Command::Bearer { token } => {
                if request.body_len != 0 {
                    return Err(Error::Invalid("authentication cannot carry a body".into()));
                }
                match service.auth.verify(&token) {
                    Ok(claims) => {
                        let value = json!({"session":claims});
                        session = Some(claims);
                        Response::ok(id, value)
                    }
                    Err(e) => {
                        session = None;
                        Response::error(id, e.status(), e.to_string())
                    }
                }
            }
            Command::Logout => {
                if request.body_len != 0 {
                    return Err(Error::Invalid("logout cannot carry a body".into()));
                }
                if let Some(claims) = session.take() {
                    service.auth.logout(&claims).await?;
                }
                Response::ok(id, json!({"authenticated":false}))
            }
            command => {
                let Some(claims) = signed_claims.as_ref().or(session.as_ref()) else {
                    write_frame(
                        &mut socket,
                        &Response::error(id, 401, "authenticate before object operations"),
                    )
                    .await?;
                    if request.body_len != 0 {
                        return Ok(());
                    }
                    continue;
                };
                if let Err(e) = service
                    .authorize(claims, &command)
                    .and_then(|()| service.limits.operation(claims, &command))
                {
                    write_frame(&mut socket, &Response::error(id, e.status(), e.to_string()))
                        .await?;
                    if request.body_len != 0 {
                        return Ok(());
                    }
                    continue;
                }
                match command {
                    Command::BackupReceive { repository } => {
                        if request.body_len != 0 {
                            return Err(Error::Invalid(
                                "replication upgrade cannot carry an object body".into(),
                            ));
                        }
                        let permit = match service.transfers.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                write_frame(
                                    &mut socket,
                                    &Response::error(id, 503, "replication admission is full"),
                                )
                                .await?;
                                continue;
                            }
                        };
                        match service.open_repository(&repository, true).await {
                            Ok(repo) => {
                                let _permit = permit;
                                write_frame(
                                    &mut socket,
                                    &Response::ok(id, json!({"protocol":"LINK-replication-v1"})),
                                )
                                .await?;
                                crate::core::backup::network::receive(&mut socket, repo)
                                    .await
                                    .map_err(crate::protocol::ops::backup_error)?;
                                return Ok(());
                            }
                            Err(e) => Response::error(id, e.status(), e.to_string()),
                        }
                    }
                    command @ (Command::UploadPart { .. }
                    | Command::UploadPut { .. }
                    | Command::UploadBatch { .. }) => {
                        let expected = match service.body_length(claims, &command) {
                            Ok(length) => length,
                            Err(e) => {
                                write_frame(
                                    &mut socket,
                                    &Response::error(id, e.status(), e.to_string()),
                                )
                                .await?;
                                return Ok(());
                            }
                        };
                        if expected != request.body_len {
                            write_frame(
                                &mut socket,
                                &Response::error(id, 400, "declared body differs from part length"),
                            )
                            .await?;
                            return Ok(());
                        }
                        let (reader, mut writer) = tokio::io::duplex(256 * 1024);
                        let service = service.clone();
                        let claims = claims.clone();
                        let work = tokio::spawn(async move {
                            service.upload_body(&claims, command, reader).await
                        });
                        let copied = feed_body(&mut socket, &mut writer, request.body_len).await;
                        drop(writer);
                        let result = work.await.map_err(|_| {
                            Error::Unavailable("upload stream worker failed".into())
                        })?;
                        copied?;
                        match result {
                            Ok(value) => Response::ok(id, value),
                            Err(e) => Response::error(id, e.status(), e.to_string()),
                        }
                    }
                    Command::ObjectGet {
                        object_key,
                        offset,
                        length,
                    } => {
                        if request.body_len != 0 {
                            return Err(Error::Invalid("download cannot carry a body".into()));
                        }
                        socket = download::stream(
                            socket,
                            service.store.clone(),
                            id,
                            claims.sub.0,
                            object_key,
                            offset,
                            length,
                        )
                        .await?;
                        continue;
                    }
                    command => {
                        if request.body_len != 0 {
                            return Err(Error::Invalid(
                                "operation does not accept raw bytes".into(),
                            ));
                        }
                        match service.control(claims, command).await {
                            Ok(value) => Response::ok(id, value),
                            Err(e) => Response::error(id, e.status(), e.to_string()),
                        }
                    }
                }
            }
        };
        response.body_len = 0;
        write_frame(&mut socket, &response).await?;
    }
}

async fn feed_body<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    mut remaining: u64,
) -> Result<()> {
    let mut buffer = vec![0; 128 * 1024];
    let mut accepted = true;
    while remaining > 0 {
        let take = (remaining as usize).min(buffer.len());
        let read = tokio::time::timeout(Duration::from_secs(30), reader.read(&mut buffer[..take]))
            .await
            .map_err(|_| Error::Unavailable("native upload stalled".into()))??;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "incomplete native upload body",
            )
            .into());
        }
        if accepted && writer.write_all(&buffer[..read]).await.is_err() {
            accepted = false;
        }
        remaining -= read as u64;
    }
    Ok(())
}

async fn read_frame<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
    limit: usize,
    memory: Arc<Semaphore>,
) -> Result<Option<(T, tokio::sync::OwnedSemaphorePermit)>> {
    let mut prefix = [0; 4];
    let first = tokio::time::timeout(Duration::from_secs(120), reader.read(&mut prefix[..1]))
        .await
        .map_err(|_| Error::Unavailable("native control timed out".into()))??;
    if first == 0 {
        return Ok(None);
    }
    tokio::time::timeout(Duration::from_secs(15), reader.read_exact(&mut prefix[1..]))
        .await
        .map_err(|_| Error::Unavailable("native control prefix timed out".into()))??;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > limit {
        return Err(Error::Invalid("native control frame exceeds limit".into()));
    }
    let permit = tokio::time::timeout(
        Duration::from_secs(5),
        memory.acquire_many_owned(length.div_ceil(1024) as u32),
    )
    .await
    .map_err(|_| Error::Unavailable("native control memory admission is full".into()))?
    .map_err(|_| Error::Unavailable("engine stopping".into()))?;
    let mut bytes = vec![0; length];
    tokio::time::timeout(Duration::from_secs(30), reader.read_exact(&mut bytes))
        .await
        .map_err(|_| Error::Unavailable("native control body timed out".into()))??;
    rmp_serde::from_slice(&bytes)
        .map(|value| Some((value, permit)))
        .map_err(|e| Error::Invalid(format!("invalid NOA control: {e}")))
}
pub async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<()> {
    let bytes = rmp_serde::to_vec_named(value).map_err(|e| Error::Invalid(e.to_string()))?;
    if bytes.len() > MAX_CONTROL_BYTES {
        return Err(Error::Invalid(
            "response control frame exceeds limit".into(),
        ));
    }
    writer
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests;
