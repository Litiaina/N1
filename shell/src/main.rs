//! Native, explicit-key operations for the LINK object engine.
use clap::Parser;
mod args;
mod offline;
#[cfg(target_os = "linux")]
mod operator;
use args::{Action, BackupAction, Cli};
use cli_core::{Credentials, Result, aligned_part, result, stripe_bytes};
use n1_link::noa::{Command, ScrubOptions};
use std::path::PathBuf;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

impl BackupAction {
    fn command(self) -> Result<Command> {
        Ok(match self {
            Self::Serve { .. } | Self::OperatorKeygen { .. } => {
                return Err("operator commands run on the repository host".into());
            }
            Self::Offline(_) => {
                return Err("offline backup must run through the offline dispatcher".into());
            }
            Self::Capture {
                repository,
                boundary_seconds,
            } => Command::BackupCapture {
                repository,
                boundary_seconds,
            },
            Self::List {
                repository,
                after,
                limit,
            } => Command::BackupList {
                repository,
                after,
                limit,
            },
            Self::Verify {
                repository,
                snapshot,
            } => Command::BackupVerify {
                repository,
                snapshot,
            },
            Self::Replicate {
                repository,
                destination,
                snapshot,
            } => Command::BackupReplicate {
                repository,
                destination,
                snapshot,
            },
            Self::Prune {
                repository,
                keep_last,
                protect,
            } => Command::BackupPrune {
                repository,
                keep_last,
                protect,
            },
            Self::Gc {
                repository,
                discard_incomplete,
            } => Command::BackupGc {
                repository,
                discard_incomplete,
            },
            Self::Job => Command::BackupJob,
            Self::Stop { job_id } => Command::BackupStop { job_id },
        })
    }
}
#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("N1 client: {error}");
        std::process::exit(1);
    }
}
async fn run(cli: Cli) -> Result<()> {
    match &cli.command {
        Action::Backup {
            command: BackupAction::Serve { config },
        } => {
            #[cfg(target_os = "linux")]
            return operator::serve(config).await;
            #[cfg(not(target_os = "linux"))]
            {
                let _ = config;
                return Err("the repository operator host requires Linux".into());
            }
        }
        Action::Backup {
            command: BackupAction::OperatorKeygen { token_file },
        } => {
            #[cfg(target_os = "linux")]
            return operator::config::generate_token(token_file);
            #[cfg(not(target_os = "linux"))]
            {
                let _ = token_file;
                return Err("the repository operator host requires Linux".into());
            }
        }
        _ => (),
    }
    let offline = match &cli.command {
        Action::Pool(args) => Some(("pool", args.clone())),
        Action::Backup {
            command: BackupAction::Offline(args),
        } => Some(("backup", args.clone())),
        _ => None,
    };
    if let Some((operation, args)) = offline {
        return tokio::task::spawn_blocking(move || offline::run(operation, &args)).await?;
    }
    if matches!(&cli.command, Action::Ping) && cli.credentials.is_none() {
        let mut client = native_client(
            cli.endpoint.as_deref().ok_or("--endpoint is required")?,
            cli.ca.as_deref(),
        )
        .await?;
        print_result(client.control(Command::Ping).await?)?;
        return Ok(());
    }
    let credentials = Credentials::load(
        cli.credentials
            .as_deref()
            .ok_or("--credentials is required")?,
    )?;
    let mut client = credentials
        .connect(cli.endpoint.as_deref(), cli.ca.as_deref())
        .await?;
    let command = match cli.command {
        Action::Pool(_) => {
            return Err("offline pool maintenance must run through the offline dispatcher".into());
        }
        Action::Put {
            file,
            pool,
            part_bytes,
            durable_parts,
        } => {
            let mut source = input(file).await?;
            let total = source.metadata().await?.len();
            let stripe = stripe_bytes(&mut client, pool).await?;
            let part = aligned_part(part_bytes.unwrap_or(total), stripe)?;
            let upload = result(
                client
                    .control(Command::UploadInit {
                        pool,
                        total_bytes: total,
                        part_bytes: part,
                        durable: durable_parts,
                        ttl_seconds: 86400,
                    })
                    .await?,
            )?;
            let ticket = upload["upload_key"]
                .as_str()
                .ok_or("missing upload ticket")?
                .to_owned();
            if part_bytes.is_none() {
                print_result(client.put(ticket, total, &mut source).await?)?;
                return Ok(());
            }
            // Emit the resumable ticket before streaming parts. The caller can
            // retain this output if interruption prevents the final response.
            println!("{}", serde_json::to_string(&upload)?);
            for index in 0..total.div_ceil(part) {
                let length = (total - index * part).min(part);
                let mut body = (&mut source).take(length);
                client
                    .upload_part(ticket.clone(), index, length, &mut body)
                    .await?;
            }
            Command::UploadFinalize { upload_key: ticket }
        }
        Action::Batch { files, pool } => {
            if files.is_empty() || files.len() > 256 {
                return Err("batch requires 1..=256 files".into());
            }
            let stripe = stripe_bytes(&mut client, pool).await?;
            let mut tickets = Vec::new();
            let mut body: Box<dyn AsyncRead + Unpin + Send> = Box::new(tokio::io::empty());
            let mut total = 0u64;
            for path in files {
                let file = input(path).await?;
                let bytes = file.metadata().await?.len();
                total = total.checked_add(bytes).ok_or("batch length overflow")?;
                if total > n1_link::noa::MAX_BODY_BYTES {
                    return Err("batch body exceeds 1 GiB".into());
                }
                let init = result(
                    client
                        .control(Command::UploadInit {
                            pool,
                            total_bytes: bytes,
                            part_bytes: aligned_part(bytes, stripe)?,
                            durable: false,
                            ttl_seconds: 86400,
                        })
                        .await?,
                )?;
                tickets.push(
                    init["upload_key"]
                        .as_str()
                        .ok_or("missing upload ticket")?
                        .to_owned(),
                );
                body = Box::new(body.chain(file));
            }
            print_result(client.put_batch(tickets, total, &mut body).await?)?;
            return Ok(());
        }
        Action::Init {
            bytes,
            part_bytes,
            pool,
            durable,
            ttl_seconds,
        } => Command::UploadInit {
            pool,
            total_bytes: bytes,
            part_bytes,
            durable,
            ttl_seconds,
        },
        Action::Part {
            upload_key,
            part,
            file,
        } => {
            let mut file = input(file).await?;
            let length = file.metadata().await?.len();
            print_result(
                client
                    .upload_part(upload_key, part, length, &mut file)
                    .await?,
            )?;
            return Ok(());
        }
        Action::Status {
            upload_key,
            start,
            limit,
        } => Command::UploadStatus {
            upload_key,
            start,
            limit,
        },
        Action::Finalize { upload_key } => Command::UploadFinalize { upload_key },
        Action::Cancel { upload_key } => Command::UploadCancel { upload_key },
        Action::Get {
            key,
            output,
            offset,
            length,
        } => {
            // The output is explicit and never silently replaced. Failure keeps
            // the partial bytes for inspection and reports a nonzero exit code.
            let mut options = tokio::fs::OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            options.mode(0o600);
            let mut file = options.open(&output).await?;
            let (response, _) = client.download(key, offset, length, &mut file).await?;
            file.flush().await?;
            file.sync_all().await?;
            print_result(response)?;
            return Ok(());
        }
        Action::Stat { key } => Command::ObjectStat { object_key: key },
        Action::Health {
            key: Some(key),
            deep,
        } => Command::ObjectHealth {
            object_key: key,
            deep,
        },
        Action::Health { key: None, .. } => Command::Health,
        Action::Repair { key } => Command::ObjectRepair { object_key: key },
        Action::Delete { mut key } => {
            if key.len() == 1 {
                Command::ObjectDelete {
                    object_key: key.pop().expect("one key"),
                }
            } else {
                Command::BulkDelete { object_keys: key }
            }
        }
        Action::Pools => Command::Pools,
        Action::Gc => Command::GarbageCollect,
        Action::Scrub {
            repair,
            bytes_per_second,
            window_seconds,
        } => Command::Scrub {
            options: ScrubOptions {
                repair,
                bytes_per_second,
                window_seconds,
            },
        },
        Action::Job => Command::MaintenanceJob,
        Action::Stop { job_id } => Command::MaintenanceStop { job_id },
        Action::Backup { command } => command.command()?,
        Action::Ping => Command::Ping,
    };
    print_result(client.control(command).await?)
}
async fn native_client(
    endpoint: &str,
    ca: Option<&std::path::Path>,
) -> Result<n1_native_client::N1NativeClient> {
    Ok(n1_native_client::N1NativeClient::connect_with_tls(
        endpoint,
        ca.map(cli_core::client_tls).transpose()?,
    )
    .await?)
}
fn print_result(response: n1_link::noa::Response) -> Result<()> {
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}

async fn input(path: PathBuf) -> Result<tokio::fs::File> {
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NONBLOCK);
    let file = options.open(path).await?;
    if !file.metadata().await?.is_file() {
        return Err("upload input must be a regular file".into());
    }
    Ok(file)
}
