//! Adapter for the retained durable backup supervisor to native NOA jobs.
use crate::core::backup::{Result, invalid};
use n1_link::noa::{Command, Request as NoaRequest, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{path::PathBuf, time::Duration};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Destination {
    pub url: String,
    pub repository: String,
    pub token_file: PathBuf,
    pub ca_file: Option<PathBuf>,
    #[serde(default)]
    pub allow_plaintext: bool,
}
impl Destination {
    pub fn validate(&self) -> Result<()> {
        let url = reqwest::Url::parse(&self.url)?;
        if !matches!(url.scheme(), "n1" | "n1s" | "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !matches!(url.path(), "" | "/")
            || (!self.allow_plaintext && matches!(url.scheme(), "n1" | "http"))
            || !super::valid_alias(&self.repository)
            || !self.token_file.is_absolute()
            || self.ca_file.as_ref().is_some_and(|p| !p.is_absolute())
        {
            return Err(invalid(
                "invalid recovery control destination; plaintext requires explicit opt-in",
            ));
        }
        Ok(())
    }
}
#[derive(Default)]
pub struct Request {
    pub operation: String,
    pub repository: String,
    pub snapshot: String,
    pub keep_last: usize,
    pub confirm: bool,
}
enum Client {
    Native(n1_native_client::N1NativeClient),
    Http {
        client: reqwest::Client,
        url: String,
        token: String,
        id: u64,
    },
}
impl Client {
    async fn connect(destination: &Destination) -> Result<Self> {
        let credentials = cli_core::Credentials::load(&destination.token_file)?;
        if destination.url.starts_with("n1") {
            return Ok(Self::Native(
                credentials
                    .connect(Some(&destination.url), destination.ca_file.as_deref())
                    .await?,
            ));
        }
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(30));
        if let Some(ca) = &destination.ca_file {
            builder =
                builder.add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(ca)?)?);
        }
        let client = builder.build()?;
        let token = match credentials.token {
            Some(token) => token,
            None => {
                let command = Command::Login {
                    fragment: credentials
                        .fragment
                        .ok_or_else(|| invalid("operator fragment missing"))?,
                    secret: credentials
                        .secret
                        .ok_or_else(|| invalid("operator secret missing"))?,
                    permissions: None,
                    ttl_seconds: Some(86400),
                };
                let response: Response = client
                    .post(format!("{}/noa", destination.url.trim_end_matches('/')))
                    .json(&NoaRequest {
                        id: 1,
                        command,
                        body_len: 0,
                    })
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                response
                    .result
                    .and_then(|v| v["token"].as_str().map(str::to_owned))
                    .ok_or_else(|| invalid("operator login failed"))?
            }
        };
        Ok(Self::Http {
            client,
            url: destination.url.clone(),
            token,
            id: 1,
        })
    }
    async fn call(&mut self, command: Command) -> Result<Value> {
        let response = match self {
            Self::Native(client) => client.control(command).await?,
            Self::Http {
                client,
                url,
                token,
                id,
            } => {
                *id += 1;
                client
                    .post(format!("{}/noa", url.trim_end_matches('/')))
                    .bearer_auth(token)
                    .json(&NoaRequest {
                        id: *id,
                        command,
                        body_len: 0,
                    })
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Response>()
                    .await?
            }
        };
        if response.status != 200 {
            return Err(invalid(format!(
                "recovery control rejected operation: {}",
                response.status
            )));
        }
        response
            .result
            .ok_or_else(|| invalid("recovery control returned no result"))
    }
}
pub async fn completed_operation(
    destination: &Destination,
    request: Request,
    seconds: u64,
) -> Result<Value> {
    destination.validate()?;
    if !request.confirm || request.repository != destination.repository {
        return Err(invalid(
            "recovery operation requires matching configured destination",
        ));
    }
    let command = match request.operation.as_str() {
        "create-online" => Command::BackupCapture {
            repository: request.repository,
            boundary_seconds: 30,
        },
        "prune" => Command::BackupPrune {
            repository: request.repository,
            keep_last: request.keep_last,
            protect: request.snapshot,
        },
        _ => return Err(invalid("unsupported supervised recovery operation")),
    };
    let mut client = Client::connect(destination).await?;
    let mut owned_job: Option<String> = None;
    let result = tokio::time::timeout(Duration::from_secs(seconds), async {
        let started = client.call(command).await?;
        let id = started["id"]
            .as_str()
            .ok_or_else(|| invalid("recovery operation returned no job ID"))?
            .to_owned();
        owned_job = Some(id.clone());
        loop {
            let value = client.call(Command::BackupJob).await?;
            if value["id"] != id {
                return Err(invalid("supervised recovery job was replaced"));
            }
            match value["phase"].as_str() {
                Some("completed") => {
                    return value
                        .get("result")
                        .filter(|v| !v.is_null())
                        .cloned()
                        .ok_or_else(|| invalid("completed recovery job has no result"));
                }
                Some("running" | "stopping") => {
                    tokio::time::sleep(Duration::from_millis(250)).await
                }
                _ => {
                    return Err(invalid(format!(
                        "recovery job incomplete: {}",
                        value["error"]
                    )));
                }
            }
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            if let Some(id) = owned_job {
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    client.call(Command::BackupStop { job_id: id }),
                )
                .await;
            }
            Err(invalid(
                "supervised recovery operation exceeded its time budget",
            ))
        }
    }
}
pub fn run_async<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let result = runtime.block_on(future);
    runtime.shutdown_timeout(Duration::from_secs(5));
    result
}
