use reqwest::{Client, Response};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs, io::Write, path::Path, time::Duration};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Deserialize)]
struct Config {
    url: String,
    identity: String,
    master: String,
    fragment_secret: String,
}

pub struct Api {
    pub client: Client,
    pub base: String,
    pub token: String,
    pub operator: String,
    pub campaign: Value,
}

impl Api {
    pub async fn attach(path: &Path) -> Result<Self> {
        let bytes = fs::read(path)?;
        let config: Config = serde_json::from_slice(&bytes)?;
        let client = Client::builder()
            .timeout(Duration::from_secs(300))
            .tcp_user_timeout(Duration::from_secs(300))
            .build()?;
        let base = format!("{}/noa/v1", config.url.trim_end_matches('/'));
        let fragment = hex::encode(Sha256::digest(
            format!("N1 qualification {} api", config.identity).as_bytes(),
        ));
        let mut tokens = Vec::new();
        for secret in [&config.fragment_secret, &config.master] {
            let logged = expect(
                client
                    .post(format!("{base}/auth/fragment"))
                    .json(&json!({"fragment":fragment,"secret":secret,
                        "read":true,"write":true,"modify":true,"delete":true,
                        "expires_in":86400}))
                    .send()
                    .await?,
                200,
            )
            .await?;
            tokens.push(string(&logged, "access_token")?.to_owned());
        }
        let campaign = json!({"endpoint_sha256":hex::encode(Sha256::digest(bytes)),
            "harness_sha256":hex::encode(Sha256::digest(fs::read(std::env::current_exe()?)?))});
        Ok(Self {
            client,
            base,
            token: tokens.remove(0),
            operator: tokens.remove(0),
            campaign,
        })
    }

    pub async fn health(&self) -> Result<Value> {
        expect(
            self.client
                .get(format!(
                    "{}/maintenance/health?live_counters=true",
                    self.base
                ))
                .bearer_auth(&self.operator)
                .send()
                .await?,
            200,
        )
        .await
    }

    pub async fn submitted(&self) -> Result<u64> {
        self.health().await?["io"]["backend"]["submitted"]
            .as_u64()
            .ok_or_else(|| "health response lacks backend submission counter".into())
    }

    pub async fn observe(&self) -> Result<Value> {
        let before = self.submitted().await?;
        let mut observed = Vec::new();
        for route in [
            "maintenance/health",
            "maintenance/pools/health?deep_scan=false",
        ] {
            observed.push(
                expect(
                    self.client
                        .get(format!("{}/{route}", self.base))
                        .bearer_auth(&self.operator)
                        .send()
                        .await?,
                    200,
                )
                .await?,
            );
        }
        let after = self.submitted().await?;
        if before != after {
            return Err("ordinary monitoring submitted object payload I/O".into());
        }
        Ok(
            json!({"responses":observed,"object_io_submissions_before":before,
            "object_io_submissions_after":after}),
        )
    }
}

pub fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .ok_or_else(|| format!("missing {field}").into())
}

pub async fn expect(response: Response, expected: u16) -> Result<Value> {
    let status = response.status().as_u16();
    if status != expected {
        // Error bodies can contain private access keys. Preserve status only.
        return Err(format!("public operation returned HTTP {status}, expected {expected}").into());
    }
    Ok(response.json().await?)
}

pub fn publish(path: &Path, value: &Value) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::File::open(path.parent().ok_or("missing output parent")?)?.sync_all()?;
    Ok(())
}
