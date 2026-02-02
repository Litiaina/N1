use crate::cli::report::print_health_report;

use super::args::Commands;
use ini::Ini;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::io::{self, Write};
use std::time::Duration;
use tracing::{error, info};

#[derive(Serialize)]
pub struct AuthRequest {
    pub fragment: String,
    pub secret: String,
    pub read: bool,
    pub write: bool,
    pub modify: bool,
    pub delete: bool,
}

#[derive(Deserialize)]
pub struct AuthResponse {
    pub access_token: String,
}

#[derive(Serialize)]
pub struct CreateFragmentRequest {
    pub name: String,
    pub quota: u64,
    pub secret: String,
}

#[derive(Deserialize, Debug)]
pub struct DiskStatus {
    pub path: String,
    pub role: String,
    pub status: String,
    pub latency_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ConsistencyStats {
    pub total_objects_scanned: u64,
    pub index_mirror_health: String,
    pub mirror_counts: Option<HashMap<String, u64>>,
}

#[derive(Deserialize, Debug)]
pub struct SystemMetrics {
    pub cpu_usage_percent: f64,
    pub ram_total_bytes: u64,
    pub ram_used_bytes: u64,
    pub ram_usage_percent: f64,
    pub load_avg_1m: f64,
}

#[derive(Deserialize, Debug)]
pub struct HealthResponse {
    pub status: String,
    #[serde(default)]
    pub database_connected: bool,
    pub disks: Vec<DiskStatus>,
    pub consistency: ConsistencyStats,
    pub system: SystemMetrics,
    pub summary: String,
}

fn get_base_url(url_opt: Option<String>) -> String {
    if let Some(u) = url_opt {
        return u;
    }

    let mut url = "http://127.0.0.1:3000".to_string();
    if let Ok(conf) = Ini::load_from_file("config.ini") {
        if let Some(section) = conf.section(Some("server")) {
            let host = section.get("host").unwrap_or("127.0.0.1");
            let port = section.get("port").unwrap_or("3000");
            let protocol = if port == "50001" { "https" } else { "http" };
            let effective_host = if host == "0.0.0.0" { "127.0.0.1" } else { host };
            url = format!("{}://{}:{}", protocol, effective_host, port);
        }
    }
    url
}

fn get_secret(secret_opt: Option<String>) -> Result<String, Box<dyn Error>> {
    if let Some(s) = secret_opt {
        return Ok(s);
    }
    if let Ok(s) = std::env::var("N1_MASTER_KEY") {
        return Ok(s);
    }

    print!("Enter Master Secret: ");
    io::stdout().flush()?;
    Ok(rpassword::read_password()?)
}

async fn authenticate(
    client: &Client,
    base_url: &str,
    secret: &str,
    fragment: &str,
) -> Result<String, Box<dyn Error>> {
    let mut hasher = sha2::Sha256::new();
    use sha2::Digest;
    hasher.update(fragment.as_bytes());
    let fragment_hash = format!("{:x}", hasher.finalize());

    let resp = client
        .post(format!("{}/authenticate/fragment", base_url))
        .json(&AuthRequest {
            fragment: fragment_hash,
            secret: secret.to_string(),
            read: true,
            write: true,
            modify: true,
            delete: true,
        })
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("Authentication failed [{}]: {}", status, text).into());
    }

    let auth: AuthResponse = resp.json().await?;
    Ok(auth.access_token)
}

pub async fn execute_command(
    cmd: &Commands,
    secret_opt: Option<String>,
    fragment_context: String,
    url_opt: Option<String>,
    insecure: bool,
) -> Result<(), Box<dyn Error>> {
    let base_url = get_base_url(url_opt);
    let secret = get_secret(secret_opt)?;

    let client = Client::builder()
        .danger_accept_invalid_certs(insecure)
        .timeout(Duration::from_secs(30))
        .build()?;

    match cmd {
        Commands::Server => {}
        Commands::CreateFragment { name, quota } => {
            info!("Creating fragment '{}' on {}...", name, base_url);
            let resp = client
                .post(format!("{}/v4/fragment/create", base_url))
                .json(&CreateFragmentRequest {
                    name: name.clone(),
                    quota: *quota,
                    secret: secret.clone(),
                })
                .send()
                .await?;

            if resp.status().is_success() {
                info!("Fragment created successfully.");
                let body = resp.text().await?;
                info!("Response: {}", body);
            } else {
                error!(
                    "Failed to create fragment [{}]: {}",
                    resp.status(),
                    resp.text().await?
                );
            }
        }
        _ => {
            info!("Authenticating as context '{}'...", fragment_context);
            let token = authenticate(&client, &base_url, &secret, &fragment_context).await?;

            match cmd {
                Commands::Gc => {
                    info!("Triggering Garbage Collector...");
                    let resp = client
                        .post(format!("{}/v4/maintenance/gc/start", base_url))
                        .bearer_auth(&token)
                        .send()
                        .await?;

                    if resp.status().is_success() {
                        info!("GC Started.");
                    } else {
                        error!("GC Start Failed: {}", resp.status());
                    }
                }
                Commands::Repair { dry_run } => {
                    info!("Triggering Repair (Dry Run: {})...", dry_run);
                    let resp = client
                        .post(format!("{}/v4/maintenance/repair/start", base_url))
                        .query(&[("dry_run", dry_run)])
                        .bearer_auth(&token)
                        .send()
                        .await?;

                    if resp.status().is_success() {
                        info!("Repair Job Started. Check server logs/status.");
                    } else {
                        error!("Repair Start Failed: {}", resp.text().await?);
                    }
                }
                Commands::Redistribute { dry_run } => {
                    info!("Triggering Redistribution (Dry Run: {})...", dry_run);
                    let resp = client
                        .post(format!("{}/v4/maintenance/redistribution/start", base_url))
                        .query(&[("dry_run", dry_run)])
                        .bearer_auth(&token)
                        .send()
                        .await?;

                    if resp.status().is_success() {
                        info!("Redistribution Job Started. Check server logs/status.");
                    } else {
                        error!("Redistribution Start Failed: {}", resp.text().await?);
                    }
                }
                Commands::Health => {
                    let resp = client
                        .get(format!("{}/v4/maintenance/health", base_url))
                        .bearer_auth(&token)
                        .send()
                        .await?;

                    if resp.status().is_success() {
                        let health: HealthResponse = resp.json().await?;
                        print_health_report(&health);
                    } else {
                        error!("Health check failed: {}", resp.status());
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}
