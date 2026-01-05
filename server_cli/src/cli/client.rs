use super::args::Commands;
use ini::Ini;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::io::{self, Write};
use std::time::Duration;
use tracing::{error, info};

#[derive(Serialize)]
struct LoginPayload {
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct AuthResponse {
    access_token: String,
}

pub async fn execute_command(
    cmd: &Commands,
    email_opt: Option<String>,
    pass_opt: Option<String>,
    url_opt: Option<String>,
    insecure: bool,
) -> Result<(), Box<dyn Error>> {
    let base_url = if let Some(u) = url_opt {
        u
    } else {
        let mut url = "https://127.0.0.1:50001".to_string();
        match Ini::load_from_file("config.ini") {
            Ok(conf) => {
                if let Some(section) = conf.section(Some("server")) {
                    let host = section.get("host").unwrap_or("127.0.0.1");
                    let port = section.get("https_port").unwrap_or("50001");
                    let effective_host = if host == "0.0.0.0" { "127.0.0.1" } else { host };
                    url = format!("https://{}:{}", effective_host, port);
                }
            }
            Err(_) => {}
        }
        url
    };

    let client = Client::builder()
        .danger_accept_invalid_certs(insecure)
        .timeout(Duration::from_secs(10))
        .build()?;

    let email = match email_opt.or_else(|| std::env::var("ADMIN_EMAIL").ok()) {
        Some(e) => e,
        None => {
            print!("Email: ");
            io::stdout().flush()?;
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            input.trim().to_string()
        }
    };

    let password = match pass_opt.or_else(|| std::env::var("ADMIN_PASSWORD").ok()) {
        Some(p) => p,
        None => {
            print!("Password: ");
            io::stdout().flush()?;
            rpassword::read_password()?
        }
    };

    info!("Authenticating please wait...");
    let auth_url = format!("{}/authenticate", base_url);
    let login_res = client
        .post(&auth_url)
        .json(&LoginPayload {
            email: email,
            password,
        })
        .send()
        .await
        .map_err(|e| format!("Failed to connect to server at {}: {}", base_url, e))?;

    if !login_res.status().is_success() {
        let status = login_res.status();
        let text = login_res.text().await.unwrap_or_default();
        error!("Authentication failed [{}]: {}", status, text);
        return Err("Authentication failed".into());
    }

    let auth_data: AuthResponse = login_res.json().await.map_err(|_| "Invalid response format from server")?;
    let token = auth_data.access_token;
    info!("Authentication successful.");

    match cmd {
        Commands::Gc => {
            info!("Starting Garbage Collector...");
            let url = format!("{}/v4/storage/gc/run", base_url);
            let res = client.post(&url)
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|e| format!("Failed to execute GC command: {}", e))?;

            if res.status().is_success() {
                info!("Garbage Collector triggered successfully.");
            } else {
                let status = res.status();
                let text = res.text().await.unwrap_or_default();
                error!("Failed to trigger GC [{}]: {}", status, text);
            }
        }
        Commands::Sync => {
            info!("Triggering Quota Sync...");
            let url = format!("{}/v4/storage/sync", base_url);
            let res = client.post(&url)
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|e| format!("Failed to execute Sync command: {}", e))?;

            if res.status().is_success() {
                info!("Quota Sync triggered successfully.");
            } else {
                let status = res.status();
                let text = res.text().await.unwrap_or_default();
                error!("Failed to trigger Sync [{}]: {}", status, text);
            }
        }
        Commands::Server => {}
    }

    Ok(())
}
