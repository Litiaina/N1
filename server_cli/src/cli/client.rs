use crate::cli::report::print_health_report;

use super::args::Commands;
use ini::Ini;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io::{self, Write};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
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

pub trait JobStatusTrait {
    fn is_running(&self) -> bool;
    fn phase(&self) -> &str;
    fn get_error(&self) -> Option<&String>;
}

#[derive(Deserialize, Debug, Clone)]
pub struct RepairJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub total_scanned: u64,
    pub total_repaired: u64,
    pub corrupt_found: u64,
    pub missing_meta_repaired: u64,
    pub divergent_meta_repaired: u64,
    pub current_fragment: String,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
    pub last_repaired: Option<String>,
    pub last_disagreement: Option<String>,
    pub last_error: Option<String>,
}

impl JobStatusTrait for RepairJobStatus {
    fn is_running(&self) -> bool {
        self.is_running
    }
    fn phase(&self) -> &str {
        &self.phase
    }
    fn get_error(&self) -> Option<&String> {
        self.last_error.as_ref()
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct RedistributionJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub total_objects_scanned: u64,
    pub total_objects_migrated: u64,
    pub total_errors: u64,
    pub current_fragment: String,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
    pub last_object: Option<String>,
    pub last_error: Option<String>,
}

impl JobStatusTrait for RedistributionJobStatus {
    fn is_running(&self) -> bool {
        self.is_running
    }
    fn phase(&self) -> &str {
        &self.phase
    }
    fn get_error(&self) -> Option<&String> {
        self.last_error.as_ref()
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct PrefixRebuildJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub total_metadata_scanned: u64,
    pub total_objects_indexed: u64,
    pub total_errors: u64,
    pub current_fragment: String,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
    pub last_error: Option<String>,
}

impl JobStatusTrait for PrefixRebuildJobStatus {
    fn is_running(&self) -> bool {
        self.is_running
    }
    fn phase(&self) -> &str {
        &self.phase
    }
    fn get_error(&self) -> Option<&String> {
        self.last_error.as_ref()
    }
}

pub static ADMIN_KEY: LazyLock<String> =
    LazyLock::new(|| env::var("ADMIN_KEY").expect("ADMIN_KEY must be set for authentication"));

const UI_WIDTH: usize = 70;
const SPINNER_FRAMES: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

fn clear_screen_once() {
    print!("\x1B[2J");
    io::stdout().flush().ok();
}

fn reset_cursor() {
    print!("\x1B[1;1H");
}

fn clear_remaining() {
    print!("\x1B[J");
}

fn hide_cursor() {
    print!("\x1B[?25l");
    io::stdout().flush().ok();
}

fn show_cursor() {
    print!("\x1B[?25h");
    io::stdout().flush().ok();
}

fn bold(s: &str) -> String {
    format!("\x1B[1m{}\x1B[0m", s)
}

fn green(s: &str) -> String {
    format!("\x1B[32m{}\x1B[0m", s)
}

fn red(s: &str) -> String {
    format!("\x1B[31m{}\x1B[0m", s)
}

fn yellow(s: &str) -> String {
    format!("\x1B[33m{}\x1B[0m", s)
}

fn cyan(s: &str) -> String {
    format!("\x1B[36m{}\x1B[0m", s)
}

fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    let mut count = 0;
    for c in s.chars().rev() {
        if count > 0 && count % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
        count += 1;
    }
    result
}

fn fmt_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() > max {
        format!("{}...", &s[..max - 3])
    } else {
        s.to_string()
    }
}

fn draw_top(title: &str, frame_idx: usize) {
    let spinner = SPINNER_FRAMES[frame_idx % SPINNER_FRAMES.len()];
    let inner_w = UI_WIDTH - 2;
    println!("╔{}╗", "═".repeat(inner_w));

    let content = format!("{}  {}", title, spinner);
    let pad = (inner_w.saturating_sub(content.chars().count())) / 2;
    let r_pad = inner_w.saturating_sub(pad + content.chars().count());

    println!(
        "║{}{}{}║",
        " ".repeat(pad),
        bold(&content),
        " ".repeat(r_pad)
    );
    println!("╠{}╣", "═".repeat(inner_w));
}

fn draw_bottom() {
    println!("╚{}╝", "═".repeat(UI_WIDTH - 2));
}

fn draw_divider(title: Option<&str>) {
    let inner_w = UI_WIDTH - 2;
    if let Some(t) = title {
        let t_len = t.len() + 2;
        let dashes = (inner_w.saturating_sub(t_len)) / 2;
        let r_dashes = inner_w.saturating_sub(dashes + t_len);
        println!(
            "╟{} {} {}╢",
            "─".repeat(dashes),
            bold(t),
            "─".repeat(r_dashes)
        );
    } else {
        println!("╟{}╢", "─".repeat(inner_w));
    }
}

fn draw_row(key: &str, value: &str) {
    let key_w = 18;
    let val_w = UI_WIDTH - 4 - key_w - 3;
    let val_trunc = truncate(value, val_w);
    println!("║ {:<w$} : {:<v$} ║", key, val_trunc, w = key_w, v = val_w);
}

fn draw_row_colored(key: &str, value: &str, color_fn: fn(&str) -> String) {
    let key_w = 18;
    let val_w = UI_WIDTH - 4 - key_w - 3;
    let val_trunc = truncate(value, val_w);
    let padding = " ".repeat(val_w.saturating_sub(val_trunc.chars().count()));
    println!(
        "║ {:<w$} : {}{} ║",
        key,
        color_fn(&val_trunc),
        padding,
        w = key_w
    );
}

fn get_base_url(url_opt: Option<String>) -> String {
    if let Some(u) = url_opt {
        return u;
    }

    let mut url = "http://127.0.0.1:50001".to_string();
    if let Ok(conf) = Ini::load_from_file("config.ini") {
        if let Some(section) = conf.section(Some("server")) {
            let host = section.get("host").unwrap_or("127.0.0.1");
            let port = section.get("port").unwrap_or("50001");
            let protocol = "https";
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
    let fragment_hash = if fragment != *ADMIN_KEY {
        let mut hasher = sha2::Sha256::new();
        use sha2::Digest;
        hasher.update(fragment.as_bytes());
        format!("{:x}", hasher.finalize())
    } else {
        ADMIN_KEY.as_str().to_string()
    };

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

struct MonitorState<T> {
    data: Option<T>,
    finished: bool,
    error_count: u32,
}

async fn run_monitor_loop<T, F>(
    client: Client,
    status_url: String,
    token: String,
    title: &str,
    render_fn: F,
) -> Result<(), Box<dyn Error>>
where
    T: DeserializeOwned + JobStatusTrait + Send + Sync + 'static + Clone,
    F: Fn(&T, usize, Duration),
{
    let state = Arc::new(Mutex::new(MonitorState {
        data: None,
        finished: false,
        error_count: 0,
    }));

    let state_clone = state.clone();
    let client_clone = client.clone();
    let url_clone = status_url.clone();
    let token_clone = token.clone();

    tokio::spawn(async move {
        loop {
            {
                let lock = state_clone.lock().unwrap();
                if lock.finished {
                    break;
                }
            }

            match client_clone
                .get(&url_clone)
                .bearer_auth(&token_clone)
                .send()
                .await
            {
                Ok(resp) => {
                    if resp.status().is_success() {
                        if let Ok(status) = resp.json::<T>().await {
                            let mut lock = state_clone.lock().unwrap();
                            lock.error_count = 0;

                            let is_running = status.is_running();
                            let phase = status.phase().to_string();
                            lock.data = Some(status);

                            if !is_running && !phase.is_empty() {
                                lock.finished = true;
                                break;
                            }
                        }
                    } else {
                        let mut lock = state_clone.lock().unwrap();
                        lock.error_count += 1;
                    }
                }
                Err(_) => {
                    let mut lock = state_clone.lock().unwrap();
                    lock.error_count += 1;
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    hide_cursor();
    clear_screen_once();
    let start_time = Instant::now();
    let mut spinner_idx = 0;

    tokio::time::sleep(Duration::from_millis(100)).await;

    loop {
        let (data, finished, err_count) = {
            let lock = state.lock().unwrap();
            (lock.data.clone(), lock.finished, lock.error_count)
        };

        reset_cursor();
        draw_top(title, spinner_idx);

        if let Some(st) = data {
            render_fn(&st, spinner_idx, start_time.elapsed());
        } else {
            draw_row("Status", "Connecting...");
            draw_row("Elapsed", &fmt_duration(start_time.elapsed().as_secs()));
            if err_count > 0 {
                draw_row_colored("Connection", &format!("Retrying ({})", err_count), yellow);
            }
        }

        draw_bottom();
        clear_remaining();
        io::stdout().flush().ok();

        if finished {
            println!("\n{}", bold("Job Completed."));
            break;
        }

        if err_count > 10 {
            println!("\n{}", red("Lost connection to server."));
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        spinner_idx = (spinner_idx + 1) % SPINNER_FRAMES.len();
    }
    show_cursor();

    Ok(())
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
                        error!("GC Start Failed: {}", resp.text().await?);
                    }
                }
                Commands::Repair { dry_run } => {
                    let resp = client
                        .post(format!("{}/v4/maintenance/repair/start", base_url))
                        .query(&[("dry_run", dry_run)])
                        .bearer_auth(&token)
                        .send()
                        .await?;

                    if !resp.status().is_success() {
                        error!("Repair Start Failed: {}", resp.text().await?);
                        return Ok(());
                    }

                    info!("Repair Job Started. Attaching monitor...");
                    let status_url = format!("{}/v4/maintenance/repair/status", base_url);

                    run_monitor_loop(
                        client,
                        status_url,
                        token,
                        "REPAIR JOB MONITOR",
                        |st: &RepairJobStatus, _, elapsed| {
                            let status_color = if st.is_running { green } else { yellow };
                            draw_row_colored(
                                "Status",
                                if st.is_running { "RUNNING" } else { "STOPPED" },
                                status_color,
                            );
                            draw_row("Phase", &st.phase);
                            draw_row("Fragment", &st.current_fragment);

                            draw_divider(Some("METRICS"));
                            draw_row("Scanned", &fmt_num(st.total_scanned));
                            draw_row("Repaired", &fmt_num(st.total_repaired));
                            draw_row_colored(
                                "Corrupt",
                                &fmt_num(st.corrupt_found),
                                if st.corrupt_found > 0 { red } else { green },
                            );
                            draw_row("Missing Meta", &fmt_num(st.missing_meta_repaired));
                            draw_row("Divergent Meta", &fmt_num(st.divergent_meta_repaired));

                            draw_divider(Some("DETAILS"));
                            if let Some(rep) = &st.last_repaired {
                                draw_row("Last Repaired", rep);
                            }
                            if let Some(dis) = &st.last_disagreement {
                                draw_row("Last Issue", dis);
                            }

                            draw_divider(Some("PERFORMANCE"));
                            draw_row("Rate", &format!("{:.2} obj/sec", st.scan_rate_per_sec));
                            draw_row("Duration", &fmt_duration(elapsed.as_secs()));

                            if let Some(err) = &st.last_error {
                                draw_divider(Some("ERROR"));
                                draw_row_colored("Message", err, red);
                            }
                        },
                    )
                    .await?;
                }
                Commands::Redistribute { dry_run } => {
                    let resp = client
                        .post(format!("{}/v4/maintenance/redistribution/start", base_url))
                        .query(&[("dry_run", dry_run)])
                        .bearer_auth(&token)
                        .send()
                        .await?;

                    if !resp.status().is_success() {
                        error!("Redistribution Start Failed: {}", resp.text().await?);
                        return Ok(());
                    }

                    info!("Redistribution Job Started. Attaching monitor...");
                    let status_url = format!("{}/v4/maintenance/redistribution/monitor", base_url);

                    run_monitor_loop(
                        client,
                        status_url,
                        token,
                        "REDISTRIBUTION MONITOR",
                        |st: &RedistributionJobStatus, _, elapsed| {
                            let status_color = if st.is_running { green } else { yellow };
                            draw_row_colored(
                                "Status",
                                if st.is_running { "RUNNING" } else { "STOPPED" },
                                status_color,
                            );
                            draw_row("Phase", &st.phase);
                            draw_row("Fragment", &st.current_fragment);

                            draw_divider(Some("METRICS"));
                            draw_row("Scanned", &fmt_num(st.total_objects_scanned));
                            draw_row_colored("Migrated", &fmt_num(st.total_objects_migrated), cyan);
                            draw_row_colored(
                                "Errors",
                                &fmt_num(st.total_errors),
                                if st.total_errors > 0 { red } else { green },
                            );

                            draw_divider(Some("DETAILS"));
                            if let Some(obj) = &st.last_object {
                                draw_row("Current/Last", obj);
                            }

                            draw_divider(Some("PERFORMANCE"));
                            draw_row("Rate", &format!("{:.2} obj/sec", st.scan_rate_per_sec));
                            draw_row("Duration", &fmt_duration(elapsed.as_secs()));

                            if let Some(err) = &st.last_error {
                                draw_divider(Some("ERROR"));
                                draw_row_colored("Message", err, red);
                            }
                        },
                    )
                    .await?;
                }
                Commands::PrefixRebuild => {
                    let resp = client
                        .post(format!("{}/v4/maintenance/prefix/rebuild/start", base_url))
                        .bearer_auth(&token)
                        .send()
                        .await?;

                    if !resp.status().is_success() {
                        error!("Prefix Rebuild Start Failed: {}", resp.text().await?);
                        return Ok(());
                    }

                    info!("Prefix Rebuild Started. Attaching monitor...");
                    let status_url = format!("{}/v4/maintenance/prefix/rebuild/status", base_url);

                    run_monitor_loop(
                        client,
                        status_url,
                        token,
                        "PREFIX REBUILD MONITOR",
                        |st: &PrefixRebuildJobStatus, _, elapsed| {
                            let status_color = if st.is_running { green } else { yellow };
                            draw_row_colored(
                                "Status",
                                if st.is_running { "RUNNING" } else { "STOPPED" },
                                status_color,
                            );
                            draw_row("Phase", &st.phase);
                            draw_row("Fragment", &st.current_fragment);

                            draw_divider(Some("METRICS"));
                            draw_row("Meta Scanned", &fmt_num(st.total_metadata_scanned));
                            draw_row_colored("Indexed", &fmt_num(st.total_objects_indexed), cyan);
                            draw_row_colored(
                                "Errors",
                                &fmt_num(st.total_errors),
                                if st.total_errors > 0 { red } else { green },
                            );

                            draw_divider(Some("PERFORMANCE"));
                            draw_row("Rate", &format!("{:.2} obj/sec", st.scan_rate_per_sec));
                            draw_row("Duration", &fmt_duration(elapsed.as_secs()));

                            if let Some(err) = &st.last_error {
                                draw_divider(Some("ERROR"));
                                draw_row_colored("Message", err, red);
                            }
                        },
                    )
                    .await?;
                }
                Commands::Health => {
                    let resp = client
                        .get(format!("{}/v4/maintenance/health?deep_scan=true", base_url))
                        .bearer_auth(&token)
                        .send()
                        .await?;

                    if resp.status().is_success() {
                        let health: HealthResponse = resp.json().await?;
                        print_health_report(&health);
                    } else {
                        error!("Health check failed: {}", resp.text().await?);
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}
