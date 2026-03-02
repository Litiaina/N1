use crate::cli::report::print_health_report;

use super::args::{Commands, JobAction, OpsCommands};
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    pub start_timestamp: i64,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RedistributionJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub total_fragments_scanned: u64,
    pub total_objects_scanned: u64,
    pub total_objects_need_redistribution: u64,
    pub total_objects_migrated: u64,
    pub total_errors: u64,
    pub current_fragment: String,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
    pub last_object: Option<String>,
    pub last_error: Option<String>,
    pub start_timestamp: i64,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrefixRebuildJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub total_fragments_scanned: u64,
    pub total_metadata_scanned: u64,
    pub total_objects_indexed: u64,
    pub total_prefixes_written: u64,
    pub total_entries_written: u64,
    pub total_errors: u64,
    pub current_fragment: String,
    pub duration_seconds: u64,
    pub scan_rate_per_sec: f64,
    pub last_error: Option<String>,
    pub start_timestamp: i64,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GcJobStatus {
    pub is_running: bool,
    pub phase: String,
    pub is_manual_run: bool,
    pub total_fragments: u64,
    pub total_fragments_scanned: u64,
    pub total_live_versions_seen: u64,
    pub total_trash_versions_expired: u64,
    pub total_sessions_deleted: u64,
    pub total_orphan_blob_dirs_deleted: u64,
    pub total_refund_bytes: u64,
    pub total_errors: u64,
    pub current_fragment: String,
    pub last_error: Option<String>,
    pub duration_seconds: u64,
    pub start_timestamp: i64,
}

impl JobStatusTrait for GcJobStatus {
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

    let mut url = "https://127.0.0.1:50001".to_string();
    let conf = Ini::load_from_file("n1_config.ini").or_else(|_| Ini::load_from_file("config.ini"));
    if let Ok(conf) = conf {
        if let Some(section) = conf.section(Some("server")) {
            let host = section.get("host").unwrap_or("127.0.0.1");
            let port = section
                .get("https_port")
                .or_else(|| section.get("port"))
                .unwrap_or("50001");
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

async fn fetch_status<T: DeserializeOwned>(
    client: &Client,
    status_url: &str,
    token: &str,
) -> Result<T, Box<dyn Error>> {
    let resp = client.get(status_url).bearer_auth(token).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("status endpoint failed [{}]: {}", status, body).into());
    }
    Ok(resp.json::<T>().await?)
}

fn print_status<T: Serialize>(
    label: &str,
    status: &T,
    json_out: bool,
) -> Result<(), Box<dyn Error>> {
    if json_out {
        println!("{}", serde_json::to_string_pretty(status)?);
    } else {
        println!("{}:", label);
        println!("{}", serde_json::to_string_pretty(status)?);
    }
    Ok(())
}

fn fmt_pct(n: u64, d: u64) -> String {
    if d == 0 {
        "n/a".to_string()
    } else {
        format!("{:.2}%", (n as f64 / d as f64) * 100.0)
    }
}

fn fmt_eta_seconds(remaining: u64, rate_per_sec: f64) -> String {
    if rate_per_sec <= 0.0 {
        "n/a".to_string()
    } else {
        let secs = (remaining as f64 / rate_per_sec).ceil().max(0.0) as u64;
        fmt_duration(secs)
    }
}

fn fmt_unix_ts(ts: i64) -> String {
    if ts <= 0 {
        return "n/a".to_string();
    }
    match chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0) {
        Some(dt) => dt.to_rfc3339(),
        None => "n/a".to_string(),
    }
}

fn print_gc_status_pretty(st: &GcJobStatus) {
    let elapsed = st.duration_seconds;
    let progress = fmt_pct(st.total_fragments_scanned, st.total_fragments);
    let remaining = st
        .total_fragments
        .saturating_sub(st.total_fragments_scanned);
    let frag_rate = if elapsed > 0 {
        st.total_fragments_scanned as f64 / elapsed as f64
    } else {
        0.0
    };

    draw_top("GC STATUS", 0);
    draw_row_colored(
        "Status",
        if st.is_running { "RUNNING" } else { "STOPPED" },
        if st.is_running { green } else { yellow },
    );
    draw_row("Phase", &st.phase);
    draw_row(
        "Mode",
        if st.is_manual_run {
            "MANUAL"
        } else {
            "SCHEDULED"
        },
    );
    draw_row("Fragment", &st.current_fragment);
    draw_row("Started At", &fmt_unix_ts(st.start_timestamp));
    draw_divider(Some("PROGRESS"));
    draw_row(
        "Fragments",
        &format!(
            "{} / {} ({})",
            fmt_num(st.total_fragments_scanned),
            fmt_num(st.total_fragments),
            progress
        ),
    );
    draw_row("Frag Rate", &format!("{:.2} frag/sec", frag_rate));
    draw_row("ETA", &fmt_eta_seconds(remaining, frag_rate));
    draw_divider(Some("METRICS"));
    draw_row("Live Versions", &fmt_num(st.total_live_versions_seen));
    draw_row("Trash Expired", &fmt_num(st.total_trash_versions_expired));
    draw_row("Sessions Deleted", &fmt_num(st.total_sessions_deleted));
    draw_row(
        "Orphan Blob Dirs",
        &fmt_num(st.total_orphan_blob_dirs_deleted),
    );
    draw_row_colored("Refunded Bytes", &fmt_num(st.total_refund_bytes), cyan);
    draw_row_colored(
        "Errors",
        &fmt_num(st.total_errors),
        if st.total_errors > 0 { red } else { green },
    );
    draw_divider(Some("TIME"));
    draw_row("Duration", &fmt_duration(elapsed));
    if let Some(err) = &st.last_error {
        draw_divider(Some("ERROR"));
        draw_row_colored("Message", err, red);
    }
    draw_bottom();
}

fn print_repair_status_pretty(st: &RepairJobStatus) {
    draw_top("REPAIR STATUS", 0);
    draw_row_colored(
        "Status",
        if st.is_running { "RUNNING" } else { "STOPPED" },
        if st.is_running { green } else { yellow },
    );
    draw_row("Phase", &st.phase);
    draw_row("Fragment", &st.current_fragment);
    draw_row("Started At", &fmt_unix_ts(st.start_timestamp));
    draw_divider(Some("METRICS"));
    draw_row("Scanned", &fmt_num(st.total_scanned));
    draw_row("Repaired", &fmt_num(st.total_repaired));
    draw_row_colored(
        "Corrupt Found",
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
    draw_row("Scan Rate", &format!("{:.2} obj/sec", st.scan_rate_per_sec));
    draw_row("Duration", &fmt_duration(st.duration_seconds));
    if let Some(err) = &st.last_error {
        draw_divider(Some("ERROR"));
        draw_row_colored("Message", err, red);
    }
    draw_bottom();
}

fn print_redistribution_status_pretty(st: &RedistributionJobStatus) {
    let progress = fmt_pct(
        st.total_objects_migrated,
        st.total_objects_need_redistribution,
    );
    let migrate_rate = if st.duration_seconds > 0 {
        st.total_objects_migrated as f64 / st.duration_seconds as f64
    } else {
        0.0
    };
    let remaining = st
        .total_objects_need_redistribution
        .saturating_sub(st.total_objects_migrated);

    draw_top("REDISTRIBUTION STATUS", 0);
    draw_row_colored(
        "Status",
        if st.is_running { "RUNNING" } else { "STOPPED" },
        if st.is_running { green } else { yellow },
    );
    draw_row("Phase", &st.phase);
    draw_row("Fragment", &st.current_fragment);
    draw_row("Started At", &fmt_unix_ts(st.start_timestamp));
    draw_divider(Some("PROGRESS"));
    draw_row("Fragments", &fmt_num(st.total_fragments_scanned));
    draw_row("Scanned", &fmt_num(st.total_objects_scanned));
    draw_row(
        "Migrated",
        &format!(
            "{} / {} ({})",
            fmt_num(st.total_objects_migrated),
            fmt_num(st.total_objects_need_redistribution),
            progress
        ),
    );
    draw_row("Migrate Rate", &format!("{:.2} obj/sec", migrate_rate));
    draw_row("ETA", &fmt_eta_seconds(remaining, migrate_rate));
    draw_divider(Some("METRICS"));
    draw_row("Scan Rate", &format!("{:.2} obj/sec", st.scan_rate_per_sec));
    draw_row_colored(
        "Errors",
        &fmt_num(st.total_errors),
        if st.total_errors > 0 { red } else { green },
    );
    if let Some(obj) = &st.last_object {
        draw_row("Last Object", obj);
    }
    draw_divider(Some("TIME"));
    draw_row("Duration", &fmt_duration(st.duration_seconds));
    if let Some(err) = &st.last_error {
        draw_divider(Some("ERROR"));
        draw_row_colored("Message", err, red);
    }
    draw_bottom();
}

fn print_prefix_rebuild_status_pretty(st: &PrefixRebuildJobStatus) {
    let idx_ratio = fmt_pct(st.total_objects_indexed, st.total_metadata_scanned);
    draw_top("PREFIX REBUILD STATUS", 0);
    draw_row_colored(
        "Status",
        if st.is_running { "RUNNING" } else { "STOPPED" },
        if st.is_running { green } else { yellow },
    );
    draw_row("Phase", &st.phase);
    draw_row("Fragment", &st.current_fragment);
    draw_row("Started At", &fmt_unix_ts(st.start_timestamp));
    draw_divider(Some("PROGRESS"));
    draw_row("Fragments", &fmt_num(st.total_fragments_scanned));
    draw_row("Meta Scanned", &fmt_num(st.total_metadata_scanned));
    draw_row(
        "Objects Indexed",
        &format!("{} ({})", fmt_num(st.total_objects_indexed), idx_ratio),
    );
    draw_row("Prefixes Written", &fmt_num(st.total_prefixes_written));
    draw_row("Entries Written", &fmt_num(st.total_entries_written));
    draw_divider(Some("PERFORMANCE"));
    draw_row("Scan Rate", &format!("{:.2} obj/sec", st.scan_rate_per_sec));
    draw_row("Duration", &fmt_duration(st.duration_seconds));
    draw_row_colored(
        "Errors",
        &fmt_num(st.total_errors),
        if st.total_errors > 0 { red } else { green },
    );
    if let Some(err) = &st.last_error {
        draw_divider(Some("ERROR"));
        draw_row_colored("Message", err, red);
    }
    draw_bottom();
}

fn render_gc_monitor(st: &GcJobStatus, elapsed: Duration) {
    let status_color = if st.is_running { green } else { yellow };
    let progress = fmt_pct(st.total_fragments_scanned, st.total_fragments);
    let remaining = st
        .total_fragments
        .saturating_sub(st.total_fragments_scanned);
    let elapsed_secs = elapsed.as_secs().max(st.duration_seconds);
    let frag_rate = if elapsed_secs > 0 {
        st.total_fragments_scanned as f64 / elapsed_secs as f64
    } else {
        0.0
    };

    draw_row_colored(
        "Status",
        if st.is_running { "RUNNING" } else { "STOPPED" },
        status_color,
    );
    draw_row("Phase", &st.phase);
    draw_row(
        "Mode",
        if st.is_manual_run {
            "MANUAL"
        } else {
            "SCHEDULED"
        },
    );
    draw_row("Fragment", &st.current_fragment);
    draw_row("Started At", &fmt_unix_ts(st.start_timestamp));

    draw_divider(Some("PROGRESS"));
    draw_row(
        "Fragments",
        &format!(
            "{} / {} ({})",
            fmt_num(st.total_fragments_scanned),
            fmt_num(st.total_fragments),
            progress
        ),
    );
    draw_row("Frag Rate", &format!("{:.2} frag/sec", frag_rate));
    draw_row("ETA", &fmt_eta_seconds(remaining, frag_rate));

    draw_divider(Some("METRICS"));
    draw_row("Live Versions", &fmt_num(st.total_live_versions_seen));
    draw_row("Trash Expired", &fmt_num(st.total_trash_versions_expired));
    draw_row("Sessions Deleted", &fmt_num(st.total_sessions_deleted));
    draw_row(
        "Orphan Blob Dirs",
        &fmt_num(st.total_orphan_blob_dirs_deleted),
    );
    draw_row_colored("Refunded Bytes", &fmt_num(st.total_refund_bytes), cyan);
    draw_row_colored(
        "Errors",
        &fmt_num(st.total_errors),
        if st.total_errors > 0 { red } else { green },
    );

    draw_divider(Some("PERFORMANCE"));
    draw_row("Duration", &fmt_duration(elapsed_secs));

    if let Some(err) = &st.last_error {
        draw_divider(Some("ERROR"));
        draw_row_colored("Message", err, red);
    }
}

fn render_repair_monitor(st: &RepairJobStatus, elapsed: Duration) {
    let status_color = if st.is_running { green } else { yellow };
    let elapsed_secs = elapsed.as_secs().max(st.duration_seconds);
    draw_row_colored(
        "Status",
        if st.is_running { "RUNNING" } else { "STOPPED" },
        status_color,
    );
    draw_row("Phase", &st.phase);
    draw_row("Fragment", &st.current_fragment);
    draw_row("Started At", &fmt_unix_ts(st.start_timestamp));

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
    draw_row("Duration", &fmt_duration(elapsed_secs));

    if let Some(err) = &st.last_error {
        draw_divider(Some("ERROR"));
        draw_row_colored("Message", err, red);
    }
}

fn render_redist_monitor(st: &RedistributionJobStatus, elapsed: Duration) {
    let status_color = if st.is_running { green } else { yellow };
    let elapsed_secs = elapsed.as_secs().max(st.duration_seconds);
    let progress = fmt_pct(
        st.total_objects_migrated,
        st.total_objects_need_redistribution,
    );
    let migrate_rate = if elapsed_secs > 0 {
        st.total_objects_migrated as f64 / elapsed_secs as f64
    } else {
        0.0
    };
    let remaining = st
        .total_objects_need_redistribution
        .saturating_sub(st.total_objects_migrated);

    draw_row_colored(
        "Status",
        if st.is_running { "RUNNING" } else { "STOPPED" },
        status_color,
    );
    draw_row("Phase", &st.phase);
    draw_row("Fragment", &st.current_fragment);
    draw_row("Started At", &fmt_unix_ts(st.start_timestamp));

    draw_divider(Some("PROGRESS"));
    draw_row("Fragments", &fmt_num(st.total_fragments_scanned));
    draw_row("Scanned", &fmt_num(st.total_objects_scanned));
    draw_row(
        "Migrated",
        &format!(
            "{} / {} ({})",
            fmt_num(st.total_objects_migrated),
            fmt_num(st.total_objects_need_redistribution),
            progress
        ),
    );
    draw_row("Migrate Rate", &format!("{:.2} obj/sec", migrate_rate));
    draw_row("ETA", &fmt_eta_seconds(remaining, migrate_rate));

    draw_divider(Some("METRICS"));
    draw_row("Scan Rate", &format!("{:.2} obj/sec", st.scan_rate_per_sec));
    draw_row_colored(
        "Errors",
        &fmt_num(st.total_errors),
        if st.total_errors > 0 { red } else { green },
    );
    if let Some(obj) = &st.last_object {
        draw_row("Last Object", obj);
    }

    draw_divider(Some("PERFORMANCE"));
    draw_row("Duration", &fmt_duration(elapsed_secs));

    if let Some(err) = &st.last_error {
        draw_divider(Some("ERROR"));
        draw_row_colored("Message", err, red);
    }
}

fn render_prefix_monitor(st: &PrefixRebuildJobStatus, elapsed: Duration) {
    let status_color = if st.is_running { green } else { yellow };
    let elapsed_secs = elapsed.as_secs().max(st.duration_seconds);
    let idx_ratio = fmt_pct(st.total_objects_indexed, st.total_metadata_scanned);

    draw_row_colored(
        "Status",
        if st.is_running { "RUNNING" } else { "STOPPED" },
        status_color,
    );
    draw_row("Phase", &st.phase);
    draw_row("Fragment", &st.current_fragment);
    draw_row("Started At", &fmt_unix_ts(st.start_timestamp));

    draw_divider(Some("PROGRESS"));
    draw_row("Fragments", &fmt_num(st.total_fragments_scanned));
    draw_row("Meta Scanned", &fmt_num(st.total_metadata_scanned));
    draw_row(
        "Objects Indexed",
        &format!("{} ({})", fmt_num(st.total_objects_indexed), idx_ratio),
    );
    draw_row("Prefixes Written", &fmt_num(st.total_prefixes_written));
    draw_row("Entries Written", &fmt_num(st.total_entries_written));

    draw_divider(Some("PERFORMANCE"));
    draw_row("Rate", &format!("{:.2} obj/sec", st.scan_rate_per_sec));
    draw_row("Duration", &fmt_duration(elapsed_secs));
    draw_row_colored(
        "Errors",
        &fmt_num(st.total_errors),
        if st.total_errors > 0 { red } else { green },
    );

    if let Some(err) = &st.last_error {
        draw_divider(Some("ERROR"));
        draw_row_colored("Message", err, red);
    }
}

fn render_health_monitor(st: &HealthResponse, elapsed: Duration, deep_scan: bool) {
    let total_index = st.disks.iter().filter(|d| d.role == "index").count() as u64;
    let total_blob = st.disks.iter().filter(|d| d.role == "blob").count() as u64;
    let online_index = st
        .disks
        .iter()
        .filter(|d| d.role == "index" && (d.status == "online" || d.status == "slow"))
        .count() as u64;
    let online_blob = st
        .disks
        .iter()
        .filter(|d| d.role == "blob" && (d.status == "online" || d.status == "slow"))
        .count() as u64;

    let status_color = match st.status.as_str() {
        "healthy" => green,
        "degraded" | "warning" => yellow,
        _ => red,
    };

    draw_row_colored("Status", &st.status.to_uppercase(), status_color);
    draw_row("Summary", &st.summary);
    draw_row(
        "Mode",
        if deep_scan {
            "DEEP_SCAN"
        } else {
            "FAST/CACHED"
        },
    );
    draw_divider(Some("SYSTEM"));
    draw_row("CPU", &format!("{:.2}%", st.system.cpu_usage_percent));
    draw_row("RAM", &format!("{:.2}%", st.system.ram_usage_percent));
    draw_row("Load 1m", &format!("{:.2}", st.system.load_avg_1m));
    draw_row("Elapsed", &fmt_duration(elapsed.as_secs()));

    draw_divider(Some("DISKS"));
    draw_row(
        "Index Online",
        &format!("{} / {}", fmt_num(online_index), fmt_num(total_index)),
    );
    draw_row(
        "Blob Online",
        &format!("{} / {}", fmt_num(online_blob), fmt_num(total_blob)),
    );

    draw_divider(Some("CONSISTENCY"));
    draw_row(
        "Index Health",
        &st.consistency.index_mirror_health.to_uppercase(),
    );
    draw_row(
        "Scanned Objects",
        &fmt_num(st.consistency.total_objects_scanned),
    );

    let mut shown = 0usize;
    for d in st
        .disks
        .iter()
        .filter(|d| d.status != "online" && d.status != "slow")
    {
        if shown == 0 {
            draw_divider(Some("DISK ISSUES"));
        }
        draw_row_colored(
            &format!("{} {}", d.role, shown + 1),
            &format!("{} ({})", d.path, d.status),
            red,
        );
        shown += 1;
        if shown >= 3 {
            break;
        }
    }
}

async fn run_health_watch_loop(
    client: Client,
    status_url: String,
    token: String,
    poll_ms: u64,
    deep_scan: bool,
) -> Result<(), Box<dyn Error>> {
    let mut latest: Option<HealthResponse> = None;
    let mut err_count: u32 = 0;
    let start_time = Instant::now();

    hide_cursor();
    clear_screen_once();

    let mut ticker = tokio::time::interval(Duration::from_millis(poll_ms.max(250)));
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                break;
            }
            _ = ticker.tick() => {
                match client.get(&status_url).bearer_auth(&token).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        match resp.json::<HealthResponse>().await {
                            Ok(h) => {
                                latest = Some(h);
                                err_count = 0;
                            }
                            Err(_) => err_count += 1,
                        }
                    }
                    _ => err_count += 1,
                }

                reset_cursor();
                draw_top("SYSTEM HEALTH MONITOR", 0);
                if let Some(st) = &latest {
                    render_health_monitor(st, start_time.elapsed(), deep_scan);
                } else {
                    draw_row("Status", "Connecting...");
                    draw_row("Elapsed", &fmt_duration(start_time.elapsed().as_secs()));
                }
                if err_count > 0 {
                    draw_divider(Some("CONNECTION"));
                    draw_row_colored("Retries", &fmt_num(err_count as u64), yellow);
                }
                draw_bottom();
                clear_remaining();
                io::stdout().flush().ok();

                if err_count > 20 {
                    println!("\n{}", red("Lost connection to server."));
                    break;
                }
            }
        }
    }

    show_cursor();
    Ok(())
}

async fn stop_job(
    client: &Client,
    method: &str,
    url: &str,
    token: &str,
) -> Result<String, Box<dyn Error>> {
    let req = match method {
        "PATCH" => client.patch(url),
        _ => client.post(url),
    };
    let resp = req.bearer_auth(token).send().await?;
    let code = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !code.is_success() {
        return Err(format!("stop failed [{}]: {}", code, body).into());
    }
    Ok(body)
}

struct MonitorState<T> {
    data: Option<T>,
    seen_running: bool,
    finished: bool,
    error_count: u32,
    last_error: Option<String>,
}

async fn run_monitor_loop<T, F>(
    client: Client,
    status_url: String,
    token: String,
    title: &str,
    poll_ms: u64,
    render_fn: F,
) -> Result<(), Box<dyn Error>>
where
    T: DeserializeOwned + JobStatusTrait + Send + Sync + 'static + Clone,
    F: Fn(&T, usize, Duration),
{
    let state = Arc::new(Mutex::new(MonitorState {
        data: None,
        seen_running: false,
        finished: false,
        error_count: 0,
        last_error: None,
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

            // Keep poll cadence responsive, but allow slow status handlers enough time
            // under heavy maintenance I/O (GC/repair/redistribution).
            let request_timeout =
                Duration::from_millis((poll_ms.max(1000) * 6).clamp(10_000, 60_000));
            let request = client_clone
                .get(&url_clone)
                .bearer_auth(&token_clone)
                .send();

            match tokio::time::timeout(request_timeout, request).await {
                Ok(Ok(resp)) => {
                    if resp.status().is_success() {
                        match resp.json::<T>().await {
                            Ok(status) => {
                                let mut lock = state_clone.lock().unwrap();
                                lock.error_count = 0;
                                lock.last_error = None;

                                let is_running = status.is_running();
                                if is_running {
                                    lock.seen_running = true;
                                }
                                lock.data = Some(status);

                                if lock.seen_running && !is_running {
                                    lock.finished = true;
                                    break;
                                }
                            }
                            Err(e) => {
                                let mut lock = state_clone.lock().unwrap();
                                lock.error_count += 1;
                                lock.last_error = Some(format!("status decode failed: {}", e));
                            }
                        }
                    } else {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        let mut lock = state_clone.lock().unwrap();
                        lock.error_count += 1;
                        lock.last_error =
                            Some(format!("status endpoint failed [{}]: {}", status, body));
                    }
                }
                Ok(Err(e)) => {
                    let mut lock = state_clone.lock().unwrap();
                    lock.error_count += 1;
                    lock.last_error = Some(format!("request failed: {}", e));
                }
                Err(_) => {
                    let mut lock = state_clone.lock().unwrap();
                    lock.error_count += 1;
                    lock.last_error = Some(format!(
                        "status request timed out after {:?}",
                        request_timeout
                    ));
                }
            }
            tokio::time::sleep(Duration::from_millis(poll_ms.max(100))).await;
        }
    });

    hide_cursor();
    clear_screen_once();
    let start_time = Instant::now();
    let mut spinner_idx = 0;

    tokio::time::sleep(Duration::from_millis(100)).await;

    loop {
        let (data, finished, err_count, last_error) = {
            let lock = state.lock().unwrap();
            (
                lock.data.clone(),
                lock.finished,
                lock.error_count,
                lock.last_error.clone(),
            )
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
                if let Some(err) = last_error {
                    draw_row_colored("Last Error", &truncate(&err, UI_WIDTH - 26), red);
                }
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
    poll_ms: u64,
    json_out: bool,
) -> Result<(), Box<dyn Error>> {
    let base_url = get_base_url(url_opt);
    let secret = get_secret(secret_opt)?;

    let client = Client::builder()
        .danger_accept_invalid_certs(insecure)
        .timeout(Duration::from_secs(30))
        .build()?;

    match cmd {
        Commands::Server => {}
        Commands::FormatBlobDevice { .. } => {
            return Err(
                "format-blob-device is a local command; run it directly on the N1 node".into(),
            );
        }
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
                Commands::Ops { cmd: ops_cmd } => match ops_cmd {
                    OpsCommands::Gc { action } => match action {
                        JobAction::Start => {
                            let resp = client
                                .post(format!("{}/v4/maintenance/gc/start", base_url))
                                .bearer_auth(&token)
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                return Err(
                                    format!("GC start failed: {}", resp.text().await?).into()
                                );
                            }
                            info!("GC start requested. Attaching monitor...");
                            run_monitor_loop(
                                client,
                                format!("{}/v4/maintenance/gc/status", base_url),
                                token,
                                "GC MONITOR",
                                poll_ms,
                                |st: &GcJobStatus, _, elapsed| render_gc_monitor(st, elapsed),
                            )
                            .await?;
                        }
                        JobAction::Status => {
                            let status: GcJobStatus = fetch_status(
                                &client,
                                &format!("{}/v4/maintenance/gc/status", base_url),
                                &token,
                            )
                            .await?;
                            if json_out {
                                print_status("GC Status", &status, true)?;
                            } else {
                                print_gc_status_pretty(&status);
                            }
                        }
                        JobAction::Stop => {
                            return Err(
                                "GC stop is not supported by server API. Use repair/redistribution/prefix stop controls."
                                    .into(),
                            );
                        }
                        JobAction::Watch => {
                            run_monitor_loop(
                                client,
                                format!("{}/v4/maintenance/gc/status", base_url),
                                token,
                                "GC MONITOR",
                                poll_ms,
                                |st: &GcJobStatus, _, elapsed| render_gc_monitor(st, elapsed),
                            )
                            .await?;
                        }
                    },
                    OpsCommands::Repair { action, dry_run } => match action {
                        JobAction::Start => {
                            let resp = client
                                .post(format!("{}/v4/maintenance/repair/start", base_url))
                                .query(&[("dry_run", dry_run)])
                                .bearer_auth(&token)
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                return Err(
                                    format!("Repair start failed: {}", resp.text().await?).into()
                                );
                            }
                            info!("Repair start requested.");
                        }
                        JobAction::Status => {
                            let status: RepairJobStatus = fetch_status(
                                &client,
                                &format!("{}/v4/maintenance/repair/status", base_url),
                                &token,
                            )
                            .await?;
                            if json_out {
                                print_status("Repair Status", &status, true)?;
                            } else {
                                print_repair_status_pretty(&status);
                            }
                        }
                        JobAction::Stop => {
                            let msg = stop_job(
                                &client,
                                "POST",
                                &format!("{}/v4/maintenance/repair/stop", base_url),
                                &token,
                            )
                            .await?;
                            info!("Repair stop requested: {}", msg);
                        }
                        JobAction::Watch => {
                            run_monitor_loop(
                                client,
                                format!("{}/v4/maintenance/repair/status", base_url),
                                token,
                                "REPAIR JOB MONITOR",
                                poll_ms,
                                |st: &RepairJobStatus, _, elapsed| {
                                    render_repair_monitor(st, elapsed)
                                },
                            )
                            .await?;
                        }
                    },
                    OpsCommands::Redistribute { action, dry_run } => match action {
                        JobAction::Start => {
                            let resp = client
                                .post(format!("{}/v4/maintenance/redistribution/start", base_url))
                                .query(&[("dry_run", dry_run)])
                                .bearer_auth(&token)
                                .send()
                                .await?;
                            if !resp.status().is_success() {
                                return Err(format!(
                                    "Redistribution start failed: {}",
                                    resp.text().await?
                                )
                                .into());
                            }
                            info!("Redistribution start requested.");
                        }
                        JobAction::Status => {
                            let status: RedistributionJobStatus = fetch_status(
                                &client,
                                &format!("{}/v4/maintenance/redistribution/monitor", base_url),
                                &token,
                            )
                            .await?;
                            if json_out {
                                print_status("Redistribution Status", &status, true)?;
                            } else {
                                print_redistribution_status_pretty(&status);
                            }
                        }
                        JobAction::Stop => {
                            let msg = stop_job(
                                &client,
                                "PATCH",
                                &format!("{}/v4/maintenance/redistribution/stop", base_url),
                                &token,
                            )
                            .await?;
                            info!("Redistribution stop requested: {}", msg);
                        }
                        JobAction::Watch => {
                            run_monitor_loop(
                                client,
                                format!("{}/v4/maintenance/redistribution/monitor", base_url),
                                token,
                                "REDISTRIBUTION MONITOR",
                                poll_ms,
                                |st: &RedistributionJobStatus, _, elapsed| {
                                    render_redist_monitor(st, elapsed)
                                },
                            )
                            .await?;
                        }
                    },
                    OpsCommands::PrefixRebuild { action, fragment } => match action {
                        JobAction::Start => {
                            let mut req = client
                                .post(format!("{}/v4/maintenance/prefix/rebuild/start", base_url))
                                .bearer_auth(&token);
                            if let Some(f) = fragment {
                                req = req.query(&[("fragment", f)]);
                            }
                            let resp = req.send().await?;
                            if !resp.status().is_success() {
                                return Err(format!(
                                    "Prefix rebuild start failed: {}",
                                    resp.text().await?
                                )
                                .into());
                            }
                            info!("Prefix rebuild start requested.");
                        }
                        JobAction::Status => {
                            let status: PrefixRebuildJobStatus = fetch_status(
                                &client,
                                &format!("{}/v4/maintenance/prefix/rebuild/status", base_url),
                                &token,
                            )
                            .await?;
                            if json_out {
                                print_status("Prefix Rebuild Status", &status, true)?;
                            } else {
                                print_prefix_rebuild_status_pretty(&status);
                            }
                        }
                        JobAction::Stop => {
                            let msg = stop_job(
                                &client,
                                "POST",
                                &format!("{}/v4/maintenance/prefix/rebuild/stop", base_url),
                                &token,
                            )
                            .await?;
                            info!("Prefix rebuild stop requested: {}", msg);
                        }
                        JobAction::Watch => {
                            run_monitor_loop(
                                client,
                                format!("{}/v4/maintenance/prefix/rebuild/status", base_url),
                                token,
                                "PREFIX REBUILD MONITOR",
                                poll_ms,
                                |st: &PrefixRebuildJobStatus, _, elapsed| {
                                    render_prefix_monitor(st, elapsed)
                                },
                            )
                            .await?;
                        }
                    },
                },
                Commands::Gc => {
                    info!("Triggering Garbage Collector...");
                    let resp = client
                        .post(format!("{}/v4/maintenance/gc/start", base_url))
                        .bearer_auth(&token)
                        .send()
                        .await?;

                    if resp.status().is_success() {
                        info!("GC Started. Attaching monitor...");
                        let status_url = format!("{}/v4/maintenance/gc/status", base_url);
                        run_monitor_loop(
                            client,
                            status_url,
                            token,
                            "GC MONITOR",
                            poll_ms,
                            |st: &GcJobStatus, _, elapsed| render_gc_monitor(st, elapsed),
                        )
                        .await?;
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
                        poll_ms,
                        |st: &RepairJobStatus, _, elapsed| render_repair_monitor(st, elapsed),
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
                        poll_ms,
                        |st: &RedistributionJobStatus, _, elapsed| {
                            render_redist_monitor(st, elapsed)
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
                        poll_ms,
                        |st: &PrefixRebuildJobStatus, _, elapsed| {
                            render_prefix_monitor(st, elapsed)
                        },
                    )
                    .await?;
                }
                Commands::Health { watch, deep_scan } => {
                    let status_url = format!(
                        "{}/v4/maintenance/health?deep_scan={}",
                        base_url,
                        if *deep_scan { "true" } else { "false" }
                    );

                    if *watch {
                        run_health_watch_loop(client, status_url, token, poll_ms, *deep_scan)
                            .await?;
                    } else {
                        let resp = client.get(status_url).bearer_auth(&token).send().await?;

                        if resp.status().is_success() {
                            let health: HealthResponse = resp.json().await?;
                            print_health_report(&health);
                        } else {
                            error!("Health check failed: {}", resp.text().await?);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}
