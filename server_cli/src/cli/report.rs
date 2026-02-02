use std::{collections::HashMap, fmt::Display};

use crate::cli::client::HealthResponse;

const BOX_WIDTH: usize = 86;
const INNER: usize = BOX_WIDTH - 2;

fn repeat(ch: char, n: usize) -> String {
    std::iter::repeat(ch).take(n).collect()
}

fn pad_to_width(mut s: String, width: usize) -> String {
    let len = s.chars().count();
    if len < width {
        s.push_str(&" ".repeat(width - len));
    }
    s
}

fn fit(s: impl ToString, width: usize) -> String {
    let s = s.to_string();
    let len = s.chars().count();
    if len <= width {
        return pad_to_width(s, width);
    }
    if width == 0 {
        return String::new();
    }
    if width == 1 {
        return "…".to_string();
    }
    let mut out: String = s.chars().take(width - 1).collect();
    out.push('…');
    out
}

fn box_top() {
    println!("╔{}╗", repeat('═', INNER));
}

fn box_bottom() {
    println!("╚{}╝", repeat('═', INNER));
}

fn box_line(content: impl ToString) {
    println!("║{}║", fit(content, INNER));
}

fn box_section(title: &str) {
    let t = format!(" {} ", title);
    let t_len = t.chars().count();
    let fill = INNER.saturating_sub(t_len);
    let left = fill / 2;
    let right = fill - left;
    println!("╠{}{}{}╣", repeat('═', left), t, repeat('═', right));
}

fn box_title(title: &str) {
    let t_len = title.chars().count();
    if t_len >= INNER {
        box_line(fit(title, INNER));
        return;
    }
    let left = (INNER - t_len) / 2;
    let right = INNER - t_len - left;
    println!("║{}{}{}║", " ".repeat(left), title, " ".repeat(right));
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut b = bytes as f64;
    let mut i = 0usize;
    while b >= 1024.0 && i < UNITS.len() - 1 {
        b /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{bytes} {}", UNITS[i])
    } else {
        format!("{:.2} {}", b, UNITS[i])
    }
}

fn kv(label: &str, value: impl Display) {
    let label_w = 16;
    let value_w = INNER.saturating_sub(label_w + 2);
    let line = format!(
        "{:<label_w$}  {}",
        format!("{label}:"),
        fit(value.to_string(), value_w)
    );
    box_line(line);
}

pub fn format_mirror_counts(counts: &HashMap<String, u64>) -> String {
    let mut items: Vec<_> = counts.iter().collect();
    items.sort_by(|a, b| a.0.cmp(b.0));
    items
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn print_health_report(health: &HealthResponse) {
    println!();

    box_top();
    box_title("SYSTEM HEALTH REPORT");

    box_section("STATUS");
    kv("Status", &health.status);
    kv("DB Connected", health.database_connected);
    kv("Summary", &health.summary);

    box_section("METRICS");
    kv(
        "CPU Usage",
        format!("{:.2}%", health.system.cpu_usage_percent),
    );
    kv(
        "RAM Usage",
        format!(
            "{:.2}% ({} / {})",
            health.system.ram_usage_percent,
            human_bytes(health.system.ram_used_bytes),
            human_bytes(health.system.ram_total_bytes),
        ),
    );
    kv("Load Avg (1m)", format!("{:.2}", health.system.load_avg_1m));

    box_section("CONSISTENCY");
    kv("Scanned Objects", health.consistency.total_objects_scanned);
    kv("Index Health", &health.consistency.index_mirror_health);
    kv(
        "Mirror Counts",
        health
            .consistency
            .mirror_counts
            .as_ref()
            .map(format_mirror_counts)
            .unwrap_or_else(|| "N/A".to_string()),
    );

    box_section("DISKS");
    box_line("ROLE       │ STATUS     │ LATENCY    │ PATH");
    box_line(repeat('─', INNER));

    for disk in &health.disks {
        let latency = disk
            .latency_ms
            .map(|l| format!("{l}ms"))
            .unwrap_or_else(|| "-".to_string());

        let row = format!(
            "{:<10}│ {:<10}│ {:<10}│ {}",
            fit(&disk.role, 10),
            fit(&disk.status, 10),
            fit(latency, 10),
            disk.path
        );
        box_line(row);

        if let Some(err) = &disk.error {
            box_line(format!("  └─ Error: {err}"));
        }
    }

    box_bottom();
    println!();
}
