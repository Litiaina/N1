//! Opt-in calendar maintenance, independent of topology health and read repair.
//! A scan runs as an existing asynchronous repair job, not an HTTP request.

use chrono::{DateTime, Datelike, Duration as CalendarDuration, Timelike, Utc, Weekday};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use super::AUTO_HEALING_STATUS;
use crate::core::{disk_manager::FS_STORAGE_ROOT, linux_io, operation_gate, repair_manager};
use crate::routes::storage_routes::AppState;
use crate::settings::config::AutoHealingConfig;

#[derive(Clone, Debug)]
struct Schedule {
    weekday: Option<Weekday>,
    hour: u32,
    minute: u32,
}

impl Schedule {
    fn parse(value: &str) -> Result<Self, String> {
        let fields = value.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 2 {
            return Err("use 'Sun 02:00' or 'daily 02:00' in UTC".into());
        }
        let weekday = match fields[0].to_ascii_lowercase().as_str() {
            "daily" => None,
            "mon" => Some(Weekday::Mon),
            "tue" => Some(Weekday::Tue),
            "wed" => Some(Weekday::Wed),
            "thu" => Some(Weekday::Thu),
            "fri" => Some(Weekday::Fri),
            "sat" => Some(Weekday::Sat),
            "sun" => Some(Weekday::Sun),
            _ => return Err("invalid scrub weekday".into()),
        };
        let (hour, minute) = fields[1].split_once(':').ok_or("invalid scrub time")?;
        if hour.len() != 2 || minute.len() != 2 {
            return Err("scrub time must be HH:MM".into());
        }
        let hour = hour.parse::<u32>().map_err(|_| "invalid scrub hour")?;
        let minute = minute.parse::<u32>().map_err(|_| "invalid scrub minute")?;
        if hour > 23 || minute > 59 {
            return Err("scrub time is outside 00:00..23:59".into());
        }
        Ok(Self {
            weekday,
            hour,
            minute,
        })
    }

    fn slot(&self, now: DateTime<Utc>) -> Option<i64> {
        (self.weekday.is_none_or(|day| day == now.weekday())
            && self.hour == now.hour()
            && self.minute == now.minute())
        .then(|| now.timestamp().div_euclid(60) * 60)
    }

    fn next_after(&self, now: DateTime<Utc>) -> i64 {
        for days in 0..=7 {
            let date = now.date_naive() + CalendarDuration::days(days);
            let candidate = date
                .and_hms_opt(self.hour, self.minute, 0)
                .unwrap()
                .and_utc();
            if candidate > now && self.weekday.is_none_or(|day| day == date.weekday()) {
                return candidate.timestamp();
            }
        }
        unreachable!("weekly schedule always has a next slot")
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ScheduledScrubStatus {
    pub schedule_utc: Option<String>,
    pub phase: String,
    pub next_scheduled_at: Option<i64>,
    pub last_started_at: Option<i64>,
    pub last_completed_at: Option<i64>,
    pub last_error: Option<String>,
    last_attempt_slot: Option<i64>,
}

impl ScheduledScrubStatus {
    fn due(&self, schedule: &Schedule, now: DateTime<Utc>) -> Option<i64> {
        schedule
            .slot(now)
            .filter(|slot| self.last_attempt_slot.is_none_or(|last| *slot > last))
    }
}

async fn publish(status: &ScheduledScrubStatus) {
    AUTO_HEALING_STATUS.write().await.scheduled_scrub = status.clone();
}

async fn save(path: &Path, status: &ScheduledScrubStatus) -> Result<(), String> {
    let bytes = serde_json::to_vec(status).map_err(|e| e.to_string())?;
    linux_io::atomic_write(path, bytes)
        .await
        .map_err(|e| e.to_string())
}

async fn load(path: &Path) -> Result<ScheduledScrubStatus, String> {
    match tokio::fs::read(path).await {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).map_err(|e| format!("invalid scrub journal: {e}"))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(ScheduledScrubStatus::default())
        }
        Err(error) => Err(format!("cannot read scrub journal: {error}")),
    }
}

pub(super) async fn run(state: Arc<AppState>, cfg: AutoHealingConfig) {
    let mut status = ScheduledScrubStatus {
        phase: "disabled".into(),
        ..Default::default()
    };
    if !cfg.enabled || cfg.scrub_schedule_utc.is_empty() {
        publish(&status).await;
        return;
    }
    status.schedule_utc = Some(cfg.scrub_schedule_utc.clone());
    let schedule = match Schedule::parse(&cfg.scrub_schedule_utc) {
        Ok(schedule) => schedule,
        Err(error) => {
            status.phase = "invalid_schedule".into();
            status.last_error = Some(error);
            publish(&status).await;
            return;
        }
    };
    let Some(root) = FS_STORAGE_ROOT.get() else {
        return;
    };
    let path = PathBuf::from(root).join(".n1/scheduled-scrub.json");
    match load(&path).await {
        Ok(previous) => {
            status.last_attempt_slot = previous.last_attempt_slot;
            status.last_started_at = previous.last_started_at;
            status.last_completed_at = previous.last_completed_at;
            AUTO_HEALING_STATUS.write().await.last_checksum_scrub_at = status.last_completed_at;
            if previous.phase == "running" {
                status.last_error = Some(
                    "previous scrub interrupted by engine restart; scan coverage is incomplete"
                        .into(),
                );
            } else {
                status.last_error = previous.last_error;
            }
        }
        Err(error) => {
            status.phase = "journal_unavailable".into();
            status.last_error = Some(error);
            publish(&status).await;
            return;
        }
    }
    status.phase = "startup_delay".into();
    status.next_scheduled_at = Some(schedule.next_after(Utc::now()));
    publish(&status).await;
    tokio::time::sleep(Duration::from_secs(cfg.initial_scan_delay_seconds)).await;
    let mut running: Option<
        tokio::task::JoinHandle<Result<bool, crate::enums::app_error_enum::AppError>>,
    > = None;
    loop {
        let now = Utc::now();
        if running.as_ref().is_some_and(|job| job.is_finished()) {
            let result = running.take().unwrap().await;
            match result {
                Ok(Ok(true)) => {
                    status.phase = "completed".into();
                    status.last_completed_at = Some(now.timestamp());
                    status.last_error = None;
                    AUTO_HEALING_STATUS.write().await.last_checksum_scrub_at =
                        status.last_completed_at;
                }
                Ok(Ok(false)) => {
                    status.phase = "incomplete".into();
                    status.last_error = Some("scrub stopped before full coverage".into());
                }
                Ok(Err(error)) => {
                    status.phase = "incomplete".into();
                    status.last_error = Some(error.to_string());
                }
                Err(error) => {
                    status.phase = "incomplete".into();
                    status.last_error = Some(format!("scrub worker: {error}"));
                }
            }
            if let Err(error) = save(&path, &status).await {
                status.last_error = Some(error);
            }
        }
        if let Some(slot) = status.due(&schedule, now) {
            status.last_attempt_slot = Some(slot);
            // Never overlap another scheduled/manual job, or start mutations
            // through an existing topology/maintenance safety gate.
            if running.is_some() {
                // Preserve the running job's result/state; this slot is skipped.
            } else if operation_gate::current_maintenance_access_mode()
                != operation_gate::MaintenanceAccessMode::Availability
            {
                status.phase = "deferred".into();
                status.last_error = Some(
                    "scheduled slot skipped: storage maintenance/safety gate is active".into(),
                );
            } else {
                match repair_manager::reserve_scheduled_scrub(
                    &cfg,
                    Arc::new(state.ws_manager.clone()),
                ) {
                    Ok(job) => {
                        status.phase = "running".into();
                        status.last_started_at = Some(now.timestamp());
                        status.last_error = None;
                        // Persist admission BEFORE spawning; a restart cannot
                        // immediately duplicate this slot. No global scan deadline.
                        if let Err(error) = save(&path, &status).await {
                            status.phase = "journal_unavailable".into();
                            status.last_error = Some(error);
                            publish(&status).await;
                            return;
                        }
                        running = Some(tokio::spawn(job));
                    }
                    Err(error) => {
                        status.phase = "deferred".into();
                        status.last_error = Some(error.to_string());
                    }
                }
            }
            if let Err(error) = save(&path, &status).await {
                status.last_error = Some(error);
            }
        } else if status.phase == "startup_delay" {
            status.phase = "waiting".into();
        }
        status.next_scheduled_at = Some(schedule.next_after(now));
        publish(&status).await;
        tokio::time::sleep(Duration::from_secs(
            cfg.topology_check_interval_seconds.clamp(1, 30),
        ))
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(value: &str) -> DateTime<Utc> {
        value.parse().unwrap()
    }

    #[test]
    fn calendar_slots_are_utc_and_do_not_duplicate_after_restart_or_clock_rollback() {
        let schedule = Schedule::parse("Sun 02:00").unwrap();
        let slot = utc("2026-11-01T02:00:00Z").timestamp();
        let mut status = ScheduledScrubStatus::default();
        assert_eq!(
            status.due(&schedule, utc("2026-11-01T02:00:59Z")),
            Some(slot)
        );
        status.last_attempt_slot = Some(slot);
        let restored: ScheduledScrubStatus =
            serde_json::from_slice(&serde_json::to_vec(&status).unwrap()).unwrap();
        assert_eq!(restored.due(&schedule, utc("2026-11-01T02:00:59Z")), None);
        assert_eq!(restored.due(&schedule, utc("2026-10-25T02:00:00Z")), None);
        assert!(
            restored
                .due(&schedule, utc("2026-11-08T02:00:00Z"))
                .is_some()
        );
        assert_eq!(
            schedule.next_after(utc("2026-11-01T02:00:30Z")),
            utc("2026-11-08T02:00:00Z").timestamp()
        );
    }

    #[test]
    fn daily_schedule_and_invalid_times() {
        let schedule = Schedule::parse("daily 03:15").unwrap();
        assert!(schedule.slot(utc("2026-10-30T03:15:00Z")).is_some());
        assert_eq!(
            schedule.next_after(utc("2026-10-30T03:15:00Z")),
            utc("2026-10-31T03:15:00Z").timestamp()
        );
        for value in [
            "",
            "Sun 24:00",
            "Sun 02:60",
            "Funday 02:00",
            "Sun 2:00",
            "Sun 02:00 extra",
        ] {
            assert!(Schedule::parse(value).is_err(), "{value}");
        }
    }
}
