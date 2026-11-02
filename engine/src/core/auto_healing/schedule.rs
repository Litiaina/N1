//! Opt-in calendar maintenance, independent of topology health and read repair.
//! A scan runs as an existing asynchronous repair job, not an HTTP request.

use chrono::{DateTime, Datelike, Duration as CalendarDuration, Timelike, Utc, Weekday};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::core::maintenance::jobs::Jobs;
use crate::core::{Store, linux_io};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalendarScrub {
    pub schedule_utc: String,
    pub initial_delay_seconds: u64,
    pub options: n1_link::noa::ScrubOptions,
}
impl CalendarScrub {
    pub fn validate(&self) -> crate::core::Result<()> {
        Schedule::parse(&self.schedule_utc).map_err(crate::core::Error::Invalid)?;
        self.options.validate()?;
        if self.initial_delay_seconds > 30 * 86400 {
            return Err(crate::core::Error::Invalid(
                "calendar startup delay exceeds 30 days".into(),
            ));
        }
        Ok(())
    }
}

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

async fn save(path: &Path, status: &ScheduledScrubStatus) -> Result<(), String> {
    let bytes = serde_json::to_vec(status).map_err(|e| e.to_string())?;
    linux_io::atomic_write(path, bytes)
        .await
        .map_err(|e| e.to_string())
}

async fn load(path: &Path) -> Result<ScheduledScrubStatus, String> {
    let path = path.to_owned();
    let read =
        tokio::task::spawn_blocking(move || crate::core::backup::fs::read_regular(&path, 65536))
            .await
            .map_err(|e| e.to_string())?;
    match read {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).map_err(|e| format!("invalid scrub journal: {e}"))
        }
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            Ok(ScheduledScrubStatus::default())
        }
        Err(error) => Err(format!("cannot read scrub journal: {error}")),
    }
}

pub async fn run(
    jobs: Arc<Jobs>,
    store: Arc<Store>,
    cfg: CalendarScrub,
    path: PathBuf,
    stop: CancellationToken,
) -> Result<(), String> {
    cfg.validate().map_err(|e| e.to_string())?;
    let schedule = Schedule::parse(&cfg.schedule_utc)?;
    let mut status = load(&path).await?;
    if status.phase == "running" {
        status.phase = "incomplete".into();
        status.last_error = Some("previous scrub interrupted by engine restart".into());
    }
    status.schedule_utc = Some(cfg.schedule_utc);
    status.next_scheduled_at = Some(schedule.next_after(Utc::now()));
    save(&path, &status).await?;
    tokio::select! { _ = stop.cancelled() => return Ok(()), _ = tokio::time::sleep(Duration::from_secs(cfg.initial_delay_seconds)) => {} }
    let mut owned: Option<String> = None;
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { _ = stop.cancelled() => return Ok(()), _ = timer.tick() => {} }
        let now = Utc::now();
        if let Some(id) = &owned {
            match jobs.status().map_err(|e| e.to_string())? {
                Some(job) if job.id == *id && matches!(job.phase, "running" | "stopping") => {}
                Some(job) if job.id == *id => {
                    status.phase = job.phase.into();
                    status.last_error = job.error;
                    if job.phase == "completed" {
                        status.last_completed_at = Some(now.timestamp());
                    }
                    owned = None;
                    save(&path, &status).await?;
                }
                _ => {
                    status.phase = "incomplete".into();
                    status.last_error =
                        Some("scheduled job result replaced before observation".into());
                    owned = None;
                    save(&path, &status).await?;
                }
            }
        }
        if let Some(slot) = status.due(&schedule, now) {
            status.last_attempt_slot = Some(slot);
            // Persist the slot before admitting any filesystem work. Restarts
            // and clock rollback cannot duplicate a previously attempted slot.
            save(&path, &status).await?;
            if owned.is_none() {
                match jobs.scrub(store.clone(), cfg.options.clone()) {
                    Ok(job) => {
                        status.phase = "running".into();
                        status.last_started_at = Some(now.timestamp());
                        status.last_error = None;
                        owned = Some(job.id);
                    }
                    Err(error) => {
                        status.phase = "deferred".into();
                        status.last_error = Some(error.to_string());
                    }
                }
            }
            status.next_scheduled_at = Some(schedule.next_after(now));
            save(&path, &status).await?;
        }
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

    #[tokio::test]
    async fn journal_rejects_symlinks_special_files_oversize_and_corrupt_content() {
        use std::os::unix::fs::symlink;
        let fixture = crate::core::tests::Fixture::new();
        let path = fixture.base.join("journal.json");
        assert_eq!(load(&path).await.unwrap().last_attempt_slot, None);
        std::fs::write(&path, b"corrupt").unwrap();
        assert!(load(&path).await.is_err());
        std::fs::write(&path, vec![b' '; 65537]).unwrap();
        assert!(load(&path).await.is_err());
        std::fs::remove_file(&path).unwrap();
        let outside = fixture.base.join("outside");
        std::fs::write(&outside, b"{}").unwrap();
        symlink(&outside, &path).unwrap();
        assert!(load(&path).await.is_err());
        assert_eq!(std::fs::read(&outside).unwrap(), b"{}");
        std::fs::remove_file(&path).unwrap();
        let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), load(&path))
                .await
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    async fn restart_marks_interrupted_scrub_without_restarting_io_or_losing_the_attempted_slot() {
        let fixture = crate::core::tests::Fixture::new();
        let store = fixture.enroll();
        let jobs = Jobs::new();
        let path = fixture.base.join("journal.json");
        let status = ScheduledScrubStatus {
            phase: "running".into(),
            last_attempt_slot: Some(1793498400),
            last_started_at: Some(1793498400),
            ..Default::default()
        };
        save(&path, &status).await.unwrap();
        let cfg = CalendarScrub {
            schedule_utc: "Sun 02:00".into(),
            initial_delay_seconds: 3600,
            options: n1_link::noa::ScrubOptions {
                repair: true,
                bytes_per_second: 1024,
                window_seconds: 60,
            },
        };
        let stop = CancellationToken::new();
        stop.cancel();
        tokio::time::timeout(
            Duration::from_secs(2),
            run(jobs.clone(), store, cfg, path.clone(), stop),
        )
        .await
        .unwrap()
        .unwrap();
        let restored = load(&path).await.unwrap();
        assert_eq!(restored.phase, "incomplete");
        assert_eq!(restored.last_attempt_slot, status.last_attempt_slot);
        assert_eq!(restored.last_started_at, status.last_started_at);
        assert!(restored.last_error.unwrap().contains("restart"));
        assert!(jobs.status().unwrap().is_none());
    }
}
