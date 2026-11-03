//! Bounded physical maintenance. Scans derive fingerprints from shard paths;
//! they do not retain a catalogue or require client keys.
pub mod jobs;
use super::{
    Error, Result, corrupt,
    download::check_health,
    storage_pool::Pool,
    store::{Store, blocking, joined, now},
};
use n1_link::{Fingerprint, shard::ShardHeader};
use serde::Serialize;
use std::{
    collections::{HashSet, VecDeque},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

pub use n1_link::noa::ScrubOptions;
#[derive(Clone, Debug, Default, Serialize)]
pub struct MaintenanceReport {
    pub examined: u64,
    pub healthy: u64,
    pub degraded: u64,
    pub repaired: u64,
    pub retired: u64,
    pub incomplete: u64,
    pub active_skipped: u64,
    pub retained: u64,
    pub removed_uploads: u64,
    pub removed_shards: u64,
    pub removed_bytes: u64,
    pub verified_bytes: u64,
    pub errors: Vec<String>,
    pub stopped: bool,
}
impl MaintenanceReport {
    fn merge(&mut self, mut other: Self) {
        macro_rules! sum {($($field:ident),*)=>{$(self.$field+=other.$field;)*}}
        sum!(
            examined,
            healthy,
            degraded,
            repaired,
            retired,
            incomplete,
            active_skipped,
            retained,
            removed_uploads,
            removed_shards,
            removed_bytes,
            verified_bytes
        );
        self.stopped |= other.stopped;
        other
            .errors
            .truncate(32usize.saturating_sub(self.errors.len()));
        self.errors.extend(other.errors);
    }
    fn error(&mut self, e: Error) {
        self.incomplete += 1;
        if self.errors.len() < 32 {
            self.errors.push(e.to_string());
        }
    }
}

pub(crate) struct WorkControl {
    pub cancel: CancellationToken,
    object: Option<Arc<CancellationToken>>,
    rate: u64,
    yield_to_foreground: bool,
    start: Instant,
    charged: Arc<AtomicU64>,
    window: Duration,
}
impl WorkControl {
    pub fn new(cancel: CancellationToken, rate: u64, seconds: u64) -> Self {
        Self {
            cancel,
            object: None,
            rate,
            yield_to_foreground: true,
            start: Instant::now(),
            charged: Arc::new(AtomicU64::new(0)),
            window: Duration::from_secs(seconds),
        }
    }
    pub(crate) fn unpaced(cancel: CancellationToken) -> Self {
        let mut control = Self::new(cancel, u64::MAX, 7 * 86400);
        control.yield_to_foreground = false;
        control
    }
    pub fn for_object(&self, cancel: Arc<CancellationToken>) -> Self {
        Self {
            cancel: self.cancel.clone(),
            object: Some(cancel),
            rate: self.rate,
            yield_to_foreground: self.yield_to_foreground,
            start: self.start,
            charged: self.charged.clone(),
            window: self.window,
        }
    }
    pub fn stopped(&self) -> bool {
        self.cancel.is_cancelled()
            || self.object.as_ref().is_some_and(|c| c.is_cancelled())
            || self.start.elapsed() >= self.window
    }
    pub(crate) async fn until_stopped(&self) {
        tokio::select! {
            _ = self.cancel.cancelled() => {},
            _ = self.object_cancelled() => {},
            _ = async {if self.window==Duration::MAX {std::future::pending::<()>().await;} else {tokio::time::sleep(self.window.saturating_sub(self.start.elapsed())).await;}} => {},
        }
    }
    async fn object_cancelled(&self) {
        match &self.object {
            Some(cancel) => cancel.cancelled().await,
            None => std::future::pending().await,
        }
    }
    pub async fn charge(&self, bytes: u64) -> Result<()> {
        // Yield a bounded slice to foreground work. Waiting for a globally
        // empty foreground lane would starve repair under sustained traffic.
        if self.yield_to_foreground
            && crate::core::io_scheduler::foreground_io_pending()
            && !self.stopped()
        {
            tokio::select! {
                _ = self.cancel.cancelled() => {},
                _ = self.object_cancelled() => {},
                _ = tokio::time::sleep(Duration::from_millis(25)) => {},
            }
        }
        if self.stopped() {
            return Err(Error::Conflict(
                "maintenance stopped or its window ended".into(),
            ));
        }
        let charged = self
            .charged
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        let next = Duration::from_secs_f64(charged as f64 / self.rate as f64).min(self.window);
        if let Some(delay) = next.checked_sub(self.start.elapsed()) {
            tokio::select! {_=self.cancel.cancelled()=>{},_=self.object_cancelled()=>{},_=tokio::time::sleep(delay)=>{}}
        }
        if self.stopped() {
            Err(Error::Conflict(
                "maintenance stopped or its window ended".into(),
            ))
        } else {
            Ok(())
        }
    }
}

struct Recent {
    set: HashSet<Fingerprint>,
    order: VecDeque<Fingerprint>,
}
impl Recent {
    fn new() -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
        }
    }
    fn new_item(&mut self, fp: Fingerprint) -> bool {
        if !self.set.insert(fp) {
            return false;
        }
        self.order.push_back(fp);
        if self.order.len() > 8192 {
            self.set
                .remove(&self.order.pop_front().expect("bounded recent scan"));
        }
        true
    }
}

/// One blocking scanner per job, not one blocked thread per pool member. Its
/// small channel leaves the blocking pool available for the consumer's I/O.
fn scan(
    pool: Arc<Pool>,
    staging: bool,
    cancel: CancellationToken,
    batch_size: usize,
) -> tokio::sync::mpsc::Receiver<Result<Fingerprint>> {
    let (tx, rx) = tokio::sync::mpsc::channel(batch_size.clamp(1, 1000));
    tokio::task::spawn_blocking(move || {
        for (slot, member) in pool.members.iter().enumerate() {
            if cancel.is_cancelled() {
                break;
            }
            let Some(member) = member else {
                let _ = tx.blocking_send(Err(Error::Unavailable(
                    "maintenance member unavailable".into(),
                )));
                continue;
            };
            if let Err(e) = member.check_binding() {
                if tx.blocking_send(Err(e)).is_err() {
                    break;
                }
                continue;
            }
            let root = if staging {
                member.path.join(".uploads")
            } else {
                member.path.clone()
            };
            let depth = if staging { 1 } else { 3 };
            let entries = walkdir::WalkDir::new(&root)
                .min_depth(depth)
                .max_depth(depth)
                .max_open(8)
                .follow_links(false)
                .into_iter()
                .filter_entry(|entry| {
                    staging
                        || entry.depth() == 0
                        || entry.depth() > 2
                        || is_hex_pair(&entry.file_name().to_string_lossy())
                });
            for entry in entries {
                if cancel.is_cancelled() {
                    return;
                }
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => {
                        if tx
                            .blocking_send(Err(Error::Unavailable(format!(
                                "physical scan incomplete: {e}"
                            ))))
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }
                };
                let name = entry.file_name().to_string_lossy();
                let fp = if staging {
                    if !entry.file_type().is_dir() {
                        continue;
                    }
                    match name.parse::<Fingerprint>() {
                        Ok(fp) => fp,
                        Err(_) => continue,
                    }
                } else {
                    if !entry.file_type().is_file() {
                        continue;
                    }
                    let Some((raw, suffix)) = name.split_once(".s") else {
                        continue;
                    };
                    let Some(shard) = suffix
                        .strip_suffix(".shard")
                        .and_then(|s| s.parse::<u16>().ok())
                    else {
                        continue;
                    };
                    let Ok(fp) = raw.parse::<Fingerprint>() else {
                        continue;
                    };
                    if pool.layout.member_slot(fp, shard).ok() != Some(slot)
                        || entry.path().strip_prefix(&member.path).ok()
                            != Some(Path::new(&fp.shard_path(shard)))
                    {
                        continue;
                    }
                    fp
                };
                if tx.blocking_send(Ok(fp)).is_err() {
                    return;
                }
            }
        }
    });
    rx
}
fn is_hex_pair(s: &str) -> bool {
    s.len() == 2
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Store {
    pub async fn garbage_collect(
        self: &Arc<Self>,
        cancel: CancellationToken,
    ) -> Result<MaintenanceReport> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            Ok(store.gc_owned(cancel).await)
        })
        .await
    }
    async fn gc_owned(self: Arc<Self>, cancel: CancellationToken) -> MaintenanceReport {
        let mut report = MaintenanceReport::default();
        for pool in &self.pools {
            let io = tokio::select! {
                _ = cancel.cancelled() => { report.stopped = true; return report; },
                io = super::io_scheduler::acquire_disk_io(super::io_scheduler::DiskIoClass::BackgroundGc, None, "upload staging GC") => match io {
                    Ok(io) => io,
                    Err(e) => { report.error(e.into()); continue; },
                },
            };
            let mut scan = scan(
                pool.clone(),
                true,
                cancel.clone(),
                self.runtime().metadata_batch_size,
            );
            let mut seen = Recent::new();
            while let Some(item) = scan.recv().await {
                if cancel.is_cancelled() {
                    report.stopped = true;
                    return report;
                }
                let fp = match item {
                    Ok(fp) => fp,
                    Err(e) => {
                        report.error(e);
                        continue;
                    }
                };
                if !seen.new_item(fp) {
                    continue;
                }
                report.examined += 1;
                io.heartbeat();
                if super::io_scheduler::foreground_io_pending() {
                    tokio::select! { _ = cancel.cancelled() => {}, _ = tokio::time::sleep(Duration::from_millis(25)) => {} }
                }
                let lease = match self.lease(fp) {
                    Ok(lease) => lease,
                    Err(e) => {
                        report.error(e);
                        continue;
                    }
                };
                let Ok(_guard) = lease.state().lifecycle.clone().try_write_owned() else {
                    report.active_skipped += 1;
                    continue;
                };
                let check = pool.clone();
                let minimum_age = self.runtime().staging_min_age_seconds;
                let result = blocking(move || gc_candidate(&check, fp, now(), minimum_age)).await;
                match result {
                    Ok(Some((shards, bytes))) => {
                        self.forget(fp);
                        report.removed_uploads += 1;
                        report.removed_shards += shards;
                        report.removed_bytes += bytes;
                        io.record_progress(bytes);
                    }
                    Ok(None) => report.retained += 1,
                    Err(e) => report.error(e),
                }
            }
            io.complete();
        }
        report
    }

    pub async fn scrub(
        self: &Arc<Self>,
        options: ScrubOptions,
        cancel: CancellationToken,
    ) -> Result<MaintenanceReport> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.scrub_owned(options, cancel, false).await
        })
        .await
    }
    pub(crate) async fn scheduled_scrub(
        self: &Arc<Self>,
        options: ScrubOptions,
        cancel: CancellationToken,
        no_timeout: bool,
    ) -> Result<MaintenanceReport> {
        let store = self.clone();
        joined(self.activity.clone(), async move {
            store.scrub_owned(options, cancel, no_timeout).await
        })
        .await
    }
    async fn scrub_owned(
        self: Arc<Self>,
        options: ScrubOptions,
        cancel: CancellationToken,
        no_timeout: bool,
    ) -> Result<MaintenanceReport> {
        options.validate()?;
        let mut work = WorkControl::new(
            cancel.clone(),
            options.bytes_per_second,
            options.window_seconds,
        );
        if no_timeout {
            work.window = Duration::MAX;
        }
        let control = Arc::new(work);
        let mut report = MaintenanceReport::default();
        for pool in &self.pools {
            let mut scan = scan(
                pool.clone(),
                false,
                cancel.clone(),
                self.runtime().metadata_batch_size,
            );
            let mut seen = Recent::new();
            loop {
                let mut batch = Vec::new();
                while batch.len() < self.runtime().object_scan_concurrency {
                    let item = tokio::select! {_=cancel.cancelled()=>None,item=scan.recv()=>item};
                    let Some(item) = item else { break };
                    match item {
                        Ok(fp) if seen.new_item(fp) => batch.push(fp),
                        Ok(_) => {}
                        Err(error) => report.error(error),
                    }
                }
                if batch.is_empty() {
                    break;
                }
                if control.stopped() {
                    cancel.cancel();
                    report.stopped = true;
                    return Ok(report);
                }
                let pause = Duration::from_millis(self.runtime().scheduled_scrub_pause_millis);
                if !pause.is_zero() {
                    tokio::select! {_=cancel.cancelled()=>{},_=tokio::time::sleep(pause)=>{}}
                }
                let reports = futures_util::future::join_all(batch.into_iter().map(|fp| {
                    self.scrub_candidate(pool.clone(), fp, control.clone(), options.repair)
                }))
                .await;
                for part in reports {
                    report.merge(part);
                }
            }
        }
        if control.stopped() {
            report.stopped = true;
        }
        Ok(report)
    }
    async fn scrub_candidate(
        self: &Arc<Self>,
        pool: Arc<Pool>,
        fp: Fingerprint,
        control: Arc<WorkControl>,
        repair: bool,
    ) -> MaintenanceReport {
        let cancel = control.cancel.clone();
        let mut report = MaintenanceReport {
            examined: 1,
            ..Default::default()
        };
        let result = async {
                    let io = tokio::select! {
                        _ = cancel.cancelled() => return Err(Error::Conflict("scrub stopped".into())),
                        io = super::io_scheduler::acquire_disk_io(super::io_scheduler::DiskIoClass::BackgroundRepair, None, "scheduled scrub") => io?,
                    };
                    let _admission = self.admit_background(&pool).await?;
                    let lease = self.lease(fp)?;
                    let background = lease.state().background();
                    let object_control = control.for_object(background.cancel.clone());
                    let _guard =
                        lease
                            .state()
                            .lifecycle
                            .clone()
                            .try_read_owned()
                            .map_err(|_| {
                                Error::Conflict("foreground object mutation is active".into())
                            })?;
                    let object = self.open_fingerprint(pool.clone(), fp).await?;
                    let result = check_health(&object, &pool.layout, fp, true, Some(&object_control), Some(&io.progress_handle())).await;
                    if result.is_ok() { io.complete(); }
                    result
                }
                .await;
        match result {
            Ok(health) => {
                report.verified_bytes += health.verified_bytes;
                if health.status == "healthy" {
                    report.healthy += 1;
                } else if health.status == "degraded" {
                    report.degraded += 1;
                    if repair {
                        match self
                            .clone()
                            .repair_owned(pool.clone(), fp, Some(control.clone()))
                            .await
                        {
                            Ok(_) => report.repaired += 1,
                            Err(e) => report.error(e),
                        }
                    }
                } else {
                    report.error(Error::Unavailable(
                        "scrub cannot confirm all holders".into(),
                    ));
                }
            }
            Err(Error::Retired) => report.retired += 1,
            Err(e) => report.error(e),
        }
        report
    }
}

fn gc_candidate(
    pool: &Pool,
    fp: Fingerprint,
    at: u64,
    minimum_age: u64,
) -> Result<Option<(u64, u64)>> {
    pool.writable()?;
    let mut candidates = Vec::new();
    let mut canonical = None;
    for shard in 0..pool.layout.width() {
        let member = pool.holder(fp, shard as u16)?;
        // Any final file or terminal state fences staging reclamation. Incomplete
        // finalization/deletion is resolved by the corresponding key operation.
        match member.open_read(Path::new(&fp.shard_path(shard as u16))) {
            Ok(_) => return Ok(None),
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        match member.open_read(Path::new(&format!(".uploads/{fp}/retire.tmp"))) {
            Ok(_) => return Ok(None),
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let file = match member.open_read(Path::new(&fp.staging_path())) {
            Ok(file) => file,
            Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        let mut bytes = [0; 128];
        crate::core::linux_io::pread_exact_at(&*file, &mut bytes, 0)?;
        let header = ShardHeader::decode(&bytes, &pool.layout, fp, shard as u16)
            .map_err(|e| corrupt(e.to_string()))?;
        if header.expires_at >= at || file.metadata()?.len() > header.file_bytes()? {
            return Ok(None);
        }
        if minimum_age > 0 {
            let changed = file
                .metadata()?
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| {
                    Error::Corrupt("staging modification time precedes Unix epoch".into())
                })?
                .as_secs();
            if at.saturating_sub(changed) < minimum_age {
                return Ok(None);
            }
        }
        let mut same = header.clone();
        same.shard = 0;
        if canonical.as_ref().is_some_and(|old| old != &same) {
            return Err(corrupt("staged GC candidates disagree"));
        }
        canonical = Some(same);
        let bytes = file.metadata()?.len();
        candidates.push((member, bytes));
    }
    if candidates.is_empty() {
        return Ok(None);
    }
    let mut removed = 0;
    let mut bytes = 0;
    for (member, size) in candidates {
        if member.unlink(Path::new(&fp.staging_path()), true)? {
            removed += 1;
            bytes += size;
        }
        member.retire_empty_stage(fp, true)?;
    }
    Ok(Some((removed, bytes)))
}
