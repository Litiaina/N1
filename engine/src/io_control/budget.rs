use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub struct MaintenanceBudgetConfig {
    pub max_duration: Option<Duration>,
    pub max_objects: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_directories: Option<u64>,
}

#[derive(Debug)]
pub struct MaintenanceBudget {
    started: Instant,
    config: MaintenanceBudgetConfig,
    objects: u64,
    bytes: u64,
    directories: u64,
}

impl MaintenanceBudget {
    pub fn new(config: MaintenanceBudgetConfig) -> Self {
        Self {
            started: Instant::now(),
            config,
            objects: 0,
            bytes: 0,
            directories: 0,
        }
    }

    pub fn record_object(&mut self) {
        self.objects = self.objects.saturating_add(1);
    }

    pub fn record_bytes(&mut self, bytes: u64) {
        self.bytes = self.bytes.saturating_add(bytes);
    }

    pub fn record_directory(&mut self) {
        self.directories = self.directories.saturating_add(1);
    }

    pub fn exhausted(&self) -> bool {
        self.exhaustion_reason().is_some()
    }

    pub fn exhaustion_reason(&self) -> Option<&'static str> {
        if self
            .config
            .max_duration
            .is_some_and(|limit| self.started.elapsed() >= limit)
        {
            return Some("duration");
        }
        if self
            .config
            .max_objects
            .is_some_and(|limit| self.objects >= limit)
        {
            return Some("objects");
        }
        if self
            .config
            .max_bytes
            .is_some_and(|limit| self.bytes >= limit)
        {
            return Some("bytes");
        }
        if self
            .config
            .max_directories
            .is_some_and(|limit| self.directories >= limit)
        {
            return Some("directories");
        }
        None
    }

    pub fn snapshot(&self) -> MaintenanceBudgetSnapshot {
        MaintenanceBudgetSnapshot {
            elapsed_ms: self.started.elapsed().as_millis() as u64,
            objects: self.objects,
            bytes: self.bytes,
            directories: self.directories,
            exhausted: self.exhausted(),
            exhaustion_reason: self.exhaustion_reason().map(str::to_string),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaintenanceBudgetSnapshot {
    pub elapsed_ms: u64,
    pub objects: u64,
    pub bytes: u64,
    pub directories: u64,
    pub exhausted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exhaustion_reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_exhausts_by_object_count() {
        let mut budget = MaintenanceBudget::new(MaintenanceBudgetConfig {
            max_duration: None,
            max_objects: Some(1),
            max_bytes: None,
            max_directories: None,
        });
        assert!(!budget.exhausted());
        budget.record_object();
        assert!(budget.exhausted());
        assert_eq!(budget.exhaustion_reason(), Some("objects"));
    }
}
