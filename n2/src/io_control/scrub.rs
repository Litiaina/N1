use serde::Serialize;

const DEFAULT_RECOMMENDED_INTERVAL_SECS: u64 = 86_400;

#[derive(Clone, Debug, Serialize)]
pub struct ScrubControlSnapshot {
    pub automatic_scheduler_enabled: bool,
    pub explicit_operator_action: bool,
    pub recommended_interval_seconds: u64,
    pub route: &'static str,
    pub native_operation: &'static str,
    pub note: &'static str,
}

fn recommended_interval_seconds() -> u64 {
    std::env::var("N2_SCRUB_RECOMMENDED_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_RECOMMENDED_INTERVAL_SECS)
        .clamp(300, 30 * 86_400)
}

pub fn scrub_control_snapshot() -> ScrubControlSnapshot {
    ScrubControlSnapshot {
        automatic_scheduler_enabled: false,
        explicit_operator_action: true,
        recommended_interval_seconds: recommended_interval_seconds(),
        route: "/n2/maintenance/gc/start?mode=scrub",
        native_operation: "maintenance.gc.start",
        note: "N2 keeps scrub explicit; operators schedule it externally so weak nodes are not surprised by background IO.",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrub_is_explicit_by_default() {
        let snap = scrub_control_snapshot();
        assert!(!snap.automatic_scheduler_enabled);
        assert!(snap.explicit_operator_action);
    }
}
