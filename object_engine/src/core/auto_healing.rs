//! Opt-in calendar scrub and repair. Read repair remains independent of this
//! schedule; an interrupted maintenance window is never a storage failure.
pub mod schedule;

#[derive(Debug, Clone, serde::Serialize)]
pub struct AutoHealingStatus {
    pub write_safe: bool,
    pub read_only_reason: Option<String>,
    pub access_mode: super::operation_gate::MaintenanceAccessMode,
}
pub fn status(gate: &super::operation_gate::OperationGate) -> AutoHealingStatus {
    let mode = gate.mode();
    AutoHealingStatus {
        write_safe: mode == super::operation_gate::MaintenanceAccessMode::Availability,
        read_only_reason: gate.safety_reason(),
        access_mode: mode,
    }
}
