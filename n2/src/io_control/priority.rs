use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IoPriority {
    High,
    Normal,
    Low,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IoWorkClass {
    Control,
    Namespace,
    ForegroundRead,
    ForegroundWrite,
    PeerRead,
    PeerWrite,
    Repair,
    GarbageCollection,
    Rebalance,
    DeepMonitor,
}

impl IoWorkClass {
    pub fn priority(self) -> IoPriority {
        match self {
            Self::Control | Self::Namespace => IoPriority::High,
            Self::ForegroundRead | Self::ForegroundWrite | Self::PeerRead | Self::PeerWrite => {
                IoPriority::Normal
            }
            Self::Repair | Self::GarbageCollection | Self::Rebalance | Self::DeepMonitor => {
                IoPriority::Low
            }
        }
    }
}
