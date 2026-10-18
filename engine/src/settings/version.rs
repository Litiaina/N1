//! Published storage contract, independent of product and wire versions.
//!
//! The RC numbering reset names the existing encoding; it does not rewrite it.
//! Keep this version unchanged for product releases that do not change storage.
pub const STORAGE_FORMAT_VERSION: &str = "1.0.0-RC.1";
pub const STORAGE_ENCODING_VERSION: u16 = crate::core::index_store::N1_ENGINE_FORMAT_VERSION;
