//! Logical N1 namespace transactions. Object bytes and LINK placement remain
//! wholly behind the native object API in `object_store`.
//!
//! `models` and `paths` retain the original N1 public types and path rules from
//! 061a40a. Storage changes here replace NCX/NDX bookkeeping with transactions;
//! they do not change the object engine or its on-disk representation.
pub mod models;
pub mod paths;
pub(crate) mod store;

pub(crate) use store::PendingUpload;
pub use store::{Namespace, NamespaceFile, NamespaceFilePage, PublishedVersion, UploadHandle};
