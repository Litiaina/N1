//! Namespace changes shared by normal, batch and multipart publication.
//! The caller persists sealed bytes first and joins the directory barriers
//! afterward, even if this function reports a partial publication failure.
use crate::core::{
    Result,
    store::{ShardFile, blocking, collect},
};
use n1_link::Fingerprint;
use std::{fs::File, sync::Arc};

pub(super) async fn publish_files(files: &[ShardFile], fp: Fingerprint) -> Result<()> {
    collect(
        futures_util::future::join_all(files.iter().map(|file| {
            let member = file.member.clone();
            let shard = file.header.shard;
            let finalized = file.finalized;
            blocking(move || {
                if !finalized {
                    member.promote(fp, shard, false)?;
                }
                member.retire_empty_stage(fp, false)
            })
        }))
        .await,
    )?;
    Ok(())
}

/// Open before rename/removal, retaining the directories' writeback-error history.
pub(super) async fn prepare_publication(
    files: &[ShardFile],
    fp: Fingerprint,
) -> Result<Vec<Arc<File>>> {
    Ok(collect(
        futures_util::future::join_all(files.iter().map(|file| {
            let member = file.member.clone();
            blocking(move || member.publication_directories(fp))
        }))
        .await,
    )?
    .into_iter()
    .flatten()
    .collect())
}

pub(super) async fn persist_publication(directories: Vec<Arc<File>>) -> Result<()> {
    collect(
        futures_util::future::join_all(directories.into_iter().map(|directory| async move {
            crate::core::linux_io::fsync_async(directory)
                .await
                .map_err(crate::core::Error::from)
        }))
        .await,
    )?;
    Ok(())
}
