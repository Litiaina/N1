//! Namespace changes shared by normal, batch and multipart publication.
//! The caller persists sealed bytes first and joins the filesystem barrier
//! afterward, even if this function reports a partial publication failure.
use crate::core::{
    Result,
    store::{ShardFile, blocking, collect},
};
use n1_link::Fingerprint;

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

pub(super) async fn persist_publication(files: &[ShardFile], fp: Fingerprint) -> Result<()> {
    collect(
        futures_util::future::join_all(files.iter().map(|file| {
            let member = file.member.clone();
            async move {
                let directories = blocking(move || member.publication_directories(fp)).await?;
                collect(
                    futures_util::future::join_all(directories.into_iter().map(
                        |directory| async move {
                            crate::core::linux_io::fsync_async(directory)
                                .await
                                .map_err(crate::core::Error::from)
                        },
                    ))
                    .await,
                )?;
                Ok(())
            }
        }))
        .await,
    )?;
    Ok(())
}
