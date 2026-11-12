//! Inline completion seals. A durable part persists this footer after payload;
//! only explicit finalization publishes the shard namespace.
use super::*;

pub(super) async fn write_seals(
    files: &[ShardFile],
    fp: Fingerprint,
    root: [u8; 32],
) -> Result<()> {
    write_seals_kind(files, fp, root, false, false).await
}

pub(super) async fn write_completion_seals(
    files: &[ShardFile],
    fp: Fingerprint,
    root: [u8; 32],
) -> Result<()> {
    write_seals_kind(files, fp, root, true, false).await
}

/// Re-dirty a footer observed without a live successful-persistence proof.
/// An earlier failed flush must not be mistaken for a clean durable footer.
pub(super) async fn refresh_completion_seals(
    files: &[ShardFile],
    fp: Fingerprint,
    root: [u8; 32],
) -> Result<()> {
    write_seals_kind(files, fp, root, true, true).await
}

async fn write_seals_kind(
    files: &[ShardFile],
    fp: Fingerprint,
    root: [u8; 32],
    completion: bool,
    refresh: bool,
) -> Result<()> {
    if files.iter().any(|f| f.root.is_some_and(|old| old != root)) {
        return Err(corrupt("finalized shard root conflicts with upload data"));
    }
    collect(
        futures_util::future::join_all(
            files
                .iter()
                .filter(|f| !f.finalized && (refresh || f.root.is_none()))
                .map(|f| {
                    crate::core::linux_io::write_at_async(
                        f.file.clone(),
                        if completion {
                            shard::encode_completion_seal(&f.header, fp, root).to_vec()
                        } else {
                            encode_seal(&f.header, fp, root).to_vec()
                        },
                        f.header.seal_offset(),
                    )
                }),
        )
        .await
        .into_iter()
        .map(|r| r.map_err(Error::from))
        .collect(),
    )?;
    Ok(())
}

/// Failed part completion must disappear from volatile progress before the
/// lifecycle fence releases. Durable inline proofs remain recovery authority.
pub(super) struct PreparedPart {
    pub store: Arc<Store>,
    pub fp: Fingerprint,
    pub part: u64,
    pub acknowledged: bool,
}
impl Drop for PreparedPart {
    fn drop(&mut self) {
        if !self.acknowledged {
            self.store.forget_part(self.fp, self.part);
        }
    }
}
