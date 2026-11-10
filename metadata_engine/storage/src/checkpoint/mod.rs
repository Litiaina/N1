//! Online checkpoints copy an immutable root, then replay its bounded tail.
//! Installation is coordinated with commits; old generations remain pinned by
//! readers until their last snapshot is released.
mod build;
mod replay;

use crate::{
    Core, Error, Result, State,
    format::{self, Appender, Generation, Manifest},
    tree,
};
pub(crate) use build::build;
pub(crate) use replay::catch_up;
use std::{
    ops::Bound,
    sync::{Arc, atomic::Ordering},
};

pub(crate) struct Prepared {
    pub source: Arc<State>,
    pub source_end: u64,
    pub source_commit: u64,
    pub writer: Appender,
    pub state: State,
}

pub(crate) fn install(core: &Core, mut prepared: Prepared) -> Result<()> {
    let current = core.current();
    catch_up(core, &current, &mut prepared)?;
    prepared.writer.generation.file.sync_all()?;
    #[cfg(test)]
    crate::fault::pause("checkpoint-synced");
    // Persist the new filename before CURRENT is allowed to reference it.
    format::sync_directory(&core.path)?;
    prepared.state.anchor_end = prepared.state.end;
    prepared
        .state
        .generation
        .remove_on_drop
        .store(false, Ordering::Release);
    // Rename followed by a directory-sync failure leaves publication uncertain.
    // Retain both complete generations and stop writes until a safe reopen.
    if let Err(error) = format::publish_manifest(&core.path, Manifest::from_state(&prepared.state))
    {
        core.poisoned.store(true, Ordering::Release);
        return Err(error);
    }
    current
        .generation
        .remove_on_drop
        .store(true, Ordering::Release);
    core.publish(prepared.state);
    #[cfg(test)]
    crate::fault::pause("checkpoint-installed");
    Ok(())
}
