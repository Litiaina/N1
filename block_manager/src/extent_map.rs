use crate::error::BlockManagerError;
use crate::types::Extent;

pub(crate) fn remove_extent(
    free_extents: &mut Vec<Extent>,
    target: Extent,
) -> Result<(), BlockManagerError> {
    let Some(idx) = free_extents.iter().position(|extent| {
        target.start_block >= extent.start_block
            && target.start_block.saturating_add(target.block_count)
                <= extent.start_block.saturating_add(extent.block_count)
    }) else {
        return Err(BlockManagerError::Corrupt(format!(
            "allocated extent {:?} not present in free list",
            target
        )));
    };

    let extent = free_extents[idx];
    let prefix_len = target.start_block.saturating_sub(extent.start_block);
    let suffix_start = target.start_block.saturating_add(target.block_count);
    let suffix_len = extent
        .start_block
        .saturating_add(extent.block_count)
        .saturating_sub(suffix_start);

    free_extents.remove(idx);
    if suffix_len > 0 {
        free_extents.insert(
            idx,
            Extent {
                start_block: suffix_start,
                block_count: suffix_len,
            },
        );
    }
    if prefix_len > 0 {
        free_extents.insert(
            idx,
            Extent {
                start_block: extent.start_block,
                block_count: prefix_len,
            },
        );
    }
    Ok(())
}

pub(crate) fn insert_and_merge_extent(free_extents: &mut Vec<Extent>, extent: Extent) {
    if extent.block_count == 0 {
        return;
    }

    free_extents.push(extent);
    free_extents.sort_by_key(|e| e.start_block);

    let mut merged: Vec<Extent> = Vec::with_capacity(free_extents.len());
    for extent in free_extents.drain(..) {
        if let Some(last) = merged.last_mut() {
            if last.start_block.saturating_add(last.block_count) >= extent.start_block {
                let new_end = last
                    .start_block
                    .saturating_add(last.block_count)
                    .max(extent.start_block.saturating_add(extent.block_count));
                last.block_count = new_end.saturating_sub(last.start_block);
                continue;
            }
        }
        merged.push(extent);
    }
    *free_extents = merged;
}
