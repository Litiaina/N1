use super::format::Root;
use super::{Result, invalid};
use crate::core::disk_manager::{ObjectMetadata, PartMetadata};
use std::collections::HashSet;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

pub fn remap_path(path: &str, source: &[Root], target: &[Root]) -> Result<String> {
    if path.starts_with("n1://") {
        resolve_target(path, target)?;
        return Ok(path.to_owned());
    }
    for (i, root) in source.iter().enumerate() {
        if let Ok(relative) =
            Path::new(path).strip_prefix(root.legacy_path.as_deref().unwrap_or(&root.path))
        {
            if relative
                .components()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
            {
                return Err(invalid("invalid recorded shard path"));
            }
            if root.legacy_path.is_some() {
                if let Some(member) = &target[i].member {
                    return Ok(format!(
                        "n1://{}/{}/{}/{}",
                        member.pool_id,
                        member.generation,
                        member.slot,
                        relative.display()
                    ));
                }
            }
            return Ok(Path::new(&target[i].path)
                .join(relative)
                .to_str()
                .ok_or_else(|| invalid("invalid target path"))?
                .to_owned());
        }
    }
    Err(invalid(
        "recorded placement refers outside the backed-up roots",
    ))
}

fn resolve_target(path: &str, roots: &[Root]) -> Result<PathBuf> {
    if let Some(logical) = path.strip_prefix("n1://") {
        let mut fields = logical.splitn(4, '/');
        let pool = fields.next().ok_or_else(|| invalid("missing pool"))?;
        let generation: u64 = fields
            .next()
            .ok_or_else(|| invalid("missing generation"))?
            .parse()?;
        let slot: usize = fields
            .next()
            .ok_or_else(|| invalid("missing slot"))?
            .parse()?;
        let relative = Path::new(
            fields
                .next()
                .ok_or_else(|| invalid("missing relative address"))?,
        );
        if relative
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err(invalid("unsafe logical address"));
        }
        let root = roots
            .iter()
            .find(|r| {
                r.member.as_ref().is_some_and(|m| {
                    m.pool_id == pool && m.generation == generation && m.slot == slot
                })
            })
            .ok_or_else(|| invalid("logical address outside restore membership"))?;
        return Ok(Path::new(&root.path).join(relative));
    }
    Ok(PathBuf::from(path))
}

fn remap_part(part: &mut PartMetadata, source: &[Root], target: &[Root]) -> Result<()> {
    let mut domains = HashSet::new();
    for shard in &mut part.shards {
        shard.absolute_path = remap_path(&shard.absolute_path, source, target)?;
        let resolved = resolve_target(&shard.absolute_path, target)?;
        let root = target
            .iter()
            .find(|root| resolved.starts_with(&root.path))
            .ok_or_else(|| invalid("restored placement outside roots"))?;
        if !root.blob || !domains.insert(std::fs::metadata(&root.path)?.dev()) {
            return Err(invalid(
                "restore target would colocate distinct EC shards on one device",
            ));
        }
    }
    Ok(())
}

pub fn remap(relative: &str, bytes: Vec<u8>, source: &[Root], target: &[Root]) -> Result<Vec<u8>> {
    let path = PathBuf::from(relative);
    if relative == ".n1-pool.json" {
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        if value.get("member_format").is_some() {
            let _: crate::core::storage_pool::MemberDescriptor = serde_json::from_value(value)?;
            return Ok(bytes);
        }
        let mut pool: crate::core::storage_pool::StoragePool = serde_json::from_slice(&bytes)?;
        for root in &mut pool.roots {
            *root = PathBuf::from(remap_path(&root.to_string_lossy(), source, target)?);
        }
        return Ok(serde_json::to_vec(&pool)?);
    }
    if path.extension().is_none_or(|ext| ext != "ndx") {
        return Ok(bytes);
    }
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    let components: Vec<_> = path.iter().filter_map(|p| p.to_str()).collect();
    if components.contains(&".objects") {
        let mut metadata: ObjectMetadata = rmp_serde::from_slice(&bytes)?;
        for part in &mut metadata.parts {
            remap_part(part, source, target)?;
        }
        return Ok(rmp_serde::to_vec(&metadata)?);
    }
    if components.contains(&".uploads") {
        if name == "manifest.ndx" {
            return crate::core::index_store::remap_backup_session_manifest(&bytes, |path| {
                remap_path(path, source, target).map_err(|e| {
                    crate::enums::app_error_enum::AppError::InternalServerError(e.to_string())
                })
            })
            .map_err(Into::into);
        }
        if name.starts_with("part_") {
            let mut part: PartMetadata = rmp_serde::from_slice(&bytes)?;
            remap_part(&mut part, source, target)?;
            return Ok(rmp_serde::to_vec(&part)?);
        }
    }
    Ok(bytes)
}

pub fn requires_remap(relative: &str) -> bool {
    if relative == ".n1-pool.json" {
        return true;
    }
    let path = Path::new(relative);
    if path.extension().is_none_or(|ext| ext != "ndx") {
        return false;
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    path.iter().any(|part| part == ".objects")
        || (path.iter().any(|part| part == ".uploads")
            && (name == "manifest.ndx" || name.starts_with("part_")))
}
