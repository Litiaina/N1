use super::*;

pub(crate) struct Opened {
    pub lease: File,
    pub file: File,
    pub identity: [u8; 16],
    pub manifest: Manifest,
}

#[derive(Clone, Copy)]
pub(crate) struct Manifest {
    pub generation: u64,
    pub sequence: u64,
    pub root: u64,
    pub end: u64,
    pub commit: u64,
}

impl Manifest {
    pub fn from_state(state: &State) -> Self {
        Self {
            generation: state.generation.id,
            sequence: state.sequence,
            root: state.root,
            end: state.end,
            commit: state.commit,
        }
    }

    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::from(&b"M1ROOT01"[..]);
        for n in [
            self.generation,
            self.sequence,
            self.root,
            self.end,
            self.commit,
        ] {
            bytes.extend_from_slice(&n.to_be_bytes());
        }
        bytes.extend_from_slice(&checksum(&bytes).to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 52
            || &bytes[..8] != b"M1ROOT01"
            || checksum(&bytes[..48]) != u32::from_be_bytes(bytes[48..52].try_into().unwrap())
        {
            return Err(Error::Corrupt("invalid M1 CURRENT root descriptor".into()));
        }
        let mut input = Input::new(&bytes[8..48]);
        let result = Self {
            generation: input.u64()?,
            sequence: input.u64()?,
            root: input.u64()?,
            end: input.u64()?,
            commit: input.u64()?,
        };
        if result.generation == 0 || result.end < DATA_START {
            return Err(Error::Corrupt("invalid M1 root boundary".into()));
        }
        Ok(result)
    }
}

pub(crate) fn publish_manifest(path: &Path, manifest: Manifest) -> Result<()> {
    let pending = path.join(".CURRENT.next");
    match std::fs::remove_file(&pending) {
        Ok(()) => (),
        Err(error) if error.kind() == io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    let file = private_open(&pending, true)?;
    file.write_all_at(&manifest.encode(), 0)?;
    file.sync_all()?;
    std::fs::rename(&pending, path.join("CURRENT"))?;
    #[cfg(test)]
    crate::fault::pause("manifest-renamed");
    sync_directory(path)
}

fn identity(path: &Path) -> Result<[u8; 16]> {
    let file = private_open(&path.join("FORMAT"), false)?;
    if file.metadata()?.len() != 32 {
        return Err(Error::Incompatible(
            "unsupported M1 storage descriptor".into(),
        ));
    }
    let mut bytes = [0; 32];
    file.read_exact_at(&mut bytes, 0)?;
    if &bytes[..8] != b"M1STORE\0"
        || u32::from_be_bytes(bytes[8..12].try_into().unwrap()) != FORMAT_VERSION
        || checksum(&bytes[..28]) != u32::from_be_bytes(bytes[28..32].try_into().unwrap())
    {
        return Err(Error::Incompatible(
            "unsupported or damaged M1 storage descriptor".into(),
        ));
    }
    Ok(bytes[12..28].try_into().unwrap())
}

fn write_identity(path: &Path) -> Result<[u8; 16]> {
    let id = *uuid::Uuid::new_v4().as_bytes();
    let mut bytes = Vec::from(&b"M1STORE\0"[..]);
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&id);
    bytes.extend_from_slice(&checksum(&bytes).to_be_bytes());
    let file = private_open(&path.join("FORMAT"), true)?;
    file.write_all_at(&bytes, 0)?;
    file.sync_all()?;
    sync_directory(path)?;
    Ok(id)
}

pub(crate) fn create_generation(path: &Path, id: u64, identity: &[u8; 16]) -> Result<File> {
    let file = private_open(&generation_path(path, id), true)?;
    let mut header = [0u8; DATA_START as usize];
    header[..8].copy_from_slice(b"M1DATA01");
    header[8..16].copy_from_slice(&id.to_be_bytes());
    header[16..32].copy_from_slice(identity);
    let checksum_at = header.len() - 4;
    let crc = checksum(&header[..checksum_at]);
    header[checksum_at..].copy_from_slice(&crc.to_be_bytes());
    file.write_all_at(&header, 0)?;
    Ok(file)
}

fn verify_generation(file: &File, id: u64, identity: &[u8; 16]) -> Result<()> {
    let mut header = [0u8; DATA_START as usize];
    file.read_exact_at(&mut header, 0)?;
    let checksum_at = header.len() - 4;
    if &header[..8] != b"M1DATA01"
        || header[8..16] != id.to_be_bytes()
        || &header[16..32] != identity
        || header[32..checksum_at].iter().any(|&b| b != 0)
        || checksum(&header[..checksum_at])
            != u32::from_be_bytes(header[checksum_at..].try_into().unwrap())
    {
        return Err(Error::Corrupt(
            "M1 generation identity or checksum differs".into(),
        ));
    }
    Ok(())
}

pub(crate) fn open(path: &Path) -> Result<Opened> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(Error::Invalid(
            "M1 requires a private real database directory".into(),
        ));
    }
    // Reject foreign files before creating even a lock file.
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| Error::Incompatible("foreign M1 directory entry".into()))?;
        if !matches!(name, "LOCK" | "FORMAT" | "CURRENT" | ".CURRENT.next")
            && generation_id(name).is_none()
        {
            return Err(Error::Incompatible(
                "directory belongs to another storage engine".into(),
            ));
        }
        if !entry.file_type()?.is_file() {
            return Err(Error::Incompatible("non-regular M1 directory entry".into()));
        }
    }
    let lease_path = path.join("LOCK");
    let lease = match private_open(&lease_path, true) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            private_open(&lease_path, false)?
        }
        Err(error) => return Err(error.into()),
    };
    match lease.try_lock() {
        Ok(()) => (),
        Err(std::fs::TryLockError::WouldBlock) => return Err(Error::Locked),
        Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
    }
    let id = if path.join("FORMAT").exists() {
        identity(path)?
    } else {
        if std::fs::read_dir(path)?.any(|e| e.is_ok_and(|e| e.file_name() != "LOCK")) {
            return Err(Error::Incompatible(
                "M1 storage descriptor is missing".into(),
            ));
        }
        write_identity(path)?
    };
    let manifest = if path.join("CURRENT").exists() {
        let file = private_open(&path.join("CURRENT"), false)?;
        if file.metadata()?.len() != 52 {
            return Err(Error::Corrupt("invalid M1 CURRENT length".into()));
        }
        let mut bytes = [0; 52];
        file.read_exact_at(&mut bytes, 0)?;
        Manifest::decode(&bytes)?
    } else {
        // An interrupted first creation can only contain an empty generation.
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            if let Some(generation) = entry.file_name().to_str().and_then(generation_id) {
                let file = private_open(&entry.path(), false)?;
                verify_generation(&file, generation, &id)?;
                if file.metadata()?.len() != DATA_START {
                    return Err(Error::Corrupt(
                        "CURRENT missing from a populated M1 database".into(),
                    ));
                }
                std::fs::remove_file(entry.path())?;
            }
        }
        let file = create_generation(path, 1, &id)?;
        file.sync_all()?;
        sync_directory(path)?;
        let manifest = Manifest {
            generation: 1,
            sequence: 0,
            root: 0,
            end: DATA_START,
            commit: 0,
        };
        publish_manifest(path, manifest)?;
        manifest
    };
    let file = private_open(&generation_path(path, manifest.generation), false)?;
    verify_generation(&file, manifest.generation, &id)?;
    // A process crash can leave an uninstalled checkpoint or old reader files.
    // The authoritative CURRENT must be valid before touching those files.
    if manifest.end > file.metadata()?.len() {
        return Err(Error::Corrupt("M1 checkpoint is truncated".into()));
    }
    Ok(Opened {
        lease,
        file,
        identity: id,
        manifest,
    })
}

/// Run only after the authoritative generation and its commit chain recovered.
/// An incomplete/unrecognized abandoned file is retained as evidence; it is
/// neither adopted nor allowed to overwrite a later checkpoint attempt.
pub(crate) fn cleanup(path: &Path, keep: u64, identity: &[u8; 16]) -> Result<()> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if let Some(generation) = entry.file_name().to_str().and_then(generation_id)
            && generation != keep
        {
            let old = private_open(&entry.path(), false)?;
            if verify_generation(&old, generation, identity).is_ok() {
                std::fs::remove_file(entry.path())?;
            }
        }
    }
    sync_directory(path)
}
