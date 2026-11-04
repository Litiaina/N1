//! Logical fragments belong to metadata. LINK only sees its service principal.
use crate::{Error, MetadataEngine, RecordKey, Result, Table};
use n1_link::PoolId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;

pub(crate) const CATALOG: PoolId = PoolId([0; 16]);

#[derive(Clone, Serialize, Deserialize)]
pub struct Fragment {
    pub id: PoolId,
    pub alias: String,
    pub name: String,
    pub(crate) key_hash: String,
    pub epoch: u64,
    pub deleted: bool,
}
pub fn alias(name: &str) -> String {
    hex::encode(Sha256::digest(name.as_bytes()))
}
pub(crate) fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| Error::Invalid("cannot encode metadata record".into()))
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(value: Vec<u8>) -> Result<T> {
    serde_json::from_slice(&value)
        .map_err(|_| Error::Incompatible("invalid metadata application record".into()))
}
pub(crate) fn key(value: impl Into<String>) -> Result<RecordKey> {
    RecordKey::new(CATALOG, value)
}
pub fn resolve(db: &MetadataEngine, identity: &str) -> Result<Fragment> {
    let snapshot = db.snapshot();
    let id = if let Ok(id) = Ulid::from_string(identity) {
        if id.to_string() != identity {
            return Err(Error::NotFound);
        }
        PoolId(id.to_bytes())
    } else {
        let hash = if identity.len() == 64 && identity.bytes().all(|b| b.is_ascii_hexdigit()) {
            identity.to_ascii_lowercase()
        } else {
            alias(identity)
        };
        decode::<PoolId>(
            snapshot
                .get(Table::Reclamation, &key(format!("alias/{hash}"))?)?
                .ok_or(Error::NotFound)?,
        )?
    };
    let record: Fragment = decode(
        snapshot
            .get(Table::Reclamation, &key(format!("fragment/{id}"))?)?
            .ok_or(Error::NotFound)?,
    )?;
    if record.deleted {
        return Err(Error::NotFound);
    }
    Ok(record)
}
pub fn create(db: &MetadataEngine, name: &str, key_hash: &str) -> Result<Fragment> {
    if name.trim().is_empty() || name.len() > 1024 || name.contains('\0') {
        return Err(Error::Invalid("invalid fragment name".into()));
    }
    let record = Fragment {
        id: PoolId(Ulid::new().to_bytes()),
        alias: alias(name),
        name: name.into(),
        key_hash: key_hash.into(),
        epoch: 0,
        deleted: false,
    };
    db.transact(64, |tx| {
        let alias_key = key(format!("alias/{}", record.alias))?;
        if tx.get(Table::Reclamation, &alias_key)?.is_some() {
            return Err(Error::NameConflict("fragment already exists".into()));
        }
        tx.put(Table::Reclamation, &alias_key, &encode(&record.id)?)?;
        tx.put(
            Table::Reclamation,
            &key(format!("fragment/{}", record.id))?,
            &encode(&record)?,
        )?;
        tx.put(
            Table::Reclamation,
            &RecordKey::new(record.id, "usage/initialized")?,
            b"1",
        )?;
        Ok(())
    })?;
    Ok(record)
}
pub fn update(
    db: &MetadataEngine,
    current: &Fragment,
    name: Option<String>,
    key_hash: Option<String>,
    delete: bool,
) -> Result<Fragment> {
    db.transact(64, |tx| {
        let record_key = key(format!("fragment/{}", current.id))?;
        let mut record: Fragment = decode(
            tx.get(Table::Reclamation, &record_key)?
                .ok_or(Error::NotFound)?,
        )?;
        if record.deleted {
            return Err(Error::NotFound);
        }
        if let Some(name) = &name {
            if name.trim().is_empty() || name.len() > 1024 {
                return Err(Error::Invalid("invalid fragment name".into()));
            }
            let next = alias(name);
            let next_key = key(format!("alias/{next}"))?;
            if let Some(bytes) = tx.get(Table::Reclamation, &next_key)?
                && decode::<PoolId>(bytes)? != record.id
            {
                return Err(Error::NameConflict("fragment alias is already used".into()));
            }
            tx.remove(Table::Reclamation, &key(format!("alias/{}", record.alias))?)?;
            tx.put(Table::Reclamation, &next_key, &encode(&record.id)?)?;
            record.alias = next;
            record.name = name.clone();
        }
        if let Some(hash) = &key_hash {
            record.key_hash = hash.clone();
            record.epoch += 1;
        }
        if delete {
            record.deleted = true;
            record.epoch += 1;
            tx.remove(Table::Reclamation, &key(format!("alias/{}", record.alias))?)?;
        }
        tx.put(Table::Reclamation, &record_key, &encode(&record)?)?;
        Ok(record)
    })
}
