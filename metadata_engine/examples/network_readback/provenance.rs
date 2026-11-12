use super::{Result, private};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};

pub fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn validate(manifest: &[u8], original: &[u8], endpoint: &[u8]) -> Result<u64> {
    let manifest: Value = serde_json::from_slice(manifest)?;
    let mut old: Value = serde_json::from_slice(original)?;
    let mut new: Value = serde_json::from_slice(endpoint)?;
    let count = manifest["count"].as_u64().ok_or("missing original count")?;
    if manifest["format"] != 1
        || manifest["mode"] != "api"
        || manifest["payload"] != "u32-xor-v1-eight-sizes"
        || manifest["endpoint_sha256"] != hash(original)
        || !(1..=10_000_000).contains(&count)
        || manifest["harness_sha256"]
            .as_str()
            .is_none_or(|s| s.len() != 64 || hex::decode(s).is_err())
    {
        return Err("unsupported or unbound original campaign".into());
    }
    for config in [&old, &new] {
        let url = reqwest::Url::parse(config["url"].as_str().ok_or("missing API URL")?)?;
        if !["http", "https"].contains(&url.scheme())
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err("invalid readback API URL".into());
        }
    }
    old.as_object_mut()
        .ok_or("invalid original endpoint")?
        .remove("url");
    new.as_object_mut()
        .ok_or("invalid readback endpoint")?
        .remove("url");
    if old != new {
        return Err("only the API URL may change when moving the verifier".into());
    }
    Ok(count)
}

pub fn journals(root: &Path) -> Result<Value> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("api-acks-") && name.ends_with(".jsonl") {
            private(&entry.path(), false)?;
            paths.push((name, entry.path()));
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err("preserved campaign has no ACK journals".into());
    }
    let mut rows = Vec::new();
    for (name, path) in paths {
        let bytes = fs::read(path)?;
        rows.push(json!({"file":name,"bytes":bytes.len(),"sha256":hash(&bytes)}));
    }
    Ok(json!(rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn moving_the_verifier_preserves_the_original_campaign_binding() {
        let original = json!({"url":"http://127.0.0.1:50001","identity":"fixed-fragment",
            "master":"test-master","fragment_secret":"test-secret"});
        let bytes = serde_json::to_vec(&original).unwrap();
        let mut endpoint = original.clone();
        endpoint["url"] = json!("http://192.0.2.12:51501");
        let manifest = serde_json::to_vec(&json!({"format":1,"mode":"api","count":1000000,
            "payload":"u32-xor-v1-eight-sizes","endpoint_sha256":hash(&bytes),
            "harness_sha256":"ab".repeat(32)}))
        .unwrap();
        assert_eq!(
            validate(&manifest, &bytes, &serde_json::to_vec(&endpoint).unwrap()).unwrap(),
            1000000
        );
        endpoint["identity"] = json!("fresh-fragment");
        assert!(validate(&manifest, &bytes, &serde_json::to_vec(&endpoint).unwrap()).is_err());
        endpoint = original.clone();
        endpoint["fragment_secret"] = json!("other-secret");
        assert!(validate(&manifest, &bytes, &serde_json::to_vec(&endpoint).unwrap()).is_err());
        let mut changed = bytes.clone();
        changed.push(b' ');
        assert!(validate(&manifest, &changed, &bytes).is_err());
    }
}
