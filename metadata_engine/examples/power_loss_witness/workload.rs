//! Verify the retained ACK set first. Recovery never resends an acknowledged part.
use super::*;
use journal::Journal;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    time::Duration,
};

#[derive(Clone, Deserialize, Serialize)]
pub struct Spec {
    pub index: u64,
    pub path: String,
    pub total: u64,
    pub part_bytes: u64,
    pub strict: bool,
    pub sha256: String,
}

impl Spec {
    fn new(index: u64, part_bytes: u64, run: &str, payload_parts: u64) -> Self {
        let mut spec = Self {
            index,
            path: format!("power-witness/{run}/object-{index:06}"),
            total: payload_parts * part_bytes + 17,
            part_bytes,
            // Publish one normal-mode control before the strict fault window.
            strict: index != 0,
            sha256: String::new(),
        };
        let mut hash = Sha256::new();
        for part in 0..spec.parts() {
            hash.update(spec.payload(part));
        }
        spec.sha256 = format!("{:x}", hash.finalize());
        spec
    }
    pub fn parts(&self) -> u64 {
        self.total.div_ceil(self.part_bytes)
    }
    pub fn route(&self) -> &str {
        if self.strict {
            "multipart/durable"
        } else {
            "multipart"
        }
    }
    pub fn durability(&self) -> &str {
        if self.strict { "strict" } else { "batched" }
    }
    pub fn payload(&self, part: u64) -> Vec<u8> {
        let start = part * self.part_bytes;
        (start..(start + self.part_bytes).min(self.total))
            .map(|offset| {
                let mut word = offset.wrapping_add(self.index.wrapping_mul(0x9e3779b97f4a7c15));
                word = (word ^ (word >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                (word ^ (word >> 27)) as u8
            })
            .collect()
    }
}

struct Item {
    spec: Spec,
    plan: Option<Value>,
    parts: BTreeSet<u64>,
    finalized: Option<Value>,
    finalizing: bool,
    deleting: bool,
    deleted: bool,
}

fn inventory(journal: &Journal) -> Result<BTreeMap<u64, Item>> {
    let mut items = BTreeMap::<u64, Item>::new();
    let mut pending: Option<(u64, String, Value)> = None;
    for record in &journal.records {
        match record.kind.as_str() {
            "header" | "interrupted" => continue,
            "recovery" => {
                pending = None;
                continue;
            }
            "begin_intent" => {
                let spec: Spec = serde_json::from_value(record.details.clone())?;
                if spec.index != record.object
                    || spec.part_bytes == 0
                    || spec.part_bytes > 8 << 20
                    || spec.total < 17
                    || !(spec.total - 17).is_multiple_of(spec.part_bytes)
                    || !(3..=127).contains(&((spec.total - 17) / spec.part_bytes))
                    || items.contains_key(&spec.index)
                {
                    return Err("invalid or duplicate source identity in journal".into());
                }
                items.insert(
                    spec.index,
                    Item {
                        spec,
                        plan: None,
                        parts: BTreeSet::new(),
                        finalized: None,
                        finalizing: false,
                        deleting: false,
                        deleted: false,
                    },
                );
            }
            "begin_ack" | "part_intent" | "part_ack" | "finalize_intent" | "finalize_ack"
            | "delete_intent" | "delete_ack" => (),
            _ => return Err("unknown journal event".into()),
        }
        let item = items
            .get_mut(&record.object)
            .ok_or("journal refers to unknown object")?;
        if let Some(action) = record.kind.strip_suffix("_intent") {
            if pending.is_some() || item.deleted {
                return Err("overlapping or post-delete journal mutation".into());
            }
            if action != "begin" && item.plan.is_none() {
                return Err("mutation without allocated identity".into());
            }
            if action == "delete" {
                item.deleting = true;
            }
            if action == "finalize" {
                item.finalizing = true;
            }
            pending = Some((record.object, action.into(), record.details.clone()));
        } else if let Some(action) = record.kind.strip_suffix("_ack") {
            let (object, expected, details) = pending.take().ok_or("ACK without request intent")?;
            if object != record.object || expected != action {
                return Err("ACK does not match request intent".into());
            }
            match action {
                "begin" => item.plan = Some(record.details.clone()),
                "part" => {
                    let part = details["part"].as_u64().ok_or("missing intended part")?;
                    let repeated = !item.parts.insert(part);
                    if part >= item.spec.parts()
                        || repeated && (item.spec.strict || details["normal_ack_lost"] != true)
                    {
                        return Err("duplicate or out-of-range acknowledged part".into());
                    }
                }
                "finalize" => item.finalized = Some(record.details.clone()),
                "delete" => item.deleted = true,
                _ => return Err("unknown acknowledged action".into()),
            }
        }
    }
    Ok(items)
}

async fn request(
    journal: &mut Journal,
    object: u64,
    action: &str,
    details: Value,
    operation: impl Future<Output = Result<Value>>,
) -> Result<Value> {
    journal.record(&format!("{action}_intent"), object, details)?;
    let result = tokio::time::timeout(Duration::from_secs(180), operation).await;
    match result {
        Ok(Ok(value)) => {
            // No printing, validation, next request or cancellation point before
            // persisting this complete successful response on the recorder host.
            journal.record(&format!("{action}_ack"), object, value.clone())?;
            Ok(value)
        }
        other => {
            let error = match other {
                Ok(Err(error)) => error.to_string(),
                _ => "request timeout; outcome uncertain".into(),
            };
            journal.record(
                "interrupted",
                object,
                json!({"action":action,"error":error}),
            )?;
            Err(error.into())
        }
    }
}

pub async fn write(
    api: &mut api::Api,
    journal: &mut Journal,
    part_bytes: u64,
    payload_parts: u64,
    count: u64,
) -> Result<()> {
    if journal.records.len() != 1 {
        return Err("write requires a new journal".into());
    }
    let run = ulid::Ulid::new().to_string();
    for index in 0..count {
        let spec = Spec::new(index, part_bytes, &run, payload_parts);
        let plan = request(
            journal,
            index,
            "begin",
            serde_json::to_value(&spec)?,
            api.begin(&spec),
        )
        .await?;
        api.validate_plan(&spec, &plan)?;
        for part in 0..spec.parts() {
            // Retain a sparse strict upload as an independent pre-cut ACK oracle.
            if index % 4 == 2 && part != 0 && part != 2 {
                continue;
            }
            request(
                journal,
                index,
                "part",
                json!({"part":part}),
                api.part(&spec, &plan, part),
            )
            .await?;
        }
        if index % 4 == 2 {
            continue;
        }
        request(
            journal,
            index,
            "finalize",
            json!({}),
            api.finalize(&spec, &plan),
        )
        .await?;
        api.verify_bytes(&spec, &plan).await?;
        if index % 4 == 3 {
            request(
                journal,
                index,
                "delete",
                json!({}),
                api.delete(&spec, &plan),
            )
            .await?;
        }
    }
    Ok(())
}

pub async fn verify(api: &mut api::Api, journal: &Journal) -> Result<Value> {
    let mut complete = 0;
    let mut deleted = 0;
    let mut strict_parts = 0;
    let mut rows = Vec::new();
    for (index, item) in inventory(journal)? {
        let Some(plan) = item.plan else {
            rows.push(json!({"object":index,"unreturned_begin":true}));
            continue;
        };
        api.validate_plan(&item.spec, &plan)?;
        if item.deleted {
            if !api.absent(&item.spec, &plan).await? {
                return Err(format!("acknowledged deletion {index} is visible").into());
            }
            deleted += 1;
            rows.push(json!({"object":index,"deleted":true}));
        } else if item.deleting {
            // A purge without a successful reply may be partly applied. Check
            // its outcome before the explicit recovery pass retries deletion.
            let observation = match api.absent(&item.spec, &plan).await {
                Ok(absent) => json!({"absent":absent}),
                Err(error) => json!({"inspection_error":error.to_string()}),
            };
            rows.push(
                json!({"object":index,"delete_outcome_uncertain":true,"observation":observation}),
            );
        } else if let Some(reply) = &item.finalized {
            api.validate_finalization(&plan, reply)?;
            api.verify_bytes(&item.spec, &plan).await?;
            complete += 1;
            rows.push(json!({"object":index,"published_sha256_verified":true}));
        } else if !item.parts.is_empty() {
            if !item.finalizing {
                api.assert_unpublished(&item.spec, &plan).await?;
            }
            let Some(missing) = api.missing(&item.spec, &plan).await? else {
                if item.spec.strict {
                    return Err(format!("strict acknowledged upload {index} disappeared").into());
                }
                rows.push(json!({"object":index,"normal_partial_lost":true,"allowed":true}));
                continue;
            };
            if item.spec.strict && item.parts.iter().any(|part| missing.contains(part)) {
                return Err(format!("strict ACK lost for object {index}").into());
            }
            if item.spec.strict {
                strict_parts += item.parts.len();
            }
            rows.push(json!({"object":index,"strict":item.spec.strict,"acknowledged_parts":item.parts,"missing_parts":missing}));
        } else {
            // An unused Begin is not a power-loss durability promise.
            rows.push(json!({"object":index,"no_payload_ack":true}));
        }
    }
    Ok(
        json!({"read_only":true,"complete_objects_verified":complete,
        "deletions_verified":deleted,"pending_strict_parts_verified":strict_parts,"objects":rows}),
    )
}

pub async fn audit(api: &mut api::Api, journal: &Journal) -> Result<Value> {
    if matches!(api, api::Api::M1 { .. }) {
        return api.fragment_health().await;
    }
    let mut rows = Vec::new();
    for (index, item) in inventory(journal)? {
        if item.deleted || item.deleting || item.finalized.is_none() {
            continue;
        }
        let plan = item
            .plan
            .as_ref()
            .ok_or("published audit identity lacks a plan")?;
        let health = api.native_health(plan).await?;
        let info = &health["info"];
        let width = info["data_shards"]
            .as_u64()
            .ok_or("missing audit data width")?
            + info["parity_shards"]
                .as_u64()
                .ok_or("missing audit parity width")?;
        if health["status"] != "healthy"
            || health["deep"] != true
            || health["verified_bytes"] != item.spec.total
            || info["total_bytes"] != item.spec.total
            || info["finalized_shards"] != width
            || ["missing_shards", "corrupt_shards", "unavailable_shards"]
                .iter()
                .any(|field| {
                    health[field]
                        .as_array()
                        .is_none_or(|values| !values.is_empty())
                })
        {
            return Err(format!("deep shard audit failed for native object {index}").into());
        }
        rows.push(json!({"object":index,"health":health}));
    }
    if rows.is_empty() {
        return Err("empty native audit is not a passing witness".into());
    }
    Ok(
        json!({"scope":"retained published native identities","objects_checked":rows.len(),"objects":rows}),
    )
}

pub async fn recover(api: &mut api::Api, journal: &mut Journal) -> Result<()> {
    let items = inventory(journal)?;
    journal.record(
        "recovery",
        0,
        json!({"prior_read_only_verification_passed":true}),
    )?;
    for (index, item) in items {
        let Some(plan) = item.plan else {
            continue;
        };
        if item.deleted || item.finalized.is_some() && !item.deleting {
            continue;
        }
        if item.deleting {
            request(
                journal,
                index,
                "delete",
                json!({}),
                api.delete(&item.spec, &plan),
            )
            .await?;
            continue;
        }
        if item.parts.is_empty() {
            continue;
        }
        let Some(missing) = api.missing(&item.spec, &plan).await? else {
            if item.spec.strict {
                return Err("strict upload lost before recovery".into());
            }
            // Continuation cannot recreate an absent identity. Keep the
            // permitted loss of this normal, unfinalized upload as evidence.
            continue;
        };
        for part in missing {
            let normal_ack_lost = item.parts.contains(&part);
            if normal_ack_lost && item.spec.strict {
                return Err("refusing to resend a previously acknowledged strict part".into());
            }
            request(
                journal,
                index,
                "part",
                json!({"part":part,"normal_ack_lost":normal_ack_lost}),
                api.part(&item.spec, &plan, part),
            )
            .await?;
        }
        request(
            journal,
            index,
            "finalize",
            json!({}),
            api.finalize(&item.spec, &plan),
        )
        .await?;
        api.verify_bytes(&item.spec, &plan).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_is_stable_across_arbitrary_part_boundaries_and_has_a_short_tail() {
        let first = Spec::new(7, 4096, "test", 3);
        let second = Spec {
            part_bytes: 6144,
            ..first.clone()
        };
        let collect = |spec: &Spec| {
            (0..spec.parts())
                .flat_map(|part| spec.payload(part))
                .collect::<Vec<_>>()
        };
        let bytes = collect(&first);
        assert_eq!(bytes, collect(&second));
        assert_eq!(bytes.len() as u64, first.total);
        assert_eq!(format!("{:x}", Sha256::digest(bytes)), first.sha256);
        assert_eq!(first.payload(3).len(), 17);
    }
    #[test]
    fn unmatched_ack_and_replayed_part_are_not_accepted_as_recovery_evidence() {
        let root = tempfile::tempdir().unwrap();
        let mut journal = Journal::create(&root.path().join("acks.jsonl"), "test").unwrap();
        journal.record("part_ack", 0, json!({})).unwrap();
        assert!(inventory(&journal).is_err());
        let mut journal = Journal::create(&root.path().join("duplicate.jsonl"), "test").unwrap();
        let spec = Spec::new(0, 4096, "test", 3);
        journal
            .record("begin_intent", 0, serde_json::to_value(spec).unwrap())
            .unwrap();
        journal
            .record("begin_ack", 0, json!({"object_key":"private-test-key"}))
            .unwrap();
        for _ in 0..2 {
            journal.record("part_intent", 0, json!({"part":0})).unwrap();
            journal.record("part_ack", 0, json!({})).unwrap();
        }
        assert!(inventory(&journal).is_err());
    }
}
