//! Bounded upload acceleration. A contiguous prefix of acknowledged part roots
//! is folded during transfer. Finalize consumes this state in constant time;
//! explicit status reconciliation rebuilds it after cache loss.
use n1_link::{Fingerprint, ShardFormat, UploadPlan, shard};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};

const MAX_UPLOADS: usize = 1024;
// The previous 64 x 4,096 root budget, shared across active objects.
const MAX_ROOTS: usize = 64 * 4096;
const RECENT_ROOTS: u64 = 64;
#[derive(Clone, Copy)]
pub(crate) struct SealCandidate {
    epoch: u64,
    pub(crate) root: [u8; 32],
}
struct Progress {
    plan: UploadPlan,
    format: ShardFormat,
    roots: BTreeMap<u64, [u8; 32]>,
    prefix: Sha256,
    completed_parts: u64,
    persisted_seal: Option<[u8; 32]>,
    epoch: u64,
    touched: Instant,
    parents_persisted: bool,
}
impl Progress {
    fn reset_prefix(&mut self) {
        self.prefix =
            shard::object_hasher(self.format, self.plan.total_bytes, self.plan.part_bytes);
        self.completed_parts = 0;
    }
}
pub(crate) struct ProgressCache {
    uploads: HashMap<Fingerprint, Progress>,
    roots: usize,
    max_uploads: usize,
    max_roots: usize,
    serial: u64,
}
impl Default for ProgressCache {
    fn default() -> Self {
        Self {
            uploads: HashMap::new(),
            roots: 0,
            max_uploads: MAX_UPLOADS,
            max_roots: MAX_ROOTS,
            serial: 0,
        }
    }
}
impl ProgressCache {
    // Tokens never repeat across entry eviction/recreation. Exhaustion disables
    // acceleration; explicit disk reconciliation remains available.
    fn next_epoch(&mut self) -> Option<u64> {
        match self.serial.checked_add(1) {
            Some(epoch) => {
                self.serial = epoch;
                Some(epoch)
            }
            None => {
                self.uploads.clear();
                self.roots = 0;
                None
            }
        }
    }
    pub(crate) fn remove(&mut self, fp: Fingerprint) {
        if let Some(old) = self.uploads.remove(&fp) {
            self.roots -= old.roots.len();
        }
    }
    pub(crate) fn invalidate(&mut self, fp: Fingerprint, part: u64) {
        let Some(epoch) = self.next_epoch() else {
            return;
        };
        if let Some(p) = self.uploads.get_mut(&fp) {
            p.epoch = epoch;
            p.persisted_seal = None;
            if part < p.completed_parts {
                p.reset_prefix();
            }
            if p.roots.remove(&part).is_some() {
                self.roots -= 1;
            }
        }
    }
    pub(crate) fn root(
        &mut self,
        fp: Fingerprint,
        plan: UploadPlan,
        part: u64,
    ) -> Option<[u8; 32]> {
        let p = self.uploads.get_mut(&fp).filter(|p| p.plan == plan)?;
        p.touched = Instant::now();
        p.roots.get(&part).copied()
    }
    pub(crate) fn completion(
        &mut self,
        fp: Fingerprint,
        plan: UploadPlan,
    ) -> Option<(Sha256, u64)> {
        let p = self.uploads.get_mut(&fp).filter(|p| p.plan == plan)?;
        p.touched = Instant::now();
        Some((p.prefix.clone(), p.completed_parts))
    }
    pub(crate) fn completion_seal(
        &self,
        fp: Fingerprint,
        plan: UploadPlan,
    ) -> Option<SealCandidate> {
        let p = self
            .uploads
            .get(&fp)
            .filter(|p| p.plan == plan && p.completed_parts == plan.parts())?;
        Some(SealCandidate {
            epoch: p.epoch,
            root: p.prefix.clone().finalize().into(),
        })
    }
    pub(crate) fn persisted_seal(&self, fp: Fingerprint, plan: UploadPlan) -> Option<[u8; 32]> {
        self.uploads
            .get(&fp)
            .filter(|p| p.plan == plan && p.completed_parts == plan.parts())?
            .persisted_seal
    }

    pub(crate) fn mark_persisted_seal(
        &mut self,
        fp: Fingerprint,
        plan: UploadPlan,
        seal: SealCandidate,
    ) {
        if self.seal_is_current(fp, plan, seal) {
            self.uploads
                .get_mut(&fp)
                .expect("current completion")
                .persisted_seal = Some(seal.root);
        }
    }

    pub(crate) fn seal_is_current(
        &self,
        fp: Fingerprint,
        plan: UploadPlan,
        seal: SealCandidate,
    ) -> bool {
        self.uploads.get(&fp).is_some_and(|p| {
            p.plan == plan && p.epoch == seal.epoch && p.completed_parts == plan.parts()
        })
    }
    pub(crate) fn parents_persisted(&self, fp: Fingerprint, plan: UploadPlan) -> bool {
        self.uploads
            .get(&fp)
            .is_some_and(|p| p.plan == plan && p.parents_persisted)
    }
    pub(crate) fn mark_parents_persisted(&mut self, fp: Fingerprint, plan: UploadPlan) {
        if let Some(p) = self.uploads.get_mut(&fp).filter(|p| p.plan == plan) {
            p.parents_persisted = true;
        }
    }
    pub(crate) fn remember(
        &mut self,
        fp: Fingerprint,
        plan: UploadPlan,
        format: ShardFormat,
        part: u64,
        root: [u8; 32],
    ) {
        if part >= plan.parts() || self.serial == u64::MAX {
            return;
        }
        let Some(epoch) = self.next_epoch() else {
            return;
        };
        if !self.uploads.contains_key(&fp)
            && self.uploads.len() >= self.max_uploads
            && let Some(oldest) = self.oldest()
        {
            self.remove(oldest);
        }
        let p = self.uploads.entry(fp).or_insert_with(|| Progress {
            plan,
            format,
            roots: BTreeMap::new(),
            prefix: shard::object_hasher(format, plan.total_bytes, plan.part_bytes),
            completed_parts: 0,
            persisted_seal: None,
            epoch,
            touched: Instant::now(),
            parents_persisted: false,
        });
        if p.plan != plan || p.format != format {
            return;
        }
        p.epoch = epoch;
        p.persisted_seal = None;
        if part < p.completed_parts {
            p.reset_prefix();
        }
        if p.roots.insert(part, root).is_none() {
            self.roots += 1;
        }
        p.touched = Instant::now();
        while let Some(root) = p.roots.get(&p.completed_parts) {
            p.prefix.update(root);
            p.completed_parts += 1;
        }
        // Folded history is not an object-sized allocation to free at finalize.
        // Keep only a fixed retry window; every other contribution is in SHA.
        let retain_from = p.completed_parts.saturating_sub(RECENT_ROOTS);
        while p
            .roots
            .first_key_value()
            .is_some_and(|(part, _)| *part < retain_from)
        {
            p.roots.pop_first();
            self.roots -= 1;
        }
        self.trim();
    }
    fn oldest(&self) -> Option<Fingerprint> {
        self.uploads
            .iter()
            .min_by_key(|(_, p)| p.touched)
            .map(|(fp, _)| *fp)
    }
    fn trim(&mut self) {
        while self.roots > self.max_roots {
            let folded = self
                .uploads
                .iter()
                .filter(|(_, p)| {
                    p.roots
                        .first_key_value()
                        .is_some_and(|(part, _)| *part < p.completed_parts)
                })
                .min_by_key(|(_, p)| p.touched)
                .map(|(fp, _)| *fp);
            if let Some(fp) = folded {
                self.uploads
                    .get_mut(&fp)
                    .expect("selected upload")
                    .roots
                    .pop_first();
                self.roots -= 1;
            } else if let Some(oldest) = self.oldest() {
                self.remove(oldest);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use n1_link::{Durability, Identity, PoolId};

    fn plan(token: u8, parts: u64) -> UploadPlan {
        UploadPlan {
            identity: Identity {
                pool: PoolId([1; 16]),
                generation: 1,
                token: [token; 32],
            },
            total_bytes: parts * 48 * 1024 * 1024,
            part_bytes: 48 * 1024 * 1024,
            expires_at: u64::MAX,
            durability: Durability::Durable,
        }
    }
    fn root(part: u64) -> [u8; 32] {
        Sha256::digest(part.to_be_bytes()).into()
    }
    fn limited(roots: usize) -> ProgressCache {
        ProgressCache {
            max_uploads: 2,
            max_roots: roots,
            ..Default::default()
        }
    }

    #[test]
    fn completion_tokens_reject_retries_eviction_and_exhaustion() {
        let mut cache = limited(8);
        let plan = plan(5, 1);
        let fp = plan.identity.fingerprint();
        let format = ShardFormat::Crc32cSha256V2;
        assert!(cache.completion_seal(fp, plan).is_none());
        cache.remember(fp, plan, format, 0, root(0));
        let seal = cache.completion_seal(fp, plan).unwrap();
        assert!(cache.seal_is_current(fp, plan, seal));
        cache.mark_persisted_seal(fp, plan, seal);
        assert_eq!(cache.persisted_seal(fp, plan), Some(seal.root));
        cache.invalidate(fp, 0);
        assert!(!cache.seal_is_current(fp, plan, seal));
        assert!(cache.persisted_seal(fp, plan).is_none());
        cache.remember(fp, plan, format, 0, root(0));
        assert!(!cache.seal_is_current(fp, plan, seal));
        let retry = cache.completion_seal(fp, plan).unwrap();
        cache.remove(fp);
        cache.remember(fp, plan, format, 0, root(0));
        assert!(!cache.seal_is_current(fp, plan, retry));
        cache.serial = u64::MAX;
        cache.invalidate(fp, 0);
        assert!(cache.completion_seal(fp, plan).is_none());
    }

    #[test]
    fn terabyte_completion_keeps_only_eight_roots_and_a_folded_digest() {
        let mut cache = limited(8);
        let mut plan = plan(6, (1_u64 << 40).div_ceil(48 * 1024 * 1024));
        plan.total_bytes = 1_u64 << 40;
        let fp = plan.identity.fingerprint();
        let format = ShardFormat::Crc32cSha256V2;
        let mut expected = shard::object_hasher(format, plan.total_bytes, plan.part_bytes);
        for part in 0..plan.parts() {
            expected.update(root(part));
            cache.remember(fp, plan, format, part, root(part));
            assert!(cache.roots <= 8);
        }
        let seal = cache.completion_seal(fp, plan).unwrap();
        assert!(cache.seal_is_current(fp, plan, seal));
        assert_eq!(seal.root, <[u8; 32]>::from(expected.finalize()));
    }

    #[test]
    fn terabyte_geometry_folds_out_of_order_roots_beyond_the_previous_cutoff() {
        for format in [ShardFormat::Sha256V1, ShardFormat::Crc32cSha256V2] {
            let mut plan = plan(1, (1_u64 << 40).div_ceil(48 * 1024 * 1024));
            plan.total_bytes = 1_u64 << 40;
            let fp = plan.identity.fingerprint();
            let mut cache = ProgressCache::default();
            let mut expected = shard::object_hasher(format, plan.total_bytes, plan.part_bytes);
            for part in 0..plan.parts() {
                expected.update(root(part));
            }
            for group in (0..plan.parts()).step_by(8) {
                for part in (group..(group + 8).min(plan.parts())).rev() {
                    cache.remember(fp, plan, format, part, root(part));
                }
            }
            let (actual, parts) = cache.completion(fp, plan).unwrap();
            assert_eq!(parts, plan.parts());
            assert!(parts > 4096);
            assert_eq!(actual.finalize(), expected.finalize());
            assert!(cache.roots <= MAX_ROOTS);
        }
    }

    #[test]
    fn root_budget_prunes_folded_records_without_losing_the_completed_prefix() {
        let mut cache = limited(8);
        let plan = plan(2, 1000);
        let fp = plan.identity.fingerprint();
        let format = ShardFormat::Crc32cSha256V2;
        let mut expected = shard::object_hasher(format, plan.total_bytes, plan.part_bytes);
        for part in 0..plan.parts() {
            expected.update(root(part));
            cache.remember(fp, plan, format, part, root(part));
            assert!(cache.roots <= 8);
        }
        let (actual, parts) = cache.completion(fp, plan).unwrap();
        assert_eq!(parts, plan.parts());
        assert_eq!(actual.finalize(), expected.finalize());
        assert!(cache.root(fp, plan, 0).is_none());
        cache.mark_parents_persisted(fp, plan);
        assert!(cache.parents_persisted(fp, plan));
        cache.remove(fp);
        assert_eq!(cache.roots, 0);
    }

    #[test]
    fn retry_of_a_pruned_part_invalidates_the_prefix_before_io() {
        let mut cache = limited(8);
        let plan = plan(3, 20);
        let fp = plan.identity.fingerprint();
        for part in 0..plan.parts() {
            cache.remember(fp, plan, ShardFormat::Crc32cSha256V2, part, root(part));
        }
        cache.invalidate(fp, 0);
        assert_eq!(cache.completion(fp, plan).unwrap().1, 0);
        cache.remember(fp, plan, ShardFormat::Crc32cSha256V2, 0, root(0));
        assert_eq!(cache.completion(fp, plan).unwrap().1, 1);
        let mut other = plan;
        other.expires_at -= 1;
        assert!(cache.completion(fp, other).is_none());
    }

    #[test]
    fn out_of_order_holes_and_upload_eviction_stay_bounded_and_never_claim_completion() {
        let mut cache = limited(8);
        for token in 1..=4 {
            let plan = plan(token, 100);
            let fp = plan.identity.fingerprint();
            for part in 1..plan.parts() {
                cache.remember(fp, plan, ShardFormat::Crc32cSha256V2, part, root(part));
                assert!(cache.roots <= 8);
                assert!(cache.uploads.len() <= 2);
                if let Some((_, prefix)) = cache.completion(fp, plan) {
                    assert_eq!(prefix, 0);
                }
            }
        }
        assert_eq!(
            cache.roots,
            cache.uploads.values().map(|p| p.roots.len()).sum::<usize>()
        );
    }
}
