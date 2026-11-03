//! Preserve N1's explicit root binding using LINK's immutable member IDs.
//! The private configuration is authoritative; startup never accepts a new
//! identity just because all replacement disks agree with one another.
use super::{Error, Result, storage_pool::Pool};
use n1_link::PoolId;
pub fn verify_pool(pool: &Pool, expected: &[PoolId]) -> Result<()> {
    if expected.len() != pool.layout.width()
        || expected
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != pool.layout.width()
    {
        return Err(Error::Invalid(
            "missing or duplicate registered member identities".into(),
        ));
    }
    for member in pool.members.iter().flatten() {
        member.check_binding()?;
        if member.marker.members != expected {
            return Err(Error::Conflict(
                "member identity differs from the configured immutable pool map".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::tests::Fixture;
    #[test]
    fn missing_or_replaced_root_never_rebinds_automatically() {
        let first = Fixture::new();
        let store = first.enroll();
        let registered = store.pools[0].members[0]
            .as_ref()
            .unwrap()
            .marker
            .members
            .clone();
        let replacement = Fixture::new();
        let other = replacement.enroll();
        assert!(verify_pool(&other.pools[0], &registered).is_err());
        assert_eq!(
            store.pools[0].members[0].as_ref().unwrap().marker.members,
            registered
        );
    }
    #[test]
    fn new_pool_enrollment_preserves_existing_root_identity() {
        let first = Fixture::new();
        let store = first.enroll();
        let registered = store.pools[0].members[0]
            .as_ref()
            .unwrap()
            .marker
            .members
            .clone();
        let mut added = Fixture::new();
        added.layout.id = PoolId([99; 16]);
        let _other = added.enroll();
        verify_pool(&store.pools[0], &registered).unwrap();
    }
    #[test]
    fn controlled_member_copy_preserves_registered_identity_and_rejects_unbound_maps() {
        let fixture = Fixture::new();
        let store = fixture.enroll();
        let registered = store.pools[0].members[0]
            .as_ref()
            .unwrap()
            .marker
            .members
            .clone();
        assert!(verify_pool(&store.pools[0], &[]).is_err());
        let mut wrong = registered.clone();
        wrong.swap(0, 1);
        assert!(verify_pool(&store.pools[0], &wrong).is_err());
        let roots = (0..4)
            .map(|slot| {
                let root = fixture.base.join(format!("copied-{slot}"));
                std::fs::create_dir(&root).unwrap();
                std::fs::create_dir(root.join(".uploads")).unwrap();
                std::fs::copy(
                    fixture.paths[slot].join(".n1-arc-pool.json"),
                    root.join(".n1-arc-pool.json"),
                )
                .unwrap();
                root
            })
            .collect::<Vec<_>>();
        let copied = Pool::reopen_fixture(fixture.layout.clone(), &roots).unwrap();
        verify_pool(&copied, &registered).unwrap();
    }
}
