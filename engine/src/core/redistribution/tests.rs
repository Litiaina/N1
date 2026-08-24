use super::{
    REDIST_INTENT_SUFFIX, RedistributionDurabilityMode, RedistributionExecutionMode,
    RedistributionIntent, RedistributionRunOptions, RedistributionScope,
    collect_redistribution_intent_debt, default_object_concurrency, default_status_batch,
    list_all_fragments, parse_durability_mode_param, parse_execution_mode_param, parse_scope_param,
    reachable_roots,
};
use crate::core::constants::FRAGMENT_METADATA_FILE;
use crate::core::fragment_identity::{
    fragment_alias_dir, fragment_root_on, physical_fragment_dir_name, physical_fragment_root_on,
};
use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::symlink;
use ulid::Ulid;

#[test]
fn durability_auto_resolves_from_fast_mode() {
    let opts_slow = RedistributionRunOptions {
        fast_mode: false,
        durability_mode: RedistributionDurabilityMode::Auto,
        ..Default::default()
    };
    assert!(opts_slow.strict_durability());
    assert_eq!(opts_slow.effective_durability_mode(), "strict");

    let opts_fast = RedistributionRunOptions {
        fast_mode: true,
        durability_mode: RedistributionDurabilityMode::Auto,
        ..Default::default()
    };
    assert!(!opts_fast.strict_durability());
    assert_eq!(opts_fast.effective_durability_mode(), "batched");
}

#[test]
fn durability_explicit_override_wins_over_fast_mode() {
    let strict = RedistributionRunOptions {
        fast_mode: true,
        durability_mode: RedistributionDurabilityMode::Strict,
        ..Default::default()
    };
    assert!(strict.strict_durability());
    assert_eq!(strict.effective_durability_mode(), "strict");

    let batched = RedistributionRunOptions {
        fast_mode: false,
        durability_mode: RedistributionDurabilityMode::Batched,
        ..Default::default()
    };
    assert!(!batched.strict_durability());
    assert_eq!(batched.effective_durability_mode(), "batched");
}

#[test]
fn parse_durability_mode_from_string_param() {
    let mut params = HashMap::new();
    params.insert("durability".to_string(), "strict".to_string());
    assert!(matches!(
        parse_durability_mode_param(&params, true),
        RedistributionDurabilityMode::Strict
    ));

    params.insert("durability".to_string(), "BATCHED".to_string());
    assert!(matches!(
        parse_durability_mode_param(&params, false),
        RedistributionDurabilityMode::Batched
    ));

    params.insert("durability".to_string(), "auto".to_string());
    assert!(matches!(
        parse_durability_mode_param(&params, false),
        RedistributionDurabilityMode::Auto
    ));
}

#[test]
fn parse_durability_mode_from_bool_fallback() {
    let mut params = HashMap::new();
    params.insert("strict_durability".to_string(), "true".to_string());
    assert!(matches!(
        parse_durability_mode_param(&params, true),
        RedistributionDurabilityMode::Strict
    ));

    params.insert("strict_durability".to_string(), "false".to_string());
    assert!(matches!(
        parse_durability_mode_param(&params, false),
        RedistributionDurabilityMode::Batched
    ));
}

#[test]
fn parse_execution_mode_defaults_to_availability() {
    let mut params = HashMap::new();
    assert_eq!(
        parse_execution_mode_param(&params),
        RedistributionExecutionMode::Availability
    );

    params.insert("mode".to_string(), "StRiCt".to_string());
    assert_eq!(
        parse_execution_mode_param(&params),
        RedistributionExecutionMode::Strict
    );

    params.insert("mode".to_string(), "scrub".to_string());
    assert_eq!(
        parse_execution_mode_param(&params),
        RedistributionExecutionMode::Scrub
    );

    params.clear();
    params.insert("access_mode".to_string(), "read_only".to_string());
    assert_eq!(
        parse_execution_mode_param(&params),
        RedistributionExecutionMode::Strict
    );

    params.insert("access_mode".to_string(), "quiesced".to_string());
    assert_eq!(
        parse_execution_mode_param(&params),
        RedistributionExecutionMode::Scrub
    );
}

#[test]
fn execution_modes_scale_default_concurrency_and_batch() {
    let availability = default_object_concurrency(RedistributionExecutionMode::Availability, true);
    let strict = default_object_concurrency(RedistributionExecutionMode::Strict, true);
    let scrub = default_object_concurrency(RedistributionExecutionMode::Scrub, true);
    assert!(availability <= strict);
    assert!(strict <= scrub);

    let availability_batch = default_status_batch(RedistributionExecutionMode::Availability, true);
    let strict_batch = default_status_batch(RedistributionExecutionMode::Strict, true);
    let scrub_batch = default_status_batch(RedistributionExecutionMode::Scrub, true);
    assert!(availability_batch <= strict_batch);
    assert!(strict_batch <= scrub_batch);
}

#[test]
fn parse_scope_defaults_and_aliases() {
    let mut params = HashMap::new();
    assert_eq!(parse_scope_param(&params), RedistributionScope::All);

    params.insert("scope".to_string(), "object".to_string());
    assert_eq!(parse_scope_param(&params), RedistributionScope::Object);

    params.insert("scope".to_string(), "namespace".to_string());
    assert_eq!(parse_scope_param(&params), RedistributionScope::Namespace);
}

#[tokio::test]
async fn list_all_fragments_ignores_filesystem_trash_directories() {
    let base = std::env::temp_dir().join(format!("n1-redist-fragments-{}", Ulid::new()));
    let index_root = base.join("index-root");
    fs::create_dir_all(&index_root).expect("create index root");

    let real_fragment = "real-fragment";
    let stable_fragment = "stable-fragment";
    let real_fragment_dir = physical_fragment_root_on(&index_root, stable_fragment);
    fs::create_dir_all(&real_fragment_dir).expect("create fragment dir");
    fs::write(real_fragment_dir.join(FRAGMENT_METADATA_FILE), b"fragment")
        .expect("write fragment metadata marker");
    fs::create_dir_all(fragment_alias_dir(&index_root)).expect("create alias dir");
    symlink(
        std::path::PathBuf::from("..").join(physical_fragment_dir_name(stable_fragment)),
        fragment_alias_dir(&index_root).join(real_fragment),
    )
    .expect("create fragment alias");

    fs::create_dir_all(index_root.join(".Trash-1000")).expect("create desktop trash dir");
    fs::create_dir_all(index_root.join("lost+found")).expect("create unrelated dir");

    let fragments = list_all_fragments(&[index_root]).await;
    assert_eq!(fragments, vec![stable_fragment.to_string()]);

    let _ = fs::remove_dir_all(base);
}

#[tokio::test]
async fn reachable_roots_ignores_blocker_files() {
    let base = std::env::temp_dir().join(format!("n1-redist-roots-{}", Ulid::new()));
    let live_root = base.join("live-root");
    let blocked_root = base.join("blocked-root");

    fs::create_dir_all(&live_root).expect("create live root");
    fs::create_dir_all(&base).expect("create base");
    fs::write(&blocked_root, b"down").expect("create blocker file");

    let live = reachable_roots(&[live_root.clone(), blocked_root.clone()], false).await;
    assert_eq!(live, vec![live_root]);

    let _ = fs::remove_dir_all(base);
}

#[tokio::test]
async fn redistribution_intent_debt_can_scope_to_one_fragment() {
    let base = std::env::temp_dir().join(format!("n1-redist-intents-{}", Ulid::new()));
    let index_root = base.join("index-root");
    let fragment_a = "fragment-a";
    let fragment_b = "fragment-b";
    let now_ms = chrono::Utc::now().timestamp_millis();

    let write_intent = |fragment: &str, object_key: &str, phase: &str| {
        let intent_dir = fragment_root_on(&index_root, fragment).join(".intents");
        fs::create_dir_all(&intent_dir).expect("create intent dir");
        let intent = RedistributionIntent {
            tx_id: Ulid::new().to_string(),
            fragment: fragment.to_string(),
            object_key: object_key.to_string(),
            version_id: Ulid::new().to_string(),
            is_trash: false,
            scope: "all".to_string(),
            phase: phase.to_string(),
            created_at_unix_ms: now_ms.saturating_sub(1000),
            updated_at_unix_ms: now_ms,
        };
        let payload = rmp_serde::to_vec(&intent).expect("serialize intent");
        fs::write(
            intent_dir.join(format!("{}{}", intent.tx_id, REDIST_INTENT_SUFFIX)),
            payload,
        )
        .expect("write intent");
    };

    write_intent(fragment_a, "a.txt", "pending");
    write_intent(fragment_b, "b.txt", "failed");

    let scoped = collect_redistribution_intent_debt(
        std::slice::from_ref(&index_root),
        Some(fragment_a.to_string()),
    )
    .await;
    assert_eq!(scoped.pending_intents, 1);
    assert_eq!(scoped.failed_intents, 0);
    assert_eq!(scoped.fragments_with_pending_intents, 1);

    let global = collect_redistribution_intent_debt(&[index_root], None).await;
    assert_eq!(global.pending_intents, 2);
    assert_eq!(global.failed_intents, 1);
    assert_eq!(global.fragments_with_pending_intents, 2);

    let _ = fs::remove_dir_all(base);
}
