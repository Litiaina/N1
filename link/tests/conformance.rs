use n1_link::{
    Durability, Fingerprint, Identity, KeySigner, ObjectKey, PoolId, PoolLayout, UploadPlan,
};

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/link-v1.json")).unwrap()
}
fn bytes<const N: usize>(value: &str) -> [u8; N] {
    let mut out = [0; N];
    hex::decode_to_slice(value, &mut out).unwrap();
    out
}
fn identity() -> Identity {
    let f = fixture();
    Identity {
        pool: f["pool_id"].as_str().unwrap().parse().unwrap(),
        generation: f["generation"].as_u64().unwrap(),
        token: bytes(f["token"].as_str().unwrap()),
    }
}
fn pool() -> PoolLayout {
    let i = identity();
    PoolLayout {
        id: i.pool,
        generation: i.generation,
        data_shards: 3,
        parity_shards: 1,
        block_bytes: 4096,
        placement_version: 1,
        shard_format: n1_link::ShardFormat::Crc32cSha256V2,
    }
}
fn plan() -> UploadPlan {
    UploadPlan {
        identity: identity(),
        total_bytes: 24577,
        part_bytes: 12288,
        expires_at: 2_000_000_000,
        durability: Durability::Durable,
    }
}
fn signer_scope() -> (KeySigner, [u8; 16]) {
    let f = fixture();
    (
        KeySigner::new(bytes(f["signing_secret"].as_str().unwrap())),
        bytes(f["fragment_id"].as_str().unwrap()),
    )
}

#[test]
fn canonical_identity_and_placement_vectors() {
    let f = fixture();
    let i = identity();
    let p = pool();
    assert_eq!(hex::encode(i.encode()), f["identity_hex"].as_str().unwrap());
    assert_eq!(Identity::decode(&i.encode()).unwrap(), i);
    assert_eq!(
        i.fingerprint().to_string(),
        f["fingerprint"].as_str().unwrap()
    );
    assert_eq!(
        p.rotation(i.fingerprint()).unwrap() as u64,
        f["rotation_3_plus_1"].as_u64().unwrap()
    );
    for shard in 0..4 {
        let slot = p.member_slot(i.fingerprint(), shard).unwrap();
        assert_eq!(
            slot as u64,
            f["slots_3_plus_1"][usize::from(shard)].as_u64().unwrap()
        );
        assert_eq!(p.shard_at_slot(i.fingerprint(), slot).unwrap(), shard);
    }
}

#[test]
fn canonical_authenticated_key_vectors() {
    let f = fixture();
    let (signer, scope) = signer_scope();
    let object = signer.object_key(&scope, identity());
    assert_eq!(object.as_str(), f["object_key"].as_str().unwrap());
    assert_eq!(
        signer.verify_object(&scope, object.as_str()).unwrap(),
        identity()
    );
}

#[test]
fn multipart_parameters_never_change_object_identity() {
    let (signer, scope) = signer_scope();
    for size in [0_u64, 1, 12288, 24577, 1 << 40] {
        // Keep every fixture within the supported part-count bound, including
        // the TiB case, while varying geometry independently of identity.
        for part in [12288, 24576, 12288 * 100] {
            let minimum = size.div_ceil(16_777_216).div_ceil(12288) * 12288;
            let part = part.max(minimum);
            let upload = UploadPlan {
                total_bytes: size,
                part_bytes: part,
                ..plan()
            };
            upload.validate(&pool()).unwrap();
            let object = signer.object_key(&scope, upload.identity);
            assert_eq!(
                ObjectKey::inspect(object.as_str()).unwrap().fingerprint(),
                identity().fingerprint()
            );
        }
    }
}

#[test]
fn every_encoded_byte_is_authenticated_and_fragment_scoped() {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let (signer, scope) = signer_scope();
    let object = signer.object_key(&scope, identity());
    let raw = URL_SAFE_NO_PAD.decode(&object.as_str()[5..]).unwrap();
    for index in 0..raw.len() {
        let mut altered = raw.clone();
        altered[index] ^= 1;
        let forged = format!("N1A1.{}", URL_SAFE_NO_PAD.encode(altered));
        assert!(
            signer.verify_object(&scope, &forged).is_err(),
            "byte {index}"
        );
    }
    let mut wrong_scope = scope;
    wrong_scope[0] ^= 1;
    let object = signer.object_key(&scope, identity());
    assert!(signer.verify_object(&wrong_scope, object.as_str()).is_err());
    assert!(
        KeySigner::new([0; 32])
            .verify_object(&scope, object.as_str())
            .is_err()
    );
}

#[test]
fn malformed_and_noncanonical_keys_are_rejected_without_allocation_growth() {
    let (signer, scope) = signer_scope();
    let key = signer.object_key(&scope, identity());
    for length in 0..key.as_str().len() {
        assert!(ObjectKey::inspect(&key.as_str()[..length]).is_err());
    }
    for suffix in ["=", "\n", " ", "A"] {
        assert!(ObjectKey::inspect(&format!("{}{suffix}", key.as_str())).is_err());
    }
    assert!(ObjectKey::inspect(&"A".repeat(1024 * 1024)).is_err());
    for len in 0..120 {
        assert!(Identity::decode(&vec![0; len]).is_err());
    }
    let mut raw = identity().encode();
    raw[0] = 2;
    assert!(Identity::decode(&raw).is_err());
}

#[test]
fn pool_slot_permutations_have_no_collisions_for_supported_widths() {
    let fp = identity().fingerprint();
    for k in 1..=32 {
        for parity in 0..=16 {
            let p = PoolLayout {
                data_shards: k,
                parity_shards: parity,
                ..pool()
            };
            let mut slots = (0..p.width())
                .map(|i| p.member_slot(fp, i as u16).unwrap())
                .collect::<Vec<_>>();
            slots.sort_unstable();
            assert_eq!(slots, (0..p.width()).collect::<Vec<_>>());
        }
    }
}

#[test]
fn part_boundaries_zero_length_and_overflows_are_explicit() {
    let p = plan();
    p.validate(&pool()).unwrap();
    assert_eq!(p.parts(), 3);
    assert_eq!(p.part_length(0).unwrap(), 12288);
    assert_eq!(p.part_length(2).unwrap(), 1);
    assert!(p.part_length(3).is_err());
    let empty = UploadPlan {
        total_bytes: 0,
        ..p
    };
    empty.validate(&pool()).unwrap();
    assert_eq!(empty.parts(), 0);
    assert!(empty.part_length(0).is_err());
    assert!(UploadPlan { part_bytes: 0, ..p }.validate(&pool()).is_err());
    assert!(
        UploadPlan {
            part_bytes: 4096,
            ..p
        }
        .validate(&pool())
        .is_err()
    );
    assert!(
        UploadPlan {
            total_bytes: u64::MAX,
            ..p
        }
        .validate(&pool())
        .is_err()
    );
    let other = PoolLayout {
        generation: pool().generation + 1,
        ..pool()
    };
    assert!(p.validate(&other).is_err());
}

#[test]
fn derived_paths_are_canonical_and_contain_no_host_paths() {
    let fp = identity().fingerprint();
    let text = fp.to_string();
    assert_eq!(fp, text.parse::<Fingerprint>().unwrap());
    assert_eq!(
        fp.shard_path(3),
        format!("{}/{}/{}.s3.shard", &text[..2], &text[2..4], text)
    );
    assert_eq!(fp.staging_path(), format!(".uploads/{text}/shard.tmp"));
    assert!(text.to_uppercase().parse::<Fingerprint>().is_err());
    assert!("../escape".parse::<PoolId>().is_err());
}
