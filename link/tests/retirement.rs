use n1_link::{
    Fingerprint,
    retirement::{Phase, Retirement},
};

#[test]
fn terminal_records_are_bound_to_fingerprint_slot_phase_and_every_byte() {
    let fp = Fingerprint([7; 32]);
    for phase in [Phase::Retiring, Phase::Retired] {
        let record = Retirement {
            phase,
            shard: 3,
            nonce: [9; 16],
            timestamp: 1730000000,
        };
        let bytes = record.encode(fp);
        assert_eq!(Retirement::decode(&bytes, fp, 3).unwrap(), record);
        assert!(Retirement::decode(&bytes, Fingerprint([8; 32]), 3).is_err());
        assert!(Retirement::decode(&bytes, fp, 2).is_err());
        for i in 0..bytes.len() {
            for bit in 0..8 {
                let mut bad = bytes;
                bad[i] ^= 1 << bit;
                assert!(
                    Retirement::decode(&bad, fp, 3).is_err(),
                    "byte {i}, bit {bit}"
                );
            }
        }
        for n in 0..bytes.len() {
            assert!(Retirement::decode(&bytes[..n], fp, 3).is_err());
        }
        let mut extra = bytes.to_vec();
        extra.push(0);
        assert!(Retirement::decode(&extra, fp, 3).is_err());
    }
}
