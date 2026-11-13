//! Bounded deterministic source independent of external multipart geometry.
//! A part starts at its logical byte offset, including unaligned boundaries.
use super::*;

#[derive(Deserialize)]
#[serde(default)]
pub struct Workload {
    pub total_bytes: u64,
    /// Zero selects one-shot. Other entries are external multipart sizes.
    pub part_bytes: Vec<u64>,
    pub large_only: bool,
    pub negotiate_part_size: bool,
}

impl Default for Workload {
    fn default() -> Self {
        Self {
            total_bytes: 1 << 30,
            part_bytes: vec![0, 8 << 20, 48 << 20],
            large_only: false,
            negotiate_part_size: false,
        }
    }
}

impl Workload {
    /// More latency samples are safe only for bounded small-object-only runs.
    /// This diagnostic limit does not change either server's upload policy.
    pub fn validate_repeats(&self, repeats: usize) -> Result<()> {
        let maximum = if self.large_only && self.total_bytes <= 64 << 10 {
            512
        } else {
            5
        };
        if !(1..=maximum).contains(&repeats) {
            return Err(format!("repeats must be 1..={maximum} for this workload").into());
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.total_bytes == 0 || self.total_bytes > 128 << 30 {
            return Err("named benchmark source must be 1 byte..=128 GiB".into());
        }
        let mut unique = std::collections::BTreeSet::new();
        if self.part_bytes.is_empty()
            || self.part_bytes.len() > 8
            || self.part_bytes.iter().any(|part| {
                !unique.insert(*part)
                    || *part > 50 << 20
                    || (*part != 0 && self.total_bytes.div_ceil(*part) > 131072)
            })
        {
            return Err("choose 1–8 unique part sizes, at most 50 MiB and 131072 parts; zero selects one-shot".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct Shape {
    pub total_bytes: u64,
    pub part_bytes: u64,
    pub negotiate_part_size: bool,
}

pub fn source(
    block: Bytes,
    offset: u64,
    length: u64,
) -> impl futures_util::Stream<Item = std::io::Result<Bytes>> + Send + 'static {
    assert!(!block.is_empty());
    futures_util::stream::unfold(
        (block, offset, length),
        |(block, offset, remaining)| async move {
            if remaining == 0 {
                return None;
            }
            let start = (offset % block.len() as u64) as usize;
            let length = remaining.min((block.len() - start) as u64) as usize;
            let chunk = block.slice(start..start + length);
            Some((
                Ok(chunk),
                (block, offset + length as u64, remaining - length as u64),
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn larger_latency_samples_require_a_bounded_small_object_only_workload() {
        let mut workload = Workload::default();
        assert!(workload.validate_repeats(5).is_ok());
        assert!(workload.validate_repeats(100).is_err());
        workload.total_bytes = 4096;
        assert!(workload.validate_repeats(100).is_err());
        workload.large_only = true;
        assert!(workload.validate_repeats(100).is_ok());
        assert!(workload.validate_repeats(512).is_ok());
        assert!(workload.validate_repeats(0).is_err());
        assert!(workload.validate_repeats(513).is_err());
        workload.total_bytes = (64 << 10) + 1;
        assert!(workload.validate_repeats(100).is_err());
    }

    #[tokio::test]
    async fn arbitrary_parts_reconstruct_one_source_including_short_tail() {
        let block = Bytes::from_static(b"0123456789abcdef");
        for part_bytes in [1_u64, 7, 16, 25, 1000] {
            let mut actual = Vec::new();
            for offset in (0..139).step_by(part_bytes as usize) {
                let stream = source(block.clone(), offset, (139 - offset).min(part_bytes));
                tokio::pin!(stream);
                while let Some(chunk) = stream.try_next().await.unwrap() {
                    assert!(chunk.len() <= block.len());
                    actual.extend_from_slice(&chunk);
                }
            }
            assert_eq!(actual, block.repeat(9)[..139]);
        }
    }

    #[test]
    fn reject_duplicate_and_unbounded_workloads_before_creating_a_fragment() {
        assert!(Workload::default().validate().is_ok());
        for workload in [
            Workload {
                total_bytes: 0,
                ..Workload::default()
            },
            Workload {
                total_bytes: (128 << 30) + 1,
                ..Workload::default()
            },
            Workload {
                part_bytes: vec![8 << 20, 8 << 20],
                ..Workload::default()
            },
            Workload {
                part_bytes: vec![1],
                ..Workload::default()
            },
            Workload {
                part_bytes: vec![51 << 20],
                ..Workload::default()
            },
            Workload {
                part_bytes: vec![],
                ..Workload::default()
            },
        ] {
            assert!(workload.validate().is_err());
        }
    }
}
