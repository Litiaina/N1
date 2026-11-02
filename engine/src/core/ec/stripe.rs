//! Byte encoding stays with the pinned, previously exercised ECS codec. LINK
//! supplies framing, checksums, ownership and placement; it changes no EC math.
use crate::core::{Error, Result, corrupt};
use bytes::Bytes;
use ecs_core::{EcLayout, ReedSolomonDecoder, ReedSolomonEncoder};
use n1_link::PoolLayout;

pub struct StripeCodec {
    k: usize,
    p: usize,
    block: usize,
    encoder: Option<ReedSolomonEncoder>,
    decoder: Option<ReedSolomonDecoder>,
    padded: Vec<u8>,
}

impl StripeCodec {
    pub fn new(pool: &PoolLayout) -> Result<Self> {
        pool.validate()?;
        let k = usize::from(pool.data_shards);
        let p = usize::from(pool.parity_shards);
        let block = pool.block_bytes as usize;
        EcLayout::new(k, p, block).map_err(|e| Error::Invalid(e.to_string()))?;
        Ok(Self {
            k,
            p,
            block,
            encoder: None,
            decoder: None,
            padded: Vec::new(),
        })
    }

    pub fn encode(&mut self, source: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut pieces = Vec::with_capacity(self.k + self.p);
        self.encode_with(source, |_, piece| {
            pieces.push(piece.to_vec());
            Ok(())
        })?;
        Ok(pieces)
    }

    /// Borrow data and codec parity directly while building owned I/O records.
    /// Only a partial source stripe needs the reusable zero-padding workspace.
    pub fn encode_with(
        &mut self,
        source: &[u8],
        mut emit: impl FnMut(usize, &[u8]) -> Result<()>,
    ) -> Result<()> {
        if source.is_empty() || source.len() > self.k * self.block {
            return Err(Error::Invalid("invalid source stripe length".into()));
        }
        let data = if source.len() == self.k * self.block {
            source
        } else {
            self.padded.resize(self.k * self.block, 0);
            self.padded[..source.len()].copy_from_slice(source);
            self.padded[source.len()..].fill(0);
            &self.padded
        };
        if self.p > 0 && self.encoder.is_none() {
            self.encoder = Some(
                ReedSolomonEncoder::new(self.k, self.p, self.block)
                    .map_err(|e| Error::Invalid(format!("invalid EC layout: {e:?}")))?,
            );
        }
        for (i, piece) in data.chunks_exact(self.block).enumerate() {
            emit(i, piece)?;
        }
        if let Some(encoder) = self.encoder.as_mut() {
            for piece in data.chunks_exact(self.block) {
                encoder
                    .add_original_shard(piece)
                    .map_err(|e| corrupt(format!("EC input: {e:?}")))?;
            }
            let encoded = encoder
                .encode()
                .map_err(|e| corrupt(format!("EC encode: {e:?}")))?;
            for i in 0..self.p {
                emit(
                    self.k + i,
                    encoded
                        .recovery(i)
                        .ok_or_else(|| corrupt("EC omitted parity"))?,
                )?;
            }
        }
        Ok(())
    }

    pub fn decode(&mut self, pieces: &[Option<Bytes>], source_bytes: usize) -> Result<Bytes> {
        if pieces.len() != self.k + self.p
            || source_bytes > self.k * self.block
            || pieces.iter().flatten().any(|p| p.len() != self.block)
        {
            return Err(corrupt("invalid EC stripe geometry"));
        }
        if pieces.iter().filter(|p| p.is_some()).count() < self.k {
            return Err(Error::Unavailable(
                "insufficient verified EC fragments".into(),
            ));
        }
        if self.k == 1
            && let Some(piece) = &pieces[0]
        {
            return Ok(piece.slice(..source_bytes));
        }
        let mut source = Vec::with_capacity(self.k * self.block);
        if pieces[..self.k].iter().all(Option::is_some) {
            for p in pieces[..self.k].iter().flatten() {
                source.extend_from_slice(p);
            }
        } else {
            if self.p > 0 && self.decoder.is_none() {
                self.decoder = Some(
                    ReedSolomonDecoder::new(self.k, self.p, self.block)
                        .map_err(|e| Error::Invalid(format!("invalid EC layout: {e:?}")))?,
                );
            }
            let decoder = self
                .decoder
                .as_mut()
                .ok_or_else(|| Error::Unavailable("missing data without parity".into()))?;
            for (index, piece) in pieces.iter().enumerate() {
                if let Some(piece) = piece {
                    if index < self.k {
                        decoder.add_original_shard(index, piece)
                    } else {
                        decoder.add_recovery_shard(index - self.k, piece)
                    }
                    .map_err(|e| corrupt(format!("EC decode input: {e:?}")))?;
                }
            }
            let restored = decoder
                .decode()
                .map_err(|e| corrupt(format!("EC decode: {e:?}")))?;
            for (i, piece) in pieces.iter().enumerate().take(self.k) {
                source.extend_from_slice(match piece {
                    Some(p) => p,
                    None => restored
                        .restored_original(i)
                        .ok_or_else(|| corrupt("EC omitted restored data"))?,
                });
            }
        }
        source.truncate(source_bytes);
        Ok(source.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use n1_link::PoolId;
    #[test]
    fn every_parity_tolerant_loss_pattern_preserves_exact_bytes() {
        for (k, p) in [(1, 0), (1, 1), (3, 1), (4, 2)] {
            let pool = PoolLayout {
                id: PoolId([0; 16]),
                generation: 1,
                data_shards: k,
                parity_shards: p,
                block_bytes: 4096,
                placement_version: 1,
                shard_format: n1_link::ShardFormat::Crc32cSha256V2,
            };
            let mut codec = StripeCodec::new(&pool).unwrap();
            for len in [1, 4095, 4096, k as usize * 4096 - 1, k as usize * 4096] {
                let source = (0..len)
                    .map(|i| (i.wrapping_mul(197) % 251) as u8)
                    .collect::<Vec<_>>();
                let shards = codec.encode(&source).unwrap();
                for mask in 0u64..(1 << pool.width()) {
                    let pieces = shards
                        .iter()
                        .enumerate()
                        .map(|(i, p)| {
                            if mask & (1 << i) == 0 {
                                Some(Bytes::copy_from_slice(p))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    let decoded = codec.decode(&pieces, len);
                    if mask.count_ones() <= u32::from(p) {
                        assert_eq!(decoded.unwrap(), source, "{k}+{p}, mask={mask}");
                    } else {
                        assert!(decoded.is_err());
                    }
                }
            }
        }
    }
}
