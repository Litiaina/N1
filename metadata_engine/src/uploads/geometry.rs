//! Optional multipart negotiation keeps clients independent of pool topology.
//! A supplied part size is a preference only when the caller opts in.
use crate::server::error::{ApiError, ApiResult};
pub(crate) const MAX_PARTS: u64 = 131072;

pub(super) fn negotiate(
    total: u64,
    preferred: u64,
    maximum: u64,
    stripe: u64,
) -> ApiResult<(u64, u64)> {
    if stripe == 0 || maximum == 0 || (total > 0 && preferred == 0) {
        return Err(ApiError::invalid("invalid multipart size policy"));
    }
    if total == 0 {
        return Ok((preferred.min(maximum), 1));
    }
    let maximum_aligned = maximum / stripe * stripe;
    let part = if maximum_aligned == 0 {
        // Existing arbitrary geometry remains available when the operator's
        // external limit is smaller than one native stripe.
        preferred.clamp(1, maximum)
    } else {
        let minimum = total.div_ceil(MAX_PARTS);
        let minimum_aligned = minimum
            .div_ceil(stripe)
            .checked_mul(stripe)
            .ok_or_else(|| ApiError::invalid("multipart size overflow"))?;
        let desired = if total <= preferred {
            total
                .div_ceil(stripe)
                .checked_mul(stripe)
                .ok_or_else(|| ApiError::invalid("multipart size overflow"))?
        } else {
            (preferred / stripe).max(1) * stripe
        };
        desired.max(minimum_aligned).min(maximum_aligned)
    };
    let count = total.div_ceil(part);
    if count > MAX_PARTS {
        return Err(ApiError(
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            "Object exceeds the configured multipart capacity".into(),
        ));
    }
    Ok((part, count))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_alignment_preserves_small_single_parts_and_scales_to_a_terabyte() {
        let stripe = 3 * 262144;
        assert_eq!(
            negotiate(15 << 30, 8 << 20, 50 << 20, stripe).unwrap().0,
            7_864_320
        );
        let (part, count) = negotiate(1 << 40, 8 << 20, 50 << 20, stripe).unwrap();
        assert!(count <= MAX_PARTS && part.is_multiple_of(stripe));
        assert_eq!(part, 8_650_752);
        assert_eq!(count, (1u64 << 40).div_ceil(part));
        assert_eq!(
            negotiate(4097, 4097, 50 << 20, stripe).unwrap(),
            (stripe, 1)
        );
        assert_eq!(negotiate(0, 1, 50 << 20, stripe).unwrap(), (1, 1));
    }
    #[test]
    fn configured_limits_bound_negotiation_without_changing_file_size_policy() {
        assert_eq!(negotiate(100, 50, 10, 16).unwrap(), (10, 10));
        assert!(negotiate(100 << 40, 8 << 20, 50 << 20, 262144).is_err());
        assert!(negotiate(1, 1, 1, 0).is_err());
        assert!(negotiate(1, 0, 1, 1).is_err());
        let (part, count) = negotiate(1 << 30, 128 << 20, 50 << 20, 262144).unwrap();
        assert!(part <= 50 << 20 && count <= MAX_PARTS);
    }
}
