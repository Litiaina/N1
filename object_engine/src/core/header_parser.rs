pub fn parse_range_header(range_header: Option<&str>, file_size: u64) -> Option<(u64, u64)> {
    let (unit, range_str) = range_header?.split_once('=')?;
    if !unit.eq_ignore_ascii_case("bytes") || file_size == 0 {
        return None;
    }
    let (start_str, end_str) = range_str.trim().split_once('-')?;

    if start_str.is_empty() {
        let suffix_len = decimal(end_str)?;
        if suffix_len == 0 {
            return None;
        }
        Some((file_size.saturating_sub(suffix_len), file_size - 1))
    } else {
        let start = decimal(start_str)?;
        if start >= file_size {
            return None;
        }
        let end = if end_str.is_empty() {
            file_size - 1
        } else {
            decimal(end_str)?.min(file_size - 1)
        };
        if start > end {
            return None;
        }
        Some((start, end))
    }
}

// RFC 9110 allows arbitrarily large decimal positions. Saturation preserves
// their ordering against any addressable object without accepting signed input.
fn decimal(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(value.bytes().fold(0_u64, |n, byte| {
        n.saturating_mul(10).saturating_add((byte - b'0') as u64)
    }))
}
