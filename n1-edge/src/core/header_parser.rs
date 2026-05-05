pub fn parse_range_header(range_header: Option<&str>, file_size: u64) -> Option<(u64, u64)> {
    let range_str = range_header?.strip_prefix("bytes=")?;
    let mut parts = range_str.split('-');
    let start_str = parts.next()?;
    let end_str = parts.next().unwrap_or("");

    if start_str.is_empty() {
        let suffix_len = end_str.parse::<u64>().ok()?;
        if suffix_len == 0 || suffix_len > file_size {
            return None;
        }
        Some((file_size - suffix_len, file_size - 1))
    } else {
        let start = start_str.parse::<u64>().ok()?;
        if start >= file_size {
            return None;
        }
        let end = if end_str.is_empty() {
            file_size - 1
        } else {
            end_str.parse::<u64>().ok()?
        };
        if start > end || end >= file_size {
            return None;
        }
        Some((start, end))
    }
}
