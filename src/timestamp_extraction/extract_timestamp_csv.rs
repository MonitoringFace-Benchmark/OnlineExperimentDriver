pub fn extract_ts(s: &str) -> Option<usize> {
    s.split(',')
        .map(str::trim)
        .find_map(|field| field.strip_prefix("ts=")?.trim().parse::<usize>().ok())
}

