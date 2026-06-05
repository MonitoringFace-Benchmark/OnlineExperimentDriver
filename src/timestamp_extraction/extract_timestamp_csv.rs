pub fn extract_ts(s: &str) -> Option<usize> {
    s.split(',')
        .map(str::trim)
        .find_map(|field| {
            let (key, val) = field.split_once('=')?;
            if key.trim() == "ts" { val.trim().parse::<usize>().ok() } else { None }
        })
}

