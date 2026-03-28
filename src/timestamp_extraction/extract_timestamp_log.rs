pub fn extract_ts(s: &str) -> Option<usize> {
    let trimmed = s.trim_start();
    if !trimmed.starts_with('@') {
        return None;
    }

    let rest = trimmed.strip_prefix('@')?.trim_start();
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }

    digits.parse::<usize>().ok()
}
