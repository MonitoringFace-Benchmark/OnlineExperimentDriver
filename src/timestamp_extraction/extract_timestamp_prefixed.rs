//! `--format prefixed`: every line is `<due>\t<line>`, where `<due>` is the
//! replay time recorded for the line (in --timestamp-units), for example a
//! claimed trace whose lines carry their receipt times. The prefix only paces
//! the line; it is stripped before the line is batched, sent or logged.

/// The due time of a prefixed line.
pub fn extract_ts(line: &str) -> Option<usize> {
    let (head, _) = line.split_once('\t')?;
    if head.is_empty() || !head.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    head.parse().ok()
}

/// The line without its due-time prefix; any other line unchanged. No csv or
/// log line starts with digits and a tab, so this is safe for every format.
pub fn strip(line: &str) -> &str {
    match line.split_once('\t') {
        Some((head, rest)) if !head.is_empty() && head.bytes().all(|b| b.is_ascii_digit()) => rest,
        _ => line,
    }
}

#[cfg(test)]
mod tests {
    use super::{extract_ts, strip};

    #[test]
    fn prefix_is_parsed_and_stripped() {
        let line = "1789247517830\tedit, tp=0, ts=1789247517, user=\"a\"";
        assert_eq!(extract_ts(line), Some(1789247517830));
        assert_eq!(strip(line), "edit, tp=0, ts=1789247517, user=\"a\"");
        assert_eq!(strip("1500\t>ELAPSED 3 @ 3<"), ">ELAPSED 3 @ 3<");
    }

    #[test]
    fn unprefixed_lines_pass_through() {
        for line in ["edit, tp=0, ts=5", "@5 p(1)", "12'edit, tp=0, ts=5", "x1\tnot a due", "\tempty head"] {
            assert_eq!(extract_ts(line), None, "{line}");
            assert_eq!(strip(line), line, "{line}");
        }
    }
}
