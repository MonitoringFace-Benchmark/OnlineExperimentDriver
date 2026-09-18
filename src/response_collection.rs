use crate::exit_with_code;

pub trait ResponseCollection {
    fn read_until(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    ) -> String;

    fn read_since(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    ) -> String;

    fn consume_until(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    );
}

fn contains_delimiter(line: &str, delimiter: &str) -> bool {
    line.to_ascii_lowercase().contains(&delimiter.to_ascii_lowercase())
}

fn read_lines_until(
    stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    delimiter: &str,
) -> String {
    let mut result = Vec::new();

    loop {
        match stdout_lines.next() {
            Some(Ok(line)) => {
                if line.trim().is_empty() { continue; }

                if contains_delimiter(&line, delimiter) { break; }

                result.push(line);
            }
            Some(Err(e)) => exit_with_code(1, &format!("[ERROR] error reading response from persistent child: {}", e)),
            // Stream ended (timeout or EOF); hand back what we have and let the
            // caller decide (the driver checks for a timeout / dead child).
            None => break,
        }
    }

    result.join("\n")
}

fn consume_lines_until(
    stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    delimiter: &str,
) {
    loop {
        match stdout_lines.next() {
            Some(Ok(line)) => {
                if line.trim().is_empty() { continue; }

                if contains_delimiter(&line, delimiter) { return; }
            }
            Some(Err(e)) => exit_with_code(1, &format!("[ERROR] error reading response from persistent child: {}", e)),
            // Stream ended before the delimiter; let the caller handle it.
            None => return,
        }
    }
}

fn read_line_since(
    stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    delimiter: &str,
) -> String {
    consume_lines_until(stdout_lines, delimiter);

    loop {
        match stdout_lines.next() {
            Some(Ok(line)) => {
                if line.trim().is_empty() {
                    continue;
                }

                return line;
            }
            Some(Err(e)) => {
                exit_with_code(
                    1,
                    &format!("[ERROR] error reading response from persistent child: {}", e),
                )
            }
            // Stream ended before a line arrived; let the caller handle it.
            None => return String::new(),
        }
    }
}

pub struct EventCountResponseCollection {
    delimiter: String,
}

impl EventCountResponseCollection {
    pub fn new(delimiter: impl Into<String>) -> Self {
        Self { delimiter: delimiter.into() }
    }
}

impl ResponseCollection for EventCountResponseCollection {
    fn read_until(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    ) -> String {
        read_lines_until(stdout_lines, &self.delimiter)
    }

    fn read_since(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    ) -> String {
        read_line_since(stdout_lines, &self.delimiter)
    }

    fn consume_until(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    ) {
        consume_lines_until(stdout_lines, &self.delimiter)
    }
}

pub struct CurrentTimepointCollection {
    delimiter: String,
}

impl CurrentTimepointCollection {
    pub fn new(delimiter: impl Into<String>) -> Self {
        Self { delimiter: delimiter.into() }
    }
}

impl ResponseCollection for CurrentTimepointCollection {
    fn read_until(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    ) -> String {
        read_lines_until(stdout_lines, &self.delimiter)
    }

    fn read_since(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    ) -> String {
        read_line_since(stdout_lines, &self.delimiter)
    }

    fn consume_until(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    ) {
        consume_lines_until(stdout_lines, &self.delimiter)
    }
}

pub fn resolve_response_collector(mode: Option<&str>) -> Box<dyn ResponseCollection> {
    match mode.unwrap_or("event-count") {
        "event-count" => Box::new(EventCountResponseCollection::new("event count")),
        "current-timepoint" => Box::new(CurrentTimepointCollection::new("At time point")),
        _ => exit_with_code(1, &format!("[ERROR] unknown response_mode: {}", mode.unwrap_or("event-count"))),
    }
}

pub fn response_delimiter(mode: Option<&str>) -> String {
    match mode.unwrap_or("event-count") {
        "event-count" => "event count".to_string(),
        "current-timepoint" => "At time point".to_string(),
        _ => exit_with_code(1, &format!("[ERROR] unknown response_mode: {}", mode.unwrap_or("event-count"))),
    }
}

#[derive(Clone, Copy)]
pub enum OutputMode {
    BeforeDelimiter,
    AfterDelimiter,
}

pub fn resolve_output_mode(output_collection_mode: &str) -> OutputMode {
    match output_collection_mode {
        "before-delimiter" => OutputMode::BeforeDelimiter,
        "after-delimiter" => OutputMode::AfterDelimiter,
        _ => exit_with_code(1, &format!("[ERROR] unknown output_collection_mode: {}", output_collection_mode)),
    }
}

/// Event-driven counterpart of the blocking collectors, for the processor
/// chain's unified event loop: lines are pushed in as they arrive and each
/// delimiter occurrence completes one response unit. `responses` is the
/// cumulative count the quiescence rule compares against the chain's
/// delivered counter.
pub struct ResponseTracker {
    delimiter: String,
    mode: OutputMode,
    raw: bool,
    current: Vec<String>,
    pending_after: bool,
    pub responses: u64,
    completed: Vec<String>,
}

impl ResponseTracker {
    pub fn new(delimiter: String, mode: OutputMode) -> Self {
        ResponseTracker {
            delimiter,
            mode,
            raw: false,
            current: Vec::new(),
            pending_after: false,
            responses: 0,
            completed: Vec::new(),
        }
    }

    /// Chain-accounting harvesting: every non-empty tool line is its own
    /// completed response, no delimiter protocol. Used when the tool gives
    /// no per-line acknowledgment (e.g. MonPoly is silent on empty
    /// time-points), so round closure runs on chain counters alone.
    pub fn new_raw() -> Self {
        let mut tracker = Self::new(String::new(), OutputMode::BeforeDelimiter);
        tracker.raw = true;
        tracker
    }

    pub fn on_line(&mut self, line: &str) {
        if line.trim().is_empty() {
            return;
        }
        if self.raw {
            self.responses += 1;
            self.completed.push(line.to_string());
            return;
        }
        let is_delim = contains_delimiter(line, &self.delimiter);
        match self.mode {
            OutputMode::BeforeDelimiter => {
                if is_delim {
                    self.responses += 1;
                    self.completed.push(self.current.join("\n"));
                    self.current.clear();
                } else {
                    self.current.push(line.to_string());
                }
            }
            OutputMode::AfterDelimiter => {
                if is_delim {
                    self.responses += 1;
                    self.completed.push(String::new());
                    self.pending_after = true;
                } else if self.pending_after {
                    if let Some(last) = self.completed.last_mut() {
                        *last = line.to_string();
                    }
                    self.pending_after = false;
                }
            }
        }
    }

    /// Hands out the responses completed since the last drain, non-empty only.
    pub fn drain(&mut self) -> Vec<String> {
        self.completed.drain(..).filter(|s| !s.is_empty()).collect()
    }
}

#[cfg(test)]
mod tracker_tests {
    use super::*;

    #[test]
    fn before_delimiter_counts_and_collects() {
        let mut t = ResponseTracker::new("event count".to_string(), OutputMode::BeforeDelimiter);
        t.on_line("verdict a");
        assert_eq!(t.responses, 0);
        t.on_line("Event count: 1");
        assert_eq!(t.responses, 1);
        t.on_line("");
        t.on_line("event count 2");
        assert_eq!(t.responses, 2);
        assert_eq!(t.drain(), vec!["verdict a".to_string()]);
        assert!(t.drain().is_empty());
    }

    #[test]
    fn after_delimiter_takes_next_line() {
        let mut t = ResponseTracker::new("At time point".to_string(), OutputMode::AfterDelimiter);
        t.on_line("at time point 3:");
        assert_eq!(t.responses, 1);
        t.on_line("verdict b");
        t.on_line("At time point 4:");
        assert_eq!(t.responses, 2);
        assert_eq!(t.drain(), vec!["verdict b".to_string()]);
    }
}