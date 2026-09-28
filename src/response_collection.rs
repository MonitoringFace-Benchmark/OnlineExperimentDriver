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

    /// Protocol lines that carry no tool output, dropped from the EOF tail.
    fn is_marker(&self, _line: &str) -> bool {
        false
    }
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

/// MonPoly with `-verbose` brackets every input step: `At time point k:` opens
/// it and `Process step` closes it. In between come the result lines of every
/// time-point the step decided, which is more than one whenever a future
/// window closes for several earlier time-points at once. Only the formula
/// header precedes the first step; at EOF MonPoly opens one more step for its
/// final flush, so every result line lies inside a bracket.
const STEP_OPEN: &str = "at time point";
const STEP_CLOSE: &str = "process step";

fn read_step(stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>) -> String {
    consume_lines_until(stdout_lines, STEP_OPEN);
    read_lines_until(stdout_lines, STEP_CLOSE)
}

pub struct ProcessStepCollection;

impl ResponseCollection for ProcessStepCollection {
    fn read_until(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    ) -> String {
        read_step(stdout_lines)
    }

    /// The step is bracketed on both sides, so the collection position
    /// relative to a delimiter does not apply.
    fn read_since(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    ) -> String {
        read_step(stdout_lines)
    }

    fn consume_until(
        &mut self,
        stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
    ) {
        consume_lines_until(stdout_lines, STEP_CLOSE)
    }

    fn is_marker(&self, line: &str) -> bool {
        contains_delimiter(line, STEP_OPEN) || contains_delimiter(line, STEP_CLOSE)
    }
}

pub fn resolve_response_collector(mode: Option<&str>) -> Box<dyn ResponseCollection> {
    match mode.unwrap_or("event-count") {
        "event-count" => Box::new(EventCountResponseCollection::new("event count")),
        "current-timepoint" => Box::new(CurrentTimepointCollection::new("At time point")),
        "process-step" => Box::new(ProcessStepCollection),
        _ => exit_with_code(1, &format!("[ERROR] unknown response_mode: {}", mode.unwrap_or("event-count"))),
    }
}

pub fn response_delimiter(mode: Option<&str>) -> String {
    match mode.unwrap_or("event-count") {
        "event-count" => "event count".to_string(),
        "current-timepoint" => "At time point".to_string(),
        "process-step" => STEP_CLOSE.to_string(),
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
    open: Option<String>,
    inside: bool,
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
            open: None,
            inside: false,
            current: Vec::new(),
            pending_after: false,
            responses: 0,
            completed: Vec::new(),
        }
    }

    /// The tracker for a response mode. `process-step` collects each bracketed
    /// MonPoly step whole, whatever the collection position.
    pub fn for_mode(response_mode: Option<&str>, mode: OutputMode) -> Self {
        if response_mode == Some("process-step") {
            let mut tracker = Self::new(STEP_CLOSE.to_string(), OutputMode::BeforeDelimiter);
            tracker.open = Some(STEP_OPEN.to_string());
            tracker
        } else {
            Self::new(response_delimiter(response_mode), mode)
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
        if let Some(open) = &self.open {
            if contains_delimiter(line, open) {
                self.inside = true;
            } else if contains_delimiter(line, &self.delimiter) {
                self.responses += 1;
                self.completed.push(self.current.join("\n"));
                self.current.clear();
                self.inside = false;
            } else if self.inside {
                self.current.push(line.to_string());
            }
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

    /// Completes whatever is buffered without a closing delimiter: the tool's
    /// EOF tail never gets one, and would otherwise be dropped.
    pub fn flush_current(&mut self) {
        if !self.current.is_empty() {
            self.responses += 1;
            self.completed.push(self.current.join("\n"));
            self.current.clear();
        }
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

    fn lines(text: &str) -> Vec<std::io::Result<String>> {
        text.lines().map(|l| Ok(l.to_string())).collect()
    }

    const MONPOLY_VERBOSE: &str = "The analyzed formula is:\n\
        \x20 insert(x,\"db2\",y,data)\n\
        The sequence of free variables is: (x,y,data)\n\
        At time point 6685:\n\
        @1282872059 (time point 6625): ()\n\
        Process step\n\
        At time point 6686:\n\
        @1282872060 (time point 6626): ()\n\
        @1282872061 (time point 6627): ()\n\
        @1282872062 (time point 6628): ()\n\
        @1282872063 (time point 6629): ((\"script\",\"86\",\"443377957\"))\n\
        Process step\n\
        At time point 6687:\n\
        Process step\n";

    #[test]
    fn process_step_reads_every_decided_time_point_of_a_step() {
        let mut c = ProcessStepCollection;
        let mut it = lines(MONPOLY_VERBOSE).into_iter();
        assert_eq!(c.read_since(&mut it), "@1282872059 (time point 6625): ()");
        assert_eq!(
            c.read_since(&mut it),
            "@1282872060 (time point 6626): ()\n\
             @1282872061 (time point 6627): ()\n\
             @1282872062 (time point 6628): ()\n\
             @1282872063 (time point 6629): ((\"script\",\"86\",\"443377957\"))"
        );
        assert_eq!(c.read_until(&mut it), "");
        assert_eq!(c.read_until(&mut it), "");
    }

    #[test]
    fn process_step_tracker_counts_steps_and_skips_the_header() {
        let mut t = ResponseTracker::for_mode(Some("process-step"), OutputMode::AfterDelimiter);
        for l in MONPOLY_VERBOSE.lines() {
            t.on_line(l);
        }
        assert_eq!(t.responses, 3);
        let got = t.drain();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], "@1282872059 (time point 6625): ()");
        assert!(got[1].contains("(time point 6629): ((\"script\",\"86\",\"443377957\"))"));
        assert_eq!(got[1].lines().count(), 4);
    }

    #[test]
    fn current_timepoint_keeps_only_one_line_per_step() {
        let mut c = CurrentTimepointCollection::new("At time point");
        let mut it = lines(MONPOLY_VERBOSE).into_iter();
        c.read_since(&mut it);
        assert_eq!(c.read_since(&mut it), "@1282872060 (time point 6626): ()");
        c.read_since(&mut it);
        assert!(it.next().is_none());
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