use crate::exit_with_code;

pub trait ResponseCollection {
    fn read_until(
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
            Some(Err(e)) => exit_with_code(1, &format!("Fatal: error reading response from persistent child: {}", e)),
            None => exit_with_code(1, "Fatal: persistent child stdout closed unexpectedly"),
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
            Some(Err(e)) => exit_with_code(1, &format!("Fatal: error reading response from persistent child: {}", e)),
            None => exit_with_code(1, "Fatal: persistent child stdout closed unexpectedly"),
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
        "current-timepoint" => Box::new(CurrentTimepointCollection::new("Process step")),
        _ => exit_with_code(1, &format!("Fatal: unknown response_mode: {}", mode.unwrap_or("event-count"))),
    }
}