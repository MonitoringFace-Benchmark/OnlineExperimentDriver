mod data_sources;
mod timestamp_extraction;
mod response_collection;

use clap::Parser;
use std::io::{BufRead, Write};
use std::process::{ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::sleep;
use std::time::{Duration, Instant};
use crate::data_sources::data_file_source::DataFileSource;
use crate::data_sources::data_script_source::DataScriptSource;
use crate::data_sources::data_source_trait::DataSourcer;
use crate::response_collection::{resolve_response_collector, ResponseCollection};
use crate::timestamp_extraction::extract_timestamp_csv;
use crate::timestamp_extraction::extract_timestamp_log;


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Config {
    /// Optional latency marker string
    #[arg(long)]
    latency_marker: Option<String>,

    /// Optional warm up input to prime the binary
    #[arg(long)]
    warm_up_input: Option<String>,

    /// Maximum acceptable latency in milliseconds
    #[arg(long)]
    maximum_latency: Option<f64>,

    /// Accumulative time budget in seconds
    #[arg(long)]
    accumulative_time: Option<f64>,

    /// Input pacing mode: accelerated or real-time
    #[arg(long, value_parser = ["accelerated", "real-time"])]
    mode: String,

    /// Timestamp unit for paced replay: seconds, milliseconds, or microseconds
    #[arg(long, value_parser = ["seconds", "milliseconds", "microseconds"])]
    timestamp_units: Option<String>,

    /// Timestamp format: csv or log
    #[arg(long, value_parser = ["csv", "log"])]
    format: String,

    /// Response collection mode for the child stdout
    #[arg(long, value_parser = ["event-count", "current-timepoint"])]
    response_mode: Option<String>,

    /// Output collection position relative to the response delimiter
    #[arg(long, value_parser = ["before-delimiter", "after-delimiter"], default_value = "after-delimiter")]
    output_collection_mode: String,

    /// Data source type: "file" or "script"
    #[arg(long)]
    data_source_type: String,

    /// Path to data source (file path or script path)
    #[arg(long)]
    data_source: String,

    /// Binary location directory
    #[arg(long)]
    binary_location: String,

    /// Binary name to execute
    #[arg(long)]
    binary_name: String,

    /// Extra arguments to pass to the target binary. Use `--` before the first target arg.
    #[arg(last = true)]
    binary_args: Vec<String>,

    /// Input aggregation number: a constant chunk size like `4` or a comma-separated list like `1,4,4,2`
    #[arg(long)]
    input_aggregation_number: Option<String>,

    /// Input aggregation pattern: a string pattern the last element of a batch starts with
    #[arg(long)]
    input_aggregation_pattern: Option<String>,

    /// r: a pattern to concatenate inputs
    #[arg(long)]
    batch_delimiter: Option<String>,
}

enum BatchingMethod {
    Numbers(Vec<usize>),
    Pattern(String)
}

fn exit_with_code(code: i32, message: &str) -> ! {
    eprintln!("{}", message);
    std::process::exit(code);
}

fn format_input_line(latency_marker: Option<&str>, line: &str) -> String {
    let base = match latency_marker {
        Some(marker) if !marker.is_empty() => format!("{}\n{}", marker, line),
        _ => line.to_string(),
    };

    if base.ends_with('\n') { base } else { format!("{}\n", base) }
}

fn timestamp_to_duration(value: usize, units: &str) -> Duration {
    match units {
        "seconds" => Duration::from_secs(value as u64),
        "milliseconds" => Duration::from_millis(value as u64),
        "microseconds" => Duration::from_micros(value as u64),
        _ => exit_with_code(1, &format!("[ERROR] unknown timestamp_units: {}", units)),
    }
}

fn sleep_until(next_due: Instant) {
    let now = Instant::now();
    if next_due > now { sleep(next_due - now); }
}

/// Paces input against an absolute wall-clock schedule anchored to the first
/// timestamped event, so each event is sent at `base + (ts - ts0)` rather than
/// sleeping a relative gap from "now". This keeps per-step processing time from
/// accumulating as drift: a slow step is absorbed by the next event's slot
/// instead of pushing the whole schedule later.
///
/// `anchor` holds the schedule origin `(base_instant, ts0_duration)`, lazily set
/// on the first timestamped event. In `accelerated` mode, or until a timestamp
/// and units are both known, this is a no-op.
fn pace_before_send(
    mode: &str,
    timestamp_units: Option<&str>,
    anchor: &mut Option<(Instant, Duration)>,
    current_timestamp: Option<usize>,
) {
    if mode != "real-time" {
        return;
    }

    let (Some(current), Some(unit_name)) = (current_timestamp, timestamp_units) else {
        return;
    };
    let current_dur = timestamp_to_duration(current, unit_name);

    match *anchor {
        // First timestamped event defines the origin and is sent immediately.
        None => *anchor = Some((Instant::now(), current_dur)),
        // Sleep until this event's absolute slot. `saturating_sub` makes an
        // out-of-order (earlier) timestamp resolve to a past instant, i.e. send
        // now, rather than underflowing.
        Some((base, first_dur)) => sleep_until(base + current_dur.saturating_sub(first_dur)),
    }
}

fn resolve_timestamp_extractor(format: &str) -> fn(&str) -> Option<usize> {
    match format {
        "csv" => extract_timestamp_csv::extract_ts,
        "log" => extract_timestamp_log::extract_ts,
        _ => exit_with_code(1, &format!("[ERROR] unknown format: {}", format)),
    }
}

fn send_line(stdin: &mut dyn Write, stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>, line: &str, collect_response: &mut dyn ResponseCollection) -> String {
    let mut to_write = line.to_string();
    if !to_write.ends_with('\n') {
        to_write.push('\n');
    }
    if let Err(e) = stdin.write_all(to_write.as_bytes()) {
        exit_with_code(1, &format!("[ERROR] failed to write to persistent child stdin: {}", e));
    }
    if let Err(e) = stdin.flush() {
        exit_with_code(1, &format!("[ERROR] failed to flush persistent child stdin: {}", e));
    }

    collect_response.consume_until(stdout_lines);
    String::new()
}

fn read_until_delimiter(
    collect_response: &mut dyn ResponseCollection,
    stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
) -> String {
    collect_response.read_until(stdout_lines)
}

fn read_since_delimiter(
    collect_response: &mut dyn ResponseCollection,
    stdout_lines: &mut dyn Iterator<Item = std::io::Result<String>>,
) -> String {
    collect_response.read_since(stdout_lines)
}

fn resolve_output_reader(
    output_collection_mode: &str,
) -> fn(&mut dyn ResponseCollection, &mut dyn Iterator<Item = std::io::Result<String>>) -> String {
    match output_collection_mode {
        "before-delimiter" => read_until_delimiter,
        "after-delimiter" => read_since_delimiter,
        _ => exit_with_code(
            1,
            &format!(
                "[ERROR] unknown output_collection_mode: {}",
                output_collection_mode
            ),
        ),
    }
}

fn parse_input_aggregation(value: Option<&str>) -> Vec<usize> {
    let Some(raw) = value else {
        return vec![1];
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return vec![1];
    }

    if let Ok(single) = trimmed.parse::<usize>() {
        return vec![single.max(1)];
    }

    let mut sizes = Vec::new();
    for part in trimmed.split(',') {
        let parsed = part.trim().parse::<usize>().unwrap_or_else(|_| exit_with_code(1, &format!("[ERROR] invalid input_aggregation value: {}", raw)));
        sizes.push(parsed.max(1));
    }

    if sizes.is_empty() {
        vec![1]
    } else {
        sizes
    }
}

/// Which time budget produced a read deadline, so a timeout reports the right
/// message and exit code.
#[derive(Clone, Copy)]
enum Budget {
    PerStep,
    Accumulative,
}

/// Owns the child's stdout on a background thread and forwards each line over a
/// channel. This lets the main loop read responses with a wall-clock deadline
/// (`recv_timeout`) instead of blocking forever on a hung monitor.
struct OutputChannel {
    rx: Receiver<std::io::Result<String>>,
}

impl OutputChannel {
    fn spawn(stdout: ChildStdout) -> Self {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                // Stop once the main thread drops the receiver.
                if tx.send(line).is_err() {
                    break;
                }
            }
            // Dropping `tx` disconnects the channel, which the reader sees as EOF.
        });
        OutputChannel { rx }
    }

    /// View the incoming lines as an iterator bounded by `deadline`. On timeout
    /// it sets `*timed_out` and ends iteration so the caller can react.
    fn lines<'a>(&'a self, deadline: Option<Instant>, timed_out: &'a mut bool) -> DeadlineLines<'a> {
        DeadlineLines { rx: &self.rx, deadline, timed_out }
    }
}

struct DeadlineLines<'a> {
    rx: &'a Receiver<std::io::Result<String>>,
    deadline: Option<Instant>,
    timed_out: &'a mut bool,
}

impl<'a> Iterator for DeadlineLines<'a> {
    type Item = std::io::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.deadline {
            None => self.rx.recv().ok(),
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                match self.rx.recv_timeout(remaining) {
                    Ok(item) => Some(item),
                    Err(RecvTimeoutError::Timeout) => {
                        *self.timed_out = true;
                        None
                    }
                    Err(RecvTimeoutError::Disconnected) => None,
                }
            }
        }
    }
}

/// Deadline for a step's response read: the earlier of the per-step latency
/// budget and the remaining accumulative budget. `None` when neither limit is
/// configured, in which case the read blocks indefinitely (previous behavior).
fn read_deadline(
    start: Instant,
    maximum_latency_ms: Option<f64>,
    accumulative_time_secs: Option<f64>,
    accumulative_elapsed: f64,
) -> Option<(Instant, Budget)> {
    let per_step = maximum_latency_ms
        .map(|ms| (start + Duration::from_secs_f64(ms.max(0.0) / 1000.0), Budget::PerStep));
    let accumulative = accumulative_time_secs.map(|secs| {
        let remaining = (secs - accumulative_elapsed).max(0.0);
        (start + Duration::from_secs_f64(remaining), Budget::Accumulative)
    });

    match (per_step, accumulative) {
        (Some(p), Some(a)) => Some(if p.0 <= a.0 { p } else { a }),
        (Some(p), None) => Some(p),
        (None, Some(a)) => Some(a),
        (None, None) => None,
    }
}

/// Print the stats block and exit with the code for the exceeded budget.
fn report_timeout(
    budget: Budget,
    elapsed: Duration,
    accumulative_elapsed: f64,
    input_count: usize,
    maximum_latency_ms: Option<f64>,
    accumulative_time_secs: Option<f64>,
) -> ! {
    println!("[Accumulative Elapsed] {:.6} s", accumulative_elapsed);
    println!("[Total Count] {}", input_count);
    match budget {
        Budget::PerStep => {
            println!("[Error] maximum latency exceeded");
            exit_with_code(250, &format!(
                "Fatal: maximum latency exceeded: {:.3} ms > {:.3} ms",
                elapsed.as_secs_f64() * 1000.0,
                maximum_latency_ms.unwrap_or(0.0)
            ));
        }
        Budget::Accumulative => {
            println!("[Error] accumulative latency exceeded");
            exit_with_code(200, &format!(
                "Fatal: accumulative latency exceeded: {:.6} s > {:.6} s",
                accumulative_elapsed,
                accumulative_time_secs.unwrap_or(0.0)
            ));
        }
    }
}

fn next_batch_size(sizes: &[usize], batch_index: usize) -> usize {
    if sizes.is_empty() {
        return 1;
    }
    if batch_index < sizes.len() {
        sizes[batch_index]
    } else {
        *sizes.last().unwrap()
    }
}

fn resolve_batcher<S: DataSourcer<Item = String>>(
    batching_method: BatchingMethod,
) -> Box<dyn FnMut(&mut S) -> Option<Vec<String>>> {
    match batching_method {
        BatchingMethod::Numbers(batch_sizes) => {
            let mut batch_index = 0usize;
            Box::new(move |src: &mut S| {
                let mut batch = vec![src.iterate()?];

                let batch_size = next_batch_size(&batch_sizes, batch_index);
                batch_index += 1;

                while batch.len() < batch_size {
                    match src.iterate() {
                        Some(next_input) => batch.push(next_input),
                        None => break,
                    }
                }
                Some(batch)
            })
        }
        BatchingMethod::Pattern(pattern) => {
            Box::new(move |src: &mut S| {
                let mut batch = vec![src.iterate()?];
                
                while !batch.last().unwrap().starts_with(&pattern) {
                    match src.iterate() {
                        Some(next_input) => batch.push(next_input),
                        None => break,
                    }
                }
                Some(batch)
            })
        }
    }
}

fn run_with_source<S: DataSourcer<Item = String>>(
    mut src: S,
    binary_path: &str,
    binary_args: &[String],
    warm_up_input: Option<&str>,
    latency_marker: Option<&str>,
    maximum_latency_ms: Option<f64>,
    accumulative_time_secs: Option<f64>,
    mode: &str,
    timestamp_units: Option<&str>,
    extract_timestamp: fn(&str) -> Option<usize>,
    batch_method: BatchingMethod,
    batch_delimiter: &str,
    output_collection_mode: &str,
    mut collect_response: Box<dyn ResponseCollection>,
) {
    if !src.start() {
        exit_with_code(1, "[ERROR] data source failed to start");
    }

    let mut child = match Command::new(binary_path)
        .args(binary_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            exit_with_code(1, &format!("[ERROR] failed to spawn persistent {}: {}", binary_path, e));
        }
    };

    let mut stdin = child.stdin.take().expect("[ERROR] Child stdin not piped");
    let output = OutputChannel::spawn(child.stdout.take().expect("[ERROR] Child stdout not piped"));
    let mut accumulative_elapsed = 0.0_f64;
    let mut pace_anchor: Option<(Instant, Duration)> = None;
    let mut input_count = 0usize;
    let output_reader = resolve_output_reader(output_collection_mode);
    let mut next_batch = resolve_batcher(batch_method);

    if let Some(warm) = warm_up_input {
        let mut warm_timed_out = false;
        let mut lines = output.lines(None, &mut warm_timed_out);
        let _ = send_line(&mut stdin, &mut lines, warm, &mut *collect_response);
    }

    while let Some(batch) = next_batch(&mut src) {
        let joined_input = batch.join(batch_delimiter);
        let to_write = format_input_line(latency_marker, &joined_input);
        pace_before_send(mode, timestamp_units, &mut pace_anchor, extract_timestamp(&batch[0]));

        let start = Instant::now();
        if let Err(e) = stdin.write_all(to_write.as_bytes()) {
            exit_with_code(1, &format!("[ERROR] failed to write to persistent child stdin: {}", e));
        }
        input_count += batch.len();
        if let Err(e) = stdin.flush() {
            exit_with_code(1, &format!("[ERROR] failed to flush persistent child stdin: {}", e));
        }

        println!("[Input  ] {}", joined_input);

        // Bound the response read by the tighter of the per-step latency budget
        // and the remaining accumulative budget, enforced inside the channel
        // read so a hung monitor can't block the driver indefinitely.
        let deadline = read_deadline(start, maximum_latency_ms, accumulative_time_secs, accumulative_elapsed);

        let mut timed_out = false;
        let response = {
            let mut lines = output.lines(deadline.map(|(at, _)| at), &mut timed_out);
            output_reader(&mut *collect_response, &mut lines)
        };
        let elapsed = start.elapsed();

        if timed_out {
            let _ = child.kill();
            report_timeout(
                deadline.expect("a timeout implies a deadline was set").1,
                elapsed,
                accumulative_elapsed + elapsed.as_secs_f64(),
                input_count,
                maximum_latency_ms,
                accumulative_time_secs,
            );
        }

        // A channel close that wasn't a timeout means the monitor exited mid-run.
        if let Ok(Some(status)) = child.try_wait() {
            exit_with_code(1, &format!("[ERROR] persistent child exited unexpectedly: {}", status));
        }

        accumulative_elapsed += elapsed.as_secs_f64();
        if !response.is_empty() {
            println!("[Output ]\n{}", response);
        }
        println!("[Processed] {}", input_count);
        println!("[Elapsed] {} ns\n", elapsed.as_nanos());
    }

    println!("[Accumulative Elapsed] {:.6} s", accumulative_elapsed);
    println!("[Total Count] {}", input_count);

    drop(stdin);
    let _ = child.wait();
}

fn main() {
    let cfg = Config::parse();

    let binary_path = format!("{}/{}", cfg.binary_location.trim_end_matches('/'), cfg.binary_name);
    let extract_timestamp = resolve_timestamp_extractor(&cfg.format);
    let collect_response = resolve_response_collector(cfg.response_mode.as_deref());

    let batch_method = if cfg.input_aggregation_pattern.is_some() {
        BatchingMethod::Pattern(cfg.input_aggregation_pattern.unwrap())
    } else {
        BatchingMethod::Numbers(parse_input_aggregation(cfg.input_aggregation_number.as_deref()))
    };

    if cfg.mode == "real-time" && cfg.timestamp_units.is_none() {
        exit_with_code(1, "[ERROR] --timestamp-units is required when --mode real-time is set");
    }

    let batch_delimiter_raw = cfg.batch_delimiter.unwrap_or("#".to_string());
    let batch_delimiter = batch_delimiter_raw.as_str();

    match cfg.data_source_type.as_str() {
        "file" => {
            run_with_source(
                DataFileSource::new(cfg.data_source),
                &binary_path,
                &cfg.binary_args,
                cfg.warm_up_input.as_deref(),
                cfg.latency_marker.as_deref(),
                cfg.maximum_latency,
                cfg.accumulative_time,
                &cfg.mode,
                cfg.timestamp_units.as_deref(),
                extract_timestamp,
                batch_method,
                batch_delimiter,
                &cfg.output_collection_mode,
                collect_response
            );
        }
        "script" => {
            run_with_source(
                DataScriptSource::new("python3", [cfg.data_source.clone()]),
                &binary_path,
                &cfg.binary_args,
                cfg.warm_up_input.as_deref(),
                cfg.latency_marker.as_deref(),
                cfg.maximum_latency,
                cfg.accumulative_time,
                &cfg.mode,
                cfg.timestamp_units.as_deref(),
                extract_timestamp,
                batch_method,
                batch_delimiter,
                &cfg.output_collection_mode,
                collect_response
            );
        }
        _ => {
            exit_with_code(1, &format!("[ERROR] unknown data_source_type: {}", cfg.data_source_type));
        }
    }
}

#[cfg(test)]
mod pacing_tests {
    use super::pace_before_send;
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    /// Absolute scheduling must absorb per-step processing: events at 0/200/400 ms
    /// with 80 ms of work between them should still finish ~400 ms after the
    /// anchor (the timestamp span), not ~560 ms as relative gap-sleeping would.
    #[test]
    fn absolute_schedule_absorbs_processing_delay() {
        let units = Some("milliseconds");
        let mut anchor: Option<(Instant, Duration)> = None;

        let start = Instant::now();
        pace_before_send("real-time", units, &mut anchor, Some(0)); // anchor, no sleep
        sleep(Duration::from_millis(80));
        pace_before_send("real-time", units, &mut anchor, Some(200));
        sleep(Duration::from_millis(80));
        pace_before_send("real-time", units, &mut anchor, Some(400));
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(390), "paced too short: {:?}", elapsed);
        assert!(elapsed < Duration::from_millis(480), "drifted, not absorbed: {:?}", elapsed);
    }

    /// An out-of-order timestamp below the anchor must not panic or oversleep; it
    /// resolves to a past slot and sends immediately.
    #[test]
    fn out_of_order_timestamp_sends_immediately() {
        let units = Some("milliseconds");
        let mut anchor: Option<(Instant, Duration)> = None;

        pace_before_send("real-time", units, &mut anchor, Some(500)); // anchor at 500
        let start = Instant::now();
        pace_before_send("real-time", units, &mut anchor, Some(100)); // earlier -> now
        assert!(start.elapsed() < Duration::from_millis(20));
    }

    /// Accelerated mode never sleeps.
    #[test]
    fn accelerated_mode_does_not_sleep() {
        let mut anchor: Option<(Instant, Duration)> = None;
        let start = Instant::now();
        for ts in [0usize, 1000, 5000] {
            pace_before_send("accelerated", Some("seconds"), &mut anchor, Some(ts));
        }
        assert!(start.elapsed() < Duration::from_millis(50));
    }
}
