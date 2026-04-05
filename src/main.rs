mod data_sources;
mod timestamp_extraction;
mod response_collection;

use clap::Parser;
use std::io::{BufRead, Write};
use std::process::{Command, Stdio};
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

    /// Input aggregation: a constant chunk size like `4` or a comma-separated list like `1,4,4,2`
    #[arg(long)]
    input_aggregation: Option<String>,

    #[arg(long)]
    batch_delimiter: Option<String>,
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

fn pace_before_send(mode: &str, timestamp_units: Option<&str>, previous_timestamp: &mut Option<usize>, current_timestamp: Option<usize>) {
    if mode != "real-time" { return; }

    let Some(current) = current_timestamp else { return; };

    if let (Some(previous), Some(unit_name)) = (*previous_timestamp, timestamp_units) {
        let current_dur = timestamp_to_duration(current, unit_name);
        let previous_dur = timestamp_to_duration(previous, unit_name);
        if current_dur > previous_dur {
            sleep_until(Instant::now() + (current_dur - previous_dur));
        }
        *previous_timestamp = Some(previous.max(current));
        return;
    }

    *previous_timestamp = Some(current);
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
    batch_sizes: Vec<usize>,
    batch_delimiter: &str,
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
    let mut stdout_lines = std::io::BufReader::new(child.stdout.take().expect("Child stdout not piped")).lines();
    let mut accumulative_elapsed = 0.0_f64;
    let mut previous_timestamp: Option<usize> = None;
    let mut batch_index = 0usize;
    let mut input_count = 0usize;

    if let Some(warm) = warm_up_input {
        let _ = send_line(&mut stdin, &mut stdout_lines, warm, &mut *collect_response);
    }

    while let Some(input) = src.iterate() {
        let mut batch = vec![input];
        let batch_size = next_batch_size(&batch_sizes, batch_index);
        batch_index += 1;

        while batch.len() < batch_size {
            match src.iterate() {
                Some(next_input) => { batch.push(next_input) },
                None => break,
            }
        }

        let joined_input = batch.join(batch_delimiter);
        let to_write = format_input_line(latency_marker, &joined_input);
        pace_before_send(mode, timestamp_units, &mut previous_timestamp, extract_timestamp(&batch[0]));

        let start = Instant::now();
        if let Err(e) = stdin.write_all(to_write.as_bytes()) {
            exit_with_code(1, &format!("[ERROR] failed to write to persistent child stdin: {}", e));
        }
        input_count += batch.len();
        if let Err(e) = stdin.flush() {
            exit_with_code(1, &format!("[ERROR] failed to flush persistent child stdin: {}", e));
        }

        let elapsed = start.elapsed();
        let elapsed_ns = elapsed.as_nanos();
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        accumulative_elapsed += elapsed.as_secs_f64();
        println!("[Input  ] {}", joined_input);
        let suppress_empty_output = collect_response.read_until(&mut stdout_lines);
        if suppress_empty_output != "".to_string() {
            println!("[Output ]\n{}", suppress_empty_output);
        }
        println!("[Processed] {}", input_count);
        println!("[Elapsed] {} ns\n", elapsed_ns);

        if let Some(max_ms) = maximum_latency_ms {
            if elapsed_ms > max_ms {
                println!("[Accumulative Elapsed] {:.6} s", accumulative_elapsed);
                println!("[Total Count] {}", input_count);
                println!("[Error] maximum latency exceeded");
                exit_with_code(250, &format!(
                    "Fatal: maximum latency exceeded: {:.3} ms > {:.3} ms",
                    elapsed_ms, max_ms
                ));
            }
        }

        if let Some(max_accumulative_secs) = accumulative_time_secs {
            if accumulative_elapsed > max_accumulative_secs {
                println!("[Accumulative Elapsed] {:.6} s", accumulative_elapsed);
                println!("[Total Count] {}", input_count);
                println!("[Error] accumulative latency exceeded");
                exit_with_code(200, &format!(
                    "Fatal: accumulative latency exceeded: {:.6} s > {:.6} s",
                    accumulative_elapsed, max_accumulative_secs
                ));
            }
        }
    }

    println!("[Accumulative Elapsed] {:.6} s", accumulative_elapsed);
    println!("[Total Count] {}", input_count);

    drop(stdin);
    let _ = child.wait();
}

fn main() {
    let cfg = Config::parse();
    //println!("Config: {cfg:#?}");

    let binary_path = format!("{}/{}", cfg.binary_location.trim_end_matches('/'), cfg.binary_name);
    let extract_timestamp = resolve_timestamp_extractor(&cfg.format);
    let collect_response = resolve_response_collector(cfg.response_mode.as_deref());
    let batch_sizes = parse_input_aggregation(cfg.input_aggregation.as_deref());
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
                batch_sizes.clone(),
                batch_delimiter,
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
                batch_sizes,
                batch_delimiter,
                collect_response
            );
        }
        _ => {
            exit_with_code(1, &format!("[ERROR] unknown data_source_type: {}", cfg.data_source_type));
        }
    }
}
