//! Open loop: the input is written to the tool on its schedule without waiting
//! for answers, so a tool that falls behind gets the lines that arrived in the
//! meantime all at once and can process them together, as a deployed monitor
//! would. A reader thread stamps every line the tool prints with the time it
//! arrived, `[Out] <ns since the start> <line>`. No per-line latency is
//! measured here: a tool that reports its own progress (TimelyMon `--progress`
//! prints `#progress N` once the input up to line N is processed) gives every
//! input line's completion time, and every verdict line carries its own.
//!
//! Log lines: `[Open Loop] 1` first; `[Anchor] <due> <ns>` once the schedule is
//! anchored (real-time mode: the first paced line's timestamp and when it was
//! sent, so `[Out]` times convert to log time as due + (ns - anchor ns) * speed);
//! `[Out] <ns> <line>` per tool line;
//! `[Sent] <lines> <ns>` every SENT_EVERY lines and at the end of the input; the
//! footer `[Total Count]`, `[Max Send Lateness] <ns> at line <n>` (how late a
//! line entered the tool's input against its slot, e.g. because the pipe was
//! full), `[Tool Exit]` and `[Wall Clock]`.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use crate::data_sources::data_source_trait::DataSourcer;
use crate::{exit_with_code, pace_before_send, resolve_batcher, scale, strip_dues, timestamp_to_duration, BatchingMethod};

/// The sender logs its position every this many input lines.
const SENT_EVERY: usize = 100_000;

/// The slot a line with timestamp `due` is paced to, once the schedule is
/// anchored (see `pace_before_send`); `None` when nothing is paced.
fn slot_of(
    mode: &str,
    timestamp_units: Option<&str>,
    speed: f64,
    anchor: Option<(Instant, Duration)>,
    due: Option<usize>,
) -> Option<Instant> {
    if mode != "real-time" {
        return None;
    }
    let (Some(due), Some(units), Some((base, first))) = (due, timestamp_units, anchor) else {
        return None;
    };
    Some(base + scale(timestamp_to_duration(due, units).saturating_sub(first), speed))
}

#[allow(clippy::too_many_arguments)]
pub fn run_open_loop<S: DataSourcer<Item = String>>(
    mut src: S,
    binary_path: &str,
    binary_args: &[String],
    maximum_latency_ms: Option<f64>,
    mode: &str,
    timestamp_units: Option<&str>,
    speed: f64,
    prefixed: bool,
    extract_timestamp: fn(&str) -> Option<usize>,
    batch_method: BatchingMethod,
    batch_delimiter: &str,
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
        Err(e) => exit_with_code(1, &format!("[ERROR] failed to spawn {}: {}", binary_path, e)),
    };
    let mut stdin = child.stdin.take().expect("[ERROR] Child stdin not piped");
    let stdout = child.stdout.take().expect("[ERROR] Child stdout not piped");

    println!("[Open Loop] 1");
    let run_start = Instant::now();
    let reader = std::thread::spawn(move || {
        let out = std::io::stdout();
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            let at = run_start.elapsed().as_nanos();
            let _ = writeln!(out.lock(), "[Out] {} {}", at, line);
        }
    });

    let mut next_batch = resolve_batcher(batch_method);
    let mut anchor: Option<(Instant, Duration)> = None;
    let mut sent = 0usize;
    let mut max_late = Duration::ZERO;
    let mut late_at = 0usize;
    let mut anchored = false;
    while let Some(batch) = next_batch(&mut src) {
        let due = extract_timestamp(&batch[0]);
        let batch = strip_dues(batch, prefixed);
        pace_before_send(mode, timestamp_units, speed, &mut anchor, due);
        if !anchored {
            if let (Some((base, _)), Some(due)) = (anchor, due) {
                println!("[Anchor] {} {}", due, base.duration_since(run_start).as_nanos());
                anchored = true;
            }
        }
        let slot = slot_of(mode, timestamp_units, speed, anchor, due);
        let mut line = batch.join(batch_delimiter);
        line.push('\n');
        // One write per line, so a line reaches the tool at its slot; when the
        // tool is behind and the pipe is full, the write waits.
        if stdin.write_all(line.as_bytes()).is_err() {
            eprintln!("[WARN] the tool stopped reading its input after {} line(s)", sent);
            break;
        }
        let before = sent;
        sent += batch.len();
        if let Some(slot) = slot {
            let late = Instant::now().saturating_duration_since(slot);
            if late > max_late {
                max_late = late;
                late_at = sent;
            }
        }
        if sent / SENT_EVERY != before / SENT_EVERY {
            println!("[Sent] {} {}", sent, run_start.elapsed().as_nanos());
        }
    }
    // End of input.
    drop(stdin);
    println!("[Sent] {} {}", sent, run_start.elapsed().as_nanos());

    // The tool finishes on its own; --maximum-latency bounds the wait.
    let deadline = maximum_latency_ms.map(|ms| Instant::now() + Duration::from_secs_f64(ms / 1000.0));
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {}
            Err(e) => exit_with_code(1, &format!("[ERROR] cannot wait for the tool: {}", e)),
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            let _ = child.kill();
            let _ = child.wait();
            eprintln!("[WARN] tool did not exit after EOF within the budget; killed");
            break None;
        }
        sleep(Duration::from_millis(10));
    };
    let _ = reader.join();

    println!("[Total Count] {}", sent);
    println!("[Max Send Lateness] {} ns at line {}", max_late.as_nanos(), late_at);
    match status {
        Some(s) => println!("[Tool Exit] {}", s),
        None => println!("[Tool Exit] killed after EOF"),
    }
    println!("[Wall Clock] {:.6} s", run_start.elapsed().as_secs_f64());
    if !status.is_some_and(|s| s.success()) {
        std::process::exit(1);
    }
}
