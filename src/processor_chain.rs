use std::io::BufRead;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::Sender;

use crate::exit_with_code;

/// One event from any of the driver's input channels: the tool's stdout or a
/// processor's stderr control channel. The main loop consumes a single unified
/// stream so a round can close on either a tool response or chain quiescence.
pub enum Ev {
    ToolLine(std::io::Result<String>),
    Ctl { stage: usize, consumed: u64, released: u64, dropped: u64, origins: Option<Vec<u64>> },
    Stats { stage: usize, json: String },
    StageEof { stage: usize },
}

pub struct ProcessorChain {
    pub children: Vec<Child>,
}

impl ProcessorChain {
    pub fn kill_all(&mut self) {
        for child in self.children.iter_mut() {
            let _ = child.kill();
        }
    }
}

/// Spawns the processor commands and chains them at the OS level:
/// p0.stdout -> p1.stdin -> ... The driver keeps p0's stdin (its write end)
/// and the last stage's stdout (which becomes the tool's stdin); each stage's
/// stderr is piped as its control channel.
pub fn spawn_chain(cmds: &[String]) -> (ProcessorChain, ChildStdin, ChildStdout) {
    assert!(!cmds.is_empty(), "spawn_chain requires at least one processor");
    let mut children: Vec<Child> = Vec::with_capacity(cmds.len());
    let mut chain_stdin: Option<ChildStdin> = None;
    let mut prev_stdout: Option<ChildStdout> = None;

    for (i, cmd) in cmds.iter().enumerate() {
        let stdin = match prev_stdout.take() {
            Some(out) => Stdio::from(out),
            None => Stdio::piped(),
        };
        let mut child = match Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .stdin(stdin)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(c) => c,
            Err(e) => exit_with_code(1, &format!("[ERROR] failed to spawn processor {} ({}): {}", i, cmd, e)),
        };
        if i == 0 {
            chain_stdin = Some(child.stdin.take().expect("[ERROR] processor 0 stdin not piped"));
        }
        prev_stdout = Some(child.stdout.take().expect("[ERROR] processor stdout not piped"));
        children.push(child);
    }

    (
        ProcessorChain { children },
        chain_stdin.expect("[ERROR] chain stdin missing"),
        prev_stdout.expect("[ERROR] chain stdout missing"),
    )
}

/// Parses a control line of the form
/// `#mfctl consumed=<u64> released=<u64> [dropped=<u64>] [origins=a,b,c]`.
/// `dropped` counts lines the stage swallowed on purpose (e.g. a release
/// signal), so the held count can tell buffering apart from dropping.
/// Returns None for anything else (which is passthrough debug output).
pub fn parse_ctl(line: &str) -> Option<(u64, u64, u64, Option<Vec<u64>>)> {
    let rest = line.strip_prefix("#mfctl")?.trim();
    let mut consumed: Option<u64> = None;
    let mut released: Option<u64> = None;
    let mut dropped: u64 = 0;
    let mut origins: Option<Vec<u64>> = None;
    for part in rest.split_whitespace() {
        if let Some(v) = part.strip_prefix("consumed=") {
            consumed = v.parse().ok();
        } else if let Some(v) = part.strip_prefix("released=") {
            released = v.parse().ok();
        } else if let Some(v) = part.strip_prefix("dropped=") {
            dropped = v.parse().ok()?;
        } else if let Some(v) = part.strip_prefix("origins=") {
            let parsed: Result<Vec<u64>, _> = v.split(',').map(|x| x.parse()).collect();
            origins = parsed.ok();
        }
    }
    Some((consumed?, released?, dropped, origins))
}

pub fn parse_stats(line: &str) -> Option<String> {
    line.strip_prefix("#mfstats").map(|rest| rest.trim().to_string())
}

/// One reader thread per processor stderr: control lines become events,
/// everything else is forwarded to the driver's stderr for debugging.
pub fn spawn_control_readers(chain: &mut ProcessorChain, tx: &Sender<Ev>) {
    for (stage, child) in chain.children.iter_mut().enumerate() {
        let stderr = child.stderr.take().expect("[ERROR] processor stderr not piped");
        let tx = tx.clone();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stderr).lines() {
                match line {
                    Ok(line) => {
                        if let Some((consumed, released, dropped, origins)) = parse_ctl(&line) {
                            if tx.send(Ev::Ctl { stage, consumed, released, dropped, origins }).is_err() {
                                return;
                            }
                        } else if let Some(json) = parse_stats(&line) {
                            if tx.send(Ev::Stats { stage, json }).is_err() {
                                return;
                            }
                        } else {
                            eprintln!("[stage {}] {}", stage, line);
                        }
                    }
                    Err(_) => break,
                }
            }
            let _ = tx.send(Ev::StageEof { stage });
        });
    }
}

/// Cumulative flow counters for the chain. Quiescence for `fed` inputs holds
/// when the first stage consumed everything the driver wrote, every later
/// stage consumed everything its predecessor released, and the tool answered
/// every line the last stage released (responses are 1:1 per delivered line
/// in event-count mode). A withheld input then closes its round via the
/// counters alone, with no tracer ever reaching the tool.
pub struct ChainState {
    pub consumed: Vec<u64>,
    pub released: Vec<u64>,
    pub dropped: Vec<u64>,
    pub stats: Vec<Option<String>>,
    pub eof: Vec<bool>,
}

impl ChainState {
    pub fn new(n: usize) -> Self {
        ChainState {
            consumed: vec![0; n],
            released: vec![0; n],
            dropped: vec![0; n],
            stats: vec![None; n],
            eof: vec![false; n],
        }
    }

    pub fn delivered(&self) -> u64 {
        *self.released.last().unwrap_or(&0)
    }

    /// Lines currently buffered inside the chain: per stage, consumed minus
    /// released minus deliberately dropped. Exact for 1:1 stages; a splitting
    /// or merging stage makes it an approximation in mixed units.
    pub fn held(&self) -> u64 {
        (0..self.consumed.len())
            .map(|i| self.consumed[i].saturating_sub(self.released[i]).saturating_sub(self.dropped[i]))
            .sum()
    }

    pub fn quiescent(&self, fed: u64, responses: u64) -> bool {
        if self.consumed.is_empty() {
            return true;
        }
        if self.consumed[0] != fed {
            return false;
        }
        for i in 1..self.consumed.len() {
            if self.consumed[i] != self.released[i - 1] {
                return false;
            }
        }
        responses == self.delivered()
    }

    pub fn all_stats(&self) -> bool {
        self.stats.iter().all(|s| s.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ctl_parses_with_and_without_origins() {
        assert_eq!(parse_ctl("#mfctl consumed=4 released=3"), Some((4, 3, 0, None)));
        assert_eq!(
            parse_ctl("#mfctl consumed=4 released=3 dropped=1 origins=2,3,1"),
            Some((4, 3, 1, Some(vec![2, 3, 1])))
        );
        assert_eq!(parse_ctl("debug output"), None);
        assert_eq!(parse_ctl("#mfctl consumed=x released=3"), None);
    }

    #[test]
    fn held_distinguishes_buffered_from_dropped() {
        let mut s = ChainState::new(1);
        s.consumed[0] = 4;
        s.released[0] = 3;
        assert_eq!(s.held(), 1);
        s.dropped[0] = 1;
        assert_eq!(s.held(), 0, "a swallowed signal line is not held");
    }

    #[test]
    fn quiescence_two_stage_chain() {
        let mut s = ChainState::new(2);
        // fed 1 line; nothing consumed yet
        assert!(!s.quiescent(1, 0));
        // stage 0 passed it through, stage 1 holds it
        s.consumed[0] = 1;
        s.released[0] = 1;
        s.consumed[1] = 1;
        s.released[1] = 0;
        assert!(s.quiescent(1, 0), "held input must close the round via counters");
        // stage 1 releases 1 line; tool has not answered yet
        s.released[1] = 1;
        assert!(!s.quiescent(1, 0));
        assert!(s.quiescent(1, 1));
    }

    #[test]
    fn quiescence_burst_after_release() {
        let mut s = ChainState::new(1);
        s.consumed[0] = 4;
        s.released[0] = 3;
        assert!(!s.quiescent(4, 0), "burst must be collected before closing");
        assert!(!s.quiescent(4, 2));
        assert!(s.quiescent(4, 3));
    }

    #[test]
    fn quiescence_empty_chain_is_trivial() {
        let s = ChainState::new(0);
        assert!(s.quiescent(7, 0));
    }
}
