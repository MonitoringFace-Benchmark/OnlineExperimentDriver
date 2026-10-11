use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, TryRecvError};
use std::sync::Arc;
use std::thread::{sleep, JoinHandle};
use std::time::{Duration, Instant};

use crate::data_sources::data_source_trait::DataSourcer;

/// Lines read ahead of the sender unless --read-ahead-lines says otherwise: about 200 MB of the
/// Wikimedia week, which at 250x (about 9,300 lines per wall second) is about 110 s of input.
pub const DEFAULT_READ_AHEAD_LINES: usize = 1 << 20;

/// Streams a file line by line. A reader thread keeps a bounded buffer of up to `read_ahead`
/// lines ahead of `iterate`, and `start` returns only once that buffer is full (or the file is
/// read), so a slow or stalled read is absorbed by the buffer instead of delaying a line's send,
/// while memory stays bounded. The former version read the whole file into memory first and
/// kept it to the end: 5 GB for the 4 GB Wikimedia week, which competed with the monitor for
/// memory.
///
/// Empty and unreadable lines are skipped, as before. Should `iterate` ever find the buffer
/// empty before the end of the file, it waits and counts the wait; the counts go to stderr as a
/// `[Read Ahead]` line at the end of the input, so a run shows whether reading ever held up a
/// send.
pub struct DataFileSource {
    path: String,
    read_ahead: usize,
    rx: Option<Receiver<String>>,
    reader: Option<JoinHandle<()>>,
    empty_waits: usize,
    longest_wait: Duration,
    reported: bool,
}

impl DataFileSource {
    pub fn with_read_ahead<P: Into<String>>(path: P, read_ahead: usize) -> Self {
        Self {
            path: path.into(),
            read_ahead: read_ahead.max(1),
            rx: None,
            reader: None,
            empty_waits: 0,
            longest_wait: Duration::ZERO,
            reported: false,
        }
    }

    fn report(&mut self) {
        if self.reported {
            return;
        }
        self.reported = true;
        eprintln!(
            "[Read Ahead] {} lines; the sender found the buffer empty {} time(s), longest wait {:.3} ms",
            self.read_ahead,
            self.empty_waits,
            self.longest_wait.as_secs_f64() * 1000.0
        );
    }

    fn finish(&mut self) -> Option<String> {
        self.report();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        None
    }
}

impl DataSourcer for DataFileSource {
    type Item = String;

    fn start(&mut self) -> bool {
        let p = Path::new(&self.path);
        if !p.exists() {
            self.rx = None;
            return false;
        }
        let f = match File::open(p) {
            Ok(f) => f,
            Err(_e) => {
                self.rx = None;
                return false;
            }
        };

        let (tx, rx) = sync_channel(self.read_ahead);
        let buffered = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let (fill, end) = (Arc::clone(&buffered), Arc::clone(&done));
        let reader = std::thread::spawn(move || {
            for line_res in BufReader::with_capacity(1 << 20, f).lines() {
                let Ok(line) = line_res else { continue };
                if line.trim().is_empty() {
                    continue;
                }
                if tx.send(line).is_err() {
                    break;
                }
                fill.fetch_add(1, Ordering::Release);
            }
            end.store(true, Ordering::Release);
        });

        // Start with a full buffer.
        while !done.load(Ordering::Acquire) && buffered.load(Ordering::Acquire) < self.read_ahead {
            sleep(Duration::from_millis(1));
        }
        self.rx = Some(rx);
        self.reader = Some(reader);
        self.empty_waits = 0;
        self.longest_wait = Duration::ZERO;
        self.reported = false;
        true
    }

    fn iterate(&mut self) -> Option<Self::Item> {
        let rx = self.rx.as_ref()?;
        let line = match rx.try_recv() {
            Ok(line) => line,
            Err(TryRecvError::Disconnected) => return self.finish(),
            Err(TryRecvError::Empty) => {
                let waited = Instant::now();
                match rx.recv() {
                    Ok(line) => {
                        self.empty_waits += 1;
                        self.longest_wait = self.longest_wait.max(waited.elapsed());
                        line
                    }
                    Err(_) => return self.finish(),
                }
            }
        };
        Some(line)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn lines_of(path: &str, read_ahead: usize) -> Vec<String> {
        let mut src = DataFileSource::with_read_ahead(path, read_ahead);
        assert!(src.start());
        std::iter::from_fn(|| src.iterate()).collect()
    }

    #[test]
    fn streams_the_same_lines_as_the_file_in_order_at_any_buffer_size() {
        let dir = std::env::temp_dir().join(format!("dfs_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("input.txt");
        let mut f = File::create(&path).unwrap();
        let mut expected = Vec::new();
        for i in 0..10_000 {
            if i % 97 == 0 {
                writeln!(f).unwrap(); // empty lines are skipped
                writeln!(f, "   ").unwrap();
            }
            writeln!(f, "{}\tline {}", 1_000 + i, i).unwrap();
            expected.push(format!("{}\tline {}", 1_000 + i, i));
        }
        drop(f);
        let p = path.to_str().unwrap();
        for read_ahead in [1, 7, 1_000, 1 << 20] {
            assert_eq!(lines_of(p, read_ahead), expected, "read_ahead {}", read_ahead);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_missing_file_does_not_start() {
        let mut src = DataFileSource::with_read_ahead("/nonexistent/input.txt", DEFAULT_READ_AHEAD_LINES);
        assert!(!src.start());
        assert_eq!(src.iterate(), None);
    }
}
