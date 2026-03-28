use std::process::{Child, ChildStdout, Command, Stdio};
use std::io::{BufRead, BufReader, Lines};

pub struct DataScriptSource {
    command: String,
    args: Vec<String>,
    child: Option<Child>,
    stdout_lines: Option<Lines<BufReader<ChildStdout>>>,
}

impl DataScriptSource {
    pub fn new<C, I, A>(command: C, args: I) -> Self
    where
        C: Into<String>,
        I: IntoIterator<Item = A>,
        A: Into<String>,
    {
        Self {
            command: command.into(),
            args: args.into_iter().map(Into::into).collect(),
            child: None,
            stdout_lines: None,
        }
    }
}

impl Drop for DataScriptSource {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl crate::DataSourcer for DataScriptSource {
    type Item = String;

    fn start(&mut self) -> bool {
        let mut cmd = Command::new(&self.command);
        cmd.args(&self.args).stdout(Stdio::piped()).stdin(Stdio::null());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to spawn script '{}': {}", self.command, e);
                self.child = None;
                self.stdout_lines = None;
                return false;
            }
        };

        match child.stdout.take() {
            Some(out) => {
                let reader = BufReader::new(out);
                self.stdout_lines = Some(reader.lines());
                self.child = Some(child);
                true
            }
            None => {
                eprintln!("Script '{}' spawned without stdout piped", self.command);
                let _ = child.kill();
                let _ = child.wait();
                self.child = None;
                self.stdout_lines = None;
                false
            }
        }
    }

    fn iterate(&mut self) -> Option<Self::Item> {
        match &mut self.stdout_lines {
            Some(lines) => {
                loop {
                    match lines.next() {
                        Some(Ok(line)) => {
                            if line.trim().is_empty() { continue; }
                            return Some(line);
                        }
                        Some(Err(e)) => {
                            eprintln!("Fatal: error reading stdout from script '{}': {}", self.command, e);
                            std::process::exit(1);
                        }
                        None => { return None; }
                    }
                }
            }
            None => None,
        }
    }
}