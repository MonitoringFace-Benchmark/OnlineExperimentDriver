use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use crate::data_sources::data_source_trait::DataSourcer;


pub struct DataFileSource {
    path: String,
    lines: Option<Vec<String>>,
    idx: usize,
}

impl DataFileSource {
    pub fn new<P: Into<String>>(path: P) -> Self {
        Self { path: path.into(), lines: None, idx: 0 }
    }
}

impl DataSourcer for DataFileSource {
    type Item = String;

    fn start(&mut self) -> bool {
        let p = Path::new(&self.path);
        if !p.exists() {
            self.lines = None;
            return false;
        }

        match File::open(p) {
            Ok(f) => {
                let buf = BufReader::new(f);
                let mut v = Vec::new();
                for line_res in buf.lines() {
                    if let Ok(line) = line_res {
                        if line.trim().is_empty() { continue; }
                        v.push(line);
                    }
                }
                self.idx = 0;
                self.lines = Some(v);
                true
            }
            Err(_e) => {
                self.lines = None;
                false
            }
        }
    }

    fn iterate(&mut self) -> Option<Self::Item> {
        match &self.lines {
            Some(vec) => {
                if self.idx < vec.len() {
                    let s = vec[self.idx].clone();
                    self.idx += 1;
                    Some(s)
                } else { None }
            }
            None => None,
        }
    }
}