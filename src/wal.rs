use std::fs::{File, OpenOptions};
use std::io::{Write, BufWriter, BufReader, BufRead};
use std::path::Path;
use serde::{Serialize, Deserialize};
use std::time::SystemTime;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum WalEntry {
    Set { key: String, value: String, timestamp: u64 },
    Delete { key: String, timestamp: u64 },
    Expire { key: String, ttl_seconds: u64, timestamp: u64 },
    Clear { timestamp: u64 },
}

pub struct WriteAheadLog {
    file_path: String,
    writer: Option<BufWriter<File>>,
}

impl WriteAheadLog {
    pub fn new(file_path: String) -> Result<Self, Box<dyn std::error::Error>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        let writer = BufWriter::new(file);

        Ok(Self {
            file_path,
            writer: Some(writer),
        })
    }

    pub fn log_entry(&mut self, entry: &WalEntry) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(writer) = &mut self.writer {
            let json = serde_json::to_string(entry)?;
            writeln!(writer, "{}", json)?;
            writer.flush()?;
        }
        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<WalEntry>, Box<dyn std::error::Error>> {
        if !Path::new(&self.file_path).exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<WalEntry>(&line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    eprintln!("Warning: Failed to parse WAL entry: {} - {}", line, e);
                }
            }
        }

        Ok(entries)
    }

    pub fn truncate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Close current writer
        self.writer = None;

        // Truncate the file
        File::create(&self.file_path)?;

        // Reopen for appending
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)?;

        self.writer = Some(BufWriter::new(file));

        Ok(())
    }

    pub fn get_current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}
