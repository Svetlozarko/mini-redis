use dashmap::DashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};

#[derive(Clone, Serialize, Deserialize)]
pub struct Entry {
    value: String,
    expires_at: Option<u64>, // UNIX timestamp in seconds
}

pub struct Database {
    store: DashMap<String, Entry>,
}

impl Database {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            store: DashMap::new(),
        })
    }

    pub fn set(&self, key: String, value: String) {
        self.store.insert(key, Entry { value, expires_at: None });
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.store.get(key).and_then(|entry| {
            if let Some(exp) = entry.expires_at {
                if current_timestamp() >= exp {
                    drop(entry);
                    self.store.remove(key);
                    return None;
                }
            }
            Some(entry.value.clone())
        })
    }

    pub fn del(&self, key: &str) -> bool {
        self.store.remove(key).is_some()
    }

    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn incr(&self, key: &str) -> Result<i64, String> {
        let current = self.get(key).unwrap_or_else(|| "0".to_string());
        match current.parse::<i64>() {
            Ok(n) => {
                let new_val = n + 1;
                self.set(key.to_string(), new_val.to_string());
                Ok(new_val)
            }
            Err(_) => Err("value is not an integer".to_string()),
        }
    }

    pub fn expire(&self, key: &str, seconds: u64) -> bool {
        if let Some(mut entry) = self.store.get_mut(key) {
            entry.expires_at = Some(current_timestamp() + seconds);
            true
        } else {
            false
        }
    }

    pub fn ttl(&self, key: &str) -> i64 {
        if let Some(entry) = self.store.get(key) {
            if let Some(expiration) = entry.expires_at {
                let now = current_timestamp();
                if expiration > now {
                    return (expiration - now) as i64;
                } else {
                    drop(entry);
                    self.store.remove(key);
                    return -2; // key expired
                }
            } else {
                return -1; // no expiration
            }
        }
        -2 // key does not exist
    }

    pub fn persist(&self, key: &str) -> bool {
        if let Some(mut entry) = self.store.get_mut(key) {
            if entry.expires_at.is_some() {
                entry.expires_at = None;
                true
            } else {
                false // no expiration to remove
            }
        } else {
            false // key doesn't exist
        }
    }

    pub fn flushdb(&self) {
        self.store.clear();
    }

    // Save snapshot to file
    pub fn save_snapshot(&self, path: &str) -> anyhow::Result<()> {
        let snapshot: Vec<(String, Entry)> = self.store.iter()
            .filter_map(|kv| {
                if let Some(exp_ts) = kv.value().expires_at {
                    if exp_ts <= current_timestamp() {
                        None // expired, skip saving
                    } else {
                        Some((kv.key().clone(), kv.value().clone()))
                    }
                } else {
                    Some((kv.key().clone(), kv.value().clone()))
                }
            })
            .collect();

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, &snapshot)?;
        Ok(())
    }

    // Load snapshot from file
    pub fn load_snapshot(&self, path: &str) -> anyhow::Result<()> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let snapshot: Vec<(String, Entry)> = serde_json::from_reader(reader)?;

        self.store.clear();

        let now = current_timestamp();

        for (key, entry) in snapshot {
            if let Some(exp_ts) = entry.expires_at {
                if exp_ts <= now {
                    continue; // skip expired
                }
            }
            self.store.insert(key, entry);
        }
        Ok(())
    }
}

// Helper to get current UNIX timestamp in seconds
fn current_timestamp() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}
