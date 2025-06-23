use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct Entry {
    value: String,
    expires_at: Option<Instant>,
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
                if Instant::now() >= exp {
                    drop(entry); // Drop read guard before removing
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
            entry.expires_at = Some(Instant::now() + Duration::from_secs(seconds));
            true
        } else {
            false
        }
    }
}
