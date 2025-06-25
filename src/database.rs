use crate::data_types::RedisValue;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub type Database = Arc<RwLock<RedisDatabase>>;

#[derive(Debug)]
pub struct RedisDatabase {
    data: HashMap<String, RedisValue>,
    expires: HashMap<String, Instant>,
}

impl RedisDatabase {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expires: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &str) -> Option<RedisValue> {
        // Check if key has expired
        if let Some(expire_time) = self.expires.get(key) {
            if Instant::now() > *expire_time {
                self.data.remove(key);
                self.expires.remove(key);
                return None;
            }
        }

        self.data.get(key).cloned()
    }

    pub fn set(&mut self, key: String, value: RedisValue) {
        self.data.insert(key, value);
    }

    pub fn set_with_expiry(&mut self, key: String, value: RedisValue, ttl: Duration) {
        self.data.insert(key.clone(), value);
        self.expires.insert(key, Instant::now() + ttl);
    }

    pub fn delete(&mut self, key: &str) -> bool {
        self.expires.remove(key);
        self.data.remove(key).is_some()
    }

    pub fn exists(&mut self, key: &str) -> bool {
        // Check expiry first
        if let Some(expire_time) = self.expires.get(key) {
            if Instant::now() > *expire_time {
                self.data.remove(key);
                self.expires.remove(key);
                return false;
            }
        }

        self.data.contains_key(key)
    }

    pub fn keys(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut RedisValue> {
        // Check if key has expired
        if let Some(expire_time) = self.expires.get(key) {
            if Instant::now() > *expire_time {
                self.data.remove(key);
                self.expires.remove(key);
                return None;
            }
        }

        self.data.get_mut(key)
    }

    pub fn expire(&mut self, key: &str, ttl: Duration) -> bool {
        if self.data.contains_key(key) {
            self.expires.insert(key.to_string(), Instant::now() + ttl);
            true
        } else {
            false
        }
    }

    pub fn ttl(&mut self, key: &str) -> Option<Duration> {
        if let Some(expire_time) = self.expires.get(key) {
            let now = Instant::now();
            if now > *expire_time {
                self.data.remove(key);
                self.expires.remove(key);
                None
            } else {
                Some(*expire_time - now)
            }
        } else if self.data.contains_key(key) {
            Some(Duration::MAX) // Key exists but has no expiry
        } else {
            None // Key doesn't exist
        }
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.expires.clear();
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

pub fn create_database() -> Database {
    Arc::new(RwLock::new(RedisDatabase::new()))
}