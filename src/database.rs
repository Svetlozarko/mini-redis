use crate::data_types::RedisValue;
use crate::memory::MemoryManager;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub type Database = Arc<RwLock<RedisDatabase>>;

#[derive(Debug)]
pub struct RedisDatabase {
    pub data: HashMap<String, RedisValue>,
    pub expires: HashMap<String, Instant>,
    pub memory_manager: MemoryManager,
}

impl RedisDatabase {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expires: HashMap::new(),
            memory_manager: MemoryManager::new(None, "allkeys-lru".to_string()),
        }
    }

    pub fn new_with_memory_config(max_memory: Option<usize>, eviction_policy: String) -> Self {
        Self {
            data: HashMap::new(),
            expires: HashMap::new(),
            memory_manager: MemoryManager::new(max_memory, eviction_policy),
        }
    }

    pub fn get(&mut self, key: &str) -> Option<RedisValue> {
        // Check if key has expired
        if let Some(expire_time) = self.expires.get(key) {
            if Instant::now() > *expire_time {
                self.data.remove(key);
                self.expires.remove(key);
                self.memory_manager.remove_tracking(key);
                return None;
            }
        }

        if let Some(value) = self.data.get(key) {
            // Track access for LRU/LFU
            self.memory_manager.track_access(key);
            Some(value.clone())
        } else {
            None
        }
    }

    pub fn set(&mut self, key: String, value: RedisValue) -> Result<(), String> {
        // Check memory limit before setting
        let memory_manager = &mut self.memory_manager;
      //  memory_manager.check_memory_limit(self)?;

        self.data.insert(key.clone(), value);
        self.memory_manager.track_access(&key);
        Ok(())
    }

    pub fn set_with_expiry(&mut self, key: String, value: RedisValue, ttl: Duration) -> Result<(), String> {
        // Check memory limit before setting
        let memory_manager = &mut self.memory_manager;
      //  memory_manager.check_memory_limit(self)?;

        self.data.insert(key.clone(), value);
        self.expires.insert(key.clone(), Instant::now() + ttl);
        self.memory_manager.track_access(&key);
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> bool {
        self.expires.remove(key);
        self.memory_manager.remove_tracking(key);
        self.data.remove(key).is_some()
    }

    pub fn exists(&mut self, key: &str) -> bool {
        // Check expiry first
        if let Some(expire_time) = self.expires.get(key) {
            if Instant::now() > *expire_time {
                self.data.remove(key);
                self.expires.remove(key);
                self.memory_manager.remove_tracking(key);
                return false;
            }
        }

        let exists = self.data.contains_key(key);
        if exists {
            self.memory_manager.track_access(key);
        }
        exists
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
                self.memory_manager.remove_tracking(key);
                return None;
            }
        }

        if self.data.contains_key(key) {
            self.memory_manager.track_access(key);
            self.data.get_mut(key)
        } else {
            None
        }
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
                self.memory_manager.remove_tracking(key);
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
        self.memory_manager.access_times.clear();
        self.memory_manager.access_counts.clear();
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn get_memory_info(&self) -> HashMap<String, String> {
        self.memory_manager.get_memory_info(self)
    }

    pub fn get_memory_usage(&self) -> usize {
        self.memory_manager.calculate_memory_usage(self)
    }
}

pub fn create_database() -> Database {
    Arc::new(RwLock::new(RedisDatabase::new()))
}

pub fn create_database_with_data(db: RedisDatabase) -> Database {
    Arc::new(RwLock::new(db))
}

pub fn create_database_with_memory_config(max_memory: Option<usize>, eviction_policy: String) -> Database {
    Arc::new(RwLock::new(RedisDatabase::new_with_memory_config(max_memory, eviction_policy)))
}
