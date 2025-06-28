use crate::data_types::RedisValue;
use crate::database::RedisDatabase;
use std::collections::HashMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use rand::Rng;

#[derive(Debug, Clone)]
pub enum EvictionPolicy {
    NoEviction,
    AllKeysLru,
    AllKeysLfu,
    VolatileLru,
    VolatileLfu,
    AllKeysRandom,
    VolatileRandom,
}

impl EvictionPolicy {
    pub fn from_string(policy: &str) -> Self {
        match policy {
            "noeviction" => EvictionPolicy::NoEviction,
            "allkeys-lru" => EvictionPolicy::AllKeysLru,
            "allkeys-lfu" => EvictionPolicy::AllKeysLfu,
            "volatile-lru" => EvictionPolicy::VolatileLru,
            "volatile-lfu" => EvictionPolicy::VolatileLfu,
            "allkeys-random" => EvictionPolicy::AllKeysRandom,
            "volatile-random" => EvictionPolicy::VolatileRandom,
            _ => EvictionPolicy::AllKeysLru, // Default
        }
    }
}

#[derive(Debug)]
pub struct MemoryManager {
    pub max_memory: Option<usize>,
    pub eviction_policy: EvictionPolicy,
    pub access_times: HashMap<String, Instant>,
    pub access_counts: HashMap<String, u64>,
}

impl MemoryManager {
    pub fn new(max_memory: Option<usize>, eviction_policy: String) -> Self {
        Self {
            max_memory,
            eviction_policy: EvictionPolicy::from_string(&eviction_policy),
            access_times: HashMap::new(),
            access_counts: HashMap::new(),
        }
    }

    pub fn track_access(&mut self, key: &str) {
        self.access_times.insert(key.to_string(), Instant::now());
        *self.access_counts.entry(key.to_string()).or_insert(0) += 1;
    }

    pub fn remove_tracking(&mut self, key: &str) {
        self.access_times.remove(key);
        self.access_counts.remove(key);
    }

    pub fn calculate_memory_usage(&self, db: &RedisDatabase) -> usize {
        let mut total_size = 0;

        // Calculate size of data HashMap
        for (key, value) in &db.data {
            total_size += key.len(); // Key size
            total_size += self.calculate_value_size(value);
        }

        // Calculate size of expires HashMap
        total_size += db.expires.len() * (std::mem::size_of::<String>() + std::mem::size_of::<Instant>());

        // Add tracking overhead
        total_size += self.access_times.len() * (std::mem::size_of::<String>() + std::mem::size_of::<Instant>());
        total_size += self.access_counts.len() * (std::mem::size_of::<String>() + std::mem::size_of::<u64>());

        // Add some overhead for the data structures themselves
        total_size += 2048; // Base overhead

        total_size
    }

    fn calculate_value_size(&self, value: &RedisValue) -> usize {
        match value {
            RedisValue::String(s) => s.len(),
            RedisValue::Integer(_) => 8, // i64 size
            RedisValue::List(list) => {
                list.iter().map(|item| item.len()).sum::<usize>() + (list.len() * 8) // Vec overhead
            },
            RedisValue::Set(set) => {
                set.iter().map(|item| item.len()).sum::<usize>() + (set.len() * 8) // HashSet overhead
            },
            RedisValue::Hash(hash) => {
                hash.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() + (hash.len() * 16) // HashMap overhead
            },
        }
    }

    pub fn check_memory_limit(&mut self, db: &mut RedisDatabase) -> Result<(), String> {
        if let Some(max_mem) = self.max_memory {
            let current_usage = self.calculate_memory_usage(db);

            if current_usage > max_mem {
                match self.eviction_policy {
                    EvictionPolicy::NoEviction => {
                        return Err(format!("OOM command not allowed when used memory > 'maxmemory'. Current: {} bytes, Max: {} bytes", current_usage, max_mem));
                    },
                    _ => {
                        // Perform eviction
                        let target_size = (max_mem as f64 * 0.9) as usize; // Evict to 90% of max
                        self.evict_keys(db, target_size)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn evict_keys(&mut self, db: &mut RedisDatabase, target_size: usize) -> Result<(), String> {
        let mut current_usage = self.calculate_memory_usage(db);
        let mut evicted_count = 0;

        while current_usage > target_size && !db.data.is_empty() {
            let key_to_evict = match self.eviction_policy {
                EvictionPolicy::AllKeysLru => self.find_lru_key(&db.data, false),
                EvictionPolicy::AllKeysLfu => self.find_lfu_key(&db.data, false),
                EvictionPolicy::VolatileLru => self.find_lru_key(&db.data, true),
                EvictionPolicy::VolatileLfu => self.find_lfu_key(&db.data, true),
                EvictionPolicy::AllKeysRandom => self.find_random_key(&db.data, false),
                EvictionPolicy::VolatileRandom => self.find_random_key(&db.data, true),
                EvictionPolicy::NoEviction => break, // Should not reach here
            };

            if let Some(key) = key_to_evict {
                db.delete(&key);
                self.remove_tracking(&key);
                evicted_count += 1;
                current_usage = self.calculate_memory_usage(db);
            } else {
                break; // No more keys to evict
            }

            // Safety check to prevent infinite loop
            if evicted_count > 1000 {
                break;
            }
        }

        println!("Evicted {} keys due to memory pressure", evicted_count);
        Ok(())
    }

    fn find_lru_key(&self, data: &HashMap<String, RedisValue>, volatile_only: bool) -> Option<String> {
        let mut oldest_key: Option<String> = None;
        let mut oldest_time = Instant::now();

        for key in data.keys() {
            if volatile_only && !self.has_expiry(key) {
                continue;
            }

            if let Some(access_time) = self.access_times.get(key) {
                if *access_time < oldest_time {
                    oldest_time = *access_time;
                    oldest_key = Some(key.clone());
                }
            } else {
                // Key never accessed, consider it oldest
                return Some(key.clone());
            }
        }

        oldest_key
    }

    fn find_lfu_key(&self, data: &HashMap<String, RedisValue>, volatile_only: bool) -> Option<String> {
        let mut least_used_key: Option<String> = None;
        let mut least_count = u64::MAX;

        for key in data.keys() {
            if volatile_only && !self.has_expiry(key) {
                continue;
            }

            let count = self.access_counts.get(key).unwrap_or(&0);
            if *count < least_count {
                least_count = *count;
                least_used_key = Some(key.clone());
            }
        }

        least_used_key
    }

    fn find_random_key(&self, data: &HashMap<String, RedisValue>, volatile_only: bool) -> Option<String> {
        let keys: Vec<&String> = if volatile_only {
            data.keys().filter(|k| self.has_expiry(k)).collect()
        } else {
            data.keys().collect()
        };

        if keys.is_empty() {
            return None;
        }

        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..keys.len());
        Some(keys[index].clone())
    }

    fn has_expiry(&self, _key: &str) -> bool {
        // This would need access to the database's expires HashMap
        // For now, we'll assume all keys are volatile for volatile policies
        true
    }

    pub fn get_memory_info(&self, db: &RedisDatabase) -> HashMap<String, String> {
        let mut info = HashMap::new();
        let current_usage = self.calculate_memory_usage(db);

        info.insert("used_memory".to_string(), current_usage.to_string());
        info.insert("used_memory_human".to_string(), format_bytes(current_usage));

        if let Some(max_mem) = self.max_memory {
            info.insert("maxmemory".to_string(), max_mem.to_string());
            info.insert("maxmemory_human".to_string(), format_bytes(max_mem));
            info.insert("used_memory_percentage".to_string(),
                        format!("{:.2}%", (current_usage as f64 / max_mem as f64) * 100.0));
        } else {
            info.insert("maxmemory".to_string(), "0".to_string());
            info.insert("maxmemory_human".to_string(), "unlimited".to_string());
            info.insert("used_memory_percentage".to_string(), "N/A".to_string());
        }

        info.insert("maxmemory_policy".to_string(), format!("{:?}", self.eviction_policy));
        info.insert("total_keys".to_string(), db.data.len().to_string());

        info
    }
}

pub fn format_bytes(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{:.0}{}", size, UNITS[unit_index])
    } else {
        format!("{:.2}{}", size, UNITS[unit_index])
    }
}
