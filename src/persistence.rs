use crate::data_types::RedisValue;
use crate::database::RedisDatabase;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
struct PersistedData {
    data: HashMap<String, RedisValue>,
    expires: HashMap<String, u64>, 
}

pub struct MmapPersistence {
    pub file_path: String,
}

impl MmapPersistence {
    pub fn new(file_path: String) -> Self {
        Self { file_path }
    }

    pub fn save_database(&self, db: &RedisDatabase) -> Result<(), Box<dyn std::error::Error>> {
        // Convert expires from Instant to u64 (seconds since UNIX_EPOCH)
        let expires_serializable: HashMap<String, u64> = db.expires
            .iter()
            .filter_map(|(key, instant)| {
                let now = std::time::Instant::now();
                let system_now = SystemTime::now();

                if *instant > now {
                    let duration_left = *instant - now;
                    // Use + operator instead of checked_add, or handle the Result properly
                    match system_now + duration_left {
                        future_time => {
                            if let Ok(duration_since_epoch) = future_time.duration_since(UNIX_EPOCH) {
                                return Some((key.clone(), duration_since_epoch.as_secs()));
                            }
                        }
                    }
                }
                None
            })
            .collect();

        let persisted_data = PersistedData {
            data: db.data.clone(),
            expires: expires_serializable,
        };

        // Use JSON serialization for simplicity and reliability
        let json_data = serde_json::to_string_pretty(&persisted_data)?;
        fs::write(&self.file_path, json_data)?;

        println!("Database saved to {} ({} keys)", self.file_path, db.data.len());
        Ok(())
    }

    pub fn load_database(&self) -> Result<RedisDatabase, Box<dyn std::error::Error>> {
        if !Path::new(&self.file_path).exists() {
            println!("Database file {} not found, starting with empty database", self.file_path);
            return Ok(RedisDatabase::new());
        }

        let json_data = fs::read_to_string(&self.file_path)?;
        if json_data.trim().is_empty() {
            println!("Database file {} is empty, starting with empty database", self.file_path);
            return Ok(RedisDatabase::new());
        }

        let persisted_data: PersistedData = serde_json::from_str(&json_data)?;

        // Convert expires back from u64 to Instant
        let now = SystemTime::now();
        let instant_now = std::time::Instant::now();
        let mut expires = HashMap::new();

        for (key, expire_timestamp) in persisted_data.expires {
            let expire_time = UNIX_EPOCH + Duration::from_secs(expire_timestamp);
            if expire_time > now {
                if let Ok(duration_until_expiry) = expire_time.duration_since(now) {
                    expires.insert(key, instant_now + duration_until_expiry);
                }
            }
        }

        let mut db = RedisDatabase::new();
        db.data = persisted_data.data;
        db.expires = expires;

        println!("Database loaded from {} ({} keys)", self.file_path, db.data.len());
        Ok(db)
    }
}