        use crate::data_types::RedisValue;
        use crate::database::RedisDatabase;
        use serde::{Deserialize, Serialize};
        use std::collections::HashMap;
        use std::fs::{self, File};
        use std::io::{BufWriter, Write};
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
                let now_instant = std::time::Instant::now();
                let now_system = SystemTime::now();
        
                let expires_serializable: HashMap<String, u64> = db
                    .expires
                    .iter()
                    .filter_map(|(key, instant)| {
                        if *instant > now_instant {
                            let duration_left = *instant - now_instant;
                            if let Ok(now_secs) = now_system.duration_since(UNIX_EPOCH) {
                                let future_secs = now_secs.as_secs() + duration_left.as_secs();
                                return Some((key.clone(), future_secs));
                            }
                        }
                        None
                    })
                    .collect();
        
                let persisted_data = PersistedData {
                    data: db.data.clone(),
                    expires: expires_serializable,
                };
        
                let json_data = serde_json::to_string_pretty(&persisted_data)?;
        
                let tmp_path = format!("{}.tmp", &self.file_path);
                let file = File::create(&tmp_path)?;
                let mut writer = BufWriter::new(&file);
        
                writer.write_all(json_data.as_bytes())?;
                writer.flush()?;          
                file.sync_all()?;        
        
                fs::rename(&tmp_path, &self.file_path)?;
        
                if let Some(parent_dir) = Path::new(&self.file_path).parent() {
                    if let Ok(dir) = File::open(parent_dir) {
                        let _ = dir.sync_all();
                    }
                }
        
                println!(
                    " Database saved to {} ({} keys)",
                    self.file_path,
                    db.data.len()
                );
        
                Ok(())
            }
        
            pub fn load_database(&self) -> Result<RedisDatabase, Box<dyn std::error::Error>> {
                if !Path::new(&self.file_path).exists() {
                    println!(
                        "Database file {} not found, starting with empty DB",
                        self.file_path
                    );
                    return Ok(RedisDatabase::new());
                }
        
                let json_data = fs::read_to_string(&self.file_path)?;
                if json_data.trim().is_empty() {
                    println!(
                        "Database file {} empty, starting fresh",
                        self.file_path
                    );
                    return Ok(RedisDatabase::new());
                }
        
                let persisted_data: PersistedData = serde_json::from_str(&json_data)?;
                let now_system = SystemTime::now();
                let now_instant = std::time::Instant::now();
        
                let mut expires = HashMap::new();
                if let Ok(current_secs) = now_system.duration_since(UNIX_EPOCH) {
                    for (key, expire_timestamp) in persisted_data.expires {
                        if expire_timestamp > current_secs.as_secs() {
                            let seconds_until_expiry = expire_timestamp - current_secs.as_secs();
                            expires.insert(key, now_instant + Duration::from_secs(seconds_until_expiry));
                        }
                    }
                }
        
                let mut db = RedisDatabase::new();
                db.data = persisted_data.data;
                db.expires = expires;
        
                println!(
                    "Database loaded from {} ({} keys)",
                    self.file_path,
                    db.data.len()
                );
                Ok(db)
            }
        }
