use crate::data_types::RedisValue;
use crate::database::RedisDatabase;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write, BufReader, Read};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use sha2::{Sha256, Digest};

#[derive(Debug, Serialize, Deserialize)]
struct PersistedData {
    version: u32,
    data: HashMap<String, RedisValue>,
    expires: HashMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum: Option<String>,
}

pub struct MmapPersistence {
    pub file_path: String,
}

impl MmapPersistence {
    pub fn new(file_path: String) -> Self {
        Self { file_path }
    }

    fn calculate_checksum(data: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let result = hasher.finalize();
        // Convert each byte to hex format
        result.iter().map(|b| format!("{:02x}", b)).collect()
    }

    fn verify_checksum(data: &str, expected_checksum: &str) -> bool {
        let actual_checksum = Self::calculate_checksum(data);
        actual_checksum == expected_checksum
    }

    fn create_backup(&self) -> Result<(), Box<dyn std::error::Error>> {
        if Path::new(&self.file_path).exists() {
            let backup_path = format!("{}.bak", &self.file_path);
            fs::copy(&self.file_path, &backup_path)?;
            println!("Created backup at {}", backup_path);
        }
        Ok(())
    }

    fn cleanup_temp_files(&self) -> Result<(), Box<dyn std::error::Error>> {
        let tmp_path = format!("{}.tmp", &self.file_path);
        if Path::new(&tmp_path).exists() {
            println!("Found stale temporary file, cleaning up: {}", tmp_path);
            fs::remove_file(&tmp_path)?;
        }
        Ok(())
    }

    pub fn save_database(&self, db: &RedisDatabase) -> Result<(), Box<dyn std::error::Error>> {
        self.create_backup()?;

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

        let mut persisted_data = PersistedData {
            version: 1,
            data: db.data.clone(),
            expires: expires_serializable,
            checksum: None,
        };

        let json_data = serde_json::to_string_pretty(&persisted_data)?;

        let checksum = Self::calculate_checksum(&json_data);
        persisted_data.checksum = Some(checksum);

        let json_data_with_checksum = serde_json::to_string_pretty(&persisted_data)?;

        let tmp_path = format!("{}.tmp", &self.file_path);
        let file = File::create(&tmp_path)?;
        let mut writer = BufWriter::new(&file);

        writer.write_all(json_data_with_checksum.as_bytes())?;
        writer.flush()?;
        file.sync_all()?;

        fs::rename(&tmp_path, &self.file_path)?;

        if let Some(parent_dir) = Path::new(&self.file_path).parent() {
            if let Ok(dir) = File::open(parent_dir) {
                let _ = dir.sync_all();
            }
        }

        println!(
            "Database saved to {} ({} keys, checksum: {})",
            self.file_path,
            db.data.len(),
            persisted_data.checksum.unwrap_or_default()
        );

        Ok(())
    }

    fn try_recover_from_backup(&self) -> Result<RedisDatabase, Box<dyn std::error::Error>> {
        let backup_path = format!("{}.bak", &self.file_path);

        if !Path::new(&backup_path).exists() {
            return Err("No backup file available for recovery".into());
        }

        println!("Attempting recovery from backup: {}", backup_path);

        let json_data = fs::read_to_string(&backup_path)?;
        if json_data.trim().is_empty() {
            return Err("Backup file is empty".into());
        }

        let persisted_data: PersistedData = serde_json::from_str(&json_data)?;

        if let Some(expected_checksum) = &persisted_data.checksum {
            let mut data_without_checksum = persisted_data.clone();
            data_without_checksum.checksum = None;
            let json_without_checksum = serde_json::to_string_pretty(&data_without_checksum)?;

            if !Self::verify_checksum(&json_without_checksum, expected_checksum) {
                return Err("Backup file checksum verification failed".into());
            }
            println!("Backup checksum verified successfully");
        }

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

        println!("Successfully recovered from backup ({} keys)", db.data.len());
        Ok(db)
    }

    pub fn load_database(&self) -> Result<RedisDatabase, Box<dyn std::error::Error>> {
        self.cleanup_temp_files()?;

        if !Path::new(&self.file_path).exists() {
            println!(
                "Database file {} not found, starting with empty DB",
                self.file_path
            );
            return Ok(RedisDatabase::new());
        }

        match self.try_load_main_file() {
            Ok(db) => Ok(db),
            Err(e) => {
                eprintln!("Failed to load main database file: {}", e);
                eprintln!("Attempting recovery from backup...");

                match self.try_recover_from_backup() {
                    Ok(db) => {
                        println!("Recovery successful! Restoring from backup.");
                        if let Err(save_err) = self.save_database(&db) {
                            eprintln!("Warning: Failed to save recovered database: {}", save_err);
                        }
                        Ok(db)
                    },
                    Err(backup_err) => {
                        eprintln!("Backup recovery also failed: {}", backup_err);
                        eprintln!("Starting with empty database");
                        Ok(RedisDatabase::new())
                    }
                }
            }
        }
    }

    fn try_load_main_file(&self) -> Result<RedisDatabase, Box<dyn std::error::Error>> {
        let json_data = fs::read_to_string(&self.file_path)?;

        if json_data.trim().is_empty() {
            return Err("Database file is empty".into());
        }

        let persisted_data: PersistedData = serde_json::from_str(&json_data)?;

        if persisted_data.version > 1 {
            return Err(format!(
                "Unsupported database version: {}. Current version: 1",
                persisted_data.version
            ).into());
        }

        if let Some(expected_checksum) = &persisted_data.checksum {
            let mut data_without_checksum = persisted_data.clone();
            data_without_checksum.checksum = None;
            let json_without_checksum = serde_json::to_string_pretty(&data_without_checksum)?;

            if !Self::verify_checksum(&json_without_checksum, expected_checksum) {
                return Err("Checksum verification failed - database file may be corrupted".into());
            }
            println!("Database checksum verified successfully");
        } else {
            println!("Warning: No checksum found in database file (older format)");
        }

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

    pub fn verify_integrity(&self) -> Result<bool, Box<dyn std::error::Error>> {
        if !Path::new(&self.file_path).exists() {
            return Err("Database file does not exist".into());
        }

        let json_data = fs::read_to_string(&self.file_path)?;
        let persisted_data: PersistedData = serde_json::from_str(&json_data)?;

        if let Some(expected_checksum) = &persisted_data.checksum {
            let mut data_without_checksum = persisted_data.clone();
            data_without_checksum.checksum = None;
            let json_without_checksum = serde_json::to_string_pretty(&data_without_checksum)?;

            Ok(Self::verify_checksum(&json_without_checksum, expected_checksum))
        } else {
            Ok(true) // No checksum to verify
        }
    }
}

impl Clone for PersistedData {
    fn clone(&self) -> Self {
        Self {
            version: self.version,
            data: self.data.clone(),
            expires: self.expires.clone(),
            checksum: self.checksum.clone(),
        }
    }
}
