use crate::data_types::RedisValue;
use crate::database::{Database, RedisDatabase};
use crate::auth::ClientAuth;
use crate::persistence_clean::MmapPersistence;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum MergeStrategy {
    Overwrite,
    Skip,
    Merge,
}

#[derive(Debug, Clone)]
pub enum Command {
    // String commands
    Get { key: String },
    Set { key: String, value: String },
    SetEx { key: String, value: String, seconds: u64 },
    Del { keys: Vec<String> },
    Exists { keys: Vec<String> },
    Incr { key: String },
    Decr { key: String },

    // List commands
    LPush { key: String, values: Vec<String> },
    RPush { key: String, values: Vec<String> },
    LPop { key: String },
    RPop { key: String },
    LLen { key: String },
    LRange { key: String, start: i32, stop: i32 },

    // Set commands
    SAdd { key: String, members: Vec<String> },
    SRem { key: String, members: Vec<String> },
    SMembers { key: String },
    SCard { key: String },
    SIsMember { key: String, member: String },

    // Hash commands
    HSet { key: String, field: String, value: String },
    HGet { key: String, field: String },
    HDel { key: String, fields: Vec<String> },
    HGetAll { key: String },
    HKeys { key: String },
    HVals { key: String },
    HLen { key: String },

    // Generic commands
    Keys { pattern: String },
    Type { key: String },
    Expire { key: String, seconds: u64 },
    Ttl { key: String },
    FlushAll,
    DbSize,

    // Connection commands
    Ping { message: Option<String> },
    Echo { message: String },
    Auth { password: String },
    Info,
    Memory,
    ShowAll,
    Merge { file_path: String, strategy: MergeStrategy },
    Quit,
}

pub async fn execute_command(db: Database, command: Command, client_auth: &mut ClientAuth) -> String {
    // Check authentication for all commands except AUTH
    if let Command::Auth { password } = &command {
        if client_auth.authenticate(password) {
            return "OK".to_string();
        } else {
            return "(error) ERR invalid password".to_string();
        }
    }

    // Check if client is authenticated for other commands
    if client_auth.requires_auth() {
        return "(error) NOAUTH Authentication required.".to_string();
    }

    match command {
        Command::Get { key } => {
            let mut db_write = db.write().await;
            match db_write.get(&key) {
                Some(RedisValue::String(s)) => format!("\"{}\"", s),
                Some(RedisValue::Integer(i)) => i.to_string(),
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(nil)".to_string(),
            }
        },

        Command::Set { key, value } => {
            let mut db_write = db.write().await;
            db_write.set(key, RedisValue::String(value));
            "OK".to_string()
        },

        Command::SetEx { key, value, seconds } => {
            let mut db_write = db.write().await;
            db_write.set_with_expiry(key, RedisValue::String(value), Duration::from_secs(seconds));
            "OK".to_string()
        },

        Command::Del { keys } => {
            let mut db_write = db.write().await;
            let mut count = 0;
            for key in keys {
                if db_write.delete(&key) {
                    count += 1;
                }
            }
            format!("(integer) {}", count)
        },

        Command::Exists { keys } => {
            let mut db_write = db.write().await;
            let mut count = 0;
            for key in keys {
                if db_write.exists(&key) {
                    count += 1;
                }
            }
            format!("(integer) {}", count)
        },

        Command::Incr { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Integer(i)) => {
                    let new_val = i + 1;
                    db_write.set(key, RedisValue::Integer(new_val));
                    format!("(integer) {}", new_val)
                },
                Some(RedisValue::String(s)) => {
                    if let Ok(i) = s.parse::<i64>() {
                        let new_val = i + 1;
                        db_write.set(key, RedisValue::Integer(new_val));
                        format!("(integer) {}", new_val)
                    } else {
                        "(error) ERR value is not an integer or out of range".to_string()
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => {
                    db_write.set(key, RedisValue::Integer(1));
                    "(integer) 1".to_string()
                }
            }
        },

        Command::LPush { key, values } => {
            let mut db_write = db.write().await;

            let mut list = match db_write.get(&key) {
                Some(RedisValue::List(existing_list)) => existing_list.clone(),
                Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => VecDeque::new(),
            };

            for value in values.iter().rev() {
                list.push_front(value.clone());
            }

            let list_len = list.len();
            db_write.set(key, RedisValue::List(list));
            format!("(integer) {}", list_len)
        },

        Command::SAdd { key, members } => {
            let mut db_write = db.write().await;

            let mut set = match db_write.get(&key) {
                Some(RedisValue::Set(existing_set)) => existing_set.clone(),
                Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => HashSet::new(),
            };

            let mut added = 0;
            for member in members {
                if set.insert(member) {
                    added += 1;
                }
            }

            db_write.set(key, RedisValue::Set(set));
            format!("(integer) {}", added)
        },

        Command::HSet { key, field, value } => {
            let mut db_write = db.write().await;

            let mut hash = match db_write.get(&key) {
                Some(RedisValue::Hash(existing_hash)) => existing_hash.clone(),
                Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => HashMap::new(),
            };

            let is_new = hash.insert(field, value).is_none();
            db_write.set(key, RedisValue::Hash(hash));
            format!("(integer) {}", if is_new { 1 } else { 0 })
        },

        Command::Keys { pattern: _ } => {
            let mut db_write = db.write().await;
            let keys = db_write.keys();
            if keys.is_empty() {
                "(empty array)".to_string()
            } else {
                keys.iter()
                    .enumerate()
                    .map(|(i, key)| format!("{}) \"{}\"", i + 1, key))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        },

        Command::Info => {
            let mut db_write = db.write().await;
            let info = format!(
                "# Server\nredis_version:7.0.0-clone\nredis_mode:standalone\n# Memory\nused_memory:{}\n# Keyspace\ndb0:keys={}",
                db_write.size() * 100,
                db_write.size()
            );
            format!("\"{}\"", info)
        },

        Command::Memory => {
            let db_write = db.write().await;
            let memory_info = db_write.get_memory_info();
            format!("used_memory:{}\nused_memory_human:{}",
                    memory_info.get("used_memory").unwrap_or(&"0".to_string()),
                    memory_info.get("used_memory_human").unwrap_or(&"0B".to_string()))
        },

        Command::ShowAll => {
            let mut db_write = db.write().await;
            if db_write.data.is_empty() {
                return "(empty database)".to_string();
            }

            let mut result = String::new();
            result.push_str(&format!("=== DATABASE CONTENTS ({} keys) ===\n", db_write.data.len()));

            for (key, value) in &db_write.data {
                let ttl_info = if let Some(expire_time) = db_write.expires.get(key) {
                    let now = std::time::Instant::now();
                    if *expire_time > now {
                        let remaining = (*expire_time - now).as_secs();
                        format!(" (TTL: {}s)", remaining)
                    } else {
                        " (EXPIRED)".to_string()
                    }
                } else {
                    "".to_string()
                };

                match value {
                    RedisValue::String(s) => {
                        result.push_str(&format!("\"{}\" -> STRING: \"{}\"{}\n", key, s, ttl_info));
                    },
                    RedisValue::Integer(i) => {
                        result.push_str(&format!("\"{}\" -> INTEGER: {}{}\n", key, i, ttl_info));
                    },
                    RedisValue::List(list) => {
                        result.push_str(&format!("\"{}\" -> LIST ({} items): [{}]{}\n",
                                                 key,
                                                 list.len(),
                                                 list.iter().map(|item| format!("\"{}\"", item)).collect::<Vec<_>>().join(", "),
                                                 ttl_info
                        ));
                    },
                    RedisValue::Set(set) => {
                        let mut items: Vec<_> = set.iter().collect();
                        items.sort();
                        result.push_str(&format!("\"{}\" -> SET ({} items): {{{}}}{}\n",
                                                 key,
                                                 set.len(),
                                                 items.iter().map(|item| format!("\"{}\"", item)).collect::<Vec<_>>().join(", "),
                                                 ttl_info
                        ));
                    },
                    RedisValue::Hash(hash) => {
                        let mut fields: Vec<_> = hash.iter().collect();
                        fields.sort_by_key(|(k, _)| *k);
                        let hash_content = fields.iter()
                            .map(|(field, val)| format!("\"{}\" => \"{}\"", field, val))
                            .collect::<Vec<_>>()
                            .join(", ");
                        result.push_str(&format!("\"{}\" -> HASH ({} fields): {{{}}}{}\n",
                                                 key,
                                                 hash.len(),
                                                 hash_content,
                                                 ttl_info
                        ));
                    },
                }
            }

            result.push_str("=== END OF DATABASE ===");
            result
        },

        Command::Merge { file_path, strategy } => {
            let mut db_write = db.write().await;

            let persistence = MmapPersistence::new(file_path.clone());
            let merge_db = match persistence.load_database() {
                Ok(db) => db,
                Err(e) => return format!("(error) ERR failed to load merge file: {}", e),
            };

            let mut merged_count = 0;
            let mut skipped_count = 0;
            let mut overwritten_count = 0;

            for (key, value) in merge_db.data {
                let key_exists = db_write.exists(&key);

                match strategy {
                    MergeStrategy::Overwrite => {
                        if key_exists {
                            overwritten_count += 1;
                        } else {
                            merged_count += 1;
                        }
                        db_write.set(key, value);
                    },

                    MergeStrategy::Skip => {
                        if key_exists {
                            skipped_count += 1;
                        } else {
                            db_write.set(key, value);
                            merged_count += 1;
                        }
                    },

                    MergeStrategy::Merge => {
                        if key_exists {
                            match (db_write.get(&key), &value) {
                                (Some(RedisValue::List(existing_list)), RedisValue::List(new_list)) => {
                                    let mut combined_list = existing_list.clone();
                                    for item in new_list {
                                        if !combined_list.contains(item) {
                                            combined_list.push_back(item.clone());
                                        }
                                    }
                                    db_write.set(key, RedisValue::List(combined_list));
                                    merged_count += 1;
                                },

                                (Some(RedisValue::Set(existing_set)), RedisValue::Set(new_set)) => {
                                    let mut combined_set = existing_set.clone();
                                    for item in new_set {
                                        combined_set.insert(item.clone());
                                    }
                                    db_write.set(key, RedisValue::Set(combined_set));
                                    merged_count += 1;
                                },

                                (Some(RedisValue::Hash(existing_hash)), RedisValue::Hash(new_hash)) => {
                                    let mut combined_hash = existing_hash.clone();
                                    for (field, val) in new_hash {
                                        combined_hash.insert(field.clone(), val.clone());
                                    }
                                    db_write.set(key, RedisValue::Hash(combined_hash));
                                    merged_count += 1;
                                },

                                _ => {
                                    db_write.set(key, value);
                                    overwritten_count += 1;
                                }
                            }
                        } else {
                            db_write.set(key, value);
                            merged_count += 1;
                        }
                    }
                }
            }

            format!(
                "OK - Merged from '{}' using {:?} strategy\nNew keys: {}\nOverwritten: {}\nSkipped: {}",
                file_path, strategy, merged_count, overwritten_count, skipped_count
            )
        },

        Command::FlushAll => {
            let mut db_write = db.write().await;
            db_write.clear();
            "OK".to_string()
        },

        Command::Ping { message } => {
            match message {
                Some(msg) => format!("\"{}\"", msg),
                None => "PONG".to_string(),
            }
        },

        Command::Auth { .. } => {
            "OK".to_string()
        },

        Command::Quit => "OK".to_string(),

        _ => "(error) ERR unknown command".to_string(),
    }
}
