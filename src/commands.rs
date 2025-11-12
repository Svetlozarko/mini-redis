use crate::data_types::RedisValue;
use crate::database::{Database, RedisDatabase};
use crate::auth::ClientAuth;
use crate::persistence_clean::MmapPersistence;
use crate::pub_sub::PubSubManager;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use clap::Error;

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
    Append { key: String, value: String },
    Strlen { key: String },
    GetRange { key: String, start: i32, end: i32 },

    // List commands
    LPush { key: String, values: Vec<String> },
    RPush { key: String, values: Vec<String> },
    LPop { key: String },
    RPop { key: String },
    LLen { key: String },
    LRange { key: String, start: i32, stop: i32 },
    LIndex { key: String, index: i32 },
    LSet { key: String, index: i32, value: String },

    // Set commands
    SAdd { key: String, members: Vec<String> },
    SRem { key: String, members: Vec<String> },
    SMembers { key: String },
    SCard { key: String },
    SIsMember { key: String, member: String },
    SInter { keys: Vec<String> },
    SUnion { keys: Vec<String> },
    SDiff { keys: Vec<String> },

    // Hash commands
    HSet { key: String, field: String, value: String },
    HGet { key: String, field: String },
    HDel { key: String, fields: Vec<String> },
    HGetAll { key: String },
    HKeys { key: String },
    HVals { key: String },
    HLen { key: String },
    HExists { key: String, field: String },
    HIncrBy { key: String, field: String, increment: i64 },

    // Generic commands
    Keys { pattern: String },
    Type { key: String },
    Expire { key: String, seconds: u64 },
    Ttl { key: String },
    FlushAll,
    DbSize,
    Persist { key: String },
    Rename { key: String, newkey: String },
    RandomKey,

    // Pub/Sub commands
    Publish { channel: String, message: String },
    Subscribe { channels: Vec<String> },
    Unsubscribe { channels: Vec<String> },
    PSubscribe { patterns: Vec<String> },
    PUnsubscribe { patterns: Vec<String> },
    PubSubChannels { pattern: Option<String> },
    PubSubNumSub { channels: Vec<String> },
    PubSubNumPat,

    // Connection commands
    Ping { message: Option<String> },
    Echo { message: String },
    Auth { password: String },
    Info,
    Memory,
    ShowAll,
    Merge { file_path: String, strategy: MergeStrategy },
    VerifyIntegrity,
    RecoverFromBackup,
    Quit,
}

pub async fn execute_command(
    db: Database,
    command: Command,
    client_auth: &mut ClientAuth,
    pubsub_manager: Option<&PubSubManager>
) -> String {
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

        Command::Decr { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Integer(i)) => {
                    let new_val = i - 1;
                    db_write.set(key, RedisValue::Integer(new_val));
                    format!("(integer) {}", new_val)
                },
                Some(RedisValue::String(s)) => {
                    if let Ok(i) = s.parse::<i64>() {
                        let new_val = i - 1;
                        db_write.set(key, RedisValue::Integer(new_val));
                        format!("(integer) {}", new_val)
                    } else {
                        "(error) ERR value is not an integer or out of range".to_string()
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => {
                    db_write.set(key, RedisValue::Integer(-1));
                    "(integer) -1".to_string()
                }
            }
        },

        Command::Append { key, value } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::String(s)) => {
                    let new_val = format!("{}{}", s, value);
                    let new_len = new_val.len();
                    db_write.set(key, RedisValue::String(new_val));
                    format!("(integer) {}", new_len)
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => {
                    let len = value.len();
                    db_write.set(key, RedisValue::String(value));
                    format!("(integer) {}", len)
                }
            }
        },

        Command::Strlen { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::String(s)) => format!("(integer) {}", s.len()),
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(integer) 0".to_string(),
            }
        },

        Command::GetRange { key, start, end } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::String(s)) => {
                    let len = s.len() as i32;
                    let start_idx = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                    let end_idx = if end < 0 { (len + end + 1).max(0) } else { (end + 1).min(len) } as usize;

                    if start_idx >= end_idx || start_idx >= s.len() {
                        "\"\"".to_string()
                    } else {
                        format!("\"{}\"", &s[start_idx..end_idx.min(s.len())])
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "\"\"".to_string(),
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

        Command::RPush { key, values } => {
            let mut db_write = db.write().await;

            let mut list = match db_write.get(&key) {
                Some(RedisValue::List(existing_list)) => existing_list.clone(),
                Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => VecDeque::new(),
            };

            for value in values {
                list.push_back(value);
            }

            let list_len = list.len();
            db_write.set(key, RedisValue::List(list));
            format!("(integer) {}", list_len)
        },

        Command::LPop { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::List(mut list)) => {
                    if let Some(value) = list.pop_front() {
                        if list.is_empty() {
                            db_write.delete(&key);
                        } else {
                            db_write.set(key, RedisValue::List(list));
                        }
                        format!("\"{}\"", value)
                    } else {
                        "(nil)".to_string()
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(nil)".to_string(),
            }
        },

        Command::RPop { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::List(mut list)) => {
                    if let Some(value) = list.pop_back() {
                        if list.is_empty() {
                            db_write.delete(&key);
                        } else {
                            db_write.set(key, RedisValue::List(list));
                        }
                        format!("\"{}\"", value)
                    } else {
                        "(nil)".to_string()
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(nil)".to_string(),
            }
        },

        Command::LLen { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::List(list)) => format!("(integer) {}", list.len()),
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(integer) 0".to_string(),
            }
        },

        Command::LRange { key, start, stop } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::List(list)) => {
                    let len = list.len() as i32;
                    let start_idx = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                    let stop_idx = if stop < 0 { (len + stop).max(-1) } else { stop.min(len - 1) } as usize;

                    if start_idx > stop_idx || start_idx >= list.len() {
                        return "(empty array)".to_string();
                    }

                    let result: Vec<String> = list.iter()
                        .skip(start_idx)
                        .take(stop_idx - start_idx + 1)
                        .enumerate()
                        .map(|(i, item)| format!("{}) \"{}\"", i + 1, item))
                        .collect();

                    if result.is_empty() {
                        "(empty array)".to_string()
                    } else {
                        result.join("\n")
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(empty array)".to_string(),
            }
        },

        Command::LIndex { key, index } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::List(list)) => {
                    let len = list.len() as i32;
                    let idx = if index < 0 { (len + index) } else { index };

                    if idx < 0 || idx >= len {
                        "(nil)".to_string()
                    } else {
                        format!("\"{}\"", list[idx as usize])
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(nil)".to_string(),
            }
        },

        Command::LSet { key, index, value } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::List(mut list)) => {
                    let len = list.len() as i32;
                    let idx = if index < 0 { (len + index) } else { index };

                    if idx < 0 || idx >= len {
                        "(error) ERR index out of range".to_string()
                    } else {
                        list[idx as usize] = value;
                        db_write.set(key, RedisValue::List(list));
                        "OK".to_string()
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(error) ERR no such key".to_string(),
            }
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

        Command::SRem { key, members } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Set(mut set)) => {
                    let mut removed = 0;
                    for member in members {
                        if set.remove(&member) {
                            removed += 1;
                        }
                    }

                    if set.is_empty() {
                        db_write.delete(&key);
                    } else {
                        db_write.set(key, RedisValue::Set(set));
                    }
                    format!("(integer) {}", removed)
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(integer) 0".to_string(),
            }
        },

        Command::SMembers { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Set(set)) => {
                    if set.is_empty() {
                        return "(empty set)".to_string();
                    }

                    let mut members: Vec<_> = set.iter().collect();
                    members.sort();
                    members.iter()
                        .enumerate()
                        .map(|(i, member)| format!("{}) \"{}\"", i + 1, member))
                        .collect::<Vec<_>>()
                        .join("\n")
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(empty set)".to_string(),
            }
        },

        Command::SCard { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Set(set)) => format!("(integer) {}", set.len()),
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(integer) 0".to_string(),
            }
        },

        Command::SIsMember { key, member } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Set(set)) => {
                    if set.contains(&member) {
                        "(integer) 1".to_string()
                    } else {
                        "(integer) 0".to_string()
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(integer) 0".to_string(),
            }
        },

        Command::SInter { keys } => {
            let mut db_write = db.write().await;

            if keys.is_empty() {
                return "(error) ERR wrong number of arguments".to_string();
            }

            let mut result: Option<HashSet<String>> = None;

            for key in keys {
                match db_write.get(&key) {
                    Some(RedisValue::Set(set)) => {
                        if let Some(ref mut res) = result {
                            *res = res.intersection(&set).cloned().collect();
                        } else {
                            result = Some(set.clone());
                        }
                    },
                    Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                    None => return "(empty set)".to_string(),
                }
            }

            match result {
                Some(set) if !set.is_empty() => {
                    let mut members: Vec<_> = set.iter().collect();
                    members.sort();
                    members.iter()
                        .enumerate()
                        .map(|(i, member)| format!("{}) \"{}\"", i + 1, member))
                        .collect::<Vec<_>>()
                        .join("\n")
                },
                _ => "(empty set)".to_string(),
            }
        },

        Command::SUnion { keys } => {
            let mut db_write = db.write().await;

            if keys.is_empty() {
                return "(error) ERR wrong number of arguments".to_string();
            }

            let mut result = HashSet::new();

            for key in keys {
                match db_write.get(&key) {
                    Some(RedisValue::Set(set)) => {
                        result = result.union(&set).cloned().collect();
                    },
                    Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                    None => continue,
                }
            }

            if result.is_empty() {
                "(empty set)".to_string()
            } else {
                let mut members: Vec<_> = result.iter().collect();
                members.sort();
                members.iter()
                    .enumerate()
                    .map(|(i, member)| format!("{}) \"{}\"", i + 1, member))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        },

        Command::SDiff { keys } => {
            let mut db_write = db.write().await;

            if keys.is_empty() {
                return "(error) ERR wrong number of arguments".to_string();
            }

            let first_key = &keys[0];
            let mut result = match db_write.get(first_key) {
                Some(RedisValue::Set(set)) => set.clone(),
                Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => return "(empty set)".to_string(),
            };

            for key in keys.iter().skip(1) {
                match db_write.get(key) {
                    Some(RedisValue::Set(set)) => {
                        result = result.difference(&set).cloned().collect();
                    },
                    Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                    None => continue,
                }
            }

            if result.is_empty() {
                "(empty set)".to_string()
            } else {
                let mut members: Vec<_> = result.iter().collect();
                members.sort();
                members.iter()
                    .enumerate()
                    .map(|(i, member)| format!("{}) \"{}\"", i + 1, member))
                    .collect::<Vec<_>>()
                    .join("\n")
            }
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

        Command::HGet { key, field } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Hash(hash)) => {
                    match hash.get(&field) {
                        Some(value) => format!("\"{}\"", value),
                        None => "(nil)".to_string(),
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(nil)".to_string(),
            }
        },

        Command::HDel { key, fields } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Hash(mut hash)) => {
                    let mut deleted = 0;
                    for field in fields {
                        if hash.remove(&field).is_some() {
                            deleted += 1;
                        }
                    }

                    if hash.is_empty() {
                        db_write.delete(&key);
                    } else {
                        db_write.set(key, RedisValue::Hash(hash));
                    }
                    format!("(integer) {}", deleted)
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(integer) 0".to_string(),
            }
        },

        Command::HGetAll { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Hash(hash)) => {
                    if hash.is_empty() {
                        return "(empty hash)".to_string();
                    }

                    let mut fields: Vec<_> = hash.iter().collect();
                    fields.sort_by_key(|(k, _)| *k);

                    let mut result = Vec::new();
                    let mut idx = 1;
                    for (field, value) in fields {
                        result.push(format!("{}) \"{}\"", idx, field));
                        result.push(format!("{}) \"{}\"", idx + 1, value));
                        idx += 2;
                    }
                    result.join("\n")
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(empty hash)".to_string(),
            }
        },

        Command::HKeys { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Hash(hash)) => {
                    if hash.is_empty() {
                        return "(empty array)".to_string();
                    }

                    let mut keys: Vec<_> = hash.keys().collect();
                    keys.sort();
                    keys.iter()
                        .enumerate()
                        .map(|(i, k)| format!("{}) \"{}\"", i + 1, k))
                        .collect::<Vec<_>>()
                        .join("\n")
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(empty array)".to_string(),
            }
        },

        Command::HVals { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Hash(hash)) => {
                    if hash.is_empty() {
                        return "(empty array)".to_string();
                    }

                    let mut entries: Vec<_> = hash.iter().collect();
                    entries.sort_by_key(|(k, _)| *k);

                    entries.iter()
                        .enumerate()
                        .map(|(i, (_, v))| format!("{}) \"{}\"", i + 1, v))
                        .collect::<Vec<_>>()
                        .join("\n")
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(empty array)".to_string(),
            }
        },

        Command::HLen { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Hash(hash)) => format!("(integer) {}", hash.len()),
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(integer) 0".to_string(),
            }
        },

        Command::HExists { key, field } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::Hash(hash)) => {
                    if hash.contains_key(&field) {
                        "(integer) 1".to_string()
                    } else {
                        "(integer) 0".to_string()
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(integer) 0".to_string(),
            }
        },

        Command::HIncrBy { key, field, increment } => {
            let mut db_write = db.write().await;

            let mut hash = match db_write.get(&key) {
                Some(RedisValue::Hash(existing_hash)) => existing_hash.clone(),
                Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => HashMap::new(),
            };

            let new_value = match hash.get(&field) {
                Some(val) => {
                    match val.parse::<i64>() {
                        Ok(current) => current + increment,
                        Err(_) => return "(error) ERR hash value is not an integer".to_string(),
                    }
                },
                None => increment,
            };

            hash.insert(field, new_value.to_string());
            db_write.set(key, RedisValue::Hash(hash));
            format!("(integer) {}", new_value)
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

        Command::Type { key } => {
            let mut db_write = db.write().await;

            match db_write.get(&key) {
                Some(RedisValue::String(_)) => "string".to_string(),
                Some(RedisValue::Integer(_)) => "string".to_string(),
                Some(RedisValue::List(_)) => "list".to_string(),
                Some(RedisValue::Set(_)) => "set".to_string(),
                Some(RedisValue::Hash(_)) => "hash".to_string(),
                None => "none".to_string(),
            }
        },

        Command::Expire { key, seconds } => {
            let mut db_write = db.write().await;

            if !db_write.exists(&key) {
                return "(integer) 0".to_string();
            }

            if let Some(value) = db_write.get(&key) {
                db_write.set_with_expiry(key, value.clone(), Duration::from_secs(seconds));
                "(integer) 1".to_string()
            } else {
                "(integer) 0".to_string()
            }
        },

        Command::Ttl { key } => {
            let mut db_write = db.write().await;

            if !db_write.exists(&key) {
                return "(integer) -2".to_string();
            }

            if let Some(expire_time) = db_write.expires.get(&key) {
                let now = std::time::Instant::now();
                if *expire_time > now {
                    let remaining = (*expire_time - now).as_secs();
                    format!("(integer) {}", remaining)
                } else {
                    "(integer) -2".to_string()
                }
            } else {
                "(integer) -1".to_string()
            }
        },

        Command::Persist { key } => {
            let mut db_write = db.write().await;

            if db_write.expires.remove(&key).is_some() {
                "(integer) 1".to_string()
            } else {
                "(integer) 0".to_string()
            }
        },

        Command::Rename { key, newkey } => {
            let mut db_write = db.write().await;

            if !db_write.exists(&key) {
                return "(error) ERR no such key".to_string();
            }

            if let Some(value) = db_write.get(&key) {
                let value_clone = value.clone();
                let expiry = db_write.expires.get(&key).copied();

                db_write.delete(&key);

                if let Some(expire_time) = expiry {
                    let now = std::time::Instant::now();
                    if expire_time > now {
                        let remaining = expire_time - now;
                        db_write.set_with_expiry(newkey, value_clone, remaining);
                    } else {
                        db_write.set(newkey, value_clone);
                    }
                } else {
                    db_write.set(newkey, value_clone);
                }

                "OK".to_string()
            } else {
                "(error) ERR no such key".to_string()
            }
        },

        Command::RandomKey => {
            let db_write = db.write().await;
            let keys = db_write.keys();

            if keys.is_empty() {
                "(nil)".to_string()
            } else {
                use std::collections::hash_map::RandomState;
                use std::hash::{BuildHasher, Hash, Hasher};

                let random_state = RandomState::new();
                let mut hasher = random_state.build_hasher();
                std::time::SystemTime::now().hash(&mut hasher);
                let random_idx = (hasher.finish() as usize) % keys.len();

                format!("\"{}\"", keys[random_idx])
            }
        },

        Command::DbSize => {
            let db_write = db.write().await;
            format!("(integer) {}", db_write.size())
        },

        Command::Echo { message } => {
            format!("\"{}\"", message)
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

        Command::Publish { channel, message } => {
            if let Some(pubsub) = pubsub_manager {
                let pubsub_state = pubsub.read().await;
                let count = pubsub_state.publish(&channel, message);
                format!("(integer) {}", count)
            } else {
                "(error) ERR Pub/Sub not available".to_string()
            }
        },

        Command::PubSubChannels { pattern } => {
            if let Some(pubsub) = pubsub_manager {
                let pubsub_state = pubsub.read().await;
                let channels = pubsub_state.get_channels();

                let filtered: Vec<String> = if let Some(pat) = pattern {
                    channels.into_iter()
                        .filter(|ch| ch.contains(&pat))
                        .collect()
                } else {
                    channels
                };

                if filtered.is_empty() {
                    "(empty array)".to_string()
                } else {
                    filtered.iter()
                        .enumerate()
                        .map(|(i, ch)| format!("{}) \"{}\"", i + 1, ch))
                        .collect::<Vec<_>>()
                        .join("\n")
                }
            } else {
                "(error) ERR Pub/Sub not available".to_string()
            }
        },

        Command::PubSubNumSub { channels } => {
            if let Some(pubsub) = pubsub_manager {
                let pubsub_state = pubsub.read().await;
                let mut result = Vec::new();

                for channel in channels {
                    let count = pubsub_state.get_channel_subscribers(&channel);
                    result.push(format!("\"{}\"", channel));
                    result.push(format!("(integer) {}", count));
                }

                if result.is_empty() {
                    "(empty array)".to_string()
                } else {
                    result.iter()
                        .enumerate()
                        .map(|(i, item)| format!("{}) {}", i + 1, item))
                        .collect::<Vec<_>>()
                        .join("\n")
                }
            } else {
                "(error) ERR Pub/Sub not available".to_string()
            }
        },

        Command::PubSubNumPat => {
            if let Some(pubsub) = pubsub_manager {
                let pubsub_state = pubsub.read().await;
                format!("(integer) {}", pubsub_state.patterns.len())  // just access fields
            } else {
                "(error) ERR Pub/Sub not available".to_string()
            }
        },
        Command::Subscribe { .. } | Command::Unsubscribe { .. } |
        Command::PSubscribe { .. } | Command::PUnsubscribe { .. } => {
            "(error) ERR only allowed in subscriber mode".to_string()
        },

        Command::Quit => "OK".to_string(),
        _ => String::new()    }
}
