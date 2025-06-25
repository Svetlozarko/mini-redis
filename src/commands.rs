use crate::data_types::RedisValue;
use crate::database::Database;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

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
    Quit,
}

pub fn execute_command(db: Database, command: Command) -> String {
    match command {
        Command::Get { key } => {
            let mut db = db.write().unwrap();
            match db.get(&key) {
                Some(RedisValue::String(s)) => format!("\"{}\"", s),
                Some(RedisValue::Integer(i)) => i.to_string(),
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(nil)".to_string(),
            }
        },

        Command::Set { key, value } => {
            let mut db = db.write().unwrap();
            db.set(key, RedisValue::String(value));
            "OK".to_string()
        },

        Command::SetEx { key, value, seconds } => {
            let mut db = db.write().unwrap();
            db.set_with_expiry(key, RedisValue::String(value), Duration::from_secs(seconds));
            "OK".to_string()
        },

        Command::Del { keys } => {
            let mut db = db.write().unwrap();
            let mut count = 0;
            for key in keys {
                if db.delete(&key) {
                    count += 1;
                }
            }
            format!("(integer) {}", count)
        },

        Command::Exists { keys } => {
            let mut db = db.write().unwrap();
            let mut count = 0;
            for key in keys {
                if db.exists(&key) {
                    count += 1;
                }
            }
            format!("(integer) {}", count)
        },

        Command::Incr { key } => {
            let mut db = db.write().unwrap();
            match db.get_mut(&key) {
                Some(RedisValue::Integer(ref mut i)) => {
                    *i += 1;
                    format!("(integer) {}", *i)
                },
                Some(RedisValue::String(s)) => {
                    if let Ok(i) = s.parse::<i64>() {
                        let new_val = i + 1;
                        db.set(key, RedisValue::Integer(new_val));
                        format!("(integer) {}", new_val)
                    } else {
                        "(error) ERR value is not an integer or out of range".to_string()
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => {
                    db.set(key, RedisValue::Integer(1));
                    "(integer) 1".to_string()
                }
            }
        },

        Command::Decr { key } => {
            let mut db = db.write().unwrap();
            match db.get_mut(&key) {
                Some(RedisValue::Integer(ref mut i)) => {
                    *i -= 1;
                    format!("(integer) {}", *i)
                },
                Some(RedisValue::String(s)) => {
                    if let Ok(i) = s.parse::<i64>() {
                        let new_val = i - 1;
                        db.set(key, RedisValue::Integer(new_val));
                        format!("(integer) {}", new_val)
                    } else {
                        "(error) ERR value is not an integer or out of range".to_string()
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => {
                    db.set(key, RedisValue::Integer(-1));
                    "(integer) -1".to_string()
                }
            }
        },

        Command::LPush { key, values } => {
            let mut db = db.write().unwrap();
            let list = match db.get_mut(&key) {
                Some(RedisValue::List(list)) => list,
                Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => {
                    db.set(key.clone(), RedisValue::List(VecDeque::new()));
                    db.get_mut(&key).unwrap().as_list_mut().unwrap()
                }
            };

            for value in values.iter().rev() {
                list.push_front(value.clone());
            }
            format!("(integer) {}", list.len())
        },

        Command::RPush { key, values } => {
            let mut db = db.write().unwrap();
            let list = match db.get_mut(&key) {
                Some(RedisValue::List(list)) => list,
                Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => {
                    db.set(key.clone(), RedisValue::List(VecDeque::new()));
                    db.get_mut(&key).unwrap().as_list_mut().unwrap()
                }
            };

            for value in values {
                list.push_back(value);
            }
            format!("(integer) {}", list.len())
        },

        Command::LPop { key } => {
            let mut db = db.write().unwrap();
            match db.get_mut(&key) {
                Some(RedisValue::List(list)) => {
                    match list.pop_front() {
                        Some(value) => format!("\"{}\"", value),
                        None => "(nil)".to_string(),
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(nil)".to_string(),
            }
        },

        Command::RPop { key } => {
            let mut db = db.write().unwrap();
            match db.get_mut(&key) {
                Some(RedisValue::List(list)) => {
                    match list.pop_back() {
                        Some(value) => format!("\"{}\"", value),
                        None => "(nil)".to_string(),
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(nil)".to_string(),
            }
        },

        Command::LLen { key } => {
            let mut db = db.write().unwrap();
            match db.get(&key) {
                Some(RedisValue::List(list)) => format!("(integer) {}", list.len()),
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(integer) 0".to_string(),
            }
        },

        Command::SAdd { key, members } => {
            let mut db = db.write().unwrap();
            let set = match db.get_mut(&key) {
                Some(RedisValue::Set(set)) => set,
                Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => {
                    db.set(key.clone(), RedisValue::Set(HashSet::new()));
                    db.get_mut(&key).unwrap().as_set_mut().unwrap()
                }
            };

            let mut added = 0;
            for member in members {
                if set.insert(member) {
                    added += 1;
                }
            }
            format!("(integer) {}", added)
        },

        Command::SMembers { key } => {
            let mut db = db.write().unwrap();
            match db.get(&key) {
                Some(RedisValue::Set(set)) => {
                    let members: Vec<String> = set.iter()
                        .enumerate()
                        .map(|(i, member)| format!("{}) \"{}\"", i + 1, member))
                        .collect();
                    if members.is_empty() {
                        "(empty set)".to_string()
                    } else {
                        members.join("\n")
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(empty set)".to_string(),
            }
        },

        Command::HSet { key, field, value } => {
            let mut db = db.write().unwrap();
            let hash = match db.get_mut(&key) {
                Some(RedisValue::Hash(hash)) => hash,
                Some(_) => return "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => {
                    db.set(key.clone(), RedisValue::Hash(HashMap::new()));
                    db.get_mut(&key).unwrap().as_hash_mut().unwrap()
                }
            };

            let is_new = hash.insert(field, value).is_none();
            format!("(integer) {}", if is_new { 1 } else { 0 })
        },

        Command::HGet { key, field } => {
            let mut db = db.write().unwrap();
            match db.get(&key) {
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

        Command::HGetAll { key } => {
            let mut db = db.write().unwrap();
            match db.get(&key) {
                Some(RedisValue::Hash(hash)) => {
                    if hash.is_empty() {
                        "(empty hash)".to_string()
                    } else {
                        let items: Vec<String> = hash.iter()
                            .enumerate()
                            .flat_map(|(i, (k, v))| vec![
                                format!("{}) \"{}\"", i * 2 + 1, k),
                                format!("{}) \"{}\"", i * 2 + 2, v)
                            ])
                            .collect();
                        items.join("\n")
                    }
                },
                Some(_) => "(error) WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                None => "(empty hash)".to_string(),
            }
        },

        Command::Keys { pattern: _ } => {
            let db = db.read().unwrap();
            let keys = db.keys();
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
            let mut db = db.write().unwrap();
            match db.get(&key) {
                Some(value) => value.type_name().to_string(),
                None => "none".to_string(),
            }
        },

        Command::Expire { key, seconds } => {
            let mut db = db.write().unwrap();
            if db.expire(&key, Duration::from_secs(seconds)) {
                "(integer) 1".to_string()
            } else {
                "(integer) 0".to_string()
            }
        },

        Command::Ttl { key } => {
            let mut db = db.write().unwrap();
            match db.ttl(&key) {
                Some(duration) => {
                    if duration == Duration::MAX {
                        "(integer) -1".to_string() // No expiry
                    } else {
                        format!("(integer) {}", duration.as_secs())
                    }
                },
                None => "(integer) -2".to_string(), // Key doesn't exist
            }
        },

        Command::FlushAll => {
            let mut db = db.write().unwrap();
            db.clear();
            "OK".to_string()
        },

        Command::DbSize => {
            let db = db.read().unwrap();
            format!("(integer) {}", db.size())
        },

        Command::Ping { message } => {
            match message {
                Some(msg) => format!("\"{}\"", msg),
                None => "PONG".to_string(),
            }
        },

        Command::Echo { message } => {
            format!("\"{}\"", message)
        },

        Command::Quit => {
            "OK".to_string()
        },

        _ => "(error) ERR unknown command".to_string(),
    }
}