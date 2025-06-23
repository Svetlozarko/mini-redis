use crate::db::Database;
use std::sync::Arc;

pub fn handle_command(input: &str, db: &Arc<Database>) -> String {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();

    match parts.as_slice() {
        ["SET", key, value] => {
            db.set(key.to_string(), value.to_string());
            "+OK\n".to_string()
        }

        ["GET", key] => {
            if let Some(value) = db.get(key) {
                format!("${}\n{}\n", value.len(), value)
            } else {
                "$-1\n".to_string()
            }
        }

        ["DEL", key] => {
            if db.del(key) {
                ":1\n".to_string()
            } else {
                ":0\n".to_string()
            }
        }

        ["EXISTS", key] => {
            if db.exists(key) {
                ":1\n".to_string()
            } else {
                ":0\n".to_string()
            }
        }

        ["INCR", key] => match db.incr(key) {
            Ok(new_val) => format!(":{}\n", new_val),
            Err(msg) => format!("-ERR {}\n", msg),
        }

        ["EXPIRE", key, seconds_str] => match seconds_str.parse::<u64>() {
            Ok(seconds) => {
                if db.expire(key, seconds) {
                    ":1\n".to_string()
                } else {
                    ":0\n".to_string()
                }
            }
            Err(_) => "-ERR invalid expiration time\n".to_string(),
        }

        ["TTL", key] => {
            let ttl = db.ttl(key);
            format!(":{}\n", ttl)
        }

        ["PERSIST", key] => {
            if db.persist(key) {
                ":1\n".to_string()
            } else {
                ":0\n".to_string()
            }
        }

        ["FLUSHDB"] => {
            db.flushdb();
            "+OK\n".to_string()
        }

        _ => "-ERR unknown command or wrong number of arguments\n".to_string(),
    }
}
