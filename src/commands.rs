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
        _ => "-ERR unknown command\n".to_string(),
    }
}
