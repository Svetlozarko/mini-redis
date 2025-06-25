use std::collections::{HashMap, HashSet, VecDeque};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedisValue {
    String(String),
    List(VecDeque<String>),
    Set(HashSet<String>),
    Hash(HashMap<String, String>),
    Integer(i64),
}

impl RedisValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Set(_) => "set",
            RedisValue::Hash(_) => "hash",
            RedisValue::Integer(_) => "integer",
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        match self {
            RedisValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_list_mut(&mut self) -> Option<&mut VecDeque<String>> {
        match self {
            RedisValue::List(list) => Some(list),
            _ => None,
        }
    }

    pub fn as_set_mut(&mut self) -> Option<&mut HashSet<String>> {
        match self {
            RedisValue::Set(set) => Some(set),
            _ => None,
        }
    }

    pub fn as_hash_mut(&mut self) -> Option<&mut HashMap<String, String>> {
        match self {
            RedisValue::Hash(hash) => Some(hash),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RedisValue::Integer(i) => Some(*i),
            _ => None,
        }
    }
}

impl std::fmt::Display for RedisValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisValue::String(s) => write!(f, "{}", s),
            RedisValue::Integer(i) => write!(f, "{}", i),
            RedisValue::List(list) => {
                let items: Vec<String> = list.iter().enumerate()
                    .map(|(i, item)| format!("{}) {}", i + 1, item))
                    .collect();
                write!(f, "{}", items.join("\n"))
            },
            RedisValue::Set(set) => {
                let items: Vec<String> = set.iter().enumerate()
                    .map(|(i, item)| format!("{}) {}", i + 1, item))
                    .collect();
                write!(f, "{}", items.join("\n"))
            },
            RedisValue::Hash(hash) => {
                let items: Vec<String> = hash.iter().enumerate()
                    .map(|(i, (k, v))| format!("{}) {}\n{}) {}", i * 2 + 1, k, i * 2 + 2, v))
                    .collect();
                write!(f, "{}", items.join("\n"))
            },
        }
    }
}