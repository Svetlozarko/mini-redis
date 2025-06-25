use crate::commands::Command;

pub fn parse_command(input: &str) -> Result<Command, String> {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();
    if parts.is_empty() {
        return Err("Empty command".to_string());
    }

    let cmd = parts[0].to_uppercase();

    match cmd.as_str() {
        "GET" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'get' command".to_string());
            }
            Ok(Command::Get { key: parts[1].to_string() })
        },

        "SET" => {
            if parts.len() < 3 {
                return Err("ERR wrong number of arguments for 'set' command".to_string());
            }
            if parts.len() == 3 {
                Ok(Command::Set {
                    key: parts[1].to_string(),
                    value: parts[2].to_string()
                })
            } else if parts.len() == 5 && parts[3].to_uppercase() == "EX" {
                match parts[4].parse::<u64>() {
                    Ok(seconds) => Ok(Command::SetEx {
                        key: parts[1].to_string(),
                        value: parts[2].to_string(),
                        seconds,
                    }),
                    Err(_) => Err("ERR invalid expire time in set".to_string()),
                }
            } else {
                Err("ERR syntax error".to_string())
            }
        },

        "DEL" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'del' command".to_string());
            }
            Ok(Command::Del {
                keys: parts[1..].iter().map(|s| s.to_string()).collect()
            })
        },

        "EXISTS" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'exists' command".to_string());
            }
            Ok(Command::Exists {
                keys: parts[1..].iter().map(|s| s.to_string()).collect()
            })
        },

        "INCR" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'incr' command".to_string());
            }
            Ok(Command::Incr { key: parts[1].to_string() })
        },

        "DECR" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'decr' command".to_string());
            }
            Ok(Command::Decr { key: parts[1].to_string() })
        },

        "LPUSH" => {
            if parts.len() < 3 {
                return Err("ERR wrong number of arguments for 'lpush' command".to_string());
            }
            Ok(Command::LPush {
                key: parts[1].to_string(),
                values: parts[2..].iter().map(|s| s.to_string()).collect()
            })
        },

        "RPUSH" => {
            if parts.len() < 3 {
                return Err("ERR wrong number of arguments for 'rpush' command".to_string());
            }
            Ok(Command::RPush {
                key: parts[1].to_string(),
                values: parts[2..].iter().map(|s| s.to_string()).collect()
            })
        },

        "LPOP" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'lpop' command".to_string());
            }
            Ok(Command::LPop { key: parts[1].to_string() })
        },

        "RPOP" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'rpop' command".to_string());
            }
            Ok(Command::RPop { key: parts[1].to_string() })
        },

        "LLEN" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'llen' command".to_string());
            }
            Ok(Command::LLen { key: parts[1].to_string() })
        },

        "SADD" => {
            if parts.len() < 3 {
                return Err("ERR wrong number of arguments for 'sadd' command".to_string());
            }
            Ok(Command::SAdd {
                key: parts[1].to_string(),
                members: parts[2..].iter().map(|s| s.to_string()).collect()
            })
        },

        "SMEMBERS" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'smembers' command".to_string());
            }
            Ok(Command::SMembers { key: parts[1].to_string() })
        },

        "HSET" => {
            if parts.len() != 4 {
                return Err("ERR wrong number of arguments for 'hset' command".to_string());
            }
            Ok(Command::HSet {
                key: parts[1].to_string(),
                field: parts[2].to_string(),
                value: parts[3].to_string()
            })
        },

        "HGET" => {
            if parts.len() != 3 {
                return Err("ERR wrong number of arguments for 'hget' command".to_string());
            }
            Ok(Command::HGet {
                key: parts[1].to_string(),
                field: parts[2].to_string()
            })
        },

        "HGETALL" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'hgetall' command".to_string());
            }
            Ok(Command::HGetAll { key: parts[1].to_string() })
        },

        "KEYS" => {
            let pattern = if parts.len() > 1 { parts[1].to_string() } else { "*".to_string() };
            Ok(Command::Keys { pattern })
        },

        "TYPE" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'type' command".to_string());
            }
            Ok(Command::Type { key: parts[1].to_string() })
        },

        "EXPIRE" => {
            if parts.len() != 3 {
                return Err("ERR wrong number of arguments for 'expire' command".to_string());
            }
            match parts[2].parse::<u64>() {
                Ok(seconds) => Ok(Command::Expire {
                    key: parts[1].to_string(),
                    seconds,
                }),
                Err(_) => Err("ERR invalid expire time".to_string()),
            }
        },

        "TTL" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'ttl' command".to_string());
            }
            Ok(Command::Ttl { key: parts[1].to_string() })
        },

        "FLUSHALL" => {
            Ok(Command::FlushAll)
        },

        "DBSIZE" => {
            Ok(Command::DbSize)
        },

        "PING" => {
            let message = if parts.len() > 1 {
                Some(parts[1..].join(" "))
            } else {
                None
            };
            Ok(Command::Ping { message })
        },

        "ECHO" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'echo' command".to_string());
            }
            Ok(Command::Echo { message: parts[1..].join(" ") })
        },

        "AUTH" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'auth' command".to_string());
            }
            Ok(Command::Auth { password: parts[1].to_string() })
        },

        "INFO" => {
            Ok(Command::Info)
        },

        "MEMORY" => {
            Ok(Command::Memory)
        },

        "SHOWALL" => {
            Ok(Command::ShowAll)
        },

        "QUIT" => {
            Ok(Command::Quit)
        },

        _ => Err(format!("ERR unknown command '{}'", cmd)),
    }
}
