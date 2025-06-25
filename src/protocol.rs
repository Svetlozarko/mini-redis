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

        "AUTH" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'auth' command".to_string());
            }
            Ok(Command::Auth { password: parts[1].to_string() })
        },

        "INFO" => {
            Ok(Command::Info)
        },

        "PING" => {
            let message = if parts.len() > 1 {
                Some(parts[1..].join(" "))
            } else {
                None
            };
            Ok(Command::Ping { message })
        },

        "KEYS" => {
            let pattern = if parts.len() > 1 { parts[1].to_string() } else { "*".to_string() };
            Ok(Command::Keys { pattern })
        },

        "FLUSHALL" => {
            Ok(Command::FlushAll)
        },

        "QUIT" => {
            Ok(Command::Quit)
        },

        _ => Err(format!("ERR unknown command '{}'", cmd)),
    }
}