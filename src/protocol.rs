use crate::commands::Command;

pub fn parse_command(input: &str) -> Result<Command, String> {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();
    if parts.is_empty() {
        return Err("Empty command".to_string());
    }

    let cmd = parts[0].to_uppercase();

    match cmd.as_str() {
        // String commands
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

        "APPEND" => {
            if parts.len() != 3 {
                return Err("ERR wrong number of arguments for 'append' command".to_string());
            }
            Ok(Command::Append {
                key: parts[1].to_string(),
                value: parts[2].to_string()
            })
        },

        "STRLEN" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'strlen' command".to_string());
            }
            Ok(Command::Strlen { key: parts[1].to_string() })
        },

        "GETRANGE" => {
            if parts.len() != 4 {
                return Err("ERR wrong number of arguments for 'getrange' command".to_string());
            }
            match (parts[2].parse::<i32>(), parts[3].parse::<i32>()) {
                (Ok(start), Ok(end)) => Ok(Command::GetRange {
                    key: parts[1].to_string(),
                    start,
                    end
                }),
                _ => Err("ERR invalid start or end index".to_string()),
            }
        },

        // List commands
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

        "LRANGE" => {
            if parts.len() != 4 {
                return Err("ERR wrong number of arguments for 'lrange' command".to_string());
            }
            match (parts[2].parse::<i32>(), parts[3].parse::<i32>()) {
                (Ok(start), Ok(stop)) => Ok(Command::LRange {
                    key: parts[1].to_string(),
                    start,
                    stop
                }),
                _ => Err("ERR invalid start or stop index".to_string()),
            }
        },

        "LINDEX" => {
            if parts.len() != 3 {
                return Err("ERR wrong number of arguments for 'lindex' command".to_string());
            }
            match parts[2].parse::<i32>() {
                Ok(index) => Ok(Command::LIndex {
                    key: parts[1].to_string(),
                    index
                }),
                Err(_) => Err("ERR invalid index".to_string()),
            }
        },

        "LSET" => {
            if parts.len() != 4 {
                return Err("ERR wrong number of arguments for 'lset' command".to_string());
            }
            match parts[2].parse::<i32>() {
                Ok(index) => Ok(Command::LSet {
                    key: parts[1].to_string(),
                    index,
                    value: parts[3].to_string()
                }),
                Err(_) => Err("ERR invalid index".to_string()),
            }
        },

        // Set commands
        "SADD" => {
            if parts.len() < 3 {
                return Err("ERR wrong number of arguments for 'sadd' command".to_string());
            }
            Ok(Command::SAdd {
                key: parts[1].to_string(),
                members: parts[2..].iter().map(|s| s.to_string()).collect()
            })
        },

        "SREM" => {
            if parts.len() < 3 {
                return Err("ERR wrong number of arguments for 'srem' command".to_string());
            }
            Ok(Command::SRem {
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

        "SCARD" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'scard' command".to_string());
            }
            Ok(Command::SCard { key: parts[1].to_string() })
        },

        "SISMEMBER" => {
            if parts.len() != 3 {
                return Err("ERR wrong number of arguments for 'sismember' command".to_string());
            }
            Ok(Command::SIsMember {
                key: parts[1].to_string(),
                member: parts[2].to_string()
            })
        },

        "SINTER" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'sinter' command".to_string());
            }
            Ok(Command::SInter {
                keys: parts[1..].iter().map(|s| s.to_string()).collect()
            })
        },

        "SUNION" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'sunion' command".to_string());
            }
            Ok(Command::SUnion {
                keys: parts[1..].iter().map(|s| s.to_string()).collect()
            })
        },

        "SDIFF" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'sdiff' command".to_string());
            }
            Ok(Command::SDiff {
                keys: parts[1..].iter().map(|s| s.to_string()).collect()
            })
        },

        // Hash commands
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

        "HDEL" => {
            if parts.len() < 3 {
                return Err("ERR wrong number of arguments for 'hdel' command".to_string());
            }
            Ok(Command::HDel {
                key: parts[1].to_string(),
                fields: parts[2..].iter().map(|s| s.to_string()).collect()
            })
        },

        "HGETALL" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'hgetall' command".to_string());
            }
            Ok(Command::HGetAll { key: parts[1].to_string() })
        },

        "HKEYS" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'hkeys' command".to_string());
            }
            Ok(Command::HKeys { key: parts[1].to_string() })
        },

        "HVALS" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'hvals' command".to_string());
            }
            Ok(Command::HVals { key: parts[1].to_string() })
        },

        "HLEN" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'hlen' command".to_string());
            }
            Ok(Command::HLen { key: parts[1].to_string() })
        },

        "HEXISTS" => {
            if parts.len() != 3 {
                return Err("ERR wrong number of arguments for 'hexists' command".to_string());
            }
            Ok(Command::HExists {
                key: parts[1].to_string(),
                field: parts[2].to_string()
            })
        },

        "HINCRBY" => {
            if parts.len() != 4 {
                return Err("ERR wrong number of arguments for 'hincrby' command".to_string());
            }
            match parts[3].parse::<i64>() {
                Ok(increment) => Ok(Command::HIncrBy {
                    key: parts[1].to_string(),
                    field: parts[2].to_string(),
                    increment
                }),
                Err(_) => Err("ERR invalid increment".to_string()),
            }
        },

        // Generic commands
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

        "PERSIST" => {
            if parts.len() != 2 {
                return Err("ERR wrong number of arguments for 'persist' command".to_string());
            }
            Ok(Command::Persist { key: parts[1].to_string() })
        },

        "RENAME" => {
            if parts.len() != 3 {
                return Err("ERR wrong number of arguments for 'rename' command".to_string());
            }
            Ok(Command::Rename {
                key: parts[1].to_string(),
                newkey: parts[2].to_string()
            })
        },

        "RANDOMKEY" => {
            Ok(Command::RandomKey)
        },

        // Pub/Sub commands
        "PUBLISH" => {
            if parts.len() < 3 {
                return Err("ERR wrong number of arguments for 'publish' command".to_string());
            }
            Ok(Command::Publish {
                channel: parts[1].to_string(),
                message: parts[2..].join(" "),
            })
        },

        "SUBSCRIBE" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'subscribe' command".to_string());
            }
            Ok(Command::Subscribe {
                channels: parts[1..].iter().map(|s| s.to_string()).collect(),
            })
        },

        "UNSUBSCRIBE" => {
            Ok(Command::Unsubscribe {
                channels: if parts.len() > 1 {
                    parts[1..].iter().map(|s| s.to_string()).collect()
                } else {
                    vec![]
                },
            })
        },

        "PSUBSCRIBE" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'psubscribe' command".to_string());
            }
            Ok(Command::PSubscribe {
                patterns: parts[1..].iter().map(|s| s.to_string()).collect(),
            })
        },

        "PUNSUBSCRIBE" => {
            Ok(Command::PUnsubscribe {
                patterns: if parts.len() > 1 {
                    parts[1..].iter().map(|s| s.to_string()).collect()
                } else {
                    vec![]
                },
            })
        },

        "PUBSUB" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'pubsub' command".to_string());
            }

            match parts[1].to_uppercase().as_str() {
                "CHANNELS" => {
                    Ok(Command::PubSubChannels {
                        pattern: if parts.len() > 2 {
                            Some(parts[2].to_string())
                        } else {
                            None
                        },
                    })
                },
                "NUMSUB" => {
                    Ok(Command::PubSubNumSub {
                        channels: if parts.len() > 2 {
                            parts[2..].iter().map(|s| s.to_string()).collect()
                        } else {
                            vec![]
                        },
                    })
                },
                "NUMPAT" => Ok(Command::PubSubNumPat),
                _ => Err(format!("ERR unknown PUBSUB subcommand '{}'", parts[1])),
            }
        },

        "VERIFYINTEGRITY" | "VERIFY" => Ok(Command::VerifyIntegrity),

        "RECOVERFROMBACKUP" | "RECOVER" => Ok(Command::RecoverFromBackup),

        // Connection commands
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

        "MERGE" => {
            if parts.len() < 2 {
                return Err("ERR wrong number of arguments for 'merge' command".to_string());
            }

            let file_path = parts[1].to_string();
            let strategy = if parts.len() > 2 {
                match parts[2].to_uppercase().as_str() {
                    "OVERWRITE" => crate::commands::MergeStrategy::Overwrite,
                    "SKIP" => crate::commands::MergeStrategy::Skip,
                    "MERGE" => crate::commands::MergeStrategy::Merge,
                    _ => return Err("ERR invalid merge strategy. Use OVERWRITE, SKIP, or MERGE".to_string()),
                }
            } else {
                crate::commands::MergeStrategy::Overwrite
            };

            Ok(Command::Merge { file_path, strategy })
        },

        "QUIT" => {
            Ok(Command::Quit)
        },

        _ => Err(format!("ERR unknown command '{}'", cmd)),
    }
}
