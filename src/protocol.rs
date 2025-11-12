use crate::commands::Command;

pub fn parse_command(input: &str) -> Result<Command, String> {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();

    if parts.is_empty() {
        return Err("Empty command".to_string());
    }

    let cmd = parts[0].to_uppercase();

    match cmd.as_str() {
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

        _ => Err(format!("Unknown command: {}", cmd)),
    }
}
